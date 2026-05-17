use std::{path::PathBuf, time::Duration};

use example_support::session_stream;
use mango_bdd::{Scenario, ScenarioFailure};
use mango_core::agent::{
    AgentSchema, Event, EventPayload, EventVisibility, ExecutionEvent, InferenceEvent,
    InteractionEvent,
};
use mango_telegram::{TelegramChatId, TelegramSurface};
use telegram_chat::{
    ChatBus, ChatInput, ChatInputContent, ChatSchema, ClaudeConversationConfig,
    ClaudeConversationInference, ConversationControl, UsernameWhitelist, chat_session,
};

const NOT_MY_CUSTOMER: &str = "sorry, you're not my customer";

#[tokio::test]
async fn unauthorized_turns_are_rejected_without_panicking() -> Result<(), ScenarioFailure> {
    let surface = TelegramSurface {
        chat_id: TelegramChatId(7),
        thread_id: None,
        username: Some("intruder".to_string()),
        display_name: "Intruder".to_string(),
    };
    let session = chat_session(surface);
    let whitelist = UsernameWhitelist::from_usernames(["trusted_customer"]);
    let backend = ClaudeConversationConfig {
        cwd: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        claude_executable: "claude".to_string(),
        model: None,
        system_prompt_append: None,
    };

    let mut scenario = Scenario::new(
        "unauthorized telegram turns are rejected in-process",
        ChatBus::new(64),
    )
    .with_recent_event_limit(8)
    .with_event_summary(summarize_chat_event);

    scenario.world().spawn_bus_worker(
        "control",
        ConversationControl::new(session.clone(), whitelist),
    );
    scenario.world().spawn_bus_worker(
        "inference",
        ClaudeConversationInference::new(session.clone(), backend),
    );

    scenario
        .given("the control and inference workers are running")
        .stays_healthy()
        .await?;

    scenario
        .when("an unauthorized telegram turn is committed")
        .publish(
            session_stream::<ChatSchema>(&session),
            EventVisibility::Internal,
            EventPayload::Interaction(InteractionEvent::InputCommitted {
                session_id: session.session_id,
                thread_id: session.thread_id,
                stream_id: ChatSchema::next_input_stream_id(),
                revision_id: ChatSchema::next_revision_id(),
                turn_id: ChatSchema::next_turn_id(),
                input: ChatInput {
                    username: Some("intruder".to_string()),
                    display_name: "Intruder".to_string(),
                    content: ChatInputContent::Text {
                        text: "hello?".to_string(),
                    },
                },
            }),
        )
        .await?;

    scenario
        .then("the whitelist rejection is emitted")
        .expect_eventually(
            "a user-visible rejection output",
            Duration::from_millis(100),
            |event| {
                matches!(
                    &event.payload,
                    EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
                        output,
                        ..
                    })) if output == NOT_MY_CUSTOMER
                )
            },
        )
        .await?;

    scenario
        .then("no Claude-backed inference is started for the rejected turn")
        .expect_no_event("a Claude inference start", |event| {
            matches!(
                &event.payload,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Started {
                    engine,
                    ..
                })) if engine.as_ref() == "claude-agent-sdk"
            )
        })
        .await?;

    Ok(())
}

fn summarize_chat_event(event: &Event<ChatSchema>) -> String {
    match &event.payload {
        EventPayload::Interaction(InteractionEvent::InputCommitted { input, .. }) => {
            let text = match &input.content {
                ChatInputContent::Text { text } => text.clone(),
                ChatInputContent::Photo {
                    local_path,
                    caption,
                } => format!("photo:{} caption={:?}", local_path.display(), caption),
            };
            format!("input_committed user={:?} text={:?}", input.username, text)
        }
        EventPayload::Execution(ExecutionEvent::Control(control)) => format!("control {control:?}"),
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Started {
            engine,
            directive,
            ..
        })) => {
            format!(
                "inference_started engine={} directive={directive:?}",
                engine.as_ref()
            )
        }
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
            output,
            ..
        })) => format!("inference_output {output:?}"),
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Completed {
            ..
        })) => "inference_completed".to_string(),
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Cancelled {
            ..
        })) => "inference_cancelled".to_string(),
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Failed {
            error,
            ..
        })) => format!("inference_failed {} {:?}", error.code, error.message),
        EventPayload::Error(error) => format!(
            "worker_error worker={} code={} message={:?}",
            error.worker_id.as_ref(),
            error.error.code,
            error.error.message
        ),
        payload => format!("{payload:?}"),
    }
}
