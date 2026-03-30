use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use code_agent::{
    ClaudeBridgeLike, ClaudeCodingInference, CodeBus, CodeSchema, CodingProjector,
    CodingToolsWorker, PromptControl, ThinkingStatusWorker, cli_session,
};
use example_support::{ConcurrentBusWorkers, session_stream};
use mango_bdd::{Scenario, ScenarioFailure};
use mango_core::agent::{
    AgentSchema, Cancellation, Event, EventPayload, EventVisibility, ExecutionEvent,
    InferenceEvent, InteractionEvent, InterruptCause, SessionContext,
};
use mango_shim_claude_agent::ClaudeBridgeEvent;
use serde_json::json;
use tokio::sync::{Mutex, broadcast};

type CodeScenario = Scenario<CodeSchema, code_agent::CodeAppError, CodeBus>;

fn spawn_code_agent_workers(
    scenario: &mut CodeScenario,
    bridge: ScriptedClaudeBridge,
) -> (SessionContext<CodeSchema>, ScriptedClaudeBridge) {
    let session = cli_session();
    scenario
        .world()
        .spawn_bus_worker("control", PromptControl::new(session.clone()));
    scenario.world().spawn_bus_worker(
        "inference",
        ClaudeCodingInference::new(session.clone(), bridge.clone()),
    );
    scenario.world().spawn_bus_worker(
        "tools",
        CodingToolsWorker::new(
            session.clone(),
            bridge.clone(),
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        ),
    );
    scenario.world().spawn_bus_worker(
        "presentation",
        ConcurrentBusWorkers::new(
            "presentation",
            ThinkingStatusWorker::new(session.clone()),
            CodingProjector::new(session.clone()),
        ),
    );

    (session, bridge)
}

#[derive(Debug, Clone)]
struct ScriptedClaudeBridge {
    auto_reply: bool,
    events: broadcast::Sender<ClaudeBridgeEvent>,
    prompts: Arc<Mutex<Vec<String>>>,
    context_windows: Arc<Mutex<Vec<Vec<String>>>>,
    interrupts: Arc<Mutex<u64>>,
}

impl ScriptedClaudeBridge {
    fn new(auto_reply: bool) -> Self {
        let (events, _) = broadcast::channel(128);
        Self {
            auto_reply,
            events,
            prompts: Arc::new(Mutex::new(Vec::new())),
            context_windows: Arc::new(Mutex::new(Vec::new())),
            interrupts: Arc::new(Mutex::new(0)),
        }
    }

    async fn prompts(&self) -> Vec<String> {
        self.prompts.lock().await.clone()
    }

    async fn context_windows(&self) -> Vec<Vec<String>> {
        self.context_windows.lock().await.clone()
    }

    async fn was_interrupted(&self) -> bool {
        *self.interrupts.lock().await > 0
    }
}

#[async_trait]
impl ClaudeBridgeLike for ScriptedClaudeBridge {
    async fn send_user_text(&self, text: String) -> Result<()> {
        let mut prompts = self.prompts.lock().await;
        prompts.push(text);
        self.context_windows.lock().await.push(prompts.clone());
        let request_id = format!("tool-{}", prompts.len());
        drop(prompts);

        if self.auto_reply {
            let _ = self.events.send(ClaudeBridgeEvent::ToolCallRequested {
                request_id,
                tool_name: "bash".to_string(),
                input: json!({ "command": "printf done" }),
            });
        }

        Ok(())
    }

    async fn respond_tool_success(&self, _request_id: String, output: String) -> Result<()> {
        let _ = self.events.send(ClaudeBridgeEvent::SdkMessage {
            message: json!({
                "type": "stream_event",
                "event": {
                    "type": "content_block_delta",
                    "delta": { "type": "text_delta", "text": output }
                }
            }),
        });
        let _ = self.events.send(ClaudeBridgeEvent::SdkMessage {
            message: json!({
                "type": "result",
                "result": output,
                "is_error": false
            }),
        });
        Ok(())
    }

    async fn respond_tool_failure(&self, _request_id: String, message: String) -> Result<()> {
        let _ = self.events.send(ClaudeBridgeEvent::BridgeError { message });
        Ok(())
    }

    async fn interrupt(&self) -> Result<()> {
        *self.interrupts.lock().await += 1;
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<ClaudeBridgeEvent> {
        self.events.subscribe()
    }
}

fn summarize_code_event(event: &Event<CodeSchema>) -> String {
    match &event.payload {
        EventPayload::Interaction(InteractionEvent::InputCommitted { input, .. }) => {
            format!("input_committed {input:?}")
        }
        EventPayload::Interaction(InteractionEvent::InputInterrupted { cause, .. }) => {
            format!("input_interrupted {cause:?}")
        }
        EventPayload::Execution(ExecutionEvent::Control(control)) => format!("control {control:?}"),
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Started {
            directive,
            ..
        })) => format!("inference_started {:?}", directive.prompt),
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
            output,
            ..
        })) => format!("inference_output {output:?}"),
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Completed {
            ..
        })) => "inference_completed".to_string(),
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Cancelled {
            cause,
            ..
        })) => format!("inference_cancelled {cause:?}"),
        EventPayload::Presentation(_) => "presentation".to_string(),
        EventPayload::Error(error) => format!(
            "worker_error worker={} code={} message={:?}",
            error.worker_id.as_ref(),
            error.error.code,
            error.error.message
        ),
        payload => format!("{payload:?}"),
    }
}

fn owned_lines(lines: &[&str]) -> Vec<String> {
    lines.iter().map(|line| (*line).to_string()).collect()
}

fn owned_windows(windows: &[&[&str]]) -> Vec<Vec<String>> {
    windows.iter().map(|window| owned_lines(window)).collect()
}

fn build_committed_input(
    session: &SessionContext<CodeSchema>,
    input: impl Into<String>,
) -> EventPayload<CodeSchema> {
    EventPayload::Interaction(InteractionEvent::InputCommitted {
        session_id: session.session_id,
        thread_id: session.thread_id,
        stream_id: CodeSchema::next_input_stream_id(),
        revision_id: CodeSchema::next_revision_id(),
        turn_id: CodeSchema::next_turn_id(),
        input: input.into(),
    })
}

fn build_interrupt(session: &SessionContext<CodeSchema>) -> EventPayload<CodeSchema> {
    EventPayload::Interaction(InteractionEvent::InputInterrupted {
        session_id: session.session_id,
        thread_id: session.thread_id,
        cause: InterruptCause::ExplicitUserAction,
    })
}

async fn publish_session_event(
    scenario: &mut CodeScenario,
    session: &SessionContext<CodeSchema>,
    description: &str,
    visibility: EventVisibility,
    payload: EventPayload<CodeSchema>,
) -> Result<(), ScenarioFailure> {
    scenario
        .when(description)
        .publish(session_stream::<CodeSchema>(session), visibility, payload)
        .await
}

async fn expect_inference_started(
    scenario: &mut CodeScenario,
    description: &str,
    prompt: &str,
    session: &SessionContext<CodeSchema>,
) -> Result<(), ScenarioFailure> {
    scenario
        .then(description)
        .expect_eventually(description, Duration::from_millis(250), |event| {
            matches!(
                &event.payload,
                EventPayload::Execution(ExecutionEvent::Inference(
                    InferenceEvent::Started {
                        directive,
                        session_id,
                        thread_id,
                        ..
                    }
                )) if directive.prompt == prompt
                    && session_id == &session.session_id
                    && thread_id == &session.thread_id
            )
        })
        .await
}

async fn expect_inference_completed(
    scenario: &mut CodeScenario,
    description: &str,
) -> Result<(), ScenarioFailure> {
    scenario
        .then(description)
        .expect_eventually(description, Duration::from_millis(250), |event| {
            matches!(
                event.payload,
                EventPayload::Execution(ExecutionEvent::Inference(
                    InferenceEvent::Completed { .. }
                ))
            )
        })
        .await
}

async fn expect_cancel_requested(
    scenario: &mut CodeScenario,
    description: &str,
) -> Result<(), ScenarioFailure> {
    scenario
        .then(description)
        .expect_eventually(description, Duration::from_millis(250), |event| {
            matches!(
                &event.payload,
                EventPayload::Execution(ExecutionEvent::Control(
                    mango_core::agent::ControlEvent::CancelRequested { cause, .. }
                )) if matches!(cause, Cancellation::UserInterrupted)
            )
        })
        .await
}

async fn expect_inference_cancelled(
    scenario: &mut CodeScenario,
    description: &str,
) -> Result<(), ScenarioFailure> {
    scenario
        .then(description)
        .expect_eventually(description, Duration::from_millis(250), |event| {
            matches!(
                &event.payload,
                EventPayload::Execution(ExecutionEvent::Inference(
                    InferenceEvent::Cancelled { cause, .. }
                )) if matches!(cause, Cancellation::UserInterrupted)
            )
        })
        .await
}

#[tokio::test]
async fn completed_turns_do_not_force_a_runtime_restart_for_follow_up_input()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "code-agent supports a second turn after the first completes",
        CodeBus::new(128),
    )
    .with_recent_event_limit(10)
    .with_event_summary(summarize_code_event);
    let (session, bridge) =
        spawn_code_agent_workers(&mut scenario, ScriptedClaudeBridge::new(true));

    scenario
        .given("the code-agent worker set is running")
        .stays_healthy()
        .await?;

    publish_session_event(
        &mut scenario,
        &session,
        "the first prompt is committed",
        EventVisibility::Internal,
        build_committed_input(&session, "List the top-level files"),
    )
    .await?;

    expect_inference_completed(&mut scenario, "the first turn completes").await?;

    scenario
        .then("the worker set stays healthy after the first completion")
        .stays_healthy()
        .await?;

    publish_session_event(
        &mut scenario,
        &session,
        "a follow-up prompt is committed in the same session",
        EventVisibility::Internal,
        build_committed_input(&session, "Now read Cargo.toml"),
    )
    .await?;

    expect_inference_started(
        &mut scenario,
        "a second inference starts for the follow-up prompt",
        "Now read Cargo.toml",
        &session,
    )
    .await?;

    assert_eq!(
        bridge.prompts().await,
        owned_lines(&["List the top-level files", "Now read Cargo.toml"])
    );
    assert_eq!(
        bridge.context_windows().await,
        owned_windows(&[
            &["List the top-level files"],
            &["List the top-level files", "Now read Cargo.toml"],
        ])
    );

    Ok(())
}

#[tokio::test]
async fn user_interrupts_cancel_the_active_run_without_poisoning_the_session()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "code-agent accepts an interrupt while inference is active",
        CodeBus::new(128),
    )
    .with_recent_event_limit(10)
    .with_event_summary(summarize_code_event);
    let (session, bridge) =
        spawn_code_agent_workers(&mut scenario, ScriptedClaudeBridge::new(false));

    scenario
        .given("the code-agent worker set is running")
        .stays_healthy()
        .await?;

    publish_session_event(
        &mut scenario,
        &session,
        "a long-running prompt is committed",
        EventVisibility::Internal,
        build_committed_input(&session, "Inspect the repository"),
    )
    .await?;

    expect_inference_started(
        &mut scenario,
        "inference starts for the prompt",
        "Inspect the repository",
        &session,
    )
    .await?;

    publish_session_event(
        &mut scenario,
        &session,
        "the user interrupts the active run",
        EventVisibility::Both,
        build_interrupt(&session),
    )
    .await?;

    expect_cancel_requested(&mut scenario, "a user-driven cancel request is emitted").await?;
    expect_inference_cancelled(&mut scenario, "the active inference run is cancelled").await?;

    scenario
        .then("the session remains reusable after cancellation")
        .stays_healthy()
        .await?;

    publish_session_event(
        &mut scenario,
        &session,
        "the user submits a new prompt after cancelling",
        EventVisibility::Internal,
        build_committed_input(&session, "Retry with a narrower request"),
    )
    .await?;

    expect_inference_started(
        &mut scenario,
        "a new inference starts after the cancellation",
        "Retry with a narrower request",
        &session,
    )
    .await?;

    assert!(bridge.was_interrupted().await);
    assert_eq!(
        bridge.prompts().await,
        owned_lines(&["Inspect the repository", "Retry with a narrower request"])
    );
    assert_eq!(
        bridge.context_windows().await,
        owned_windows(&[
            &["Inspect the repository"],
            &["Inspect the repository", "Retry with a narrower request"],
        ])
    );

    Ok(())
}
