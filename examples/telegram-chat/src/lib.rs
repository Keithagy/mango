mod automation;
pub mod testing;

use std::{collections::HashSet, fmt::Write as _, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use example_support::{
    BoxFuture, DefaultAgentIds, EngineId, ExampleAppError, ExampleBridge, ExampleBus,
    ExampleRuntime, ExampleSubstrate, ExampleSurface, InMemoryEventBusError, InferenceRunId,
    ToolName, WorkerId, error_descriptor, new_session, next_event, publish, publish_worker_error,
    session_stream, session_subscription,
};
use mango_core::agent::{
    AgentRuntime, AgentSchema, AgentSchemaIds, BusWorker, Cancellation, Completion, ControlEvent,
    EventBus, EventPayload, ExecutionEvent, InferenceEvent, InteractionEvent, SessionContext,
    Subscription, Worker,
};
use mango_shim_claude_agent::{ClaudeAgentBridge, ClaudeAgentConfig, ClaudeBridgeEvent};
use mango_telegram::{
    DisplayTelegramTextMapper, TelegramClient, TelegramEgress, TelegramInbox, TelegramIngress,
    TelegramIngressMapper, TelegramInputTurn, TelegramSurface, TeloxideTelegramError,
};
use serde_json::Value;
use tokio::{
    sync::{Mutex, broadcast},
    task::JoinHandle,
};
use tracing::{error, warn};

pub use automation::{
    AutomationDispatchOutcome, AutomationTurnDispatcher, BundleAutomationDispatcher,
    NoopAutomationDispatcher, SharedAutomationDispatcher,
};

const DEFAULT_SYSTEM_PROMPT: &str = "You are the conversational backend for a Mango Telegram chat example. Reply directly, stay concise unless the user asks for detail, and do not assume any tool access.";
const NOT_MY_CUSTOMER: &str = "sorry, you're not my customer";
const BACKEND_UNAVAILABLE: &str = "sorry, I'm having trouble reaching my backend right now";
const AUTOMATION_UNAVAILABLE: &str =
    "sorry, I hit a problem while checking your expense automations";
const CLAUDE_ENGINE_ID: &str = "claude-agent-sdk";
const WHITELIST_ENGINE_ID: &str = "telegram-whitelist";

#[derive(Debug, Clone)]
pub struct ClaudeConversationConfig {
    pub cwd: PathBuf,
    pub claude_executable: String,
    pub model: Option<String>,
    pub system_prompt_append: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ChatInputKind {
    Message,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatInput {
    pub username: Option<String>,
    pub display_name: String,
    pub content: ChatInputContent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatInputContent {
    Text {
        text: String,
    },
    Photo {
        local_path: PathBuf,
        caption: Option<String>,
    },
}

impl ChatInput {
    #[must_use]
    pub fn baseline_prompt(&self) -> String {
        match &self.content {
            ChatInputContent::Text { text } => text.clone(),
            ChatInputContent::Photo {
                local_path,
                caption,
            } => {
                let mut prompt =
                    format!("The user sent a photo saved at {}.", local_path.display());
                if let Some(caption) = caption
                    && !caption.trim().is_empty()
                {
                    let _ = write!(prompt, " Caption: {caption}");
                }
                prompt.push_str(" Reply conversationally without assuming tool access.");
                prompt
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatDirective {
    ConversationTurn { prompt: String },
    AutomationHandled { response: String },
    RejectedByWhitelist { response: String },
}

impl ChatDirective {
    #[must_use]
    fn is_conversation_turn(&self) -> bool {
        matches!(self, Self::ConversationTurn { .. })
    }
}

#[derive(Debug, Clone)]
pub struct ChatSchema;

type ChatSubscription = Subscription<ChatSchema>;
type ChatSession = SessionContext<ChatSchema>;

impl AgentSchema for ChatSchema {
    type Ids = DefaultAgentIds;
    type Surface = TelegramSurface;
    type InputKind = ChatInputKind;
    type Input = ChatInput;
    type InterruptDetail = ();
    type Directive = ChatDirective;
    type Output = String;
    type ToolData = ();
    type Status = String;
    type CancellationDetail = ();
    type CompletionDetail = ();
    type EngineId = EngineId;
    type ToolName = ToolName;
}

#[derive(Debug, thiserror::Error)]
pub enum ChatAppError {
    #[error("event bus closed")]
    BusClosed,
    #[error("event bus lagged by {0} events")]
    BusLagged(u64),
    #[error("task join failed: {0}")]
    TaskJoin(String),
    #[error("telegram error: {0}")]
    Telegram(String),
}

impl From<InMemoryEventBusError> for ChatAppError {
    fn from(value: InMemoryEventBusError) -> Self {
        match value {
            InMemoryEventBusError::Closed => Self::BusClosed,
            InMemoryEventBusError::Lagged(skipped) => Self::BusLagged(skipped),
        }
    }
}

impl From<TeloxideTelegramError> for ChatAppError {
    fn from(value: TeloxideTelegramError) -> Self {
        Self::Telegram(value.to_string())
    }
}

impl From<mango_telegram::TestTelegramError> for ChatAppError {
    fn from(value: mango_telegram::TestTelegramError) -> Self {
        Self::Telegram(value.to_string())
    }
}

impl ExampleAppError for ChatAppError {
    fn task_join(message: String) -> Self {
        Self::TaskJoin(message)
    }
}

pub type ChatBus = ExampleBus<ChatSchema, ChatAppError>;
pub type ChatIngress = TelegramIngress<ChatSchema, ChatTelegramInputMapper>;
pub type ChatEgress<C> = TelegramEgress<ChatSchema, C, DisplayTelegramTextMapper>;
pub type ChatRuntime<C> = ExampleRuntime<
    ChatSchema,
    ChatAppError,
    ChatBus,
    ChatIngress,
    ChatEgress<C>,
    ConversationControl,
    ClaudeConversationInference,
    SessionSentinel,
    SessionSentinel,
>;

#[derive(Debug, Default)]
struct ControlState {
    active_run: Option<InferenceRunId>,
}

#[must_use]
#[derive(Debug, Clone)]
pub struct UsernameWhitelist {
    usernames: Arc<HashSet<String>>,
}

impl UsernameWhitelist {
    pub fn from_usernames<I, S>(usernames: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            usernames: Arc::new(
                usernames
                    .into_iter()
                    .map(|username| normalize_username(username.as_ref()))
                    .filter(|username| !username.is_empty())
                    .collect(),
            ),
        }
    }

    #[must_use]
    pub fn contains(&self, username: Option<&str>) -> bool {
        username
            .map(normalize_username)
            .is_some_and(|username| self.usernames.contains(&username))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.usernames.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.usernames.len()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ChatTelegramInputMapper;

impl TelegramIngressMapper<ChatSchema> for ChatTelegramInputMapper {
    fn map_message(
        &self,
        message: &mango_telegram::TelegramInboundMessage,
    ) -> Option<TelegramInputTurn<ChatSchema>> {
        let content = if let Some(photo) = &message.photo {
            ChatInputContent::Photo {
                local_path: photo.local_path.clone(),
                caption: message.caption.clone(),
            }
        } else {
            ChatInputContent::Text {
                text: message.text.clone(),
            }
        };
        Some(TelegramInputTurn {
            kind: ChatInputKind::Message,
            input: ChatInput {
                username: message.username.clone(),
                display_name: message.display_name.clone(),
                content,
            },
        })
    }
}

#[derive(Clone)]
pub struct ConversationControl {
    worker_id: WorkerId,
    session: ChatSession,
    allowed_usernames: UsernameWhitelist,
    automation_dispatcher: SharedAutomationDispatcher,
    state: Arc<Mutex<ControlState>>,
}

impl ConversationControl {
    #[must_use]
    pub fn new(session: ChatSession, allowed_usernames: UsernameWhitelist) -> Self {
        Self::with_automation(
            session,
            allowed_usernames,
            Arc::new(NoopAutomationDispatcher),
        )
    }

    #[must_use]
    pub fn with_automation(
        session: ChatSession,
        allowed_usernames: UsernameWhitelist,
        automation_dispatcher: SharedAutomationDispatcher,
    ) -> Self {
        Self {
            worker_id: WorkerId::from("telegram-chat-control"),
            session,
            allowed_usernames,
            automation_dispatcher,
            state: Arc::new(Mutex::new(ControlState::default())),
        }
    }
}

impl Worker for ConversationControl {
    type WorkerId = WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<ChatSchema>(&self.session)
    }
}

impl BusWorker<ChatSchema, ChatBus> for ConversationControl {
    type Error = ChatAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: ChatBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;

            while let Some(event) = next_event(&mut events).await? {
                match event.payload {
                    EventPayload::Interaction(InteractionEvent::InputCommitted {
                        turn_id,
                        input,
                        ..
                    }) => {
                        let directive = directive_for_input(
                            &self.allowed_usernames,
                            self.automation_dispatcher.as_ref(),
                            &self.session.surface,
                            input,
                        )
                        .await;
                        let supersedes = if directive.is_conversation_turn() {
                            self.state.lock().await.active_run
                        } else {
                            None
                        };

                        if let Some(run_id) = supersedes {
                            publish::<ChatSchema, _>(
                                bus,
                                session_stream::<ChatSchema>(&self.session),
                                mango_core::agent::EventVisibility::Internal,
                                EventPayload::Execution(ExecutionEvent::Control(
                                    ControlEvent::CancelRequested {
                                        session_id: self.session.session_id,
                                        run_id: Some(run_id),
                                        cause: Cancellation::SupersededByNewInput,
                                    },
                                )),
                            )
                            .await?;
                        }

                        publish::<ChatSchema, _>(
                            bus,
                            session_stream::<ChatSchema>(&self.session),
                            mango_core::agent::EventVisibility::Internal,
                            EventPayload::Execution(ExecutionEvent::Control(
                                ControlEvent::Requested {
                                    request_id: ChatSchema::next_inference_request_id(),
                                    session_id: self.session.session_id,
                                    thread_id: self.session.thread_id,
                                    turn_id: Some(turn_id),
                                    directive,
                                    supersedes,
                                },
                            )),
                        )
                        .await?;
                    }
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Started {
                            run_id, directive, ..
                        },
                    )) => {
                        if directive.is_conversation_turn() {
                            self.state.lock().await.active_run = Some(run_id);
                        }
                    }
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Completed { run_id, .. }
                        | InferenceEvent::Cancelled { run_id, .. }
                        | InferenceEvent::Failed { run_id, .. },
                    )) => {
                        let mut state = self.state.lock().await;
                        if state.active_run.as_ref() == Some(&run_id) {
                            state.active_run = None;
                        }
                    }
                    EventPayload::Interaction(InteractionEvent::SessionClosed {
                        session_id,
                        ..
                    }) if session_id == self.session.session_id => {
                        break;
                    }
                    _ => {}
                }
            }

            Ok(())
        })
    }
}

#[derive(Debug, Default)]
struct InferenceState {
    current_run_id: Option<InferenceRunId>,
    next_sequence: u64,
    last_snapshot: String,
}

#[derive(Debug, Clone)]
pub struct ClaudeConversationInference {
    worker_id: WorkerId,
    session: ChatSession,
    backend: ClaudeConversationConfig,
    state: Arc<Mutex<InferenceState>>,
}

impl ClaudeConversationInference {
    #[must_use]
    pub fn new(session: ChatSession, backend: ClaudeConversationConfig) -> Self {
        Self {
            worker_id: WorkerId::from("telegram-chat-claude-inference"),
            session,
            backend,
            state: Arc::new(Mutex::new(InferenceState::default())),
        }
    }
}

impl Worker for ClaudeConversationInference {
    type WorkerId = WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<ChatSchema>(&self.session)
    }
}

impl BusWorker<ChatSchema, ChatBus> for ClaudeConversationInference {
    type Error = ChatAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: ChatBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;
            let mut bridge = None;
            let mut bridge_events = None;

            loop {
                if let Some(receiver) = bridge_events.as_mut() {
                    tokio::select! {
                        maybe_event = next_event(&mut events) => {
                            let Some(event) = maybe_event? else {
                                break;
                            };
                            if handle_control_event(
                                &self,
                                bus,
                                &mut bridge,
                                &mut bridge_events,
                                event.payload,
                            ).await? {
                                break;
                            }
                        }
                        bridge_event = recv_bridge_event(receiver) => {
                            if handle_bridge_event(&self, bus, bridge_event).await? {
                                bridge = None;
                                bridge_events = None;
                            }
                        }
                    }
                } else {
                    let Some(event) = next_event(&mut events).await? else {
                        break;
                    };
                    if handle_control_event(
                        &self,
                        bus,
                        &mut bridge,
                        &mut bridge_events,
                        event.payload,
                    )
                    .await?
                    {
                        break;
                    }
                }
            }

            if let Some(bridge) = bridge {
                let _ = bridge.close().await;
            }

            Ok(())
        })
    }
}

#[derive(Debug, Clone)]
pub struct SessionSentinel {
    worker_id: WorkerId,
    session: ChatSession,
}

impl SessionSentinel {
    #[must_use]
    pub fn new(worker_id: impl Into<WorkerId>, session: ChatSession) -> Self {
        Self {
            worker_id: worker_id.into(),
            session,
        }
    }
}

impl Worker for SessionSentinel {
    type WorkerId = WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<ChatSchema>(&self.session)
    }
}

impl BusWorker<ChatSchema, ChatBus> for SessionSentinel {
    type Error = ChatAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: ChatBus) -> Self::Run {
        Box::pin(async move {
            let mut events = bus.subscribe(self.subscription())?;
            while let Some(event) = next_event(&mut events).await? {
                if let EventPayload::Interaction(InteractionEvent::SessionClosed {
                    session_id, ..
                }) = event.payload
                    && session_id == self.session.session_id
                {
                    break;
                }
            }
            Ok(())
        })
    }
}

#[must_use]
pub fn chat_session(surface: TelegramSurface) -> ChatSession {
    new_session::<ChatSchema>(surface)
}

/// Spawn a Telegram chat session runtime backed by a lazy conversational
/// Claude bridge.
#[must_use]
pub fn spawn_chat_runtime<C>(
    client: C,
    surface: TelegramSurface,
    inbox: TelegramInbox,
    bus_capacity: usize,
    allowed_usernames: UsernameWhitelist,
    backend: &ClaudeConversationConfig,
) -> JoinHandle<()>
where
    C: TelegramClient + Clone + Send + Sync + 'static,
    ChatAppError: From<C::Error>,
{
    spawn_chat_runtime_with_automation(
        client,
        surface,
        inbox,
        bus_capacity,
        allowed_usernames,
        Arc::new(NoopAutomationDispatcher),
        backend,
    )
}

#[must_use]
pub fn spawn_chat_runtime_with_automation<C>(
    client: C,
    surface: TelegramSurface,
    inbox: TelegramInbox,
    bus_capacity: usize,
    allowed_usernames: UsernameWhitelist,
    automation_dispatcher: SharedAutomationDispatcher,
    backend: &ClaudeConversationConfig,
) -> JoinHandle<()>
where
    C: TelegramClient + Clone + Send + Sync + 'static,
    ChatAppError: From<C::Error>,
{
    let session = chat_session(surface);
    let runtime = ChatRuntime::new(
        ExampleSubstrate::new(
            ChatBus::new(bus_capacity),
            ConversationControl::with_automation(
                session.clone(),
                allowed_usernames,
                automation_dispatcher,
            ),
        ),
        ExampleSurface::new(
            TelegramIngress::new(
                WorkerId::from("telegram-chat-ingress"),
                inbox,
                ChatTelegramInputMapper,
            ),
            TelegramEgress::new(
                WorkerId::from("telegram-chat-egress"),
                client,
                DisplayTelegramTextMapper,
            ),
            SessionSentinel::new("telegram-chat-presentation", session.clone()),
        ),
        ExampleBridge::new(
            ClaudeConversationInference::new(session.clone(), backend.clone()),
            SessionSentinel::new("telegram-chat-tools", session.clone()),
        ),
    );

    tokio::spawn(async move {
        if let Err(error) = runtime.startup(session.clone()).await {
            tracing::error!("telegram-chat runtime startup failed: {error}");
            return;
        }
        if let Err(error) = runtime.run_session(session).await {
            tracing::error!("telegram-chat runtime failed: {error}");
        }
    })
}

fn spawn_claude_bridge(
    backend: &ClaudeConversationConfig,
    session: &ChatSession,
) -> Result<ClaudeAgentBridge> {
    let mut config = ClaudeAgentConfig::new(
        backend.cwd.clone(),
        session.session_id.to_string(),
        backend.claude_executable.clone(),
    )
    .with_tools(Vec::<String>::new())
    .with_conversational_turns()
    .without_tool_use_hooks()
    .with_system_prompt_append(build_system_prompt(backend.system_prompt_append.as_deref()));

    if let Some(model) = &backend.model {
        config = config.with_model(model.clone());
    }

    ClaudeAgentBridge::spawn(config)
}

fn build_system_prompt(extra: Option<&str>) -> String {
    extra
        .filter(|prompt| !prompt.trim().is_empty())
        .map_or_else(
            || DEFAULT_SYSTEM_PROMPT.to_string(),
            |prompt| format!("{DEFAULT_SYSTEM_PROMPT}\n\n{prompt}"),
        )
}

fn format_bridge_exit(label: &str, code: Option<i32>) -> String {
    format!(
        "{label} exited{}",
        code.map(|value| format!(" with code {value}"))
            .unwrap_or_default()
    )
}

fn normalize_username(username: &str) -> String {
    username.trim().trim_start_matches('@').to_ascii_lowercase()
}

async fn directive_for_input(
    allowed_usernames: &UsernameWhitelist,
    automation_dispatcher: &dyn AutomationTurnDispatcher,
    surface: &TelegramSurface,
    input: ChatInput,
) -> ChatDirective {
    if !allowed_usernames.contains(input.username.as_deref()) {
        return ChatDirective::RejectedByWhitelist {
            response: NOT_MY_CUSTOMER.to_string(),
        };
    }

    match automation_dispatcher.dispatch(surface, &input).await {
        Ok(outcome) if outcome.handled => ChatDirective::AutomationHandled {
            response: outcome.response.unwrap_or_default(),
        },
        Ok(_) => ChatDirective::ConversationTurn {
            prompt: input.baseline_prompt(),
        },
        Err(error) => {
            error!("automation dispatch failed: {error}");
            ChatDirective::AutomationHandled {
                response: AUTOMATION_UNAVAILABLE.to_string(),
            }
        }
    }
}

async fn recv_bridge_event(
    receiver: &mut broadcast::Receiver<ClaudeBridgeEvent>,
) -> std::result::Result<ClaudeBridgeEvent, broadcast::error::RecvError> {
    receiver.recv().await
}

fn ensure_bridge_started(
    worker: &ClaudeConversationInference,
    bridge: &mut Option<ClaudeAgentBridge>,
    bridge_events: &mut Option<broadcast::Receiver<ClaudeBridgeEvent>>,
) -> Result<()> {
    if bridge.is_some() {
        return Ok(());
    }

    let spawned_bridge = spawn_claude_bridge(&worker.backend, &worker.session)
        .context("failed to start conversational Claude backend")?;
    let receiver = spawned_bridge.subscribe();
    *bridge = Some(spawned_bridge);
    *bridge_events = Some(receiver);
    Ok(())
}

#[derive(Debug, Clone)]
struct RequestedTurn {
    request: <ChatSchema as AgentSchemaIds>::InferenceRequestId,
    session: <ChatSchema as AgentSchemaIds>::SessionId,
    thread: <ChatSchema as AgentSchemaIds>::ThreadId,
}

async fn publish_inference_started(
    bus: &ChatBus,
    session: &ChatSession,
    run_id: InferenceRunId,
    request: RequestedTurn,
    directive: ChatDirective,
    engine: &'static str,
) -> Result<(), ChatAppError> {
    publish::<ChatSchema, _>(
        bus,
        session_stream::<ChatSchema>(session),
        mango_core::agent::EventVisibility::Internal,
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Started {
            run_id,
            request_id: request.request,
            session_id: request.session,
            thread_id: request.thread,
            directive,
            engine: EngineId::from(engine),
        })),
    )
    .await
}

async fn publish_backend_unavailable(
    bus: &ChatBus,
    session: &ChatSession,
    state: &Arc<Mutex<InferenceState>>,
) -> Result<(), ChatAppError> {
    publish_active_run_failed(
        bus,
        session,
        state,
        error_descriptor("backend_unavailable", BACKEND_UNAVAILABLE.to_string()),
    )
    .await
}

async fn handle_authorized_turn(
    worker: &ClaudeConversationInference,
    bus: &ChatBus,
    bridge: &mut Option<ClaudeAgentBridge>,
    bridge_events: &mut Option<broadcast::Receiver<ClaudeBridgeEvent>>,
    request: RequestedTurn,
    prompt: String,
) -> Result<(), ChatAppError> {
    let run_id = ChatSchema::next_inference_run_id();
    {
        let mut state = worker.state.lock().await;
        state.current_run_id = Some(run_id);
        state.next_sequence = 0;
        state.last_snapshot.clear();
    }

    publish_inference_started(
        bus,
        &worker.session,
        run_id,
        request,
        ChatDirective::ConversationTurn {
            prompt: prompt.clone(),
        },
        CLAUDE_ENGINE_ID,
    )
    .await?;

    if let Err(error) = ensure_bridge_started(worker, bridge, bridge_events) {
        error!("failed to start Claude bridge: {error:#}");
        return publish_backend_unavailable(bus, &worker.session, &worker.state).await;
    }

    if let Some(active_bridge) = bridge.as_ref()
        && let Err(error) = active_bridge.send_user_text(prompt).await
    {
        error!("failed to send prompt to Claude bridge: {error:#}");
        return publish_backend_unavailable(bus, &worker.session, &worker.state).await;
    }

    Ok(())
}

async fn handle_immediate_response(
    bus: &ChatBus,
    session: &ChatSession,
    request: RequestedTurn,
    directive: ChatDirective,
    response: String,
    engine: &'static str,
) -> Result<(), ChatAppError> {
    let run_id = ChatSchema::next_inference_run_id();

    publish_inference_started(bus, session, run_id, request, directive, engine).await?;
    publish::<ChatSchema, _>(
        bus,
        session_stream::<ChatSchema>(session),
        mango_core::agent::EventVisibility::Both,
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
            run_id,
            sequence: 0,
            output: response,
        })),
    )
    .await?;
    publish::<ChatSchema, _>(
        bus,
        session_stream::<ChatSchema>(session),
        mango_core::agent::EventVisibility::Internal,
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Completed {
            run_id,
            result: Completion::Completed,
        })),
    )
    .await
}

async fn handle_cancel_requested(
    worker: &ClaudeConversationInference,
    bus: &ChatBus,
    bridge: Option<&ClaudeAgentBridge>,
    run_id: Option<InferenceRunId>,
    cause: Cancellation<ChatSchema>,
) -> Result<(), ChatAppError> {
    let active_run = worker.state.lock().await.current_run_id;
    if run_id.is_none() || active_run == run_id {
        if let Some(active_bridge) = bridge
            && let Err(error) = active_bridge.interrupt().await
        {
            error!("failed to interrupt Claude bridge: {error:#}");
        }

        if let Some(active_run) = active_run {
            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(&worker.session),
                mango_core::agent::EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Cancelled {
                    run_id: active_run,
                    cause,
                })),
            )
            .await?;
            reset_inference_state(&worker.state).await;
        }
    }

    Ok(())
}

async fn handle_control_event(
    worker: &ClaudeConversationInference,
    bus: &ChatBus,
    bridge: &mut Option<ClaudeAgentBridge>,
    bridge_events: &mut Option<broadcast::Receiver<ClaudeBridgeEvent>>,
    payload: EventPayload<ChatSchema>,
) -> Result<bool, ChatAppError> {
    match payload {
        EventPayload::Execution(ExecutionEvent::Control(ControlEvent::Requested {
            request_id,
            session_id,
            thread_id,
            directive,
            ..
        })) if session_id == worker.session.session_id => {
            let request = RequestedTurn {
                request: request_id,
                session: session_id,
                thread: thread_id,
            };

            match directive {
                ChatDirective::ConversationTurn { prompt } => {
                    handle_authorized_turn(worker, bus, bridge, bridge_events, request, prompt)
                        .await?;
                }
                ChatDirective::AutomationHandled { response } => {
                    handle_immediate_response(
                        bus,
                        &worker.session,
                        request,
                        ChatDirective::AutomationHandled {
                            response: response.clone(),
                        },
                        response,
                        "automation-bundle",
                    )
                    .await?;
                }
                ChatDirective::RejectedByWhitelist { response } => {
                    handle_immediate_response(
                        bus,
                        &worker.session,
                        request,
                        ChatDirective::RejectedByWhitelist {
                            response: response.clone(),
                        },
                        response,
                        WHITELIST_ENGINE_ID,
                    )
                    .await?;
                }
            }
        }
        EventPayload::Execution(ExecutionEvent::Control(ControlEvent::CancelRequested {
            session_id,
            run_id,
            cause,
        })) if session_id == worker.session.session_id => {
            handle_cancel_requested(worker, bus, bridge.as_ref(), run_id, cause).await?;
        }
        EventPayload::Interaction(InteractionEvent::SessionClosed { session_id, .. })
            if session_id == worker.session.session_id =>
        {
            return Ok(true);
        }
        _ => {}
    }

    Ok(false)
}

async fn handle_bridge_event(
    worker: &ClaudeConversationInference,
    bus: &ChatBus,
    event: std::result::Result<ClaudeBridgeEvent, broadcast::error::RecvError>,
) -> Result<bool, ChatAppError> {
    match event {
        Ok(ClaudeBridgeEvent::Ready { .. } | ClaudeBridgeEvent::ToolCallRequested { .. }) => {}
        Ok(ClaudeBridgeEvent::SdkMessage { message }) => {
            handle_claude_sdk_message(
                bus,
                &worker.worker_id,
                &worker.session,
                &worker.state,
                message,
            )
            .await?;
        }
        Ok(ClaudeBridgeEvent::BridgeError { message }) => {
            publish_worker_error::<ChatSchema, _>(
                bus,
                &worker.worker_id,
                &worker.session,
                "bridge_error",
                message.clone(),
            )
            .await?;
            publish_active_run_failed(
                bus,
                &worker.session,
                &worker.state,
                error_descriptor("bridge_error", message),
            )
            .await?;
        }
        Ok(ClaudeBridgeEvent::Stderr { line }) => {
            warn!("claude bridge stderr: {line}");
        }
        Ok(ClaudeBridgeEvent::Exited { code }) => {
            let message = format_bridge_exit("claude bridge", code);
            publish_worker_error::<ChatSchema, _>(
                bus,
                &worker.worker_id,
                &worker.session,
                "bridge_error",
                message.clone(),
            )
            .await?;
            publish_active_run_failed(
                bus,
                &worker.session,
                &worker.state,
                error_descriptor("bridge_exited", message),
            )
            .await?;
            return Ok(true);
        }
        Err(broadcast::error::RecvError::Lagged(skipped)) => {
            warn!("claude bridge events lagged by {skipped}");
        }
        Err(broadcast::error::RecvError::Closed) => return Ok(true),
    }

    Ok(false)
}

async fn handle_claude_sdk_message(
    bus: &ChatBus,
    worker_id: &WorkerId,
    session: &ChatSession,
    state: &Arc<Mutex<InferenceState>>,
    message: Value,
) -> Result<(), ChatAppError> {
    match message.get("type").and_then(Value::as_str) {
        Some("stream_event") => handle_claude_stream_event(bus, session, state, &message).await,
        Some("assistant") => handle_claude_assistant_snapshot(bus, session, state, &message).await,
        Some("result") => {
            handle_claude_result_event(bus, worker_id, session, state, &message).await
        }
        _ => Ok(()),
    }
}

async fn handle_claude_stream_event(
    bus: &ChatBus,
    session: &ChatSession,
    state: &Arc<Mutex<InferenceState>>,
    message: &Value,
) -> Result<(), ChatAppError> {
    let mut state = state.lock().await;
    let Some(run_id) = state.current_run_id else {
        return Ok(());
    };

    if let Some(delta) = extract_stream_text_delta(message) {
        state.last_snapshot.push_str(&delta);
        publish::<ChatSchema, _>(
            bus,
            session_stream::<ChatSchema>(session),
            mango_core::agent::EventVisibility::Both,
            EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
                run_id,
                sequence: state.next_sequence,
                output: delta,
            })),
        )
        .await?;
        state.next_sequence += 1;
    }

    Ok(())
}

async fn handle_claude_assistant_snapshot(
    bus: &ChatBus,
    session: &ChatSession,
    state: &Arc<Mutex<InferenceState>>,
    message: &Value,
) -> Result<(), ChatAppError> {
    let mut state = state.lock().await;
    let Some(run_id) = state.current_run_id else {
        return Ok(());
    };

    if let Some(snapshot) = extract_text_snapshot(message)
        && let Some(delta) = incremental_suffix(&state.last_snapshot, &snapshot)
    {
        publish::<ChatSchema, _>(
            bus,
            session_stream::<ChatSchema>(session),
            mango_core::agent::EventVisibility::Both,
            EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
                run_id,
                sequence: state.next_sequence,
                output: delta,
            })),
        )
        .await?;
        state.next_sequence += 1;
        state.last_snapshot = snapshot;
    }

    Ok(())
}

async fn handle_claude_result_event(
    bus: &ChatBus,
    worker_id: &WorkerId,
    session: &ChatSession,
    state: &Arc<Mutex<InferenceState>>,
    message: &Value,
) -> Result<(), ChatAppError> {
    let mut state = state.lock().await;
    let result_text = message
        .get("result")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_default();

    if let Some(run_id) = state.current_run_id {
        if let Some(delta) = incremental_suffix(&state.last_snapshot, &result_text) {
            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(session),
                mango_core::agent::EventVisibility::Both,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
                    run_id,
                    sequence: state.next_sequence,
                    output: delta,
                })),
            )
            .await?;
            state.next_sequence += 1;
        }

        if message
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let descriptor = error_descriptor(
                message
                    .get("subtype")
                    .and_then(Value::as_str)
                    .unwrap_or("claude_result_error"),
                result_text.clone(),
            );

            publish_worker_error::<ChatSchema, _>(
                bus,
                worker_id,
                session,
                "bridge_error",
                result_text.clone(),
            )
            .await?;

            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(session),
                mango_core::agent::EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Failed {
                    run_id,
                    error: descriptor,
                })),
            )
            .await?;
        } else {
            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(session),
                mango_core::agent::EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Completed {
                    run_id,
                    result: Completion::Completed,
                })),
            )
            .await?;
        }
    }

    state.current_run_id = None;
    state.next_sequence = 0;
    state.last_snapshot.clear();
    Ok(())
}

async fn publish_active_run_failed(
    bus: &ChatBus,
    session: &ChatSession,
    state: &Arc<Mutex<InferenceState>>,
    error: mango_core::agent::ErrorDescriptor,
) -> Result<(), ChatAppError> {
    let run_id = state.lock().await.current_run_id;
    if let Some(run_id) = run_id {
        publish::<ChatSchema, _>(
            bus,
            session_stream::<ChatSchema>(session),
            mango_core::agent::EventVisibility::Internal,
            EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Failed {
                run_id,
                error,
            })),
        )
        .await?;
        reset_inference_state(state).await;
    }

    Ok(())
}

async fn reset_inference_state(state: &Arc<Mutex<InferenceState>>) {
    let mut state = state.lock().await;
    state.current_run_id = None;
    state.next_sequence = 0;
    state.last_snapshot.clear();
}

fn extract_stream_text_delta(message: &Value) -> Option<String> {
    let event = message.get("event")?;
    if event.get("type").and_then(Value::as_str) != Some("content_block_delta") {
        return None;
    }

    let delta = event.get("delta")?;
    if delta.get("type").and_then(Value::as_str) != Some("text_delta") {
        return None;
    }

    delta.get("text").and_then(Value::as_str).map(str::to_owned)
}

fn extract_text_snapshot(message: &Value) -> Option<String> {
    if let Some(content) = message
        .get("message")
        .and_then(|value| value.get("content"))
        .or_else(|| message.get("content"))
    {
        let text = flatten_content(content);
        if !text.is_empty() {
            return Some(text);
        }
    }

    None
}

fn flatten_content(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => items.iter().map(flatten_content_item).collect(),
        _ => String::new(),
    }
}

fn flatten_content_item(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Object(object) => object
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        _ => String::new(),
    }
}

fn incremental_suffix(previous: &str, next: &str) -> Option<String> {
    if next.is_empty() || previous == next {
        return None;
    }

    if let Some(suffix) = next.strip_prefix(previous) {
        if suffix.is_empty() {
            None
        } else {
            Some(suffix.to_string())
        }
    } else {
        Some(next.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChatDirective, ChatInput, ChatInputContent, NOT_MY_CUSTOMER, NoopAutomationDispatcher,
        UsernameWhitelist, directive_for_input, normalize_username,
    };
    use mango_telegram::{TelegramChatId, TelegramSurface};
    use std::sync::Arc;

    #[test]
    fn username_whitelist_normalizes_configured_usernames() {
        let whitelist = UsernameWhitelist::from_usernames([" @KeithIsms ", "keithisms", ""]);

        assert_eq!(whitelist.len(), 1);
        assert!(whitelist.contains(Some("keithisms")));
        assert!(whitelist.contains(Some("@KeithIsms")));
    }

    #[tokio::test]
    async fn directive_for_input_allows_whitelisted_username() {
        let whitelist = UsernameWhitelist::from_usernames(["keithisms"]);
        let surface = TelegramSurface {
            chat_id: TelegramChatId(7),
            thread_id: None,
            username: Some("keithisms".to_string()),
            display_name: "Keith".to_string(),
        };
        let directive = directive_for_input(
            &whitelist,
            Arc::new(NoopAutomationDispatcher).as_ref(),
            &surface,
            ChatInput {
                username: Some("@KeithIsms".to_string()),
                display_name: "Keith".to_string(),
                content: ChatInputContent::Text {
                    text: "hello".to_string(),
                },
            },
        )
        .await;

        assert_eq!(
            directive,
            ChatDirective::ConversationTurn {
                prompt: "hello".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn directive_for_input_rejects_unknown_username() {
        let whitelist = UsernameWhitelist::from_usernames(["keithisms"]);
        let surface = TelegramSurface {
            chat_id: TelegramChatId(7),
            thread_id: None,
            username: Some("keithisms".to_string()),
            display_name: "Keith".to_string(),
        };
        let directive = directive_for_input(
            &whitelist,
            Arc::new(NoopAutomationDispatcher).as_ref(),
            &surface,
            ChatInput {
                username: Some("someone_else".to_string()),
                display_name: "Someone Else".to_string(),
                content: ChatInputContent::Text {
                    text: "hello".to_string(),
                },
            },
        )
        .await;

        assert_eq!(
            directive,
            ChatDirective::RejectedByWhitelist {
                response: NOT_MY_CUSTOMER.to_string(),
            }
        );
    }

    #[test]
    fn normalize_username_trims_prefix_and_case() {
        assert_eq!(normalize_username(" @KeithIsms "), "keithisms");
    }
}
