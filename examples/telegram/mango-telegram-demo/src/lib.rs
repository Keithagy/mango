use std::{collections::HashSet, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use mango_core::agent::{
    AgentRuntime, AgentSchema, AgentSchemaIds, BusWorker, Cancellation, Completion, ControlEvent,
    EventBus, EventPayload, ExecutionEvent, InferenceEvent, InteractionEvent, SessionContext,
    Subscription, Worker,
};
use mango_example_support::{
    BoxFuture, DefaultAgentIds, EngineId, ExampleAppError, ExampleBridge, ExampleBus,
    ExampleRuntime, ExampleSubstrate, ExampleSurface, InMemoryEventBusError, InferenceRunId,
    ToolName, WorkerId, error_descriptor, new_session, next_event, publish, publish_worker_error,
    session_stream, session_subscription,
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

const DEFAULT_SYSTEM_PROMPT: &str = "You are the conversational backend for a Mango Telegram demo. Reply directly, stay concise unless the user asks for detail, and do not assume any tool access.";
const NOT_MY_CUSTOMER: &str = "sorry, you're not my customer";
const BACKEND_UNAVAILABLE: &str = "sorry, I'm having trouble reaching my backend right now";
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
pub enum DemoInputKind {
    Message,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemoInput {
    pub text: String,
    pub username: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DemoDirective {
    ConversationTurn { prompt: String },
    RejectedByWhitelist { response: String },
}

impl DemoDirective {
    #[must_use]
    fn is_conversation_turn(&self) -> bool {
        matches!(self, Self::ConversationTurn { .. })
    }
}

#[derive(Debug, Clone)]
pub struct DemoSchema;

type DemoSubscription = Subscription<DemoSchema>;
type DemoSession = SessionContext<DemoSchema>;

impl AgentSchema for DemoSchema {
    type Ids = DefaultAgentIds;
    type Surface = TelegramSurface;
    type InputKind = DemoInputKind;
    type Input = DemoInput;
    type InterruptDetail = ();
    type Directive = DemoDirective;
    type Output = String;
    type ToolData = ();
    type Status = String;
    type CancellationDetail = ();
    type CompletionDetail = ();
    type EngineId = EngineId;
    type ToolName = ToolName;
}

#[derive(Debug, thiserror::Error)]
pub enum DemoAppError {
    #[error("event bus closed")]
    BusClosed,
    #[error("event bus lagged by {0} events")]
    BusLagged(u64),
    #[error("task join failed: {0}")]
    TaskJoin(String),
    #[error("telegram error: {0}")]
    Telegram(String),
}

impl From<InMemoryEventBusError> for DemoAppError {
    fn from(value: InMemoryEventBusError) -> Self {
        match value {
            InMemoryEventBusError::Closed => Self::BusClosed,
            InMemoryEventBusError::Lagged(skipped) => Self::BusLagged(skipped),
        }
    }
}

impl From<TeloxideTelegramError> for DemoAppError {
    fn from(value: TeloxideTelegramError) -> Self {
        Self::Telegram(value.to_string())
    }
}

impl ExampleAppError for DemoAppError {
    fn task_join(message: String) -> Self {
        Self::TaskJoin(message)
    }
}

pub type DemoBus = ExampleBus<DemoSchema, DemoAppError>;
pub type DemoIngress = TelegramIngress<DemoSchema, DemoTelegramInputMapper>;
pub type DemoEgress<C> = TelegramEgress<DemoSchema, C, DisplayTelegramTextMapper>;
pub type DemoRuntime<C> = ExampleRuntime<
    DemoSchema,
    DemoAppError,
    DemoBus,
    DemoIngress,
    DemoEgress<C>,
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
pub struct DemoTelegramInputMapper;

impl TelegramIngressMapper<DemoSchema> for DemoTelegramInputMapper {
    fn map_message(
        &self,
        message: &mango_telegram::TelegramInboundMessage,
    ) -> Option<TelegramInputTurn<DemoSchema>> {
        Some(TelegramInputTurn {
            kind: DemoInputKind::Message,
            input: DemoInput {
                text: message.text.clone(),
                username: message.username.clone(),
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct ConversationControl {
    worker_id: WorkerId,
    session: DemoSession,
    allowed_usernames: UsernameWhitelist,
    state: Arc<Mutex<ControlState>>,
}

impl ConversationControl {
    #[must_use]
    pub fn new(session: DemoSession, allowed_usernames: UsernameWhitelist) -> Self {
        Self {
            worker_id: WorkerId::from("telegram-demo-control"),
            session,
            allowed_usernames,
            state: Arc::new(Mutex::new(ControlState::default())),
        }
    }
}

impl Worker for ConversationControl {
    type WorkerId = WorkerId;
    type Subscription = DemoSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<DemoSchema>(&self.session)
    }
}

impl BusWorker<DemoSchema, DemoBus> for ConversationControl {
    type Error = DemoAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: DemoBus) -> Self::Run {
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
                        let directive = directive_for_input(&self.allowed_usernames, input);
                        let supersedes = if directive.is_conversation_turn() {
                            self.state.lock().await.active_run
                        } else {
                            None
                        };

                        if let Some(run_id) = supersedes {
                            publish::<DemoSchema, _>(
                                bus,
                                session_stream::<DemoSchema>(&self.session),
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

                        publish::<DemoSchema, _>(
                            bus,
                            session_stream::<DemoSchema>(&self.session),
                            mango_core::agent::EventVisibility::Internal,
                            EventPayload::Execution(ExecutionEvent::Control(
                                ControlEvent::Requested {
                                    request_id: DemoSchema::next_inference_request_id(),
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
    session: DemoSession,
    backend: ClaudeConversationConfig,
    state: Arc<Mutex<InferenceState>>,
}

impl ClaudeConversationInference {
    #[must_use]
    pub fn new(session: DemoSession, backend: ClaudeConversationConfig) -> Self {
        Self {
            worker_id: WorkerId::from("telegram-demo-claude-inference"),
            session,
            backend,
            state: Arc::new(Mutex::new(InferenceState::default())),
        }
    }
}

impl Worker for ClaudeConversationInference {
    type WorkerId = WorkerId;
    type Subscription = DemoSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<DemoSchema>(&self.session)
    }
}

impl BusWorker<DemoSchema, DemoBus> for ClaudeConversationInference {
    type Error = DemoAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: DemoBus) -> Self::Run {
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
    session: DemoSession,
}

impl SessionSentinel {
    #[must_use]
    pub fn new(worker_id: impl Into<WorkerId>, session: DemoSession) -> Self {
        Self {
            worker_id: worker_id.into(),
            session,
        }
    }
}

impl Worker for SessionSentinel {
    type WorkerId = WorkerId;
    type Subscription = DemoSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<DemoSchema>(&self.session)
    }
}

impl BusWorker<DemoSchema, DemoBus> for SessionSentinel {
    type Error = DemoAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: DemoBus) -> Self::Run {
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
pub fn demo_session(surface: TelegramSurface) -> DemoSession {
    new_session::<DemoSchema>(surface)
}

/// Spawn a Telegram demo session runtime backed by a lazy conversational
/// Claude bridge.
#[must_use]
pub fn spawn_demo_runtime<C>(
    client: C,
    surface: TelegramSurface,
    inbox: TelegramInbox,
    bus_capacity: usize,
    allowed_usernames: UsernameWhitelist,
    backend: &ClaudeConversationConfig,
) -> JoinHandle<()>
where
    C: TelegramClient + Clone + Send + Sync + 'static,
    DemoAppError: From<C::Error>,
{
    let session = demo_session(surface);
    let runtime = DemoRuntime::new(
        ExampleSubstrate::new(
            DemoBus::new(bus_capacity),
            ConversationControl::new(session.clone(), allowed_usernames),
        ),
        ExampleSurface::new(
            TelegramIngress::new(
                WorkerId::from("telegram-demo-ingress"),
                inbox,
                DemoTelegramInputMapper,
            ),
            TelegramEgress::new(
                WorkerId::from("telegram-demo-egress"),
                client,
                DisplayTelegramTextMapper,
            ),
            SessionSentinel::new("telegram-demo-presentation", session.clone()),
        ),
        ExampleBridge::new(
            ClaudeConversationInference::new(session.clone(), backend.clone()),
            SessionSentinel::new("telegram-demo-tools", session.clone()),
        ),
    );

    tokio::spawn(async move {
        if let Err(error) = runtime.startup(session.clone()).await {
            tracing::error!("mango-telegram-demo runtime startup failed: {error}");
            return;
        }
        if let Err(error) = runtime.run_session(session).await {
            tracing::error!("mango-telegram-demo runtime failed: {error}");
        }
    })
}

fn spawn_claude_bridge(
    backend: &ClaudeConversationConfig,
    session: &DemoSession,
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

fn directive_for_input(allowed_usernames: &UsernameWhitelist, input: DemoInput) -> DemoDirective {
    if allowed_usernames.contains(input.username.as_deref()) {
        DemoDirective::ConversationTurn { prompt: input.text }
    } else {
        DemoDirective::RejectedByWhitelist {
            response: NOT_MY_CUSTOMER.to_string(),
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
    request: <DemoSchema as AgentSchemaIds>::InferenceRequestId,
    session: <DemoSchema as AgentSchemaIds>::SessionId,
    thread: <DemoSchema as AgentSchemaIds>::ThreadId,
}

async fn publish_inference_started(
    bus: &DemoBus,
    session: &DemoSession,
    run_id: InferenceRunId,
    request: RequestedTurn,
    directive: DemoDirective,
    engine: &'static str,
) -> Result<(), DemoAppError> {
    publish::<DemoSchema, _>(
        bus,
        session_stream::<DemoSchema>(session),
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
    bus: &DemoBus,
    session: &DemoSession,
    state: &Arc<Mutex<InferenceState>>,
) -> Result<(), DemoAppError> {
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
    bus: &DemoBus,
    bridge: &mut Option<ClaudeAgentBridge>,
    bridge_events: &mut Option<broadcast::Receiver<ClaudeBridgeEvent>>,
    request: RequestedTurn,
    prompt: String,
) -> Result<(), DemoAppError> {
    let run_id = DemoSchema::next_inference_run_id();
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
        DemoDirective::ConversationTurn {
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

async fn handle_rejected_by_whitelist(
    bus: &DemoBus,
    session: &DemoSession,
    request: RequestedTurn,
    response: String,
) -> Result<(), DemoAppError> {
    let run_id = DemoSchema::next_inference_run_id();

    publish_inference_started(
        bus,
        session,
        run_id,
        request,
        DemoDirective::RejectedByWhitelist {
            response: response.clone(),
        },
        WHITELIST_ENGINE_ID,
    )
    .await?;
    publish::<DemoSchema, _>(
        bus,
        session_stream::<DemoSchema>(session),
        mango_core::agent::EventVisibility::Both,
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
            run_id,
            sequence: 0,
            output: response,
        })),
    )
    .await?;
    publish::<DemoSchema, _>(
        bus,
        session_stream::<DemoSchema>(session),
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
    bus: &DemoBus,
    bridge: Option<&ClaudeAgentBridge>,
    run_id: Option<InferenceRunId>,
    cause: Cancellation<DemoSchema>,
) -> Result<(), DemoAppError> {
    let active_run = worker.state.lock().await.current_run_id;
    if run_id.is_none() || active_run == run_id {
        if let Some(active_bridge) = bridge
            && let Err(error) = active_bridge.interrupt().await
        {
            error!("failed to interrupt Claude bridge: {error:#}");
        }

        if let Some(active_run) = active_run {
            publish::<DemoSchema, _>(
                bus,
                session_stream::<DemoSchema>(&worker.session),
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
    bus: &DemoBus,
    bridge: &mut Option<ClaudeAgentBridge>,
    bridge_events: &mut Option<broadcast::Receiver<ClaudeBridgeEvent>>,
    payload: EventPayload<DemoSchema>,
) -> Result<bool, DemoAppError> {
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
                DemoDirective::ConversationTurn { prompt } => {
                    handle_authorized_turn(worker, bus, bridge, bridge_events, request, prompt)
                        .await?;
                }
                DemoDirective::RejectedByWhitelist { response } => {
                    handle_rejected_by_whitelist(bus, &worker.session, request, response).await?;
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
    bus: &DemoBus,
    event: std::result::Result<ClaudeBridgeEvent, broadcast::error::RecvError>,
) -> Result<bool, DemoAppError> {
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
            publish_worker_error::<DemoSchema, _>(
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
            publish_worker_error::<DemoSchema, _>(
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
    bus: &DemoBus,
    worker_id: &WorkerId,
    session: &DemoSession,
    state: &Arc<Mutex<InferenceState>>,
    message: Value,
) -> Result<(), DemoAppError> {
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
    bus: &DemoBus,
    session: &DemoSession,
    state: &Arc<Mutex<InferenceState>>,
    message: &Value,
) -> Result<(), DemoAppError> {
    let mut state = state.lock().await;
    let Some(run_id) = state.current_run_id else {
        return Ok(());
    };

    if let Some(delta) = extract_stream_text_delta(message) {
        state.last_snapshot.push_str(&delta);
        publish::<DemoSchema, _>(
            bus,
            session_stream::<DemoSchema>(session),
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
    bus: &DemoBus,
    session: &DemoSession,
    state: &Arc<Mutex<InferenceState>>,
    message: &Value,
) -> Result<(), DemoAppError> {
    let mut state = state.lock().await;
    let Some(run_id) = state.current_run_id else {
        return Ok(());
    };

    if let Some(snapshot) = extract_text_snapshot(message)
        && let Some(delta) = incremental_suffix(&state.last_snapshot, &snapshot)
    {
        publish::<DemoSchema, _>(
            bus,
            session_stream::<DemoSchema>(session),
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
    bus: &DemoBus,
    worker_id: &WorkerId,
    session: &DemoSession,
    state: &Arc<Mutex<InferenceState>>,
    message: &Value,
) -> Result<(), DemoAppError> {
    let mut state = state.lock().await;
    let result_text = message
        .get("result")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_default();

    if let Some(run_id) = state.current_run_id {
        if let Some(delta) = incremental_suffix(&state.last_snapshot, &result_text) {
            publish::<DemoSchema, _>(
                bus,
                session_stream::<DemoSchema>(session),
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

            publish_worker_error::<DemoSchema, _>(
                bus,
                worker_id,
                session,
                "bridge_error",
                result_text.clone(),
            )
            .await?;

            publish::<DemoSchema, _>(
                bus,
                session_stream::<DemoSchema>(session),
                mango_core::agent::EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Failed {
                    run_id,
                    error: descriptor,
                })),
            )
            .await?;
        } else {
            publish::<DemoSchema, _>(
                bus,
                session_stream::<DemoSchema>(session),
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
    bus: &DemoBus,
    session: &DemoSession,
    state: &Arc<Mutex<InferenceState>>,
    error: mango_core::agent::ErrorDescriptor,
) -> Result<(), DemoAppError> {
    let run_id = state.lock().await.current_run_id;
    if let Some(run_id) = run_id {
        publish::<DemoSchema, _>(
            bus,
            session_stream::<DemoSchema>(session),
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
        DemoDirective, DemoInput, NOT_MY_CUSTOMER, UsernameWhitelist, directive_for_input,
        normalize_username,
    };

    #[test]
    fn username_whitelist_normalizes_configured_usernames() {
        let whitelist = UsernameWhitelist::from_usernames([" @KeithIsms ", "keithisms", ""]);

        assert_eq!(whitelist.len(), 1);
        assert!(whitelist.contains(Some("keithisms")));
        assert!(whitelist.contains(Some("@KeithIsms")));
    }

    #[test]
    fn directive_for_input_allows_whitelisted_username() {
        let whitelist = UsernameWhitelist::from_usernames(["keithisms"]);
        let directive = directive_for_input(
            &whitelist,
            DemoInput {
                text: "hello".to_string(),
                username: Some("@KeithIsms".to_string()),
            },
        );

        assert_eq!(
            directive,
            DemoDirective::ConversationTurn {
                prompt: "hello".to_string(),
            }
        );
    }

    #[test]
    fn directive_for_input_rejects_unknown_username() {
        let whitelist = UsernameWhitelist::from_usernames(["keithisms"]);
        let directive = directive_for_input(
            &whitelist,
            DemoInput {
                text: "hello".to_string(),
                username: Some("someone_else".to_string()),
            },
        );

        assert_eq!(
            directive,
            DemoDirective::RejectedByWhitelist {
                response: NOT_MY_CUSTOMER.to_string(),
            }
        );
    }

    #[test]
    fn normalize_username_trims_prefix_and_case() {
        assert_eq!(normalize_username(" @KeithIsms "), "keithisms");
    }
}
