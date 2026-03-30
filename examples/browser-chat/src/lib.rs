use std::{convert::Infallible, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::State,
    response::{
        Html, IntoResponse, Sse,
        sse::{Event as SseEvent, KeepAlive},
    },
    routing::{get, post},
};
use example_support::{
    BoxFuture, ConcurrentBusWorkers, DefaultAgentIds, EngineId, ExampleAppError, ExampleBus,
    ExampleRuntime, InMemoryEventBusError, InferenceRunId, StatusId, ToolName, WorkerId,
    all_subscription, error_descriptor, new_session, next_event, publish, publish_worker_error,
    session_stream, session_subscription,
};
use mango_core::agent::{
    AgentSchema, BusWorker, Cancellation, ControlEvent, EventBus, EventPayload, EventVisibility,
    ExecutionEvent, InferenceEvent, InteractionEvent, InterruptCause, OutboundEvent,
    PresentationEvent, SessionCloseReason, SessionContext, SessionWorker, StatusEvent,
    Subscription, Worker,
};
use mango_shim_claude_agent::{ClaudeAgentBridge, ClaudeBridgeEvent};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::{error, warn};

#[derive(Debug, Clone)]
pub enum ChatSurface {
    Browser,
}

#[derive(Debug, Clone)]
pub enum ChatInputKind {
    Text,
}

#[derive(Debug, Clone)]
pub struct ChatDirective {
    pub prompt: String,
}

#[derive(Debug, Clone)]
pub struct ChatSchema;

type ChatSubscription = Subscription<ChatSchema>;
type ChatSession = SessionContext<ChatSchema>;

impl AgentSchema for ChatSchema {
    type Ids = DefaultAgentIds;
    type Surface = ChatSurface;
    type InputKind = ChatInputKind;
    type Input = String;
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
    #[error("browser ingress already running")]
    IngressAlreadyRunning,
    #[error("{0}")]
    Bridge(String),
}

impl From<InMemoryEventBusError> for ChatAppError {
    fn from(value: InMemoryEventBusError) -> Self {
        match value {
            InMemoryEventBusError::Closed => Self::BusClosed,
            InMemoryEventBusError::Lagged(skipped) => Self::BusLagged(skipped),
        }
    }
}

impl ExampleAppError for ChatAppError {
    fn task_join(message: String) -> Self {
        Self::TaskJoin(message)
    }
}

pub type ChatBus = ExampleBus<ChatSchema, ChatAppError>;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UiEvent {
    InputEcho { text: String },
    AssistantToken { text: String },
    Status { text: String },
    StatusClear,
    Error { text: String },
}

#[derive(Debug, Deserialize)]
pub struct MessageRequest {
    pub text: String,
}

#[derive(Debug)]
enum BrowserIngressCommand {
    UserText(String),
    Interrupt,
}

/// Claude bridge used by the inference worker.
#[async_trait]
pub trait ClaudeBridgeLike: Clone + Send + Sync + 'static {
    async fn send_user_text(&self, text: String) -> Result<()>;
    async fn interrupt(&self) -> Result<()>;
    fn subscribe(&self) -> broadcast::Receiver<ClaudeBridgeEvent>;
}

#[async_trait]
impl ClaudeBridgeLike for ClaudeAgentBridge {
    async fn send_user_text(&self, text: String) -> Result<()> {
        ClaudeAgentBridge::send_user_text(self, text).await
    }

    async fn interrupt(&self) -> Result<()> {
        ClaudeAgentBridge::interrupt(self).await
    }

    fn subscribe(&self) -> broadcast::Receiver<ClaudeBridgeEvent> {
        ClaudeAgentBridge::subscribe(self)
    }
}

#[must_use]
#[derive(Clone)]
pub struct BrowserIngress {
    worker_id: WorkerId,
    commands: mpsc::Sender<BrowserIngressCommand>,
    receiver: Arc<Mutex<Option<mpsc::Receiver<BrowserIngressCommand>>>>,
}

impl BrowserIngress {
    pub fn new() -> Self {
        let (commands, receiver) = mpsc::channel(256);
        Self {
            worker_id: WorkerId::from("browser-ingress"),
            commands,
            receiver: Arc::new(Mutex::new(Some(receiver))),
        }
    }

    pub async fn submit_text(&self, text: String) -> bool {
        self.commands
            .send(BrowserIngressCommand::UserText(text))
            .await
            .is_ok()
    }

    pub async fn interrupt(&self) -> bool {
        self.commands
            .send(BrowserIngressCommand::Interrupt)
            .await
            .is_ok()
    }
}

impl Default for BrowserIngress {
    fn default() -> Self {
        Self::new()
    }
}

async fn publish_session_opened(bus: &ChatBus, session: &ChatSession) -> Result<(), ChatAppError> {
    publish::<ChatSchema, _>(
        bus,
        session_stream::<ChatSchema>(session),
        EventVisibility::Internal,
        EventPayload::Interaction(InteractionEvent::SessionOpened {
            session: session.clone(),
        }),
    )
    .await
}

async fn publish_session_closed(bus: &ChatBus, session: &ChatSession) -> Result<(), ChatAppError> {
    publish::<ChatSchema, _>(
        bus,
        session_stream::<ChatSchema>(session),
        EventVisibility::Internal,
        EventPayload::Interaction(InteractionEvent::SessionClosed {
            session_id: session.session_id,
            thread_id: session.thread_id,
            reason: SessionCloseReason::SurfaceDisconnected,
        }),
    )
    .await
}

async fn handle_browser_ingress_command(
    bus: &ChatBus,
    session: &ChatSession,
    command: BrowserIngressCommand,
) -> Result<(), ChatAppError> {
    match command {
        BrowserIngressCommand::UserText(text) => {
            let stream_id = ChatSchema::next_input_stream_id();
            let revision_id = ChatSchema::next_revision_id();
            let turn_id = ChatSchema::next_turn_id();

            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(session),
                EventVisibility::Internal,
                EventPayload::Interaction(InteractionEvent::InputStreamOpened {
                    session_id: session.session_id,
                    thread_id: session.thread_id,
                    stream_id,
                    kind: ChatInputKind::Text,
                }),
            )
            .await?;

            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(session),
                EventVisibility::Both,
                EventPayload::Interaction(InteractionEvent::InputDelta {
                    stream_id,
                    revision_id,
                    sequence: 0,
                    input: text.clone(),
                    stability: mango_core::agent::InputStability::Final,
                }),
            )
            .await?;

            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(session),
                EventVisibility::Internal,
                EventPayload::Interaction(InteractionEvent::InputCommitted {
                    session_id: session.session_id,
                    thread_id: session.thread_id,
                    stream_id,
                    revision_id,
                    turn_id,
                    input: text,
                }),
            )
            .await?;

            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(session),
                EventVisibility::Internal,
                EventPayload::Interaction(InteractionEvent::InputStreamClosed {
                    session_id: session.session_id,
                    thread_id: session.thread_id,
                    stream_id,
                }),
            )
            .await?;
        }
        BrowserIngressCommand::Interrupt => {
            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(session),
                EventVisibility::Both,
                EventPayload::Interaction(InteractionEvent::InputInterrupted {
                    session_id: session.session_id,
                    thread_id: session.thread_id,
                    cause: InterruptCause::ExplicitUserAction,
                }),
            )
            .await?;
        }
    }

    Ok(())
}

impl Worker for BrowserIngress {
    type WorkerId = WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        all_subscription::<ChatSchema>()
    }
}

impl SessionWorker<ChatSchema, ChatBus> for BrowserIngress {
    type Error = ChatAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: ChatBus, session: ChatSession) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut commands = self
                .receiver
                .lock()
                .await
                .take()
                .ok_or(ChatAppError::IngressAlreadyRunning)?;

            publish_session_opened(bus, &session).await?;

            while let Some(command) = commands.recv().await {
                handle_browser_ingress_command(bus, &session, command).await?;
            }

            publish_session_closed(bus, &session).await?;

            Ok(())
        })
    }
}

#[must_use]
#[derive(Clone)]
pub struct BrowserEgress {
    worker_id: WorkerId,
    ui_events: broadcast::Sender<UiEvent>,
}

impl BrowserEgress {
    pub fn new(capacity: usize) -> Self {
        let (ui_events, _) = broadcast::channel(capacity);
        Self {
            worker_id: WorkerId::from("browser-egress"),
            ui_events,
        }
    }

    #[must_use]
    pub fn subscribe_ui(&self) -> broadcast::Receiver<UiEvent> {
        self.ui_events.subscribe()
    }
}

impl Worker for BrowserEgress {
    type WorkerId = WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        all_subscription::<ChatSchema>()
    }
}

impl SessionWorker<ChatSchema, ChatBus> for BrowserEgress {
    type Error = ChatAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: ChatBus, session: ChatSession) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(session_subscription::<ChatSchema>(&session))?;

            while let Some(event) = next_event(&mut events).await? {
                if let Some(ui_event) = ui_event_from_event(&event, &session) {
                    let _ = self.ui_events.send(ui_event);
                }
            }

            Ok(())
        })
    }
}

#[derive(Default)]
struct ControlState {
    active_run: Option<InferenceRunId>,
}

#[must_use]
#[derive(Clone)]
pub struct SimpleChatControl {
    worker_id: WorkerId,
    session: SessionContext<ChatSchema>,
    state: Arc<Mutex<ControlState>>,
}

impl SimpleChatControl {
    pub fn new(session: SessionContext<ChatSchema>) -> Self {
        Self {
            worker_id: WorkerId::from("simple-chat-control"),
            session,
            state: Arc::new(Mutex::new(ControlState::default())),
        }
    }
}

impl Worker for SimpleChatControl {
    type WorkerId = WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<ChatSchema>(&self.session)
    }
}

impl BusWorker<ChatSchema, ChatBus> for SimpleChatControl {
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
                        input: prompt,
                        ..
                    }) => {
                        let state = self.state.lock().await;
                        let supersedes = state.active_run;
                        drop(state);

                        if let Some(run_id) = supersedes {
                            publish::<ChatSchema, _>(
                                bus,
                                session_stream::<ChatSchema>(&self.session),
                                EventVisibility::Internal,
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
                            EventVisibility::Internal,
                            EventPayload::Execution(ExecutionEvent::Control(
                                ControlEvent::Requested {
                                    request_id: ChatSchema::next_inference_request_id(),
                                    session_id: self.session.session_id,
                                    thread_id: self.session.thread_id,
                                    turn_id: Some(turn_id),
                                    directive: ChatDirective { prompt },
                                    supersedes,
                                },
                            )),
                        )
                        .await?;
                    }
                    EventPayload::Interaction(InteractionEvent::InputInterrupted { .. }) => {
                        if let Some(run_id) = self.state.lock().await.active_run {
                            publish::<ChatSchema, _>(
                                bus,
                                session_stream::<ChatSchema>(&self.session),
                                EventVisibility::Internal,
                                EventPayload::Execution(ExecutionEvent::Control(
                                    ControlEvent::CancelRequested {
                                        session_id: self.session.session_id,
                                        run_id: Some(run_id),
                                        cause: Cancellation::UserInterrupted,
                                    },
                                )),
                            )
                            .await?;
                        }
                    }
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Started { run_id, .. },
                    )) => {
                        self.state.lock().await.active_run = Some(run_id);
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
                    _ => {}
                }
            }

            Ok(())
        })
    }
}

#[derive(Default)]
struct InferenceState {
    current_run_id: Option<InferenceRunId>,
    next_sequence: u64,
    last_snapshot: String,
}

#[must_use]
#[derive(Clone)]
pub struct ClaudeChatInference<B> {
    worker_id: WorkerId,
    session: SessionContext<ChatSchema>,
    bridge: B,
    state: Arc<Mutex<InferenceState>>,
}

impl<B> ClaudeChatInference<B> {
    pub fn new(session: SessionContext<ChatSchema>, bridge: B) -> Self {
        Self {
            worker_id: WorkerId::from("claude-chat-inference"),
            session,
            bridge,
            state: Arc::new(Mutex::new(InferenceState::default())),
        }
    }
}

impl<B> Worker for ClaudeChatInference<B>
where
    B: ClaudeBridgeLike,
{
    type WorkerId = WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<ChatSchema>(&self.session)
    }
}

fn format_bridge_exit(label: &str, code: Option<i32>) -> String {
    format!(
        "{label} exited{}",
        code.map(|value| format!(" with code {value}"))
            .unwrap_or_default()
    )
}

async fn handle_chat_control_event<B>(
    worker: &ClaudeChatInference<B>,
    bus: &ChatBus,
    payload: EventPayload<ChatSchema>,
) -> Result<(), ChatAppError>
where
    B: ClaudeBridgeLike,
{
    match payload {
        EventPayload::Execution(ExecutionEvent::Control(ControlEvent::Requested {
            request_id,
            session_id,
            thread_id,
            directive,
            ..
        })) if session_id == worker.session.session_id => {
            let run_id = ChatSchema::next_inference_run_id();
            {
                let mut state = worker.state.lock().await;
                state.current_run_id = Some(run_id);
                state.next_sequence = 0;
                state.last_snapshot.clear();
            }

            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(&worker.session),
                EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Started {
                    run_id,
                    request_id,
                    session_id,
                    thread_id,
                    directive: directive.clone(),
                    engine: EngineId::from("claude-agent-sdk"),
                })),
            )
            .await?;

            if let Err(error) = worker.bridge.send_user_text(directive.prompt).await {
                publish_worker_error::<ChatSchema, _>(
                    bus,
                    &worker.worker_id,
                    &worker.session,
                    "bridge_error",
                    format!("failed to send prompt: {error}"),
                )
                .await?;
                publish::<ChatSchema, _>(
                    bus,
                    session_stream::<ChatSchema>(&worker.session),
                    EventVisibility::Internal,
                    EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Failed {
                        run_id,
                        error: error_descriptor("bridge_send_failed", error.to_string()),
                    })),
                )
                .await?;
            }
        }
        EventPayload::Execution(ExecutionEvent::Control(ControlEvent::CancelRequested {
            session_id,
            run_id,
            cause,
        })) if session_id == worker.session.session_id => {
            let active_run = worker.state.lock().await.current_run_id;
            if run_id.is_none() || active_run == run_id {
                if let Err(error) = worker.bridge.interrupt().await {
                    publish_worker_error::<ChatSchema, _>(
                        bus,
                        &worker.worker_id,
                        &worker.session,
                        "bridge_error",
                        format!("failed to interrupt: {error}"),
                    )
                    .await?;
                }

                if let Some(active_run) = active_run {
                    publish::<ChatSchema, _>(
                        bus,
                        session_stream::<ChatSchema>(&worker.session),
                        EventVisibility::Internal,
                        EventPayload::Execution(ExecutionEvent::Inference(
                            InferenceEvent::Cancelled {
                                run_id: active_run,
                                cause,
                            },
                        )),
                    )
                    .await?;
                    worker.state.lock().await.current_run_id = None;
                }
            }
        }
        _ => {}
    }

    Ok(())
}

async fn handle_chat_bridge_event<B>(
    worker: &ClaudeChatInference<B>,
    bus: &ChatBus,
    event: std::result::Result<ClaudeBridgeEvent, broadcast::error::RecvError>,
) -> Result<bool, ChatAppError>
where
    B: ClaudeBridgeLike,
{
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
                message,
            )
            .await?;
        }
        Ok(ClaudeBridgeEvent::Stderr { line }) => {
            warn!("claude bridge stderr: {line}");
        }
        Ok(ClaudeBridgeEvent::Exited { code }) => {
            publish_worker_error::<ChatSchema, _>(
                bus,
                &worker.worker_id,
                &worker.session,
                "bridge_error",
                format_bridge_exit("claude bridge", code),
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

impl<B> BusWorker<ChatSchema, ChatBus> for ClaudeChatInference<B>
where
    B: ClaudeBridgeLike,
{
    type Error = ChatAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: ChatBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;
            let mut bridge_events = self.bridge.subscribe();

            loop {
                tokio::select! {
                    maybe_event = next_event(&mut events) => {
                        let Some(event) = maybe_event? else {
                            break;
                        };
                        handle_chat_control_event(&self, bus, event.payload).await?;
                    }
                    bridge_event = bridge_events.recv() => {
                        if handle_chat_bridge_event(&self, bus, bridge_event).await? {
                            break;
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

#[derive(Default)]
struct StatusState {
    current_status_id: Option<StatusId>,
}

#[must_use]
#[derive(Clone)]
pub struct ThinkingStatusWorker {
    worker_id: WorkerId,
    session: SessionContext<ChatSchema>,
    state: Arc<Mutex<StatusState>>,
}

impl ThinkingStatusWorker {
    pub fn new(session: SessionContext<ChatSchema>) -> Self {
        Self {
            worker_id: WorkerId::from("thinking-status"),
            session,
            state: Arc::new(Mutex::new(StatusState::default())),
        }
    }
}

impl Worker for ThinkingStatusWorker {
    type WorkerId = WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<ChatSchema>(&self.session)
    }
}

impl BusWorker<ChatSchema, ChatBus> for ThinkingStatusWorker {
    type Error = ChatAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: ChatBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;

            while let Some(event) = next_event(&mut events).await? {
                match event.payload {
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Started { run_id, .. },
                    )) => {
                        let status_id = ChatSchema::next_status_id();
                        self.state.lock().await.current_status_id = Some(status_id);
                        publish::<ChatSchema, _>(
                            bus,
                            session_stream::<ChatSchema>(&self.session),
                            EventVisibility::Both,
                            EventPayload::Presentation(PresentationEvent::Status(
                                StatusEvent::Opened {
                                    status_id,
                                    session_id: self.session.session_id,
                                    run_id: Some(run_id),
                                    status: "thinking...".to_string(),
                                },
                            )),
                        )
                        .await?;
                    }
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Completed { .. }
                        | InferenceEvent::Cancelled { .. }
                        | InferenceEvent::Failed { .. },
                    )) => {
                        if let Some(status_id) = self.state.lock().await.current_status_id.take() {
                            publish::<ChatSchema, _>(
                                bus,
                                session_stream::<ChatSchema>(&self.session),
                                EventVisibility::Both,
                                EventPayload::Presentation(PresentationEvent::Status(
                                    StatusEvent::Closed { status_id },
                                )),
                            )
                            .await?;
                        }
                    }
                    _ => {}
                }
            }

            Ok(())
        })
    }
}

#[must_use]
#[derive(Clone)]
pub struct ChatProjector {
    worker_id: WorkerId,
    session: SessionContext<ChatSchema>,
}

impl ChatProjector {
    pub fn new(session: SessionContext<ChatSchema>) -> Self {
        Self {
            worker_id: WorkerId::from("chat-projector"),
            session,
        }
    }
}

impl Worker for ChatProjector {
    type WorkerId = WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<ChatSchema>(&self.session)
    }
}

async fn project_chat_status(
    worker: &ChatProjector,
    bus: &ChatBus,
    status: StatusEvent<ChatSchema>,
) -> Result<(), ChatAppError> {
    match status {
        StatusEvent::Opened {
            status_id, status, ..
        } => {
            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(
                    OutboundEvent::StatusOpened {
                        session_id: worker.session.session_id,
                        status_id,
                        status,
                    },
                )),
            )
            .await?;
        }
        StatusEvent::Updated {
            status_id, status, ..
        } => {
            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(
                    OutboundEvent::StatusUpdated {
                        session_id: worker.session.session_id,
                        status_id,
                        status,
                    },
                )),
            )
            .await?;
        }
        StatusEvent::Closed { status_id } => {
            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(
                    OutboundEvent::StatusClosed {
                        session_id: worker.session.session_id,
                        status_id,
                    },
                )),
            )
            .await?;
        }
    }

    Ok(())
}

async fn project_chat_event(
    worker: &ChatProjector,
    bus: &ChatBus,
    payload: EventPayload<ChatSchema>,
) -> Result<(), ChatAppError> {
    match payload {
        EventPayload::Interaction(InteractionEvent::InputDelta {
            stream_id,
            revision_id,
            input,
            stability,
            ..
        }) => {
            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(OutboundEvent::InputEcho {
                    session_id: worker.session.session_id,
                    stream_id,
                    revision_id,
                    input,
                    stability,
                })),
            )
            .await?;
        }
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
            run_id,
            sequence,
            output,
        })) => {
            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(OutboundEvent::Output {
                    session_id: worker.session.session_id,
                    run_id,
                    sequence,
                    output,
                })),
            )
            .await?;
        }
        EventPayload::Presentation(PresentationEvent::Status(status)) => {
            project_chat_status(worker, bus, status).await?;
        }
        EventPayload::Error(error_event)
            if error_event.stream == session_stream::<ChatSchema>(&worker.session) =>
        {
            publish::<ChatSchema, _>(
                bus,
                session_stream::<ChatSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(OutboundEvent::Error {
                    session_id: worker.session.session_id,
                    error: error_event.error,
                })),
            )
            .await?;
        }
        _ => {}
    }

    Ok(())
}

impl BusWorker<ChatSchema, ChatBus> for ChatProjector {
    type Error = ChatAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: ChatBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;

            while let Some(event) = next_event(&mut events).await? {
                project_chat_event(&self, bus, event.payload).await?;
            }

            Ok(())
        })
    }
}

#[must_use]
#[derive(Clone)]
pub struct NoopToolsWorker {
    worker_id: WorkerId,
}

impl NoopToolsWorker {
    pub fn new() -> Self {
        Self {
            worker_id: WorkerId::from("noop-tools"),
        }
    }
}

impl Default for NoopToolsWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl Worker for NoopToolsWorker {
    type WorkerId = WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        all_subscription::<ChatSchema>()
    }
}

impl BusWorker<ChatSchema, ChatBus> for NoopToolsWorker {
    type Error = ChatAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, _bus: ChatBus) -> Self::Run {
        Box::pin(async { std::future::pending::<Result<(), ChatAppError>>().await })
    }
}

pub type ChatPresentation = ConcurrentBusWorkers<WorkerId, ThinkingStatusWorker, ChatProjector>;
pub type ChatRuntime<B> = ExampleRuntime<
    ChatSchema,
    ChatAppError,
    ChatBus,
    BrowserIngress,
    BrowserEgress,
    SimpleChatControl,
    ClaudeChatInference<B>,
    NoopToolsWorker,
    ChatPresentation,
>;

#[must_use]
#[derive(Clone)]
pub struct AppState<B>
where
    B: ClaudeBridgeLike,
{
    runtime: Arc<ChatRuntime<B>>,
}

pub fn browser_router<B>(runtime: Arc<ChatRuntime<B>>) -> Router
where
    B: ClaudeBridgeLike,
{
    Router::new()
        .route("/", get(index))
        .route("/api/events", get(events::<B>))
        .route("/api/message", post(message::<B>))
        .route("/api/interrupt", post(interrupt::<B>))
        .with_state(AppState { runtime })
}

#[must_use]
pub fn browser_session() -> SessionContext<ChatSchema> {
    new_session::<ChatSchema>(ChatSurface::Browser)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn events<B>(
    State(state): State<AppState<B>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<SseEvent, Infallible>>>
where
    B: ClaudeBridgeLike,
{
    let stream =
        BroadcastStream::new(state.runtime.surface().egress().subscribe_ui()).filter_map(|event| {
            match event {
                Ok(event) => {
                    let payload = serde_json::to_string(&event).ok()?;
                    Some(Ok(SseEvent::default().data(payload)))
                }
                Err(_) => None,
            }
        });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn message<B>(
    State(state): State<AppState<B>>,
    Json(request): Json<MessageRequest>,
) -> impl IntoResponse
where
    B: ClaudeBridgeLike,
{
    if state
        .runtime
        .surface()
        .ingress()
        .submit_text(request.text)
        .await
    {
        axum::http::StatusCode::ACCEPTED
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn interrupt<B>(State(state): State<AppState<B>>) -> impl IntoResponse
where
    B: ClaudeBridgeLike,
{
    if state.runtime.surface().ingress().interrupt().await {
        axum::http::StatusCode::ACCEPTED
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

fn ui_event_from_event(
    event: &mango_core::agent::Event<ChatSchema>,
    session: &SessionContext<ChatSchema>,
) -> Option<UiEvent> {
    match &event.payload {
        EventPayload::Presentation(PresentationEvent::Outbound(outbound)) => match outbound {
            OutboundEvent::InputEcho {
                session_id, input, ..
            } if session_id == &session.session_id => Some(UiEvent::InputEcho {
                text: input.clone(),
            }),
            OutboundEvent::Output {
                session_id, output, ..
            } if session_id == &session.session_id => Some(UiEvent::AssistantToken {
                text: output.clone(),
            }),
            OutboundEvent::StatusOpened {
                session_id, status, ..
            } if session_id == &session.session_id => Some(UiEvent::Status {
                text: status.clone(),
            }),
            OutboundEvent::StatusUpdated {
                session_id, status, ..
            } if session_id == &session.session_id => Some(UiEvent::Status {
                text: status.clone(),
            }),
            OutboundEvent::StatusClosed { session_id, .. } if session_id == &session.session_id => {
                Some(UiEvent::StatusClear)
            }
            OutboundEvent::Error { session_id, error } if session_id == &session.session_id => {
                Some(UiEvent::Error {
                    text: error.message.clone(),
                })
            }
            _ => None,
        },
        _ => None,
    }
}

async fn handle_claude_sdk_message(
    bus: &ChatBus,
    worker_id: &WorkerId,
    session: &SessionContext<ChatSchema>,
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
    session: &SessionContext<ChatSchema>,
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
            EventVisibility::Both,
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
    session: &SessionContext<ChatSchema>,
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
            EventVisibility::Both,
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
    session: &SessionContext<ChatSchema>,
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
                EventVisibility::Both,
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
                EventVisibility::Internal,
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
                EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Completed {
                    run_id,
                    result: mango_core::agent::Completion::Completed,
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

#[must_use]
#[derive(Debug, Clone)]
pub struct FakeClaudeBridge {
    events: broadcast::Sender<ClaudeBridgeEvent>,
    submitted_prompts: Arc<Mutex<Vec<String>>>,
    interrupted: Arc<Mutex<bool>>,
    auto_reply: bool,
}

impl FakeClaudeBridge {
    pub fn new(auto_reply: bool) -> Self {
        let (events, _) = broadcast::channel(128);
        Self {
            events,
            submitted_prompts: Arc::new(Mutex::new(Vec::new())),
            interrupted: Arc::new(Mutex::new(false)),
            auto_reply,
        }
    }

    pub async fn was_interrupted(&self) -> bool {
        *self.interrupted.lock().await
    }

    pub async fn submitted_prompts(&self) -> Vec<String> {
        self.submitted_prompts.lock().await.clone()
    }

    fn emit_sdk_message(&self, message: Value) {
        let _ = self.events.send(ClaudeBridgeEvent::SdkMessage { message });
    }
}

#[async_trait]
impl ClaudeBridgeLike for FakeClaudeBridge {
    async fn send_user_text(&self, text: String) -> Result<()> {
        self.submitted_prompts.lock().await.push(text);

        if self.auto_reply {
            self.emit_sdk_message(json!({
                "type": "stream_event",
                "event": {
                    "type": "content_block_delta",
                    "delta": {
                        "type": "text_delta",
                        "text": "hi"
                    }
                }
            }));
            self.emit_sdk_message(json!({
                "type": "result",
                "result": "hi",
                "is_error": false
            }));
        }

        Ok(())
    }

    async fn interrupt(&self) -> Result<()> {
        *self.interrupted.lock().await = true;
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<ClaudeBridgeEvent> {
        self.events.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use example_support::{ExampleBridge, ExampleSubstrate, ExampleSurface};
    use mango_core::agent::AgentRuntime;
    use tokio::time::{Duration, timeout};

    async fn next_ui_event(receiver: &mut broadcast::Receiver<UiEvent>) -> UiEvent {
        timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("timed out waiting for ui event")
            .expect("ui event channel closed")
    }

    fn test_runtime(
        session: &ChatSession,
        bridge: FakeClaudeBridge,
    ) -> Arc<ChatRuntime<FakeClaudeBridge>> {
        Arc::new(ChatRuntime::new(
            ExampleSubstrate::new(ChatBus::new(256), SimpleChatControl::new(session.clone())),
            ExampleSurface::new(
                BrowserIngress::new(),
                BrowserEgress::new(256),
                ConcurrentBusWorkers::new(
                    "presentation",
                    ThinkingStatusWorker::new(session.clone()),
                    ChatProjector::new(session.clone()),
                ),
            ),
            ExampleBridge::new(
                ClaudeChatInference::new(session.clone(), bridge),
                NoopToolsWorker::new(),
            ),
        ))
    }

    #[tokio::test]
    async fn chat_runtime_streams_user_and_assistant_events() {
        let bridge = FakeClaudeBridge::new(true);
        let session = browser_session();
        let runtime = test_runtime(&session, bridge.clone());
        runtime
            .startup(session.clone())
            .await
            .expect("startup should stay healthy");
        let mut ui_events = runtime.surface().egress().subscribe_ui();

        let task = tokio::spawn({
            let runtime = runtime.clone();
            let session = session.clone();
            async move {
                runtime
                    .run_session(session)
                    .await
                    .expect("runtime should stay healthy");
            }
        });

        assert!(
            runtime
                .surface()
                .ingress()
                .submit_text("hello".to_string())
                .await
        );

        let first = next_ui_event(&mut ui_events).await;
        let second = next_ui_event(&mut ui_events).await;
        let third = next_ui_event(&mut ui_events).await;
        let fourth = next_ui_event(&mut ui_events).await;

        let events = [first, second, third, fourth];
        assert!(
            events
                .iter()
                .any(|event| matches!(event, UiEvent::InputEcho { text } if text == "hello"))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, UiEvent::Status { text } if text == "thinking..."))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, UiEvent::AssistantToken { text } if text == "hi"))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, UiEvent::StatusClear))
        );
        assert_eq!(bridge.submitted_prompts().await, vec!["hello".to_string()]);

        task.abort();
    }

    #[tokio::test]
    async fn chat_runtime_cancels_active_run() {
        let bridge = FakeClaudeBridge::new(false);
        let session = browser_session();
        let runtime = test_runtime(&session, bridge.clone());
        runtime
            .startup(session.clone())
            .await
            .expect("startup should stay healthy");
        let mut ui_events = runtime.surface().egress().subscribe_ui();

        let task = tokio::spawn({
            let runtime = runtime.clone();
            let session = session.clone();
            async move {
                runtime
                    .run_session(session)
                    .await
                    .expect("runtime should stay healthy");
            }
        });

        assert!(
            runtime
                .surface()
                .ingress()
                .submit_text("hello".to_string())
                .await
        );
        let _ = next_ui_event(&mut ui_events).await;
        let _ = next_ui_event(&mut ui_events).await;

        assert!(runtime.surface().ingress().interrupt().await);
        let cleared = next_ui_event(&mut ui_events).await;
        assert!(matches!(cleared, UiEvent::StatusClear));
        assert!(bridge.was_interrupted().await);

        task.abort();
    }
}
