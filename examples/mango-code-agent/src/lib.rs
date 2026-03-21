use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use mango_core::agent::{
    AgentSchema, BusWorker, ControlEvent, EventBus, EventPayload, EventVisibility, ExecutionEvent,
    InferenceEvent, InteractionEvent, OutboundEvent, PresentationEvent, SessionContext,
    SessionWorker, StatusEvent, Subscription, ToolEvent, Worker,
};
use mango_example_support::{
    BoxFuture, ConcurrentBusWorkers, DefaultAgentIds, EngineId, ExampleAppError, ExampleBus,
    ExampleRuntime, InMemoryEventBusError, InferenceRunId, StatusId, ToolCallId, ToolName,
    WorkerId, all_subscription, error_descriptor, new_session, next_event, publish,
    publish_worker_error, session_stream, session_subscription,
};
use mango_shim_claude_agent::{ClaudeAgentBridge, ClaudeBridgeEvent};
use serde_json::Value;
use tokio::{
    fs,
    process::Command,
    sync::{Mutex, broadcast},
    task::JoinSet,
    time::{Duration, timeout},
};
use tracing::warn;

#[derive(Debug, Clone)]
pub enum CodeSurface {
    Cli,
}

#[derive(Debug, Clone)]
pub enum CodeInputKind {
    Prompt,
}

#[derive(Debug, Clone)]
pub struct CodeDirective {
    pub prompt: String,
}

#[derive(Debug, Clone)]
pub enum CodeToolData {
    Arguments(Value),
    Result(String),
}

#[derive(Debug, Clone)]
pub struct CodeSchema;

type CodeSubscription = Subscription<CodeSchema>;
type CodeSession = SessionContext<CodeSchema>;

impl AgentSchema for CodeSchema {
    type Ids = DefaultAgentIds;
    type Surface = CodeSurface;
    type InputKind = CodeInputKind;
    type Input = String;
    type InterruptDetail = ();
    type Directive = CodeDirective;
    type Output = String;
    type ToolData = CodeToolData;
    type Status = String;
    type CancellationDetail = ();
    type CompletionDetail = ();
    type EngineId = EngineId;
    type ToolName = ToolName;
}

#[derive(Debug, thiserror::Error)]
pub enum CodeAppError {
    #[error("event bus closed")]
    BusClosed,
    #[error("event bus lagged by {0} events")]
    BusLagged(u64),
    #[error("task join failed: {0}")]
    TaskJoin(String),
    #[error("{0}")]
    Bridge(String),
}

impl From<InMemoryEventBusError> for CodeAppError {
    fn from(value: InMemoryEventBusError) -> Self {
        match value {
            InMemoryEventBusError::Closed => Self::BusClosed,
            InMemoryEventBusError::Lagged(skipped) => Self::BusLagged(skipped),
        }
    }
}

impl ExampleAppError for CodeAppError {
    fn task_join(message: String) -> Self {
        Self::TaskJoin(message)
    }
}

pub type CodeBus = ExampleBus<CodeSchema, CodeAppError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsoleEvent {
    InputEcho(String),
    Status(String),
    StatusClear,
    Tool(String),
    AssistantToken(String),
    Error(String),
}

#[async_trait]
pub trait ClaudeBridgeLike: Clone + Send + Sync + 'static {
    async fn send_user_text(&self, text: String) -> Result<()>;
    async fn respond_tool_success(&self, request_id: String, output: String) -> Result<()>;
    async fn respond_tool_failure(&self, request_id: String, message: String) -> Result<()>;
    async fn interrupt(&self) -> Result<()>;
    fn subscribe(&self) -> broadcast::Receiver<ClaudeBridgeEvent>;
}

#[async_trait]
impl ClaudeBridgeLike for ClaudeAgentBridge {
    async fn send_user_text(&self, text: String) -> Result<()> {
        ClaudeAgentBridge::send_user_text(self, text).await
    }

    async fn respond_tool_success(&self, request_id: String, output: String) -> Result<()> {
        ClaudeAgentBridge::respond_tool_success(self, request_id, output).await
    }

    async fn respond_tool_failure(&self, request_id: String, message: String) -> Result<()> {
        ClaudeAgentBridge::respond_tool_failure(self, request_id, message).await
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
pub struct PromptIngress {
    worker_id: WorkerId,
    prompt: String,
}

impl PromptIngress {
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            worker_id: WorkerId::from("prompt-ingress"),
            prompt: prompt.into(),
        }
    }
}

impl Worker for PromptIngress {
    type WorkerId = WorkerId;
    type Subscription = CodeSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        all_subscription::<CodeSchema>()
    }
}

impl SessionWorker<CodeBus, CodeSession> for PromptIngress {
    type Error = CodeAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: CodeBus, session: CodeSession) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let stream_id = CodeSchema::next_input_stream_id();
            let revision_id = CodeSchema::next_revision_id();
            let turn_id = CodeSchema::next_turn_id();

            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&session),
                EventVisibility::Internal,
                EventPayload::Interaction(InteractionEvent::SessionOpened {
                    session: session.clone(),
                }),
            )
            .await?;
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&session),
                EventVisibility::Internal,
                EventPayload::Interaction(InteractionEvent::InputStreamOpened {
                    session_id: session.session_id,
                    thread_id: session.thread_id,
                    stream_id,
                    kind: CodeInputKind::Prompt,
                }),
            )
            .await?;
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&session),
                EventVisibility::Both,
                EventPayload::Interaction(InteractionEvent::InputDelta {
                    stream_id,
                    revision_id,
                    sequence: 0,
                    input: self.prompt.clone(),
                    stability: mango_core::agent::InputStability::Final,
                }),
            )
            .await?;
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&session),
                EventVisibility::Internal,
                EventPayload::Interaction(InteractionEvent::InputCommitted {
                    session_id: session.session_id,
                    thread_id: session.thread_id,
                    stream_id,
                    revision_id,
                    turn_id,
                    input: self.prompt.clone(),
                }),
            )
            .await?;
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&session),
                EventVisibility::Internal,
                EventPayload::Interaction(InteractionEvent::InputStreamClosed {
                    session_id: session.session_id,
                    thread_id: session.thread_id,
                    stream_id,
                }),
            )
            .await?;
            Ok(())
        })
    }
}

#[must_use]
#[derive(Clone)]
pub struct PromptControl {
    worker_id: WorkerId,
    session: SessionContext<CodeSchema>,
}

impl PromptControl {
    pub fn new(session: SessionContext<CodeSchema>) -> Self {
        Self {
            worker_id: WorkerId::from("prompt-control"),
            session,
        }
    }
}

impl Worker for PromptControl {
    type WorkerId = WorkerId;
    type Subscription = CodeSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<CodeSchema>(&self.session)
    }
}

impl BusWorker<CodeBus> for PromptControl {
    type Error = CodeAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: CodeBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;

            while let Some(event) = next_event(&mut events).await? {
                match event.payload {
                    EventPayload::Interaction(InteractionEvent::InputCommitted {
                        input: prompt,
                        turn_id,
                        ..
                    }) => {
                        publish::<CodeSchema, _>(
                            bus,
                            session_stream::<CodeSchema>(&self.session),
                            EventVisibility::Internal,
                            EventPayload::Execution(ExecutionEvent::Control(
                                ControlEvent::Requested {
                                    request_id: CodeSchema::next_inference_request_id(),
                                    session_id: self.session.session_id,
                                    thread_id: self.session.thread_id,
                                    turn_id: Some(turn_id),
                                    directive: CodeDirective { prompt },
                                    supersedes: None,
                                },
                            )),
                        )
                        .await?;
                    }
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Completed { .. }
                        | InferenceEvent::Cancelled { .. }
                        | InferenceEvent::Failed { .. },
                    )) => break,
                    _ => {}
                }
            }

            Ok(())
        })
    }
}

#[derive(Default)]
struct CodingState {
    current_run_id: Option<InferenceRunId>,
    next_sequence: u64,
    last_snapshot: String,
}

#[must_use]
#[derive(Clone)]
pub struct ClaudeCodingInference<B> {
    worker_id: WorkerId,
    session: SessionContext<CodeSchema>,
    bridge: B,
    state: Arc<Mutex<CodingState>>,
}

impl<B> ClaudeCodingInference<B> {
    pub fn new(session: SessionContext<CodeSchema>, bridge: B) -> Self {
        Self {
            worker_id: WorkerId::from("claude-coding-inference"),
            session,
            bridge,
            state: Arc::new(Mutex::new(CodingState::default())),
        }
    }
}

impl<B> Worker for ClaudeCodingInference<B>
where
    B: ClaudeBridgeLike,
{
    type WorkerId = WorkerId;
    type Subscription = CodeSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<CodeSchema>(&self.session)
    }
}

fn format_bridge_exit(label: &str, code: Option<i32>) -> String {
    format!(
        "{label} exited{}",
        code.map(|value| format!(" with code {value}"))
            .unwrap_or_default()
    )
}

async fn handle_coding_control_event<B>(
    worker: &ClaudeCodingInference<B>,
    bus: &CodeBus,
    payload: EventPayload<CodeSchema>,
) -> Result<bool, CodeAppError>
where
    B: ClaudeBridgeLike,
{
    if let EventPayload::Execution(ExecutionEvent::Control(ControlEvent::Requested {
        request_id,
        session_id,
        thread_id,
        directive,
        ..
    })) = payload
    {
        let run_id = CodeSchema::next_inference_run_id();
        {
            let mut state = worker.state.lock().await;
            state.current_run_id = Some(run_id);
            state.next_sequence = 0;
            state.last_snapshot.clear();
        }

        publish::<CodeSchema, _>(
            bus,
            session_stream::<CodeSchema>(&worker.session),
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
            publish_worker_error::<CodeSchema, _>(
                bus,
                &worker.worker_id,
                &worker.session,
                "bridge_error",
                error.to_string(),
            )
            .await?;
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
                EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Failed {
                    run_id,
                    error: error_descriptor("bridge_send_failed", error.to_string()),
                })),
            )
            .await?;
            return Ok(true);
        }
    }

    Ok(false)
}

async fn handle_coding_bridge_event<B>(
    worker: &ClaudeCodingInference<B>,
    bus: &CodeBus,
    event: std::result::Result<ClaudeBridgeEvent, broadcast::error::RecvError>,
) -> Result<bool, CodeAppError>
where
    B: ClaudeBridgeLike,
{
    match event {
        Ok(ClaudeBridgeEvent::Ready { .. }) => {}
        Ok(ClaudeBridgeEvent::ToolCallRequested {
            request_id,
            tool_name,
            input,
        }) => {
            if let Some(run_id) = worker.state.lock().await.current_run_id {
                publish::<CodeSchema, _>(
                    bus,
                    session_stream::<CodeSchema>(&worker.session),
                    EventVisibility::Internal,
                    EventPayload::Execution(ExecutionEvent::Tool(ToolEvent::Requested {
                        call_id: ToolCallId::from(request_id),
                        run_id,
                        tool: ToolName::from(tool_name),
                        input: CodeToolData::Arguments(input),
                    })),
                )
                .await?;
            } else {
                publish_worker_error::<CodeSchema, _>(
                    bus,
                    &worker.worker_id,
                    &worker.session,
                    "bridge_error",
                    "received a tool request before inference started".to_string(),
                )
                .await?;
            }
        }
        Ok(ClaudeBridgeEvent::SdkMessage { message }) => {
            if handle_claude_coding_message(
                bus,
                &worker.worker_id,
                &worker.session,
                &worker.state,
                message,
            )
            .await?
            {
                return Ok(true);
            }
        }
        Ok(ClaudeBridgeEvent::BridgeError { message }) => {
            publish_worker_error::<CodeSchema, _>(
                bus,
                &worker.worker_id,
                &worker.session,
                "bridge_error",
                message,
            )
            .await?;
            return Ok(true);
        }
        Ok(ClaudeBridgeEvent::Stderr { line }) => {
            warn!("claude bridge stderr: {line}");
        }
        Ok(ClaudeBridgeEvent::Exited { code }) => {
            publish_worker_error::<CodeSchema, _>(
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

impl<B> BusWorker<CodeBus> for ClaudeCodingInference<B>
where
    B: ClaudeBridgeLike,
{
    type Error = CodeAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: CodeBus) -> Self::Run {
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

                        if handle_coding_control_event(&self, bus, event.payload).await? {
                            break;
                        }
                    }
                    bridge_event = bridge_events.recv() => {
                        if handle_coding_bridge_event(&self, bus, bridge_event).await? {
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
    session: SessionContext<CodeSchema>,
    state: Arc<Mutex<StatusState>>,
}

impl ThinkingStatusWorker {
    pub fn new(session: SessionContext<CodeSchema>) -> Self {
        Self {
            worker_id: WorkerId::from("thinking-status"),
            session,
            state: Arc::new(Mutex::new(StatusState::default())),
        }
    }
}

impl Worker for ThinkingStatusWorker {
    type WorkerId = WorkerId;
    type Subscription = CodeSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<CodeSchema>(&self.session)
    }
}

impl BusWorker<CodeBus> for ThinkingStatusWorker {
    type Error = CodeAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: CodeBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;
            while let Some(event) = next_event(&mut events).await? {
                match event.payload {
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Started { run_id, .. },
                    )) => {
                        let status_id = CodeSchema::next_status_id();
                        self.state.lock().await.current_status_id = Some(status_id);
                        publish::<CodeSchema, _>(
                            bus,
                            session_stream::<CodeSchema>(&self.session),
                            EventVisibility::Both,
                            EventPayload::Presentation(PresentationEvent::Status(
                                StatusEvent::Opened {
                                    status_id,
                                    session_id: self.session.session_id,
                                    run_id: Some(run_id),
                                    status: "coding...".to_string(),
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
                            publish::<CodeSchema, _>(
                                bus,
                                session_stream::<CodeSchema>(&self.session),
                                EventVisibility::Both,
                                EventPayload::Presentation(PresentationEvent::Status(
                                    StatusEvent::Closed { status_id },
                                )),
                            )
                            .await?;
                        }
                        break;
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
pub struct CodingProjector {
    worker_id: WorkerId,
    session: SessionContext<CodeSchema>,
    state: Arc<Mutex<ProjectorState>>,
}

#[derive(Default)]
struct ProjectorState {
    tool_names: HashMap<ToolCallId, String>,
}

impl CodingProjector {
    pub fn new(session: SessionContext<CodeSchema>) -> Self {
        Self {
            worker_id: WorkerId::from("coding-projector"),
            session,
            state: Arc::new(Mutex::new(ProjectorState::default())),
        }
    }
}

impl Worker for CodingProjector {
    type WorkerId = WorkerId;
    type Subscription = CodeSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<CodeSchema>(&self.session)
    }
}

async fn project_code_status(
    worker: &CodingProjector,
    bus: &CodeBus,
    status: StatusEvent<CodeSchema>,
) -> Result<bool, CodeAppError> {
    match status {
        StatusEvent::Opened {
            status_id, status, ..
        } => {
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
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
        StatusEvent::Closed { status_id } => {
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(
                    OutboundEvent::StatusClosed {
                        session_id: worker.session.session_id,
                        status_id,
                    },
                )),
            )
            .await?;
            return Ok(true);
        }
        StatusEvent::Updated {
            status_id, status, ..
        } => {
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
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
    }

    Ok(false)
}

async fn project_tool_event(
    worker: &CodingProjector,
    bus: &CodeBus,
    event: ToolEvent<CodeSchema>,
) -> Result<(), CodeAppError> {
    match event {
        ToolEvent::Requested {
            call_id,
            tool,
            input,
            ..
        } => {
            worker
                .state
                .lock()
                .await
                .tool_names
                .insert(call_id.clone(), tool.to_string());
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(
                    OutboundEvent::ToolProgress {
                        session_id: worker.session.session_id,
                        call_id,
                        status: format!("call {} {}", tool, preview_tool_payload(&input)),
                    },
                )),
            )
            .await?;
        }
        ToolEvent::Started { .. } => {}
        ToolEvent::Succeeded { call_id, output } => {
            let tool_name = worker
                .state
                .lock()
                .await
                .tool_names
                .remove(&call_id)
                .unwrap_or_else(|| call_id.to_string());
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(
                    OutboundEvent::ToolProgress {
                        session_id: worker.session.session_id,
                        call_id,
                        status: format!("result {} {}", tool_name, preview_tool_payload(&output)),
                    },
                )),
            )
            .await?;
        }
        ToolEvent::Failed { call_id, error } => {
            let tool_name = worker
                .state
                .lock()
                .await
                .tool_names
                .remove(&call_id)
                .unwrap_or_else(|| call_id.to_string());
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(OutboundEvent::Error {
                    session_id: worker.session.session_id,
                    error: error_descriptor(
                        error.code,
                        format!("tool {tool_name} failed: {}", error.message),
                    ),
                })),
            )
            .await?;
        }
        ToolEvent::Progress {
            call_id, update, ..
        } => {
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
                EventVisibility::UserVisible,
                EventPayload::Presentation(PresentationEvent::Outbound(
                    OutboundEvent::ToolProgress {
                        session_id: worker.session.session_id,
                        call_id,
                        status: update,
                    },
                )),
            )
            .await?;
        }
        ToolEvent::Cancelled { call_id, .. } => {
            worker.state.lock().await.tool_names.remove(&call_id);
        }
    }

    Ok(())
}

async fn project_code_event(
    worker: &CodingProjector,
    bus: &CodeBus,
    payload: EventPayload<CodeSchema>,
) -> Result<bool, CodeAppError> {
    match payload {
        EventPayload::Interaction(InteractionEvent::InputDelta {
            stream_id,
            revision_id,
            input,
            stability,
            ..
        }) => {
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
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
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
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
        EventPayload::Execution(ExecutionEvent::Tool(event)) => {
            project_tool_event(worker, bus, event).await?;
        }
        EventPayload::Presentation(PresentationEvent::Status(status)) => {
            return project_code_status(worker, bus, status).await;
        }
        EventPayload::Error(error_event)
            if error_event.stream == session_stream::<CodeSchema>(&worker.session) =>
        {
            publish::<CodeSchema, _>(
                bus,
                session_stream::<CodeSchema>(&worker.session),
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

    Ok(false)
}

impl BusWorker<CodeBus> for CodingProjector {
    type Error = CodeAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: CodeBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;
            while let Some(event) = next_event(&mut events).await? {
                if project_code_event(&self, bus, event.payload).await? {
                    break;
                }
            }
            Ok(())
        })
    }
}

#[must_use]
#[derive(Clone)]
pub struct TerminalEgress {
    worker_id: WorkerId,
    events: broadcast::Sender<ConsoleEvent>,
}

impl TerminalEgress {
    pub fn new(capacity: usize) -> Self {
        let (events, _) = broadcast::channel(capacity);
        Self {
            worker_id: WorkerId::from("terminal-egress"),
            events,
        }
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ConsoleEvent> {
        self.events.subscribe()
    }
}

impl Worker for TerminalEgress {
    type WorkerId = WorkerId;
    type Subscription = CodeSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        all_subscription::<CodeSchema>()
    }
}

impl SessionWorker<CodeBus, CodeSession> for TerminalEgress {
    type Error = CodeAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: CodeBus, session: CodeSession) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(session_subscription::<CodeSchema>(&session))?;
            while let Some(event) = next_event(&mut events).await? {
                if let EventPayload::Presentation(PresentationEvent::Outbound(outbound)) =
                    event.payload
                {
                    match outbound {
                        OutboundEvent::InputEcho {
                            session_id, input, ..
                        } if session_id == session.session_id => {
                            let _ = self.events.send(ConsoleEvent::InputEcho(input));
                        }
                        OutboundEvent::Output {
                            session_id, output, ..
                        } if session_id == session.session_id => {
                            let _ = self.events.send(ConsoleEvent::AssistantToken(output));
                        }
                        OutboundEvent::ToolProgress {
                            session_id, status, ..
                        } if session_id == session.session_id => {
                            let _ = self.events.send(ConsoleEvent::Tool(status));
                        }
                        OutboundEvent::StatusOpened {
                            session_id, status, ..
                        } if session_id == session.session_id => {
                            let _ = self.events.send(ConsoleEvent::Status(status));
                        }
                        OutboundEvent::StatusUpdated {
                            session_id, status, ..
                        } if session_id == session.session_id => {
                            let _ = self.events.send(ConsoleEvent::Status(status));
                        }
                        OutboundEvent::StatusClosed { session_id, .. }
                            if session_id == session.session_id =>
                        {
                            let _ = self.events.send(ConsoleEvent::StatusClear);
                            break;
                        }
                        OutboundEvent::Error { session_id, error }
                            if session_id == session.session_id =>
                        {
                            let _ = self.events.send(ConsoleEvent::Error(error.message));
                        }
                        _ => {}
                    }
                }
            }
            Ok(())
        })
    }
}

#[must_use]
#[derive(Clone)]
pub struct CodingToolsWorker<B>
where
    B: ClaudeBridgeLike,
{
    worker_id: WorkerId,
    session: SessionContext<CodeSchema>,
    bridge: B,
    cwd: PathBuf,
}

impl<B> CodingToolsWorker<B>
where
    B: ClaudeBridgeLike,
{
    pub fn new(session: SessionContext<CodeSchema>, bridge: B, cwd: PathBuf) -> Self {
        Self {
            worker_id: WorkerId::from("coding-tools"),
            session,
            bridge,
            cwd,
        }
    }
}

impl<B> Worker for CodingToolsWorker<B>
where
    B: ClaudeBridgeLike,
{
    type WorkerId = WorkerId;
    type Subscription = CodeSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<CodeSchema>(&self.session)
    }
}

impl<B> BusWorker<CodeBus> for CodingToolsWorker<B>
where
    B: ClaudeBridgeLike,
{
    type Error = CodeAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: CodeBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;
            let mut running_calls = JoinSet::new();
            let mut inference_finished = false;

            loop {
                tokio::select! {
                    Some(joined) = running_calls.join_next(), if !running_calls.is_empty() => {
                        joined.map_err(|error| CodeAppError::TaskJoin(error.to_string()))??;
                        if inference_finished && running_calls.is_empty() {
                            break;
                        }
                    }
                    maybe_event = next_event(&mut events), if !inference_finished => {
                        let Some(event) = maybe_event? else {
                            inference_finished = true;
                            continue;
                        };

                        match event.payload {
                            EventPayload::Execution(ExecutionEvent::Tool(ToolEvent::Requested {
                                call_id,
                                tool,
                                input,
                                ..
                            })) => {
                                let bus = bus.clone();
                                let bridge = self.bridge.clone();
                                let session = self.session.clone();
                                let worker_id = self.worker_id.clone();
                                let cwd = self.cwd.clone();
                                running_calls.spawn(async move {
                                    execute_tool_call(
                                        ToolCallContext {
                                            bus: &bus,
                                            bridge: &bridge,
                                            worker_id: &worker_id,
                                            session: &session,
                                            cwd: &cwd,
                                        },
                                        call_id,
                                        tool,
                                        input,
                                    )
                                    .await
                                });
                            }
                            EventPayload::Execution(ExecutionEvent::Inference(
                                InferenceEvent::Completed { .. }
                                | InferenceEvent::Cancelled { .. }
                                | InferenceEvent::Failed { .. },
                            )) => {
                                inference_finished = true;
                                if running_calls.is_empty() {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    else => {
                        if inference_finished && running_calls.is_empty() {
                            break;
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

pub type CodePresentation = ConcurrentBusWorkers<WorkerId, ThinkingStatusWorker, CodingProjector>;
pub type CodeAgentRuntime<B> = ExampleRuntime<
    CodeSchema,
    CodeAppError,
    CodeBus,
    PromptIngress,
    TerminalEgress,
    PromptControl,
    ClaudeCodingInference<B>,
    CodingToolsWorker<B>,
    CodePresentation,
>;

#[must_use]
pub fn cli_session() -> SessionContext<CodeSchema> {
    new_session::<CodeSchema>(CodeSurface::Cli)
}

struct ToolCallContext<'a, B> {
    bus: &'a CodeBus,
    bridge: &'a B,
    worker_id: &'a WorkerId,
    session: &'a SessionContext<CodeSchema>,
    cwd: &'a Path,
}

async fn execute_tool_call<B>(
    context: ToolCallContext<'_, B>,
    call_id: ToolCallId,
    tool: ToolName,
    input: CodeToolData,
) -> Result<(), CodeAppError>
where
    B: ClaudeBridgeLike,
{
    publish::<CodeSchema, _>(
        context.bus,
        session_stream::<CodeSchema>(context.session),
        EventVisibility::Internal,
        EventPayload::Execution(ExecutionEvent::Tool(ToolEvent::Started {
            call_id: call_id.clone(),
        })),
    )
    .await?;

    let CodeToolData::Arguments(arguments) = input else {
        let message = "tool request did not carry argument payload".to_string();
        context
            .bridge
            .respond_tool_failure(call_id.to_string(), message.clone())
            .await
            .map_err(|error| CodeAppError::Bridge(error.to_string()))?;
        publish::<CodeSchema, _>(
            context.bus,
            session_stream::<CodeSchema>(context.session),
            EventVisibility::Internal,
            EventPayload::Execution(ExecutionEvent::Tool(ToolEvent::Failed {
                call_id,
                error: error_descriptor("invalid_tool_request", message),
            })),
        )
        .await?;
        return Ok(());
    };

    match run_code_tool(context.cwd, tool.as_str(), &arguments).await {
        Ok(output) => {
            context
                .bridge
                .respond_tool_success(call_id.to_string(), output.clone())
                .await
                .map_err(|error| CodeAppError::Bridge(error.to_string()))?;
            publish::<CodeSchema, _>(
                context.bus,
                session_stream::<CodeSchema>(context.session),
                EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Tool(ToolEvent::Succeeded {
                    call_id,
                    output: CodeToolData::Result(output),
                })),
            )
            .await?;
        }
        Err(message) => {
            if let Err(error) = context
                .bridge
                .respond_tool_failure(call_id.to_string(), message.clone())
                .await
            {
                publish_worker_error::<CodeSchema, _>(
                    context.bus,
                    context.worker_id,
                    context.session,
                    "bridge_error",
                    format!("failed returning tool error to bridge: {error}"),
                )
                .await?;
            }

            publish::<CodeSchema, _>(
                context.bus,
                session_stream::<CodeSchema>(context.session),
                EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Tool(ToolEvent::Failed {
                    call_id,
                    error: error_descriptor(format!("tool_{tool}_failed"), message),
                })),
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_code_tool(
    cwd: &Path,
    tool_name: &str,
    arguments: &Value,
) -> std::result::Result<String, String> {
    match tool_name {
        "bash" => run_bash_tool(cwd, arguments).await,
        "read_file" => run_read_file_tool(cwd, arguments).await,
        "write_file" => run_write_file_tool(cwd, arguments).await,
        "glob" => run_glob_tool(cwd, arguments).await,
        "grep" => run_grep_tool(cwd, arguments).await,
        other => Err(format!("unknown Mango coding tool: {other}")),
    }
}

async fn run_bash_tool(cwd: &Path, arguments: &Value) -> std::result::Result<String, String> {
    let command = required_string(arguments, "command")?;
    let timeout_ms = optional_u64(arguments, "timeout_ms")?.unwrap_or(30_000);

    let output = timeout(
        Duration::from_millis(timeout_ms),
        Command::new("/bin/zsh")
            .arg("-lc")
            .arg(&command)
            .current_dir(cwd)
            .output(),
    )
    .await
    .map_err(|_| format!("bash command timed out after {timeout_ms}ms"))?
    .map_err(|error| format!("failed to spawn shell command: {error}"))?;

    let rendered = render_command_output(&output.stdout, &output.stderr);
    if output.status.success() {
        Ok(rendered)
    } else {
        Err(format!(
            "command exited with status {}\n{}",
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string()),
            rendered
        ))
    }
}

async fn run_read_file_tool(cwd: &Path, arguments: &Value) -> std::result::Result<String, String> {
    let path = resolve_tool_path(cwd, &required_string(arguments, "path")?);
    let start_line = optional_u64(arguments, "start_line")?;
    let end_line = optional_u64(arguments, "end_line")?;

    let content = fs::read_to_string(&path)
        .await
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    Ok(render_numbered_file(&path, &content, start_line, end_line))
}

async fn run_write_file_tool(cwd: &Path, arguments: &Value) -> std::result::Result<String, String> {
    let path = resolve_tool_path(cwd, &required_string(arguments, "path")?);
    let content = required_string(arguments, "content")?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }

    fs::write(&path, content.as_bytes())
        .await
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;

    Ok(format!(
        "wrote {} bytes to {}",
        content.len(),
        path.display()
    ))
}

async fn run_glob_tool(cwd: &Path, arguments: &Value) -> std::result::Result<String, String> {
    let pattern = required_string(arguments, "pattern")?;
    let root = optional_string(arguments, "path")?
        .map_or_else(|| cwd.to_path_buf(), |path| resolve_tool_path(cwd, &path));

    let output = Command::new("rg")
        .arg("--files")
        .arg("-g")
        .arg(&pattern)
        .arg(".")
        .current_dir(&root)
        .output()
        .await
        .map_err(|error| format!("failed to run rg for glob search: {error}"))?;

    if !output.status.success() && output.status.code() != Some(1) {
        return Err(render_command_failure("glob search failed", &output));
    }

    Ok(clean_relative_listing(
        String::from_utf8_lossy(&output.stdout).as_ref(),
    ))
}

async fn run_grep_tool(cwd: &Path, arguments: &Value) -> std::result::Result<String, String> {
    let pattern = required_string(arguments, "pattern")?;
    let root = optional_string(arguments, "path")?
        .map_or_else(|| cwd.to_path_buf(), |path| resolve_tool_path(cwd, &path));

    let mut command = Command::new("rg");
    command
        .arg("-n")
        .arg("--no-heading")
        .arg("--color")
        .arg("never");
    if let Some(glob) = optional_string(arguments, "glob")? {
        command.arg("-g").arg(glob);
    }
    command.arg(&pattern).arg(".").current_dir(&root);

    let output = command
        .output()
        .await
        .map_err(|error| format!("failed to run rg for grep search: {error}"))?;

    match output.status.code() {
        Some(0) => Ok(clean_relative_listing(
            String::from_utf8_lossy(&output.stdout).as_ref(),
        )),
        Some(1) => Ok("(no matches)".to_string()),
        _ => Err(render_command_failure("grep search failed", &output)),
    }
}

fn required_string(arguments: &Value, key: &str) -> std::result::Result<String, String> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("missing string field `{key}`"))
}

fn optional_string(arguments: &Value, key: &str) -> std::result::Result<Option<String>, String> {
    match arguments.get(key) {
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(format!("field `{key}` must be a string")),
    }
}

fn optional_u64(arguments: &Value, key: &str) -> std::result::Result<Option<u64>, String> {
    match arguments.get(key) {
        Some(Value::Number(value)) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("field `{key}` must be a positive integer")),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(format!("field `{key}` must be a positive integer")),
    }
}

fn resolve_tool_path(cwd: &Path, raw_path: &str) -> PathBuf {
    let path = PathBuf::from(raw_path);
    if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    }
}

fn render_numbered_file(
    path: &Path,
    content: &str,
    start_line: Option<u64>,
    end_line: Option<u64>,
) -> String {
    let mut lines = Vec::new();
    let start = start_line.unwrap_or(1);
    let end = end_line.unwrap_or(u64::MAX);

    for (index, line) in content.lines().enumerate() {
        let line_number = index as u64 + 1;
        if line_number < start || line_number > end {
            continue;
        }
        lines.push(format!("{line_number:>4} {line}"));
    }

    if lines.is_empty() {
        format!("{} is empty in the requested range", path.display())
    } else {
        format!("{}\n{}", path.display(), lines.join("\n"))
    }
}

fn render_command_output(stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = String::from_utf8_lossy(stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();

    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => "(no output)".to_string(),
        (false, true) => stdout,
        (true, false) => format!("stderr:\n{stderr}"),
        (false, false) => format!("stdout:\n{stdout}\n\nstderr:\n{stderr}"),
    }
}

fn render_command_failure(prefix: &str, output: &std::process::Output) -> String {
    format!(
        "{prefix}: {}\n{}",
        output
            .status
            .code()
            .map_or_else(|| "signal".to_string(), |code| code.to_string()),
        render_command_output(&output.stdout, &output.stderr)
    )
}

fn clean_relative_listing(text: &str) -> String {
    let cleaned = text
        .lines()
        .map(|line| line.strip_prefix("./").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n");
    if cleaned.trim().is_empty() {
        "(no results)".to_string()
    } else {
        cleaned
    }
}

async fn handle_claude_coding_message(
    bus: &CodeBus,
    worker_id: &WorkerId,
    session: &SessionContext<CodeSchema>,
    state: &Arc<Mutex<CodingState>>,
    message: Value,
) -> Result<bool, CodeAppError> {
    match message.get("type").and_then(Value::as_str) {
        Some("stream_event") => handle_claude_stream_message(bus, session, state, &message).await,
        Some("assistant") => handle_claude_assistant_message(bus, session, state, &message).await,
        Some("result") => {
            handle_claude_result_message(bus, worker_id, session, state, &message).await
        }
        _ => Ok(false),
    }
}

async fn handle_claude_stream_message(
    bus: &CodeBus,
    session: &SessionContext<CodeSchema>,
    state: &Arc<Mutex<CodingState>>,
    message: &Value,
) -> Result<bool, CodeAppError> {
    let mut state = state.lock().await;
    let Some(run_id) = state.current_run_id else {
        return Ok(false);
    };

    if let Some(delta) = extract_stream_text_delta(message) {
        state.last_snapshot.push_str(&delta);
        publish::<CodeSchema, _>(
            bus,
            session_stream::<CodeSchema>(session),
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

    Ok(false)
}

async fn handle_claude_assistant_message(
    bus: &CodeBus,
    session: &SessionContext<CodeSchema>,
    state: &Arc<Mutex<CodingState>>,
    message: &Value,
) -> Result<bool, CodeAppError> {
    let mut state = state.lock().await;
    let Some(run_id) = state.current_run_id else {
        return Ok(false);
    };

    if let Some(snapshot) = extract_text_snapshot(message)
        && let Some(delta) = incremental_suffix(&state.last_snapshot, &snapshot)
    {
        publish::<CodeSchema, _>(
            bus,
            session_stream::<CodeSchema>(session),
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

    Ok(false)
}

async fn handle_claude_result_message(
    bus: &CodeBus,
    worker_id: &WorkerId,
    session: &SessionContext<CodeSchema>,
    state: &Arc<Mutex<CodingState>>,
    message: &Value,
) -> Result<bool, CodeAppError> {
    let mut state = state.lock().await;
    let Some(run_id) = state.current_run_id else {
        return Ok(true);
    };
    let result_text = message
        .get("result")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    if let Some(delta) = incremental_suffix(&state.last_snapshot, &result_text) {
        publish::<CodeSchema, _>(
            bus,
            session_stream::<CodeSchema>(session),
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
        publish_worker_error::<CodeSchema, _>(bus, worker_id, session, "bridge_error", result_text)
            .await?;
        publish::<CodeSchema, _>(
            bus,
            session_stream::<CodeSchema>(session),
            EventVisibility::Internal,
            EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Failed {
                run_id,
                error: descriptor,
            })),
        )
        .await?;
    } else {
        publish::<CodeSchema, _>(
            bus,
            session_stream::<CodeSchema>(session),
            EventVisibility::Internal,
            EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Completed {
                run_id,
                result: mango_core::agent::Completion::Completed,
            })),
        )
        .await?;
    }

    state.current_run_id = None;
    state.next_sequence = 0;
    state.last_snapshot.clear();
    Ok(true)
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
    let content = message
        .get("message")
        .and_then(|value| value.get("content"))
        .or_else(|| message.get("content"))?;

    let text = match content {
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                item.get("type")
                    .and_then(Value::as_str)
                    .filter(|kind| *kind == "text")
                    .and_then(|_| item.get("text"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect::<String>(),
        Value::String(text) => text.clone(),
        _ => String::new(),
    };

    if text.is_empty() { None } else { Some(text) }
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

fn preview_tool_payload(value: &CodeToolData) -> String {
    let raw = match value {
        CodeToolData::Arguments(arguments) => {
            serde_json::to_string(arguments).unwrap_or_else(|_| arguments.to_string())
        }
        CodeToolData::Result(output) => output.clone(),
    };

    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= 120 {
        collapsed
    } else {
        format!("{}...", collapsed.chars().take(117).collect::<String>())
    }
}

#[must_use]
#[derive(Debug, Clone)]
pub struct FakeClaudeBridge {
    events: broadcast::Sender<ClaudeBridgeEvent>,
    prompts: Arc<Mutex<Vec<String>>>,
    tool_outputs: Arc<Mutex<Vec<(String, String)>>>,
}

impl FakeClaudeBridge {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(128);
        Self {
            events,
            prompts: Arc::new(Mutex::new(Vec::new())),
            tool_outputs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn prompts(&self) -> Vec<String> {
        self.prompts.lock().await.clone()
    }

    pub async fn tool_outputs(&self) -> Vec<(String, String)> {
        self.tool_outputs.lock().await.clone()
    }
}

impl Default for FakeClaudeBridge {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ClaudeBridgeLike for FakeClaudeBridge {
    async fn send_user_text(&self, text: String) -> Result<()> {
        self.prompts.lock().await.push(text);
        let _ = self.events.send(ClaudeBridgeEvent::ToolCallRequested {
            request_id: "tool-1".to_string(),
            tool_name: "bash".to_string(),
            input: serde_json::json!({
                "command": "printf done"
            }),
        });
        Ok(())
    }

    async fn respond_tool_success(&self, request_id: String, output: String) -> Result<()> {
        self.tool_outputs
            .lock()
            .await
            .push((request_id.clone(), output.clone()));
        let _ = self.events.send(ClaudeBridgeEvent::SdkMessage {
            message: serde_json::json!({
                "type": "stream_event",
                "event": {
                    "type": "content_block_delta",
                    "delta": { "type": "text_delta", "text": output }
                }
            }),
        });
        let _ = self.events.send(ClaudeBridgeEvent::SdkMessage {
            message: serde_json::json!({
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
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<ClaudeBridgeEvent> {
        self.events.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mango_core::agent::AgentRuntime;
    use mango_example_support::ExampleWorkers;
    use tokio::time::{Duration, timeout};

    async fn next_console_event(receiver: &mut broadcast::Receiver<ConsoleEvent>) -> ConsoleEvent {
        timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("timed out waiting for console event")
            .expect("console channel closed")
    }

    #[tokio::test]
    async fn code_agent_streams_tool_events_and_answer() {
        let bridge = FakeClaudeBridge::new();
        let session = cli_session();
        let runtime = Arc::new(CodeAgentRuntime::new(
            CodeBus::new(256),
            session.clone(),
            ExampleWorkers::new(
                PromptIngress::new("List the top-level files"),
                TerminalEgress::new(256),
                PromptControl::new(session.clone()),
                ClaudeCodingInference::new(session.clone(), bridge.clone()),
                CodingToolsWorker::new(
                    session.clone(),
                    bridge.clone(),
                    std::env::current_dir().expect("cwd should resolve"),
                ),
                ConcurrentBusWorkers::new(
                    "presentation",
                    ThinkingStatusWorker::new(session.clone()),
                    CodingProjector::new(session.clone()),
                ),
            ),
        ));

        let mut egress = runtime.egress().subscribe();
        let task = tokio::spawn({
            let runtime = runtime.clone();
            async move {
                runtime
                    .run_session(runtime.session().clone())
                    .await
                    .expect("runtime should stay healthy");
            }
        });

        let mut events = Vec::new();
        for _ in 0..8 {
            let event = next_console_event(&mut egress).await;
            let done = matches!(event, ConsoleEvent::StatusClear);
            events.push(event);
            if done {
                break;
            }
        }
        assert!(events.iter().any(|event| matches!(event, ConsoleEvent::InputEcho(text) if text == "List the top-level files")));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ConsoleEvent::Status(text) if text == "coding..."))
        );
        assert!(
            events.iter().any(
                |event| matches!(event, ConsoleEvent::Tool(text) if text.contains("call bash"))
            )
        );
        assert!(events.iter().any(
            |event| matches!(event, ConsoleEvent::Tool(text) if text.contains("result bash"))
        ));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ConsoleEvent::AssistantToken(text) if text == "done"))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ConsoleEvent::StatusClear))
        );
        assert_eq!(
            bridge.prompts().await,
            vec!["List the top-level files".to_string()]
        );
        assert_eq!(
            bridge.tool_outputs().await,
            vec![("tool-1".to_string(), "done".to_string())]
        );

        task.abort();
    }
}
