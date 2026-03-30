use std::{collections::HashMap, convert::Infallible, sync::Arc};

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
    ExecutionEvent, InferenceEvent, InteractionEvent, OutboundEvent, PresentationEvent,
    SessionContext, SessionWorker, StatusEvent, Subscription, Worker,
};
use mango_shim_claude_agent::{ClaudeAgentBridge, ClaudeBridgeEvent};
use mango_shim_codex::{CodexAgentBridge, CodexBridgeEvent};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Speaker {
    Codex,
    Claude,
    Moderator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DebateStage {
    Opening,
    Rebuttal,
    Final,
}

#[derive(Debug, Clone)]
pub enum DebateSurface {
    Browser,
}

#[derive(Debug, Clone)]
pub enum DebateInputKind {
    Question,
}

#[derive(Debug, Clone)]
pub struct DebateDirective {
    pub speaker: Speaker,
    pub stage: DebateStage,
    pub prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebateChunk {
    pub speaker: Speaker,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct DebateSchema;

type DebateSubscription = Subscription<DebateSchema>;
type DebateSession = SessionContext<DebateSchema>;

impl AgentSchema for DebateSchema {
    type Ids = DefaultAgentIds;
    type Surface = DebateSurface;
    type InputKind = DebateInputKind;
    type Input = String;
    type InterruptDetail = ();
    type Directive = DebateDirective;
    type Output = DebateChunk;
    type ToolData = ();
    type Status = String;
    type CancellationDetail = ();
    type CompletionDetail = ();
    type EngineId = EngineId;
    type ToolName = ToolName;
}

#[derive(Debug, thiserror::Error)]
pub enum DebateAppError {
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

impl From<InMemoryEventBusError> for DebateAppError {
    fn from(value: InMemoryEventBusError) -> Self {
        match value {
            InMemoryEventBusError::Closed => Self::BusClosed,
            InMemoryEventBusError::Lagged(skipped) => Self::BusLagged(skipped),
        }
    }
}

impl ExampleAppError for DebateAppError {
    fn task_join(message: String) -> Self {
        Self::TaskJoin(message)
    }
}

pub type DebateBus = ExampleBus<DebateSchema, DebateAppError>;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UiEvent {
    InputEcho { text: String },
    DebateToken { speaker: Speaker, text: String },
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
    Question(String),
    Interrupt,
}

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

#[async_trait]
pub trait CodexBridgeLike: Clone + Send + Sync + 'static {
    async fn send_user_text(&self, text: String) -> Result<()>;
    async fn interrupt(&self) -> Result<()>;
    fn subscribe(&self) -> broadcast::Receiver<CodexBridgeEvent>;
}

#[async_trait]
impl CodexBridgeLike for CodexAgentBridge {
    async fn send_user_text(&self, text: String) -> Result<()> {
        CodexAgentBridge::send_user_text(self, text).await
    }

    async fn interrupt(&self) -> Result<()> {
        CodexAgentBridge::interrupt(self).await
    }

    fn subscribe(&self) -> broadcast::Receiver<CodexBridgeEvent> {
        CodexAgentBridge::subscribe(self)
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

    pub async fn submit_question(&self, question: String) -> bool {
        self.commands
            .send(BrowserIngressCommand::Question(question))
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

impl Worker for BrowserIngress {
    type WorkerId = WorkerId;
    type Subscription = DebateSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        all_subscription::<DebateSchema>()
    }
}

impl SessionWorker<DebateSchema, DebateBus> for BrowserIngress {
    type Error = DebateAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: DebateBus, session: DebateSession) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut commands = self
                .receiver
                .lock()
                .await
                .take()
                .ok_or(DebateAppError::IngressAlreadyRunning)?;

            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(&session),
                EventVisibility::Internal,
                EventPayload::Interaction(InteractionEvent::SessionOpened {
                    session: session.clone(),
                }),
            )
            .await?;

            while let Some(command) = commands.recv().await {
                match command {
                    BrowserIngressCommand::Question(question) => {
                        let stream_id = DebateSchema::next_input_stream_id();
                        let revision_id = DebateSchema::next_revision_id();
                        let turn_id = DebateSchema::next_turn_id();
                        publish::<DebateSchema, _>(
                            bus,
                            session_stream::<DebateSchema>(&session),
                            EventVisibility::Internal,
                            EventPayload::Interaction(InteractionEvent::InputStreamOpened {
                                session_id: session.session_id,
                                thread_id: session.thread_id,
                                stream_id,
                                kind: DebateInputKind::Question,
                            }),
                        )
                        .await?;
                        publish::<DebateSchema, _>(
                            bus,
                            session_stream::<DebateSchema>(&session),
                            EventVisibility::Both,
                            EventPayload::Interaction(InteractionEvent::InputDelta {
                                stream_id,
                                revision_id,
                                sequence: 0,
                                input: question.clone(),
                                stability: mango_core::agent::InputStability::Final,
                            }),
                        )
                        .await?;
                        publish::<DebateSchema, _>(
                            bus,
                            session_stream::<DebateSchema>(&session),
                            EventVisibility::Internal,
                            EventPayload::Interaction(InteractionEvent::InputCommitted {
                                session_id: session.session_id,
                                thread_id: session.thread_id,
                                stream_id,
                                revision_id,
                                turn_id,
                                input: question,
                            }),
                        )
                        .await?;
                        publish::<DebateSchema, _>(
                            bus,
                            session_stream::<DebateSchema>(&session),
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
                        publish::<DebateSchema, _>(
                            bus,
                            session_stream::<DebateSchema>(&session),
                            EventVisibility::Both,
                            EventPayload::Interaction(InteractionEvent::InputInterrupted {
                                session_id: session.session_id,
                                thread_id: session.thread_id,
                                cause: mango_core::agent::InterruptCause::ExplicitUserAction,
                            }),
                        )
                        .await?;
                    }
                }
            }

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
    type Subscription = DebateSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        all_subscription::<DebateSchema>()
    }
}

impl SessionWorker<DebateSchema, DebateBus> for BrowserEgress {
    type Error = DebateAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: DebateBus, session: DebateSession) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(session_subscription::<DebateSchema>(&session))?;
            while let Some(event) = next_event(&mut events).await? {
                if let Some(ui_event) = ui_event_from_event(&event, &session) {
                    let _ = self.ui_events.send(ui_event);
                }
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone)]
struct RunInfo {
    speaker: Speaker,
    stage: DebateStage,
    text: String,
}

#[derive(Default)]
struct DebateControlState {
    current_question: Option<String>,
    active_runs: HashMap<InferenceRunId, RunInfo>,
    openings: HashMap<Speaker, String>,
    rebuttals: HashMap<Speaker, String>,
    final_started: bool,
}

#[must_use]
#[derive(Clone)]
pub struct DebateControl {
    worker_id: WorkerId,
    session: SessionContext<DebateSchema>,
    state: Arc<Mutex<DebateControlState>>,
}

impl DebateControl {
    pub fn new(session: SessionContext<DebateSchema>) -> Self {
        Self {
            worker_id: WorkerId::from("debate-control"),
            session,
            state: Arc::new(Mutex::new(DebateControlState::default())),
        }
    }
}

impl Worker for DebateControl {
    type WorkerId = WorkerId;
    type Subscription = DebateSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<DebateSchema>(&self.session)
    }
}

enum PendingDebateRequests {
    Rebuttals {
        codex_opening: String,
        claude_opening: String,
    },
    Final {
        current_question: String,
        codex_opening: String,
        claude_opening: String,
        codex_rebuttal: String,
        claude_rebuttal: String,
    },
}

async fn active_run_ids(state: &Arc<Mutex<DebateControlState>>) -> Vec<InferenceRunId> {
    state.lock().await.active_runs.keys().copied().collect()
}

async fn cancel_runs(
    bus: &DebateBus,
    session: &SessionContext<DebateSchema>,
    run_ids: Vec<InferenceRunId>,
    cause: Cancellation<DebateSchema>,
) -> Result<(), DebateAppError> {
    for run_id in run_ids {
        publish::<DebateSchema, _>(
            bus,
            session_stream::<DebateSchema>(session),
            EventVisibility::Internal,
            EventPayload::Execution(ExecutionEvent::Control(ControlEvent::CancelRequested {
                session_id: session.session_id,
                run_id: Some(run_id),
                cause: cause.clone(),
            })),
        )
        .await?;
    }

    Ok(())
}

async fn emit_opening_requests(
    bus: &DebateBus,
    session: &SessionContext<DebateSchema>,
    question: &str,
) -> Result<(), DebateAppError> {
    emit_debate_request(
        bus,
        session,
        Speaker::Codex,
        DebateStage::Opening,
        format!(
            "The user asks: {question}\n\nYou are debating Claude. Give one short opening argument in at most 35 words. Do not browse, inspect files, or use tools."
        ),
    )
    .await?;
    emit_debate_request(
        bus,
        session,
        Speaker::Claude,
        DebateStage::Opening,
        format!(
            "The user asks: {question}\n\nYou are debating Codex. Give one short opening argument in at most 35 words. Do not browse, inspect files, or use tools."
        ),
    )
    .await
}

async fn handle_new_question(
    worker: &DebateControl,
    bus: &DebateBus,
    question: String,
) -> Result<(), DebateAppError> {
    let run_ids = active_run_ids(&worker.state).await;
    cancel_runs(
        bus,
        &worker.session,
        run_ids,
        Cancellation::SupersededByNewInput,
    )
    .await?;

    let mut state = worker.state.lock().await;
    state.current_question = Some(question.clone());
    state.active_runs.clear();
    state.openings.clear();
    state.rebuttals.clear();
    state.final_started = false;
    drop(state);

    emit_opening_requests(bus, &worker.session, &question).await
}

async fn handle_interrupt(worker: &DebateControl, bus: &DebateBus) -> Result<(), DebateAppError> {
    let run_ids = active_run_ids(&worker.state).await;
    cancel_runs(bus, &worker.session, run_ids, Cancellation::UserInterrupted).await
}

async fn handle_run_started(
    worker: &DebateControl,
    run_id: InferenceRunId,
    directive: DebateDirective,
) {
    worker.state.lock().await.active_runs.insert(
        run_id,
        RunInfo {
            speaker: directive.speaker,
            stage: directive.stage,
            text: String::new(),
        },
    );
}

async fn handle_run_output(worker: &DebateControl, run_id: InferenceRunId, output: DebateChunk) {
    if let Some(run) = worker.state.lock().await.active_runs.get_mut(&run_id) {
        run.text.push_str(&output.text);
    }
}

fn stage_completion_requests(
    state: &mut DebateControlState,
    finished: &RunInfo,
) -> Option<PendingDebateRequests> {
    match finished.stage {
        DebateStage::Opening => {
            state
                .openings
                .insert(finished.speaker, finished.text.clone());
            if state.openings.contains_key(&Speaker::Codex)
                && state.openings.contains_key(&Speaker::Claude)
                && state.rebuttals.is_empty()
            {
                return Some(PendingDebateRequests::Rebuttals {
                    codex_opening: state
                        .openings
                        .get(&Speaker::Codex)
                        .cloned()
                        .unwrap_or_default(),
                    claude_opening: state
                        .openings
                        .get(&Speaker::Claude)
                        .cloned()
                        .unwrap_or_default(),
                });
            }
        }
        DebateStage::Rebuttal => {
            state
                .rebuttals
                .insert(finished.speaker, finished.text.clone());
            if state.rebuttals.contains_key(&Speaker::Codex)
                && state.rebuttals.contains_key(&Speaker::Claude)
                && !state.final_started
            {
                state.final_started = true;
                return Some(PendingDebateRequests::Final {
                    current_question: state.current_question.clone().unwrap_or_default(),
                    codex_opening: state
                        .openings
                        .get(&Speaker::Codex)
                        .cloned()
                        .unwrap_or_default(),
                    claude_opening: state
                        .openings
                        .get(&Speaker::Claude)
                        .cloned()
                        .unwrap_or_default(),
                    codex_rebuttal: state
                        .rebuttals
                        .get(&Speaker::Codex)
                        .cloned()
                        .unwrap_or_default(),
                    claude_rebuttal: state
                        .rebuttals
                        .get(&Speaker::Claude)
                        .cloned()
                        .unwrap_or_default(),
                });
            }
        }
        DebateStage::Final => {}
    }

    None
}

async fn dispatch_pending_debate_requests(
    bus: &DebateBus,
    session: &SessionContext<DebateSchema>,
    requests: Option<PendingDebateRequests>,
) -> Result<(), DebateAppError> {
    match requests {
        Some(PendingDebateRequests::Rebuttals {
            codex_opening,
            claude_opening,
        }) => {
            emit_debate_request(
                bus,
                session,
                Speaker::Codex,
                DebateStage::Rebuttal,
                format!(
                    "Claude's opening:\n{claude_opening}\n\nGive one short rebuttal in at most 35 words. Defend your answer directly. Do not browse, inspect files, or use tools."
                ),
            )
            .await?;
            emit_debate_request(
                bus,
                session,
                Speaker::Claude,
                DebateStage::Rebuttal,
                format!(
                    "Codex's opening:\n{codex_opening}\n\nGive one short rebuttal in at most 35 words. Defend your answer directly. Do not browse, inspect files, or use tools."
                ),
            )
            .await?;
        }
        Some(PendingDebateRequests::Final {
            current_question,
            codex_opening,
            claude_opening,
            codex_rebuttal,
            claude_rebuttal,
        }) => {
            emit_debate_request(
                bus,
                session,
                Speaker::Moderator,
                DebateStage::Final,
                format!(
                    "You are the moderator. User question: {current_question}\n\nCodex opening:\n{codex_opening}\n\nClaude opening:\n{claude_opening}\n\nCodex rebuttal:\n{codex_rebuttal}\n\nClaude rebuttal:\n{claude_rebuttal}\n\nGive a final answer in at most 2 short sentences. Say which side was stronger and why. Do not browse, inspect files, or use tools."
                ),
            )
            .await?;
        }
        None => {}
    }

    Ok(())
}

async fn handle_run_finished(
    worker: &DebateControl,
    bus: &DebateBus,
    run_id: InferenceRunId,
) -> Result<(), DebateAppError> {
    let finished = worker.state.lock().await.active_runs.remove(&run_id);
    let Some(finished) = finished else {
        return Ok(());
    };

    let requests = {
        let mut state = worker.state.lock().await;
        stage_completion_requests(&mut state, &finished)
    };

    dispatch_pending_debate_requests(bus, &worker.session, requests).await
}

async fn handle_debate_control_event(
    worker: &DebateControl,
    bus: &DebateBus,
    payload: EventPayload<DebateSchema>,
) -> Result<(), DebateAppError> {
    match payload {
        EventPayload::Interaction(InteractionEvent::InputCommitted {
            input: question, ..
        }) => {
            handle_new_question(worker, bus, question).await?;
        }
        EventPayload::Interaction(InteractionEvent::InputInterrupted { .. }) => {
            handle_interrupt(worker, bus).await?;
        }
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Started {
            run_id,
            directive,
            ..
        })) => {
            handle_run_started(worker, run_id, directive).await;
        }
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
            run_id,
            output,
            ..
        })) => {
            handle_run_output(worker, run_id, output).await;
        }
        EventPayload::Execution(ExecutionEvent::Inference(
            InferenceEvent::Completed { run_id, .. }
            | InferenceEvent::Cancelled { run_id, .. }
            | InferenceEvent::Failed { run_id, .. },
        )) => {
            handle_run_finished(worker, bus, run_id).await?;
        }
        _ => {}
    }

    Ok(())
}

impl BusWorker<DebateSchema, DebateBus> for DebateControl {
    type Error = DebateAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: DebateBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;

            while let Some(event) = next_event(&mut events).await? {
                handle_debate_control_event(&self, bus, event.payload).await?;
            }

            Ok(())
        })
    }
}

async fn emit_debate_request(
    bus: &DebateBus,
    session: &SessionContext<DebateSchema>,
    speaker: Speaker,
    stage: DebateStage,
    prompt: String,
) -> Result<(), DebateAppError> {
    publish::<DebateSchema, _>(
        bus,
        session_stream::<DebateSchema>(session),
        EventVisibility::Internal,
        EventPayload::Execution(ExecutionEvent::Control(ControlEvent::Requested {
            request_id: DebateSchema::next_inference_request_id(),
            session_id: session.session_id,
            thread_id: session.thread_id,
            turn_id: None,
            directive: DebateDirective {
                speaker,
                stage,
                prompt,
            },
            supersedes: None,
        })),
    )
    .await
}

#[derive(Default)]
struct BridgeState {
    current_run_id: Option<InferenceRunId>,
    current_speaker: Option<Speaker>,
    next_sequence: u64,
    last_snapshot: String,
}

#[must_use]
#[derive(Clone)]
pub struct ClaudeDebater<B> {
    worker_id: WorkerId,
    session: SessionContext<DebateSchema>,
    bridge: B,
    state: Arc<Mutex<BridgeState>>,
}

impl<B> ClaudeDebater<B> {
    pub fn new(session: SessionContext<DebateSchema>, bridge: B) -> Self {
        Self {
            worker_id: WorkerId::from("claude-debater"),
            session,
            bridge,
            state: Arc::new(Mutex::new(BridgeState::default())),
        }
    }
}

impl<B> Worker for ClaudeDebater<B>
where
    B: ClaudeBridgeLike,
{
    type WorkerId = WorkerId;
    type Subscription = DebateSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<DebateSchema>(&self.session)
    }
}

fn format_bridge_exit(label: &str, code: Option<i32>) -> String {
    format!(
        "{label} exited{}",
        code.map(|value| format!(" with code {value}"))
            .unwrap_or_default()
    )
}

async fn handle_claude_debater_control_event<B>(
    worker: &ClaudeDebater<B>,
    bus: &DebateBus,
    payload: EventPayload<DebateSchema>,
) -> Result<(), DebateAppError>
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
            if directive.speaker != Speaker::Claude {
                return Ok(());
            }

            let run_id = DebateSchema::next_inference_run_id();
            {
                let mut state = worker.state.lock().await;
                state.current_run_id = Some(run_id);
                state.current_speaker = Some(Speaker::Claude);
                state.next_sequence = 0;
                state.last_snapshot.clear();
            }
            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(&worker.session),
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
                publish_worker_error::<DebateSchema, _>(
                    bus,
                    &worker.worker_id,
                    &worker.session,
                    "bridge_error",
                    error.to_string(),
                )
                .await?;
            }
        }
        EventPayload::Execution(ExecutionEvent::Control(ControlEvent::CancelRequested {
            run_id,
            ..
        })) => {
            let active = worker.state.lock().await.current_run_id;
            if run_id.is_none() || active == run_id {
                let _ = worker.bridge.interrupt().await;
                if let Some(active) = active {
                    publish::<DebateSchema, _>(
                        bus,
                        session_stream::<DebateSchema>(&worker.session),
                        EventVisibility::Internal,
                        EventPayload::Execution(ExecutionEvent::Inference(
                            InferenceEvent::Cancelled {
                                run_id: active,
                                cause: Cancellation::UserInterrupted,
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

async fn handle_claude_debater_bridge_event<B>(
    worker: &ClaudeDebater<B>,
    bus: &DebateBus,
    event: std::result::Result<ClaudeBridgeEvent, broadcast::error::RecvError>,
) -> Result<bool, DebateAppError>
where
    B: ClaudeBridgeLike,
{
    match event {
        Ok(ClaudeBridgeEvent::Ready { .. } | ClaudeBridgeEvent::ToolCallRequested { .. }) => {}
        Ok(ClaudeBridgeEvent::SdkMessage { message }) => {
            handle_claude_message(bus, &worker.session, &worker.state, message).await?;
        }
        Ok(ClaudeBridgeEvent::BridgeError { message }) => {
            publish_worker_error::<DebateSchema, _>(
                bus,
                &worker.worker_id,
                &worker.session,
                "bridge_error",
                message,
            )
            .await?;
        }
        Ok(ClaudeBridgeEvent::Stderr { line }) => warn!("claude bridge stderr: {line}"),
        Ok(ClaudeBridgeEvent::Exited { code }) => {
            publish_worker_error::<DebateSchema, _>(
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
            warn!("claude bridge lagged by {skipped}");
        }
        Err(broadcast::error::RecvError::Closed) => return Ok(true),
    }

    Ok(false)
}

impl<B> BusWorker<DebateSchema, DebateBus> for ClaudeDebater<B>
where
    B: ClaudeBridgeLike,
{
    type Error = DebateAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: DebateBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;
            let mut bridge_events = self.bridge.subscribe();

            loop {
                tokio::select! {
                    maybe_event = next_event(&mut events) => {
                        let Some(event) = maybe_event? else { break; };
                        handle_claude_debater_control_event(&self, bus, event.payload).await?;
                    }
                    bridge_event = bridge_events.recv() => {
                        if handle_claude_debater_bridge_event(&self, bus, bridge_event).await? {
                            break;
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

#[must_use]
#[derive(Clone)]
pub struct CodexDebater<B> {
    worker_id: WorkerId,
    session: SessionContext<DebateSchema>,
    bridge: B,
    state: Arc<Mutex<BridgeState>>,
}

impl<B> CodexDebater<B> {
    pub fn new(session: SessionContext<DebateSchema>, bridge: B) -> Self {
        Self {
            worker_id: WorkerId::from("codex-debater"),
            session,
            bridge,
            state: Arc::new(Mutex::new(BridgeState::default())),
        }
    }
}

impl<B> Worker for CodexDebater<B>
where
    B: CodexBridgeLike,
{
    type WorkerId = WorkerId;
    type Subscription = DebateSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<DebateSchema>(&self.session)
    }
}

async fn handle_codex_debater_control_event<B>(
    worker: &CodexDebater<B>,
    bus: &DebateBus,
    payload: EventPayload<DebateSchema>,
) -> Result<(), DebateAppError>
where
    B: CodexBridgeLike,
{
    match payload {
        EventPayload::Execution(ExecutionEvent::Control(ControlEvent::Requested {
            request_id,
            session_id,
            thread_id,
            directive,
            ..
        })) if session_id == worker.session.session_id
            && matches!(directive.speaker, Speaker::Codex | Speaker::Moderator) =>
        {
            let run_id = DebateSchema::next_inference_run_id();
            {
                let mut state = worker.state.lock().await;
                state.current_run_id = Some(run_id);
                state.current_speaker = Some(directive.speaker);
                state.next_sequence = 0;
                state.last_snapshot.clear();
            }
            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(&worker.session),
                EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Started {
                    run_id,
                    request_id,
                    session_id,
                    thread_id,
                    directive: directive.clone(),
                    engine: EngineId::from("codex-sdk"),
                })),
            )
            .await?;
            if let Err(error) = worker.bridge.send_user_text(directive.prompt).await {
                publish_worker_error::<DebateSchema, _>(
                    bus,
                    &worker.worker_id,
                    &worker.session,
                    "bridge_error",
                    error.to_string(),
                )
                .await?;
            }
        }
        EventPayload::Execution(ExecutionEvent::Control(ControlEvent::CancelRequested {
            run_id,
            ..
        })) => {
            let active = worker.state.lock().await.current_run_id;
            if run_id.is_none() || active == run_id {
                let _ = worker.bridge.interrupt().await;
                if let Some(active) = active {
                    publish::<DebateSchema, _>(
                        bus,
                        session_stream::<DebateSchema>(&worker.session),
                        EventVisibility::Internal,
                        EventPayload::Execution(ExecutionEvent::Inference(
                            InferenceEvent::Cancelled {
                                run_id: active,
                                cause: Cancellation::UserInterrupted,
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

async fn handle_codex_debater_bridge_event<B>(
    worker: &CodexDebater<B>,
    bus: &DebateBus,
    event: std::result::Result<CodexBridgeEvent, broadcast::error::RecvError>,
) -> Result<bool, DebateAppError>
where
    B: CodexBridgeLike,
{
    match event {
        Ok(CodexBridgeEvent::Ready) => {}
        Ok(CodexBridgeEvent::ThreadEvent { event }) => {
            handle_codex_message(bus, &worker.session, &worker.state, event).await?;
        }
        Ok(CodexBridgeEvent::BridgeError { message }) => {
            publish_worker_error::<DebateSchema, _>(
                bus,
                &worker.worker_id,
                &worker.session,
                "bridge_error",
                message,
            )
            .await?;
        }
        Ok(CodexBridgeEvent::Stderr { line }) => warn!("codex bridge stderr: {line}"),
        Ok(CodexBridgeEvent::Exited { code }) => {
            publish_worker_error::<DebateSchema, _>(
                bus,
                &worker.worker_id,
                &worker.session,
                "bridge_error",
                format_bridge_exit("codex bridge", code),
            )
            .await?;
            return Ok(true);
        }
        Err(broadcast::error::RecvError::Lagged(skipped)) => {
            warn!("codex bridge lagged by {skipped}");
        }
        Err(broadcast::error::RecvError::Closed) => return Ok(true),
    }

    Ok(false)
}

impl<B> BusWorker<DebateSchema, DebateBus> for CodexDebater<B>
where
    B: CodexBridgeLike,
{
    type Error = DebateAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: DebateBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;
            let mut bridge_events = self.bridge.subscribe();

            loop {
                tokio::select! {
                    maybe_event = next_event(&mut events) => {
                        let Some(event) = maybe_event? else { break; };
                        handle_codex_debater_control_event(&self, bus, event.payload).await?;
                    }
                    bridge_event = bridge_events.recv() => {
                        if handle_codex_debater_bridge_event(&self, bus, bridge_event).await? {
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
    open_status_id: Option<StatusId>,
    active_runs: usize,
}

#[must_use]
#[derive(Clone)]
pub struct DebateStatusWorker {
    worker_id: WorkerId,
    session: SessionContext<DebateSchema>,
    state: Arc<Mutex<StatusState>>,
}

impl DebateStatusWorker {
    pub fn new(session: SessionContext<DebateSchema>) -> Self {
        Self {
            worker_id: WorkerId::from("debate-status"),
            session,
            state: Arc::new(Mutex::new(StatusState::default())),
        }
    }
}

impl Worker for DebateStatusWorker {
    type WorkerId = WorkerId;
    type Subscription = DebateSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }
    fn subscription(&self) -> Self::Subscription {
        session_subscription::<DebateSchema>(&self.session)
    }
}

fn debate_status_text(stage: DebateStage) -> String {
    match stage {
        DebateStage::Opening => "opening arguments...",
        DebateStage::Rebuttal => "rebuttals...",
        DebateStage::Final => "final synthesis...",
    }
    .to_string()
}

async fn handle_debate_status_event(
    worker: &DebateStatusWorker,
    bus: &DebateBus,
    payload: EventPayload<DebateSchema>,
) -> Result<(), DebateAppError> {
    match payload {
        EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Started {
            directive,
            run_id,
            ..
        })) => {
            let mut state = worker.state.lock().await;
            state.active_runs += 1;
            let status_text = debate_status_text(directive.stage);

            if let Some(status_id) = state.open_status_id {
                publish::<DebateSchema, _>(
                    bus,
                    session_stream::<DebateSchema>(&worker.session),
                    EventVisibility::Both,
                    EventPayload::Presentation(PresentationEvent::Status(StatusEvent::Updated {
                        status_id,
                        sequence: state.active_runs as u64,
                        status: status_text,
                    })),
                )
                .await?;
            } else {
                let status_id = DebateSchema::next_status_id();
                state.open_status_id = Some(status_id);
                publish::<DebateSchema, _>(
                    bus,
                    session_stream::<DebateSchema>(&worker.session),
                    EventVisibility::Both,
                    EventPayload::Presentation(PresentationEvent::Status(StatusEvent::Opened {
                        status_id,
                        session_id: worker.session.session_id,
                        run_id: Some(run_id),
                        status: status_text,
                    })),
                )
                .await?;
            }
        }
        EventPayload::Execution(ExecutionEvent::Inference(
            InferenceEvent::Completed { .. }
            | InferenceEvent::Cancelled { .. }
            | InferenceEvent::Failed { .. },
        )) => {
            let mut state = worker.state.lock().await;
            if state.active_runs > 0 {
                state.active_runs -= 1;
            }
            if state.active_runs == 0
                && let Some(status_id) = state.open_status_id.take()
            {
                publish::<DebateSchema, _>(
                    bus,
                    session_stream::<DebateSchema>(&worker.session),
                    EventVisibility::Both,
                    EventPayload::Presentation(PresentationEvent::Status(StatusEvent::Closed {
                        status_id,
                    })),
                )
                .await?;
            }
        }
        _ => {}
    }

    Ok(())
}

impl BusWorker<DebateSchema, DebateBus> for DebateStatusWorker {
    type Error = DebateAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: DebateBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;
            while let Some(event) = next_event(&mut events).await? {
                handle_debate_status_event(&self, bus, event.payload).await?;
            }
            Ok(())
        })
    }
}

#[must_use]
#[derive(Clone)]
pub struct DebateProjector {
    worker_id: WorkerId,
    session: SessionContext<DebateSchema>,
}

impl DebateProjector {
    pub fn new(session: SessionContext<DebateSchema>) -> Self {
        Self {
            worker_id: WorkerId::from("debate-projector"),
            session,
        }
    }
}

impl Worker for DebateProjector {
    type WorkerId = WorkerId;
    type Subscription = DebateSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }
    fn subscription(&self) -> Self::Subscription {
        session_subscription::<DebateSchema>(&self.session)
    }
}

async fn project_debate_status(
    worker: &DebateProjector,
    bus: &DebateBus,
    status: StatusEvent<DebateSchema>,
) -> Result<(), DebateAppError> {
    match status {
        StatusEvent::Opened {
            status_id, status, ..
        } => {
            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(&worker.session),
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
            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(&worker.session),
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
            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(&worker.session),
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

async fn project_debate_event(
    worker: &DebateProjector,
    bus: &DebateBus,
    payload: EventPayload<DebateSchema>,
) -> Result<(), DebateAppError> {
    match payload {
        EventPayload::Interaction(InteractionEvent::InputDelta {
            stream_id,
            revision_id,
            input,
            stability,
            ..
        }) => {
            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(&worker.session),
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
            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(&worker.session),
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
            project_debate_status(worker, bus, status).await?;
        }
        EventPayload::Error(error_event)
            if error_event.stream == session_stream::<DebateSchema>(&worker.session) =>
        {
            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(&worker.session),
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

impl BusWorker<DebateSchema, DebateBus> for DebateProjector {
    type Error = DebateAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: DebateBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;
            while let Some(event) = next_event(&mut events).await? {
                project_debate_event(&self, bus, event.payload).await?;
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
    type Subscription = DebateSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }
    fn subscription(&self) -> Self::Subscription {
        all_subscription::<DebateSchema>()
    }
}

impl BusWorker<DebateSchema, DebateBus> for NoopToolsWorker {
    type Error = DebateAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, _bus: DebateBus) -> Self::Run {
        Box::pin(async { std::future::pending::<Result<(), DebateAppError>>().await })
    }
}

pub type DebateInferenceGroup<C, O> =
    ConcurrentBusWorkers<WorkerId, ClaudeDebater<C>, CodexDebater<O>>;
pub type DebatePresentationGroup =
    ConcurrentBusWorkers<WorkerId, DebateStatusWorker, DebateProjector>;
pub type DebateRuntime<C, O> = ExampleRuntime<
    DebateSchema,
    DebateAppError,
    DebateBus,
    BrowserIngress,
    BrowserEgress,
    DebateControl,
    DebateInferenceGroup<C, O>,
    NoopToolsWorker,
    DebatePresentationGroup,
>;

#[must_use]
#[derive(Clone)]
pub struct AppState<C, O>
where
    C: ClaudeBridgeLike,
    O: CodexBridgeLike,
{
    runtime: Arc<DebateRuntime<C, O>>,
}

pub fn browser_router<C, O>(runtime: Arc<DebateRuntime<C, O>>) -> Router
where
    C: ClaudeBridgeLike,
    O: CodexBridgeLike,
{
    Router::new()
        .route("/", get(index))
        .route("/api/events", get(events::<C, O>))
        .route("/api/message", post(message::<C, O>))
        .route("/api/interrupt", post(interrupt::<C, O>))
        .with_state(AppState { runtime })
}

#[must_use]
pub fn browser_session() -> SessionContext<DebateSchema> {
    new_session::<DebateSchema>(DebateSurface::Browser)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn events<C, O>(
    State(state): State<AppState<C, O>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<SseEvent, Infallible>>>
where
    C: ClaudeBridgeLike,
    O: CodexBridgeLike,
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

async fn message<C, O>(
    State(state): State<AppState<C, O>>,
    Json(request): Json<MessageRequest>,
) -> impl IntoResponse
where
    C: ClaudeBridgeLike,
    O: CodexBridgeLike,
{
    if state
        .runtime
        .surface()
        .ingress()
        .submit_question(request.text)
        .await
    {
        axum::http::StatusCode::ACCEPTED
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn interrupt<C, O>(State(state): State<AppState<C, O>>) -> impl IntoResponse
where
    C: ClaudeBridgeLike,
    O: CodexBridgeLike,
{
    if state.runtime.surface().ingress().interrupt().await {
        axum::http::StatusCode::ACCEPTED
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn handle_claude_message(
    bus: &DebateBus,
    session: &SessionContext<DebateSchema>,
    state: &Arc<Mutex<BridgeState>>,
    message: Value,
) -> Result<(), DebateAppError> {
    let message_type = message.get("type").and_then(Value::as_str);
    if message_type == Some("stream_event") {
        let mut state = state.lock().await;
        if let (Some(run_id), Some(speaker)) = (state.current_run_id, state.current_speaker)
            && let Some(delta) = extract_claude_delta(&message)
        {
            state.last_snapshot.push_str(&delta);
            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(session),
                EventVisibility::Both,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
                    run_id,
                    sequence: state.next_sequence,
                    output: DebateChunk {
                        speaker,
                        text: delta,
                    },
                })),
            )
            .await?;
            state.next_sequence += 1;
        }
    } else if message_type == Some("result") {
        let mut state = state.lock().await;
        if let Some(run_id) = state.current_run_id {
            publish::<DebateSchema, _>(
                bus,
                session_stream::<DebateSchema>(session),
                EventVisibility::Internal,
                EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Completed {
                    run_id,
                    result: mango_core::agent::Completion::Completed,
                })),
            )
            .await?;
            state.current_run_id = None;
            state.current_speaker = None;
            state.next_sequence = 0;
            state.last_snapshot.clear();
        }
    }
    Ok(())
}

async fn handle_codex_message(
    bus: &DebateBus,
    session: &SessionContext<DebateSchema>,
    state: &Arc<Mutex<BridgeState>>,
    event: Value,
) -> Result<(), DebateAppError> {
    let event_type = event.get("type").and_then(Value::as_str);
    match event_type {
        Some("item.started" | "item.updated" | "item.completed") => {
            let item = event.get("item");
            if item
                .and_then(|value| value.get("type"))
                .and_then(Value::as_str)
                == Some("agent_message")
            {
                let snapshot = item
                    .and_then(|value| value.get("text"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let mut state = state.lock().await;
                if let (Some(run_id), Some(speaker)) = (state.current_run_id, state.current_speaker)
                    && let Some(delta) = incremental_suffix(&state.last_snapshot, &snapshot)
                {
                    publish::<DebateSchema, _>(
                        bus,
                        session_stream::<DebateSchema>(session),
                        EventVisibility::Both,
                        EventPayload::Execution(ExecutionEvent::Inference(
                            InferenceEvent::Output {
                                run_id,
                                sequence: state.next_sequence,
                                output: DebateChunk {
                                    speaker,
                                    text: delta,
                                },
                            },
                        )),
                    )
                    .await?;
                    state.next_sequence += 1;
                    state.last_snapshot = snapshot;
                }
            }
        }
        Some("turn.completed") => {
            let mut state = state.lock().await;
            if let Some(run_id) = state.current_run_id {
                publish::<DebateSchema, _>(
                    bus,
                    session_stream::<DebateSchema>(session),
                    EventVisibility::Internal,
                    EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Completed {
                        run_id,
                        result: mango_core::agent::Completion::Completed,
                    })),
                )
                .await?;
                state.current_run_id = None;
                state.current_speaker = None;
                state.next_sequence = 0;
                state.last_snapshot.clear();
            }
        }
        Some("turn.failed" | "error") => {
            let mut state = state.lock().await;
            if let Some(run_id) = state.current_run_id {
                publish::<DebateSchema, _>(
                    bus,
                    session_stream::<DebateSchema>(session),
                    EventVisibility::Internal,
                    EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Failed {
                        run_id,
                        error: error_descriptor("codex_failed", "codex debate turn failed"),
                    })),
                )
                .await?;
                state.current_run_id = None;
                state.current_speaker = None;
                state.next_sequence = 0;
                state.last_snapshot.clear();
            }
        }
        _ => {}
    }
    Ok(())
}

fn extract_claude_delta(message: &Value) -> Option<String> {
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

fn ui_event_from_event(
    event: &mango_core::agent::Event<DebateSchema>,
    session: &SessionContext<DebateSchema>,
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
            } if session_id == &session.session_id => Some(UiEvent::DebateToken {
                speaker: output.speaker,
                text: output.text.clone(),
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

#[must_use]
#[derive(Debug, Clone)]
pub struct FakeClaudeBridge {
    events: broadcast::Sender<ClaudeBridgeEvent>,
}

impl FakeClaudeBridge {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(128);
        Self { events }
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
        let reply = if text.contains("rebuttal") {
            "Claude rebuttal."
        } else {
            "Claude opening."
        };
        let _ = self.events.send(ClaudeBridgeEvent::SdkMessage {
            message: json!({
                "type": "stream_event",
                "event": {
                    "type": "content_block_delta",
                    "delta": { "type": "text_delta", "text": reply }
                }
            }),
        });
        let _ = self.events.send(ClaudeBridgeEvent::SdkMessage {
            message: json!({
                "type": "result",
                "result": reply,
                "is_error": false
            }),
        });
        Ok(())
    }

    async fn interrupt(&self) -> Result<()> {
        Ok(())
    }
    fn subscribe(&self) -> broadcast::Receiver<ClaudeBridgeEvent> {
        self.events.subscribe()
    }
}

#[must_use]
#[derive(Debug, Clone)]
pub struct FakeCodexBridge {
    events: broadcast::Sender<CodexBridgeEvent>,
}

impl FakeCodexBridge {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(128);
        Self { events }
    }
}

impl Default for FakeCodexBridge {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CodexBridgeLike for FakeCodexBridge {
    async fn send_user_text(&self, text: String) -> Result<()> {
        let reply = if text.contains("moderator") || text.contains("best final answer") {
            "Final answer."
        } else if text.contains("rebuttal") {
            "Codex rebuttal."
        } else {
            "Codex opening."
        };
        let _ = self.events.send(CodexBridgeEvent::ThreadEvent {
            event: json!({
                "type": "item.updated",
                "item": {
                    "type": "agent_message",
                    "id": "msg-1",
                    "text": reply
                }
            }),
        });
        let _ = self.events.send(CodexBridgeEvent::ThreadEvent {
            event: json!({ "type": "turn.completed", "usage": { "input_tokens": 1, "cached_input_tokens": 0, "output_tokens": 1 } }),
        });
        Ok(())
    }

    async fn interrupt(&self) -> Result<()> {
        Ok(())
    }
    fn subscribe(&self) -> broadcast::Receiver<CodexBridgeEvent> {
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
            .expect("ui channel closed")
    }

    #[tokio::test]
    async fn debate_runtime_streams_both_sides_and_final_answer() {
        let session = browser_session();
        let runtime = Arc::new(DebateRuntime::new(
            ExampleSubstrate::new(DebateBus::new(256), DebateControl::new(session.clone())),
            ExampleSurface::new(
                BrowserIngress::new(),
                BrowserEgress::new(256),
                ConcurrentBusWorkers::new(
                    "presentation",
                    DebateStatusWorker::new(session.clone()),
                    DebateProjector::new(session.clone()),
                ),
            ),
            ExampleBridge::new(
                ConcurrentBusWorkers::new(
                    "inference",
                    ClaudeDebater::new(session.clone(), FakeClaudeBridge::new()),
                    CodexDebater::new(session.clone(), FakeCodexBridge::new()),
                ),
                NoopToolsWorker::new(),
            ),
        ));

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
                .submit_question("What is the best language for a local agent runtime?".to_string())
                .await
        );

        let mut seen_codex = false;
        let mut seen_claude = false;
        let mut seen_final = false;
        let mut seen_status_clear = false;

        for _ in 0..24 {
            match next_ui_event(&mut ui_events).await {
                UiEvent::DebateToken {
                    speaker: Speaker::Codex,
                    ..
                } => seen_codex = true,
                UiEvent::DebateToken {
                    speaker: Speaker::Claude,
                    ..
                } => seen_claude = true,
                UiEvent::DebateToken {
                    speaker: Speaker::Moderator,
                    text,
                } if text.contains("Final answer.") => seen_final = true,
                UiEvent::StatusClear => {
                    seen_status_clear = true;
                    if seen_codex && seen_claude && seen_final {
                        break;
                    }
                }
                _ => {}
            }
        }

        assert!(seen_codex);
        assert!(seen_claude);
        assert!(seen_final);
        assert!(seen_status_clear);

        task.abort();
    }
}
