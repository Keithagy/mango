use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use mango_core::agent::{
    AgentIds, AgentSchema, Event, EventBus, EventPayload, ExecutionEvent, Filter, InferenceEvent,
    InteractionEvent, SessionCloseReason, SessionContext, SessionWorker, StatusEvent, Subscription,
    Worker,
};
use mango_runtime_support::{BoxFuture, next_event, publish};
use tokio::sync::{Mutex, mpsc};

use crate::{
    TelegramClient, TelegramInboundMessage, TelegramOutboundMessage, TelegramSessionSurface,
};

#[derive(Debug, Clone)]
pub struct TelegramInputTurn<S: AgentSchema> {
    pub kind: S::InputKind,
    pub input: S::Input,
}

pub trait TelegramIngressMapper<S: AgentSchema>: Clone + Send + Sync + 'static {
    fn map_message(&self, message: &TelegramInboundMessage) -> Option<TelegramInputTurn<S>>;
}

#[derive(Debug, Clone)]
pub struct PlainTextTelegramInputMapper<K> {
    kind: K,
}

impl<K> PlainTextTelegramInputMapper<K> {
    pub fn new(kind: K) -> Self {
        Self { kind }
    }
}

impl<S> TelegramIngressMapper<S> for PlainTextTelegramInputMapper<S::InputKind>
where
    S: AgentSchema<Input = String>,
    S::InputKind: Clone + Send + Sync + 'static,
{
    fn map_message(&self, message: &TelegramInboundMessage) -> Option<TelegramInputTurn<S>> {
        Some(TelegramInputTurn {
            kind: self.kind.clone(),
            input: message.text.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TelegramInboxSender {
    inner: mpsc::Sender<TelegramInboundMessage>,
}

impl TelegramInboxSender {
    /// Queue a Telegram message for a session ingress worker.
    ///
    /// # Errors
    ///
    /// Returns an error if the receiving side has already been dropped.
    pub async fn send(
        &self,
        message: TelegramInboundMessage,
    ) -> Result<(), mpsc::error::SendError<TelegramInboundMessage>> {
        self.inner.send(message).await
    }
}

#[derive(Debug, Clone)]
pub struct TelegramInbox {
    inner: Arc<Mutex<mpsc::Receiver<TelegramInboundMessage>>>,
}

impl TelegramInbox {
    pub async fn recv(&self) -> Option<TelegramInboundMessage> {
        self.inner.lock().await.recv().await
    }
}

#[must_use]
pub fn telegram_inbox(capacity: usize) -> (TelegramInboxSender, TelegramInbox) {
    let (tx, rx) = mpsc::channel(capacity);
    (
        TelegramInboxSender { inner: tx },
        TelegramInbox {
            inner: Arc::new(Mutex::new(rx)),
        },
    )
}

#[derive(Debug, Clone)]
pub struct TelegramIngress<S, M>
where
    S: AgentSchema,
{
    worker_id: S::WorkerId,
    inbox: TelegramInbox,
    mapper: M,
    marker: PhantomData<fn() -> S>,
}

impl<S, M> TelegramIngress<S, M>
where
    S: AgentSchema,
{
    pub fn new(worker_id: S::WorkerId, inbox: TelegramInbox, mapper: M) -> Self {
        Self {
            worker_id,
            inbox,
            mapper,
            marker: PhantomData,
        }
    }
}

impl<S, M> Worker for TelegramIngress<S, M>
where
    S: AgentSchema + Clone + Send + Sync + 'static,
    S::Surface: Send + Sync + 'static,
    S::InputKind: Send + Sync + 'static,
    S::Input: Send + Sync + 'static,
    S::InterruptDetail: Send + Sync + 'static,
    S::Directive: Send + Sync + 'static,
    S::Output: Send + Sync + 'static,
    S::ToolData: Send + Sync + 'static,
    S::Status: Send + Sync + 'static,
    S::CancellationDetail: Send + Sync + 'static,
    S::CompletionDetail: Send + Sync + 'static,
    S::EngineId: Send + Sync + 'static,
    S::ToolName: Send + Sync + 'static,
    S::SessionId: Send + Sync + 'static,
    S::ThreadId: Send + Sync + 'static,
    S::TurnId: Send + Sync + 'static,
    S::InputStreamId: Send + Sync + 'static,
    S::RevisionId: Send + Sync + 'static,
    S::InferenceRequestId: Send + Sync + 'static,
    S::InferenceRunId: Send + Sync + 'static,
    S::StatusId: Send + Sync + 'static,
    S::ToolCallId: Send + Sync + 'static,
    S::EventId: Send + Sync + 'static,
    S::WorkerId: Send + Sync + 'static,
    S::WorkerId: Clone,
{
    type WorkerId = S::WorkerId;
    type Subscription = Subscription<S>;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        Subscription::all()
    }
}

impl<S, B, M> SessionWorker<S, B> for TelegramIngress<S, M>
where
    S: AgentSchema + Clone + Send + Sync + 'static,
    S::Surface: Send + Sync + 'static,
    S::InputKind: Send + Sync + 'static,
    S::Input: Send + Sync + 'static,
    S::InterruptDetail: Send + Sync + 'static,
    S::Directive: Send + Sync + 'static,
    S::Output: Send + Sync + 'static,
    S::ToolData: Send + Sync + 'static,
    S::Status: Send + Sync + 'static,
    S::CancellationDetail: Send + Sync + 'static,
    S::CompletionDetail: Send + Sync + 'static,
    S::EngineId: Send + Sync + 'static,
    S::ToolName: Send + Sync + 'static,
    S::SessionId: Send + Sync + 'static,
    S::ThreadId: Send + Sync + 'static,
    S::TurnId: Send + Sync + 'static,
    S::InputStreamId: Send + Sync + 'static,
    S::RevisionId: Send + Sync + 'static,
    S::InferenceRequestId: Send + Sync + 'static,
    S::InferenceRunId: Send + Sync + 'static,
    S::StatusId: Send + Sync + 'static,
    S::ToolCallId: Send + Sync + 'static,
    S::EventId: Send + Sync + 'static,
    S::WorkerId: Send + Sync + 'static,
    S::Ids: AgentIds<
            EventId = S::EventId,
            InputStreamId = S::InputStreamId,
            RevisionId = S::RevisionId,
            TurnId = S::TurnId,
        >,
    SessionContext<S>: Send + Sync + 'static,
    Event<S>: Clone + Send + 'static,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>> + Send + Sync + 'static,
    for<'a> B::Publish<'a>: Send,
    M: TelegramIngressMapper<S>,
{
    type Error = B::Error;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: B, session: SessionContext<S>) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;

            publish::<S, _>(
                bus,
                session_stream(&session),
                mango_core::agent::EventVisibility::Internal,
                EventPayload::Interaction(InteractionEvent::SessionOpened {
                    session: session.clone(),
                }),
            )
            .await?;

            while let Some(message) = self.inbox.recv().await {
                let Some(turn) = self.mapper.map_message(&message) else {
                    continue;
                };

                let stream_id = S::next_input_stream_id();
                let revision_id = S::next_revision_id();
                let turn_id = S::next_turn_id();

                publish::<S, _>(
                    bus,
                    session_stream(&session),
                    mango_core::agent::EventVisibility::Internal,
                    EventPayload::Interaction(InteractionEvent::InputStreamOpened {
                        session_id: session.session_id.clone(),
                        thread_id: session.thread_id.clone(),
                        stream_id: stream_id.clone(),
                        kind: turn.kind.clone(),
                    }),
                )
                .await?;
                publish::<S, _>(
                    bus,
                    session_stream(&session),
                    mango_core::agent::EventVisibility::Both,
                    EventPayload::Interaction(InteractionEvent::InputDelta {
                        stream_id: stream_id.clone(),
                        revision_id: revision_id.clone(),
                        sequence: 0,
                        input: turn.input.clone(),
                        stability: mango_core::agent::InputStability::Final,
                    }),
                )
                .await?;
                publish::<S, _>(
                    bus,
                    session_stream(&session),
                    mango_core::agent::EventVisibility::Internal,
                    EventPayload::Interaction(InteractionEvent::InputCommitted {
                        session_id: session.session_id.clone(),
                        thread_id: session.thread_id.clone(),
                        stream_id: stream_id.clone(),
                        revision_id,
                        turn_id,
                        input: turn.input,
                    }),
                )
                .await?;
                publish::<S, _>(
                    bus,
                    session_stream(&session),
                    mango_core::agent::EventVisibility::Internal,
                    EventPayload::Interaction(InteractionEvent::InputStreamClosed {
                        session_id: session.session_id.clone(),
                        thread_id: session.thread_id.clone(),
                        stream_id,
                    }),
                )
                .await?;
            }

            publish::<S, _>(
                bus,
                session_stream(&session),
                mango_core::agent::EventVisibility::Internal,
                EventPayload::Interaction(InteractionEvent::SessionClosed {
                    session_id: session.session_id,
                    thread_id: session.thread_id,
                    reason: SessionCloseReason::Normal,
                }),
            )
            .await?;

            Ok(())
        })
    }
}

pub trait TelegramTextMapper<S: AgentSchema>: Clone + Send + Sync + 'static {
    fn render_output_chunk(&self, output: &S::Output) -> String;

    fn render_status(&self, _status: &S::Status) -> Option<String> {
        None
    }

    fn render_error(&self, error: &mango_core::agent::ErrorDescriptor) -> Option<String> {
        Some(error.message.clone())
    }

    fn finalize_output(&self, output: String) -> Option<String> {
        if output.is_empty() {
            None
        } else {
            Some(output)
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DisplayTelegramTextMapper;

impl<S> TelegramTextMapper<S> for DisplayTelegramTextMapper
where
    S: AgentSchema,
    S::Output: ToString,
{
    fn render_output_chunk(&self, output: &S::Output) -> String {
        output.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct TelegramEgress<S, C, M>
where
    S: AgentSchema,
{
    worker_id: S::WorkerId,
    client: C,
    mapper: M,
    marker: PhantomData<fn() -> S>,
}

impl<S, C, M> TelegramEgress<S, C, M>
where
    S: AgentSchema,
{
    pub fn new(worker_id: S::WorkerId, client: C, mapper: M) -> Self {
        Self {
            worker_id,
            client,
            mapper,
            marker: PhantomData,
        }
    }
}

impl<S, C, M> Worker for TelegramEgress<S, C, M>
where
    S: AgentSchema + Clone + Send + Sync + 'static,
    S::Surface: Send + Sync + 'static,
    S::InputKind: Send + Sync + 'static,
    S::Input: Send + Sync + 'static,
    S::InterruptDetail: Send + Sync + 'static,
    S::Directive: Send + Sync + 'static,
    S::Output: Send + Sync + 'static,
    S::ToolData: Send + Sync + 'static,
    S::Status: Send + Sync + 'static,
    S::CancellationDetail: Send + Sync + 'static,
    S::CompletionDetail: Send + Sync + 'static,
    S::EngineId: Send + Sync + 'static,
    S::ToolName: Send + Sync + 'static,
    S::SessionId: Send + Sync + 'static,
    S::ThreadId: Send + Sync + 'static,
    S::TurnId: Send + Sync + 'static,
    S::InputStreamId: Send + Sync + 'static,
    S::RevisionId: Send + Sync + 'static,
    S::InferenceRequestId: Send + Sync + 'static,
    S::InferenceRunId: Send + Sync + 'static,
    S::StatusId: Send + Sync + 'static,
    S::ToolCallId: Send + Sync + 'static,
    S::EventId: Send + Sync + 'static,
    S::WorkerId: Send + Sync + 'static,
    S::WorkerId: Clone,
{
    type WorkerId = S::WorkerId;
    type Subscription = Subscription<S>;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        Subscription::all()
    }
}

impl<S, B, C, M, E> SessionWorker<S, B> for TelegramEgress<S, C, M>
where
    S: AgentSchema + Clone + Send + Sync + 'static,
    S::Surface: Send + Sync + 'static,
    S::InputKind: Send + Sync + 'static,
    S::Input: Send + Sync + 'static,
    S::InterruptDetail: Send + Sync + 'static,
    S::Directive: Send + Sync + 'static,
    S::Output: Send + Sync + 'static,
    S::ToolData: Send + Sync + 'static,
    S::Status: Send + Sync + 'static,
    S::CancellationDetail: Send + Sync + 'static,
    S::CompletionDetail: Send + Sync + 'static,
    S::EngineId: Send + Sync + 'static,
    S::ToolName: Send + Sync + 'static,
    S::SessionId: Send + Sync + 'static,
    S::ThreadId: Send + Sync + 'static,
    S::TurnId: Send + Sync + 'static,
    S::InputStreamId: Send + Sync + 'static,
    S::RevisionId: Send + Sync + 'static,
    S::InferenceRequestId: Send + Sync + 'static,
    S::InferenceRunId: Send + Sync + 'static,
    S::StatusId: Send + Sync + 'static,
    S::ToolCallId: Send + Sync + 'static,
    S::EventId: Send + Sync + 'static,
    S::WorkerId: Send + Sync + 'static,
    S::Surface: TelegramSessionSurface + Send + Sync + 'static,
    SessionContext<S>: Send + Sync + 'static,
    Event<S>: Clone + Send + 'static,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>
        + Send
        + Sync
        + 'static,
    for<'a> B::Stream<'a>: Send + Unpin,
    C: TelegramClient + Send + Sync + 'static,
    M: TelegramTextMapper<S>,
    E: From<C::Error> + Send + 'static,
{
    type Error = E;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: B, session: SessionContext<S>) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(session_subscription(&session))?;
            let mut buffers = HashMap::<S::InferenceRunId, String>::new();

            while let Some(event) = next_event(&mut events).await? {
                match event.payload {
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Output { run_id, output, .. },
                    )) => {
                        buffers
                            .entry(run_id)
                            .or_default()
                            .push_str(&self.mapper.render_output_chunk(&output));
                    }
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Completed { run_id, .. },
                    )) => {
                        if let Some(buffer) = buffers.remove(&run_id)
                            && let Some(text) = self.mapper.finalize_output(buffer)
                        {
                            send_text(&self.client, &session, text).await?;
                        }
                    }
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Failed { run_id, error },
                    )) => {
                        if let Some(buffer) = buffers.remove(&run_id)
                            && let Some(text) = self.mapper.finalize_output(buffer)
                        {
                            send_text(&self.client, &session, text).await?;
                        }

                        if let Some(text) = self.mapper.render_error(&error) {
                            send_text(&self.client, &session, text).await?;
                        }
                    }
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Cancelled { run_id, .. },
                    )) => {
                        buffers.remove(&run_id);
                    }
                    EventPayload::Presentation(mango_core::agent::PresentationEvent::Status(
                        StatusEvent::Opened { status, .. } | StatusEvent::Updated { status, .. },
                    )) => {
                        if let Some(text) = self.mapper.render_status(&status) {
                            send_text(&self.client, &session, text).await?;
                        }
                    }
                    EventPayload::Error(error_event)
                        if error_event.stream == session_stream(&session) =>
                    {
                        if let Some(text) = self.mapper.render_error(&error_event.error) {
                            send_text(&self.client, &session, text).await?;
                        }
                    }
                    EventPayload::Interaction(InteractionEvent::SessionClosed {
                        session_id,
                        ..
                    }) if session_id == session.session_id => {
                        break;
                    }
                    _ => {}
                }
            }

            Ok(())
        })
    }
}

async fn send_text<S, C, E>(client: &C, session: &SessionContext<S>, text: String) -> Result<(), E>
where
    S: AgentSchema,
    S::Surface: TelegramSessionSurface,
    C: TelegramClient,
    E: From<C::Error>,
{
    client
        .send_message(TelegramOutboundMessage {
            chat_id: session.surface.telegram_chat_id(),
            thread_id: session.surface.telegram_thread_id(),
            reply_to_message_id: None,
            text,
        })
        .await
        .map_err(E::from)
}

fn session_stream<S>(session: &SessionContext<S>) -> mango_core::agent::StreamKey<S>
where
    S: AgentSchema,
{
    mango_core::agent::StreamKey::Session(session.session_id.clone())
}

fn session_subscription<S>(session: &SessionContext<S>) -> Subscription<S>
where
    S: AgentSchema,
{
    Subscription {
        streams: Filter::Only(vec![session_stream(session)]),
        visibility: Filter::Any,
    }
}
