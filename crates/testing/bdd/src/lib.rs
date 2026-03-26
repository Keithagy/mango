//! Gherkin-inspired event-sourced test scenarios for Mango workers.
//!
//! The harness is built around Mango primitives:
//! - real [`EventBus`](mango_core::agent::EventBus) instances
//! - real [`BusWorker`](mango_core::agent::BusWorker) and
//!   [`SessionWorker`](mango_core::agent::SessionWorker) tasks
//! - arbitrary event publication and subscription assertions
//!
//! Tests remain standard Rust tests, so they integrate directly with
//! `cargo test`, IDEs, and agent-led debugging workflows.

use std::{
    fmt::{Debug, Display, Formatter},
    sync::{Arc, Mutex},
    time::Duration,
};

use mango_core::agent::{
    AgentIds, AgentSchema, BusWorker, Event, EventBus, EventPayload, EventVisibility,
    SessionContext, SessionWorker, StreamKey, Subscription,
};
use mango_runtime_support::{next_event, publish};
use tokio::{task::JoinHandle, time::sleep};

const DEFAULT_RECENT_EVENT_LIMIT: usize = 6;
const DEFAULT_SUMMARY_LIMIT: usize = 160;
const EXPECTATION_POLL_INTERVAL: Duration = Duration::from_millis(5);
const STARTUP_SETTLE_DELAY: Duration = Duration::from_millis(10);

type EventSummary<S> = Arc<dyn Fn(&Event<S>) -> String + Send + Sync>;

#[derive(Debug, Clone, Copy)]
enum StepKind {
    Given,
    When,
    Then,
}

impl Display for StepKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Given => f.write_str("Given"),
            Self::When => f.write_str("When"),
            Self::Then => f.write_str("Then"),
        }
    }
}

enum ScenarioIssue {
    PublishFailed {
        message: String,
        recent_events: Vec<String>,
    },
    ExpectationNotMet {
        expectation: String,
        recent_events: Vec<String>,
    },
    WorkerExited {
        label: String,
        recent_events: Vec<String>,
    },
    WorkerFailed {
        label: String,
        message: String,
        recent_events: Vec<String>,
    },
    WorkerPanicked {
        label: String,
        message: String,
        recent_events: Vec<String>,
    },
}

impl Display for ScenarioIssue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PublishFailed {
                message,
                recent_events,
            } => {
                writeln!(f, "publish failed: {message}")?;
                write_recent_events(f, recent_events)
            }
            Self::ExpectationNotMet {
                expectation,
                recent_events,
            } => {
                writeln!(f, "expectation not met: {expectation}")?;
                write_recent_events(f, recent_events)
            }
            Self::WorkerExited {
                label,
                recent_events,
            } => {
                writeln!(f, "worker `{label}` exited unexpectedly")?;
                write_recent_events(f, recent_events)
            }
            Self::WorkerFailed {
                label,
                message,
                recent_events,
            } => {
                writeln!(f, "worker `{label}` returned an error: {message}")?;
                write_recent_events(f, recent_events)
            }
            Self::WorkerPanicked {
                label,
                message,
                recent_events,
            } => {
                writeln!(f, "worker `{label}` panicked: {message}")?;
                write_recent_events(f, recent_events)
            }
        }
    }
}

impl std::fmt::Debug for ScenarioIssue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

pub struct ScenarioFailure {
    scenario: Box<str>,
    phase: StepKind,
    step: Box<str>,
    issue: Box<ScenarioIssue>,
}

impl Display for ScenarioFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Scenario `{}` failed at {} {}",
            self.scenario, self.phase, self.step
        )?;
        Display::fmt(&self.issue, f)
    }
}

impl std::fmt::Debug for ScenarioFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl std::error::Error for ScenarioFailure {}

#[derive(Debug)]
struct TrackedTask {
    label: String,
    handle: JoinHandle<Result<(), String>>,
}

impl TrackedTask {
    fn new(label: impl Into<String>, handle: JoinHandle<Result<(), String>>) -> Self {
        Self {
            label: label.into(),
            handle,
        }
    }
}

fn write_recent_events(f: &mut Formatter<'_>, recent_events: &[String]) -> std::fmt::Result {
    if recent_events.is_empty() {
        return f.write_str("recent events: <none>");
    }

    writeln!(f, "recent events:")?;
    for (index, event) in recent_events.iter().enumerate() {
        writeln!(f, "{}. {}", index + 1, event)?;
    }
    Ok(())
}

fn compress_summary(summary: &str) -> String {
    let mut compact = summary.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() > DEFAULT_SUMMARY_LIMIT {
        compact.truncate(DEFAULT_SUMMARY_LIMIT - 1);
        compact.push('…');
    }
    compact
}

fn default_event_summary<S>(event: &Event<S>) -> String
where
    S: AgentSchema,
{
    let domain = match &event.payload {
        EventPayload::Interaction(_) => "interaction",
        EventPayload::Execution(_) => "execution",
        EventPayload::Presentation(_) => "presentation",
        EventPayload::Error(_) => "error",
    };
    let stream = match &event.stream {
        StreamKey::Global => "stream=global",
        StreamKey::Session(_) => "stream=session",
        StreamKey::Thread(_) => "stream=thread",
        StreamKey::Worker(_) => "stream=worker",
    };
    compress_summary(&format!(
        "{stream} visibility={:?} domain={domain}",
        event.visibility
    ))
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(payload) => format!("{payload:?}"),
        },
    }
}

pub struct ScenarioWorld<S, E, B>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>,
{
    bus: B,
    recorded_events: Arc<Mutex<Vec<Event<S>>>>,
    recent_event_limit: usize,
    summary: EventSummary<S>,
    tasks: Vec<TrackedTask>,
    recorder_label: String,
    needs_stabilize: bool,
}

impl<S, E, B> ScenarioWorld<S, E, B>
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
    S::Ids: AgentIds<EventId = S::EventId>,
    Event<S>: Clone + Send + 'static,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>
        + Clone
        + Send
        + Sync
        + 'static,
    for<'a> B::Stream<'a>: Send + Unpin,
    E: Debug + Send + Sync + 'static,
{
    fn new(bus: B) -> Self {
        let recorded_events = Arc::new(Mutex::new(Vec::new()));
        let summary: EventSummary<S> = Arc::new(default_event_summary::<S>);
        let mut world = Self {
            bus,
            recorded_events,
            recent_event_limit: DEFAULT_RECENT_EVENT_LIMIT,
            summary,
            tasks: Vec::new(),
            recorder_label: "recorder".to_string(),
            needs_stabilize: true,
        };
        world.start_recorder(Subscription::all());
        world
    }

    fn set_event_summary<F>(&mut self, summary: F)
    where
        F: Fn(&Event<S>) -> String + Send + Sync + 'static,
    {
        self.summary = Arc::new(move |event| compress_summary(&summary(event)));
    }

    fn set_recent_event_limit(&mut self, limit: usize) {
        self.recent_event_limit = limit.max(1);
    }

    fn start_recorder(&mut self, subscription: Subscription<S>) {
        let recorded_events = Arc::clone(&self.recorded_events);
        let bus = self.bus.clone();

        let handle = tokio::spawn(async move {
            let mut stream = bus
                .subscribe(subscription)
                .map_err(|error| format!("{error:?}"))?;
            while let Some(event) = next_event(&mut stream)
                .await
                .map_err(|error| format!("{error:?}"))?
            {
                recorded_events
                    .lock()
                    .expect("scenario event recorder mutex poisoned")
                    .push(event);
            }

            Ok(())
        });

        self.tasks
            .push(TrackedTask::new(&self.recorder_label, handle));
        self.needs_stabilize = true;
    }

    pub fn bus(&self) -> &B {
        &self.bus
    }

    /// Get the current event checkpoint.
    ///
    /// # Panics
    ///
    /// Panics if the recorder mutex is poisoned.
    pub fn checkpoint(&self) -> usize {
        self.recorded_events
            .lock()
            .expect("scenario recorded events mutex poisoned")
            .len()
    }

    /// Return all recorded events so far.
    ///
    /// # Panics
    ///
    /// Panics if the recorder mutex is poisoned.
    pub fn events(&self) -> Vec<Event<S>> {
        self.recorded_events
            .lock()
            .expect("scenario recorded events mutex poisoned")
            .clone()
    }

    /// Return events emitted after a checkpoint.
    ///
    /// # Panics
    ///
    /// Panics if the recorder mutex is poisoned.
    pub fn events_since(&self, checkpoint: usize) -> Vec<Event<S>> {
        self.recorded_events
            .lock()
            .expect("scenario recorded events mutex poisoned")
            .iter()
            .skip(checkpoint)
            .cloned()
            .collect()
    }

    /// Publish an event into the scenario bus.
    ///
    /// # Errors
    ///
    /// Returns a stringified bus error if the publish fails.
    pub async fn publish(
        &self,
        stream: StreamKey<S>,
        visibility: EventVisibility,
        payload: EventPayload<S>,
    ) -> Result<(), String> {
        publish::<S, B>(&self.bus, stream, visibility, payload)
            .await
            .map_err(|error| format!("{error:?}"))
    }

    pub fn spawn_bus_worker<W>(&mut self, label: impl Into<String>, worker: W)
    where
        W: BusWorker<S, B, Error = E> + Send + 'static,
        W::Run: Send + 'static,
    {
        let label = label.into();
        let bus = self.bus.clone();
        let handle =
            tokio::spawn(
                async move { worker.run(bus).await.map_err(|error| format!("{error:?}")) },
            );
        self.tasks.push(TrackedTask::new(label, handle));
        self.needs_stabilize = true;
    }

    pub fn spawn_session_worker<W>(
        &mut self,
        label: impl Into<String>,
        worker: W,
        session: SessionContext<S>,
    ) where
        W: SessionWorker<S, B, Error = E> + Send + 'static,
        W::Run: Send + 'static,
    {
        let label = label.into();
        let bus = self.bus.clone();
        let handle = tokio::spawn(async move {
            worker
                .run(bus, session)
                .await
                .map_err(|error| format!("{error:?}"))
        });
        self.tasks.push(TrackedTask::new(label, handle));
        self.needs_stabilize = true;
    }

    async fn expect_eventually<P>(
        &mut self,
        expectation: String,
        timeout: Duration,
        predicate: P,
    ) -> Result<(), ScenarioIssue>
    where
        P: Fn(&Event<S>) -> bool,
    {
        let started = tokio::time::Instant::now();
        loop {
            self.ensure_workers_healthy().await?;

            if self.events().iter().any(&predicate) {
                return Ok(());
            }

            if started.elapsed() >= timeout {
                return Err(ScenarioIssue::ExpectationNotMet {
                    expectation,
                    recent_events: self.recent_event_summaries(),
                });
            }

            sleep(EXPECTATION_POLL_INTERVAL).await;
        }
    }

    async fn expect_immediate<P>(
        &mut self,
        expectation: String,
        predicate: P,
    ) -> Result<(), ScenarioIssue>
    where
        P: Fn(&Event<S>) -> bool,
    {
        self.ensure_workers_healthy().await?;
        if self.events().iter().any(predicate) {
            Ok(())
        } else {
            Err(ScenarioIssue::ExpectationNotMet {
                expectation,
                recent_events: self.recent_event_summaries(),
            })
        }
    }

    async fn expect_absent<P>(
        &mut self,
        expectation: String,
        predicate: P,
    ) -> Result<(), ScenarioIssue>
    where
        P: Fn(&Event<S>) -> bool,
    {
        self.ensure_workers_healthy().await?;
        if self.events().iter().any(predicate) {
            Err(ScenarioIssue::ExpectationNotMet {
                expectation,
                recent_events: self.recent_event_summaries(),
            })
        } else {
            Ok(())
        }
    }

    fn recent_event_summaries(&self) -> Vec<String> {
        let events = self
            .recorded_events
            .lock()
            .expect("scenario recorded events mutex poisoned");
        let summary = Arc::clone(&self.summary);

        events
            .iter()
            .rev()
            .take(self.recent_event_limit)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|event| summary(event))
            .collect()
    }

    async fn ensure_workers_healthy(&mut self) -> Result<(), ScenarioIssue> {
        let mut index = 0;
        while index < self.tasks.len() {
            if !self.tasks[index].handle.is_finished() {
                index += 1;
                continue;
            }

            let task = self.tasks.remove(index);
            let label = task.label;
            match task.handle.await {
                Ok(Ok(())) => {
                    return Err(ScenarioIssue::WorkerExited {
                        label,
                        recent_events: self.recent_event_summaries(),
                    });
                }
                Ok(Err(message)) => {
                    return Err(ScenarioIssue::WorkerFailed {
                        label,
                        message,
                        recent_events: self.recent_event_summaries(),
                    });
                }
                Err(join_error) => {
                    if join_error.is_panic() {
                        return Err(ScenarioIssue::WorkerPanicked {
                            label,
                            message: panic_message(join_error.into_panic()),
                            recent_events: self.recent_event_summaries(),
                        });
                    }

                    return Err(ScenarioIssue::WorkerFailed {
                        label,
                        message: join_error.to_string(),
                        recent_events: self.recent_event_summaries(),
                    });
                }
            }
        }

        Ok(())
    }

    async fn stabilize(&mut self) {
        if !self.needs_stabilize {
            return;
        }

        tokio::task::yield_now().await;
        sleep(STARTUP_SETTLE_DELAY).await;
        tokio::task::yield_now().await;
        self.needs_stabilize = false;
    }
}

impl<S, E, B> Drop for ScenarioWorld<S, E, B>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>,
{
    fn drop(&mut self) {
        for task in &self.tasks {
            task.handle.abort();
        }
    }
}

pub struct Scenario<S, E, B>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>,
{
    name: String,
    world: ScenarioWorld<S, E, B>,
}

impl<S, E, B> Scenario<S, E, B>
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
    S::Ids: AgentIds<EventId = S::EventId>,
    Event<S>: Clone + Send + 'static,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>
        + Clone
        + Send
        + Sync
        + 'static,
    for<'a> B::Stream<'a>: Send + Unpin,
    E: Debug + Send + Sync + 'static,
{
    #[must_use]
    pub fn new(name: impl Into<String>, bus: B) -> Self {
        let name = name.into();
        let world = ScenarioWorld::new(bus);

        Self { name, world }
    }

    pub fn world(&mut self) -> &mut ScenarioWorld<S, E, B> {
        &mut self.world
    }

    #[must_use]
    pub fn with_event_summary<F>(mut self, summary: F) -> Self
    where
        F: Fn(&Event<S>) -> String + Send + Sync + 'static,
    {
        self.world.set_event_summary(summary);
        self
    }

    #[must_use]
    pub fn with_recent_event_limit(mut self, limit: usize) -> Self {
        self.world.set_recent_event_limit(limit);
        self
    }

    /// Get the given step builder.
    pub fn given(&mut self, description: impl Into<String>) -> Step<'_, S, E, B> {
        Step::new(self, StepKind::Given, description.into())
    }

    /// Get the when step builder.
    pub fn when(&mut self, description: impl Into<String>) -> Step<'_, S, E, B> {
        Step::new(self, StepKind::When, description.into())
    }

    /// Get the then step builder.
    pub fn then(&mut self, description: impl Into<String>) -> Step<'_, S, E, B> {
        Step::new(self, StepKind::Then, description.into())
    }

    async fn before_step(
        &mut self,
        step_kind: StepKind,
        step: &str,
    ) -> Result<(), ScenarioFailure> {
        self.world.stabilize().await;
        self.world
            .ensure_workers_healthy()
            .await
            .map_err(|issue| self.failure(step_kind, step, issue))
    }

    async fn after_step(&mut self, step_kind: StepKind, step: &str) -> Result<(), ScenarioFailure> {
        tokio::task::yield_now().await;
        self.world
            .ensure_workers_healthy()
            .await
            .map_err(|issue| self.failure(step_kind, step, issue))
    }

    fn failure(
        &self,
        step_kind: StepKind,
        step: impl Into<String>,
        issue: ScenarioIssue,
    ) -> ScenarioFailure {
        ScenarioFailure {
            scenario: self.name.clone().into_boxed_str(),
            phase: step_kind,
            step: step.into().into_boxed_str(),
            issue: Box::new(issue),
        }
    }
}

pub struct Step<'a, S, E, B>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>,
{
    scenario: &'a mut Scenario<S, E, B>,
    phase: StepKind,
    description: String,
}

impl<'a, S, E, B> Step<'a, S, E, B>
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
    S::Ids: AgentIds<EventId = S::EventId>,
    Event<S>: Clone + Send + 'static,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>
        + Clone
        + Send
        + Sync
        + 'static,
    for<'b> B::Stream<'b>: Send + Unpin,
    E: Debug + Send + Sync + 'static,
{
    fn new(scenario: &'a mut Scenario<S, E, B>, step_kind: StepKind, description: String) -> Self {
        Self {
            scenario,
            phase: step_kind,
            description,
        }
    }

    /// Publish an event as part of the step.
    ///
    /// # Errors
    ///
    /// Returns [`ScenarioFailure`] if a worker panics, exits unexpectedly, or
    /// the publish operation fails.
    pub async fn publish(
        self,
        stream: StreamKey<S>,
        visibility: EventVisibility,
        payload: EventPayload<S>,
    ) -> Result<(), ScenarioFailure> {
        let step_kind = self.phase;
        let description = self.description;
        let scenario = self.scenario;

        scenario.before_step(step_kind, &description).await?;
        scenario
            .world
            .publish(stream, visibility, payload)
            .await
            .map_err(|message| {
                scenario.failure(
                    step_kind,
                    &description,
                    ScenarioIssue::PublishFailed {
                        message,
                        recent_events: scenario.world.recent_event_summaries(),
                    },
                )
            })?;
        scenario.after_step(step_kind, &description).await
    }

    /// Assert that an event exists immediately.
    ///
    /// # Errors
    ///
    /// Returns [`ScenarioFailure`] if the expectation is not met or a worker
    /// fails while the step is executing.
    pub async fn expect_event(
        self,
        expectation: impl Into<String>,
        predicate: impl Fn(&Event<S>) -> bool,
    ) -> Result<(), ScenarioFailure> {
        let step_kind = self.phase;
        let description = self.description;
        let scenario = self.scenario;

        scenario.before_step(step_kind, &description).await?;
        scenario
            .world
            .expect_immediate(expectation.into(), predicate)
            .await
            .map_err(|issue| scenario.failure(step_kind, &description, issue))?;
        scenario.after_step(step_kind, &description).await
    }

    /// Assert that an event eventually appears.
    ///
    /// # Errors
    ///
    /// Returns [`ScenarioFailure`] if the expectation is not met or a worker
    /// fails while the step is executing.
    pub async fn expect_eventually(
        self,
        expectation: impl Into<String>,
        timeout: Duration,
        predicate: impl Fn(&Event<S>) -> bool,
    ) -> Result<(), ScenarioFailure> {
        let step_kind = self.phase;
        let description = self.description;
        let scenario = self.scenario;

        scenario.before_step(step_kind, &description).await?;
        scenario
            .world
            .expect_eventually(expectation.into(), timeout, predicate)
            .await
            .map_err(|issue| scenario.failure(step_kind, &description, issue))?;
        scenario.after_step(step_kind, &description).await
    }

    /// Assert that no matching event appears.
    ///
    /// # Errors
    ///
    /// Returns [`ScenarioFailure`] if the expectation is violated or a worker
    /// fails while the step is executing.
    pub async fn expect_no_event(
        self,
        expectation: impl Into<String>,
        predicate: impl Fn(&Event<S>) -> bool,
    ) -> Result<(), ScenarioFailure> {
        let step_kind = self.phase;
        let description = self.description;
        let scenario = self.scenario;

        scenario.before_step(step_kind, &description).await?;
        scenario
            .world
            .expect_absent(expectation.into(), predicate)
            .await
            .map_err(|issue| scenario.failure(step_kind, &description, issue))?;
        scenario.after_step(step_kind, &description).await
    }

    /// Assert that the scenario remains healthy.
    ///
    /// # Errors
    ///
    /// Returns [`ScenarioFailure`] if any worker fails while the step is
    /// executing.
    pub async fn stays_healthy(self) -> Result<(), ScenarioFailure> {
        let step_kind = self.phase;
        let description = self.description;
        let scenario = self.scenario;

        scenario.before_step(step_kind, &description).await?;
        scenario.after_step(step_kind, &description).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mango_core::agent::{
        AgentSchema, BusWorker, EventPayload, EventVisibility, ExecutionEvent, InferenceEvent,
        StreamKey, Subscription, Worker,
    };
    use mango_runtime_support::{DefaultAgentIds, EngineId, ToolName, WorkerId, publish};
    use mango_shim_inmemory::InMemoryAgentBus;

    use super::Scenario;

    #[derive(Debug, Clone)]
    struct TestSchema;

    impl AgentSchema for TestSchema {
        type Ids = DefaultAgentIds;
        type Surface = ();
        type InputKind = ();
        type Input = ();
        type InterruptDetail = ();
        type Directive = ();
        type Output = String;
        type ToolData = ();
        type Status = ();
        type CancellationDetail = ();
        type CompletionDetail = ();
        type EngineId = EngineId;
        type ToolName = ToolName;
    }

    #[derive(Clone)]
    struct PublishOutputWorker;

    impl Worker for PublishOutputWorker {
        type WorkerId = WorkerId;
        type Subscription = Subscription<TestSchema>;

        fn worker_id(&self) -> Self::WorkerId {
            WorkerId::from("publish-output")
        }

        fn subscription(&self) -> Self::Subscription {
            Subscription::all()
        }
    }

    impl BusWorker<TestSchema, InMemoryAgentBus<TestSchema>> for PublishOutputWorker {
        type Error = mango_shim_inmemory::InMemoryEventBusError;
        type Run = mango_runtime_support::BoxFuture<'static, Self::Error>;

        fn run(self, bus: InMemoryAgentBus<TestSchema>) -> Self::Run {
            Box::pin(async move {
                publish::<TestSchema, _>(
                    &bus,
                    StreamKey::Global,
                    EventVisibility::Both,
                    EventPayload::Execution(ExecutionEvent::Inference(InferenceEvent::Output {
                        run_id: TestSchema::next_inference_run_id(),
                        sequence: 0,
                        output: "hello".to_string(),
                    })),
                )
                .await?;
                std::future::pending::<()>().await;
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn scenario_reports_emitted_events() -> Result<(), super::ScenarioFailure> {
        let bus = InMemoryAgentBus::<TestSchema>::new(32);
        let mut scenario = Scenario::new("worker emits output", bus).with_event_summary(|event| {
            let label = match &event.payload {
                EventPayload::Execution(_) => "execution",
                EventPayload::Interaction(_) => "interaction",
                EventPayload::Presentation(_) => "presentation",
                EventPayload::Error(_) => "error",
            };
            label.to_string()
        });
        scenario
            .world()
            .spawn_bus_worker("publisher", PublishOutputWorker);

        scenario
            .then("an output event appears")
            .expect_eventually("an inference output", Duration::from_millis(50), |event| {
                matches!(
                    event.payload,
                    EventPayload::Execution(ExecutionEvent::Inference(
                        InferenceEvent::Output { .. }
                    ))
                )
            })
            .await?;

        Ok(())
    }
}
