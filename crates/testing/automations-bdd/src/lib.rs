use std::{
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use mango_automations::{AutomationsError, TraceRecord};
use tokio::time::sleep;

const DEFAULT_RECENT_TRACE_LIMIT: usize = 8;
const EXPECTATION_POLL_INTERVAL: Duration = Duration::from_millis(5);

type TraceSummary = Arc<dyn Fn(&TraceRecord) -> String + Send + Sync>;

#[async_trait]
pub trait AutomationsScenarioWorld {
    async fn traces(&mut self) -> Result<Vec<TraceRecord>, AutomationsError>;

    /// Validate any world-specific health invariants between scenario steps.
    ///
    /// # Errors
    ///
    /// Returns an error when the world has entered an invalid state and the
    /// scenario should stop immediately.
    fn ensure_healthy(&mut self) -> Result<(), AutomationsError> {
        Ok(())
    }
}

#[async_trait]
pub trait TimeDrivenScenarioWorld: AutomationsScenarioWorld {
    fn advance_time_by(&mut self, seconds: i64);

    /// Drive the simulated automation world until no more work remains due at
    /// the current simulated time.
    ///
    /// # Errors
    ///
    /// Returns an error when the simulator cannot settle its pending work.
    async fn settle_automations(&mut self) -> Result<(), AutomationsError>;

    /// Advance simulated time and settle any work that becomes due.
    ///
    /// # Errors
    ///
    /// Returns an error when the simulator cannot settle its pending work.
    async fn advance_time_by_and_settle(&mut self, seconds: i64) -> Result<(), AutomationsError> {
        self.advance_time_by(seconds);
        self.settle_automations().await
    }
}

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

#[derive(Debug)]
enum ScenarioIssue {
    ActionFailed(String),
    ExpectationNotMet {
        expectation: String,
        recent_traces: Vec<String>,
    },
    WorldFailed(String),
}

impl Display for ScenarioIssue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ActionFailed(message) => write!(f, "action failed: {message}"),
            Self::ExpectationNotMet {
                expectation,
                recent_traces,
            } => {
                writeln!(f, "expectation not met: {expectation}")?;
                if recent_traces.is_empty() {
                    return write!(f, "recent traces: <none>");
                }
                writeln!(f, "recent traces:")?;
                for (index, trace) in recent_traces.iter().enumerate() {
                    writeln!(f, "{}. {}", index + 1, trace)?;
                }
                Ok(())
            }
            Self::WorldFailed(message) => write!(f, "world failed: {message}"),
        }
    }
}

#[derive(Debug)]
pub struct ScenarioFailure {
    scenario: String,
    phase: StepKind,
    step: String,
    issue: ScenarioIssue,
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

impl std::error::Error for ScenarioFailure {}

pub struct Scenario<W> {
    name: String,
    recent_trace_limit: usize,
    trace_summary: TraceSummary,
    world: W,
}

impl<W> Scenario<W> {
    #[must_use]
    pub fn new(name: impl Into<String>, world: W) -> Self {
        Self {
            name: name.into(),
            recent_trace_limit: DEFAULT_RECENT_TRACE_LIMIT,
            trace_summary: Arc::new(|trace| format!("{:?}", trace.event)),
            world,
        }
    }

    #[must_use]
    pub fn with_recent_trace_limit(mut self, recent_trace_limit: usize) -> Self {
        self.recent_trace_limit = recent_trace_limit.max(1);
        self
    }

    #[must_use]
    pub fn with_trace_summary(
        mut self,
        trace_summary: impl Fn(&TraceRecord) -> String + Send + Sync + 'static,
    ) -> Self {
        self.trace_summary = Arc::new(trace_summary);
        self
    }

    pub fn world(&mut self) -> &mut W {
        &mut self.world
    }

    pub fn given(&mut self, step: impl Into<String>) -> ScenarioStep<'_, W> {
        ScenarioStep::new(self, StepKind::Given, step.into())
    }

    pub fn when(&mut self, step: impl Into<String>) -> ScenarioStep<'_, W> {
        ScenarioStep::new(self, StepKind::When, step.into())
    }

    pub fn then(&mut self, step: impl Into<String>) -> ScenarioStep<'_, W> {
        ScenarioStep::new(self, StepKind::Then, step.into())
    }
}

pub struct ScenarioStep<'a, W> {
    scenario: &'a mut Scenario<W>,
    phase: StepKind,
    step: String,
}

impl<'a, W> ScenarioStep<'a, W> {
    fn new(scenario: &'a mut Scenario<W>, phase: StepKind, step: String) -> Self {
        Self {
            scenario,
            phase,
            step,
        }
    }
}

impl<W> ScenarioStep<'_, W>
where
    W: AutomationsScenarioWorld,
{
    /// Execute one action against the scenario world.
    ///
    /// # Errors
    ///
    /// Returns a scenario failure when the action or post-action world health
    /// check fails.
    pub async fn perform<F>(self, action: F) -> Result<(), ScenarioFailure>
    where
        F: for<'w> FnOnce(
            &'w mut W,
        )
            -> Pin<Box<dyn Future<Output = Result<(), AutomationsError>> + 'w>>,
    {
        action(&mut self.scenario.world)
            .await
            .map_err(|error| self.failure(ScenarioIssue::ActionFailed(error.to_string())))?;
        self.scenario
            .world
            .ensure_healthy()
            .map_err(|error| self.failure(ScenarioIssue::WorldFailed(error.to_string())))
    }

    /// Poll until a trace matches the supplied predicate or the timeout expires.
    ///
    /// # Errors
    ///
    /// Returns a scenario failure when the world becomes unhealthy, trace
    /// collection fails, or the predicate is not satisfied before the timeout.
    pub async fn expect_eventually<P>(
        self,
        expectation: impl Into<String>,
        timeout: Duration,
        predicate: P,
    ) -> Result<(), ScenarioFailure>
    where
        P: Fn(&TraceRecord) -> bool,
    {
        let expectation = expectation.into();
        let started = tokio::time::Instant::now();
        loop {
            self.scenario
                .world
                .ensure_healthy()
                .map_err(|error| self.failure(ScenarioIssue::WorldFailed(error.to_string())))?;

            let traces =
                self.scenario.world.traces().await.map_err(|error| {
                    self.failure(ScenarioIssue::ActionFailed(error.to_string()))
                })?;
            if traces.iter().any(&predicate) {
                return Ok(());
            }

            if started.elapsed() >= timeout {
                return Err(self.failure(ScenarioIssue::ExpectationNotMet {
                    expectation,
                    recent_traces: self.scenario.recent_trace_summaries(&traces),
                }));
            }

            sleep(EXPECTATION_POLL_INTERVAL).await;
        }
    }

    fn failure(&self, issue: ScenarioIssue) -> ScenarioFailure {
        ScenarioFailure {
            scenario: self.scenario.name.clone(),
            phase: self.phase,
            step: self.step.clone(),
            issue,
        }
    }
}

impl<W> ScenarioStep<'_, W>
where
    W: TimeDrivenScenarioWorld,
{
    /// Advance simulated time and settle pending automation work.
    ///
    /// # Errors
    ///
    /// Returns a scenario failure when time advancement, settling, or the
    /// post-step health check fails.
    pub async fn advance_time_by_and_settle(self, seconds: i64) -> Result<(), ScenarioFailure> {
        self.scenario.world.advance_time_by(seconds);
        self.scenario
            .world
            .settle_automations()
            .await
            .map_err(|error| self.failure(ScenarioIssue::ActionFailed(error.to_string())))?;
        self.scenario
            .world
            .ensure_healthy()
            .map_err(|error| self.failure(ScenarioIssue::WorldFailed(error.to_string())))
    }
}

impl<W> Scenario<W> {
    fn recent_trace_summaries(&self, traces: &[TraceRecord]) -> Vec<String> {
        traces
            .iter()
            .rev()
            .take(self.recent_trace_limit)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|trace| (self.trace_summary)(trace))
            .collect()
    }
}
