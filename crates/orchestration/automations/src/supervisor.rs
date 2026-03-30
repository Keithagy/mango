use std::time::Duration;

use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};

use crate::{AutomationsControlPlane, AutomationsError, Clock, ControlPlaneStore, EffectHandler};

#[derive(Debug, Clone, Copy)]
pub struct SupervisorConfig {
    pub poll_interval: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(250),
        }
    }
}

#[derive(Debug)]
pub struct SupervisorHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), AutomationsError>>,
}

impl SupervisorHandle {
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub fn abort(&self) {
        self.task.abort();
    }

    /// Stop the supervisor loop and await shutdown completion.
    ///
    /// # Errors
    ///
    /// Returns an error when the supervisor task exits with a runtime failure.
    pub async fn shutdown(mut self) -> Result<(), AutomationsError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task
            .await
            .map_err(|error| AutomationsError::State(error.to_string()))?
    }
}

pub fn spawn_supervisor<B, R, H, C>(
    control_plane: AutomationsControlPlane<B, R, H, C>,
    config: SupervisorConfig,
) -> SupervisorHandle
where
    B: ControlPlaneStore,
    R: crate::AutomationRuntime,
    H: EffectHandler,
    C: Clock,
{
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut ticker = interval(config.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => return Ok(()),
                _ = ticker.tick() => {
                    control_plane.reconcile_due().await?;
                }
            }
        }
    });

    SupervisorHandle {
        shutdown: Some(shutdown_tx),
        task,
    }
}
