use serde_json::Value;

use crate::{
    ActivationMode, AutomationRuntime, AutomationsControlPlane, AutomationsError, EffectHandler,
    EventSubmission, ManagedAutomation, ManualClock, MemoryControlPlaneStore, RegistrationRequest,
    RevisionId, TraceRecord,
};

#[derive(Debug, Clone)]
pub struct PocketUniverse<R, H> {
    clock: ManualClock,
    control_plane: AutomationsControlPlane<MemoryControlPlaneStore, R, H, ManualClock>,
}

impl<R, H> PocketUniverse<R, H>
where
    R: AutomationRuntime,
    H: EffectHandler,
{
    #[must_use]
    pub fn new(initial_timestamp: i64, runtime: R, effect_handler: H) -> Self {
        let clock = ManualClock::new(initial_timestamp);
        let control_plane = AutomationsControlPlane::with_runtime(
            MemoryControlPlaneStore::new(),
            runtime,
            effect_handler,
            clock.clone(),
        );
        Self {
            clock,
            control_plane,
        }
    }

    #[must_use]
    pub fn clock(&self) -> &ManualClock {
        &self.clock
    }

    pub fn advance_time_by(&self, seconds: i64) {
        self.clock.advance_by(seconds);
    }

    #[must_use]
    pub fn control_plane(
        &self,
    ) -> &AutomationsControlPlane<MemoryControlPlaneStore, R, H, ManualClock> {
        &self.control_plane
    }

    /// Register a guest revision into the in-memory pocket universe.
    ///
    /// # Errors
    ///
    /// Returns an error when the control plane cannot register the artifact.
    pub fn register_revision(
        &self,
        request: &RegistrationRequest,
    ) -> Result<crate::RegisteredRevision, AutomationsError> {
        self.control_plane.register_revision(request)
    }

    /// Activate a registered revision inside the simulator.
    ///
    /// # Errors
    ///
    /// Returns an error when activation fails for the same reasons as the real
    /// control plane.
    pub async fn activate_revision(
        &self,
        automation_id: &str,
        revision_id: RevisionId,
        mode: ActivationMode,
    ) -> Result<(), AutomationsError> {
        self.control_plane
            .activate_revision(automation_id, revision_id, mode)
            .await
    }

    /// Deactivate the current revision inside the simulator.
    ///
    /// # Errors
    ///
    /// Returns an error when the automation has no active revision.
    pub fn deactivate_automation(&self, automation_id: &str) -> Result<(), AutomationsError> {
        self.control_plane.deactivate_automation(automation_id)
    }

    /// Delete an automation and all persisted simulator state for it.
    ///
    /// # Errors
    ///
    /// Returns an error when the automation does not exist.
    pub fn delete_automation(&self, automation_id: &str) -> Result<(), AutomationsError> {
        self.control_plane.delete_automation(automation_id)
    }

    /// Dispatch all wakeups currently due in the simulator.
    ///
    /// # Errors
    ///
    /// Returns an error when the simulated control plane cannot reconcile due
    /// wakeups.
    pub async fn reconcile_due(&self) -> Result<usize, AutomationsError> {
        self.control_plane.reconcile_due().await
    }

    /// Repeatedly reconcile the simulated control plane until no wakeups remain
    /// due at the current simulated time.
    ///
    /// # Errors
    ///
    /// Returns an error when any reconcile cycle fails.
    pub async fn settle(&self) -> Result<usize, AutomationsError> {
        let mut total_dispatched = 0;
        loop {
            let dispatched = self.control_plane.reconcile_due().await?;
            total_dispatched += dispatched;
            if dispatched == 0 {
                return Ok(total_dispatched);
            }
        }
    }

    /// Advance simulated time and then settle all due wakeups at that time.
    ///
    /// # Errors
    ///
    /// Returns an error when settling due wakeups fails.
    pub async fn advance_time_by_and_settle(
        &self,
        seconds: i64,
    ) -> Result<usize, AutomationsError> {
        self.advance_time_by(seconds);
        self.settle().await
    }

    /// Inject a user signal into the simulated control plane.
    ///
    /// # Errors
    ///
    /// Returns an error when the signal cannot be delivered to the active
    /// automation revision.
    pub async fn submit_user_signal(
        &self,
        automation_id: &str,
        signal: impl Into<String>,
        payload: Value,
    ) -> Result<EventSubmission, AutomationsError> {
        self.control_plane
            .submit_user_signal(automation_id, signal, payload)
            .await
    }

    /// Inject a normalized runtime trigger into the simulated control plane.
    ///
    /// # Errors
    ///
    /// Returns an error when the trigger cannot be delivered to the active
    /// automation revision.
    pub async fn submit_trigger(
        &self,
        automation_id: &str,
        trigger: impl Into<String>,
        payload: Value,
    ) -> Result<EventSubmission, AutomationsError> {
        self.control_plane
            .submit_trigger(automation_id, trigger, payload)
            .await
    }

    /// Read the current simulated automation map.
    ///
    /// # Errors
    ///
    /// Returns an error when the simulated control plane snapshot cannot be
    /// loaded.
    pub fn automations(
        &self,
    ) -> Result<std::collections::BTreeMap<String, ManagedAutomation>, AutomationsError> {
        self.control_plane.automations()
    }

    /// Read the current simulated trace log.
    ///
    /// # Errors
    ///
    /// Returns an error when the simulated control plane snapshot cannot be
    /// loaded.
    pub fn traces(&self) -> Result<Vec<TraceRecord>, AutomationsError> {
        self.control_plane.traces()
    }

    /// Read the full simulated control-plane snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the simulated control plane snapshot cannot be
    /// loaded.
    pub fn snapshot(&self) -> Result<crate::ControlPlaneState, AutomationsError> {
        self.control_plane.control_plane_snapshot()
    }
}
