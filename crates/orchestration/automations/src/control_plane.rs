use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
};

use async_trait::async_trait;
use mango_automation_protocol::{
    AdvanceRequest, AutomationEvent, Capability, EffectKind, EffectRequest,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::guest::{AutomationRuntime, WasmAutomationRuntime};
use crate::{
    ActivationMode, AutomationsError, Clock, ControlPlaneState, ControlPlaneStore,
    ManagedAutomation, RegisteredRevision, RevisionId, ScheduledWakeup, TraceEvent, TraceRecord,
    effect_kind_label,
};

#[derive(Debug, Clone)]
pub struct RegistrationRequest {
    pub automation_id: String,
    pub artifact_path: PathBuf,
    pub config: Value,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct EffectHandlerOutcome {
    pub follow_up_events: Vec<AutomationEvent>,
}

#[async_trait]
pub trait EffectHandler: Clone + Send + Sync + 'static {
    /// Execute a host-mediated effect requested by a guest automation.
    ///
    /// # Errors
    ///
    /// Returns an error when the host cannot complete the requested effect.
    async fn handle_effect(
        &self,
        automation_id: &str,
        revision_id: RevisionId,
        effect: &EffectRequest,
        now: i64,
    ) -> Result<EffectHandlerOutcome, AutomationsError>;
}

#[derive(Debug, Clone, Default)]
pub struct NoopEffectHandler;

#[async_trait]
impl EffectHandler for NoopEffectHandler {
    async fn handle_effect(
        &self,
        _automation_id: &str,
        _revision_id: RevisionId,
        _effect: &EffectRequest,
        _now: i64,
    ) -> Result<EffectHandlerOutcome, AutomationsError> {
        Ok(EffectHandlerOutcome::default())
    }
}

#[derive(Debug, Clone)]
pub struct AutomationsControlPlane<B, R, H, C> {
    store: B,
    runtime: R,
    effect_handler: H,
    clock: C,
}

impl<B, H, C> AutomationsControlPlane<B, WasmAutomationRuntime, H, C>
where
    B: ControlPlaneStore,
    H: EffectHandler,
    C: Clock,
{
    #[must_use]
    pub fn new(store: B, effect_handler: H, clock: C) -> Self {
        Self::with_runtime(store, WasmAutomationRuntime::new(), effect_handler, clock)
    }
}

impl<B, R, H, C> AutomationsControlPlane<B, R, H, C>
where
    B: ControlPlaneStore,
    R: AutomationRuntime,
    H: EffectHandler,
    C: Clock,
{
    #[must_use]
    pub fn with_runtime(store: B, runtime: R, effect_handler: H, clock: C) -> Self {
        Self {
            store,
            runtime,
            effect_handler,
            clock,
        }
    }

    /// Register a guest artifact as a distinct control-plane revision without
    /// activating it.
    ///
    /// # Errors
    ///
    /// Returns an error when the artifact cannot be read, the guest cannot be
    /// registered, or the control-plane state cannot be persisted.
    pub fn register_revision(
        &self,
        request: &RegistrationRequest,
    ) -> Result<RegisteredRevision, AutomationsError> {
        let now = self.clock.now();
        let artifact_bytes = std::fs::read(&request.artifact_path)
            .map_err(|error| AutomationsError::Io(error.to_string()))?;
        let artifact_sha256 = format!("{:x}", Sha256::digest(&artifact_bytes));
        let registration = self.runtime.register(&request.artifact_path)?;

        self.store.transact(|state| {
            let revision_id = state.allocate_revision_id();
            let automation = state
                .automations
                .entry(request.automation_id.clone())
                .or_insert_with(|| ManagedAutomation::new(request.automation_id.clone()));
            let revision = RegisteredRevision {
                revision_id,
                artifact_path: request.artifact_path.clone(),
                artifact_sha256: artifact_sha256.clone(),
                registered_at: now,
                descriptor: registration.descriptor.clone(),
                config: request.config.clone(),
                initial_state: registration.initial_state.clone(),
            };
            automation.revisions.insert(revision_id, revision.clone());
            push_trace(
                state,
                now,
                TraceEvent::RevisionRegistered {
                    automation_id: request.automation_id.clone(),
                    revision_id,
                    artifact_sha256: artifact_sha256.clone(),
                },
            );
            Ok(revision)
        })
    }

    /// Activate a previously registered revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the automation or revision does not exist, when a
    /// preserve-state activation is schema-incompatible, or when the guest fails
    /// its activation transition.
    pub async fn activate_revision(
        &self,
        automation_id: &str,
        revision_id: RevisionId,
        mode: ActivationMode,
    ) -> Result<(), AutomationsError> {
        let now = self.clock.now();
        self.store.transact(|state| {
            let automation = state
                .automations
                .get_mut(automation_id)
                .ok_or_else(|| AutomationsError::AutomationNotFound(automation_id.to_string()))?;
            let revision = automation.revisions.get(&revision_id).ok_or_else(|| {
                AutomationsError::RevisionNotFound {
                    automation_id: automation_id.to_string(),
                    revision_id,
                }
            })?;

            if matches!(mode, ActivationMode::PreserveState) {
                match (
                    automation
                        .active_revision_id
                        .and_then(|id| automation.revisions.get(&id)),
                    automation.current_state.as_ref(),
                ) {
                    (Some(previous), Some(_))
                        if previous.descriptor.state_schema_version
                            != revision.descriptor.state_schema_version =>
                    {
                        return Err(AutomationsError::IncompatibleState {
                            automation_id: automation_id.to_string(),
                            revision_id,
                        });
                    }
                    _ => {}
                }
            }

            automation.active_revision_id = Some(revision_id);
            if matches!(mode, ActivationMode::ColdStart) || automation.current_state.is_none() {
                automation.current_state = Some(revision.initial_state.clone());
            }
            automation.scheduled_wakeups.clear();
            automation.last_status = None;

            push_trace(
                state,
                now,
                TraceEvent::RevisionActivated {
                    automation_id: automation_id.to_string(),
                    revision_id,
                    mode,
                },
            );
            Ok(())
        })?;

        self.submit_event(automation_id, AutomationEvent::Activated { at: now }, now)
            .await
    }

    /// Deactivate the currently active revision for an automation while
    /// preserving the registered revisions and current guest state.
    ///
    /// # Errors
    ///
    /// Returns an error when the automation does not exist or does not have an
    /// active revision.
    pub fn deactivate_automation(&self, automation_id: &str) -> Result<(), AutomationsError> {
        let now = self.clock.now();
        self.store.transact(|state| {
            let automation = state
                .automations
                .get_mut(automation_id)
                .ok_or_else(|| AutomationsError::AutomationNotFound(automation_id.to_string()))?;
            let revision_id = automation.active_revision_id.ok_or_else(|| {
                AutomationsError::NoActiveRevision {
                    automation_id: automation_id.to_string(),
                }
            })?;
            automation.active_revision_id = None;
            automation.scheduled_wakeups.clear();
            automation.last_status = Some("deactivated".to_string());
            push_trace(
                state,
                now,
                TraceEvent::AutomationDeactivated {
                    automation_id: automation_id.to_string(),
                    revision_id,
                },
            );
            Ok(())
        })
    }

    /// Delete an automation definition and all of its persisted revisions,
    /// state, wakeups, and traceable runtime ownership.
    ///
    /// # Errors
    ///
    /// Returns an error when the automation does not exist or the deletion
    /// cannot be persisted.
    pub fn delete_automation(&self, automation_id: &str) -> Result<(), AutomationsError> {
        let now = self.clock.now();
        self.store.transact(|state| {
            state
                .automations
                .remove(automation_id)
                .ok_or_else(|| AutomationsError::AutomationNotFound(automation_id.to_string()))?;
            push_trace(
                state,
                now,
                TraceEvent::AutomationDeleted {
                    automation_id: automation_id.to_string(),
                },
            );
            Ok(())
        })
    }

    /// Deliver a user-originated signal into the active automation revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the automation has no active revision or when the
    /// guest transition fails.
    pub async fn submit_user_signal(
        &self,
        automation_id: &str,
        signal: impl Into<String>,
        payload: Value,
    ) -> Result<(), AutomationsError> {
        let now = self.clock.now();
        self.submit_event(
            automation_id,
            AutomationEvent::UserSignal {
                signal: signal.into(),
                payload,
                at: now,
            },
            now,
        )
        .await
    }

    /// Dispatch every wakeup due at the current host time.
    ///
    /// # Errors
    ///
    /// Returns an error when due wakeups cannot be loaded or when any resulting
    /// guest transition fails.
    pub async fn reconcile_due(&self) -> Result<usize, AutomationsError> {
        let now = self.clock.now();
        let due = self.due_wakeups(now)?;
        for (automation_id, revision_id, wakeup_id, at) in &due {
            self.store.transact(|state| {
                let automation = state
                    .automations
                    .get_mut(automation_id)
                    .ok_or_else(|| AutomationsError::AutomationNotFound(automation_id.clone()))?;
                automation.scheduled_wakeups.remove(wakeup_id);
                push_trace(
                    state,
                    now,
                    TraceEvent::WakeupDispatched {
                        automation_id: automation_id.clone(),
                        revision_id: *revision_id,
                        wakeup_id: wakeup_id.clone(),
                        at: *at,
                    },
                );
                Ok(())
            })?;
            self.submit_event(
                automation_id,
                AutomationEvent::WakeupFired {
                    wakeup_id: wakeup_id.clone(),
                    at: now,
                },
                now,
            )
            .await?;
        }
        Ok(due.len())
    }

    /// Return the current managed automation map.
    ///
    /// # Errors
    ///
    /// Returns an error when the control-plane snapshot cannot be read.
    pub fn automations(&self) -> Result<BTreeMap<String, ManagedAutomation>, AutomationsError> {
        Ok(self.store.snapshot()?.automations)
    }

    /// Return the current trace log.
    ///
    /// # Errors
    ///
    /// Returns an error when the control-plane snapshot cannot be read.
    pub fn traces(&self) -> Result<Vec<TraceRecord>, AutomationsError> {
        Ok(self.store.snapshot()?.traces)
    }

    /// Return the full persisted control-plane snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying store cannot be read.
    pub fn control_plane_snapshot(&self) -> Result<ControlPlaneState, AutomationsError> {
        self.store.snapshot()
    }

    fn due_wakeups(
        &self,
        now: i64,
    ) -> Result<Vec<(String, RevisionId, String, i64)>, AutomationsError> {
        let snapshot = self.store.snapshot()?;
        let mut due = snapshot
            .automations
            .into_iter()
            .flat_map(|(automation_id, automation)| {
                let revision_id = automation.active_revision_id.unwrap_or_default();
                automation
                    .scheduled_wakeups
                    .into_values()
                    .filter(move |wakeup| wakeup.at <= now)
                    .map(move |wakeup| {
                        (
                            automation_id.clone(),
                            revision_id,
                            wakeup.wakeup_id,
                            wakeup.at,
                        )
                    })
            })
            .collect::<Vec<_>>();
        due.sort_by_key(|item| (item.3, item.0.clone(), item.2.clone()));
        Ok(due)
    }

    async fn submit_event(
        &self,
        automation_id: &str,
        event: AutomationEvent,
        now: i64,
    ) -> Result<(), AutomationsError> {
        let mut pending_events = VecDeque::from([event]);
        while let Some(event) = pending_events.pop_front() {
            let (revision, response) = self.advance_guest(automation_id, event.clone(), now)?;
            let follow_up_effects =
                self.persist_response(automation_id, &revision, event, now, &response)?;
            for effect in &follow_up_effects {
                let follow_up_events = self
                    .apply_effect(automation_id, revision.revision_id, effect, now)
                    .await?;
                pending_events.extend(follow_up_events);
            }
        }
        Ok(())
    }

    fn advance_guest(
        &self,
        automation_id: &str,
        event: AutomationEvent,
        now: i64,
    ) -> Result<
        (
            RegisteredRevision,
            mango_automation_protocol::AdvanceResponse,
        ),
        AutomationsError,
    > {
        let snapshot = self.store.snapshot()?;
        let automation = snapshot
            .automations
            .get(automation_id)
            .ok_or_else(|| AutomationsError::AutomationNotFound(automation_id.to_string()))?;
        let revision_id =
            automation
                .active_revision_id
                .ok_or_else(|| AutomationsError::NoActiveRevision {
                    automation_id: automation_id.to_string(),
                })?;
        let revision = automation
            .revisions
            .get(&revision_id)
            .ok_or_else(|| AutomationsError::RevisionNotFound {
                automation_id: automation_id.to_string(),
                revision_id,
            })?
            .clone();
        let current_state = automation
            .current_state
            .clone()
            .unwrap_or_else(|| revision.initial_state.clone());

        let response = self.runtime.advance(
            &revision.artifact_path,
            &AdvanceRequest {
                automation_id: automation_id.to_string(),
                revision_id,
                now,
                config: revision.config.clone(),
                state: current_state,
                event,
            },
        )?;
        Ok((revision, response))
    }

    fn persist_response(
        &self,
        automation_id: &str,
        revision: &RegisteredRevision,
        event: AutomationEvent,
        now: i64,
        response: &mango_automation_protocol::AdvanceResponse,
    ) -> Result<Vec<EffectRequest>, AutomationsError> {
        self.store.transact(|state| {
            let mut external_effects = Vec::new();
            let mut trace_events = vec![
                TraceEvent::EventSubmitted {
                    automation_id: automation_id.to_string(),
                    revision_id: revision.revision_id,
                    event,
                },
                TraceEvent::StateAdvanced {
                    automation_id: automation_id.to_string(),
                    revision_id: revision.revision_id,
                    response: response.clone(),
                },
            ];

            {
                let automation = state.automations.get_mut(automation_id).ok_or_else(|| {
                    AutomationsError::AutomationNotFound(automation_id.to_string())
                })?;
                if automation.active_revision_id != Some(revision.revision_id) {
                    return Err(AutomationsError::RevisionNotFound {
                        automation_id: automation_id.to_string(),
                        revision_id: revision.revision_id,
                    });
                }

                automation.current_state = Some(response.state.clone());
                automation.last_status.clone_from(&response.status);

                for effect in &response.effects {
                    match &effect.kind {
                        EffectKind::ScheduleWakeup { wakeup_id, at } => {
                            automation.scheduled_wakeups.insert(
                                wakeup_id.clone(),
                                ScheduledWakeup {
                                    wakeup_id: wakeup_id.clone(),
                                    at: *at,
                                },
                            );
                            trace_events.push(TraceEvent::WakeupScheduled {
                                automation_id: automation_id.to_string(),
                                revision_id: revision.revision_id,
                                wakeup_id: wakeup_id.clone(),
                                at: *at,
                            });
                        }
                        EffectKind::CancelWakeup { wakeup_id } => {
                            automation.scheduled_wakeups.remove(wakeup_id);
                            trace_events.push(TraceEvent::WakeupCancelled {
                                automation_id: automation_id.to_string(),
                                revision_id: revision.revision_id,
                                wakeup_id: wakeup_id.clone(),
                            });
                        }
                        other => {
                            external_effects.push(effect.clone());
                            trace_events.push(TraceEvent::EffectRequested {
                                automation_id: automation_id.to_string(),
                                revision_id: revision.revision_id,
                                effect_id: effect.effect_id.clone(),
                                effect_kind: effect_kind_label(other).to_string(),
                            });
                        }
                    }
                }
            }

            for trace_event in trace_events {
                push_trace(state, now, trace_event);
            }

            Ok(external_effects)
        })
    }

    async fn apply_effect(
        &self,
        automation_id: &str,
        revision_id: RevisionId,
        effect: &EffectRequest,
        now: i64,
    ) -> Result<Vec<AutomationEvent>, AutomationsError> {
        self.ensure_capability(automation_id, revision_id, &effect.kind)?;
        let outcome = self
            .effect_handler
            .handle_effect(automation_id, revision_id, effect, now)
            .await?;
        self.store.transact(|state| {
            push_trace(
                state,
                now,
                TraceEvent::EffectHandled {
                    automation_id: automation_id.to_string(),
                    revision_id,
                    effect_id: effect.effect_id.clone(),
                    follow_up_events: outcome.follow_up_events.len(),
                },
            );
            Ok(())
        })?;
        Ok(outcome.follow_up_events)
    }

    fn ensure_capability(
        &self,
        automation_id: &str,
        revision_id: RevisionId,
        effect: &EffectKind,
    ) -> Result<(), AutomationsError> {
        let snapshot = self.store.snapshot()?;
        let automation = snapshot
            .automations
            .get(automation_id)
            .ok_or_else(|| AutomationsError::AutomationNotFound(automation_id.to_string()))?;
        let revision = automation.revisions.get(&revision_id).ok_or_else(|| {
            AutomationsError::RevisionNotFound {
                automation_id: automation_id.to_string(),
                revision_id,
            }
        })?;
        let required = capability_for_effect(effect);
        if revision.descriptor.capabilities.contains(&required) {
            return Ok(());
        }
        Err(AutomationsError::MissingCapability {
            automation_id: automation_id.to_string(),
            capability: format!("{required:?}"),
        })
    }
}

fn capability_for_effect(kind: &EffectKind) -> Capability {
    match kind {
        EffectKind::ScheduleWakeup { .. } | EffectKind::CancelWakeup { .. } => {
            Capability::ScheduleWakeups
        }
        EffectKind::EmitNotification { .. } => Capability::EmitNotifications,
        EffectKind::FetchHttp { .. } => Capability::FetchHttp,
        EffectKind::ReadProfile { .. } => Capability::ReadProfile,
        EffectKind::RunCommand { .. } => Capability::RunCommand,
        EffectKind::RunModel { .. } => Capability::RunModel,
    }
}

fn push_trace(state: &mut ControlPlaneState, at: i64, event: TraceEvent) {
    state.traces.push(TraceRecord { at, event });
}
