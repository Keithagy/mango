use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::guest::{AutomationRuntime, WasmAutomationRuntime};
use crate::{
    ActivationMode, AutomationBundleManifest, AutomationEvent, AutomationsControlPlane,
    AutomationsError, Clock, ControlPlaneStore, EffectHandler, EffectHandlerOutcome, EffectKind,
    EffectObservation, EffectRequest, EventDisposition, InferenceRegistry,
    JsonFileControlPlaneStore, ManagedAutomation, RegistrationRequest, SystemClock, ToolRegistry,
    TraceRecord,
};

#[derive(Debug, Clone, PartialEq)]
pub struct BundleTriggerEvent {
    pub name: String,
    pub payload: Value,
}

impl BundleTriggerEvent {
    #[must_use]
    pub fn new(name: impl Into<String>, payload: Value) -> Self {
        Self {
            name: name.into(),
            payload,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BundleDispatchOutcome {
    pub handled_automations: Vec<String>,
    pub observations: Vec<EffectObservation>,
}

pub type FileBackedBundleAutomationHost =
    AutomationBundleHost<JsonFileControlPlaneStore, WasmAutomationRuntime, SystemClock>;

#[derive(Debug, Clone)]
pub struct AutomationBundleHost<B, R, C> {
    control_plane: AutomationsControlPlane<B, R, BundleEffectHandler, C>,
    bundles: Vec<LoadedBundle>,
    bindings: BundleBindingRegistry,
}

impl<B, C> AutomationBundleHost<B, WasmAutomationRuntime, C>
where
    B: ControlPlaneStore,
    C: Clock,
{
    #[must_use]
    pub fn new(
        store: B,
        bundles: Vec<AutomationBundleManifest>,
        host_context: &Value,
        clock: C,
    ) -> Self {
        Self::with_runtime(
            store,
            WasmAutomationRuntime::new(),
            bundles,
            host_context,
            clock,
        )
    }
}

impl<B, R, C> AutomationBundleHost<B, R, C>
where
    B: ControlPlaneStore,
    R: AutomationRuntime,
    C: Clock,
{
    #[must_use]
    pub fn with_runtime(
        store: B,
        runtime: R,
        bundles: Vec<AutomationBundleManifest>,
        host_context: &Value,
        clock: C,
    ) -> Self {
        let bindings = BundleBindingRegistry::default();
        let loaded_bundles = bundles
            .into_iter()
            .map(|bundle| LoadedBundle::new(bundle, host_context))
            .collect();
        let effect_handler = BundleEffectHandler {
            bindings: bindings.clone(),
        };

        Self {
            control_plane: AutomationsControlPlane::with_runtime(
                store,
                runtime,
                effect_handler,
                clock,
            ),
            bundles: loaded_bundles,
            bindings,
        }
    }

    /// Dispatch normalized runtime triggers to every bundle that subscribed to
    /// them for the provided scope key.
    ///
    /// The host remains generic here: callers choose the scope key shape,
    /// normalize ingress into string trigger names plus JSON payloads, and map
    /// resulting observations back into their own presentation layer.
    ///
    /// # Errors
    ///
    /// Returns an error if bundle activation, guest execution, or capability
    /// provider invocation fails.
    pub async fn dispatch_triggers(
        &self,
        scope_key: &str,
        triggers: &[BundleTriggerEvent],
    ) -> Result<BundleDispatchOutcome, AutomationsError> {
        let mut handled_automations = Vec::new();
        let mut observations = Vec::new();

        for trigger in triggers {
            for bundle in self
                .bundles
                .iter()
                .filter(|bundle| bundle.trigger_subscriptions.contains(trigger.name.as_str()))
            {
                let automation_id = self.ensure_bundle_active(bundle, scope_key).await?;
                let submission = self
                    .control_plane
                    .submit_trigger(&automation_id, &trigger.name, trigger.payload.clone())
                    .await?;

                if submission.disposition == EventDisposition::Handled
                    && !handled_automations
                        .iter()
                        .any(|value| value == &automation_id)
                {
                    handled_automations.push(automation_id.clone());
                }
                observations.extend(submission.observations);
            }
        }

        Ok(BundleDispatchOutcome {
            handled_automations,
            observations,
        })
    }

    /// Return the current managed automation map.
    ///
    /// # Errors
    ///
    /// Returns an error if the control-plane snapshot cannot be read.
    pub fn automations(
        &self,
    ) -> Result<std::collections::BTreeMap<String, ManagedAutomation>, AutomationsError> {
        self.control_plane.automations()
    }

    /// Return the control-plane trace history for all managed bundle-backed
    /// automations.
    ///
    /// # Errors
    ///
    /// Returns an error if the control-plane state cannot be read.
    pub fn traces(&self) -> Result<Vec<TraceRecord>, AutomationsError> {
        self.control_plane.traces()
    }

    async fn ensure_bundle_active(
        &self,
        bundle: &LoadedBundle,
        scope_key: &str,
    ) -> Result<String, AutomationsError> {
        let automation_id = automation_id_for_bundle_scope(&bundle.manifest.name, scope_key);
        self.bindings
            .bind(&automation_id, Arc::clone(&bundle.runtime_bindings))?;

        let automations = self.control_plane.automations()?;

        if let Some(automation) = automations.get(&automation_id) {
            if automation.active_revision_id.is_some() {
                return Ok(automation_id);
            }
            let latest_revision_id =
                automation.revisions.keys().max().copied().ok_or_else(|| {
                    AutomationsError::State(format!(
                        "automation `{automation_id}` has no registered revisions"
                    ))
                })?;
            self.control_plane
                .activate_revision(
                    &automation_id,
                    latest_revision_id,
                    ActivationMode::PreserveState,
                )
                .await?;
            return Ok(automation_id);
        }

        let revision = self.control_plane.register_revision(&RegistrationRequest {
            automation_id: automation_id.clone(),
            artifact_path: bundle.manifest.artifact.clone(),
            config: json!({
                "bundle_name": bundle.manifest.name,
                "scope_key": scope_key,
            }),
        })?;
        self.control_plane
            .activate_revision(
                &automation_id,
                revision.revision_id,
                ActivationMode::ColdStart,
            )
            .await?;
        Ok(automation_id)
    }
}

#[must_use]
pub fn automation_id_for_bundle_scope(bundle_name: &str, scope_key: &str) -> String {
    format!("{bundle_name}/{scope_key}")
}

#[derive(Debug, Clone)]
struct LoadedBundle {
    manifest: AutomationBundleManifest,
    trigger_subscriptions: BTreeSet<String>,
    runtime_bindings: Arc<BundleRuntimeBindings>,
}

impl LoadedBundle {
    fn new(manifest: AutomationBundleManifest, host_context: &Value) -> Self {
        let tools = ToolRegistry::new();
        let inference = InferenceRegistry::new();
        let prepared_tools = prepare_bindings(&manifest.tools, host_context);
        let prepared_inference = prepare_bindings(&manifest.inference, host_context);

        tools.register_bindings(&prepared_tools);
        inference.register_bindings(&prepared_inference);

        Self {
            trigger_subscriptions: manifest.trigger_subscriptions.iter().cloned().collect(),
            manifest,
            runtime_bindings: Arc::new(BundleRuntimeBindings { tools, inference }),
        }
    }
}

#[derive(Debug, Clone)]
struct BundleRuntimeBindings {
    tools: ToolRegistry,
    inference: InferenceRegistry,
}

#[derive(Debug, Clone, Default)]
struct BundleBindingRegistry {
    bindings: Arc<Mutex<HashMap<String, Arc<BundleRuntimeBindings>>>>,
}

impl BundleBindingRegistry {
    fn bind(
        &self,
        automation_id: &str,
        bindings: Arc<BundleRuntimeBindings>,
    ) -> Result<(), AutomationsError> {
        self.bindings
            .lock()
            .map_err(|_| {
                AutomationsError::State("bundle binding registry was poisoned".to_string())
            })?
            .insert(automation_id.to_string(), bindings);
        Ok(())
    }

    fn resolve(&self, automation_id: &str) -> Result<Arc<BundleRuntimeBindings>, AutomationsError> {
        self.bindings
            .lock()
            .map_err(|_| {
                AutomationsError::State("bundle binding registry was poisoned".to_string())
            })?
            .get(automation_id)
            .cloned()
            .ok_or_else(|| {
                AutomationsError::Provider(format!(
                    "no runtime bindings registered for automation `{automation_id}`"
                ))
            })
    }
}

#[derive(Debug, Clone)]
struct BundleEffectHandler {
    bindings: BundleBindingRegistry,
}

#[async_trait]
impl EffectHandler for BundleEffectHandler {
    async fn handle_effect(
        &self,
        automation_id: &str,
        _revision_id: u64,
        effect: &EffectRequest,
        now: i64,
    ) -> Result<EffectHandlerOutcome, AutomationsError> {
        match &effect.kind {
            EffectKind::EmitNotification {
                channel,
                title,
                body,
                metadata,
            } => Ok(EffectHandlerOutcome {
                follow_up_events: Vec::new(),
                observations: vec![EffectObservation::Notification {
                    effect_id: effect.effect_id.clone(),
                    channel: channel.clone(),
                    title: title.clone(),
                    body: body.clone(),
                    metadata: metadata.clone(),
                }],
            }),
            EffectKind::CallTool { slug, input } => {
                let bindings = self.bindings.resolve(automation_id)?;
                let result = bindings
                    .tools
                    .invoke(slug, input.clone())
                    .await
                    .map_or_else(
                        |error| mango_automation_protocol::EffectResult::Err(error.to_string()),
                        mango_automation_protocol::EffectResult::Ok,
                    );
                Ok(EffectHandlerOutcome {
                    follow_up_events: vec![AutomationEvent::EffectCompleted {
                        effect_id: effect.effect_id.clone(),
                        result,
                        at: now,
                    }],
                    observations: Vec::new(),
                })
            }
            EffectKind::RunInference { capability, input } => {
                let bindings = self.bindings.resolve(automation_id)?;
                let result = bindings
                    .inference
                    .invoke(capability, input.clone())
                    .await
                    .map_or_else(
                        |error| mango_automation_protocol::EffectResult::Err(error.to_string()),
                        mango_automation_protocol::EffectResult::Ok,
                    );
                Ok(EffectHandlerOutcome {
                    follow_up_events: vec![AutomationEvent::EffectCompleted {
                        effect_id: effect.effect_id.clone(),
                        result,
                        at: now,
                    }],
                    observations: Vec::new(),
                })
            }
            other => Err(AutomationsError::Provider(format!(
                "automation bundle host does not implement effect {other:?}"
            ))),
        }
    }
}

fn prepare_bindings(
    bindings: &[crate::CapabilityBinding],
    host_context: &Value,
) -> Vec<crate::CapabilityBinding> {
    bindings
        .iter()
        .cloned()
        .map(|mut binding| {
            binding.config = config_with_host_context(&binding.config, host_context);
            binding
        })
        .collect()
}

fn config_with_host_context(config: &Value, host_context: &Value) -> Value {
    match config {
        Value::Object(object) => {
            let mut merged = object.clone();
            merged.insert("host".to_string(), host_context.clone());
            Value::Object(merged)
        }
        Value::Null => json!({ "host": host_context }),
        other => json!({
            "binding": other,
            "host": host_context,
        }),
    }
}
