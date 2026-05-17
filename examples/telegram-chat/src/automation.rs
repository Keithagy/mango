use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use mango_automations::{
    AutomationBundleHost, AutomationBundleManifest, BundleTriggerEvent, EffectObservation,
    JsonFileControlPlaneStore, SystemClock, WasmAutomationRuntime,
};
use mango_telegram::TelegramSurface;
use serde_json::{Value, json};

use crate::{ChatInput, ChatInputContent};

const DEFAULT_BUNDLE_MANIFESTS: &[&str] = &["examples/telegram-chat-expense-bundle/bundle.toml"];
const TELEGRAM_TEXT_TRIGGER: &str = "telegram.text_received";
const TELEGRAM_PHOTO_TRIGGER: &str = "telegram.photo_received";

type BundleHost =
    AutomationBundleHost<JsonFileControlPlaneStore, WasmAutomationRuntime, SystemClock>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AutomationDispatchOutcome {
    pub handled: bool,
    pub handled_automations: Vec<String>,
    pub response: Option<String>,
}

#[async_trait]
pub trait AutomationTurnDispatcher: Send + Sync {
    async fn dispatch(
        &self,
        surface: &TelegramSurface,
        input: &ChatInput,
    ) -> Result<AutomationDispatchOutcome, String>;
}

#[derive(Debug, Clone, Default)]
pub struct NoopAutomationDispatcher;

#[async_trait]
impl AutomationTurnDispatcher for NoopAutomationDispatcher {
    async fn dispatch(
        &self,
        _surface: &TelegramSurface,
        _input: &ChatInput,
    ) -> Result<AutomationDispatchOutcome, String> {
        Ok(AutomationDispatchOutcome::default())
    }
}

#[derive(Debug, Clone)]
pub struct BundleAutomationDispatcher {
    host: BundleHost,
}

impl BundleAutomationDispatcher {
    #[must_use]
    pub fn new(
        control_plane_state_path: PathBuf,
        bundles: Vec<AutomationBundleManifest>,
        host_context: &Value,
    ) -> Self {
        Self {
            host: AutomationBundleHost::new(
                JsonFileControlPlaneStore::new(control_plane_state_path),
                bundles,
                host_context,
                SystemClock,
            ),
        }
    }

    /// Load the default bundle manifests, auto-build their artifacts, and
    /// construct a dispatcher that only knows how to normalize Telegram turns
    /// into generic bundle triggers.
    ///
    /// # Errors
    ///
    /// Returns an error if bundle manifests cannot be loaded or their
    /// artifacts cannot be built.
    pub fn from_default_bundles(
        workspace_root: &Path,
        control_plane_state_path: PathBuf,
        host_context: &Value,
    ) -> Result<Self, String> {
        let manifests = load_default_bundle_manifests(workspace_root)?;
        Ok(Self::new(control_plane_state_path, manifests, host_context))
    }

    /// Load the provided bundle manifests, ensure their declared artifacts are
    /// built, and construct a dispatcher that only knows how to normalize
    /// Telegram turns into generic bundle triggers.
    ///
    /// # Errors
    ///
    /// Returns an error if bundle manifests cannot be loaded or their
    /// artifacts cannot be built.
    pub fn from_bundle_manifests(
        workspace_root: &Path,
        control_plane_state_path: PathBuf,
        manifest_paths: &[PathBuf],
        host_context: &Value,
    ) -> Result<Self, String> {
        let manifests = load_bundle_manifests(workspace_root, manifest_paths)?;
        Ok(Self::new(control_plane_state_path, manifests, host_context))
    }

    /// Return the control-plane trace history for all managed bundle-backed
    /// automations.
    ///
    /// # Errors
    ///
    /// Returns an error if the control-plane state cannot be read.
    pub fn traces(&self) -> Result<Vec<mango_automations::TraceRecord>, String> {
        self.host.traces().map_err(|error| error.to_string())
    }
}

#[async_trait]
impl AutomationTurnDispatcher for BundleAutomationDispatcher {
    async fn dispatch(
        &self,
        surface: &TelegramSurface,
        input: &ChatInput,
    ) -> Result<AutomationDispatchOutcome, String> {
        let scope_key = automation_scope_for_surface(surface);
        let outcome = self
            .host
            .dispatch_triggers(&scope_key, &normalized_trigger_events(input))
            .await
            .map_err(|error| error.to_string())?;

        Ok(AutomationDispatchOutcome {
            handled: !outcome.handled_automations.is_empty(),
            handled_automations: outcome.handled_automations,
            response: notification_response(&outcome.observations),
        })
    }
}

fn automation_scope_for_surface(surface: &TelegramSurface) -> String {
    format!(
        "{}/{}",
        surface.chat_id.0,
        surface
            .thread_id
            .map_or_else(|| "root".to_string(), |thread_id| thread_id.0.to_string())
    )
}

fn normalized_trigger_events(input: &ChatInput) -> Vec<BundleTriggerEvent> {
    match &input.content {
        ChatInputContent::Text { text } => vec![BundleTriggerEvent::new(
            TELEGRAM_TEXT_TRIGGER,
            json!({
                "text": text,
                "username": input.username,
                "display_name": input.display_name,
            }),
        )],
        ChatInputContent::Photo {
            local_path,
            caption,
        } => vec![BundleTriggerEvent::new(
            TELEGRAM_PHOTO_TRIGGER,
            json!({
                "local_path": local_path.display().to_string(),
                "caption": caption,
                "username": input.username,
                "display_name": input.display_name,
            }),
        )],
    }
}

fn notification_response(observations: &[EffectObservation]) -> Option<String> {
    let responses = observations
        .iter()
        .map(|observation| match observation {
            EffectObservation::Notification { body, .. } => body.clone(),
        })
        .collect::<Vec<_>>();
    (!responses.is_empty()).then_some(responses.join("\n"))
}

/// Load the default bundle manifests for `telegram-chat`.
///
/// # Errors
///
/// Returns an error if the default bundle artifacts cannot be built or any
/// manifest cannot be loaded.
pub fn load_default_bundle_manifests(
    workspace_root: &Path,
) -> Result<Vec<AutomationBundleManifest>, String> {
    load_bundle_manifests(workspace_root, default_bundle_manifest_paths())
}

#[must_use]
pub fn default_bundle_manifest_paths() -> &'static [PathBuf] {
    static DEFAULTS: std::sync::OnceLock<Vec<PathBuf>> = std::sync::OnceLock::new();
    DEFAULTS
        .get_or_init(|| {
            DEFAULT_BUNDLE_MANIFESTS
                .iter()
                .map(PathBuf::from)
                .collect::<Vec<_>>()
        })
        .as_slice()
}

/// Load bundle manifests from disk and ensure any manifest-declared build
/// steps have run.
///
/// # Errors
///
/// Returns an error if any manifest cannot be loaded or its declared build
/// steps fail.
pub fn load_bundle_manifests(
    workspace_root: &Path,
    manifest_paths: &[PathBuf],
) -> Result<Vec<AutomationBundleManifest>, String> {
    let manifests = manifest_paths
        .iter()
        .map(|manifest_path| {
            let path = if manifest_path.is_absolute() {
                manifest_path.clone()
            } else {
                workspace_root.join(manifest_path)
            };
            AutomationBundleManifest::load(&path).map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    for manifest in &manifests {
        manifest
            .ensure_artifacts_built(workspace_root)
            .map_err(|error| error.to_string())?;
    }
    Ok(manifests)
}

pub type SharedAutomationDispatcher = Arc<dyn AutomationTurnDispatcher>;
