use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use mango_automation_control::{
    ActivationMode, AutomationsControlPlane, AutomationsError, EffectHandler, EffectHandlerOutcome,
    JsonFileControlPlaneStore, ManualClock, RegistrationRequest, TraceEvent,
};
use mango_automation_sdk::{EffectKind, EffectRequest};
use serde_json::Value;

const START_TIME: i64 = 1_774_087_080; // 2026-03-31T08:58:00Z

#[derive(Debug, Clone, Default)]
struct DemoEffectHandler {
    notifications: Arc<Mutex<Vec<String>>>,
}

impl DemoEffectHandler {
    fn notifications(&self) -> Vec<String> {
        self.notifications
            .lock()
            .expect("notifications lock")
            .clone()
    }
}

#[async_trait]
impl EffectHandler for DemoEffectHandler {
    async fn handle_effect(
        &self,
        automation_id: &str,
        revision_id: u64,
        effect: &EffectRequest,
        now: i64,
    ) -> Result<EffectHandlerOutcome, AutomationsError> {
        match &effect.kind {
            EffectKind::EmitNotification {
                channel,
                title,
                body,
                metadata: _,
            } => {
                let rendered = format!(
                    "[{now}] automation={automation_id} revision={revision_id} channel={channel} title={title} body={body}"
                );
                println!("{rendered}");
                self.notifications
                    .lock()
                    .expect("notifications lock")
                    .push(rendered);
                Ok(EffectHandlerOutcome::default())
            }
            other => Err(AutomationsError::Io(format!(
                "demo handler does not implement external effect {other:?}"
            ))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let workspace_root = workspace_root()?;
    let artifact_path = build_demo_guest(&workspace_root)?;
    let state_path = workspace_root.join("target/automation-demo-state.json");
    if state_path.exists() {
        std::fs::remove_file(&state_path)
            .with_context(|| format!("failed to remove old state file {}", state_path.display()))?;
    }

    let handler = DemoEffectHandler::default();
    let clock = ManualClock::new(START_TIME);
    run_demo_session(&state_path, &artifact_path, &clock, handler.clone()).await?;

    println!("\nDemo notifications:");
    for notification in handler.notifications() {
        println!("  {notification}");
    }

    let store = JsonFileControlPlaneStore::new(&state_path);
    let control_plane = AutomationsControlPlane::new(store, handler, clock);
    let snapshot = control_plane.control_plane_snapshot()?;

    println!(
        "\nFinal control-plane snapshot stored at {}",
        state_path.display()
    );
    for (automation_id, automation) in snapshot.automations {
        println!(
            "automation={} active_revision={:?} last_status={:?} state={}",
            automation_id,
            automation.active_revision_id,
            automation.last_status,
            automation
                .current_state
                .as_ref()
                .map_or_else(|| "null".to_string(), Value::to_string)
        );
    }

    println!("\nRecent traces:");
    for trace in control_plane.traces()?.into_iter().rev().take(8).rev() {
        println!("  [{}] {:?}", trace.at, trace.event);
    }

    Ok(())
}

async fn run_demo_session(
    state_path: &Path,
    artifact_path: &Path,
    clock: &ManualClock,
    handler: DemoEffectHandler,
) -> Result<()> {
    {
        let control_plane = AutomationsControlPlane::new(
            JsonFileControlPlaneStore::new(state_path),
            handler.clone(),
            clock.clone(),
        );
        let revision = control_plane.register_revision(&RegistrationRequest {
            automation_id: "demo".to_string(),
            artifact_path: artifact_path.to_path_buf(),
            config: Value::Null,
        })?;
        control_plane
            .activate_revision("demo", revision.revision_id, ActivationMode::ColdStart)
            .await?;
        clock.advance_by(120);
        control_plane.reconcile_due().await?;
    }

    {
        let control_plane = AutomationsControlPlane::new(
            JsonFileControlPlaneStore::new(state_path),
            handler,
            clock.clone(),
        );
        clock.advance_by(60);
        control_plane.reconcile_due().await?;
        control_plane
            .submit_user_signal("demo", "confirm_water", Value::Null)
            .await?;
        clock.advance_by(60);
        control_plane.reconcile_due().await?;

        let traces = control_plane.traces()?;
        assert!(traces.iter().any(|trace| matches!(
            trace.event,
            TraceEvent::WakeupCancelled { ref wakeup_id, .. } if wakeup_id == "ping"
        )));
    }

    Ok(())
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("failed to locate workspace root from automation-demo manifest directory")
}

fn build_demo_guest(workspace_root: &Path) -> Result<PathBuf> {
    let status = Command::new("cargo")
        .arg("build")
        .arg("--manifest-path")
        .arg("examples/automation-demo/guests/hydration-automation/Cargo.toml")
        .arg("--target")
        .arg("wasm32-unknown-unknown")
        .current_dir(workspace_root)
        .status()
        .context("failed to spawn cargo build for hydration-automation guest")?;
    if !status.success() {
        anyhow::bail!("hydration-automation build failed with status {status}");
    }

    let artifact = workspace_root.join(
        "examples/automation-demo/guests/hydration-automation/target/wasm32-unknown-unknown/debug/hydration_automation.wasm",
    );
    if !artifact.exists() {
        anyhow::bail!(
            "guest artifact was not produced at expected path {}",
            artifact.display()
        );
    }
    Ok(artifact)
}
