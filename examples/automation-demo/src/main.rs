use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use mango_automation_control::{
    ActivationMode, AutomationsError, EffectHandler, EffectHandlerOutcome, PocketUniverse,
    RegistrationRequest, TraceEvent, WasmAutomationRuntime,
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
    let handler = DemoEffectHandler::default();
    let universe = PocketUniverse::new(START_TIME, WasmAutomationRuntime::new(), handler.clone());
    run_demo_session(&universe, &artifact_path).await?;

    println!("\nDemo notifications:");
    for notification in handler.notifications() {
        println!("  {notification}");
    }

    let snapshot = universe.snapshot()?;
    println!("\nFinal pocket-universe snapshot:");
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
    for trace in universe.traces()?.into_iter().rev().take(8).rev() {
        println!("  [{}] {:?}", trace.at, trace.event);
    }

    Ok(())
}

async fn run_demo_session(
    universe: &PocketUniverse<WasmAutomationRuntime, DemoEffectHandler>,
    artifact_path: &Path,
) -> Result<()> {
    let revision = universe.register_revision(&RegistrationRequest {
        automation_id: "demo".to_string(),
        artifact_path: artifact_path.to_path_buf(),
        config: Value::Null,
    })?;
    universe
        .activate_revision("demo", revision.revision_id, ActivationMode::ColdStart)
        .await?;
    universe.advance_time_by_and_settle(120).await?;
    universe.advance_time_by_and_settle(60).await?;
    universe
        .submit_user_signal("demo", "confirm_water", Value::Null)
        .await?;
    universe.advance_time_by_and_settle(60).await?;

    let traces = universe.traces()?;
    assert!(traces.iter().any(|trace| matches!(
        trace.event,
        TraceEvent::WakeupCancelled { ref wakeup_id, .. } if wakeup_id == "ping"
    )));

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
