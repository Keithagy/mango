use std::{
    fs,
    path::{Path, PathBuf},
};

use mango_automations::{
    AutomationBundleHost, AutomationBundleManifest, BundleTriggerEvent, JsonFileControlPlaneStore,
    SystemClock,
};
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn bundle_host_dispatches_manifest_subscribed_triggers() {
    let workspace_root = workspace_root();
    let manifest = AutomationBundleManifest::load(
        &workspace_root.join("examples/telegram-chat-expense-bundle/bundle.toml"),
    )
    .expect("manifest");
    manifest
        .ensure_artifacts_built(&workspace_root)
        .expect("bundle artifacts");
    let tempdir = tempdir().expect("tempdir");
    let state_root = tempdir.path().join("state");
    fs::create_dir_all(&state_root).expect("state root");
    let photo_path = state_root.join("receipt.jpg");
    fs::write(&photo_path, b"fixture-image").expect("fixture");

    let host = AutomationBundleHost::new(
        JsonFileControlPlaneStore::new(tempdir.path().join("automation-control-plane.json")),
        vec![manifest],
        &json!({
            "state_root": state_root.display().to_string(),
        }),
        SystemClock,
    );

    let outcome = host
        .dispatch_triggers(
            "chat/7/root",
            &[BundleTriggerEvent::new(
                "telegram.photo_received",
                json!({
                    "local_path": photo_path.display().to_string(),
                    "caption": "receipt",
                    "username": "trusted_customer",
                    "display_name": "Trusted Customer",
                }),
            )],
        )
        .await
        .expect("dispatch");

    assert!(!outcome.handled_automations.is_empty());
    let notifications = outcome
        .observations
        .into_iter()
        .map(|observation| match observation {
            mango_automations::EffectObservation::Notification { body, .. } => body,
        })
        .collect::<Vec<_>>();
    assert!(
        notifications
            .iter()
            .any(|body| body.contains("I think that is an expense"))
    );
}

#[tokio::test]
async fn bundle_host_ignores_unsubscribed_triggers() {
    let workspace_root = workspace_root();
    let mut manifest = AutomationBundleManifest::load(
        &workspace_root.join("examples/telegram-chat-expense-bundle/bundle.toml"),
    )
    .expect("manifest");
    manifest
        .ensure_artifacts_built(&workspace_root)
        .expect("bundle artifacts");
    manifest.trigger_subscriptions.clear();
    let tempdir = tempdir().expect("tempdir");

    let host = AutomationBundleHost::new(
        JsonFileControlPlaneStore::new(tempdir.path().join("automation-control-plane.json")),
        vec![manifest],
        &json!({
            "state_root": tempdir.path().join("state").display().to_string(),
        }),
        SystemClock,
    );

    let outcome = host
        .dispatch_triggers(
            "chat/7/root",
            &[BundleTriggerEvent::new(
                "telegram.photo_received",
                json!({
                    "local_path": tempdir.path().join("receipt.jpg").display().to_string(),
                    "caption": "receipt",
                    "username": "trusted_customer",
                    "display_name": "Trusted Customer",
                }),
            )],
        )
        .await
        .expect("dispatch");

    assert!(outcome.handled_automations.is_empty());
    assert!(outcome.observations.is_empty());
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("workspace root")
        .to_path_buf()
}
