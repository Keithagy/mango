use std::{path::Path, time::Duration};

use mango_automations::AutomationBundleManifest;
use mango_telegram::{TelegramChatId, TelegramSurface};
use serde_json::json;
use telegram_chat::{
    AutomationTurnDispatcher, BundleAutomationDispatcher, ChatInput, ChatInputContent,
};
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test]
async fn bundle_dispatcher_handles_receipt_photos_directly() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root");
    let tempdir = tempdir().expect("tempdir");
    let dispatcher = BundleAutomationDispatcher::from_default_bundles(
        workspace_root,
        tempdir.path().join("automation-control-plane.json"),
        &json!({
            "state_root": tempdir.path().join("state").display().to_string(),
        }),
    )
    .expect("dispatcher");
    let photo_path = tempdir.path().join("receipt.jpg");
    std::fs::write(&photo_path, b"fixture-image").expect("fixture");

    let outcome = timeout(
        Duration::from_secs(20),
        dispatcher.dispatch(
            &TelegramSurface {
                chat_id: TelegramChatId(7),
                thread_id: None,
                username: Some("trusted_customer".to_string()),
                display_name: "Trusted Customer".to_string(),
            },
            &ChatInput {
                username: Some("trusted_customer".to_string()),
                display_name: "Trusted Customer".to_string(),
                content: ChatInputContent::Photo {
                    local_path: photo_path,
                    caption: Some("receipt".to_string()),
                },
            },
        ),
    )
    .await
    .expect("dispatcher timed out")
    .expect("dispatch");

    assert!(outcome.handled);
    assert!(!outcome.handled_automations.is_empty());
    assert!(
        outcome
            .response
            .as_deref()
            .is_some_and(|response| response.contains("I think that is an expense"))
    );
}

#[tokio::test]
async fn bundle_dispatcher_respects_manifest_trigger_subscriptions() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root");
    let tempdir = tempdir().expect("tempdir");
    let mut manifest = AutomationBundleManifest::load(
        &workspace_root.join("examples/telegram-chat-expense-bundle/bundle.toml"),
    )
    .expect("manifest");
    manifest.trigger_subscriptions.clear();

    let dispatcher = BundleAutomationDispatcher::new(
        tempdir.path().join("automation-control-plane.json"),
        vec![manifest],
        &json!({
            "state_root": tempdir.path().join("state").display().to_string(),
        }),
    );

    let outcome = dispatcher
        .dispatch(
            &TelegramSurface {
                chat_id: TelegramChatId(7),
                thread_id: None,
                username: Some("trusted_customer".to_string()),
                display_name: "Trusted Customer".to_string(),
            },
            &ChatInput {
                username: Some("trusted_customer".to_string()),
                display_name: "Trusted Customer".to_string(),
                content: ChatInputContent::Photo {
                    local_path: tempdir.path().join("receipt.jpg"),
                    caption: Some("receipt".to_string()),
                },
            },
        )
        .await
        .expect("dispatch");

    assert!(!outcome.handled);
    assert!(outcome.response.is_none());
}

#[tokio::test]
async fn bundle_dispatcher_falls_through_for_general_text_without_error() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root");
    let tempdir = tempdir().expect("tempdir");
    let dispatcher = BundleAutomationDispatcher::from_default_bundles(
        workspace_root,
        tempdir.path().join("automation-control-plane.json"),
        &json!({
            "state_root": tempdir.path().join("state").display().to_string(),
        }),
    )
    .expect("dispatcher");

    let outcome = timeout(
        Duration::from_secs(20),
        dispatcher.dispatch(
            &TelegramSurface {
                chat_id: TelegramChatId(7),
                thread_id: None,
                username: Some("trusted_customer".to_string()),
                display_name: "Trusted Customer".to_string(),
            },
            &ChatInput {
                username: Some("trusted_customer".to_string()),
                display_name: "Trusted Customer".to_string(),
                content: ChatInputContent::Text {
                    text: "tell me a joke".to_string(),
                },
            },
        ),
    )
    .await
    .expect("dispatcher timed out")
    .expect("dispatch");

    assert!(!outcome.handled);
    assert!(outcome.response.is_none());
}
