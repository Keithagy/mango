use std::{fs, os::unix::fs::PermissionsExt, path::Path};

use mango_automations::{AutomationBundleManifest, InferenceRegistry, ToolRegistry};
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn telegram_chat_bundle_tool_binding_invokes_successfully() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root");
    let mut manifest = AutomationBundleManifest::load(
        &workspace_root.join("examples/telegram-chat-expense-bundle/bundle.toml"),
    )
    .expect("manifest");
    manifest
        .ensure_artifacts_built(workspace_root)
        .expect("bundle artifacts");

    let tempdir = tempdir().expect("tempdir");
    for binding in &mut manifest.tools {
        if binding.slug == "expense.markdown_store" {
            binding.config = json!({ "state_root": tempdir.path() });
        }
    }

    let registry = ToolRegistry::new();
    registry.register_bindings(&manifest.tools);
    let output = registry
        .invoke("expense.markdown_store", json!({ "kind": "list_active" }))
        .await
        .expect("tool invocation should succeed");

    assert_eq!(output, json!({ "kind": "expenses", "expenses": [] }));
}

#[tokio::test]
async fn telegram_chat_bundle_receipt_extractor_binding_invokes_successfully() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root");
    let mut manifest = AutomationBundleManifest::load(
        &workspace_root.join("examples/telegram-chat-expense-bundle/bundle.toml"),
    )
    .expect("manifest");
    manifest
        .ensure_artifacts_built(workspace_root)
        .expect("bundle artifacts");

    let tempdir = tempdir().expect("tempdir");
    let ocr_script = tempdir.path().join("fake-ocr.sh");
    write_executable(
        &ocr_script,
        "#!/bin/sh\nprintf 'Acme Lunch\\nSGD 12.50\\n'\n",
    );

    for binding in &mut manifest.inference {
        if binding.slug == "expense.receipt_extractor" {
            binding.config = json!({ "ocr_executable": ocr_script.display().to_string() });
        }
    }

    let registry = InferenceRegistry::new();
    registry.register_bindings(&manifest.inference);
    let output = registry
        .invoke(
            "expense.receipt_extractor",
            json!({
                "local_path": tempdir.path().join("receipt.jpg").display().to_string(),
                "caption": "receipt",
            }),
        )
        .await
        .expect("extractor invocation should succeed");

    assert_eq!(
        output,
        json!({
            "local_path": tempdir.path().join("receipt.jpg").display().to_string(),
            "caption": "receipt",
            "ocr_text": "Acme Lunch\nSGD 12.50",
            "looks_like_expense": true,
            "merchant": "Acme Lunch",
            "amount": "12.50",
            "currency": "SGD",
            "spent_at": null,
        })
    );
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).expect("script");
    let mut permissions = fs::metadata(path).expect("metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod");
}
