use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use mango_automations::{CapabilityBinding, CapabilityTransport, InferenceRegistry, ToolRegistry};
use serde_json::json;
use telegram_chat_expense_bundle::ReceiptExtraction;
use tempfile::tempdir;

#[tokio::test]
async fn expense_provider_responds_over_command_transport() {
    let tempdir = tempdir().expect("tempdir");
    let registry = ToolRegistry::new();
    registry.register_binding(&CapabilityBinding {
        slug: "expense.markdown_store".to_string(),
        config: json!({ "state_root": tempdir.path() }),
        transport: CapabilityTransport::Command {
            program: PathBuf::from(env!("CARGO_BIN_EXE_expense-bundle-provider")),
            args: Vec::new(),
            env: BTreeMap::default(),
        },
    });

    let output = registry
        .invoke("expense.markdown_store", json!({ "kind": "list_active" }))
        .await
        .expect("provider should respond");

    assert_eq!(output, json!({ "kind": "expenses", "expenses": [] }));
}

#[tokio::test]
async fn expense_router_captures_ocr_stdout_without_corrupting_transport() {
    let tempdir = tempdir().expect("tempdir");
    let ocr_script = tempdir.path().join("fake-ocr.sh");
    write_executable(
        &ocr_script,
        "#!/bin/sh\nprintf 'Acme Lunch\\nSGD 12.50\\n'\n",
    );

    let registry = InferenceRegistry::new();
    registry.register_binding(&CapabilityBinding {
        slug: "expense.receipt_extractor".to_string(),
        config: json!({ "ocr_executable": ocr_script.display().to_string() }),
        transport: CapabilityTransport::Command {
            program: PathBuf::from(env!("CARGO_BIN_EXE_expense-bundle-provider")),
            args: Vec::new(),
            env: BTreeMap::default(),
        },
    });

    let output = registry
        .invoke(
            "expense.receipt_extractor",
            json!({
                "local_path": tempdir.path().join("receipt.jpg").display().to_string(),
                "caption": "had these expenses today"
            }),
        )
        .await
        .expect("provider should respond with valid json");
    let extraction: ReceiptExtraction =
        serde_json::from_value(output).expect("extractor output should decode");

    assert!(extraction.looks_like_expense);
    assert_eq!(extraction.merchant.as_deref(), Some("Acme Lunch"));
    assert_eq!(extraction.amount.as_deref(), Some("12.50"));
    assert_eq!(extraction.currency.as_deref(), Some("SGD"));
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).expect("script should write");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let mut permissions = fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod");
    }
}
