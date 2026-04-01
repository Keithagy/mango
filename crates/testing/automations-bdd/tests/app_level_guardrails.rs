use std::{fs, path::PathBuf};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("workspace root should exist")
        .to_path_buf()
}

fn test_modules(source: &str) -> String {
    source
        .split("\n#[cfg(test)]\nmod ")
        .skip(1)
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn telegram_automations_tests_must_flow_through_pocket_universe() {
    let source = fs::read_to_string(
        workspace_root().join("examples/telegram-automations/src/lib.rs"),
    )
    .expect("telegram-automations source should be readable");
    let tests = test_modules(&source);

    assert!(
        tests.contains("PocketUniverse::new("),
        "telegram-automations tests should construct the pocket universe"
    );
    assert!(
        tests.contains("advance_time_by_and_settle("),
        "telegram-automations tests should advance simulated time through the BDD helper"
    );

    for forbidden in [
        "AutomationsControlPlane::new(",
        "AutomationsControlPlane::with_runtime(",
        ".reconcile_due().await",
    ] {
        assert!(
            !tests.contains(forbidden),
            "telegram-automations tests should not bypass the pocket universe with `{forbidden}`"
        );
    }
}

#[test]
fn automation_demo_must_use_pocket_universe_as_the_exemplar_surface() {
    let source =
        fs::read_to_string(workspace_root().join("examples/automation-demo/src/main.rs"))
            .expect("automation-demo source should be readable");

    assert!(
        source.contains("PocketUniverse::new("),
        "automation-demo should model automation behavior through the pocket universe"
    );

    for forbidden in [
        "AutomationsControlPlane::new(",
        "AutomationsControlPlane::with_runtime(",
        ".reconcile_due().await",
    ] {
        assert!(
            !source.contains(forbidden),
            "automation-demo should not bypass the pocket universe with `{forbidden}`"
        );
    }
}
