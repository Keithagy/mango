use std::sync::Arc;

use anyhow::{Context, Result};
use mango_core::agent::AgentRuntime;
use mango_debate_poc::{
    BrowserEgress, BrowserIngress, ClaudeDebater, DebateBus, DebateControl, DebateRuntime,
    DebateStatusWorker, browser_router, browser_session,
};
use mango_example_support::{
    ConcurrentBusWorkers, ExampleBridge, ExampleSubstrate, ExampleSurface,
};
use mango_shim_claude_agent::{ClaudeAgentBridge, ClaudeAgentConfig};
use mango_shim_codex::{CodexAgentBridge, CodexAgentConfig};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let session = browser_session();

    let claude = ClaudeAgentBridge::spawn(
        ClaudeAgentConfig::new(
            cwd.clone(),
            format!("{}-claude", session.session_id),
            "claude",
        )
        .with_model("haiku")
        .with_tools(vec![])
        .with_ephemeral_session()
        .with_one_shot_turns()
        .with_system_prompt_append(
            "You are the Claude side of a Mango debate demo. Keep every answer brief, direct, and free of bullet lists.",
        ),
    )
    .context("failed to start Claude bridge")?;

    let codex = CodexAgentBridge::spawn(
        CodexAgentConfig::new(cwd, "codex")
            .with_model("gpt-5.4-mini")
            .with_sandbox_mode("read-only")
            .with_approval_policy("never"),
    )
    .context("failed to start Codex bridge")?;

    let runtime = DebateRuntime::new(
        ExampleSubstrate::new(DebateBus::new(1024), DebateControl::new(session.clone())),
        ExampleSurface::new(
            BrowserIngress::new(),
            BrowserEgress::new(1024),
            ConcurrentBusWorkers::new(
                "presentation",
                DebateStatusWorker::new(session.clone()),
                mango_debate_poc::DebateProjector::new(session.clone()),
            ),
        ),
        ExampleBridge::new(
            ConcurrentBusWorkers::new(
                "inference",
                ClaudeDebater::new(session.clone(), claude),
                mango_debate_poc::CodexDebater::new(session.clone(), codex),
            ),
            mango_debate_poc::NoopToolsWorker::new(),
        ),
    );

    let runtime = Arc::new(runtime);
    runtime.startup(session.clone()).await?;
    tokio::spawn({
        let runtime = runtime.clone();
        let session = session.clone();
        async move {
            if let Err(error) = runtime.run_session(session).await {
                tracing::error!("mango-debate-poc runtime failed: {error}");
            }
        }
    });

    let app = browser_router(runtime);
    let bind_addr =
        std::env::var("MANGO_DEBATE_POC_ADDR").unwrap_or_else(|_| "127.0.0.1:3002".to_string());
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind {bind_addr}"))?;

    info!("mango-debate-poc listening on http://{bind_addr}");
    axum::serve(listener, app).await.context("server error")
}
