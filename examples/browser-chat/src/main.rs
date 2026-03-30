use std::sync::Arc;

use anyhow::{Context, Result};
use browser_chat::{
    BrowserEgress, BrowserIngress, ChatBus, ChatRuntime, ClaudeChatInference, SimpleChatControl,
    ThinkingStatusWorker, browser_router, browser_session,
};
use example_support::{ConcurrentBusWorkers, ExampleBridge, ExampleSubstrate, ExampleSurface};
use mango_core::agent::AgentRuntime;
use mango_shim_claude_agent::{ClaudeAgentBridge, ClaudeAgentConfig};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let session = browser_session();
    let bridge = ClaudeAgentBridge::spawn(
        ClaudeAgentConfig::new(cwd, session.session_id.to_string(), "claude")
            .with_tools(vec![])
            .with_system_prompt_append("You are the assistant in the Mango browser chat example."),
    )
    .context("failed to start Claude bridge")?;

    let runtime = ChatRuntime::new(
        ExampleSubstrate::new(ChatBus::new(1024), SimpleChatControl::new(session.clone())),
        ExampleSurface::new(
            BrowserIngress::new(),
            BrowserEgress::new(1024),
            ConcurrentBusWorkers::new(
                "presentation",
                ThinkingStatusWorker::new(session.clone()),
                browser_chat::ChatProjector::new(session.clone()),
            ),
        ),
        ExampleBridge::new(
            ClaudeChatInference::new(session.clone(), bridge),
            browser_chat::NoopToolsWorker::new(),
        ),
    );

    let runtime = Arc::new(runtime);
    runtime.startup(session.clone()).await?;
    tokio::spawn({
        let runtime = runtime.clone();
        let session = session.clone();
        async move {
            if let Err(error) = runtime.run_session(session).await {
                tracing::error!("browser-chat runtime failed: {error}");
            }
        }
    });

    let app = browser_router(runtime);
    let bind_addr =
        std::env::var("MANGO_BROWSER_CHAT_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind {bind_addr}"))?;

    info!("browser-chat listening on http://{bind_addr}");
    axum::serve(listener, app).await.context("server error")
}
