use std::sync::Arc;

use anyhow::{Context, Result};
use mango_code_agent::{
    ClaudeCodingInference, CodeAgentRuntime, CodeBus, CodingProjector, CodingToolsWorker,
    ConsoleEvent, PromptControl, PromptIngress, TerminalEgress, ThinkingStatusWorker, cli_session,
};
use mango_core::agent::AgentRuntime;
use mango_example_support::{ConcurrentBusWorkers, ExampleWorkers};
use mango_shim_claude_agent::{ClaudeAgentBridge, ClaudeAgentConfig};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let prompt = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    if prompt.trim().is_empty() {
        anyhow::bail!("usage: cargo run -p mango-code-agent -- \"your prompt\"");
    }

    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let session = cli_session();
    let bridge = ClaudeAgentBridge::spawn(
        ClaudeAgentConfig::new(cwd.clone(), session.session_id.to_string(), "claude")
            .with_tools(Vec::<String>::new())
            .with_mango_coding_tools()
            .with_system_prompt_append("You are a minimal Mango coding agent. This session exposes Mango-owned MCP tools named bash, read_file, write_file, glob, and grep. When the user asks you to inspect or modify the workspace, use those tools directly instead of talking about needing approval. Keep the final explanation concise."),
    )
    .context("failed to start Claude bridge")?;

    let runtime = Arc::new(CodeAgentRuntime::new(
        CodeBus::new(1024),
        session.clone(),
        ExampleWorkers::new(
            PromptIngress::new(prompt),
            TerminalEgress::new(1024),
            PromptControl::new(session.clone()),
            ClaudeCodingInference::new(session.clone(), bridge.clone()),
            CodingToolsWorker::new(session.clone(), bridge.clone(), cwd.clone()),
            ConcurrentBusWorkers::new(
                "presentation",
                ThinkingStatusWorker::new(session.clone()),
                CodingProjector::new(session.clone()),
            ),
        ),
    ));

    let mut console = runtime.egress().subscribe();
    let mut task = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.run_session(runtime.session().clone()).await }
    });

    loop {
        tokio::select! {
            result = &mut task => {
                result??;
                break;
            }
            event = console.recv() => {
                let Ok(event) = event else {
                    break;
                };
                match event {
                    ConsoleEvent::InputEcho(text) => println!("> {text}"),
                    ConsoleEvent::Status(text) => println!("[status] {text}"),
                    ConsoleEvent::StatusClear => println!("[status] done"),
                    ConsoleEvent::Tool(text) => println!("[tool] {text}"),
                    ConsoleEvent::AssistantToken(text) => print!("{text}"),
                    ConsoleEvent::Error(text) => eprintln!("[error] {text}"),
                }
            }
        }
    }

    println!();
    Ok(())
}
