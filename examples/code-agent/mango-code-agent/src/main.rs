use std::sync::Arc;

use anyhow::{Context, Result};
use mango_code_agent::{
    ClaudeCodingInference, CodeAgentRuntime, CodeBus, CodingProjector, CodingToolsWorker,
    ConsoleEvent, PromptControl, PromptIngress, TerminalEgress, ThinkingStatusWorker, cli_session,
};
use mango_core::agent::AgentRuntime;
use mango_example_support::{
    ConcurrentBusWorkers, ExampleBridge, ExampleSubstrate, ExampleSurface,
};
use mango_shim_claude_agent::{ClaudeAgentBridge, ClaudeAgentConfig};
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let prompt = std::env::args().skip(1).collect::<Vec<_>>().join(" ");

    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let session = cli_session();
    let ingress = PromptIngress::new(prompt);
    let bridge = ClaudeAgentBridge::spawn(
        ClaudeAgentConfig::new(cwd.clone(), session.session_id.to_string(), "claude")
            .with_tools(Vec::<String>::new())
            .with_mango_coding_tools()
            .with_system_prompt_append("You are a minimal Mango coding agent. This session exposes Mango-owned MCP tools named bash, read_file, write_file, glob, and grep. When the user asks you to inspect or modify the workspace, use those tools directly instead of talking about needing approval. Keep the final explanation concise."),
    )
    .context("failed to start Claude bridge")?;

    let runtime = Arc::new(CodeAgentRuntime::new(
        ExampleSubstrate::new(CodeBus::new(1024), PromptControl::new(session.clone())),
        ExampleSurface::new(
            ingress.clone(),
            TerminalEgress::new(1024),
            ConcurrentBusWorkers::new(
                "presentation",
                ThinkingStatusWorker::new(session.clone()),
                CodingProjector::new(session.clone()),
            ),
        ),
        ExampleBridge::new(
            ClaudeCodingInference::new(session.clone(), bridge.clone()),
            CodingToolsWorker::new(session.clone(), bridge.clone(), cwd.clone()),
        ),
    ));

    runtime.startup(session.clone()).await?;
    let mut console = runtime.surface().egress().subscribe();
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut stdin_closed = false;
    let mut task = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.run_session(session).await }
    });

    eprintln!("type a prompt and press enter");
    eprintln!("commands: /interrupt to cancel, /exit to quit");

    loop {
        tokio::select! {
            result = &mut task => {
                result??;
                break;
            }
            line = stdin.next_line(), if !stdin_closed => {
                if let Some(line) = line? {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    match trimmed {
                        "/interrupt" | "/cancel" => {
                            if !ingress.interrupt().await {
                                break;
                            }
                        }
                        "/exit" | "/quit" => {
                            stdin_closed = true;
                            let _ = ingress.close().await;
                        }
                        _ => {
                            if !ingress.submit_text(line).await {
                                break;
                            }
                        }
                    }
                } else {
                    stdin_closed = true;
                    let _ = ingress.close().await;
                }
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
