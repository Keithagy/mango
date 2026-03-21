use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{broadcast, mpsc},
};

#[must_use]
#[derive(Debug, Clone)]
pub struct CodexAgentConfig {
    pub cwd: PathBuf,
    pub codex_executable: String,
    pub thread_id: Option<String>,
    pub model: Option<String>,
    pub sandbox_mode: Option<String>,
    pub approval_policy: Option<String>,
    pub skip_git_repo_check: bool,
    pub network_access_enabled: bool,
    pub additional_directories: Vec<PathBuf>,
}

impl CodexAgentConfig {
    pub fn new(cwd: PathBuf, codex_executable: impl Into<String>) -> Self {
        Self {
            cwd,
            codex_executable: codex_executable.into(),
            thread_id: None,
            model: None,
            sandbox_mode: None,
            approval_policy: None,
            skip_git_repo_check: false,
            network_access_enabled: false,
            additional_directories: Vec::new(),
        }
    }

    pub fn with_thread_id(mut self, thread_id: impl Into<String>) -> Self {
        self.thread_id = Some(thread_id.into());
        self
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_sandbox_mode(mut self, sandbox_mode: impl Into<String>) -> Self {
        self.sandbox_mode = Some(sandbox_mode.into());
        self
    }

    pub fn with_approval_policy(mut self, approval_policy: impl Into<String>) -> Self {
        self.approval_policy = Some(approval_policy.into());
        self
    }

    pub fn with_skip_git_repo_check(mut self, skip_git_repo_check: bool) -> Self {
        self.skip_git_repo_check = skip_git_repo_check;
        self
    }

    pub fn with_network_access(mut self, network_access_enabled: bool) -> Self {
        self.network_access_enabled = network_access_enabled;
        self
    }

    pub fn with_additional_directories(mut self, directories: Vec<PathBuf>) -> Self {
        self.additional_directories = directories;
        self
    }
}

#[must_use]
#[derive(Debug, Clone)]
pub struct CodexAgentBridge {
    commands: mpsc::Sender<BridgeCommand>,
    events: broadcast::Sender<CodexBridgeEvent>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BridgeCommand {
    UserText { text: String },
    Interrupt,
    Close,
}

#[derive(Debug, Clone)]
pub enum CodexBridgeEvent {
    Ready,
    ThreadEvent { event: Value },
    BridgeError { message: String },
    Stderr { line: String },
    Exited { code: Option<i32> },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BridgeStdout {
    Ready,
    ThreadEvent { event: Value },
    Error { message: String },
}

struct CodexBridgeIo {
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
}

impl CodexAgentBridge {
    /// Spawn the Codex bridge.
    pub fn spawn(config: CodexAgentConfig) -> Result<Self> {
        let (js_dir, script) = bridge_paths();
        let mut command = build_command(config, &js_dir, &script)?;
        let mut child = command
            .spawn()
            .with_context(|| format!("failed to start node bridge at {}", script.display()))?;
        let io = take_bridge_io(&mut child)?;

        let (command_tx, command_rx) = mpsc::channel::<BridgeCommand>(64);
        let (event_tx, _) = broadcast::channel::<CodexBridgeEvent>(1024);

        spawn_command_task(io.stdin, command_rx, event_tx.clone());
        spawn_stdout_task(io.stdout, event_tx.clone());
        spawn_stderr_task(io.stderr, event_tx.clone());
        spawn_wait_task(child, event_tx.clone());

        Ok(Self {
            commands: command_tx,
            events: event_tx,
        })
    }

    /// Send a user turn.
    pub async fn send_user_text(&self, text: impl Into<String>) -> Result<()> {
        self.send_command(BridgeCommand::UserText { text: text.into() })
            .await
    }

    /// Interrupt the active turn.
    pub async fn interrupt(&self) -> Result<()> {
        self.send_command(BridgeCommand::Interrupt).await
    }

    /// Shut the bridge down.
    pub async fn close(&self) -> Result<()> {
        self.send_command(BridgeCommand::Close).await
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<CodexBridgeEvent> {
        self.events.subscribe()
    }

    async fn send_command(&self, command: BridgeCommand) -> Result<()> {
        self.commands
            .send(command)
            .await
            .map_err(|_| anyhow!("bridge command channel closed"))
    }
}

fn bridge_paths() -> (PathBuf, PathBuf) {
    let js_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("js");
    let script = js_dir.join("bridge.mjs");
    (js_dir, script)
}

fn build_command(config: CodexAgentConfig, js_dir: &Path, script: &Path) -> Result<Command> {
    let CodexAgentConfig {
        cwd,
        codex_executable,
        thread_id,
        model,
        sandbox_mode,
        approval_policy,
        skip_git_repo_check,
        network_access_enabled,
        additional_directories,
    } = config;
    let mut command = Command::new("node");
    command
        .arg(script)
        .current_dir(js_dir)
        .env("MANGO_CWD", cwd)
        .env("MANGO_CODEX_PATH", codex_executable)
        .env(
            "MANGO_SKIP_GIT_REPO_CHECK",
            if skip_git_repo_check { "true" } else { "false" },
        )
        .env(
            "MANGO_NETWORK_ACCESS_ENABLED",
            if network_access_enabled {
                "true"
            } else {
                "false"
            },
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(thread_id) = thread_id {
        command.env("MANGO_THREAD_ID", thread_id);
    }
    if let Some(model) = model {
        command.env("MANGO_MODEL", model);
    }
    if let Some(sandbox_mode) = sandbox_mode {
        command.env("MANGO_SANDBOX_MODE", sandbox_mode);
    }
    if let Some(approval_policy) = approval_policy {
        command.env("MANGO_APPROVAL_POLICY", approval_policy);
    }
    if !additional_directories.is_empty() {
        command.env(
            "MANGO_ADDITIONAL_DIRECTORIES_JSON",
            serde_json::to_string(&additional_directories)
                .context("failed to serialize additional directories")?,
        );
    }

    Ok(command)
}

fn take_bridge_io(child: &mut Child) -> Result<CodexBridgeIo> {
    Ok(CodexBridgeIo {
        stdin: take_pipe(child.stdin.take(), "stdin")?,
        stdout: take_pipe(child.stdout.take(), "stdout")?,
        stderr: take_pipe(child.stderr.take(), "stderr")?,
    })
}

fn take_pipe<T>(pipe: Option<T>, name: &str) -> Result<T> {
    pipe.ok_or_else(|| anyhow!("bridge {name} unavailable"))
}

fn spawn_command_task(
    stdin: ChildStdin,
    mut command_rx: mpsc::Receiver<BridgeCommand>,
    event_tx: broadcast::Sender<CodexBridgeEvent>,
) {
    tokio::spawn(async move {
        let mut stdin = stdin;
        while let Some(command) = command_rx.recv().await {
            match serde_json::to_vec(&command) {
                Ok(mut line) => {
                    line.push(b'\n');
                    if let Err(error) = stdin.write_all(&line).await {
                        emit_bridge_error(
                            &event_tx,
                            format!("failed to write to bridge stdin: {error}"),
                        );
                        return;
                    }
                }
                Err(error) => {
                    emit_bridge_error(
                        &event_tx,
                        format!("failed to serialize bridge command: {error}"),
                    );
                    return;
                }
            }
        }
    });
}

fn spawn_stdout_task(stdout: ChildStdout, event_tx: broadcast::Sender<CodexBridgeEvent>) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    handle_stdout_line(&event_tx, &line);
                }
                Ok(None) => break,
                Err(error) => {
                    emit_bridge_error(&event_tx, format!("failed reading bridge stdout: {error}"));
                    break;
                }
            }
        }
    });
}

fn handle_stdout_line(event_tx: &broadcast::Sender<CodexBridgeEvent>, line: &str) {
    match serde_json::from_str::<BridgeStdout>(line) {
        Ok(BridgeStdout::Ready) => {
            let _ = event_tx.send(CodexBridgeEvent::Ready);
        }
        Ok(BridgeStdout::ThreadEvent { event }) => {
            let _ = event_tx.send(CodexBridgeEvent::ThreadEvent { event });
        }
        Ok(BridgeStdout::Error { message }) => {
            emit_bridge_error(event_tx, message);
        }
        Err(error) => {
            emit_bridge_error(event_tx, format!("invalid bridge stdout: {error}: {line}"));
        }
    }
}

fn spawn_stderr_task(stderr: ChildStderr, event_tx: broadcast::Sender<CodexBridgeEvent>) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let _ = event_tx.send(CodexBridgeEvent::Stderr { line });
                }
                Ok(None) => break,
                Err(error) => {
                    emit_bridge_error(&event_tx, format!("failed reading bridge stderr: {error}"));
                    break;
                }
            }
        }
    });
}

fn spawn_wait_task(mut child: Child, event_tx: broadcast::Sender<CodexBridgeEvent>) {
    tokio::spawn(async move {
        match child.wait().await {
            Ok(status) => {
                let _ = event_tx.send(CodexBridgeEvent::Exited {
                    code: status.code(),
                });
            }
            Err(error) => {
                emit_bridge_error(
                    &event_tx,
                    format!("failed waiting on bridge process: {error}"),
                );
            }
        }
    });
}

fn emit_bridge_error(event_tx: &broadcast::Sender<CodexBridgeEvent>, message: String) {
    let _ = event_tx.send(CodexBridgeEvent::BridgeError { message });
}
