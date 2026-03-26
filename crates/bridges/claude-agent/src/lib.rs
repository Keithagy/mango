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
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ClaudeToolChoice {
    Named(Vec<String>),
    Preset {
        #[serde(rename = "type")]
        kind: String,
        preset: String,
    },
}

impl From<Vec<String>> for ClaudeToolChoice {
    fn from(value: Vec<String>) -> Self {
        Self::Named(value)
    }
}

impl ClaudeToolChoice {
    pub fn claude_code_preset() -> Self {
        Self::Preset {
            kind: "preset".to_string(),
            preset: "claude_code".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum ClaudeSessionPersistence {
    #[default]
    Persistent,
    Ephemeral,
}

impl ClaudeSessionPersistence {
    const fn as_env_value(self) -> &'static str {
        match self {
            Self::Persistent => "true",
            Self::Ephemeral => "false",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum ClaudePartialMessageMode {
    #[default]
    Include,
    Exclude,
}

impl ClaudePartialMessageMode {
    const fn as_env_value(self) -> &'static str {
        match self {
            Self::Include => "true",
            Self::Exclude => "false",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum ClaudeTurnMode {
    #[default]
    Conversational,
    OneShot,
}

impl ClaudeTurnMode {
    const fn as_env_value(self) -> &'static str {
        match self {
            Self::Conversational => "false",
            Self::OneShot => "true",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum ClaudeToolUseHookMode {
    #[default]
    Disabled,
    Enabled,
}

impl ClaudeToolUseHookMode {
    const fn as_env_value(self) -> &'static str {
        match self {
            Self::Disabled => "false",
            Self::Enabled => "true",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaudeMcpToolset {
    MangoCoding,
}

#[must_use]
#[derive(Debug, Clone)]
pub struct ClaudeAgentConfig {
    pub cwd: PathBuf,
    pub session_id: String,
    pub claude_executable: String,
    pub model: Option<String>,
    pub tools: Option<ClaudeToolChoice>,
    pub system_prompt_append: Option<String>,
    pub session_persistence: ClaudeSessionPersistence,
    pub partial_message_mode: ClaudePartialMessageMode,
    pub turn_mode: ClaudeTurnMode,
    pub tool_use_hook_mode: ClaudeToolUseHookMode,
    pub mcp_toolset: Option<ClaudeMcpToolset>,
}

impl ClaudeAgentConfig {
    pub fn new(
        cwd: PathBuf,
        session_id: impl Into<String>,
        claude_executable: impl Into<String>,
    ) -> Self {
        Self {
            cwd,
            session_id: session_id.into(),
            claude_executable: claude_executable.into(),
            model: None,
            tools: None,
            system_prompt_append: None,
            session_persistence: ClaudeSessionPersistence::Persistent,
            partial_message_mode: ClaudePartialMessageMode::Include,
            turn_mode: ClaudeTurnMode::Conversational,
            tool_use_hook_mode: ClaudeToolUseHookMode::Disabled,
            mcp_toolset: None,
        }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_tools(mut self, tools: impl Into<ClaudeToolChoice>) -> Self {
        self.tools = Some(tools.into());
        self
    }

    pub fn with_default_claude_code_tools(mut self) -> Self {
        self.tools = Some(ClaudeToolChoice::claude_code_preset());
        self
    }

    pub fn with_system_prompt_append(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt_append = Some(prompt.into());
        self
    }

    pub fn with_persistent_session(mut self) -> Self {
        self.session_persistence = ClaudeSessionPersistence::Persistent;
        self
    }

    pub fn with_ephemeral_session(mut self) -> Self {
        self.session_persistence = ClaudeSessionPersistence::Ephemeral;
        self
    }

    pub fn with_partial_messages(mut self) -> Self {
        self.partial_message_mode = ClaudePartialMessageMode::Include;
        self
    }

    pub fn without_partial_messages(mut self) -> Self {
        self.partial_message_mode = ClaudePartialMessageMode::Exclude;
        self
    }

    pub fn with_one_shot_turns(mut self) -> Self {
        self.turn_mode = ClaudeTurnMode::OneShot;
        self
    }

    pub fn with_conversational_turns(mut self) -> Self {
        self.turn_mode = ClaudeTurnMode::Conversational;
        self
    }

    pub fn with_tool_use_hooks(mut self) -> Self {
        self.tool_use_hook_mode = ClaudeToolUseHookMode::Enabled;
        self
    }

    pub fn without_tool_use_hooks(mut self) -> Self {
        self.tool_use_hook_mode = ClaudeToolUseHookMode::Disabled;
        self
    }

    pub fn with_mango_coding_tools(mut self) -> Self {
        self.mcp_toolset = Some(ClaudeMcpToolset::MangoCoding);
        self
    }
}

#[must_use]
#[derive(Debug, Clone)]
pub struct ClaudeAgentBridge {
    commands: mpsc::Sender<BridgeCommand>,
    events: broadcast::Sender<ClaudeBridgeEvent>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BridgeCommand {
    UserText { text: String },
    ToolSuccess { request_id: String, output: String },
    ToolFailure { request_id: String, message: String },
    Interrupt,
    Close,
}

#[derive(Debug, Clone)]
pub enum ClaudeBridgeEvent {
    Ready {
        session_id: String,
    },
    ToolCallRequested {
        request_id: String,
        tool_name: String,
        input: Value,
    },
    SdkMessage {
        message: Value,
    },
    BridgeError {
        message: String,
    },
    Stderr {
        line: String,
    },
    Exited {
        code: Option<i32>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BridgeStdout {
    Ready {
        session_id: String,
    },
    ToolCallRequested {
        request_id: String,
        tool_name: String,
        input: Value,
    },
    SdkMessage {
        message: Value,
    },
    Error {
        message: String,
    },
}

struct ClaudeBridgeIo {
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
}

impl ClaudeAgentBridge {
    /// Spawn the Claude bridge.
    ///
    /// # Errors
    ///
    /// Returns an error if the bridge command cannot be constructed, the
    /// child process cannot be spawned, or its stdio handles cannot be taken.
    pub fn spawn(config: ClaudeAgentConfig) -> Result<Self> {
        let (js_dir, script) = bridge_paths();
        let mut command = build_command(config, &js_dir, &script)?;
        let mut child = command
            .spawn()
            .with_context(|| format!("failed to start node bridge at {}", script.display()))?;
        let io = take_bridge_io(&mut child)?;

        let (command_tx, command_rx) = mpsc::channel::<BridgeCommand>(64);
        let (event_tx, _) = broadcast::channel::<ClaudeBridgeEvent>(1024);

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
    ///
    /// # Errors
    ///
    /// Returns an error if the bridge command channel is closed.
    pub async fn send_user_text(&self, text: impl Into<String>) -> Result<()> {
        self.send_command(BridgeCommand::UserText { text: text.into() })
            .await
    }

    /// Interrupt the active turn.
    ///
    /// # Errors
    ///
    /// Returns an error if the bridge command channel is closed.
    pub async fn interrupt(&self) -> Result<()> {
        self.send_command(BridgeCommand::Interrupt).await
    }

    /// Send a successful tool result.
    ///
    /// # Errors
    ///
    /// Returns an error if the bridge command channel is closed.
    pub async fn respond_tool_success(
        &self,
        request_id: impl Into<String>,
        output: impl Into<String>,
    ) -> Result<()> {
        self.send_command(BridgeCommand::ToolSuccess {
            request_id: request_id.into(),
            output: output.into(),
        })
        .await
    }

    /// Send a failed tool result.
    ///
    /// # Errors
    ///
    /// Returns an error if the bridge command channel is closed.
    pub async fn respond_tool_failure(
        &self,
        request_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<()> {
        self.send_command(BridgeCommand::ToolFailure {
            request_id: request_id.into(),
            message: message.into(),
        })
        .await
    }

    /// Shut the bridge down.
    ///
    /// # Errors
    ///
    /// Returns an error if the bridge command channel is closed.
    pub async fn close(&self) -> Result<()> {
        self.send_command(BridgeCommand::Close).await
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ClaudeBridgeEvent> {
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

fn build_command(config: ClaudeAgentConfig, js_dir: &Path, script: &Path) -> Result<Command> {
    let ClaudeAgentConfig {
        cwd,
        session_id,
        claude_executable,
        model,
        tools,
        system_prompt_append,
        session_persistence,
        partial_message_mode,
        turn_mode,
        tool_use_hook_mode,
        mcp_toolset,
    } = config;
    let mut command = Command::new("node");
    command
        .arg(script)
        .current_dir(js_dir)
        .env("MANGO_CWD", cwd)
        .env("MANGO_SESSION_ID", session_id)
        .env("MANGO_CLAUDE_PATH", claude_executable)
        .env("MANGO_PERSIST_SESSION", session_persistence.as_env_value())
        .env(
            "MANGO_INCLUDE_PARTIAL_MESSAGES",
            partial_message_mode.as_env_value(),
        )
        .env("MANGO_ONESHOT_TURNS", turn_mode.as_env_value())
        .env(
            "MANGO_EMIT_TOOL_USE_HOOKS",
            tool_use_hook_mode.as_env_value(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(model) = model {
        command.env("MANGO_MODEL", model);
    }
    if let Some(system_prompt_append) = system_prompt_append {
        command.env("MANGO_SYSTEM_PROMPT_APPEND", system_prompt_append);
    }
    if let Some(tools) = tools {
        command.env(
            "MANGO_TOOLS_JSON",
            serde_json::to_string(&tools).context("failed to serialize tool list")?,
        );
    }
    if let Some(mcp_toolset) = mcp_toolset {
        command.env(
            "MANGO_MCP_TOOLSET",
            serde_json::to_string(&mcp_toolset)
                .context("failed to serialize MCP toolset config")?,
        );
    }

    Ok(command)
}

fn take_bridge_io(child: &mut Child) -> Result<ClaudeBridgeIo> {
    Ok(ClaudeBridgeIo {
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
    event_tx: broadcast::Sender<ClaudeBridgeEvent>,
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

fn spawn_stdout_task(stdout: ChildStdout, event_tx: broadcast::Sender<ClaudeBridgeEvent>) {
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

fn handle_stdout_line(event_tx: &broadcast::Sender<ClaudeBridgeEvent>, line: &str) {
    match serde_json::from_str::<BridgeStdout>(line) {
        Ok(BridgeStdout::Ready { session_id }) => {
            let _ = event_tx.send(ClaudeBridgeEvent::Ready { session_id });
        }
        Ok(BridgeStdout::ToolCallRequested {
            request_id,
            tool_name,
            input,
        }) => {
            let _ = event_tx.send(ClaudeBridgeEvent::ToolCallRequested {
                request_id,
                tool_name,
                input,
            });
        }
        Ok(BridgeStdout::SdkMessage { message }) => {
            let _ = event_tx.send(ClaudeBridgeEvent::SdkMessage { message });
        }
        Ok(BridgeStdout::Error { message }) => {
            emit_bridge_error(event_tx, message);
        }
        Err(error) => {
            emit_bridge_error(event_tx, format!("invalid bridge stdout: {error}: {line}"));
        }
    }
}

fn spawn_stderr_task(stderr: ChildStderr, event_tx: broadcast::Sender<ClaudeBridgeEvent>) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let _ = event_tx.send(ClaudeBridgeEvent::Stderr { line });
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

fn spawn_wait_task(mut child: Child, event_tx: broadcast::Sender<ClaudeBridgeEvent>) {
    tokio::spawn(async move {
        match child.wait().await {
            Ok(status) => {
                let _ = event_tx.send(ClaudeBridgeEvent::Exited {
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

fn emit_bridge_error(event_tx: &broadcast::Sender<ClaudeBridgeEvent>, message: String) {
    let _ = event_tx.send(ClaudeBridgeEvent::BridgeError { message });
}
