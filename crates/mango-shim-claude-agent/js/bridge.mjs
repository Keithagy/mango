import { createSdkMcpServer, query, tool } from "@anthropic-ai/claude-agent-sdk";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";
import { z } from "zod";

class AsyncQueue {
  constructor() {
    this.items = [];
    this.resolvers = [];
    this.closed = false;
  }

  push(item) {
    if (this.closed) return;
    const resolver = this.resolvers.shift();
    if (resolver) {
      resolver({ value: item, done: false });
      return;
    }
    this.items.push(item);
  }

  close() {
    this.closed = true;
    while (this.resolvers.length > 0) {
      const resolver = this.resolvers.shift();
      resolver({ value: undefined, done: true });
    }
  }

  async next() {
    if (this.items.length > 0) {
      return { value: this.items.shift(), done: false };
    }
    if (this.closed) {
      return { value: undefined, done: true };
    }
    return await new Promise((resolve) => this.resolvers.push(resolve));
  }

  [Symbol.asyncIterator]() {
    return this;
  }
}

const sessionId = process.env.MANGO_SESSION_ID || randomUUID();
const cwd = process.env.MANGO_CWD || process.cwd();
const claudePath = process.env.MANGO_CLAUDE_PATH || "claude";
const model = process.env.MANGO_MODEL || undefined;
const tools = process.env.MANGO_TOOLS_JSON
  ? JSON.parse(process.env.MANGO_TOOLS_JSON)
  : undefined;
const mcpToolset = process.env.MANGO_MCP_TOOLSET
  ? JSON.parse(process.env.MANGO_MCP_TOOLSET)
  : undefined;
const persistSession = process.env.MANGO_PERSIST_SESSION !== "false";
const includePartialMessages =
  process.env.MANGO_INCLUDE_PARTIAL_MESSAGES !== "false";
const oneShotTurns = process.env.MANGO_ONESHOT_TURNS === "true";
const emitToolUseHooks = process.env.MANGO_EMIT_TOOL_USE_HOOKS === "true";
const systemPromptAppend =
  process.env.MANGO_SYSTEM_PROMPT_APPEND ||
  "You are running inside a Mango runtime. Keep responses concise unless the user asks for depth.";
const inputQueue = new AsyncQueue();
const pendingToolCalls = new Map();

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });
  return { promise, resolve, reject };
}

function emitToolHook(phase, payload) {
  emit({
    type: "sdk_message",
    message: {
      type: "mango_tool_use",
      phase,
      ...payload,
    },
  });
}

function toolHooks() {
  if (!emitToolUseHooks) {
    return undefined;
  }

  return {
    PreToolUse: [
      {
        hooks: [
          async (input) => {
            emitToolHook("pre", {
              tool_name: input.tool_name,
              tool_input: input.tool_input,
              tool_use_id: input.tool_use_id,
            });
            return { continue: true };
          },
        ],
      },
    ],
    PostToolUse: [
      {
        hooks: [
          async (input) => {
            emitToolHook("post", {
              tool_name: input.tool_name,
              tool_input: input.tool_input,
              tool_response: input.tool_response,
              tool_use_id: input.tool_use_id,
            });
            return { continue: true };
          },
        ],
      },
    ],
    PostToolUseFailure: [
      {
        hooks: [
          async (input) => {
            emitToolHook("failure", {
              tool_name: input.tool_name,
              tool_input: input.tool_input,
              tool_use_id: input.tool_use_id,
              error: input.error,
              is_interrupt: input.is_interrupt ?? false,
            });
            return { continue: true };
          },
        ],
      },
    ],
  };
}

function toolTextResult(text) {
  return {
    content: [
      {
        type: "text",
        text,
      },
    ],
  };
}

function rejectPendingToolCalls(message) {
  for (const pending of pendingToolCalls.values()) {
    pending.reject(new Error(message));
  }
  pendingToolCalls.clear();
}

async function requestRustTool(toolName, input) {
  const requestId = randomUUID();
  const pending = deferred();
  pendingToolCalls.set(requestId, pending);

  // The MCP handler suspends here until Rust answers. That makes the Mango runtime the executor
  // of record while Claude remains responsible only for deciding when a tool should be used.
  emit({
    type: "tool_call_requested",
    request_id: requestId,
    tool_name: toolName,
    input,
  });

  try {
    return await pending.promise;
  } finally {
    pendingToolCalls.delete(requestId);
  }
}

function createMcpServers() {
  if (mcpToolset !== "mango_coding") {
    return undefined;
  }

  const bash = tool(
    "bash",
    "Run a shell command inside the current working directory. Use this for inspection, builds, tests, and other command-line tasks.",
    {
      command: z.string(),
      description: z.string().optional(),
      timeout_ms: z.number().int().positive().max(120000).optional(),
    },
    async (args) => requestRustTool("bash", args),
  );

  const readFile = tool(
    "read_file",
    "Read a UTF-8 text file. Paths are resolved relative to the current working directory unless absolute.",
    {
      path: z.string(),
      start_line: z.number().int().positive().optional(),
      end_line: z.number().int().positive().optional(),
    },
    async (args) => requestRustTool("read_file", args),
  );

  const writeFile = tool(
    "write_file",
    "Write a UTF-8 text file, replacing its contents if it already exists.",
    {
      path: z.string(),
      content: z.string(),
    },
    async (args) => requestRustTool("write_file", args),
  );

  const glob = tool(
    "glob",
    "List files matching a glob pattern. The optional path selects the search root.",
    {
      pattern: z.string(),
      path: z.string().optional(),
    },
    async (args) => requestRustTool("glob", args),
  );

  const grep = tool(
    "grep",
    "Search files for a regular-expression pattern. The optional path limits the search root and the optional glob narrows matching files.",
    {
      pattern: z.string(),
      path: z.string().optional(),
      glob: z.string().optional(),
    },
    async (args) => requestRustTool("grep", args),
  );

  return {
    mango: createSdkMcpServer({
      name: "mango-coding",
      tools: [bash, readFile, writeFile, glob, grep],
    }),
  };
}

function allowedTools() {
  if (mcpToolset !== "mango_coding") {
    return undefined;
  }

  // Custom MCP tools are not built-ins, so the SDK will otherwise treat them like any other
  // permissioned tool. Auto-allowing this explicit list keeps Mango in charge of the tool loop
  // without forcing the model into a dead-end "I would need approval" response.
  return ["mcp__mango__bash", "mcp__mango__read_file", "mcp__mango__write_file", "mcp__mango__glob", "mcp__mango__grep"];
}

function queryOptions() {
  const mcpServers = createMcpServers();
  const approvedTools = allowedTools();
  return {
    cwd,
    includePartialMessages,
    pathToClaudeCodeExecutable: claudePath,
    permissionMode: "default",
    persistSession,
    settingSources: ["user", "project", "local"],
    ...(!oneShotTurns ? { sessionId } : {}),
    ...(model ? { model } : {}),
    systemPrompt: {
      type: "preset",
      preset: "claude_code",
      append: systemPromptAppend,
    },
    ...(approvedTools ? { allowedTools: approvedTools } : {}),
    ...(tools !== undefined ? { tools } : {}),
    ...(mcpServers ? { mcpServers } : {}),
    ...(emitToolUseHooks ? { hooks: toolHooks() } : {}),
  };
}

function emit(payload) {
  process.stdout.write(JSON.stringify(payload) + "\n");
}

function userMessage(text) {
  return {
    type: "user",
    session_id: sessionId,
    message: {
      role: "user",
      content: text,
    },
    parent_tool_use_id: null,
  };
}

let runner = null;
let sdkLoop = null;
let initialization = null;
let readyEmitted = false;

function isTerminalClaudeMessage(message) {
  return message?.type === "result";
}

function createRunner() {
  return query({
    prompt: inputQueue,
    options: queryOptions(),
  });
}

async function ensureRunner() {
  if (!runner) {
    runner = createRunner();
    sdkLoop = (async () => {
      try {
        for await (const message of runner) {
          emit({ type: "sdk_message", message });
        }
      } catch (error) {
        emit({
          type: "error",
          message: error instanceof Error ? error.message : String(error),
        });
      }
    })();
  }

  if (!initialization) {
    initialization = (async () => {
      // Mango wants a long-lived bridge process, but the underlying Claude query is happiest when
      // it is created only once the first real input exists. We therefore initialize lazily and
      // treat "ready" as "the query transport is now usable for this session".
      await runner.initialization;
      if (!readyEmitted) {
        emit({ type: "ready", session_id: sessionId });
        readyEmitted = true;
      }
    })();
  }

  await initialization;
  return runner;
}

async function runOneShotTurn(text) {
  if (sdkLoop) {
    emit({ type: "error", message: "a Claude turn is already running" });
    return;
  }

  const turnRunner = query({
    prompt: text,
    options: queryOptions(),
  });
  runner = turnRunner;

  sdkLoop = (async () => {
    try {
      await turnRunner.initialization;
      if (!readyEmitted) {
        emit({ type: "ready", session_id: sessionId });
        readyEmitted = true;
      }
      for await (const message of turnRunner) {
        if (isTerminalClaudeMessage(message)) {
          runner = null;
          sdkLoop = null;
        }
        emit({ type: "sdk_message", message });
      }
    } catch (error) {
      emit({
        type: "error",
        message: error instanceof Error ? error.message : String(error),
      });
    } finally {
      if (runner === turnRunner) {
        runner = null;
      }
      sdkLoop = null;
    }
  })();
}

const rl = createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
});

if (oneShotTurns) {
  emit({ type: "ready", session_id: sessionId });
  readyEmitted = true;
}

for await (const line of rl) {
  if (!line.trim()) continue;

  let command;
  try {
    command = JSON.parse(line);
  } catch (error) {
    emit({
      type: "error",
      message: `invalid command json: ${error instanceof Error ? error.message : String(error)}`,
    });
    continue;
  }

  if (command.type === "user_text") {
    if (oneShotTurns) {
      await runOneShotTurn(command.text);
      continue;
    }

    // Queue the message before initialization completes so the first real turn is already waiting
    // when Claude Code starts consuming streamed input.
    inputQueue.push(userMessage(command.text));
    try {
      await ensureRunner();
    } catch (error) {
      emit({
        type: "error",
        message: error instanceof Error ? error.message : String(error),
      });
      process.exit(1);
    }
    continue;
  }

  if (command.type === "interrupt") {
    try {
      if (oneShotTurns) {
        runner?.close();
        rejectPendingToolCalls("tool call interrupted");
        continue;
      }
      await ensureRunner();
      await runner.interrupt();
      rejectPendingToolCalls("tool call interrupted");
    } catch (error) {
      emit({
        type: "error",
        message: error instanceof Error ? error.message : String(error),
      });
    }
    continue;
  }

  if (command.type === "tool_success") {
    const pending = pendingToolCalls.get(command.request_id);
    if (!pending) {
      emit({
        type: "error",
        message: `unknown tool request id: ${command.request_id}`,
      });
      continue;
    }
    pending.resolve(toolTextResult(command.output));
    continue;
  }

  if (command.type === "tool_failure") {
    const pending = pendingToolCalls.get(command.request_id);
    if (!pending) {
      emit({
        type: "error",
        message: `unknown tool request id: ${command.request_id}`,
      });
      continue;
    }
    pending.reject(new Error(command.message));
    continue;
  }

  if (command.type === "close") {
    inputQueue.close();
    rejectPendingToolCalls("bridge closed");
    runner?.close();
    break;
  }
}

inputQueue.close();
rejectPendingToolCalls("bridge closed");
if (sdkLoop) {
  await sdkLoop;
}
