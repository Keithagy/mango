import { Codex } from "@openai/codex-sdk";
import { createInterface } from "node:readline";

const cwd = process.env.MANGO_CWD || process.cwd();
const codexPath = process.env.MANGO_CODEX_PATH || "codex";
const threadId = process.env.MANGO_THREAD_ID || undefined;
const model = process.env.MANGO_MODEL || undefined;
const sandboxMode = process.env.MANGO_SANDBOX_MODE || undefined;
const approvalPolicy = process.env.MANGO_APPROVAL_POLICY || undefined;
const skipGitRepoCheck = process.env.MANGO_SKIP_GIT_REPO_CHECK === "true";
const networkAccessEnabled =
  process.env.MANGO_NETWORK_ACCESS_ENABLED === "true";
const additionalDirectories = process.env.MANGO_ADDITIONAL_DIRECTORIES_JSON
  ? JSON.parse(process.env.MANGO_ADDITIONAL_DIRECTORIES_JSON)
  : undefined;

function emit(payload) {
  process.stdout.write(JSON.stringify(payload) + "\n");
}

const codex = new Codex({
  codexPathOverride: codexPath,
});

const threadOptions = {
  workingDirectory: cwd,
  skipGitRepoCheck,
  networkAccessEnabled,
  ...(model ? { model } : {}),
  ...(sandboxMode ? { sandboxMode } : {}),
  ...(approvalPolicy ? { approvalPolicy } : {}),
  ...(additionalDirectories ? { additionalDirectories } : {}),
};

const thread = threadId
  ? codex.resumeThread(threadId, threadOptions)
  : codex.startThread(threadOptions);

let activeAbortController = null;
let activeTurnPromise = null;

function isTerminalTurnEvent(event) {
  return (
    event?.type === "turn.completed" ||
    event?.type === "turn.failed" ||
    event?.type === "error"
  );
}

async function startTurn(text) {
  if (activeTurnPromise) {
    emit({ type: "error", message: "a codex turn is already running" });
    return;
  }

  const abortController = new AbortController();
  activeAbortController = abortController;
  activeTurnPromise = (async () => {
    try {
      const { events } = await thread.runStreamed(text, {
        signal: abortController.signal,
      });
      for await (const event of events) {
        if (isTerminalTurnEvent(event)) {
          // Mango control workers react to terminal turn events immediately. Clearing the active
          // turn before emitting the terminal event keeps the bridge's command boundary aligned
          // with the runtime's lifecycle boundary.
          activeAbortController = null;
          activeTurnPromise = null;
        }
        emit({ type: "thread_event", event });
      }
    } catch (error) {
      const aborted =
        error instanceof Error &&
        (error.name === "AbortError" || error.message.includes("aborted"));
      if (!aborted) {
        emit({
          type: "error",
          message: error instanceof Error ? error.message : String(error),
        });
      }
    } finally {
      activeAbortController = null;
      activeTurnPromise = null;
    }
  })();
}

emit({ type: "ready" });

const rl = createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
});

for await (const line of rl) {
  if (!line.trim()) {
    continue;
  }

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
    await startTurn(command.text);
    continue;
  }

  if (command.type === "interrupt") {
    activeAbortController?.abort();
    continue;
  }

  if (command.type === "close") {
    activeAbortController?.abort();
    if (activeTurnPromise) {
      await activeTurnPromise;
    }
    break;
  }
}

if (activeTurnPromise) {
  await activeTurnPromise;
}
