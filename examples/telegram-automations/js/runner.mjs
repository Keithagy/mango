import { pathToFileURL } from "node:url";

function deterministicRoll(seed, sides) {
  if (!Number.isInteger(sides) || sides <= 0) {
    throw new Error(`rollDie requires a positive integer side count, received ${sides}`);
  }

  return Number(BigInt(seed) % BigInt(sides)) + 1;
}

async function main() {
  const [scriptPath, contextJson] = process.argv.slice(2);
  if (!scriptPath || !contextJson) {
    throw new Error("usage: node runner.mjs <script-path> <context-json>");
  }

  const moduleUrl = pathToFileURL(scriptPath).href;
  const mod = await import(moduleUrl);
  if (typeof mod.run !== "function") {
    throw new Error(`${scriptPath} must export an async run(ctx) function`);
  }

  const context = JSON.parse(contextJson);
  const result = await mod.run({
    automationId: context.automation_id,
    runId: context.run_id,
    nominalFireAt: context.nominal_fire_at,
    seed: context.seed,
    rollDie: (sides) => deterministicRoll(context.seed, sides),
  });

  process.stdout.write(JSON.stringify(result));
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error.message || String(error)}\n`);
  process.exitCode = 1;
});
