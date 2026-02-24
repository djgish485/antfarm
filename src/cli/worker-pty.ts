#!/usr/bin/env node

/**
 * PTY Worker — standalone script that runs a coding agent to completion.
 *
 * Usage: node dist/cli/worker-pty.js --work-file <path> [--timeout <seconds>]
 *
 * Reads a work file JSON, spawns Claude Code CLI (or fallback), monitors
 * for completion, and auto-fails the step if the agent exits without reporting.
 */

import fs from "node:fs";
import { spawn, execFileSync } from "node:child_process";
import { resolveAntfarmCli } from "../installer/paths.js";
import { getWorkerLogPath } from "../installer/worker-state.js";
import { failStep } from "../installer/step-ops.js";
import { getDb } from "../db.js";
import { resolveWorkflowWorkspaceRoot } from "../installer/paths.js";
import path from "node:path";

interface WorkFileData {
  stepId: string;
  runId: string;
  workflowId: string;
  agentRole: string;
  input: string;
  model?: string;
  claimedAt: string;
  pid?: number;
}

// ── Parse args ──────────────────────────────────────────────────────
const args = process.argv.slice(2);
let workFilePath = "";
let timeoutSeconds = 30 * 60; // default 30 min

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--work-file" && args[i + 1]) {
    workFilePath = args[++i];
  } else if (args[i] === "--timeout" && args[i + 1]) {
    timeoutSeconds = parseInt(args[++i], 10) || 30 * 60;
  }
}

if (!workFilePath) {
  process.stderr.write("Usage: node worker-pty.js --work-file <path> [--timeout <seconds>]\n");
  process.exit(1);
}

// ── Read work file ──────────────────────────────────────────────────
let workData: WorkFileData;
try {
  workData = JSON.parse(fs.readFileSync(workFilePath, "utf-8"));
} catch (err) {
  process.stderr.write(`Failed to read work file: ${workFilePath}\n`);
  process.exit(1);
}

const { stepId, runId, workflowId, input, model } = workData;
const cli = resolveAntfarmCli();
const logPath = getWorkerLogPath(runId, stepId);
const logStream = fs.createWriteStream(logPath, { flags: "a" });

function log(msg: string): void {
  const line = `[${new Date().toISOString()}] ${msg}\n`;
  logStream.write(line);
  process.stderr.write(line);
}

log(`Worker starting for step=${stepId.slice(0, 8)} run=${runId.slice(0, 8)}`);
log(`Timeout: ${timeoutSeconds}s`);

// ── Resolve workspace ───────────────────────────────────────────────
const workspaceDir = path.join(resolveWorkflowWorkspaceRoot(), workflowId, "runs", runId);
if (!fs.existsSync(workspaceDir)) {
  fs.mkdirSync(workspaceDir, { recursive: true });
}

// ── Resolve coding agent ────────────────────────────────────────────
function commandExists(cmd: string): boolean {
  try {
    execFileSync("which", [cmd], { encoding: "utf-8", stdio: ["pipe", "pipe", "pipe"] });
    return true;
  } catch {
    return false;
  }
}

function resolveAgent(agentModel?: string): { cmd: string; args: string[] } {
  const m = agentModel ?? "sonnet";
  if (commandExists("claude")) {
    const args = [
      "--allowedTools", [
        "Read", "Write", "Edit", "Bash", "Glob", "Grep",
        "WebSearch", "WebFetch",
        "mcp__playwright_browser_navigate",
        "mcp__playwright_browser_snapshot",
        "mcp__playwright_browser_click",
        "mcp__playwright_browser_type",
        "mcp__playwright_browser_take_screenshot",
        "mcp__playwright_browser_wait",
        "mcp__playwright_browser_tab_list",
        "mcp__playwright_browser_close",
      ].join(","),
      "--effort", "high",
      "--model", m,
    ];
    // Add MCP config if it exists
    const mcpConfig = path.join(resolveWorkflowWorkspaceRoot(), "..", "worker-mcp-config.json");
    if (fs.existsSync(mcpConfig)) {
      args.push("--mcp-config", mcpConfig);
    }
    args.push("-p");
    return { cmd: "claude", args };
  }
  if (commandExists("codex")) {
    return {
      cmd: "codex",
      args: ["--model", m, "--full-auto"],
    };
  }
  // Fallback to openclaw agent
  return { cmd: "openclaw", args: ["agent", "--model", m] };
}

// ── Build prompt ────────────────────────────────────────────────────
const prompt = `You are an Antfarm workflow agent working in: ${workspaceDir}

YOUR TASK:
${input}

MANDATORY COMPLETION REPORTING:
When you finish the work, you MUST run one of these commands:

If SUCCESSFUL:
\`\`\`
cat <<'ANTFARM_EOF' > /tmp/antfarm-step-output.txt
STATUS: done
CHANGES: <describe what you did>
TESTS: <describe what tests you ran>
ANTFARM_EOF
cat /tmp/antfarm-step-output.txt | node ${cli} step complete "${stepId}"
\`\`\`

If FAILED:
\`\`\`
node ${cli} step fail "${stepId}" "description of what went wrong"
\`\`\`

RULES:
1. You MUST call step complete or step fail before you finish. This is non-negotiable.
2. Write output to a file first, then pipe via stdin (shell escaping breaks direct args).
3. If unsure whether to complete or fail, call step fail with an explanation.
4. NEVER call step complete with empty output — output MUST include at least a STATUS: line.
5. Work in the directory: ${workspaceDir}`;

// ── Spawn agent ─────────────────────────────────────────────────────
const agent = resolveAgent(model);
const agentArgs = [...agent.args, prompt];

log(`Spawning: ${agent.cmd} ${agent.args.slice(0, -1).join(" ")} -p "<prompt>"`);

const child = spawn(agent.cmd, agentArgs, {
  cwd: workspaceDir,
  stdio: ["ignore", "pipe", "pipe"],
  env: { ...process.env, FORCE_COLOR: "0" },
});

// Update work file with PID
try {
  const current = JSON.parse(fs.readFileSync(workFilePath, "utf-8"));
  current.pid = child.pid;
  current.dispatchedAt = new Date().toISOString();
  fs.writeFileSync(workFilePath, JSON.stringify(current, null, 2));
} catch {
  // best-effort
}

// Stream output to log
child.stdout?.on("data", (data: Buffer) => {
  logStream.write(data);
});
child.stderr?.on("data", (data: Buffer) => {
  logStream.write(data);
});

// ── Timeout handling ────────────────────────────────────────────────
let timedOut = false;
const timeoutMs = timeoutSeconds * 1000;

const timeoutHandle = setTimeout(() => {
  timedOut = true;
  log(`Worker timed out after ${timeoutSeconds}s — sending SIGTERM`);
  child.kill("SIGTERM");

  // SIGKILL after 10s grace period
  setTimeout(() => {
    try {
      process.kill(child.pid!, 0); // check if still alive
      log("Worker still alive after SIGTERM grace — sending SIGKILL");
      child.kill("SIGKILL");
    } catch {
      // already dead
    }
  }, 10_000);
}, timeoutMs);

// ── Handle exit ─────────────────────────────────────────────────────
child.on("close", (code, signal) => {
  clearTimeout(timeoutHandle);
  log(`Agent exited: code=${code} signal=${signal}`);

  // Check if step was already completed/failed by the agent
  try {
    const db = getDb();
    const step = db.prepare("SELECT status FROM steps WHERE id = ?").get(stepId) as { status: string } | undefined;

    if (step && (step.status === "done" || step.status === "failed")) {
      log(`Step already ${step.status} — no auto-fail needed`);
      cleanup();
      return;
    }
  } catch (err) {
    log(`Could not check step status: ${err}`);
  }

  // Agent exited without reporting — auto-fail
  const reason = timedOut
    ? `Worker timed out after ${Math.round(timeoutSeconds / 60)} minutes`
    : `Worker process exited without reporting (code: ${code}, signal: ${signal})`;

  log(`Auto-failing step: ${reason}`);
  try {
    failStep(stepId, reason);
  } catch (err) {
    log(`failStep error: ${err}`);
  }

  cleanup();
});

function cleanup(): void {
  log("Worker finished.");
  logStream.end();
}

child.on("error", (err) => {
  clearTimeout(timeoutHandle);
  log(`Spawn error: ${err.message}`);
  try {
    failStep(stepId, `Worker spawn error: ${err.message}`);
  } catch {
    // best-effort
  }
  cleanup();
});
