import { spawn } from "node:child_process";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  writeWorkFile,
  readWorkFile,
  isWorkerAlive,
  getWorkerLogPath,
  type WorkFile,
} from "../installer/worker-state.js";
import { claimStep } from "../installer/step-ops.js";
import { getDb } from "../db.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

export interface DispatchResult {
  dispatched: boolean;
  pid?: number;
  stepId?: string;
  runId?: string;
  reason?: string;
}

export function dispatchWorker(agentRole: string, timeoutSeconds?: number): DispatchResult {
  // Step 1: Claim a pending step
  const claim = claimStep(agentRole);
  if (!claim.found || !claim.stepId || !claim.runId || !claim.resolvedInput) {
    return { dispatched: false, reason: "NO_WORK" };
  }

  const { stepId, runId, resolvedInput } = claim;

  // Step 2: Check for existing worker
  const existing = readWorkFile(runId, stepId);
  if (existing?.pid && isWorkerAlive(existing.pid)) {
    return { dispatched: false, reason: `Worker already running (PID ${existing.pid})`, stepId, runId };
  }

  // Step 3: Resolve workflow ID from the run
  const db = getDb();
  const run = db.prepare("SELECT workflow_id FROM runs WHERE id = ?").get(runId) as { workflow_id: string } | undefined;
  const workflowId = run?.workflow_id ?? "unknown";

  // Step 4: Determine model from the step's agent config
  const stepRow = db.prepare("SELECT agent_id FROM steps WHERE id = ?").get(stepId) as { agent_id: string } | undefined;
  const model = resolveModelForAgent(stepRow?.agent_id);

  // Step 5: Write work file
  const workFile: WorkFile = {
    stepId,
    runId,
    workflowId,
    agentRole,
    input: resolvedInput,
    model,
    claimedAt: new Date().toISOString(),
  };
  const workFilePath = writeWorkFile(runId, stepId, workFile);

  // Step 6: Spawn worker-pty.js as a detached background process
  const workerScript = resolve(__dirname, "worker-pty.js");
  const logPath = getWorkerLogPath(runId, stepId);
  const workerArgs = ["--work-file", workFilePath];
  if (timeoutSeconds) {
    workerArgs.push("--timeout", String(timeoutSeconds));
  }

  const child = spawn("node", [workerScript, ...workerArgs], {
    detached: true,
    stdio: ["ignore", "ignore", "ignore"],
    env: process.env,
  });

  child.unref();
  const pid = child.pid!;

  // Step 7: Update work file with PID
  workFile.pid = pid;
  workFile.dispatchedAt = new Date().toISOString();
  writeWorkFile(runId, stepId, workFile);

  return { dispatched: true, pid, stepId, runId };
}

function resolveModelForAgent(agentId?: string): string | undefined {
  if (!agentId) return undefined;
  // Try to look up the agent's model from workflow config, but this is optional
  // The worker-pty script will use a sensible default if not set
  return undefined;
}
