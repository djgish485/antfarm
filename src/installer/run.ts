import crypto from "node:crypto";
import { loadWorkflowSpec } from "./workflow-spec.js";
import { resolveWorkflowDir } from "./paths.js";
import { getDb, nextRunNumber } from "../db.js";
import { logger } from "../lib/logger.js";
import { ensureWorkflowCrons } from "./agent-cron.js";
import { emitEvent } from "./events.js";


type ActiveRunRow = {
  id: string;
  run_number: number | null;
  workflow_id: string;
  task: string;
  status: string;
  created_at: string;
};

const SINGLE_FLIGHT_WORKFLOWS = new Set(["bug-fix", "bug-fix-fast"]);
const SINGLE_FLIGHT_MAX_AGE_MS = 60 * 60 * 1000; // 1 hour

function findActiveSingleFlightRun(workflowId: string): ActiveRunRow | null {
  if (!SINGLE_FLIGHT_WORKFLOWS.has(workflowId)) return null;
  if (process.env.ANTFARM_ALLOW_PARALLEL_BUGFIX === "1") return null;

  const db = getDb();
  const row = db.prepare(
    "SELECT id, run_number, workflow_id, task, status, created_at FROM runs WHERE workflow_id = ? AND status = 'running' ORDER BY created_at DESC LIMIT 1"
  ).get(workflowId) as ActiveRunRow | undefined;

  if (!row) return null;

  const createdAtMs = Date.parse(row.created_at);
  if (Number.isFinite(createdAtMs)) {
    const ageMs = Date.now() - createdAtMs;
    if (ageMs > SINGLE_FLIGHT_MAX_AGE_MS) return null;
  }

  return row;
}

export async function runWorkflow(params: {
  workflowId: string;
  taskTitle: string;
  notifyUrl?: string;
}): Promise<{ id: string; runNumber: number; workflowId: string; task: string; status: string }> {
  const workflowDir = resolveWorkflowDir(params.workflowId);
  const workflow = await loadWorkflowSpec(workflowDir);
  const db = getDb();

  const existingRun = findActiveSingleFlightRun(workflow.id);
  if (existingRun) {
    logger.info(`Deduped run request; reusing active run #${existingRun.run_number ?? "?"}`, {
      workflowId: workflow.id,
      runId: existingRun.id,
      stepId: workflow.steps[0]?.id,
    });

    return {
      id: existingRun.id,
      runNumber: existingRun.run_number ?? 0,
      workflowId: existingRun.workflow_id,
      task: existingRun.task,
      status: existingRun.status,
    };
  }

  const now = new Date().toISOString();
  const runId = crypto.randomUUID();
  const runNumber = nextRunNumber();

  const initialContext: Record<string, string> = {
    task: params.taskTitle,
    ...workflow.context,
  };

  db.exec("BEGIN");
  try {
    const notifyUrl = params.notifyUrl ?? workflow.notifications?.url ?? null;
    const insertRun = db.prepare(
      "INSERT INTO runs (id, run_number, workflow_id, task, status, context, notify_url, created_at, updated_at) VALUES (?, ?, ?, ?, 'running', ?, ?, ?, ?)"
    );
    insertRun.run(runId, runNumber, workflow.id, params.taskTitle, JSON.stringify(initialContext), notifyUrl, now, now);

    const insertStep = db.prepare(
      "INSERT INTO steps (id, run_id, step_id, agent_id, step_index, input_template, expects, status, max_retries, type, loop_config, retry_step_id, on_exhausted_workflow, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );

    for (let i = 0; i < workflow.steps.length; i++) {
      const step = workflow.steps[i];
      const stepUuid = crypto.randomUUID();
      const agentId = `${workflow.id}_${step.agent}`;
      const status = i === 0 ? "pending" : "waiting";
      const maxRetries = step.max_retries ?? step.on_fail?.max_retries ?? 2;
      const stepType = step.type ?? "single";
      const loopConfig = step.loop ? JSON.stringify(step.loop) : null;
      const retryStepId = step.on_fail?.retry_step ?? null;
      const exhaustedEscalateRaw = step.on_fail?.on_exhausted?.escalate_to ?? step.on_fail?.escalate_to ?? null;
      const onExhaustedWorkflow = exhaustedEscalateRaw && exhaustedEscalateRaw !== "human"
        ? exhaustedEscalateRaw
        : null;
      insertStep.run(
        stepUuid,
        runId,
        step.id,
        agentId,
        i,
        step.input,
        step.expects,
        status,
        maxRetries,
        stepType,
        loopConfig,
        retryStepId,
        onExhaustedWorkflow,
        now,
        now
      );
    }

    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }

  // Start crons for this workflow (no-op if already running from another run)
  try {
    await ensureWorkflowCrons(workflow);
  } catch (err) {
    // Roll back the run since it can't advance without crons
    const db2 = getDb();
    db2.prepare("UPDATE runs SET status = 'failed', updated_at = ? WHERE id = ?").run(new Date().toISOString(), runId);
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`Cannot start workflow run: cron setup failed. ${message}`);
  }

  emitEvent({ ts: new Date().toISOString(), event: "run.started", runId, workflowId: workflow.id });

  logger.info(`Run started: "${params.taskTitle}"`, {
    workflowId: workflow.id,
    runId,
    stepId: workflow.steps[0]?.id,
  });

  return { id: runId, runNumber, workflowId: workflow.id, task: params.taskTitle, status: "running" };
}
