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

type ActiveRunMatch = {
  run: ActiveRunRow;
  dedupeKey: string;
};

const DEDUPE_WORKFLOWS = new Set(["bug-fix", "bug-fix-fast"]);
const DEDUPE_MAX_AGE_MS = 60 * 60 * 1000; // 1 hour
const DEDUPE_SCAN_LIMIT = 30;

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function extractRepo(task: string): string {
  const m = task.match(/\brepo:\s*([^\s,;\n]+)/i);
  return m?.[1]?.trim().toLowerCase() || "unknown-repo";
}

function extractBranch(task: string): string {
  const m =
    task.match(/\bbranch:\s*([\w./-]+)/i) ||
    task.match(/\bon\s+branch\s+([\w./-]+)/i);
  return m?.[1]?.trim().toLowerCase() || "unknown-branch";
}

function extractIncidentText(task: string): string {
  const lines = task
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  for (const line of lines) {
    if (/^(repo|branch|task|acceptance criteria|requirements|constraints)\b/i.test(line)) {
      continue;
    }
    if (/^\[[ xX]\]/.test(line)) {
      continue;
    }

    return line
      .replace(/^repo:\s*[^\n]+?\bon\s+branch\s+[\w./-]+\.?\s*/i, "")
      .replace(/^repo:\s*[^\n]+\.?\s*/i, "")
      .trim();
  }

  return lines[0] || task;
}

function buildBugfixDedupeKey(task: string): string {
  const repo = extractRepo(task);
  const branch = extractBranch(task);
  const incident = normalizeWhitespace(extractIncidentText(task).toLowerCase())
    .replace(/[0-9a-f]{8}-[0-9a-f-]{27,}/gi, "<id>")
    .replace(/\b\d{4}-\d{2}-\d{2}\b/g, "<date>")
    .replace(/\brun\s*#?\d+\b/gi, "run")
    .replace(/\b\d{1,6}\b/g, "#")
    .slice(0, 220);

  return `${repo}|${branch}|${incident}`;
}

function findActiveDedupedBugfixRun(workflowId: string, taskTitle: string): ActiveRunMatch | null {
  if (!DEDUPE_WORKFLOWS.has(workflowId)) return null;
  if (process.env.ANTFARM_ALLOW_PARALLEL_BUGFIX === "1") return null;

  const db = getDb();
  const requestKey = buildBugfixDedupeKey(taskTitle);

  const rows = db.prepare(
    "SELECT id, run_number, workflow_id, task, status, created_at FROM runs WHERE workflow_id = ? AND status = 'running' ORDER BY created_at DESC LIMIT ?"
  ).all(workflowId, DEDUPE_SCAN_LIMIT) as ActiveRunRow[];

  for (const row of rows) {
    const createdAtMs = Date.parse(row.created_at);
    if (Number.isFinite(createdAtMs)) {
      const ageMs = Date.now() - createdAtMs;
      if (ageMs > DEDUPE_MAX_AGE_MS) continue;
    }

    const rowKey = buildBugfixDedupeKey(row.task || "");
    if (rowKey === requestKey) {
      return { run: row, dedupeKey: requestKey };
    }
  }

  return null;
}

export async function runWorkflow(params: {
  workflowId: string;
  taskTitle: string;
  notifyUrl?: string;
}): Promise<{ id: string; runNumber: number; workflowId: string; task: string; status: string }> {
  const workflowDir = resolveWorkflowDir(params.workflowId);
  const workflow = await loadWorkflowSpec(workflowDir);
  const db = getDb();

  const existingRunMatch = findActiveDedupedBugfixRun(workflow.id, params.taskTitle);
  if (existingRunMatch) {
    const existingRun = existingRunMatch.run;

    logger.info(`Deduped run request by incident key; reusing active run #${existingRun.run_number ?? "?"}`, {
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
