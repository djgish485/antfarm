import fs from "node:fs";
import path from "node:path";
import { resolveAntfarmRoot } from "./paths.js";

export interface WorkFile {
  stepId: string;
  runId: string;
  workflowId: string;
  agentRole: string;
  input: string;
  model?: string;
  claimedAt: string;
  pid?: number;
  dispatchedAt?: string;
}

export function getWorkersDir(): string {
  const dir = path.join(resolveAntfarmRoot(), "workers");
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function workFileName(runId: string, stepId: string): string {
  return `${runId.slice(0, 8)}-${stepId.slice(0, 8)}.json`;
}

export function writeWorkFile(runId: string, stepId: string, data: WorkFile): string {
  const filePath = path.join(getWorkersDir(), workFileName(runId, stepId));
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
  return filePath;
}

export function readWorkFile(runId: string, stepId: string): WorkFile | null {
  const filePath = path.join(getWorkersDir(), workFileName(runId, stepId));
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf-8")) as WorkFile;
  } catch {
    return null;
  }
}

export function removeWorkFile(runId: string, stepId: string): void {
  const filePath = path.join(getWorkersDir(), workFileName(runId, stepId));
  try {
    fs.unlinkSync(filePath);
  } catch {
    // already gone
  }
}

export function isWorkerAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export function getWorkerLogPath(runId: string, stepId: string): string {
  return path.join(getWorkersDir(), `${runId.slice(0, 8)}-${stepId.slice(0, 8)}.log`);
}

export interface ActiveWorker {
  stepId: string;
  runId: string;
  workflowId: string;
  agentRole: string;
  pid: number;
  claimedAt: string;
  dispatchedAt?: string;
  elapsedMs: number;
  logPath: string;
}

export function listActiveWorkers(): ActiveWorker[] {
  const dir = getWorkersDir();
  const workers: ActiveWorker[] = [];

  let entries: string[];
  try {
    entries = fs.readdirSync(dir).filter(f => f.endsWith(".json"));
  } catch {
    return [];
  }

  for (const file of entries) {
    try {
      const data: WorkFile = JSON.parse(fs.readFileSync(path.join(dir, file), "utf-8"));
      if (!data.pid || !isWorkerAlive(data.pid)) continue;
      const startTime = data.dispatchedAt ?? data.claimedAt;
      workers.push({
        stepId: data.stepId,
        runId: data.runId,
        workflowId: data.workflowId,
        agentRole: data.agentRole,
        pid: data.pid,
        claimedAt: data.claimedAt,
        dispatchedAt: data.dispatchedAt,
        elapsedMs: Date.now() - new Date(startTime).getTime(),
        logPath: getWorkerLogPath(data.runId, data.stepId),
      });
    } catch {
      // skip malformed files
    }
  }

  return workers;
}

export function findWorkFileByStepId(stepId: string): WorkFile | null {
  const dir = getWorkersDir();
  let entries: string[];
  try {
    entries = fs.readdirSync(dir).filter(f => f.endsWith(".json"));
  } catch {
    return null;
  }

  const prefix = stepId.slice(0, 8);
  for (const file of entries) {
    if (file.includes(prefix)) {
      try {
        return JSON.parse(fs.readFileSync(path.join(dir, file), "utf-8")) as WorkFile;
      } catch {
        continue;
      }
    }
  }
  return null;
}
