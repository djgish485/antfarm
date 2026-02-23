import fs from "node:fs";
import path from "node:path";
import { resolveWorkflowWorkspaceRoot } from "./paths.js";
import { getDb } from "../db.js";

const logger = {
  info: (msg: string, meta?: Record<string, unknown>) => {
    const ts = new Date().toISOString();
    const extra = meta ? ` ${JSON.stringify(meta)}` : "";
    process.stderr.write(`[${ts}] [cleanup] ${msg}${extra}\n`);
  },
  warn: (msg: string, meta?: Record<string, unknown>) => {
    const ts = new Date().toISOString();
    const extra = meta ? ` ${JSON.stringify(meta)}` : "";
    process.stderr.write(`[${ts}] [cleanup] WARN: ${msg}${extra}\n`);
  },
};

function getDirSizeBytes(dirPath: string): number {
  let total = 0;
  try {
    const stack = [dirPath];
    while (stack.length > 0) {
      const dir = stack.pop()!;
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          stack.push(fullPath);
        } else if (entry.isFile()) {
          try {
            total += fs.statSync(fullPath).size;
          } catch {
            // skip inaccessible files
          }
        }
      }
    }
  } catch {
    // directory may not exist
  }
  return total;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`;
}

/**
 * Clean up the workspace directory for a specific run.
 * Safe to call even if the workspace doesn't exist.
 */
export function cleanupRunWorkspace(runId: string, workflowId: string): void {
  const wsRoot = resolveWorkflowWorkspaceRoot();
  const runDir = path.join(wsRoot, workflowId, "runs", runId);

  if (fs.existsSync(runDir)) {
    const size = getDirSizeBytes(runDir);
    try {
      fs.rmSync(runDir, { recursive: true, force: true });
      logger.info(`Cleaned run workspace`, { runId: runId.slice(0, 8), workflowId, freed: formatBytes(size) });
    } catch (err) {
      logger.warn(`Failed to clean run workspace`, { runId: runId.slice(0, 8), error: String(err) });
    }
  }

  // Also clean merge-queue entries for this workflow if no other active runs
  const mergeQueueDir = path.join(wsRoot, workflowId, "merge-queue");
  if (fs.existsSync(mergeQueueDir)) {
    const db = getDb();
    const activeRuns = db.prepare(
      "SELECT COUNT(*) as cnt FROM runs WHERE workflow_id = ? AND status IN ('running', 'pending')"
    ).get(workflowId) as { cnt: number } | undefined;

    if (!activeRuns || activeRuns.cnt === 0) {
      const size = getDirSizeBytes(mergeQueueDir);
      try {
        fs.rmSync(mergeQueueDir, { recursive: true, force: true });
        logger.info(`Cleaned merge-queue`, { workflowId, freed: formatBytes(size) });
      } catch (err) {
        logger.warn(`Failed to clean merge-queue`, { workflowId, error: String(err) });
      }
    }
  }
}

/**
 * Clean all workspaces for terminal runs. Used by `antfarm cleanup` CLI command.
 */
export function cleanupAllTerminalWorkspaces(dryRun = false): { freed: number; cleaned: number } {
  const db = getDb();
  const wsRoot = resolveWorkflowWorkspaceRoot();
  let totalFreed = 0;
  let totalCleaned = 0;

  // Get all terminal run IDs
  const terminalRuns = db.prepare(
    "SELECT id, workflow_id FROM runs WHERE status IN ('completed', 'cancelled', 'failed')"
  ).all() as Array<{ id: string; workflow_id: string }>;

  const terminalIds = new Set(terminalRuns.map((r) => r.id));

  // Scan all workflow workspace dirs
  if (!fs.existsSync(wsRoot)) return { freed: 0, cleaned: 0 };

  for (const wfDir of fs.readdirSync(wsRoot, { withFileTypes: true })) {
    if (!wfDir.isDirectory()) continue;
    const runsDir = path.join(wsRoot, wfDir.name, "runs");
    if (!fs.existsSync(runsDir)) continue;

    for (const runDir of fs.readdirSync(runsDir, { withFileTypes: true })) {
      if (!runDir.isDirectory()) continue;
      if (!terminalIds.has(runDir.name)) continue;

      const fullPath = path.join(runsDir, runDir.name);
      const size = getDirSizeBytes(fullPath);

      if (dryRun) {
        logger.info(`[dry-run] Would remove ${fullPath} (${formatBytes(size)})`);
      } else {
        try {
          fs.rmSync(fullPath, { recursive: true, force: true });
          logger.info(`Removed ${fullPath} (${formatBytes(size)})`);
        } catch (err) {
          logger.warn(`Failed to remove ${fullPath}: ${err}`);
        }
      }
      totalFreed += size;
      totalCleaned++;
    }

    // Clean merge-queue if no active runs for this workflow
    const mergeQueueDir = path.join(wsRoot, wfDir.name, "merge-queue");
    if (fs.existsSync(mergeQueueDir)) {
      const activeRuns = db.prepare(
        "SELECT COUNT(*) as cnt FROM runs WHERE workflow_id = ? AND status IN ('running', 'pending')"
      ).get(wfDir.name) as { cnt: number } | undefined;

      if (!activeRuns || activeRuns.cnt === 0) {
        const size = getDirSizeBytes(mergeQueueDir);
        if (dryRun) {
          logger.info(`[dry-run] Would remove merge-queue ${mergeQueueDir} (${formatBytes(size)})`);
        } else {
          try {
            fs.rmSync(mergeQueueDir, { recursive: true, force: true });
            logger.info(`Removed merge-queue ${mergeQueueDir} (${formatBytes(size)})`);
          } catch (err) {
            logger.warn(`Failed to remove merge-queue: ${err}`);
          }
        }
        totalFreed += size;
      }
    }
  }

  logger.info(dryRun ? `[dry-run] Would free ${formatBytes(totalFreed)}` : `Total freed: ${formatBytes(totalFreed)}`);
  return { freed: totalFreed, cleaned: totalCleaned };
}
