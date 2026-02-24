import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getDb } from "../db.js";
import { resolveBundledWorkflowsDir } from "../installer/paths.js";
import YAML from "yaml";

import type { RunInfo, StepInfo } from "../installer/status.js";
import { getRunEvents } from "../installer/events.js";
import { getMedicStatus, getRecentMedicChecks } from "../medic/medic.js";
import { listActiveWorkers, findWorkFileByStepId, getWorkerLogPath, isWorkerAlive, removeWorkFile } from "../installer/worker-state.js";
import { failStep } from "../installer/step-ops.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

interface WorkflowDef {
  id: string;
  name: string;
  steps: Array<{ id: string; agent: string }>;
}

function loadWorkflows(): WorkflowDef[] {
  const dir = resolveBundledWorkflowsDir();
  const results: WorkflowDef[] = [];
  try {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const ymlPath = path.join(dir, entry.name, "workflow.yml");
      if (!fs.existsSync(ymlPath)) continue;
      const parsed = YAML.parse(fs.readFileSync(ymlPath, "utf-8"));
      results.push({
        id: parsed.id ?? entry.name,
        name: parsed.name ?? entry.name,
        steps: (parsed.steps ?? []).map((s: any) => ({ id: s.id, agent: s.agent })),
      });
    }
  } catch { /* empty */ }
  return results;
}

function getRuns(workflowId?: string, includeArchived = false): Array<RunInfo & { steps: StepInfo[] }> {
  const db = getDb();
  const archiveFilter = includeArchived ? "" : " AND archived_at IS NULL";
  const runs = workflowId
    ? db.prepare(`SELECT * FROM runs WHERE workflow_id = ?${archiveFilter} ORDER BY created_at DESC`).all(workflowId) as RunInfo[]
    : db.prepare(`SELECT * FROM runs WHERE 1=1${archiveFilter} ORDER BY created_at DESC`).all() as RunInfo[];
  return runs.map((r) => {
    const steps = db.prepare("SELECT * FROM steps WHERE run_id = ? ORDER BY step_index ASC").all(r.id) as StepInfo[];
    return { ...r, steps };
  });
}

/** Archive completed/cancelled/failed runs older than `hours` hours. */
function archiveOldRuns(hours = 16): { archived: number } {
  const db = getDb();
  const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  const result = db.prepare(
    `UPDATE runs SET archived_at = datetime('now')
     WHERE archived_at IS NULL
       AND status IN ('completed', 'cancelled', 'failed')
       AND updated_at < ?`
  ).run(cutoff);
  return { archived: Number(result.changes) };
}

function getRunById(id: string): (RunInfo & { steps: StepInfo[] }) | null {
  const db = getDb();
  const run = db.prepare("SELECT * FROM runs WHERE id = ?").get(id) as RunInfo | undefined;
  if (!run) return null;
  const steps = db.prepare("SELECT * FROM steps WHERE run_id = ? ORDER BY step_index ASC").all(run.id) as StepInfo[];
  return { ...run, steps };
}

function json(res: http.ServerResponse, data: unknown, status = 200) {
  res.writeHead(status, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
  res.end(JSON.stringify(data));
}

function serveHTML(res: http.ServerResponse) {
  const htmlPath = path.join(__dirname, "index.html");
  // In dist, index.html won't exist—serve from src
  const srcHtmlPath = path.resolve(__dirname, "..", "..", "src", "server", "index.html");
  const filePath = fs.existsSync(htmlPath) ? htmlPath : srcHtmlPath;
  res.writeHead(200, { "Content-Type": "text/html" });
  res.end(fs.readFileSync(filePath, "utf-8"));
}

export function startDashboard(port = 3333): http.Server {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url ?? "/", `http://localhost:${port}`);
    const p = url.pathname;

    if (p === "/api/workflows") {
      return json(res, loadWorkflows());
    }

    const eventsMatch = p.match(/^\/api\/runs\/([^/]+)\/events$/);
    if (eventsMatch) {
      return json(res, getRunEvents(eventsMatch[1]));
    }

    // Worker log by run + step (persists after worker finishes)
    const stepLogMatch = p.match(/^\/api\/runs\/([^/]+)\/steps\/([^/]+)\/log$/);
    if (stepLogMatch) {
      const [, runId, stepId] = stepLogMatch;
      const logPath = getWorkerLogPath(runId, stepId);
      try {
        const content = fs.readFileSync(logPath, "utf-8");
        const lines = content.split("\n");
        const tail = lines.slice(-500).join("\n");
        return json(res, { log: tail, found: true });
      } catch {
        return json(res, { log: "", found: false });
      }
    }

    const storiesMatch = p.match(/^\/api\/runs\/([^/]+)\/stories$/);
    if (storiesMatch) {
      const db = getDb();
      const stories = db.prepare(
        "SELECT * FROM stories WHERE run_id = ? ORDER BY story_index ASC"
      ).all(storiesMatch[1]);
      return json(res, stories);
    }

    const runMatch = p.match(/^\/api\/runs\/(.+)$/);
    if (runMatch) {
      const run = getRunById(runMatch[1]);
      return run ? json(res, run) : json(res, { error: "not found" }, 404);
    }

    if (p === "/api/runs") {
      const wf = url.searchParams.get("workflow") ?? undefined;
      const includeArchived = url.searchParams.get("archived") === "true";
      return json(res, getRuns(wf, includeArchived));
    }

    if (p === "/api/runs/archive") {
      const hours = parseInt(url.searchParams.get("hours") ?? "16", 10);
      return json(res, archiveOldRuns(hours));
    }

    // Medic API
    if (p === "/api/medic/status") {
      return json(res, getMedicStatus());
    }

    if (p === "/api/medic/checks") {
      const limit = parseInt(url.searchParams.get("limit") ?? "20", 10);
      return json(res, getRecentMedicChecks(limit));
    }

    // Worker API
    if (p === "/api/workers") {
      return json(res, listActiveWorkers());
    }

    const workerLogMatch = p.match(/^\/api\/workers\/([^/]+)\/log$/);
    if (workerLogMatch) {
      const work = findWorkFileByStepId(workerLogMatch[1]);
      if (!work) return json(res, { error: "not found" }, 404);
      const logPath = getWorkerLogPath(work.runId, work.stepId);
      try {
        const content = fs.readFileSync(logPath, "utf-8");
        const lines = content.split("\n");
        const tail = lines.slice(-200).join("\n");
        return json(res, { log: tail });
      } catch {
        return json(res, { log: "(no log file)" });
      }
    }

    // Archive/unarchive a single run
    const archiveRunMatch = p.match(/^\/api\/runs\/([^/]+)\/archive$/);
    if (archiveRunMatch && req.method === "POST") {
      const db = getDb();
      db.prepare("UPDATE runs SET archived_at = datetime('now') WHERE id = ?").run(archiveRunMatch[1]);
      return json(res, { ok: true });
    }
    const unarchiveRunMatch = p.match(/^\/api\/runs\/([^/]+)\/unarchive$/);
    if (unarchiveRunMatch && req.method === "POST") {
      const db = getDb();
      db.prepare("UPDATE runs SET archived_at = NULL WHERE id = ?").run(unarchiveRunMatch[1]);
      return json(res, { ok: true });
    }

    const workerKillMatch = p.match(/^\/api\/workers\/([^/]+)\/kill$/);
    if (workerKillMatch && req.method === "POST") {
      const work = findWorkFileByStepId(workerKillMatch[1]);
      if (!work) return json(res, { error: "not found" }, 404);
      if (work.pid && isWorkerAlive(work.pid)) {
        try { process.kill(work.pid, "SIGTERM"); } catch {}
        setTimeout(() => {
          try { process.kill(work.pid!, 0); process.kill(work.pid!, "SIGKILL"); } catch {}
        }, 3000);
      }
      try {
        failStep(work.stepId, "Worker killed via dashboard");
      } catch {
        // step may already be terminal
      }
      removeWorkFile(work.runId, work.stepId);
      return json(res, { ok: true });
    }

    // Serve fonts
    if (p.startsWith("/fonts/")) {
      const fontName = path.basename(p);
      const fontPath = path.resolve(__dirname, "..", "..", "assets", "fonts", fontName);
      const srcFontPath = path.resolve(__dirname, "..", "..", "src", "..", "assets", "fonts", fontName);
      const resolvedFont = fs.existsSync(fontPath) ? fontPath : srcFontPath;
      if (fs.existsSync(resolvedFont)) {
        res.writeHead(200, { "Content-Type": "font/woff2", "Cache-Control": "public, max-age=31536000", "Access-Control-Allow-Origin": "*" });
        return res.end(fs.readFileSync(resolvedFont));
      }
    }

    // Serve logo
    if (p === "/logo.jpeg") {
      const logoPath = path.resolve(__dirname, "..", "..", "assets", "logo.jpeg");
      const srcLogoPath = path.resolve(__dirname, "..", "..", "src", "..", "assets", "logo.jpeg");
      const resolvedLogo = fs.existsSync(logoPath) ? logoPath : srcLogoPath;
      if (fs.existsSync(resolvedLogo)) {
        res.writeHead(200, { "Content-Type": "image/jpeg", "Cache-Control": "public, max-age=86400" });
        return res.end(fs.readFileSync(resolvedLogo));
      }
    }

    // Serve frontend
    serveHTML(res);
  });

  server.listen(port, () => {
    console.log(`Antfarm Dashboard: http://localhost:${port}`);
  });

  return server;
}
