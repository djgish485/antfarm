/**
 * Test: bug-fix-fast workflow has polling + fast cron config
 */

import path from "node:path";
import { loadWorkflowSpec } from "../dist/installer/workflow-spec.js";
import { describe, it } from "node:test";
import assert from "node:assert/strict";

const WORKFLOW_DIR = path.resolve(
  import.meta.dirname,
  "..",
  "workflows",
  "bug-fix-fast"
);

describe("bug-fix-fast workflow config", () => {
  it("has polling config with default model and 120s timeout", async () => {
    const spec = await loadWorkflowSpec(WORKFLOW_DIR);
    assert.ok(spec.polling, "polling config should exist");
    assert.equal(spec.polling?.model, "default");
    assert.equal(spec.polling?.timeoutSeconds, 120);
  });

  it("has fast cron interval override", async () => {
    const spec = await loadWorkflowSpec(WORKFLOW_DIR);
    assert.equal(spec.cron?.intervalMs, 60_000);
  });

  it("has expected agents and no PR stage", async () => {
    const spec = await loadWorkflowSpec(WORKFLOW_DIR);
    assert.deepEqual(spec.agents.map((a) => a.id), ["setup", "fixer", "verifier"]);
    assert.deepEqual(spec.steps.map((s) => s.id), ["setup", "fix", "verify"]);
  });

  it("workflow id and version are correct", async () => {
    const spec = await loadWorkflowSpec(WORKFLOW_DIR);
    assert.equal(spec.id, "bug-fix-fast");
    assert.equal(spec.version, 1);
  });
});
