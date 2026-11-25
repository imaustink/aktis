import { readdir, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { historyFromJSON } from "@temporalio/common/proto-utils";
import { Worker } from "@temporalio/worker";
import { describe, expect, it } from "vitest";
import { HISTORY_TASK_QUEUE, workflows } from "../__fixtures__/temporal.js";

/**
 * Replays committed workflow histories (src/runs/__histories__, written by
 * capture-histories.test.ts) against the current workflow code (docs/adr/0057
 * "Changing a workflow"). A change to what a workflow does -- an activity,
 * timer, signal or child added, removed or reordered -- fails here with a
 * determinism error. That is the failure a running `heldRunEvent` (weeks),
 * `runQueue` (hours) or `eventRun` would hit on the next rollout: gate the
 * change with `patched()` instead of re-capturing the histories.
 */

const DIR = fileURLToPath(new URL("./__histories__/", import.meta.url));

describe("workflow replay (docs/adr/0057)", () => {
  it("replays every committed history without a determinism error", async () => {
    const files = (await readdir(DIR)).filter((f) => f.endsWith(".json")).sort();
    // One history per workflow type, at least: a type with none is not checked at all.
    for (const type of ["eventRun", "heldRunEvent", "runQueue", "conflictSweep", "claudeCredentialRefresh", "claudeCredentialSweep"]) {
      expect(files.some((f) => f.startsWith(`${type}-`)), `no history for ${type}`).toBe(true);
    }
    const histories = await Promise.all(
      files.map(async (f) => ({ workflowId: f.replace(/\.json$/, ""), history: historyFromJSON(JSON.parse(await readFile(`${DIR}${f}`, "utf8"))) })),
    );
    const failures: string[] = [];
    // The histories' task queue, not the SDK's generated replay queue (see
    // HISTORY_TASK_QUEUE). ReplayWorkerOptions leaves `taskQueue` out of its
    // type, but the replay worker takes it; were that ever ignored, every
    // child workflow id would mismatch and this test would fail, not pass.
    const options = { workflowBundle: await workflows(), taskQueue: HISTORY_TASK_QUEUE } as Parameters<typeof Worker.runReplayHistories>[0];
    for await (const result of Worker.runReplayHistories(options, histories)) {
      if (result.error) failures.push(`${result.workflowId}: ${result.error.message}`);
    }
    expect(failures).toEqual([]);
  }, 120_000);
});
