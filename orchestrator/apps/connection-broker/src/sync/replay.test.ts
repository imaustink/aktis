import { readdir, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { historyFromJSON } from "@temporalio/common/proto-utils";
import { Worker } from "@temporalio/worker";
import { describe, expect, it } from "vitest";
import { HISTORY_TASK_QUEUE, workflows } from "../__fixtures__/temporal.js";

/**
 * Replays committed workflow histories (src/sync/__histories__, written by
 * capture-histories.test.ts) against the current workflow code (docs/adr/0057
 * "Changing a workflow", docs/adr/0058). A change to what `corpusSync` does --
 * an activity, timer, signal or update handler added, removed or reordered --
 * fails here with a determinism error. That is the failure every running
 * Corpus loop would hit on the next rollout: gate the change with `patched()`
 * instead of re-capturing the histories.
 */

const DIR = fileURLToPath(new URL("./__histories__/", import.meta.url));

describe("workflow replay (docs/adr/0058)", () => {
  it("replays every committed history without a determinism error", async () => {
    const files = (await readdir(DIR)).filter((f) => f.endsWith(".json")).sort();
    expect(files.some((f) => f.startsWith("corpusSync-")), "no history for corpusSync").toBe(true);
    const histories = await Promise.all(
      files.map(async (f) => ({ workflowId: f.replace(/\.json$/, ""), history: historyFromJSON(JSON.parse(await readFile(`${DIR}${f}`, "utf8"))) })),
    );
    const failures: string[] = [];
    // The histories' task queue, as the gateway's replay does: ReplayWorkerOptions
    // leaves `taskQueue` out of its type, but the replay worker takes it.
    const options = { workflowBundle: await workflows(), taskQueue: HISTORY_TASK_QUEUE } as Parameters<typeof Worker.runReplayHistories>[0];
    for await (const result of Worker.runReplayHistories(options, histories)) {
      if (result.error) failures.push(`${result.workflowId}: ${result.error.message}`);
    }
    expect(failures).toEqual([]);
  }, 120_000);
});
