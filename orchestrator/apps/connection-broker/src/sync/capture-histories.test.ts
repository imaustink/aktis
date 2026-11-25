import { mkdir, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { fileURLToPath } from "node:url";
import { historyToJSON } from "@temporalio/common/proto-utils";
import { describe, expect, it } from "vitest";
import { FakeSyncSource, until } from "../__fixtures__/fake-sync.js";
import { HISTORY_TASK_QUEUE, testSyncs, type TestSyncs } from "../__fixtures__/temporal.js";
import { SyncPassError } from "./temporal.js";

/**
 * Captures the workflow histories that replay.test.ts replays (docs/adr/0057
 * "Changing a workflow", docs/adr/0058). Skipped unless
 * CAPTURE_WORKFLOW_HISTORIES is set:
 *
 *   CAPTURE_WORKFLOW_HISTORIES=1 npx vitest run src/sync/capture-histories.test.ts
 *
 * It drives `corpusSync` through the paths a change is most likely to
 * reorder -- a webhook kept across a running pass, an escalated webhook, a
 * run-now, a failed pass, the interval, and the loop ending -- and writes
 * each history to src/sync/__histories__/. Re-capture only alongside a change
 * that is meant to break replay (one shipped behind Worker Versioning, say),
 * never to make the replay test pass.
 */

const OUT = fileURLToPath(new URL("./__histories__/", import.meta.url));

async function save(t: TestSyncs, name: string, corpus: string): Promise<void> {
  const history = await t.client.workflow.getHandle(t.syncs.workflowId(corpus)).fetchHistory();
  await mkdir(OUT, { recursive: true });
  // Who ran it (pid@host, also in sticky queue names) and where an error was thrown are this machine's, not the workflow's: replay reads neither.
  const scrubbed = JSON.parse(historyToJSON(history), (key, value) => {
    if (key === "identity" && typeof value === "string") return "capture-histories";
    if (key === "stackTrace") return "";
    return typeof value === "string" ? value.replaceAll(hostname(), "capture-host") : value;
  });
  await writeFile(`${OUT}${name}.json`, `${JSON.stringify(scrubbed, null, 2)}\n`);
}

describe.skipIf(!process.env.CAPTURE_WORKFLOW_HISTORIES)("capture workflow histories for replay.test.ts", () => {
  it("webhooks: kept across a running pass, a partial pass, an escalation, then the corpus goes", async () => {
    const t = await testSyncs({ taskQueue: HISTORY_TASK_QUEUE });
    const corpus = "capture-webhooks";
    const fake = new FakeSyncSource(corpus);
    fake.hold();
    await t.serve(fake.activities());

    await t.syncs.ensure([corpus]);
    await until(() => fake.passesOf(corpus).length === 1, "the first pass");
    await t.syncs.notify(corpus, ["page-1"]);
    await t.syncs.notify(corpus, ["page-2"]);
    fake.release();
    await until(() => fake.passesOf(corpus).length === 2, "the partial pass");
    await t.syncs.notify(corpus, []);
    await until(() => fake.passesOf(corpus).length === 3, "the escalated pass");

    fake.present.delete(corpus);
    await expect(t.syncs.runNow(corpus)).rejects.toThrow(SyncPassError);
    await until(async () => !(await t.syncs.isRunning(corpus)), "the loop to end");
    await save(t, "corpusSync-webhooks-then-gone", corpus);
    await t.stop();
  });

  it("run-now: a failed pass, a good one, the interval, then the corpus goes while the loop waits", async () => {
    const t = await testSyncs({ taskQueue: HISTORY_TASK_QUEUE });
    const corpus = "capture-run-now";
    const fake = new FakeSyncSource(corpus);
    await t.serve(fake.activities());

    await t.syncs.ensure([corpus]);
    await until(() => fake.passesOf(corpus).length === 1, "the first pass");
    fake.failNext = "qdrant unreachable";
    await expect(t.syncs.runNow(corpus)).rejects.toThrow(SyncPassError);
    fake.intervalMs = 400;
    await t.syncs.runNow(corpus);
    await until(() => fake.passesOf(corpus).length >= 4, "a pass on the interval");
    fake.present.delete(corpus);
    await until(async () => !(await t.syncs.isRunning(corpus)), "the loop to end");
    await save(t, "corpusSync-run-now-interval", corpus);
    await t.stop();
  });
});
