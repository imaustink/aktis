import { describe, expect, it } from "vitest";
import { FakeSyncSource, until } from "../__fixtures__/fake-sync.js";
import { testSyncs, type TestSyncs } from "../__fixtures__/temporal.js";
import { SyncPassError } from "./temporal.js";

/**
 * The Corpus sync workflow against a real Temporal (docs/adr/0058): one loop
 * per Corpus, so passes never overlap however many brokers ask; webhooks that
 * arrive mid-pass are kept; a run-now gets a pass of its own.
 */

async function closed(t: TestSyncs, corpus: string): Promise<boolean> {
  return !(await t.syncs.isRunning(corpus));
}

describe("corpusSync", () => {
  it("runs a full pass as soon as its loop starts, then waits for the interval", async () => {
    const t = await testSyncs();
    const fake = new FakeSyncSource("docs");
    await t.serve(fake.activities());

    await t.syncs.ensure(["docs"]);
    await until(() => fake.passesOf("docs").length === 1, "the first pass");
    expect(fake.passesOf("docs")[0]).toEqual({});

    // Long interval: nothing more happens on its own.
    await new Promise((r) => setTimeout(r, 500));
    expect(fake.passesOf("docs")).toHaveLength(1);
    await t.stop();
  });

  it("runs ONE loop per corpus however many replicas ensure it, so passes never overlap", async () => {
    const t = await testSyncs();
    const fake = new FakeSyncSource("docs");
    fake.hold();
    await t.serve(fake.activities());

    // Every replica calls ensure on every look; a second loop would run a
    // second full pass alongside the first, and two concurrent full passes
    // can each delete the chunks the other just wrote.
    await Promise.all([t.syncs.ensure(["docs"]), t.syncs.ensure(["docs"]), t.syncs.ensure(["docs"])]);
    await until(() => fake.passesOf("docs").length === 1, "the first pass");
    const runNow = t.syncs.runNow("docs");
    await new Promise((r) => setTimeout(r, 300));
    expect(fake.passesOf("docs")).toHaveLength(1);

    fake.release();
    await runNow;
    expect(fake.maxActive).toBe(1);
    await t.stop();
  });

  it("re-runs on the interval, re-reading it each cycle", async () => {
    const t = await testSyncs();
    const fake = new FakeSyncSource("docs");
    fake.intervalMs = 60_000;
    await t.serve(fake.activities());

    await t.syncs.ensure(["docs"]);
    await until(() => fake.passesOf("docs").length === 1, "the first pass");
    // Shortened after the first pass: it applies from the next re-read, which
    // follows the next pass. A run-now gets us there without waiting a minute.
    fake.intervalMs = 300;
    await t.syncs.runNow("docs");
    await until(() => fake.passesOf("docs").length >= 4, "passes on the shortened interval");
    expect(fake.passesOf("docs").every((p) => p.sourceIds === undefined)).toBe(true);
    await t.stop();
  });

  it("keeps a webhook that arrives mid-pass, and runs a partial pass for exactly what it named", async () => {
    const t = await testSyncs();
    const fake = new FakeSyncSource("docs");
    fake.hold();
    await t.serve(fake.activities());

    await t.syncs.ensure(["docs"]);
    await until(() => fake.passesOf("docs").length === 1, "the first pass");
    // The in-process scheduler dropped these: a pass was running, so the
    // webhook's pass was refused and nothing remembered it.
    await t.syncs.notify("docs", ["page-1"]);
    await t.syncs.notify("docs", ["page-2", "page-1"]);
    fake.release();

    await until(() => fake.passesOf("docs").length === 2, "the webhook's pass");
    expect(fake.passesOf("docs")[1]!.sourceIds?.sort()).toEqual(["page-1", "page-2"]);
    await t.stop();
  });

  it("escalates a webhook that names nothing to a full pass", async () => {
    const t = await testSyncs();
    const fake = new FakeSyncSource("docs");
    await t.serve(fake.activities());

    await t.syncs.ensure(["docs"]);
    await until(() => fake.passesOf("docs").length === 1, "the first pass");
    // "Something changed, we do not know what" must not be dropped: a deletion
    // that sent no event would otherwise never be noticed (ADR 0038 §4).
    await t.syncs.notify("docs", []);
    await until(() => fake.passesOf("docs").length === 2, "the escalated pass");
    expect(fake.passesOf("docs")[1]).toEqual({});
    await t.stop();
  });

  it("starts the loop for a webhook to a corpus that has none yet", async () => {
    const t = await testSyncs();
    const fake = new FakeSyncSource("docs");
    await t.serve(fake.activities());

    await t.syncs.notify("docs", ["page-9"]);
    // A fresh loop's first pass is a full one, which covers what was named.
    await until(() => fake.passesOf("docs").length === 1, "a pass");
    expect(fake.passesOf("docs")[0]).toEqual({});
    await new Promise((r) => setTimeout(r, 300));
    expect(fake.passesOf("docs")).toHaveLength(1);
    await t.stop();
  });

  it("answers a run-now with a full pass that STARTED after it was asked", async () => {
    const t = await testSyncs();
    const fake = new FakeSyncSource("docs");
    fake.hold();
    await t.serve(fake.activities());

    await t.syncs.ensure(["docs"]);
    await until(() => fake.passesOf("docs").length === 1, "the first pass");
    // The running pass may have listed the source before whatever prompted
    // this request, so it does not count.
    const asked = t.syncs.runNow("docs");
    fake.release();
    const result = await asked;

    expect(fake.passesOf("docs")).toHaveLength(2);
    expect(result).toMatchObject({ full: true, indexed: 3 });
    await t.stop();
  });

  it("reports a failed pass to the run-now that asked for it, and keeps looping", async () => {
    const t = await testSyncs();
    const fake = new FakeSyncSource("docs");
    await t.serve(fake.activities());

    await t.syncs.ensure(["docs"]);
    await until(() => fake.passesOf("docs").length === 1, "the first pass");
    fake.failNext = "qdrant unreachable";
    await expect(t.syncs.runNow("docs")).rejects.toThrow(SyncPassError);
    await until(() => fake.logs.some((l) => l.includes("qdrant unreachable")), "the failure logged");

    // The loop survives a failed pass: the next one runs as usual.
    await expect(t.syncs.runNow("docs")).resolves.toMatchObject({ full: true });
    expect(await t.syncs.isRunning("docs")).toBe(true);
    await t.stop();
  });

  it("ends once its corpus is no longer synced, and fails a run-now waiting on it", async () => {
    const t = await testSyncs();
    const fake = new FakeSyncSource("docs");
    await t.serve(fake.activities());

    await t.syncs.ensure(["docs"]);
    await until(() => fake.passesOf("docs").length === 1, "the first pass");
    fake.present.delete("docs");

    await expect(t.syncs.runNow("docs")).rejects.toThrow(SyncPassError);
    await until(() => closed(t, "docs"), "the loop to end");
    await t.stop();
  });
});
