import { describe, expect, it, vi } from "vitest";
import { InMemoryInflightRunStore, InflightRunRecovery, type InflightRun } from "./inflight-runs.js";

function run(key: string): InflightRun {
  return {
    key,
    invocationId: key,
    envelope: { source: "github", kind: "webhook", id: `d-${key}`, time: "", headers: {}, body: {}, repository: "acme/widgets" },
    trigger: { name: "t", sessionId: "s", reply: null },
    recordedAt: 0,
  };
}

describe("InMemoryInflightRunStore leases", () => {
  it("an owner can take a free lease and renew its own, but not take one held by another until it expires", async () => {
    let now = 1_000;
    const store = new InMemoryInflightRunStore(() => now);
    expect(await store.lease("k", "a", 100)).toBe(true);
    expect(await store.lease("k", "b", 100)).toBe(false);
    now += 50;
    expect(await store.lease("k", "a", 100)).toBe(true); // renewed to 1150
    now += 80;
    expect(await store.lease("k", "b", 100)).toBe(false);
    now += 30;
    expect(await store.lease("k", "b", 100)).toBe(true);
  });

  it("clear drops the run and its lease", async () => {
    const store = new InMemoryInflightRunStore();
    await store.save(run("k"));
    await store.lease("k", "a", 60_000);
    await store.clear("k");
    expect(await store.list()).toEqual([]);
    expect(await store.lease("k", "b", 60_000)).toBe(true);
  });
});

describe("InflightRunRecovery", () => {
  it("resumes only runs that are unleased and not already running here, leasing each to itself", async () => {
    const store = new InMemoryInflightRunStore();
    await Promise.all(["free", "held", "mine"].map((k) => store.save(run(k))));
    await store.lease("held", "other", 60_000);
    const resume = vi.fn().mockResolvedValue(undefined);
    const recovery = new InflightRunRecovery({ store, owner: "me", resume, isRunning: (k) => k === "mine" });

    await recovery.sweepOnce();

    expect(resume.mock.calls.map((c) => (c[0] as InflightRun).key)).toEqual(["free"]);
    expect(await store.lease("free", "other", 60_000)).toBe(false);
  });

  it("reports a failed resume instead of throwing", async () => {
    const store = new InMemoryInflightRunStore();
    await store.save(run("k"));
    const onError = vi.fn();
    const recovery = new InflightRunRecovery({
      store,
      owner: "me",
      resume: vi.fn().mockRejectedValue(new Error("boom")),
      isRunning: () => false,
      onError,
    });
    await recovery.sweepOnce();
    await vi.waitFor(() => expect(onError).toHaveBeenCalled());
  });
});
