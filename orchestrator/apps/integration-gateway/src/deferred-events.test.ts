import { describe, expect, it } from "vitest";
import { InMemoryDeferredEventStore, type HeldRunEvent } from "./deferred-events.js";

const held = (id: string): HeldRunEvent => ({
  id,
  createdAt: 1,
  envelope: { source: "runs", kind: "run", id: "e", time: "t", headers: {}, sender: {}, repository: "a/b", body: {} },
  trigger: { name: "t", sessionId: "s", reply: null },
  finished: { status: "succeeded" },
});

describe("InMemoryDeferredEventStore (docs/adr/0055)", () => {
  it("claim removes an event exactly once", async () => {
    const store = new InMemoryDeferredEventStore();
    await store.save(held("x"));
    expect(await store.list()).toHaveLength(1);
    expect(await store.claim("x")).toBe(true);
    expect(await store.claim("x")).toBe(false);
    expect(await store.list()).toHaveLength(0);
  });

  it("a queue has one holder: renewable by it, released only by it, and freed by expiry", async () => {
    let now = 0;
    const store = new InMemoryDeferredEventStore(() => now);
    expect(await store.acquireQueue("q", "a", 100)).toBe(true);
    expect(await store.acquireQueue("q", "b", 100)).toBe(false);
    expect(await store.acquireQueue("q", "a", 100)).toBe(true);
    await store.releaseQueue("q", "b");
    expect(await store.acquireQueue("q", "b", 100)).toBe(false);
    await store.releaseQueue("q", "a");
    expect(await store.acquireQueue("q", "b", 100)).toBe(true);
    now = 101;
    expect(await store.acquireQueue("q", "c", 100)).toBe(true);
  });

  it("markOnce sets once until unmarked or expired", async () => {
    let now = 0;
    const store = new InMemoryDeferredEventStore(() => now);
    expect(await store.markOnce("k", 100)).toBe(true);
    expect(await store.markOnce("k", 100)).toBe(false);
    await store.unmark("k");
    expect(await store.markOnce("k", 100)).toBe(true);
    now = 200;
    expect(await store.markOnce("k", 100)).toBe(true);
  });
});
