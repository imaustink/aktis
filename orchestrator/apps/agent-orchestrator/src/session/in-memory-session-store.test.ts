import { describe, expect, it } from "vitest";
import { InMemorySessionStore } from "./in-memory-session-store.js";

function makeStore(opts: { ttlMs?: number; maxEntries?: number } = {}) {
  let time = 1_000;
  const store = new InMemorySessionStore({
    ttlMs: opts.ttlMs ?? 60_000,
    maxEntries: opts.maxEntries ?? 10,
    now: () => time,
  });
  return { store, advance: (ms: number) => (time += ms) };
}

describe("InMemorySessionStore", () => {
  it("returns undefined for unknown session ids", async () => {
    const { store } = makeStore();
    expect(await store.get("nope")).toBeUndefined();
  });

  it("round-trips a record and stamps updatedAt", async () => {
    const { store } = makeStore();
    await store.set("chat-1", { readOnly: true });
    expect(await store.get("chat-1")).toEqual({ readOnly: true, updatedAt: 1_000 });
  });

  it("expires entries idle past the TTL", async () => {
    const { store, advance } = makeStore({ ttlMs: 1_000 });
    await store.set("chat-1", { readOnly: true });
    advance(1_001);
    expect(await store.get("chat-1")).toBeUndefined();
  });

  it("slides the TTL forward on set", async () => {
    const { store, advance } = makeStore({ ttlMs: 1_000 });
    await store.set("chat-1", { readOnly: true });
    advance(900);
    await store.set("chat-1", {});
    advance(900);
    expect(await store.get("chat-1")).toEqual({ updatedAt: 1_900 });
  });

  it("evicts the least-recently-updated entry past maxEntries", async () => {
    const { store } = makeStore({ maxEntries: 2 });
    await store.set("chat-1", { readOnly: true });
    await store.set("chat-2", { readOnly: true });
    await store.set("chat-1", { readOnly: true }); // touch chat-1 -> chat-2 is now oldest
    await store.set("chat-3", { readOnly: true });
    expect(await store.get("chat-2")).toBeUndefined();
    expect(await store.get("chat-1")).toBeDefined();
    expect(await store.get("chat-3")).toBeDefined();
  });
});
