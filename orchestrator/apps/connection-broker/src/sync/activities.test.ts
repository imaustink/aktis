import { ApplicationFailure } from "@temporalio/common";
import { describe, expect, it, vi } from "vitest";
import type { CorpusBinding } from "../registry.js";
import { createSyncActivities, type SyncTarget } from "./activities.js";
import { FAILED_IDS_KEPT } from "./model.js";
import type { CorpusWriter, ResourceSource } from "./worker.js";

const binding = (name: string): CorpusBinding =>
  ({ name, allowedRoles: ["reader"], serviceToken: "t", scope: { space: "S" } }) as unknown as CorpusBinding;

const target = (name: string): SyncTarget => ({ binding: binding(name), collection: `coll-${name}`, intervalMs: 60_000 });

function deps(overrides: { list?: ResourceSource["list"]; fetch?: ResourceSource["fetch"] } = {}) {
  const source: ResourceSource = {
    list: overrides.list ?? (async () => ({ resources: [], cursor: undefined })),
    fetch: overrides.fetch ?? (async () => ({ id: "1", title: "t", url: "u", markdown: "m" })),
  };
  const writer: CorpusWriter = {
    indexed: async () => [],
    upsert: async () => {},
    remove: async () => {},
  };
  return { source, writer };
}

describe("runPass", () => {
  it("syncs the named corpus and reports it", async () => {
    const { source, writer } = deps();
    const onReport = vi.fn();
    const activities = createSyncActivities({
      sourceFor: () => source,
      writerFor: () => writer,
      target: (c) => (c === "a" ? target("a") : undefined),
      onReport,
    });

    const result = await activities.runPass("a");
    expect(result).toMatchObject({ full: true, indexed: 0, removed: 0 });
    // The status write-back: without it every syncing Corpus reports stale forever.
    expect(onReport).toHaveBeenCalledWith("a", expect.objectContaining({ connection: "a", full: true }));
  });

  it("builds a writer PER CORPUS", async () => {
    const { source, writer } = deps();
    const writerFor = vi.fn((_b: CorpusBinding) => writer);
    const activities = createSyncActivities({ sourceFor: () => source, writerFor, target });

    await activities.runPass("a");
    await activities.runPass("b");

    // Each point carries its own corpus's allowedRoles. One shared writer
    // would stamp one client's roles onto another client's chunks.
    expect(writerFor.mock.calls.map(([b]) => b.name)).toEqual(["a", "b"]);
  });

  it("builds a source PER CORPUS, so each pass carries its own token", async () => {
    const { writer } = deps();
    const tokens: Record<string, string> = { a: "token-a", b: "token-b" };
    const used: { connection: string; token: string }[] = [];
    const sourceFor = vi.fn((b: CorpusBinding) => {
      const token = tokens[b.name]!;
      return {
        list: async (connection: string) => {
          used.push({ connection, token });
          return { resources: [], cursor: undefined };
        },
        fetch: async () => ({ id: "1", title: "t", url: "u", markdown: "m" }),
      } satisfies ResourceSource;
    });
    const activities = createSyncActivities({ sourceFor, writerFor: () => writer, target });

    await activities.runPass("a");
    await activities.runPass("b");

    // The broker scopes each sync token to one corpus, so a shared source
    // carrying one token would 403 on every other corpus's list and never
    // index it.
    expect(used).toEqual([
      { connection: "a", token: "token-a" },
      { connection: "b", token: "token-b" },
    ]);
  });

  it("runs a partial pass for exactly the named resources", async () => {
    const fetched: string[] = [];
    const { writer } = deps();
    const source: ResourceSource = {
      list: async () => {
        throw new Error("a partial pass must not list");
      },
      fetch: async (_c, id) => {
        fetched.push(id);
        return { id, title: id, url: "u", markdown: "m" };
      },
    };
    const activities = createSyncActivities({ sourceFor: () => source, writerFor: () => writer, target });

    const result = await activities.runPass("a", ["p1", "p2"]);
    expect(fetched).toEqual(["p1", "p2"]);
    expect(result?.full).toBe(false);
  });

  it("lets a failing pass throw, so Temporal retries it", async () => {
    const { writer } = deps();
    const activities = createSyncActivities({
      sourceFor: () => ({
        list: async () => {
          throw new Error("broker down");
        },
        fetch: async () => ({ id: "1", title: "t", url: "u", markdown: "m" }),
      }),
      writerFor: () => writer,
      target,
    });

    await expect(activities.runPass("a")).rejects.toThrow("broker down");
  });

  it("keeps only the first few failed ids, so a broken source cannot bloat the history", async () => {
    const { writer } = deps();
    const ids = Array.from({ length: FAILED_IDS_KEPT + 5 }, (_, i) => ({ id: `r${i}` }));
    const activities = createSyncActivities({
      sourceFor: () => ({
        list: async () => ({ resources: ids as never, cursor: undefined }),
        fetch: async () => {
          throw new Error("gone");
        },
      }),
      writerFor: () => writer,
      target,
    });

    const result = await activities.runPass("a");
    expect(result?.failed).toHaveLength(FAILED_IDS_KEPT);
    expect(result?.failedCount).toBe(FAILED_IDS_KEPT + 5);
    // A pass that could not read some resources is no longer full.
    expect(result?.full).toBe(false);
  });

  it("answers null for a corpus it does not sync, which ends the corpus's loop", async () => {
    const { source, writer } = deps();
    const activities = createSyncActivities({ sourceFor: () => source, writerFor: () => writer, target: () => undefined });
    expect(await activities.runPass("gone")).toBeNull();
    expect(await activities.syncTarget("gone")).toBeNull();
  });

  it("answers null only when the cluster agrees the corpus is gone", async () => {
    const { source, writer } = deps();
    const activities = createSyncActivities({
      sourceFor: () => source,
      writerFor: () => writer,
      target: () => undefined,
      confirm: async () => "gone",
    });
    expect(await activities.runPass("gone")).toBeNull();
    expect(await activities.syncTarget("gone")).toBeNull();
  });

  // One lagging replica (watch behind, or no token yet mid-rollout) must not
  // end the loop for every replica: it fails retryably instead.
  it("fails retryably when this replica cannot see a corpus the cluster still syncs", async () => {
    const { source, writer } = deps();
    const activities = createSyncActivities({
      sourceFor: () => source,
      writerFor: () => writer,
      target: () => undefined,
      confirm: async () => "syncable",
    });
    for (const call of [() => activities.runPass("lagging"), () => activities.syncTarget("lagging")]) {
      const error = await call().then(
        () => undefined,
        (e: unknown) => e as ApplicationFailure,
      );
      expect(error).toBeInstanceOf(ApplicationFailure);
      expect(error?.type).toBe("CorpusNotVisibleHere");
      expect(error?.nonRetryable).toBe(false);
    }
  });
});

describe("syncTarget", () => {
  it("reads the interval fresh, so an edited Corpus applies without a restart", async () => {
    const { source, writer } = deps();
    let intervalMs = 1_000;
    const activities = createSyncActivities({
      sourceFor: () => source,
      writerFor: () => writer,
      target: (c) => ({ ...target(c), intervalMs }),
    });
    expect(await activities.syncTarget("a")).toEqual({ intervalMs: 1_000 });
    intervalMs = 5_000;
    expect(await activities.syncTarget("a")).toEqual({ intervalMs: 5_000 });
  });

  it("passes on when the Corpus's status says it last fully synced", async () => {
    const { source, writer } = deps();
    const activities = createSyncActivities({
      sourceFor: () => source,
      writerFor: () => writer,
      target: (c) => ({ ...target(c), lastReconcileAt: 1_700_000_000_000 }),
    });
    expect(await activities.syncTarget("a")).toEqual({ intervalMs: 60_000, lastReconcileAt: 1_700_000_000_000 });
  });
});
