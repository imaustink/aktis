import { randomBytes } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { testRuns, type TestRuns } from "../__fixtures__/temporal.js";
import type { RunActivities } from "../runs/contract.js";
import { workflowIds } from "../runs/model.js";
import { ClaudeCredentialRefresher, ClaudeCredentialRenewer } from "./credential-refresher.js";
import { createClaudeCredentialActivities, TemporalClaudeCredentials } from "./temporal-refresh.js";
import type { ClaudeCredentialRefreshResult } from "./refresh-contract.js";
import type { ClaudeTokenRecord } from "./store.js";

/**
 * The Claude credential workflows against a real Temporal: the refresh is
 * serialized across replicas, not just within one process.
 */

const HOUR = 3_600_000;
const MARGIN = 30 * 60_000;
const KEY = randomBytes(32);

function blob(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({ claudeAiOauth: { accessToken: "at-old", refreshToken: "rt-old", expiresAt: Date.now() + 8 * HOUR, ...overrides } });
}

function newTokens(): Response {
  return { ok: true, status: 200, json: async () => ({ access_token: "at-new", refresh_token: "rt-new", expires_in: 28800 }) } as unknown as Response;
}

function store(entries: Record<string, string>) {
  const records = new Map<string, ClaudeTokenRecord>(
    Object.entries(entries).map(([s, b]) => [s, { kind: "login", credentialsJson: b, createdAt: "t0" }]),
  );
  return {
    records,
    get: vi.fn(async (subject: string) => records.get(subject)),
    set: vi.fn(async (subject: string, record: ClaudeTokenRecord) => {
      records.set(subject, record);
    }),
    listSubjects: vi.fn(async () => [...records.keys()]),
  };
}

async function serve(
  t: TestRuns,
  s: ReturnType<typeof store> | undefined,
  fetchImpl: ReturnType<typeof vi.fn>,
  logs: string[] = [],
  sweep: { sweepIntervalMs?: number; sweepMarginMs?: number } = {},
): Promise<void> {
  await t.serve({
    log: async (line: string) => {
      logs.push(line);
    },
    ...createClaudeCredentialActivities({
      ...(s ? { renewer: new ClaudeCredentialRenewer({ store: s, fetchImpl: fetchImpl as unknown as typeof fetch, log: (l) => logs.push(l) }), key: KEY } : {}),
      ...sweep,
    }),
  } as unknown as RunActivities);
}

/** One replica's read path: its own refresher (its own in-memory dedupe) over the shared Temporal. */
function replica(t: TestRuns, s: ReturnType<typeof store>, logs: string[] = [], waitMs?: number): ClaudeCredentialRefresher {
  return new ClaudeCredentialRefresher({
    store: s,
    serializer: new TemporalClaudeCredentials({ client: t.client, taskQueue: t.taskQueue, key: KEY, ...(waitMs ? { waitMs } : {}) }),
    marginMs: MARGIN,
    log: (l) => logs.push(l),
  });
}

async function until(check: () => boolean | Promise<boolean>, what: string): Promise<void> {
  const deadline = Date.now() + 20_000;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

describe("Claude credential refresh on Temporal", () => {
  // The bug this exists to fix: two replicas each refreshing from the same
  // stored refresh token, the second invalidating the first.
  it("two replicas refreshing one subject at once spend ONE refresh token", async () => {
    const t = await testRuns();
    const s = store({ "github:alice": blob({ expiresAt: Date.now() + 60_000 }) });
    let release: (() => void) | undefined;
    const fetchImpl = vi.fn(() => new Promise<Response>((r) => (release = () => r(newTokens()))));
    await serve(t, s, fetchImpl);

    const a = replica(t, s).ensureFresh("github:alice");
    await until(() => release !== undefined, "the first refresh to reach the token endpoint");
    const b = replica(t, s).ensureFresh("github:alice");
    // Let the second replica attach to the running workflow before it finishes.
    await new Promise((r) => setTimeout(r, 300));
    release?.();

    const [fromA, fromB] = await Promise.all([a, b]);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(JSON.parse(fromA ?? "{}").claudeAiOauth.refreshToken).toBe("rt-new");
    expect(fromB).toBe(fromA);
    expect(JSON.parse(s.records.get("github:alice")?.credentialsJson ?? "{}").claudeAiOauth.refreshToken).toBe("rt-new");
    await t.stop();
  });

  it("starts a fresh run for a later refresh, which finds the credential already fresh", async () => {
    const t = await testRuns();
    const s = store({ "github:alice": blob({ expiresAt: Date.now() + 60_000 }) });
    const fetchImpl = vi.fn(async () => newTokens());
    await serve(t, s, fetchImpl);
    const credentials = new TemporalClaudeCredentials({ client: t.client, taskQueue: t.taskQueue, key: KEY });

    await credentials.refresh("github:alice", MARGIN);
    await credentials.refresh("github:alice", MARGIN);

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    const result = (await t.client.workflow.getHandle(workflowIds.claudeCredentialRefresh(t.taskQueue, "github:alice")).result()) as ClaudeCredentialRefreshResult;
    expect(result.status).toBe("not-needed");
    await t.stop();
  });

  // Anthropic rotated the token but the store would not take the result: the
  // blob is the only live copy, and the caller may be on another replica.
  it("hands an unpersisted refresh to the caller, sealed -- never in the clear in history", async () => {
    const t = await testRuns();
    const s = store({ "github:alice": blob({ expiresAt: Date.now() + 60_000 }) });
    s.set = vi.fn(async () => {
      /* silently drops it, as the real store can */
    });
    await serve(t, s, vi.fn(async () => newTokens()));

    const served = await replica(t, s).ensureFresh("github:alice");

    expect(JSON.parse(served ?? "{}").claudeAiOauth.refreshToken).toBe("rt-new");
    const result = (await t.client.workflow.getHandle(workflowIds.claudeCredentialRefresh(t.taskQueue, "github:alice")).result()) as ClaudeCredentialRefreshResult;
    expect(result.status).toBe("refreshed");
    expect(result.sealedCredentialsJson).toBeTruthy();
    expect(JSON.stringify(result)).not.toContain("rt-new");
    expect(JSON.stringify(result)).not.toContain("at-new");
    await t.stop();
  });

  it("does not put a persisted refresh's tokens in history at all", async () => {
    const t = await testRuns();
    const s = store({ "github:alice": blob({ expiresAt: Date.now() + 60_000 }) });
    await serve(t, s, vi.fn(async () => newTokens()));

    await replica(t, s).ensureFresh("github:alice");

    const result = (await t.client.workflow.getHandle(workflowIds.claudeCredentialRefresh(t.taskQueue, "github:alice")).result()) as ClaudeCredentialRefreshResult;
    expect(result).toMatchObject({ status: "refreshed" });
    expect(result.sealedCredentialsJson).toBeUndefined();
    await t.stop();
  });

  // Never worse off than before, and never an unserialized local refresh.
  it("serves the stored credential when no refresh answers in time", async () => {
    const t = await testRuns(); // no worker: the workflow is never run
    const original = blob({ expiresAt: Date.now() + 60_000 });
    const s = store({ "github:alice": original });
    const logs: string[] = [];

    expect(await replica(t, s, logs, 300).ensureFresh("github:alice")).toBe(original);
    expect(s.set).not.toHaveBeenCalled();
    expect(logs.join("\n")).toMatch(/REFRESH UNAVAILABLE for github:alice/);
    await t.client.workflow.getHandle(workflowIds.claudeCredentialRefresh(t.taskQueue, "github:alice")).terminate();
  });

  it("encodes the subject into the workflow id", () => {
    expect(workflowIds.claudeCredentialRefresh("q", "github:a/b c")).toBe("q/claude-credential-refresh:github%3Aa%2Fb%20c");
  });
});

describe("Claude credential sweep on Temporal", () => {
  it("renews the credentials inside the sweep margin, one workflow for every replica", async () => {
    const t = await testRuns();
    const s = store({
      "github:stale": blob({ expiresAt: Date.now() + HOUR }),
      "github:healthy": blob({ expiresAt: Date.now() + 40 * HOUR }),
    });
    const fetchImpl = vi.fn(async () => newTokens());
    const logs: string[] = [];
    await serve(t, s, fetchImpl, logs, { sweepIntervalMs: 60_000, sweepMarginMs: 4 * HOUR });
    const replicas = [1, 2].map(() => new TemporalClaudeCredentials({ client: t.client, taskQueue: t.taskQueue, key: KEY }));

    await Promise.all(replicas.map((r) => r.ensureSweep()));
    await until(() => logs.some((l) => l.includes("sweep:")), "a sweep");

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(JSON.parse(s.records.get("github:stale")?.credentialsJson ?? "{}").claudeAiOauth.refreshToken).toBe("rt-new");
    expect(JSON.parse(s.records.get("github:healthy")?.credentialsJson ?? "{}").claudeAiOauth.refreshToken).toBe("rt-old");
    expect(logs.find((l) => l.includes("sweep:"))).toMatch(/2 examined, 1 refreshed, 0 already refreshing, 0 still due/);
    const running = [];
    for await (const w of t.client.workflow.list({ query: `WorkflowType = 'claudeCredentialSweep' AND ExecutionStatus = 'Running' AND TaskQueue = '${t.taskQueue}'` })) running.push(w);
    expect(running.length).toBeLessThanOrEqual(1);
    await t.client.workflow.getHandle(workflowIds.claudeCredentialSweep(t.taskQueue)).terminate();
    await t.stop();
  });

  it("stands down, out loud, where it may not sweep", async () => {
    const t = await testRuns();
    const logs: string[] = [];
    await serve(t, undefined, vi.fn(), logs);

    await new TemporalClaudeCredentials({ client: t.client, taskQueue: t.taskQueue, key: KEY }).ensureSweep();
    await t.client.workflow.getHandle(workflowIds.claudeCredentialSweep(t.taskQueue)).result();

    expect(logs.join("\n")).toMatch(/sweep disabled: Claude auth is not configured/);
    await t.stop();
  });
});
