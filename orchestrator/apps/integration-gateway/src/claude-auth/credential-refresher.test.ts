import { describe, expect, it, vi } from "vitest";
import {
  ClaudeCredentialRefresher,
  ClaudeCredentialRenewer,
  CLAUDE_OAUTH_CLIENT_ID,
  CLAUDE_TOKEN_URL,
  credentialExpiresAt,
  isRefreshable,
  needsRefresh,
  refreshCredentialsBlob,
} from "./credential-refresher.js";
import type { ClaudeTokenRecord } from "./store.js";

const HOUR = 3_600_000;
const MARGIN = 30 * 60_000;

function blob(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({
    claudeAiOauth: {
      accessToken: "at-old",
      refreshToken: "rt-old",
      expiresAt: Date.now() + 8 * HOUR,
      scopes: ["user:inference"],
      subscriptionType: "max",
      ...overrides,
    },
  });
}

function okResponse(body: Record<string, unknown>): Response {
  return { ok: true, status: 200, json: async () => body } as unknown as Response;
}

function errResponse(status: number, text = ""): Response {
  return { ok: false, status, text: async () => text } as unknown as Response;
}

/** In-memory `login`-only store with the read-back behaviour the real one has. */
function fakeStore(initial?: string) {
  const records = new Map<string, ClaudeTokenRecord>();
  if (initial !== undefined) records.set("github:alice", { kind: "login", credentialsJson: initial, createdAt: "t0" });
  return {
    records,
    get: vi.fn(async (subject: string) => records.get(subject)),
    set: vi.fn(async (subject: string, record: ClaudeTokenRecord) => {
      records.set(subject, record);
    }),
  };
}

describe("needsRefresh / credentialExpiresAt", () => {
  it("leaves a comfortably valid credential alone", () => {
    expect(needsRefresh(blob({ expiresAt: Date.now() + 8 * HOUR }))).toBe(false);
  });

  it("refreshes inside the margin, and when already expired", () => {
    expect(needsRefresh(blob({ expiresAt: Date.now() + 60_000 }))).toBe(true);
    expect(needsRefresh(blob({ expiresAt: Date.now() - HOUR }))).toBe(true);
  });

  // An unknown expiry is far more likely to be an expired credential than a
  // healthy one, and a needless refresh costs a round trip while a skipped one
  // costs the user a re-link.
  it("treats a blob with no readable expiry as due", () => {
    expect(needsRefresh(JSON.stringify({ claudeAiOauth: { refreshToken: "rt" } }))).toBe(true);
  });

  it("reports expiry without exposing token material", () => {
    const at = Date.UTC(2026, 6, 28, 12, 0, 0);
    const expiry = credentialExpiresAt(blob({ expiresAt: at }));
    expect(expiry).toBe("2026-07-28T12:00:00.000Z");
    expect(expiry).not.toContain("rt-old");
  });
});

describe("refreshCredentialsBlob", () => {
  it("posts the standard refresh grant and merges the new tokens, preserving unrelated fields", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      okResponse({ access_token: "at-new", refresh_token: "rt-new", expires_in: 28800, scope: "user:inference user:profile" }),
    );
    const outcome = await refreshCredentialsBlob(blob(), { fetchImpl: fetchImpl as unknown as typeof fetch, now: () => 1_000 });

    expect(outcome.status).toBe("refreshed");
    const [url, init] = fetchImpl.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(CLAUDE_TOKEN_URL);
    const body = new URLSearchParams(String(init.body));
    expect(body.get("grant_type")).toBe("refresh_token");
    expect(body.get("refresh_token")).toBe("rt-old");
    expect(body.get("client_id")).toBe(CLAUDE_OAUTH_CLIENT_ID);

    const merged = JSON.parse(outcome.credentialsJson ?? "{}").claudeAiOauth;
    expect(merged.accessToken).toBe("at-new");
    expect(merged.refreshToken).toBe("rt-new");
    expect(merged.expiresAt).toBe(1_000 + 28800 * 1000);
    expect(merged.scopes).toEqual(["user:inference", "user:profile"]);
    // Fields this code has no business understanding must survive, or runs get
    // a blob subtly unlike the one `claude auth login` produces.
    expect(merged.subscriptionType).toBe("max");
  });

  it("carries the old refresh token forward when the response omits a new one", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(okResponse({ access_token: "at-new", expires_in: 3600 }));
    const outcome = await refreshCredentialsBlob(blob(), { fetchImpl: fetchImpl as unknown as typeof fetch });
    expect(JSON.parse(outcome.credentialsJson ?? "{}").claudeAiOauth.refreshToken).toBe("rt-old");
  });

  it("calls a spent/expired refresh token a rejection -- retrying cannot help", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(errResponse(400, '{"error":"invalid_grant"}'));
    expect((await refreshCredentialsBlob(blob(), { fetchImpl: fetchImpl as unknown as typeof fetch })).status).toBe("rejected");
  });

  it("calls anything else transient, so the stored credential is left alone and retried later", async () => {
    for (const res of [errResponse(503), errResponse(500), errResponse(400, '{"error":"server_error"}')]) {
      const fetchImpl = vi.fn().mockResolvedValue(res);
      expect((await refreshCredentialsBlob(blob(), { fetchImpl: fetchImpl as unknown as typeof fetch })).status).toBe("transient");
    }
    const throwing = vi.fn().mockRejectedValue(new Error("ECONNREFUSED"));
    expect((await refreshCredentialsBlob(blob(), { fetchImpl: throwing as unknown as typeof fetch })).status).toBe("transient");
  });

  // A 200 we cannot use must NOT be treated as a refresh: claiming success here
  // would overwrite a working credential with a broken one.
  it("treats a 200 with no usable tokens as transient, not refreshed", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(okResponse({ hello: "world" }));
    expect((await refreshCredentialsBlob(blob(), { fetchImpl: fetchImpl as unknown as typeof fetch })).status).toBe("transient");
  });

  it("does not attempt a blob with no refresh token", async () => {
    const fetchImpl = vi.fn();
    const outcome = await refreshCredentialsBlob(JSON.stringify({ claudeAiOauth: { accessToken: "a" } }), {
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    expect(outcome.status).toBe("unrefreshable");
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});


describe("isRefreshable", () => {
  it("tells a blob it can refresh from one it cannot", () => {
    expect(isRefreshable(blob())).toBe(true);
    expect(isRefreshable(JSON.stringify({ claudeAiOauth: { accessToken: "a" } }))).toBe(false);
    expect(isRefreshable(JSON.stringify({ e2e: true }))).toBe(false);
    expect(isRefreshable("not json")).toBe(false);
  });
});

/** The activity body: the one place a stored refresh token is ever spent. */
describe("ClaudeCredentialRenewer.renew", () => {
  const newTokens = () => okResponse({ access_token: "at-new", refresh_token: "rt-new", expires_in: 28800 });

  it("refreshes a near-expiry credential and persists it before returning", async () => {
    const store = fakeStore(blob({ expiresAt: Date.now() + 60_000 }));
    const fetchImpl = vi.fn().mockResolvedValue(newTokens());
    const renewer = new ClaudeCredentialRenewer({ store, fetchImpl: fetchImpl as unknown as typeof fetch, log: () => {} });

    const result = await renewer.renew("github:alice", MARGIN);

    expect(result).toMatchObject({ status: "refreshed", persisted: true });
    expect(JSON.parse(result.credentialsJson ?? "{}").claudeAiOauth.accessToken).toBe("at-new");
    // Persisted, because the old refresh token is dead the instant the service
    // answered -- this blob is now the only living copy.
    expect(store.set).toHaveBeenCalledTimes(1);
    expect(JSON.parse(store.records.get("github:alice")?.credentialsJson ?? "{}").claudeAiOauth.refreshToken).toBe("rt-new");
  });

  // The workflow may start after another refresh already rotated the token:
  // refreshing from the caller's stale copy would spend a dead token.
  it("re-reads the store, and leaves a credential someone else already refreshed alone", async () => {
    const store = fakeStore(blob({ expiresAt: Date.now() + 8 * HOUR }));
    const fetchImpl = vi.fn();
    const renewer = new ClaudeCredentialRenewer({ store, fetchImpl: fetchImpl as unknown as typeof fetch, log: () => {} });

    expect(await renewer.renew("github:alice", MARGIN)).toEqual({ status: "not-needed" });
    expect(fetchImpl).not.toHaveBeenCalled();
    expect(store.set).not.toHaveBeenCalled();
  });

  it("never overlaps two refreshes of one subject in one process", async () => {
    const store = fakeStore(blob({ expiresAt: Date.now() + 60_000 }));
    let resolveFetch: ((r: Response) => void) | undefined;
    const fetchImpl = vi.fn().mockImplementation(() => new Promise<Response>((r) => (resolveFetch = r)));
    const renewer = new ClaudeCredentialRenewer({ store, fetchImpl: fetchImpl as unknown as typeof fetch, log: () => {} });

    const both = Promise.all([renewer.renew("github:alice", MARGIN), renewer.renew("github:alice", MARGIN)]);
    await new Promise((r) => setTimeout(r, 5));
    resolveFetch?.(newTokens());
    const [a, b] = await both;

    // The second waited for the first, re-read the store, and found it fresh.
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(a.status).toBe("refreshed");
    expect(b.status).toBe("not-needed");
  });

  it("leaves the stored credential unchanged when a refresh fails", async () => {
    const store = fakeStore(blob({ expiresAt: Date.now() + 60_000 }));
    const renewer = new ClaudeCredentialRenewer({
      store,
      fetchImpl: vi.fn().mockResolvedValue(errResponse(503)) as unknown as typeof fetch,
      log: () => {},
    });

    expect(await renewer.renew("github:alice", MARGIN)).toMatchObject({ status: "transient" });
    expect(store.set).not.toHaveBeenCalled();
  });

  // Deleting here would let a refresh-endpoint hiccup that merely looked like a
  // rejection cost a working link; the orchestrator's re-auth path owns
  // invalidation.
  it("does not delete a rejected credential -- it reports and leaves it", async () => {
    const store = fakeStore(blob({ expiresAt: Date.now() - HOUR }));
    const logs: string[] = [];
    const renewer = new ClaudeCredentialRenewer({
      store,
      fetchImpl: vi.fn().mockResolvedValue(errResponse(400, '{"error":"invalid_grant"}')) as unknown as typeof fetch,
      log: (m) => logs.push(m),
    });

    expect(await renewer.renew("github:alice", MARGIN)).toMatchObject({ status: "rejected" });
    expect(store.records.has("github:alice")).toBe(true);
    expect(logs.join("\n")).toMatch(/rejected/);
  });

  // `set` swallows its own storage errors by design, so a silent drop would be
  // indistinguishable from success -- and it is the one outcome that destroys
  // the link, because the old refresh token is already spent.
  it("shouts, and still returns the live blob, when the refreshed credential will not persist", async () => {
    const store = fakeStore(blob({ expiresAt: Date.now() + 60_000 }));
    store.set = vi.fn(async () => {
      /* silently drops it, exactly as the real store can */
    });
    const logs: string[] = [];
    const renewer = new ClaudeCredentialRenewer({
      store,
      fetchImpl: vi.fn().mockResolvedValue(newTokens()) as unknown as typeof fetch,
      log: (m) => logs.push(m),
    });

    const result = await renewer.renew("github:alice", MARGIN);

    expect(result).toMatchObject({ status: "refreshed", persisted: false });
    expect(JSON.parse(result.credentialsJson ?? "{}").claudeAiOauth.accessToken).toBe("at-new");
    expect(logs.join("\n")).toMatch(/STORED-BUT-UNVERIFIED/);
    expect(logs.join("\n")).toMatch(/need re-linking/);
  });

  it("still returns the live blob when persisting throws", async () => {
    const store = fakeStore(blob({ expiresAt: Date.now() + 60_000 }));
    store.set = vi.fn(async () => {
      throw new Error("API server unreachable");
    });
    const logs: string[] = [];
    const renewer = new ClaudeCredentialRenewer({
      store,
      fetchImpl: vi.fn().mockResolvedValue(newTokens()) as unknown as typeof fetch,
      log: (m) => logs.push(m),
    });

    const result = await renewer.renew("github:alice", MARGIN);

    expect(result).toMatchObject({ status: "refreshed", persisted: false });
    expect(JSON.parse(result.credentialsJson ?? "{}").claudeAiOauth.refreshToken).toBe("rt-new");
    expect(logs.join("\n")).toMatch(/FAILED TO PERSIST/);
  });

  it("is inert when disabled, and for a subject with no credential", async () => {
    const fetchImpl = vi.fn();
    const off = new ClaudeCredentialRenewer({
      store: fakeStore(blob({ expiresAt: Date.now() - HOUR })),
      enabled: false,
      fetchImpl: fetchImpl as unknown as typeof fetch,
      log: () => {},
    });
    expect(await off.renew("github:alice", MARGIN)).toEqual({ status: "disabled" });

    const empty = new ClaudeCredentialRenewer({ store: fakeStore(), fetchImpl: fetchImpl as unknown as typeof fetch, log: () => {} });
    expect(await empty.renew("github:nobody", MARGIN)).toEqual({ status: "missing" });
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});

/**
 * The sweep is what keeps an UNUSED link alive. Refresh-on-read cannot: it only
 * fires when something reads, and a refresh token is not valid forever (the CLI
 * ships `refresh_token_expired`), so a user who links today and triggers nothing
 * for long enough would be asked to link again for no visible reason.
 */
describe("ClaudeCredentialRenewer.due (the sweep's listing)", () => {
  function sweepStore(entries: Record<string, string>) {
    const records = new Map(
      Object.entries(entries).map(([s, b]) => [s, { kind: "login" as const, credentialsJson: b, createdAt: "t0" }]),
    );
    return {
      records,
      get: vi.fn(async (subject: string) => records.get(subject)),
      set: vi.fn(),
      listSubjects: vi.fn(async () => [...records.keys()]),
    };
  }

  it("lists the credentials inside the sweep margin, and leaves healthy ones alone", async () => {
    const store = sweepStore({
      "github:stale": blob({ expiresAt: Date.now() + HOUR }),
      "github:healthy": blob({ expiresAt: Date.now() + 40 * HOUR }),
      "github:seeded": JSON.stringify({ e2e: true }),
    });
    const renewer = new ClaudeCredentialRenewer({ store, log: () => {} });

    expect(await renewer.due(4 * HOUR)).toEqual({ kind: "listed", examined: 3, due: ["github:stale"], unrefreshable: 1 });
  });

  // A list that failed is not an empty store. Reading it as one is how a sweep
  // does nothing forever while looking perfectly healthy.
  it("reports a failed listing instead of treating it as 'no links'", async () => {
    const renewer = new ClaudeCredentialRenewer({
      store: {
        get: vi.fn(),
        set: vi.fn(),
        listSubjects: vi.fn(async () => {
          throw new Error("API server unreachable");
        }),
      },
      log: () => {},
    });

    expect(await renewer.due(4 * HOUR)).toEqual({ kind: "failed", detail: "API server unreachable" });
  });

  it("stands down when the store cannot enumerate, or refresh is off", async () => {
    const cannot = new ClaudeCredentialRenewer({ store: { get: vi.fn(), set: vi.fn() }, log: () => {} });
    expect(await cannot.due(4 * HOUR)).toMatchObject({ kind: "stop", reason: expect.stringMatching(/cannot enumerate/) });

    const off = new ClaudeCredentialRenewer({ store: sweepStore({}), enabled: false, log: () => {} });
    expect(await off.due(4 * HOUR)).toMatchObject({ kind: "stop", reason: expect.stringMatching(/disabled/) });
  });
});

/** The read path: never refreshes itself, only through the serializer (the subject's Temporal workflow). */
describe("ClaudeCredentialRefresher.ensureFresh", () => {
  function serializer(impl: () => Promise<{ credentialsJson?: string }> = async () => ({})) {
    return { refresh: vi.fn(impl) };
  }

  it("serves a healthy credential without leaving the process", async () => {
    const original = blob({ expiresAt: Date.now() + 8 * HOUR });
    const s = serializer();
    const refresher = new ClaudeCredentialRefresher({ store: fakeStore(original), serializer: s, log: () => {} });

    expect(await refresher.ensureFresh("github:alice")).toBe(original);
    expect(s.refresh).not.toHaveBeenCalled();
  });

  it("hands a due credential to the serializer, then serves what the store holds", async () => {
    const store = fakeStore(blob({ expiresAt: Date.now() + 60_000 }));
    const fresh = blob({ accessToken: "at-new", expiresAt: Date.now() + 8 * HOUR });
    const s = serializer(async () => {
      store.records.set("github:alice", { kind: "login", credentialsJson: fresh, createdAt: "t1" });
      return {};
    });
    const refresher = new ClaudeCredentialRefresher({ store, serializer: s, marginMs: MARGIN, log: () => {} });

    expect(await refresher.ensureFresh("github:alice")).toBe(fresh);
    expect(s.refresh).toHaveBeenCalledWith("github:alice", MARGIN);
  });

  // The refresh rotated the token but the store would not take the result:
  // the blob the serializer hands back is the only live copy.
  it("serves the blob the serializer hands back when it is not in the store", async () => {
    const live = blob({ accessToken: "at-live" });
    const refresher = new ClaudeCredentialRefresher({
      store: fakeStore(blob({ expiresAt: Date.now() + 60_000 })),
      serializer: serializer(async () => ({ credentialsJson: live })),
      log: () => {},
    });

    expect(await refresher.ensureFresh("github:alice")).toBe(live);
  });

  it("collapses concurrent callers in one process onto ONE serialized refresh", async () => {
    let release: (() => void) | undefined;
    const s = serializer(() => new Promise((r) => (release = () => r({}))));
    const refresher = new ClaudeCredentialRefresher({ store: fakeStore(blob({ expiresAt: Date.now() + 60_000 })), serializer: s, log: () => {} });

    const both = Promise.all([refresher.ensureFresh("github:alice"), refresher.ensureFresh("github:alice")]);
    while (!release) await new Promise((r) => setTimeout(r, 1));
    release();
    const [a, b] = await both;

    expect(s.refresh).toHaveBeenCalledTimes(1);
    expect(a).toBe(b);
  });

  // An unserialized local refresh is the rotation race itself, so a refresh
  // that cannot be reached costs a renewal, never the link.
  it("serves the stored credential, loudly, when the refresh cannot be reached -- and never refreshes itself", async () => {
    const original = blob({ expiresAt: Date.now() + 60_000 });
    const store = fakeStore(original);
    const logs: string[] = [];
    const refresher = new ClaudeCredentialRefresher({
      store,
      serializer: serializer(async () => {
        throw new Error("14 UNAVAILABLE: connection refused");
      }),
      log: (m) => logs.push(m),
    });

    expect(await refresher.ensureFresh("github:alice")).toBe(original);
    expect(store.set).not.toHaveBeenCalled();
    expect(logs.join("\n")).toMatch(/REFRESH UNAVAILABLE for github:alice/);
    expect(logs.join("\n")).toMatch(/connection refused/);
  });

  it("does not ask to refresh a blob that has no refresh token", async () => {
    const seeded = JSON.stringify({ e2e: true });
    const s = serializer();
    const refresher = new ClaudeCredentialRefresher({ store: fakeStore(seeded), serializer: s, log: () => {} });

    expect(await refresher.ensureFresh("github:alice")).toBe(seeded);
    expect(s.refresh).not.toHaveBeenCalled();
  });

  it("is inert when disabled, and for a subject with no credential", async () => {
    const original = blob({ expiresAt: Date.now() - HOUR });
    const s = serializer();
    const off = new ClaudeCredentialRefresher({ store: fakeStore(original), serializer: s, enabled: false, log: () => {} });
    expect(await off.ensureFresh("github:alice")).toBe(original);

    const empty = new ClaudeCredentialRefresher({ store: fakeStore(), serializer: s, log: () => {} });
    expect(await empty.ensureFresh("github:nobody")).toBeUndefined();
    expect(s.refresh).not.toHaveBeenCalled();
  });
});
