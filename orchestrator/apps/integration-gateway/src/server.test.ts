import { createHmac } from "node:crypto";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { parseBody, type Envelope, type EventSourceResource } from "@aktis/event-triggers";
import type { ClaudeTokenStore } from "./claude-auth/store.js";
import { StaticEventSourceRegistry, type SecretLookup } from "./event-sources.js";
import { StaticEventTriggerTable } from "./event-triggers.js";
import type { GithubReplyClient } from "./github-client.js";
import { GithubIdentityResolver } from "./identity.js";
import { InMemoryPendingLabelStore } from "./label-reconciler.js";
import { InMemoryInflightRunStore, type InflightRun } from "./inflight-runs.js";
import { InMemoryDeferredEventStore } from "./deferred-events.js";
import type { AcceptedTrigger, EventSubmitResult, OrchestratorClient, OrchestratorInvokeResult, PollHooks } from "./orchestrator-client.js";
import { GatewayServer, type GatewayServerOptions } from "./server.js";
import { InMemorySessionPageStore } from "./session-page-store.js";
import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { setupTelemetry } from "./telemetry.js";

// Wrapped so a test can prove verification rejected a request BEFORE anything
// parsed it -- the property the whole handler order exists for.
vi.mock("@aktis/event-triggers", async (importOriginal) => {
  const mod = await importOriginal<typeof import("@aktis/event-triggers")>();
  return { ...mod, parseBody: vi.fn(mod.parseBody) };
});

/**
 * Every request in this file goes over a fresh TCP connection — see the long
 * explanation on the identical shim in agent-orchestrator's `server.test.ts`.
 * Short version: these tests listen on ephemeral ports and close their servers,
 * and Node's `fetch` caches keep-alive sockets per origin, so a recycled port
 * can hand a later test a dead socket ("other side closed", zero bytes read).
 * `connection: close` keeps anything from being pooled in the first place.
 */
const nativeFetch = globalThis.fetch;
const fetch: typeof globalThis.fetch = (input, init = {}) => {
  const headers = new Headers(init.headers);
  headers.set("connection", "close");
  return nativeFetch(input, { ...init, headers });
};

const SECRETS: Record<string, string> = {
  "gh/secret": "gh-secret",
  "slack/secret": "slack-secret",
  "ci/token": "ci-token",
  "callback/secret": "callback-secret",
};

/** Tokens the fake authenticator recognises, and who each one is. */
const NIGHTLY_TOKEN = "aaa.nightly.sig";
const OTHER_TOKEN = "aaa.other.sig";
const TICK_IDENTITIES: Record<string, string> = {
  [NIGHTLY_TOKEN]: "system:serviceaccount:agents:eventsource-nightly",
  [OTHER_TOKEN]: "system:serviceaccount:agents:eventsource-other",
  "aaa.wrongns.sig": "system:serviceaccount:elsewhere:eventsource-nightly",
  "aaa.github.sig": "system:serviceaccount:agents:eventsource-github",
  "aaa.runs.sig": "system:serviceaccount:agents:eventsource-runs",
};

const SOURCES: EventSourceResource[] = [
  {
    metadata: { name: "github" },
    spec: {
      webhook: {
        verify: { scheme: "hmacSha256", header: "x-hub-signature-256", prefix: "sha256=", secretRef: { name: "gh", key: "secret" } },
        deliveryIdHeader: "x-github-delivery",
      },
      sender: {
        provider: "github",
        loginFrom: "body.sender.login",
        isBotFrom: 'body.sender.type == "Bot"',
        repositoryFrom: "body.repository.full_name",
      },
    },
  },
  {
    // docs/adr/0049 §5: a source that also extracts the immutable account id.
    metadata: { name: "github-ids" },
    spec: {
      webhook: {
        verify: { scheme: "hmacSha256", header: "x-hub-signature-256", prefix: "sha256=", secretRef: { name: "gh", key: "secret" } },
        deliveryIdHeader: "x-github-delivery",
      },
      sender: {
        provider: "github",
        loginFrom: "body.sender.login",
        isBotFrom: 'body.sender.type == "Bot"',
        repositoryFrom: "body.repository.full_name",
        accountIdFrom: "body.sender.id",
      },
    },
  },
  {
    metadata: { name: "plain" },
    spec: { webhook: { verify: { scheme: "hmacSha256", header: "x-sig", secretRef: { name: "gh", key: "secret" } } } },
  },
  {
    metadata: { name: "nokey" },
    spec: { webhook: { verify: { scheme: "hmacSha256", header: "x-sig", secretRef: { name: "absent", key: "secret" } } } },
  },
  { metadata: { name: "slack" }, spec: { webhook: { verify: { scheme: "slackV0", secretRef: { name: "slack", key: "secret" } } } } },
  { metadata: { name: "ci" }, spec: { webhook: { verify: { scheme: "bearer", secretRef: { name: "ci", key: "token" } } } } },
  {
    metadata: { name: "ci-custom" },
    spec: { webhook: { verify: { scheme: "bearer", header: "x-ci-token", secretRef: { name: "ci", key: "token" } } } },
  },
  { metadata: { name: "nightly" }, spec: { cron: { schedule: "0 7 * * *", payload: { repo: "acme/widgets", n: 3 } } } },
  // docs/adr/0052 §1: receives the gateway's own run-completion events.
  { metadata: { name: "runs" }, spec: { run: {} } },
  { metadata: { name: "pr-conflicts" }, spec: { conflicts: { author: "app/k5s-bot", skipLabels: ["ai-triage"] } } },
];

const DEFAULT_TRIGGER: AcceptedTrigger = {
  name: "github-review",
  sessionId: "github:acme/widgets#7",
  reply: {
    github: {
      repository: "acme/widgets",
      issueNumber: 7,
      ack: "Starting a review…",
      sessionPage: true,
      result: true,
      removeLabel: "ai-review",
    },
    http: { url: "http://ci-bridge.tools.svc/results", secretRef: { name: "callback", key: "secret" } },
  },
};

interface Harness {
  server: GatewayServer;
  port: number;
  order: string[];
  logs: string[];
  submitEvent: ReturnType<typeof vi.fn>;
  poll: ReturnType<typeof vi.fn>;
  invoke: ReturnType<typeof vi.fn>;
  postIssueComment: ReturnType<typeof vi.fn>;
  updateIssueComment: ReturnType<typeof vi.fn>;
  removeIssueLabel: ReturnType<typeof vi.fn>;
  addIssueLabels: ReturnType<typeof vi.fn>;
  addIssueAssignees: ReturnType<typeof vi.fn>;
  getIssueState: ReturnType<typeof vi.fn>;
  searchOpenPullRequests: ReturnType<typeof vi.fn>;
  getPullRequest: ReturnType<typeof vi.fn>;
  getAssignmentActors: ReturnType<typeof vi.fn>;
  replyFetch: ReturnType<typeof vi.fn>;
  secretsGet: ReturnType<typeof vi.fn>;
  resolve: ReturnType<typeof vi.spyOn>;
  pendingLabelStore: InMemoryPendingLabelStore;
  record: ReturnType<typeof vi.spyOn>;
  clear: ReturnType<typeof vi.spyOn>;
  sessionPageStore: InMemorySessionPageStore;
  onBackgroundError: ReturnType<typeof vi.fn>;
}

let current: Harness | undefined;

afterEach(async () => {
  await current?.server.close();
  current = undefined;
  vi.mocked(parseBody).mockClear();
});

async function start(overrides: Partial<GatewayServerOptions> = {}, secrets: Record<string, string> = SECRETS): Promise<Harness> {
  const order: string[] = [];
  const logs: string[] = [];
  const identityResolver = new GithubIdentityResolver(new Map([["alice", { subject: "alice", roles: ["reporter"] }]]), "agent-controller[bot]");
  const resolve = vi.spyOn(identityResolver, "resolve");
  const submitEvent = vi.fn(async (): Promise<EventSubmitResult> => ({ kind: "accepted", id: "inv-1", trigger: DEFAULT_TRIGGER }));
  const poll = vi.fn(async (_id: string, _hooks?: PollHooks): Promise<OrchestratorInvokeResult> => {
    order.push("poll");
    return { status: "succeeded", result: "Looks good." };
  });
  const invoke = vi.fn(async (): Promise<OrchestratorInvokeResult> => ({ status: "succeeded", result: "Follow-up done." }));
  let nextCommentId = 1000;
  const postIssueComment = vi.fn(async (_o: string, _r: string, _n: number, body: string): Promise<number | undefined> => {
    order.push(`comment:${body.split("\n")[0]}`);
    return nextCommentId++;
  });
  const updateIssueComment = vi.fn(async (_o: string, _r: string, id: number) => {
    order.push(`edit:${id}`);
  });
  const removeIssueLabel = vi.fn(async () => {
    order.push("removeLabel");
  });
  const addIssueLabels = vi.fn(async (_o: string, _r: string, _n: number, labels: string[]) => {
    order.push(`addLabels:${labels.join(",")}`);
  });
  const addIssueAssignees = vi.fn(async (_o: string, _r: string, _n: number, logins: string[]) => {
    order.push(`assign:${logins.join(",")}`);
  });
  const getIssueState = vi.fn(async (): Promise<"open" | "closed" | "missing"> => "open");
  const searchOpenPullRequests = vi.fn(async (): Promise<Array<{ owner: string; repo: string; number: number }>> => []);
  const getPullRequest = vi.fn(async (): Promise<unknown> => undefined);
  const getAssignmentActors = vi.fn(async (): Promise<Map<string, string>> => new Map());
  const replyFetch = vi.fn(async () => {
    order.push("http");
    return new Response(null, { status: 204 });
  });
  const secretsGet = vi.fn(async (name: string, key: string) => secrets[`${name}/${key}`]);
  const pendingLabelStore = new InMemoryPendingLabelStore();
  const record = vi.spyOn(pendingLabelStore, "record").mockImplementation(async function (this: InMemoryPendingLabelStore, entry) {
    order.push("record");
    return InMemoryPendingLabelStore.prototype.record.call(pendingLabelStore, entry);
  });
  const clear = vi.spyOn(pendingLabelStore, "clear");
  const sessionPageStore = new InMemorySessionPageStore();
  const onBackgroundError = vi.fn();

  const server = new GatewayServer({
    eventSources: new StaticEventSourceRegistry(SOURCES, () => {}),
    secrets: { get: secretsGet } as SecretLookup,
    tickAuthenticator: { authenticate: async (token: string) => TICK_IDENTITIES[token] },
    tickNamespace: "agents",
    identityResolver,
    orchestratorClient: { submitEvent, poll, invoke, checkLive: vi.fn().mockResolvedValue({ live: false }) } as unknown as OrchestratorClient,
    githubReplyClient: {
      postIssueComment,
      updateIssueComment,
      removeIssueLabel,
      addIssueLabels,
      addIssueAssignees,
      getIssueState,
      searchOpenPullRequests,
      getPullRequest,
      getAssignmentActors,
    } as unknown as GithubReplyClient,
    replyFetch: replyFetch as unknown as typeof fetch,
    log: (line) => logs.push(line),
    onBackgroundError,
    pendingLabelStore,
    // The reconciler's startup sweep must not race these tests' own removals.
    labelReconcilerGraceMs: 60 * 60 * 1000,
    sessionPageStore,
    publicBaseUrl: "https://gateway.example.com",
    ...overrides,
  });
  await server.listen(0);
  const port = (server as unknown as { server: { address: () => AddressInfo } }).server.address().port;
  current = {
    server,
    port,
    order,
    logs,
    submitEvent,
    poll,
    invoke,
    postIssueComment,
    updateIssueComment,
    removeIssueLabel,
    addIssueLabels,
    addIssueAssignees,
    getIssueState,
    searchOpenPullRequests,
    getPullRequest,
    getAssignmentActors,
    replyFetch,
    secretsGet,
    resolve,
    pendingLabelStore,
    record,
    clear,
    sessionPageStore,
    onBackgroundError,
  };
  return current;
}

const hmacHex = (secret: string, data: string): string => createHmac("sha256", secret).update(data).digest("hex");

const GITHUB_PAYLOAD = {
  action: "labeled",
  label: { name: "ai-review" },
  repository: { full_name: "acme/widgets" },
  sender: { login: "alice", type: "User" },
  pull_request: { number: 7 },
};

function postGithub(
  h: Harness,
  payload: unknown = GITHUB_PAYLOAD,
  opts: { signature?: string | null; extraHeaders?: Record<string, string> } = {},
): Promise<Response> {
  const body = JSON.stringify(payload);
  const headers: Record<string, string> = {
    "content-type": "application/json",
    "x-github-event": "pull_request",
    "x-github-delivery": "delivery-123",
    ...opts.extraHeaders,
  };
  if (opts.signature !== null) headers["x-hub-signature-256"] = opts.signature ?? `sha256=${hmacHex("gh-secret", body)}`;
  return fetch(`http://localhost:${h.port}/webhooks/github`, { method: "POST", headers, body });
}

/** Lets the background relay run; used where the assertion is that something did NOT happen. */
function settle(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 30));
}

function submittedEnvelope(h: Harness, call = 0): Envelope {
  return h.submitEvent.mock.calls[call]?.[0] as Envelope;
}

describe("POST /webhooks/:source routing", () => {
  it("responds to healthz", async () => {
    const h = await start();
    expect((await fetch(`http://localhost:${h.port}/healthz`)).status).toBe(200);
  });

  it("404s an unknown source and a cron source, without reading a secret", async () => {
    const h = await start();
    for (const name of ["nope", "nightly", "NOT_A_K8S_NAME"]) {
      const res = await fetch(`http://localhost:${h.port}/webhooks/${name}`, { method: "POST", body: "{}" });
      expect(res.status).toBe(404);
    }
    expect(h.secretsGet).not.toHaveBeenCalled();
    expect(h.submitEvent).not.toHaveBeenCalled();
  });

  it("404s everything when no EventSources are configured", async () => {
    const h = await start({ eventSources: new StaticEventSourceRegistry([]) });
    expect((await postGithub(h)).status).toBe(404);
  });
});

describe("webhook verification (hmacSha256)", () => {
  it("401s a missing signature: nothing parsed, nothing resolved, nothing relayed", async () => {
    const h = await start();
    expect((await postGithub(h, GITHUB_PAYLOAD, { signature: null })).status).toBe(401);
    await settle();
    expect(parseBody).not.toHaveBeenCalled();
    expect(h.resolve).not.toHaveBeenCalled();
    expect(h.submitEvent).not.toHaveBeenCalled();
    expect(h.logs.join("\n")).toMatch(/webhook github: rejected, verification failed \(missing x-hub-signature-256\)/);
  });

  it("401s a bad signature: nothing parsed, nothing relayed", async () => {
    const h = await start();
    const res = await postGithub(h, GITHUB_PAYLOAD, { signature: `sha256=${hmacHex("gh-secret", "tampered")}` });
    expect(res.status).toBe(401);
    await settle();
    expect(parseBody).not.toHaveBeenCalled();
    expect(h.submitEvent).not.toHaveBeenCalled();
  });

  it("401s (fails closed) when the referenced Secret/key is missing", async () => {
    const h = await start();
    const body = "{}";
    const res = await fetch(`http://localhost:${h.port}/webhooks/nokey`, {
      method: "POST",
      headers: { "x-sig": hmacHex("", body) },
      body,
    });
    expect(res.status).toBe(401);
    await settle();
    expect(h.submitEvent).not.toHaveBeenCalled();
  });

  it("accepts a just-rotated secret by re-reading once on mismatch", async () => {
    const h = await start();
    h.secretsGet.mockImplementation(async (name: string, key: string, opts?: { refresh?: boolean }) =>
      name === "gh" && key === "secret" ? (opts?.refresh ? "rotated" : "gh-secret") : undefined,
    );
    const body = JSON.stringify(GITHUB_PAYLOAD);
    const res = await postGithub(h, GITHUB_PAYLOAD, { signature: `sha256=${hmacHex("rotated", body)}` });
    expect(res.status).toBe(202);
  });

  it("accepts a valid signature with 202 and builds the envelope without the verify/authorization/cookie headers", async () => {
    const h = await start();
    const res = await postGithub(h, GITHUB_PAYLOAD, {
      extraHeaders: { authorization: "Bearer leak", cookie: "session=leak", "X-Custom": "kept" },
    });
    expect(res.status).toBe(202);
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalled());
    const envelope = submittedEnvelope(h);
    expect(envelope).toMatchObject({ source: "github", kind: "webhook", id: "delivery-123", body: GITHUB_PAYLOAD });
    expect(envelope.time).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/);
    expect(envelope.headers["x-github-event"]).toBe("pull_request");
    expect(envelope.headers["x-custom"]).toBe("kept");
    expect(envelope.headers).not.toHaveProperty("x-hub-signature-256");
    expect(envelope.headers).not.toHaveProperty("authorization");
    expect(envelope.headers).not.toHaveProperty("cookie");
  });
});

describe("webhook verification (slackV0)", () => {
  const slackPost = (h: Harness, payload: unknown, tsOffset = 0, secret = "slack-secret"): Promise<Response> => {
    const body = JSON.stringify(payload);
    const ts = String(Math.floor(Date.now() / 1000) + tsOffset);
    return fetch(`http://localhost:${h.port}/webhooks/slack`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-slack-request-timestamp": ts,
        "x-slack-signature": `v0=${hmacHex(secret, `v0:${ts}:${body}`)}`,
      },
      body,
    });
  };

  it("answers url_verification with its challenge synchronously and relays nothing", async () => {
    const h = await start();
    const res = await slackPost(h, { type: "url_verification", challenge: "chal-42" });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ challenge: "chal-42" });
    await settle();
    expect(h.submitEvent).not.toHaveBeenCalled();
  });

  it("does not answer a url_verification it cannot verify", async () => {
    const h = await start();
    const res = await slackPost(h, { type: "url_verification", challenge: "chal-42" }, 0, "wrong");
    expect(res.status).toBe(401);
  });

  it("401s a stale timestamp (replay) without parsing", async () => {
    const h = await start();
    const res = await slackPost(h, { type: "event_callback" }, -301);
    expect(res.status).toBe(401);
    await settle();
    expect(parseBody).not.toHaveBeenCalled();
    expect(h.submitEvent).not.toHaveBeenCalled();
  });

  it("relays a verified event without the signature header", async () => {
    const h = await start();
    expect((await slackPost(h, { type: "event_callback", event: { text: "hi" } })).status).toBe(202);
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalled());
    const envelope = submittedEnvelope(h);
    expect(envelope.body).toEqual({ type: "event_callback", event: { text: "hi" } });
    expect(envelope.headers).not.toHaveProperty("x-slack-signature");
    expect(envelope.headers).toHaveProperty("x-slack-request-timestamp");
    // A source without `sender` forwards no login.
    expect(h.submitEvent.mock.calls[0]?.[1]).toBeUndefined();
  });
});

describe("webhook verification (bearer)", () => {
  it("default authorization header: 'Bearer <secret>' accepted, anything else 401", async () => {
    const h = await start();
    const post = (auth: string): Promise<Response> =>
      fetch(`http://localhost:${h.port}/webhooks/ci`, { method: "POST", headers: { authorization: auth, "content-type": "text/plain" }, body: "build ok" });
    expect((await post("Bearer wrong")).status).toBe(401);
    expect((await post("ci-token")).status).toBe(401);
    expect((await post("Bearer ci-token")).status).toBe(202);
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalledTimes(1));
    const envelope = submittedEnvelope(h);
    expect(envelope.body).toEqual({ text: "build ok" });
    expect(envelope.headers).not.toHaveProperty("authorization");
  });

  it("custom header carries the raw secret and is dropped from the envelope", async () => {
    const h = await start();
    const res = await fetch(`http://localhost:${h.port}/webhooks/ci-custom`, {
      method: "POST",
      headers: { "x-ci-token": "ci-token", "content-type": "application/x-www-form-urlencoded" },
      body: "status=green",
    });
    expect(res.status).toBe(202);
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalled());
    const envelope = submittedEnvelope(h);
    expect(envelope.body).toEqual({ status: "green" });
    expect(envelope.headers).not.toHaveProperty("x-ci-token");
  });
});

describe("sender verification", () => {
  it("forwards a VERIFIED login with the signed-assertion path, in envelope.sender, with repo context", async () => {
    const h = await start();
    await postGithub(h);
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalled());
    expect(h.resolve).toHaveBeenCalledWith("alice", false, { owner: "acme", repo: "widgets" });
    expect(h.submitEvent.mock.calls[0]?.[1]).toEqual({ login: "alice" });
    const envelope = submittedEnvelope(h);
    expect(envelope.sender).toEqual({ login: "alice", isBot: false });
    expect(envelope.repository).toBe("acme/widgets");
  });

  it("drops a bot-authored event (202 + log line) without resolving or relaying", async () => {
    const h = await start();
    const res = await postGithub(h, { ...GITHUB_PAYLOAD, sender: { login: "agent-controller[bot]", type: "Bot" } });
    expect(res.status).toBe(202);
    await settle();
    expect(h.resolve).not.toHaveBeenCalled();
    expect(h.submitEvent).not.toHaveBeenCalled();
    expect(h.logs.join("\n")).toMatch(/webhook github event delivery-123: dropped \(sender agent-controller\[bot\] is a bot\)/);
  });

  it("drops an unresolved sender (202 + log line)", async () => {
    const h = await start();
    const res = await postGithub(h, { ...GITHUB_PAYLOAD, sender: { login: "mallory", type: "User" } });
    expect(res.status).toBe(202);
    await settle();
    expect(h.resolve).toHaveBeenCalled();
    expect(h.submitEvent).not.toHaveBeenCalled();
    expect(h.logs.join("\n")).toMatch(/dropped \(sender mallory not verified by provider github\)/);
  });

  it("drops an event whose loginFrom cannot be evaluated (fail closed)", async () => {
    const h = await start();
    expect((await postGithub(h, { action: "ping" })).status).toBe(202);
    await settle();
    expect(h.submitEvent).not.toHaveBeenCalled();
    expect(h.logs.join("\n")).toMatch(/dropped \(sender.loginFrom failed/);
  });

  it("a source without `sender` forwards no login and an empty sender, even if the body names one", async () => {
    const h = await start();
    const body = JSON.stringify(GITHUB_PAYLOAD);
    await fetch(`http://localhost:${h.port}/webhooks/plain`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-sig": hmacHex("gh-secret", body) },
      body,
    });
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalled());
    expect(h.resolve).not.toHaveBeenCalled();
    expect(h.submitEvent.mock.calls[0]?.[1]).toBeUndefined();
    expect(submittedEnvelope(h).sender).toEqual({});
    expect(submittedEnvelope(h).repository).toBe("");
  });
});

describe("sender lookups are skipped for deliveries no trigger could match", () => {
  const reviewTrigger = {
    metadata: { name: "github-pr-review" },
    spec: {
      sourceRef: "github",
      match: { cel: 'headers["x-github-event"] == "pull_request" && body.action == "labeled" && body.label.name == "ai-review"' },
      target: { agentRef: "swe", promptTemplate: "go" },
    },
  };

  it("a push no trigger matches never reaches the identity resolver (no GitHub API call) or the orchestrator", async () => {
    const h = await start({ eventTriggers: new StaticEventTriggerTable([reviewTrigger]) });
    const res = await postGithub(h, { ref: "refs/heads/main", repository: { full_name: "acme/widgets" }, sender: { login: "alice", type: "User" } }, {
      extraHeaders: { "x-github-event": "push" },
    });
    expect(res.status).toBe(202);
    await settle();
    expect(h.resolve).not.toHaveBeenCalled();
    expect(h.submitEvent).not.toHaveBeenCalled();
    expect(h.logs.join("\n")).toMatch(/webhook github event delivery-123: no trigger could match \(ignored before sender lookup\)/);
  });

  it("a delivery a trigger matches is still sender-verified before it is relayed", async () => {
    const h = await start({ eventTriggers: new StaticEventTriggerTable([reviewTrigger]) });
    await postGithub(h);
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalled());
    expect(h.resolve).toHaveBeenCalledWith("alice", false, { owner: "acme", repo: "widgets" });
    expect(h.submitEvent.mock.calls[0]?.[1]).toEqual({ login: "alice" });
  });

  it("a trigger that reads `sender` can't be decided early, so the sender is resolved", async () => {
    const senderTrigger = { metadata: { name: "by-alice" }, spec: { sourceRef: "github", match: { cel: 'sender.login == "alice"' }, target: { agentRef: "swe", promptTemplate: "go" } } };
    const h = await start({ eventTriggers: new StaticEventTriggerTable([senderTrigger]) });
    await postGithub(h, { ...GITHUB_PAYLOAD, action: "opened" });
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalled());
    expect(h.resolve).toHaveBeenCalled();
  });

  it("triggers for OTHER sources don't count", async () => {
    const slackOnly = { ...reviewTrigger, metadata: { name: "slack-only" }, spec: { ...reviewTrigger.spec, sourceRef: "slack" } };
    const h = await start({ eventTriggers: new StaticEventTriggerTable([slackOnly]) });
    await postGithub(h);
    await settle();
    expect(h.resolve).not.toHaveBeenCalled();
    expect(h.submitEvent).not.toHaveBeenCalled();
  });

  it("an unloaded trigger table resolves every sender, as before", async () => {
    const h = await start({ eventTriggers: { all: () => undefined } });
    await postGithub(h, { ...GITHUB_PAYLOAD, action: "opened" });
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalled());
    expect(h.resolve).toHaveBeenCalled();
  });
});

describe("/invoke outcomes drive exactly the declared side effects", () => {
  it("ignored: logs and does nothing else", async () => {
    const h = await start();
    h.submitEvent.mockResolvedValueOnce({ kind: "ignored" });
    await postGithub(h);
    await vi.waitFor(() => expect(h.logs.join("\n")).toMatch(/no trigger matched \(ignored\)/));
    await settle();
    expect(h.poll).not.toHaveBeenCalled();
    expect(h.postIssueComment).not.toHaveBeenCalled();
    expect(h.record).not.toHaveBeenCalled();
    expect(h.removeIssueLabel).not.toHaveBeenCalled();
    expect(h.replyFetch).not.toHaveBeenCalled();
  });

  it("422: logs and does nothing else", async () => {
    const h = await start();
    h.submitEvent.mockResolvedValueOnce({ kind: "rejected", error: "issueNumber did not render to a positive integer" });
    await postGithub(h);
    await vi.waitFor(() => expect(h.logs.join("\n")).toMatch(/\(422\): issueNumber did not render/));
    await settle();
    expect(h.poll).not.toHaveBeenCalled();
    expect(h.postIssueComment).not.toHaveBeenCalled();
    expect(h.record).not.toHaveBeenCalled();
    expect(h.removeIssueLabel).not.toHaveBeenCalled();
    expect(h.replyFetch).not.toHaveBeenCalled();
  });

  it("202: records the owed label, acks (with session page link), polls, posts the result, calls back, removes the label -- in that order", async () => {
    const h = await start();
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    expect(h.order).toEqual(["record", "comment:Starting a review…", "poll", "comment:Looks good.", "http", "removeLabel"]);

    const ack = h.postIssueComment.mock.calls[0];
    expect(ack?.slice(0, 3)).toEqual(["acme", "widgets", 7]);
    const token = /https:\/\/gateway\.example\.com\/sessions\/([A-Za-z0-9_-]+)/.exec(ack?.[3] as string)?.[1];
    expect(token).toBeDefined();
    const entry = await h.sessionPageStore.getByToken(token!);
    expect(entry).toMatchObject({ sessionId: "github:acme/widgets#7", owner: "acme", repo: "widgets", issueNumber: 7 });
    expect(entry?.turns).toHaveLength(1);
    expect(entry?.turns[0]).toMatchObject({ status: "succeeded", result: "Looks good." });

    expect(h.poll).toHaveBeenCalledWith("inv-1", expect.anything());
    expect(h.removeIssueLabel).toHaveBeenCalledWith("acme", "widgets", 7, "ai-review");
    expect(h.record).toHaveBeenCalledWith(expect.objectContaining({ owner: "acme", repo: "widgets", issueNumber: 7, label: "ai-review", sessionId: "github:acme/widgets#7" }));
    expect(h.clear).toHaveBeenCalledWith("github:acme/widgets#7", "ai-review");
    expect(await h.pendingLabelStore.list()).toEqual([]);
  });

  it("http reply: exact body, signature verifiable with the trigger's secret, literal url", async () => {
    const h = await start();
    await postGithub(h);
    await vi.waitFor(() => expect(h.replyFetch).toHaveBeenCalled());
    const [url, init] = h.replyFetch.mock.calls[0] as unknown as [URL, RequestInit];
    expect(String(url)).toBe("http://ci-bridge.tools.svc/results");
    const body = init.body as string;
    expect(JSON.parse(body)).toEqual({ trigger: "github-review", sessionId: "github:acme/widgets#7", status: "succeeded", result: "Looks good." });
    const headers = init.headers as Record<string, string>;
    expect(headers["x-event-signature"]).toBe(`sha256=${hmacHex("callback-secret", body)}`);
    expect(init.redirect).toBe("manual");
  });

  it("failure: result comment explains, http reply says failed, and the label still comes off", async () => {
    const h = await start();
    h.poll.mockResolvedValueOnce({ status: "failed", error: "agent crashed" });
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalledWith("acme", "widgets", 7, "ai-review"));
    expect(h.postIssueComment).toHaveBeenCalledWith("acme", "widgets", 7, "Something went wrong processing this: agent crashed");
    const body = JSON.parse((h.replyFetch.mock.calls[0] as unknown as [URL, RequestInit])[1].body as string);
    expect(body).toEqual({ trigger: "github-review", sessionId: "github:acme/widgets#7", status: "failed", error: "agent crashed" });
  });

  it("does not send an http reply whose signing secret is unavailable", async () => {
    const { "callback/secret": _omit, ...rest } = SECRETS;
    const h = await start({}, rest);
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    expect(h.replyFetch).not.toHaveBeenCalled();
    expect(h.logs.join("\n")).toMatch(/reply.http not sent \(signing secret callback\/secret unavailable\)/);
  });

  it("only declared effects run: no ack, no result, no label when reply.github is absent", async () => {
    const h = await start();
    h.submitEvent.mockResolvedValueOnce({ kind: "accepted", id: "inv-2", trigger: { name: "t", sessionId: "event:ci:x", reply: null } });
    await postGithub(h);
    await vi.waitFor(() => expect(h.poll).toHaveBeenCalled());
    await settle();
    expect(h.postIssueComment).not.toHaveBeenCalled();
    expect(h.record).not.toHaveBeenCalled();
    expect(h.removeIssueLabel).not.toHaveBeenCalled();
    expect(h.replyFetch).not.toHaveBeenCalled();
  });

  it("keeps the pending record when label removal fails, so the reconciler heals it", async () => {
    const h = await start();
    h.removeIssueLabel.mockRejectedValueOnce(new Error("502 after retries"));
    await postGithub(h);
    await vi.waitFor(() => expect(h.onBackgroundError).toHaveBeenCalledWith(expect.objectContaining({ message: "502 after retries" })));
    expect(h.clear).not.toHaveBeenCalled();
    expect(await h.pendingLabelStore.list()).toHaveLength(1);
    // The run's own outcome was still reported.
    expect(h.postIssueComment).toHaveBeenCalledWith("acme", "widgets", 7, "Looks good.");
  });

  it("still polls, reports and removes the label when the ack comment fails", async () => {
    const h = await start();
    h.postIssueComment.mockRejectedValueOnce(new Error("github down"));
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    expect(h.poll).toHaveBeenCalled();
    expect(h.replyFetch).toHaveBeenCalled();
  });

  it("edits the Remote Control link into the ack (at most once) instead of posting a second comment", async () => {
    const h = await start();
    h.poll.mockImplementationOnce(async (_id: string, hooks?: PollHooks) => {
      await hooks?.onRemoteControlUrl?.("https://claude.ai/code/session_abc");
      await hooks?.onRemoteControlUrl?.("https://claude.ai/code/session_abc");
      return { status: "succeeded", result: "Done." };
    });
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    const ackBody = h.postIssueComment.mock.calls[0]?.[3] as string;
    const ackId = await h.postIssueComment.mock.results[0]?.value;
    expect(h.updateIssueComment).toHaveBeenCalledTimes(1);
    expect(h.updateIssueComment).toHaveBeenCalledWith(
      "acme",
      "widgets",
      ackId,
      `${ackBody}\n\n🤖 Watch live or take over the session here: https://claude.ai/code/session_abc`,
    );
    expect(h.postIssueComment.mock.calls.filter((c) => String(c[3]).includes("Watch live"))).toEqual([]);
  });

  it("posts the Remote Control link as its own comment when there is no ack to edit", async () => {
    const h = await start();
    h.postIssueComment.mockRejectedValueOnce(new Error("github down")); // the ack
    h.poll.mockImplementationOnce(async (_id: string, hooks?: PollHooks) => {
      await hooks?.onRemoteControlUrl?.("https://claude.ai/code/session_abc");
      return { status: "succeeded", result: "Done." };
    });
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    expect(h.updateIssueComment).not.toHaveBeenCalled();
    const rc = h.postIssueComment.mock.calls.filter((c) => String(c[3]).includes("Watch live"));
    expect(rc).toEqual([["acme", "widgets", 7, "🤖 Watch live or take over the session here: https://claude.ai/code/session_abc"]]);
  });

  it("falls back to a separate Remote Control comment when editing the ack fails", async () => {
    const h = await start();
    h.updateIssueComment.mockRejectedValueOnce(new Error("422"));
    h.poll.mockImplementationOnce(async (_id: string, hooks?: PollHooks) => {
      await hooks?.onRemoteControlUrl?.("https://claude.ai/code/session_abc");
      return { status: "succeeded", result: "Done." };
    });
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    expect(h.updateIssueComment).toHaveBeenCalledTimes(1);
    const rc = h.postIssueComment.mock.calls.filter((c) => String(c[3]).includes("Watch live"));
    expect(rc).toEqual([["acme", "widgets", 7, "🤖 Watch live or take over the session here: https://claude.ai/code/session_abc"]]);
  });
});

describe("identity-link auto-resume on the event path", () => {
  const pending: OrchestratorInvokeResult = {
    status: "succeeded",
    identityLinkPending: true,
    identityLink: { provider: "claude", subject: "client-integration-gateway" },
    result: "To continue, please [link your Claude account](https://gateway.example.com/claude-auth/abc).",
  };

  it("posts the link prompt, waits, re-submits the same event and reports the resumed result (ack not repeated)", async () => {
    const waitForCompletion = vi.fn().mockResolvedValue({ token: "t", createdAt: "x" });
    const h = await start({
      claudeAuthStore: { get: vi.fn(), set: vi.fn(), delete: vi.fn(), waitForCompletion } as unknown as ClaudeTokenStore,
      resumeWaitMs: 20_000,
    });
    h.poll.mockResolvedValueOnce(pending);
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    expect(waitForCompletion).toHaveBeenCalledWith("client-integration-gateway", 20_000, "setup-token");
    expect(h.submitEvent).toHaveBeenCalledTimes(2);
    expect(submittedEnvelope(h, 1)).toEqual(submittedEnvelope(h, 0));
    expect(h.order.filter((o) => o.startsWith("comment:"))).toEqual([
      "comment:Starting a review…",
      "comment:To continue, please [link your Claude account](https://gateway.example.com/claude-auth/abc).",
      "comment:Looks good.",
    ]);
  });

  it("posts a Remote Control link from the resumed turn as a new comment, not an edit of the ack above the prompt", async () => {
    const waitForCompletion = vi.fn().mockResolvedValue({ token: "t", createdAt: "x" });
    const h = await start({
      claudeAuthStore: { get: vi.fn(), set: vi.fn(), delete: vi.fn(), waitForCompletion } as unknown as ClaudeTokenStore,
    });
    h.poll.mockResolvedValueOnce(pending);
    h.poll.mockImplementationOnce(async (_id: string, hooks?: PollHooks) => {
      await hooks?.onRemoteControlUrl?.("https://claude.ai/code/session_abc");
      return { status: "succeeded", result: "Looks good." };
    });
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    expect(h.updateIssueComment).not.toHaveBeenCalled();
    const rc = h.postIssueComment.mock.calls.filter((c) => String(c[3]).includes("Watch live"));
    expect(rc).toEqual([["acme", "widgets", 7, "🤖 Watch live or take over the session here: https://claude.ai/code/session_abc"]]);
  });

  it("leaves the prompt standing and reports failure over http when the link never completes", async () => {
    const waitForCompletion = vi.fn().mockResolvedValue(undefined);
    const h = await start({
      claudeAuthStore: { get: vi.fn(), set: vi.fn(), delete: vi.fn(), waitForCompletion } as unknown as ClaudeTokenStore,
    });
    h.poll.mockResolvedValueOnce(pending);
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    expect(h.submitEvent).toHaveBeenCalledTimes(1);
    const body = JSON.parse((h.replyFetch.mock.calls[0] as unknown as [URL, RequestInit])[1].body as string);
    expect(body.status).toBe("failed");
  });
});

describe("POST /events/:source/tick (cron)", () => {
  const tick = (h: Harness, name: string, auth?: string, body = '{"repo":"evil/evil"}'): Promise<Response> =>
    fetch(`http://localhost:${h.port}/events/${name}/tick`, {
      method: "POST",
      headers: { "content-type": "application/json", ...(auth ? { authorization: auth } : {}) },
      body,
    });

  it("401s a missing, malformed or unknown token, before revealing whether the source exists", async () => {
    const h = await start();
    expect((await tick(h, "nightly")).status).toBe(401);
    expect((await tick(h, "nightly", "Bearer wrong")).status).toBe(401);
    expect((await tick(h, "nightly", "Bearer aaa.unknown.sig")).status).toBe(401);
    expect((await tick(h, "does-not-exist", "Bearer aaa.unknown.sig")).status).toBe(401);
    expect((await tick(h, "nightly", NIGHTLY_TOKEN)).status).toBe(401);
    await settle();
    expect(h.submitEvent).not.toHaveBeenCalled();
  });

  it("401s a valid ServiceAccount that is not THIS source's (another source, another namespace)", async () => {
    const h = await start();
    expect((await tick(h, "nightly", `Bearer ${OTHER_TOKEN}`)).status).toBe(401);
    expect((await tick(h, "nightly", "Bearer aaa.wrongns.sig")).status).toBe(401);
    await settle();
    expect(h.submitEvent).not.toHaveBeenCalled();
  });

  it("401s every tick when no authenticator or namespace is configured (fail closed)", async () => {
    const h = await start({ tickAuthenticator: undefined });
    expect((await tick(h, "nightly", `Bearer ${NIGHTLY_TOKEN}`)).status).toBe(401);
    await h.server.close();
    const h2 = await start({ tickNamespace: undefined });
    expect((await tick(h2, "nightly", `Bearer ${NIGHTLY_TOKEN}`)).status).toBe(401);
  });

  it("never sends a malformed bearer or source name to the token authenticator", async () => {
    const authenticate = vi.fn(async () => undefined);
    const h = await start({ tickAuthenticator: { authenticate } });
    await tick(h, "nightly", "Bearer not-a-jwt");
    await tick(h, "nightly", "Basic aaa.nightly.sig");
    await tick(h, "Bad_Name", `Bearer ${NIGHTLY_TOKEN}`);
    expect(authenticate).not.toHaveBeenCalled();
  });

  it("404s an authenticated tick for a webhook or unknown source", async () => {
    const h = await start();
    expect((await tick(h, "github", "Bearer aaa.github.sig")).status).toBe(404);
    expect((await tick(h, "other", `Bearer ${OTHER_TOKEN}`)).status).toBe(404);
  });

  it("202s and relays a cron envelope whose body is the CR's payload (never the tick's)", async () => {
    const h = await start();
    expect((await tick(h, "nightly", `Bearer ${NIGHTLY_TOKEN}`)).status).toBe(202);
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalled());
    const envelope = submittedEnvelope(h);
    expect(envelope).toMatchObject({ source: "nightly", kind: "cron", headers: {}, body: { repo: "acme/widgets", n: 3 }, sender: {}, repository: "" });
    expect(envelope.id).toMatch(/^nightly-\d+-[0-9a-f]{8}$/);
    expect(h.submitEvent.mock.calls[0]?.[1]).toBeUndefined();
  });
});

describe("session pages", () => {
  it("relays a follow-up prompt as a conversational turn on the same session and posts to the STORED issue", async () => {
    const h = await start();
    // A session id that encodes no GitHub address at all: the issue must come
    // from the entry, not be parsed out of the id.
    const entry = await h.sessionPageStore.getOrCreate("event:ci:abc", { owner: "acme", repo: "widgets", issueNumber: 7 });
    const res = await fetch(`http://localhost:${h.port}/sessions/${entry.token}/prompts`, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ prompt: "actually, target develop" }).toString(),
      redirect: "manual",
    });
    expect(res.status).toBe(303);
    expect(res.headers.get("location")).toBe(`/sessions/${entry.token}`);
    await vi.waitFor(() => expect(h.postIssueComment).toHaveBeenCalled());
    expect(h.invoke).toHaveBeenCalledWith("actually, target develop", "event:ci:abc", "device");
    expect(h.submitEvent).not.toHaveBeenCalled();
    expect(h.postIssueComment).toHaveBeenCalledWith("acme", "widgets", 7, "Follow-up done.");
    const updated = await h.sessionPageStore.getByToken(entry.token);
    expect(updated?.turns[0]).toMatchObject({ status: "succeeded", result: "Follow-up done." });
  });

  it("serves the rendered page for a valid token and 404s an unknown one", async () => {
    const h = await start();
    const entry = await h.sessionPageStore.getOrCreate("s1", { owner: "acme", repo: "widgets", issueNumber: 7 });
    await h.sessionPageStore.addTurn(entry.sessionId, "do the thing");
    const res = await fetch(`http://localhost:${h.port}/sessions/${entry.token}`);
    expect(res.status).toBe(200);
    const html = await res.text();
    expect(html).toContain("acme/widgets #7");
    expect(html).toContain("do the thing");
    expect((await fetch(`http://localhost:${h.port}/sessions/does-not-exist`)).status).toBe(404);
  });
});

describe("live session tunnel (ADR 0026)", () => {
  async function startLive(): Promise<{ h: Harness; checkLive: ReturnType<typeof vi.fn>; openEventStream: ReturnType<typeof vi.fn>; forwardOpencode: ReturnType<typeof vi.fn> }> {
    const checkLive = vi.fn();
    const openEventStream = vi.fn();
    const forwardOpencode = vi.fn();
    const h = await start({
      orchestratorClient: { checkLive, openEventStream, forwardOpencode } as unknown as OrchestratorClient,
    });
    return { h, checkLive, openEventStream, forwardOpencode };
  }

  it("renders the live badge and caches the discovered agentRunId when checkLive reports live", async () => {
    const { h, checkLive } = await startLive();
    const entry = await h.sessionPageStore.getOrCreate("s1", { owner: "acme", repo: "widgets", issueNumber: 7 });
    checkLive.mockResolvedValue({ live: true, agentRunId: "run-42" });
    const html = await (await fetch(`http://localhost:${h.port}/sessions/${entry.token}`)).text();
    expect(html).toContain("LIVE");
    expect(html).toContain(`action="/sessions/${entry.token}/live-prompt"`);
    expect((await h.sessionPageStore.getByToken(entry.token))?.live).toEqual({ agentRunId: "run-42" });
  });

  it("clears cached live info once checkLive stops reporting live", async () => {
    const { h, checkLive } = await startLive();
    const entry = await h.sessionPageStore.getOrCreate("s1", { owner: "acme", repo: "widgets", issueNumber: 7 });
    await h.sessionPageStore.setLive(entry.sessionId, { agentRunId: "run-42" });
    checkLive.mockResolvedValue({ live: false });
    expect(await (await fetch(`http://localhost:${h.port}/sessions/${entry.token}`)).text()).not.toContain("LIVE");
    expect((await h.sessionPageStore.getByToken(entry.token))?.live).toBeUndefined();
  });

  it("proxies the live event stream, and 404s it when not live", async () => {
    const { h, openEventStream } = await startLive();
    const entry = await h.sessionPageStore.getOrCreate("s1", { owner: "acme", repo: "widgets", issueNumber: 7 });
    expect((await fetch(`http://localhost:${h.port}/sessions/${entry.token}/live-events`)).status).toBe(404);
    await h.sessionPageStore.setLive(entry.sessionId, { agentRunId: "run-42" });
    openEventStream.mockResolvedValue(
      new Response(new TextEncoder().encode('data: {"type":"x"}\n\n'), { status: 200, headers: { "content-type": "text/event-stream" } }),
    );
    const res = await fetch(`http://localhost:${h.port}/sessions/${entry.token}/live-events`);
    expect(res.status).toBe(200);
    expect(await res.text()).toBe('data: {"type":"x"}\n\n');
    expect(openEventStream).toHaveBeenCalledWith("run-42", "s1");
  });

  it("discovers and caches the opencode session id on first live prompt", async () => {
    const { h, forwardOpencode } = await startLive();
    const entry = await h.sessionPageStore.getOrCreate("s1", { owner: "acme", repo: "widgets", issueNumber: 7 });
    await h.sessionPageStore.setLive(entry.sessionId, { agentRunId: "run-42" });
    forwardOpencode.mockImplementation((_r: string, _s: string, req: { path: string }) =>
      Promise.resolve(req.path === "/session" ? { status: 200, body: [{ id: "ses_abc123" }] } : { status: 204 }),
    );
    const res = await fetch(`http://localhost:${h.port}/sessions/${entry.token}/live-prompt`, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ prompt: "also fix the tests" }).toString(),
      redirect: "manual",
    });
    expect(res.status).toBe(303);
    await vi.waitFor(() => expect(forwardOpencode).toHaveBeenCalledTimes(2));
    expect(forwardOpencode).toHaveBeenNthCalledWith(2, "run-42", "s1", {
      method: "POST",
      path: "/session/ses_abc123/prompt_async",
      body: { parts: [{ type: "text", text: "also fix the tests" }] },
    });
    expect((await h.sessionPageStore.getByToken(entry.token))?.live).toEqual({ agentRunId: "run-42", opencodeSessionId: "ses_abc123" });
  });
});

describe("webhook sender -> delegated subject (docs/adr/0049 §5-§6)", () => {
  const PAYLOAD = { ...GITHUB_PAYLOAD, sender: { login: "alice", type: "User", id: 583231 } };
  function postIds(h: Harness): Promise<Response> {
    const body = JSON.stringify(PAYLOAD);
    return fetch(`http://localhost:${h.port}/webhooks/github-ids`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-github-event": "pull_request",
        "x-github-delivery": "delivery-ids",
        "x-hub-signature-256": `sha256=${hmacHex("gh-secret", body)}`,
      },
      body,
    });
  }

  it.each([
    ["one linked subject", async () => ["openwebui:alice"], { subject: "openwebui:alice", subjectStatus: "linked" }],
    ["no linked subject", async () => [], { subjectStatus: "unlinked" }],
    ["several linked subjects", async () => ["openwebui:a", "openwebui:b"], { subjectStatus: "ambiguous" }],
    [
      "a failed lookup",
      async () => {
        throw new Error("apiserver down");
      },
      { subjectStatus: "unavailable" },
    ],
  ] as Array<[string, () => Promise<string[]>, Record<string, string>]>)(
    "vouches for the sender's account id and subject: %s",
    async (_, find, expected) => {
      const findSubjectsByAccount = vi.fn(find);
      const h = await start({ senderSubjects: { findSubjectsByAccount } });
      expect((await postIds(h)).status).toBe(202);
      expect(findSubjectsByAccount).toHaveBeenCalledWith("github", "583231");
      expect(h.submitEvent.mock.calls[0]?.[1]).toEqual({ login: "alice", accountId: "583231", ...expected });
    },
  );

  it("is unavailable, never linked, when no store is configured", async () => {
    const h = await start();
    await postIds(h);
    expect(h.submitEvent.mock.calls[0]?.[1]).toEqual({ login: "alice", accountId: "583231", subjectStatus: "unavailable" });
  });

  const identityError = (onIdentityError: boolean): EventSubmitResult => ({
    kind: "identity-required",
    body: {
      status: "failed",
      error: {
        code: "identity_link_required",
        message: "alice: link your GitHub account so swe can act as you: https://gw/connections",
        trigger: {
          name: "review",
          sessionId: "s",
          reply: { github: { repository: "acme/widgets", issueNumber: 7, ...(onIdentityError ? { onIdentityError: true } : {}) } },
        },
      },
    },
  });

  it("answers 403 with the engine's identity error body, verbatim", async () => {
    const h = await start({ senderSubjects: { findSubjectsByAccount: async () => [] } });
    h.submitEvent.mockResolvedValueOnce(identityError(false));
    const res = await postIds(h);
    expect(res.status).toBe(403);
    const body = (await res.json()) as { error: { code: string; message: string } };
    expect(body.error.code).toBe("identity_link_required");
    expect(body.error.message).toContain("link your GitHub account");
    await settle();
    expect(h.poll).not.toHaveBeenCalled();
    expect(h.postIssueComment).not.toHaveBeenCalled();
  });

  it("posts the identity error's message on the issue only when the trigger opts in", async () => {
    const h = await start({ senderSubjects: { findSubjectsByAccount: async () => [] } });
    h.submitEvent.mockResolvedValueOnce(identityError(true));
    expect((await postIds(h)).status).toBe(403);
    await vi.waitFor(() => expect(h.postIssueComment).toHaveBeenCalled());
    expect(h.postIssueComment).toHaveBeenCalledWith(
      "acme",
      "widgets",
      7,
      "alice: link your GitHub account so swe can act as you: https://gw/connections",
    );
  });
});

describe("webhook trace context (docs/adr/0050 §4)", () => {
  it("relays the delivery in the sender's trace when it sent one, else a new one", async () => {
    const h = await start();
    const body = JSON.stringify(GITHUB_PAYLOAD);
    await fetch(`http://localhost:${h.port}/webhooks/github`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-github-event": "pull_request",
        "x-github-delivery": "d-trace",
        "x-hub-signature-256": `sha256=${hmacHex("gh-secret", body)}`,
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      },
      body,
    });
    const traced = h.submitEvent.mock.calls[0] as unknown as [unknown, unknown, string];
    expect(traced[2]).toMatch(/^00-4bf92f3577b34da6a3ce929d0e0e4736-[0-9a-f]{16}-01$/);

    await postGithub(h);
    const fresh = h.submitEvent.mock.calls[1] as unknown as [unknown, unknown, string];
    expect(fresh[2]).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
    expect(fresh[2].slice(3, 35)).not.toBe("4bf92f3577b34da6a3ce929d0e0e4736");
  });

  describe("with export on", () => {
    // A fresh exporter per test: shutting a provider down stops its exporter.
    let exporter = new InMemorySpanExporter();
    let stop: () => Promise<void> = async () => {};
    afterEach(async () => {
      await stop();
      exporter = new InMemorySpanExporter();
    });

    it("exports one server span per delivery, continuing the sender's trace, and forwards THAT span to /invoke", async () => {
      stop = setupTelemetry("integration-gateway", { spanProcessor: new SimpleSpanProcessor(exporter) });
      const h = await start();
      await postGithub(h, GITHUB_PAYLOAD, { extraHeaders: { traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" } });
      await settle();
      const [span] = exporter.getFinishedSpans();
      expect(span?.name).toBe("webhook delivery");
      expect(span?.spanContext().traceId).toBe("4bf92f3577b34da6a3ce929d0e0e4736");
      expect(span?.parentSpanContext?.spanId).toBe("00f067aa0ba902b7");
      // The orchestrator's span must nest under an EXPORTED span, not a
      // minted id nobody reports.
      const sent = (h.submitEvent.mock.calls[0] as unknown as [unknown, unknown, string])[2];
      expect(sent).toBe(`00-${span!.spanContext().traceId}-${span!.spanContext().spanId}-01`);
      expect(span?.attributes).toMatchObject({
        "aktis.event_source": "github",
        "aktis.event_id": "delivery-123",
        "aktis.event.outcome": "accepted",
        "http.response.status_code": 202,
      });
    });

    it("ends a refused delivery's trace in an error span carrying the identity error code", async () => {
      stop = setupTelemetry("integration-gateway", { spanProcessor: new SimpleSpanProcessor(exporter) });
      const h = await start();
      h.submitEvent.mockResolvedValueOnce({
        kind: "identity-required",
        body: { status: "failed", error: { code: "identity_link_required", message: "link your account" } },
      });
      expect((await postGithub(h)).status).toBe(403);
      await settle();
      const [span] = exporter.getFinishedSpans();
      expect(span?.status.code).toBe(2); // SpanStatusCode.ERROR
      expect(span?.attributes).toMatchObject({
        "error.type": "identity_link_required",
        "aktis.identity_error": "identity_link_required",
        "aktis.event.outcome": "identity-required",
        "http.response.status_code": 403,
      });
    });
  });
});

describe("run-completion events (docs/adr/0052 §1)", () => {
  const TRIAGE: AcceptedTrigger = {
    name: "github-issue-triage",
    sessionId: "github:acme/widgets#7",
    reply: { github: { repository: "acme/widgets", issueNumber: 7, removeLabel: "ai-triage" } },
    params: { repository: "acme/widgets", number: "7" },
    onComplete: { emitTo: "runs" },
  };
  const REVIEW: AcceptedTrigger = {
    name: "factory-review",
    sessionId: "github:acme/widgets#12",
    reply: { github: { repository: "acme/widgets", issueNumber: 12, removeLabel: "ai-review" } },
    params: { repository: "acme/widgets", number: "12" },
    onComplete: { emitTo: "runs" },
  };
  const PR = { pullRequest: { number: 12, url: "https://github.com/acme/widgets/pull/12" }, madeNewCommits: true };

  /** Accepts the webhook with `first`, then answers each following submit with the next entry (`ignored` once they run out). */
  function chain(h: Harness, first: AcceptedTrigger, ...rest: AcceptedTrigger[]): void {
    const queue = [first, ...rest];
    let n = 0;
    h.submitEvent.mockImplementation(async (): Promise<EventSubmitResult> => {
      h.order.push(`submit:${n}`);
      const trigger = queue[n];
      n++;
      return trigger ? { kind: "accepted", id: `inv-${n}`, trigger } : { kind: "ignored" };
    });
  }

  const runEnvelopes = (h: Harness): Envelope[] => h.submitEvent.mock.calls.map((c) => c[0] as Envelope).filter((e) => e.kind === "run");

  it("publishes the finished run to the run source with trigger, status, params, outcome and a new chain -- after the label comes off", async () => {
    const h = await start();
    chain(h, TRIAGE);
    h.poll.mockResolvedValueOnce({ status: "succeeded", result: "token", outcome: PR });
    await postGithub(h);
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalledTimes(2));

    const event = submittedEnvelope(h, 1);
    expect(event).toEqual({
      source: "runs",
      kind: "run",
      id: expect.stringMatching(/^[0-9a-f-]{36}$/),
      time: expect.any(String),
      headers: {},
      sender: { login: "alice", isBot: false },
      repository: "acme/widgets",
      body: {
        trigger: "github-issue-triage",
        status: "succeeded",
        params: { repository: "acme/widgets", number: "7" },
        outcome: PR,
        chain: { id: "delivery-123", depth: 1, counts: { "github-issue-triage": 1 }, origin: { login: "alice", repository: "acme/widgets" } },
      },
    });
    // Relayed with the re-verified sender, exactly as a webhook's.
    expect(h.submitEvent.mock.calls[1]?.[1]).toEqual({ login: "alice" });
    expect(h.resolve).toHaveBeenLastCalledWith("alice", false, { owner: "acme", repo: "widgets" });
    expect(h.order.indexOf("removeLabel")).toBeLessThan(h.order.indexOf("submit:1"));
  });

  it("carries the origin's account id and vouches for its subject on every hop", async () => {
    const findSubjectsByAccount = vi.fn(async () => ["openwebui:alice"]);
    const h = await start({ senderSubjects: { findSubjectsByAccount } });
    chain(h, TRIAGE);
    const body = JSON.stringify({ ...GITHUB_PAYLOAD, sender: { login: "alice", type: "User", id: 583231 } });
    await fetch(`http://localhost:${h.port}/webhooks/github-ids`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-github-delivery": "delivery-ids",
        "x-hub-signature-256": `sha256=${hmacHex("gh-secret", body)}`,
      },
      body,
    });
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalledTimes(2));
    expect((submittedEnvelope(h, 1).body as { chain: unknown }).chain).toEqual({
      id: "delivery-ids",
      depth: 1,
      counts: { "github-issue-triage": 1 },
      origin: { login: "alice", accountId: "583231", repository: "acme/widgets" },
    });
    expect(h.submitEvent.mock.calls[1]?.[1]).toEqual({ login: "alice", accountId: "583231", subject: "openwebui:alice", subjectStatus: "linked" });
  });

  it("publishes a failed run too, with outcome {}", async () => {
    const h = await start();
    chain(h, TRIAGE);
    h.poll.mockResolvedValueOnce({ status: "failed", error: "agent crashed" });
    await postGithub(h);
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalledTimes(2));
    expect(submittedEnvelope(h, 1).body).toMatchObject({ status: "failed", outcome: {} });
  });

  it("publishes nothing for a trigger without onComplete.emitTo", async () => {
    const h = await start();
    const { onComplete: _omit, ...plain } = TRIAGE;
    chain(h, plain);
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    await settle();
    expect(h.submitEvent).toHaveBeenCalledTimes(1);
  });

  it.each([
    ["a missing source", "nope", /no such usable EventSource/],
    ["a webhook source", "github", /source is kind webhook, not run/],
    ["a cron source", "nightly", /source is kind cron, not run/],
  ])("skips (and logs) emitTo naming %s", async (_, emitTo, why) => {
    const h = await start();
    chain(h, { ...TRIAGE, onComplete: { emitTo } });
    await postGithub(h);
    await vi.waitFor(() => expect(h.logs.join("\n")).toMatch(why));
    await settle();
    expect(h.submitEvent).toHaveBeenCalledTimes(1);
  });

  it("can't emit for a chain with no verified human (a cron tick)", async () => {
    const h = await start();
    chain(h, TRIAGE);
    await fetch(`http://localhost:${h.port}/events/nightly/tick`, { method: "POST", headers: { authorization: `Bearer ${NIGHTLY_TOKEN}` } });
    await vi.waitFor(() => expect(h.logs.join("\n")).toMatch(/skipped \(the chain has no verified human sender to act as\)/));
    await settle();
    expect(h.submitEvent).toHaveBeenCalledTimes(1);
  });

  it("publishes nothing while the turn is parked on an identity link that never completes", async () => {
    const waitForCompletion = vi.fn().mockResolvedValue(undefined);
    const h = await start({
      claudeAuthStore: { get: vi.fn(), set: vi.fn(), delete: vi.fn(), waitForCompletion } as unknown as ClaudeTokenStore,
    });
    chain(h, TRIAGE);
    h.poll.mockResolvedValueOnce({ status: "succeeded", identityLinkPending: true, identityLink: { provider: "claude", subject: "s" }, result: "link" });
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    await settle();
    expect(h.submitEvent).toHaveBeenCalledTimes(1);
  });

  it("drops the hop when the origin no longer verifies, and says so on the issue", async () => {
    const h = await start();
    chain(h, TRIAGE);
    // The webhook's own lookup passes; the per-hop re-verification does not
    // (say alice's access was revoked mid-run).
    h.resolve.mockResolvedValueOnce({ subject: "alice", roles: ["reporter"] }).mockResolvedValueOnce(undefined);
    await postGithub(h);
    await vi.waitFor(() => expect(h.logs.join("\n")).toMatch(/dropped, chain origin alice no longer verifies/));
    await settle();
    expect(h.resolve).toHaveBeenCalledTimes(2);
    expect(h.submitEvent).toHaveBeenCalledTimes(1);
    expect(h.postIssueComment).toHaveBeenCalledWith("acme", "widgets", 7, expect.stringContaining("alice could not be re-verified"));
  });

  it("a run started by a run event publishes the next hop: depth and that trigger's count go up, origin is kept", async () => {
    const h = await start();
    chain(h, TRIAGE, REVIEW, REVIEW);
    h.poll
      .mockResolvedValueOnce({ status: "succeeded", result: "t1", outcome: PR })
      .mockResolvedValueOnce({ status: "succeeded", result: "t2", outcome: { verdict: "changes-requested" } })
      .mockResolvedValueOnce({ status: "succeeded", result: "t3", outcome: { verdict: "ready" } });
    await postGithub(h);
    await vi.waitFor(() => expect(h.submitEvent).toHaveBeenCalledTimes(4));

    const [, second, third] = runEnvelopes(h);
    expect(second?.body).toEqual({
      trigger: "factory-review",
      status: "succeeded",
      params: { repository: "acme/widgets", number: "12" },
      outcome: { verdict: "changes-requested" },
      chain: {
        id: "delivery-123",
        depth: 2,
        counts: { "github-issue-triage": 1, "factory-review": 1 },
        origin: { login: "alice", repository: "acme/widgets" },
      },
    });
    expect((third?.body as { chain: unknown }).chain).toEqual({
      id: "delivery-123",
      depth: 3,
      counts: { "github-issue-triage": 1, "factory-review": 2 },
      origin: { login: "alice", repository: "acme/widgets" },
    });
    // Every hop is re-verified and relayed as alice.
    expect(h.submitEvent.mock.calls.slice(1).map((c) => c[1])).toEqual([{ login: "alice" }, { login: "alice" }, { login: "alice" }]);
    expect(runEnvelopes(h).map((e) => e.sender)).toEqual(Array(3).fill({ login: "alice", isBot: false }));
  });

  it("drops a hop past RUN_EVENT_MAX_DEPTH and comments on the run's issue/PR", async () => {
    const h = await start({ runEventMaxDepth: 1 });
    chain(h, TRIAGE, REVIEW);
    await postGithub(h);
    await vi.waitFor(() => expect(h.logs.join("\n")).toMatch(/dropped, chain delivery-123 would reach depth 2 \(max 1\)/));
    await settle();
    // Depth 1 went out; depth 2 did not.
    expect(runEnvelopes(h)).toHaveLength(1);
    expect(h.postIssueComment).toHaveBeenCalledWith("acme", "widgets", 12, "Stopped the automated chain here: it exceeded 1 steps.");
  });

  it("allows a chain right up to the cap", async () => {
    const h = await start({ runEventMaxDepth: 2 });
    chain(h, TRIAGE, REVIEW);
    await postGithub(h);
    await vi.waitFor(() => expect(runEnvelopes(h)).toHaveLength(2));
  });

  it("a run source is not reachable over HTTP: no webhook route, no tick route", async () => {
    const h = await start();
    expect((await fetch(`http://localhost:${h.port}/webhooks/runs`, { method: "POST", body: "{}" })).status).toBe(404);
    expect((await fetch(`http://localhost:${h.port}/events/runs/tick`, { method: "POST", headers: { authorization: "Bearer aaa.runs.sig" } })).status).toBe(404);
    await settle();
    expect(h.submitEvent).not.toHaveBeenCalled();
  });
});

describe("reply.github addLabels / assignees (docs/adr/0052 §3)", () => {
  const HANDOFF: AcceptedTrigger = {
    name: "factory-handoff",
    sessionId: "github:acme/widgets#12",
    reply: { github: { repository: "acme/widgets", issueNumber: 12, ack: "Ready for review.", addLabels: ["ai-reviewed", ""], assignees: ["alice"] } },
  };

  it("applies them at acceptance, with the ack and before polling (empty entries dropped)", async () => {
    const h = await start();
    h.submitEvent.mockResolvedValueOnce({ kind: "accepted", id: "inv-1", trigger: HANDOFF });
    await postGithub(h);
    await vi.waitFor(() => expect(h.poll).toHaveBeenCalled());
    expect(h.addIssueLabels).toHaveBeenCalledWith("acme", "widgets", 12, ["ai-reviewed"]);
    expect(h.addIssueAssignees).toHaveBeenCalledWith("acme", "widgets", 12, ["alice"]);
    expect(h.order.slice(0, 4)).toEqual(["comment:Ready for review.", "addLabels:ai-reviewed", "assign:alice", "poll"]);
  });

  it("is best effort: a failed label add is reported and the run still proceeds", async () => {
    const h = await start();
    h.submitEvent.mockResolvedValueOnce({ kind: "accepted", id: "inv-1", trigger: HANDOFF });
    h.addIssueLabels.mockRejectedValueOnce(new Error("422 after retries"));
    await postGithub(h);
    await vi.waitFor(() => expect(h.poll).toHaveBeenCalled());
    expect(h.onBackgroundError).toHaveBeenCalledWith(expect.objectContaining({ message: "422 after retries" }));
    expect(h.addIssueAssignees).toHaveBeenCalled();
  });

  it("calls neither when the reply declares none", async () => {
    const h = await start();
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    expect(h.addIssueLabels).not.toHaveBeenCalled();
    expect(h.addIssueAssignees).not.toHaveBeenCalled();
  });
});

describe("in-flight run takeover (a gateway restarted mid-run)", () => {
  const REVIEW: AcceptedTrigger = {
    name: "factory-review",
    sessionId: "github:acme/widgets#12",
    reply: { github: { repository: "acme/widgets", issueNumber: 12, ack: "Starting a review…", result: true, removeLabel: "ai-review" } },
    params: { repository: "acme/widgets", number: "12" },
    onComplete: { emitTo: "runs" },
  };
  const VERDICT = { verdict: "changes-requested" };

  /** What a gateway that died mid-poll leaves in the store. */
  function orphan(overrides: Partial<InflightRun> = {}): InflightRun {
    return {
      key: "inv-dead",
      invocationId: "inv-dead",
      envelope: {
        source: "github",
        kind: "webhook",
        id: "delivery-dead",
        time: "2026-11-22T03:05:00Z",
        headers: {},
        body: {},
        sender: { login: "alice", isBot: false },
        repository: "acme/widgets",
      },
      sender: { login: "alice" },
      trigger: REVIEW,
      ackComment: { id: 77, body: "Starting a review…" },
      recordedAt: Date.now() - 60_000,
      ...overrides,
    };
  }

  const runEvents = (h: Harness): Envelope[] => h.submitEvent.mock.calls.map((c) => c[0] as Envelope).filter((e) => e.kind === "run");

  /** One recovery pass, as the periodic sweep would run it. */
  const sweep = (h: Harness): Promise<void> =>
    (h.server as unknown as { inflightRecovery: { sweepOnce: () => Promise<void> } }).inflightRecovery.sweepOnce();

  it("records an accepted run while it runs and clears it once finished", async () => {
    const store = new InMemoryInflightRunStore();
    const save = vi.spyOn(store, "save");
    const h = await start({ inflightRunStore: store });
    await postGithub(h);
    await vi.waitFor(() => expect(h.removeIssueLabel).toHaveBeenCalled());
    expect(save.mock.calls[0]?.[0]).toMatchObject({ key: "inv-1", invocationId: "inv-1", trigger: { name: "github-review" } });
    await vi.waitFor(async () => expect(await store.list()).toEqual([]));
  });

  it("finishes a run a dead gateway left: polls it, posts the result, removes the label, emits the run event -- no second ack", async () => {
    const store = new InMemoryInflightRunStore();
    const h = await start({ inflightRunStore: store });
    h.poll.mockResolvedValueOnce({ status: "succeeded", result: "Changes requested.", outcome: VERDICT });
    h.submitEvent.mockResolvedValue({ kind: "ignored" });
    await store.save(orphan());
    await sweep(h);

    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(1));
    expect(h.poll.mock.calls[0]?.[0]).toBe("inv-dead");
    expect(h.postIssueComment.mock.calls.map((c) => c[3])).toEqual(["Changes requested."]);
    expect(h.removeIssueLabel).toHaveBeenCalledWith("acme", "widgets", 12, "ai-review");
    expect(runEvents(h)[0]?.body).toMatchObject({
      trigger: "factory-review",
      status: "succeeded",
      outcome: VERDICT,
      chain: { id: "delivery-dead", depth: 1, counts: { "factory-review": 1 }, origin: { login: "alice", repository: "acme/widgets" } },
    });
    await vi.waitFor(async () => expect(await store.list()).toEqual([]));
  });

  it("leaves alone a run another gateway still holds the lease on", async () => {
    const store = new InMemoryInflightRunStore();
    await store.save(orphan());
    await store.lease("inv-dead", "the-old-pod", 60_000);
    const h = await start({ inflightRunStore: store });
    await settle();
    expect(h.poll).not.toHaveBeenCalled();
    expect(await store.list()).toHaveLength(1);
  });

  it("takes it over once that lease lapses", async () => {
    const store = new InMemoryInflightRunStore();
    await store.save(orphan());
    await store.lease("inv-dead", "the-old-pod", 1);
    await new Promise((r) => setTimeout(r, 5));
    const h = await start({ inflightRunStore: store });
    h.submitEvent.mockResolvedValue({ kind: "ignored" });
    await vi.waitFor(() => expect(h.poll).toHaveBeenCalledWith("inv-dead", expect.anything()));
  });

  it("never repeats a result comment or run event the dead gateway already sent", async () => {
    const store = new InMemoryInflightRunStore();
    await store.save(orphan({ resultPosted: true, emitted: true }));
    const h = await start({ inflightRunStore: store });
    await vi.waitFor(() => expect(h.poll).toHaveBeenCalled());
    await vi.waitFor(async () => expect(await store.list()).toEqual([]));
    expect(h.postIssueComment).not.toHaveBeenCalled();
    expect(h.submitEvent).not.toHaveBeenCalled();
  });

  it("polls the re-submitted turn when the run had moved on past an identity link", async () => {
    const store = new InMemoryInflightRunStore();
    await store.save(orphan({ invocationId: "inv-resubmitted" }));
    const h = await start({ inflightRunStore: store });
    h.submitEvent.mockResolvedValue({ kind: "ignored" });
    await vi.waitFor(() => expect(h.poll).toHaveBeenCalledWith("inv-resubmitted", expect.anything()));
  });
});

describe("held run events and conflict events (docs/adr/0055)", () => {
  const PARK: AcceptedTrigger = {
    name: "factory-park",
    sessionId: "event:park",
    reply: { github: { repository: "acme/widgets", issueNumber: 7 } },
    params: { repository: "acme/widgets", number: "7", waitFor: "5" },
    onComplete: { emitTo: "runs", waitUntilClosed: { repositoryParam: "repository", numberParam: "waitFor" } },
  };
  const INTAKE: AcceptedTrigger = {
    name: "factory-intake",
    sessionId: "event:intake",
    reply: { github: { repository: "acme/widgets", issueNumber: 7 } },
    params: { repository: "acme/widgets", number: "7" },
    onComplete: { emitTo: "runs", serialize: { group: "coord", keyParam: "repository" } },
  };
  const COORDINATE: AcceptedTrigger = {
    name: "factory-coordinate",
    sessionId: "factory-coordinate:acme/widgets#7",
    reply: { github: { repository: "acme/widgets", issueNumber: 7 } },
    params: { repository: "acme/widgets", number: "7" },
  };

  function chain(h: Harness, ...queue: AcceptedTrigger[]): void {
    let n = 0;
    h.submitEvent.mockImplementation(async (): Promise<EventSubmitResult> => {
      const trigger = queue[n];
      n++;
      return trigger ? { kind: "accepted", id: `inv-${n}`, trigger } : { kind: "ignored" };
    });
  }
  const runEvents = (h: Harness): Envelope[] => h.submitEvent.mock.calls.map((c) => c[0] as Envelope).filter((e) => e.kind === "run");
  const kick = (h: Harness): void => (h.server as unknown as { kickHeldEvents: () => void }).kickHeldEvents();
  const settle = (): Promise<void> => new Promise((r) => setTimeout(r, 30));

  it("holds a waitUntilClosed completion while the issue is open, and publishes it once it closes", async () => {
    const store = new InMemoryDeferredEventStore();
    const h = await start({ deferredEventStore: store });
    chain(h, PARK);
    await postGithub(h);
    await vi.waitFor(async () => expect(await store.list()).toHaveLength(1));
    expect(runEvents(h)).toHaveLength(0);
    expect(h.getIssueState).toHaveBeenCalledWith("acme", "widgets", 5);
    expect(h.logs.some((l) => l.includes("held until acme/widgets#5 is closed"))).toBe(true);

    kick(h);
    await settle();
    expect(runEvents(h)).toHaveLength(0);

    h.getIssueState.mockImplementation(async (_o: string, _r: string, n: number) => (n === 5 ? "closed" : "open"));
    kick(h);
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(1));
    expect(runEvents(h)[0]?.body).toMatchObject({ trigger: "factory-park", params: { waitFor: "5" } });
    expect(await store.list()).toHaveLength(0);
  });

  it("keeps a held event whose publish hits a failed relay, and publishes it on a later sweep", async () => {
    const store = new InMemoryDeferredEventStore();
    const h = await start({ deferredEventStore: store });
    chain(h, PARK);
    await postGithub(h);
    await vi.waitFor(async () => expect(await store.list()).toHaveLength(1));
    const [held] = await store.list();

    h.getIssueState.mockImplementation(async (_o: string, _r: string, n: number) => (n === 5 ? "closed" : "open"));
    h.submitEvent.mockResolvedValueOnce({ kind: "failed", error: "orchestrator down" });
    kick(h);
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(1));
    await vi.waitFor(async () => expect(await store.list()).toHaveLength(1));
    // Same record: its place in line holds.
    expect((await store.list())[0]).toMatchObject({ id: held!.id, createdAt: held!.createdAt });
    expect(h.logs.some((l) => l.includes("held again for the next sweep"))).toBe(true);

    h.submitEvent.mockResolvedValue({ kind: "ignored" });
    kick(h);
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(2));
    expect(runEvents(h)[1]?.body).toMatchObject({ trigger: "factory-park", params: { waitFor: "5" } });
    await vi.waitFor(async () => expect(await store.list()).toHaveLength(0));
  });

  it("stops retrying a held event whose publish keeps failing, and frees its queue", async () => {
    const store = new InMemoryDeferredEventStore();
    let clock = Date.parse("2026-01-01T00:00:00Z");
    const h = await start({ deferredEventStore: store, now: () => new Date(clock) });
    chain(h, INTAKE);
    h.submitEvent.mockResolvedValue({ kind: "failed", error: "404 not found" });
    h.submitEvent.mockResolvedValueOnce({ kind: "accepted", id: "inv-1", trigger: INTAKE });
    await postGithub(h);
    // Held for its queue, published at once, and the publish fails.
    await vi.waitFor(async () => expect((await store.list())[0]?.publishFailure).toEqual({ firstAt: clock, attempts: 1 }));
    const [held] = await store.list();
    const queue = held!.queue!;
    const firstAt = clock;
    clock += 60 * 60 * 1000;
    kick(h);
    await vi.waitFor(async () => expect((await store.list())[0]?.publishFailure).toEqual({ firstAt, attempts: 2 }));
    expect((await store.list())[0]).toMatchObject({ id: held!.id, createdAt: held!.createdAt });

    clock = firstAt + 24 * 60 * 60 * 1000;
    kick(h);
    await vi.waitFor(() => expect(h.logs.some((l) => l.includes("publish failed 3 times") && l.includes("dropped"))).toBe(true));
    expect(await store.list()).toHaveLength(0);
    // Its queue was released: another holder can take it.
    expect(await store.acquireQueue(queue, "other", 60_000)).toBe(true);
  });

  it("drops a held event the orchestrator delivered but did not accept", async () => {
    const store = new InMemoryDeferredEventStore();
    const h = await start({ deferredEventStore: store });
    chain(h, PARK);
    await postGithub(h);
    await vi.waitFor(async () => expect(await store.list()).toHaveLength(1));
    h.getIssueState.mockImplementation(async (_o: string, _r: string, n: number) => (n === 5 ? "closed" : "open"));
    h.submitEvent.mockResolvedValue({ kind: "rejected", error: "bad params" });
    kick(h);
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(1));
    await settle();
    expect(await store.list()).toHaveLength(0);
  });

  it("restarts the chain depth when the wait really blocked (counts are kept)", async () => {
    const store = new InMemoryDeferredEventStore();
    const FIRST: AcceptedTrigger = { ...PARK, name: "factory-coordinate", onComplete: { emitTo: "runs" } };
    const h = await start({ deferredEventStore: store, runEventMaxDepth: 1 });
    chain(h, FIRST, PARK);
    await postGithub(h);
    // webhook -> coordinate (its event at depth 1) -> park, held.
    await vi.waitFor(async () => expect(await store.list()).toHaveLength(1));
    h.getIssueState.mockImplementation(async (_o: string, _r: string, n: number) => (n === 5 ? "closed" : "open"));
    kick(h);
    // Without the reset this would be depth 2 > max 1, and dropped.
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(2));
    expect((runEvents(h)[1]?.body as { chain: unknown }).chain).toMatchObject({
      depth: 1,
      counts: { "factory-coordinate": 1, "factory-park": 1 },
    });
  });

  it("abandons a wait when the held run's own issue is closed", async () => {
    const store = new InMemoryDeferredEventStore();
    const h = await start({ deferredEventStore: store });
    chain(h, PARK);
    await postGithub(h);
    await vi.waitFor(async () => expect(await store.list()).toHaveLength(1));
    h.getIssueState.mockImplementation(async (_o: string, _r: string, n: number) => (n === 7 ? "closed" : "open"));
    kick(h);
    await vi.waitFor(async () => expect(await store.list()).toHaveLength(0));
    expect(runEvents(h)).toHaveLength(0);
    expect(h.logs.some((l) => l.includes("abandoned"))).toBe(true);
  });

  it.each([
    ["its own issue", { waitFor: "7" }, "its own issue"],
    ["an unusable number", { waitFor: "none" }, "are not a repository and number"],
  ])("does not wait on %s -- publishes at once", async (_, params, why) => {
    const store = new InMemoryDeferredEventStore();
    const h = await start({ deferredEventStore: store });
    chain(h, { ...PARK, params: { ...PARK.params!, ...params } });
    await postGithub(h);
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(1));
    expect(await store.list()).toHaveLength(0);
    expect(h.logs.some((l) => l.includes(why))).toBe(true);
  });

  it("does not wait on an issue that is already closed", async () => {
    const h = await start();
    h.getIssueState.mockResolvedValue("closed");
    chain(h, PARK);
    await postGithub(h);
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(1));
    expect(h.logs.some((l) => l.includes("is already closed"))).toBe(true);
  });

  it("refuses a wait cycle: #7 waiting on #5 while #5 waits on #7 publishes at once", async () => {
    const store = new InMemoryDeferredEventStore();
    await store.save({
      id: "other",
      createdAt: 0,
      envelope: { source: "runs", kind: "run", id: "e", time: "t", headers: {}, sender: {}, repository: "acme/widgets", body: {} },
      trigger: { ...PARK, reply: { github: { repository: "acme/widgets", issueNumber: 5 } } },
      finished: { status: "succeeded" },
      waitFor: { owner: "acme", repo: "widgets", number: 7 },
      parkedOn: { owner: "acme", repo: "widgets", number: 5 },
      blocked: true,
    });
    const h = await start({ deferredEventStore: store });
    chain(h, PARK);
    await postGithub(h);
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(1));
    expect(runEvents(h)[0]?.body).toMatchObject({ trigger: "factory-park" });
    expect(h.logs.some((l) => l.includes("would be a cycle"))).toBe(true);
    expect((await store.list()).map((e) => e.id)).toEqual(["other"]);
  });

  it("serializes: a second completion in the same queue waits until the run the first one started finishes", async () => {
    const store = new InMemoryDeferredEventStore();
    const h = await start({ deferredEventStore: store });
    chain(h, INTAKE, COORDINATE, INTAKE, COORDINATE);
    let finishFirst!: () => void;
    const firstCoordinate = new Promise<OrchestratorInvokeResult>((resolve) => {
      finishFirst = () => resolve({ status: "succeeded", result: "decided" });
    });
    h.poll.mockImplementation(async (id: string) => (id === "inv-2" ? firstCoordinate : { status: "succeeded", result: "ok" }));

    await postGithub(h);
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(1));
    // The coordinate run (inv-2) now holds queue coord:acme/widgets.
    await postGithub(h);
    await vi.waitFor(async () => expect(await store.list()).toHaveLength(1));
    kick(h);
    await settle();
    expect(runEvents(h)).toHaveLength(1);

    finishFirst();
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(2));
    expect(await store.list()).toHaveLength(0);
  });

  it("frees the queue when the held event is not accepted", async () => {
    const store = new InMemoryDeferredEventStore();
    const h = await start({ deferredEventStore: store });
    chain(h, INTAKE); // its run event is then "ignored"
    await postGithub(h);
    await vi.waitFor(() => expect(runEvents(h)).toHaveLength(1));
    await vi.waitFor(async () => expect(await store.acquireQueue("coord:acme/widgets", "probe", 1000)).toBe(true));
  });

  describe("conflict sweep", () => {
    const DIRTY = {
      number: 12,
      state: "open",
      draft: false,
      title: "Add widgets",
      url: "https://github.com/acme/widgets/pull/12",
      mergeable: false,
      mergeableState: "dirty",
      baseRef: "main",
      baseSha: "base1",
      headRef: "feature",
      headSha: "head1",
      labels: [] as string[],
      assignees: [{ login: "alice", id: 42 }],
    };
    const sweep = (h: Harness): Promise<void> =>
      (h.server as unknown as { conflictSweeper: { sweepSource: (n: string, c: unknown) => Promise<void> } }).conflictSweeper.sweepSource(
        "pr-conflicts",
        { author: "app/k5s-bot", intervalMs: 300_000, skipLabels: ["ai-triage"] },
      );

    const appAssigned = (h: Harness, ...logins: string[]): void => {
      h.getAssignmentActors.mockResolvedValue(new Map(logins.map((l) => [l, "k5s-bot[bot]"])));
    };

    it("emits one conflict event per PR and base commit, acting as the verified assignee", async () => {
      const h = await start();
      appAssigned(h, "alice");
      h.submitEvent.mockResolvedValue({ kind: "ignored" });
      h.searchOpenPullRequests.mockResolvedValue([{ owner: "acme", repo: "widgets", number: 12 }]);
      h.getPullRequest.mockResolvedValue(DIRTY);
      await sweep(h);
      expect(h.searchOpenPullRequests).toHaveBeenCalledWith("app/k5s-bot");
      expect(h.submitEvent).toHaveBeenCalledTimes(1);
      expect(submittedEnvelope(h)).toMatchObject({
        source: "pr-conflicts",
        kind: "conflict",
        sender: { login: "alice", isBot: false },
        repository: "acme/widgets",
        body: { pullRequest: { number: 12, baseRef: "main", baseSha: "base1", headSha: "head1", url: DIRTY.url } },
      });
      expect(h.submitEvent.mock.calls[0]?.[1]).toMatchObject({ login: "alice", accountId: "42" });

      await sweep(h);
      expect(h.submitEvent).toHaveBeenCalledTimes(1);
      // The base moved: once more.
      h.getPullRequest.mockResolvedValue({ ...DIRTY, baseSha: "base2" });
      await sweep(h);
      expect(h.submitEvent).toHaveBeenCalledTimes(2);
    });

    it.each([
      ["mergeable", { mergeable: true, mergeableState: "clean" }],
      ["not computed yet", { mergeable: null, mergeableState: "unknown" }],
      ["a draft", { draft: true }],
      ["carrying a skip label", { labels: ["ai-triage"] }],
      ["unassigned", { assignees: [] }],
      ["assigned only to someone who does not verify", { assignees: [{ login: "mallory", id: 1 }] }],
    ])("skips a PR that is %s", async (_, patch) => {
      const h = await start();
      appAssigned(h, "alice", "mallory");
      h.searchOpenPullRequests.mockResolvedValue([{ owner: "acme", repo: "widgets", number: 12 }]);
      h.getPullRequest.mockResolvedValue({ ...DIRTY, ...patch });
      await sweep(h);
      expect(h.submitEvent).not.toHaveBeenCalled();
    });

    it("skips an assignee a person assigned, even one who verifies", async () => {
      const h = await start();
      h.getAssignmentActors.mockResolvedValue(new Map([["alice", "some-triager"]]));
      h.searchOpenPullRequests.mockResolvedValue([{ owner: "acme", repo: "widgets", number: 12 }]);
      h.getPullRequest.mockResolvedValue(DIRTY);
      await sweep(h);
      expect(h.getAssignmentActors).toHaveBeenCalledWith("acme", "widgets", 12);
      expect(h.submitEvent).not.toHaveBeenCalled();
      expect(h.logs.some((l) => l.includes("not running as assignee alice (assigned by some-triager"))).toBe(true);
    });

    it("uses the App-assigned assignee and passes over one a person added", async () => {
      const h = await start();
      h.getAssignmentActors.mockResolvedValue(new Map([["mallory", "some-triager"], ["alice", "K5S-Bot[bot]"]]));
      h.searchOpenPullRequests.mockResolvedValue([{ owner: "acme", repo: "widgets", number: 12 }]);
      h.getPullRequest.mockResolvedValue({ ...DIRTY, assignees: [{ login: "mallory", id: 1 }, { login: "alice", id: 42 }] });
      h.submitEvent.mockResolvedValue({ kind: "ignored" });
      await sweep(h);
      expect(h.submitEvent).toHaveBeenCalledTimes(1);
      expect(submittedEnvelope(h)).toMatchObject({ sender: { login: "alice", isBot: false } });
    });

    it("retries next sweep when the orchestrator was unreachable", async () => {
      const h = await start();
      appAssigned(h, "alice");
      h.searchOpenPullRequests.mockResolvedValue([{ owner: "acme", repo: "widgets", number: 12 }]);
      h.getPullRequest.mockResolvedValue(DIRTY);
      h.submitEvent.mockResolvedValueOnce({ kind: "failed", error: "down" }).mockResolvedValue({ kind: "ignored" });
      await sweep(h);
      await sweep(h);
      expect(h.submitEvent).toHaveBeenCalledTimes(2);
    });
  });
});

