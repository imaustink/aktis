import { describe, expect, it, vi, type Mock } from "vitest";
import { SENDER_ASSERTION_HEADER, mintSenderAssertion } from "./rbac/sender-assertion.js";
import { InMemoryInvocationStore, type InvocationStore } from "./invocation/types.js";
import { InvokeServer, type InvocationRecord } from "./server.js";
import type { IdentityRefusal, TurnInput, TurnOutcome } from "./engine/types.js";
import { InMemorySessionStore } from "./session/in-memory-session-store.js";
import type { Envelope, EventTriggerResource } from "@aktis/event-triggers";
import { CrdEventTriggerRegistry } from "./routing/crd-event-trigger-registry.js";
import { traceIdOf } from "./traceparent.js";
import { setupTelemetry } from "./telemetry.js";
import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

/** Any version-00 traceparent: every /invoke turn carries its server span's (docs/adr/0050 §4). */
const ANY_TRACEPARENT = expect.stringMatching(/^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/);

/**
 * Every request in this file goes over a fresh TCP connection.
 *
 * Without this the suite failed roughly one run in ten, always as
 * `TypeError: fetch failed` / `SocketError: other side closed` on a request to a
 * server that had just started -- with `bytesWritten: 339, bytesRead: 0`, i.e.
 * the request was written to a socket whose peer had already gone. Nothing was
 * wrong with the server: it is connection REUSE across tests.
 *
 * Node's `fetch` (undici) keeps a keep-alive pool keyed by origin
 * (`127.0.0.1:<port>`) for several seconds. Each test here listens on port 0,
 * gets an ephemeral port, and closes the server when it finishes -- so under
 * load, when the OS recycles an ephemeral port quickly enough, a later test's
 * server can land on a port whose pooled (and now dead) socket is still cached.
 * The next request to that origin is handed the corpse.
 *
 * `connection: close` opts every request out of pooling, so there is never a
 * cached socket to inherit. Verified: three requests to one origin open three
 * sockets with this header and reuse one without it. Declared at module scope so
 * it shadows the global for the whole file -- no call site has to remember.
 */
const nativeFetch = globalThis.fetch;
const fetch: typeof globalThis.fetch = (input, init = {}) => {
  // Via the Headers API rather than an object spread, so a caller passing
  // Headers or an entry array keeps its headers instead of losing them.
  const headers = new Headers(init.headers);
  headers.set("connection", "close");
  return nativeFetch(input, { ...init, headers });
};

/** A well-formed §2 envelope (docs/event-triggers.md), overridable per test. */
function envelope(overrides: Partial<Envelope> = {}): Envelope {
  return {
    source: "github",
    kind: "webhook",
    id: "delivery-1",
    time: "2026-10-13T00:00:00Z",
    headers: { "x-github-event": "issues" },
    body: { action: "labeled", label: { name: "ai-triage" }, repository: { full_name: "acme/widgets" }, issue: { number: 7, title: "Add dark mode" } },
    sender: { login: "imaustink", isBot: false },
    repository: "acme/widgets",
    ...overrides,
  };
}

/**
 * A real trigger table over the given CRs (compiled by the shared package, as in
 * production), each stamped controller-`Ready=True` -- the registry offers no
 * other trigger to selection.
 */
async function triggerRegistry(triggers: EventTriggerResource[]): Promise<CrdEventTriggerRegistry> {
  const ready = triggers.map((t) => ({ ...t, status: { conditions: [{ type: "Ready", status: "True" as const }] } }));
  const registry = new CrdEventTriggerRegistry("default", "core.aktis.ai", "v1alpha1", {
    listNamespacedCustomObject: vi.fn().mockResolvedValue({ items: ready }),
  });
  await registry.listAll();
  return registry;
}

/** Matches every event from any source: an ordinary conversational turn on "triage". */
const catchAllTrigger: EventTriggerResource = {
  metadata: { name: "catch-all" },
  spec: { target: { orchestrator: {}, promptTemplate: "triage" } },
};

function listenOn(server: InvokeServer): Promise<number> {
  return server.listen(0).then(() => {
    const address = server["server"]?.address();
    return typeof address === "object" && address ? address.port : 0;
  });
}

/** A TurnEngine whose calls can be inspected. */
interface FakeEngine {
  invoke: Mock<(input: TurnInput) => Promise<TurnOutcome>>;
  checkIdentity: Mock<(input: TurnInput) => Promise<IdentityRefusal | undefined>>;
}

/**
 * A fake engine. `turn` is either the outcome every turn ends with, or an
 * implementation -- the place a streaming test drives `input.progressListener`
 * before the turn settles, exactly as the Temporal adapter relays narration.
 */
function fakeEngine(turn: TurnOutcome | ((input: TurnInput) => Promise<TurnOutcome>) = { result: "done" }): FakeEngine {
  return {
    invoke: vi.fn(typeof turn === "function" ? turn : async () => turn),
    checkIdentity: vi.fn(async (): Promise<IdentityRefusal | undefined> => undefined),
  };
}

/** A turn that settles only when the test says so. */
function deferredEngine(): { engine: FakeEngine; settle: (outcome: TurnOutcome) => void } {
  let settle!: (outcome: TurnOutcome) => void;
  const pending = new Promise<TurnOutcome>((resolve) => (settle = resolve));
  return { engine: fakeEngine(() => pending), settle: (outcome) => settle(outcome) };
}

async function readSse(res: Response): Promise<unknown[]> {
  const text = await res.text();
  return text
    .split("\n\n")
    .map((line) => line.trim())
    .filter((line) => line.startsWith("data: ") && line !== "data: [DONE]")
    .map((line) => JSON.parse(line.slice("data: ".length)) as unknown);
}

type SseChunk = {
  event?: { type?: string; data?: { description?: string; done?: boolean } };
  choices?: { delta: { content?: string; tool_calls?: unknown[] }; finish_reason: string | null }[];
};

function contentOf(chunks: SseChunk[]): string {
  return chunks
    .filter((c) => c.choices)
    .map((c) => c.choices![0]?.delta.content ?? "")
    .join("");
}

function statusesOf(chunks: SseChunk[]): string[] {
  return chunks.filter((c) => c.event?.type === "status").map((c) => c.event?.data?.description ?? "");
}

function chat(port: number, body: Record<string, unknown>, headers: Record<string, string> = {}): Promise<Response> {
  return fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer tok-1", ...headers },
    body: JSON.stringify(body),
  });
}

function invoke(port: number, body: unknown, headers: Record<string, string> = {}): Promise<Response> {
  return fetch(`http://127.0.0.1:${port}/invoke`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer tok-1", ...headers },
    body: JSON.stringify(body),
  });
}

const tick = (ms = 10): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe("InvokeServer", () => {
  // The record is the request, and it used to live only in the memory of the
  // pod that accepted it. That pod therefore had to survive to answer the
  // poll -- so a rollout lost every turn accepted around it (AgentRun created
  // at 04:48:30 against a pod whose replacement started at 04:48:32; its
  // answer stranded behind a 404), and /invoke could not run two replicas at
  // all. A shared store means whichever replica is asked can answer.
  it("serves a poll from a store, so a DIFFERENT instance can answer it", async () => {
    const shared = new InMemoryInvocationStore();
    const { engine, settle } = deferredEngine();
    const accepting = new InvokeServer(engine, undefined, undefined, undefined, undefined, undefined, 5, "authcode", shared);
    // A second process entirely -- same store, its own engine it never calls.
    const polling = new InvokeServer(fakeEngine(), undefined, undefined, undefined, undefined, undefined, 5, "authcode", shared);
    const acceptPort = await listenOn(accepting);
    const pollPort = await listenOn(polling);

    const { id } = (await (await invoke(acceptPort, { request: "do the thing" })).json()) as { id: string };

    settle({ result: "done" });
    await tick();

    // The instance that never saw the POST answers the poll.
    const polled = await fetch(`http://127.0.0.1:${pollPort}/invoke/${id}`);
    expect(polled.status).toBe(200);
    expect((await polled.json()) as { status: string }).toMatchObject({ status: "succeeded" });
    await accepting.close();
    await polling.close();
  });

  // The terminal record write is the last thing a fire-and-forget turn does,
  // on a `void`-ed chain. RedisInvocationStore.set deliberately PROPAGATES its
  // error (a lost answer is the failure the durable store exists to remove) --
  // but a rejection escaping the void-ed chain is an unhandled rejection, and
  // Node >=20 terminates the process by default, wiping every OTHER in-flight
  // record on this pod. So a Redis blip at terminal-write time must log and
  // stay on its feet, costing only THIS record its answer.
  it("logs and does not crash the process when the terminal invocation write rejects", async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (err: unknown): void => {
      unhandled.push(err);
    };
    process.on("unhandledRejection", onUnhandled);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const records = new Map<string, InvocationRecord>();
    // Only the terminal writes reject; the initial `pending` write (awaited in
    // the request path, surfaced as a 500) must succeed so a 202 is returned
    // and the void-ed chain is the thing exercised.
    const failingTerminalStore: InvocationStore = {
      get: async (id) => records.get(id),
      set: async (_id, record) => {
        if (record.status === "pending") {
          records.set(record.id, record);
          return;
        }
        throw new Error("redis terminal write failed");
      },
    };
    const { engine, settle } = deferredEngine();
    const server = new InvokeServer(engine, undefined, undefined, undefined, undefined, undefined, 5, "authcode", failingTerminalStore);
    const port = await listenOn(server);
    try {
      const postRes = await invoke(port, { request: "do the thing" });
      expect(postRes.status).toBe(202);

      // Settle the turn so the terminal write fires -- and rejects.
      settle({ result: "done" });
      // Let the void-ed chain run and give any unhandled rejection a chance to surface.
      await tick(30);

      expect(unhandled).toEqual([]);
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining("failed to persist terminal invocation record"),
        expect.stringContaining("redis terminal write failed"),
      );
    } finally {
      process.off("unhandledRejection", onUnhandled);
      errorSpy.mockRestore();
      await server.close();
    }
  });

  // Durable records make an answer RETRIEVABLE from any replica; they do not
  // keep this process's poll of the engine alive. So a pod on the way out must
  // refuse new work rather than hand back a 202 for a record nobody will ever
  // complete -- the caller retries and reaches the replacement.
  it("refuses new turns once draining, instead of accepting one it cannot finish", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    server.beginDraining();

    const res = await invoke(port, { request: "do the thing" });
    expect(res.status).toBe(503);
    expect(res.headers.get("retry-after")).toBe("1");
    // The decisive part: no turn was started that could not have been completed.
    expect(engine.invoke).not.toHaveBeenCalled();
    await server.close();
  });

  it("accepts a request, returns 202 + id, and the result becomes available once the turn resolves", async () => {
    const { engine, settle } = deferredEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const postRes = await invoke(port, { request: "scrape https://example.com/recipe" });
    expect(postRes.status).toBe(202);
    const { id, status } = (await postRes.json()) as { id: string; status: string };
    expect(status).toBe("pending");
    await tick();
    expect(engine.invoke).toHaveBeenCalledWith({
      request: "scrape https://example.com/recipe",
      remoteControlUrlListener: expect.any(Function),
      reportEngineInvocation: expect.any(Function),
      traceparent: ANY_TRACEPARENT,
    });

    const pendingRes = await fetch(`http://127.0.0.1:${port}/invoke/${id}`);
    expect(pendingRes.status).toBe(200);
    expect((await pendingRes.json()) as { status: string }).toMatchObject({ status: "pending" });

    settle({ result: "Pancakes" });
    await tick();

    const doneRes = await fetch(`http://127.0.0.1:${port}/invoke/${id}`);
    expect((await doneRes.json()) as { status: string; result: unknown }).toMatchObject({
      status: "succeeded",
      result: "Pancakes",
    });

    await server.close();
  });

  it("surfaces remoteControlUrl on GET /invoke/:id as soon as the engine reports one, and carries it through to the terminal response", async () => {
    let settle!: (outcome: TurnOutcome) => void;
    const engine = fakeEngine((input) => {
      // This fire-and-forget /invoke path never sets `progressListener` (see
      // TurnInput's doc on why), only `remoteControlUrlListener`.
      input.remoteControlUrlListener?.("https://claude.ai/code/session_abc123");
      return new Promise<TurnOutcome>((resolve) => (settle = resolve));
    });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const { id } = (await (await invoke(port, { request: "triage this" })).json()) as { id: string };
    await tick();

    const pendingRes = await fetch(`http://127.0.0.1:${port}/invoke/${id}`);
    expect((await pendingRes.json()) as { remoteControlUrl?: string }).toMatchObject({
      status: "pending",
      remoteControlUrl: "https://claude.ai/code/session_abc123",
    });
    expect(engine.invoke.mock.calls[0]![0].progressListener).toBeUndefined();

    settle({ result: "opened PR #7" });
    await tick();

    const doneRes = await fetch(`http://127.0.0.1:${port}/invoke/${id}`);
    expect((await doneRes.json()) as { status: string; remoteControlUrl?: string }).toMatchObject({
      status: "succeeded",
      remoteControlUrl: "https://claude.ai/code/session_abc123",
    });

    await server.close();
  });

  it("omits remoteControlUrl entirely when the engine never reports one", async () => {
    const server = new InvokeServer(fakeEngine({ result: "done" }));
    const port = await listenOn(server);

    const { id } = (await (await invoke(port, { request: "open a PR" })).json()) as { id: string };
    await tick();

    const body = (await (await fetch(`http://127.0.0.1:${port}/invoke/${id}`)).json()) as Record<string, unknown>;
    expect(body.status).toBe("succeeded");
    expect(Object.keys(body)).not.toContain("remoteControlUrl");

    await server.close();
  });

  it("marks the invocation failed when the turn settles with an error", async () => {
    const engine = fakeEngine({ error: "no matching skill for this request" });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const postRes = await fetch(`http://127.0.0.1:${port}/invoke`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ request: "do a thing" }),
    });
    const { id } = (await postRes.json()) as { id: string };

    await tick();
    const res = await fetch(`http://127.0.0.1:${port}/invoke/${id}`);
    expect((await res.json()) as { status: string; error: string }).toMatchObject({
      status: "failed",
      error: "no matching skill for this request",
    });

    await server.close();
  });

  it("marks the invocation failed when the engine call itself rejects", async () => {
    const engine = fakeEngine(async () => {
      throw new Error("temporal engine /invoke failed: 502");
    });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const { id } = (await (await invoke(port, { request: "do a thing" })).json()) as { id: string };
    await tick();

    expect(await (await fetch(`http://127.0.0.1:${port}/invoke/${id}`)).json()).toMatchObject({
      status: "failed",
      error: "temporal engine /invoke failed: 502",
    });
    await server.close();
  });

  it("rejects a missing/invalid body with 400 without starting a turn", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const res = await invoke(port, {});
    expect(res.status).toBe(400);
    expect(engine.invoke).not.toHaveBeenCalled();

    await server.close();
  });

  it("returns 404 for an unknown invocation id", async () => {
    const server = new InvokeServer(fakeEngine());
    const port = await listenOn(server);

    const res = await fetch(`http://127.0.0.1:${port}/invoke/does-not-exist`);
    expect(res.status).toBe(404);

    await server.close();
  });

  /**
   * `buildTurnInput` awaits `sessionStore.get` before the turn ever starts
   * (see server.ts) -- a Redis blip there rejects that promise before the
   * `this.engine.invoke(...).catch(...)` chain even exists to catch it. Left
   * unhandled, that crashes the process, wiping every other in-flight
   * invocation and turning their polls into "poll failed: 404" for the
   * caller. This asserts the record still resolves to "failed" instead.
   */
  it("marks the invocation failed (not an unhandled rejection) when buildTurnInput itself rejects", async () => {
    const engine = fakeEngine();
    const failingSessionStore = {
      get: vi.fn().mockRejectedValue(new Error("redis: connection reset")),
      set: vi.fn(),
    };
    const server = new InvokeServer(engine, failingSessionStore);
    const port = await listenOn(server);

    const { id } = (await (await invoke(port, { request: "do a thing", session_id: "session-1" })).json()) as { id: string };

    await tick();
    const res = await fetch(`http://127.0.0.1:${port}/invoke/${id}`);
    expect(res.status).toBe(200);
    expect((await res.json()) as { status: string; error: string }).toMatchObject({
      status: "failed",
      error: "redis: connection reset",
    });
    expect(engine.invoke).not.toHaveBeenCalled();

    await server.close();
  });

  it("carries an optional session_id on POST /invoke into the turn", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine, new InMemorySessionStore({ ttlMs: 60_000, maxEntries: 10 }));
    const port = await listenOn(server);

    await invoke(port, { request: "extract https://example.com", session_id: "cli-7" });
    await tick();
    expect(engine.invoke).toHaveBeenCalledWith({
      request: "extract https://example.com",
      remoteControlUrlListener: expect.any(Function),
      reportEngineInvocation: expect.any(Function),
      traceparent: ANY_TRACEPARENT,
      sessionId: "cli-7",
    });

    await server.close();
  });
});

describe("InvokeServer /invoke with an event envelope (ADR 0048, docs/event-triggers.md §5)", () => {
  const reviewTrigger: EventTriggerResource = {
    metadata: { name: "github-pr-review" },
    spec: {
      sourceRef: "github",
      match: { cel: 'body.action == "labeled" && body.label.name == "ai-review"' },
      params: { repo: "body.repository.full_name", number: "body.issue.number" },
      sessionKey: "github:{{repo}}#{{number}}",
      target: { agentRef: "claude-code-swe-agent", readOnly: true, promptTemplate: "Review {{repo}}#{{number}}" },
      reply: { github: { repository: "{{repo}}", issueNumber: "{{number}}", ack: "Reviewing…", result: true } },
    },
  };
  const reviewEvent = (): Envelope =>
    envelope({
      body: { action: "labeled", label: { name: "ai-review" }, repository: { full_name: "acme/widgets" }, issue: { number: 7 } },
    });

  it("answers 200 ignored and starts NO turn when no trigger matches", async () => {
    const engine = fakeEngine();
    const invocations = new InMemoryInvocationStore();
    const setSpy = vi.spyOn(invocations, "set");
    const server = new InvokeServer(
      engine, undefined, undefined, await triggerRegistry([reviewTrigger]),
      undefined, undefined, 5, "authcode", invocations,
    );
    const port = await listenOn(server);

    // An event that matches nothing -- even with a perfectly good `request`,
    // which must NOT be run as a RAG fallback.
    const res = await invoke(port, { request: "please do something", event: envelope() });
    await tick();

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ignored" });
    expect(engine.invoke).not.toHaveBeenCalled();
    expect(setSpy).not.toHaveBeenCalled();
    await server.close();
  });

  it("ignores every event when no trigger table is configured", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const res = await invoke(port, { event: reviewEvent() });
    await tick();

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ignored" });
    expect(engine.invoke).not.toHaveBeenCalled();
    await server.close();
  });

  it("answers 422 failed, starting no turn, when the matched trigger's params cannot be resolved", async () => {
    const broken: EventTriggerResource = {
      metadata: { name: "needs-a-pr" },
      spec: {
        params: { number: "body.pull_request.number" },
        target: { orchestrator: {}, promptTemplate: "PR {{number}}" },
      },
    };
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, await triggerRegistry([broken]));
    const port = await listenOn(server);

    const res = await invoke(port, { event: envelope() });
    await tick();

    expect(res.status).toBe(422);
    const body = (await res.json()) as { status: string; error: string };
    expect(body.status).toBe("failed");
    expect(body.error).toContain("needs-a-pr");
    expect(engine.invoke).not.toHaveBeenCalled();
    await server.close();
  });

  it("answers 400 for a malformed envelope and starts no turn", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, await triggerRegistry([catchAllTrigger]));
    const port = await listenOn(server);

    const { kind: _kind, ...noKind } = envelope();
    const res = await invoke(port, { request: "triage", event: noKind });
    await tick();

    expect(res.status).toBe(400);
    // Rejected by envelope validation itself, naming the problem.
    expect(((await res.json()) as { error: string }).error).toContain("event.kind");
    expect(engine.invoke).not.toHaveBeenCalled();
    await server.close();
  });

  it("starts the resolved turn and answers 202 with the trigger, its session id and rendered reply", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, await triggerRegistry([reviewTrigger]));
    const port = await listenOn(server);

    const res = await invoke(port, { request: "ignored when an event matches", event: reviewEvent() });
    await tick();

    expect(res.status).toBe(202);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body).toEqual({
      id: expect.any(String),
      status: "pending",
      trigger: {
        name: "github-pr-review",
        sessionId: "github:acme/widgets#7",
        reply: { github: { repository: "acme/widgets", issueNumber: 7, ack: "Reviewing…", result: true } },
        params: { repo: "acme/widgets", number: "7" },
      },
    });
    expect(engine.invoke).toHaveBeenCalledTimes(1);
    const input = engine.invoke.mock.calls[0]![0];
    expect(input).toMatchObject({
      request: "Review acme/widgets#7",
      sessionId: "github:acme/widgets#7",
      forcedAgentId: "claude-code-swe-agent",
      readOnly: true,
      params: { repo: "acme/widgets", number: "7" },
      triggerName: "github-pr-review",
      targetRepository: "acme/widgets",
    });
    expect(input.forcedSkillId).toBeUndefined();
    expect(input.forcedToolId).toBeUndefined();
    await server.close();
  });

  it("forces a skill for a skillRef target", async () => {
    const skillTrigger: EventTriggerResource = {
      metadata: { name: "ci-failed" },
      spec: { target: { skillRef: "ci-triage-skill", promptTemplate: "CI failed" } },
    };
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, await triggerRegistry([skillTrigger]));
    const port = await listenOn(server);

    await invoke(port, { event: envelope() });
    await tick();

    const input = engine.invoke.mock.calls[0]![0];
    expect(input).toMatchObject({ request: "CI failed", forcedSkillId: "ci-triage-skill", triggerName: "ci-failed" });
    expect(input.forcedAgentId).toBeUndefined();
    // Not read-only: the trigger didn't ask for it, and nothing defaults it on.
    expect(input.readOnly).toBeUndefined();
    await server.close();
  });

  it("forces a one-off tool run with the contract's tool input and a descriptive request for a toolRef target", async () => {
    const toolTrigger: EventTriggerResource = {
      metadata: { name: "notify" },
      spec: { params: { input: "body.issue.title" }, target: { toolRef: "slack-notify" } },
    };
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, await triggerRegistry([toolTrigger]));
    const port = await listenOn(server);

    await invoke(port, { event: envelope() });
    await tick();

    expect(engine.invoke.mock.calls[0]![0]).toMatchObject({
      request: "Run tool slack-notify",
      forcedToolId: "slack-notify",
      toolInput: "Add dark mode",
    });
    await server.close();
  });

  it("runs an orchestrator target as an ordinary conversational turn on the rendered prompt", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, await triggerRegistry([catchAllTrigger]));
    const port = await listenOn(server);

    await invoke(port, { event: envelope({ repository: "" }), session_id: "caller-session" });
    await tick();

    const input = engine.invoke.mock.calls[0]![0];
    expect(input).toMatchObject({ request: "triage", sessionId: "caller-session", triggerName: "catch-all" });
    expect(input.forcedSkillId).toBeUndefined();
    expect(input.forcedAgentId).toBeUndefined();
    expect(input.forcedToolId).toBeUndefined();
    // An empty envelope.repository names no repository.
    expect(input).not.toHaveProperty("targetRepository");
    await server.close();
  });

  it("leaves a request without an event exactly as it was: conversational, nothing forced", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, await triggerRegistry([catchAllTrigger]));
    const port = await listenOn(server);

    const res = await invoke(port, { request: "what's for dinner?" });
    await tick();

    expect(res.status).toBe(202);
    expect(await res.json()).not.toHaveProperty("trigger");
    const input = engine.invoke.mock.calls[0]![0];
    expect(input.request).toBe("what's for dinner?");
    expect(input.triggerName).toBeUndefined();
    expect(input.readOnly).toBeUndefined();
    await server.close();
  });

  // The repository a webhook fired on is the one integration-gateway checked
  // the sender's permission on, so it has to reach the engine's read gate as
  // data rather than only as words in the rendered prompt.
  it("carries a webhook event's repository into the turn as the target repository", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, await triggerRegistry([catchAllTrigger]));
    const port = await listenOn(server);

    await invoke(port, { event: envelope({ repository: "e2e-org/e2e-repo" }) });
    await tick();
    expect(engine.invoke).toHaveBeenLastCalledWith(expect.objectContaining({ targetRepository: "e2e-org/e2e-repo" }));

    await invoke(port, { event: envelope({ repository: "" }) });
    await tick();
    expect(engine.invoke.mock.lastCall![0]).not.toHaveProperty("targetRepository");

    await server.close();
  });

  describe("sticky readOnly per session", () => {
    function store() {
      return new InMemorySessionStore({ ttlMs: 60_000, maxEntries: 10 });
    }

    it("makes every later turn in a read-only session read-only, and never leaks into another session", async () => {
      const sessions = store();
      const engine = fakeEngine();
      const server = new InvokeServer(engine, sessions, undefined, await triggerRegistry([reviewTrigger]));
      const port = await listenOn(server);

      await invoke(port, { event: reviewEvent() });
      await tick();
      expect((await sessions.get("github:acme/widgets#7"))?.readOnly).toBe(true);

      // A session-page follow-up: no event, just a prompt on the same session.
      await invoke(port, { request: "now push a fix", session_id: "github:acme/widgets#7" });
      await tick();
      expect(engine.invoke.mock.calls[1]![0]).toMatchObject({ readOnly: true });
      expect((await sessions.get("github:acme/widgets#7"))?.readOnly).toBe(true);

      // A different conversation is untouched.
      await invoke(port, { request: "now push a fix", session_id: "github:acme/widgets#8" });
      await tick();
      expect(engine.invoke.mock.calls[2]![0].readOnly).toBeUndefined();
      expect((await sessions.get("github:acme/widgets#8"))?.readOnly).toBeUndefined();
      await server.close();
    });

    it("applies the flag to a chat-completions follow-up on the same session too", async () => {
      const sessions = store();
      await sessions.set("chat-ro", { readOnly: true });
      const engine = fakeEngine();
      const server = new InvokeServer(engine, sessions);
      const port = await listenOn(server);

      await chat(port, { messages: [{ role: "user", content: "push it" }] }, { "x-openwebui-chat-id": "chat-ro" });
      expect(engine.invoke.mock.calls[0]![0]).toMatchObject({ readOnly: true, sessionId: "chat-ro" });
      await server.close();
    });

    it("lets a writable trigger on the same session key make the session writable again", async () => {
      // ai-review and ai-triage share github:<repo>#<n>. A ratchet would leave
      // every triage on an issue read-only (no GITHUB_TOKEN, no push) after a
      // single review of it.
      const triageTrigger: EventTriggerResource = {
        metadata: { name: "github-pr-triage" },
        spec: {
          sourceRef: "github",
          match: { cel: 'body.action == "labeled" && body.label.name == "ai-triage"' },
          params: { repo: "body.repository.full_name", number: "body.issue.number" },
          sessionKey: "github:{{repo}}#{{number}}",
          target: { agentRef: "claude-code-swe-agent", promptTemplate: "Triage {{repo}}#{{number}}" },
        },
      };
      const triageEvent = envelope({
        body: { action: "labeled", label: { name: "ai-triage" }, repository: { full_name: "acme/widgets" }, issue: { number: 7 } },
      });
      const sessions = store();
      const engine = fakeEngine();
      const server = new InvokeServer(engine, sessions, undefined, await triggerRegistry([reviewTrigger, triageTrigger]));
      const port = await listenOn(server);

      await invoke(port, { event: reviewEvent() });
      await tick();
      expect((await sessions.get("github:acme/widgets#7"))?.readOnly).toBe(true);

      await invoke(port, { event: triageEvent });
      await tick();
      expect(engine.invoke.mock.calls[1]![0]).toMatchObject({ triggerName: "github-pr-triage" });
      expect(engine.invoke.mock.calls[1]![0].readOnly).toBeUndefined();
      expect((await sessions.get("github:acme/widgets#7"))?.readOnly).toBeUndefined();

      // Its follow-ups inherit the writable mode...
      await invoke(port, { request: "also fix the typo", session_id: "github:acme/widgets#7" });
      await tick();
      expect(engine.invoke.mock.calls[2]![0].readOnly).toBeUndefined();

      // ...and a later review makes it read-only again.
      await invoke(port, { event: reviewEvent() });
      await tick();
      expect(engine.invoke.mock.calls[3]![0]).toMatchObject({ readOnly: true });
      expect((await sessions.get("github:acme/widgets#7"))?.readOnly).toBe(true);
      await server.close();
    });

    it("records the flag before the turn runs, so a turn that never finishes still leaves the session read-only", async () => {
      const sessions = store();
      let sawFlagAtInvoke: boolean | undefined;
      const engine = fakeEngine(async () => {
        sawFlagAtInvoke = (await sessions.get("github:acme/widgets#7"))?.readOnly;
        return new Promise<TurnOutcome>(() => {}); // the pod "dies" mid-turn
      });
      const server = new InvokeServer(engine, sessions, undefined, await triggerRegistry([reviewTrigger]));
      const port = await listenOn(server);

      await invoke(port, { event: reviewEvent() });
      await tick();

      expect(sawFlagAtInvoke).toBe(true);
      await server.close();
    });
  });
});

describe("InvokeServer OpenAI-compatible chat completions (ADR 0007)", () => {
  it("GET /v1/models lists a single model id", async () => {
    const server = new InvokeServer(fakeEngine());
    const port = await listenOn(server);

    const res = await fetch(`http://127.0.0.1:${port}/v1/models`);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { data: { id: string }[] };
    expect(body.data.map((m) => m.id)).toContain("agent-orchestrator");

    await server.close();
  });

  it("POST /v1/chat/completions (non-streaming) returns a chat.completion with the rendered result", async () => {
    const engine = fakeEngine({ result: "# Pancakes" });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const res = await chat(port, { model: "agent-orchestrator", messages: [{ role: "user", content: "scrape https://example.com" }] });

    expect(res.status).toBe(200);
    expect(engine.invoke).toHaveBeenCalledWith({ request: "scrape https://example.com" });
    const body = (await res.json()) as { object: string; choices: { message: { content: string } }[] };
    expect(body.object).toBe("chat.completion");
    expect(body.choices[0]?.message.content).toContain("Pancakes");

    await server.close();
  });

  it("POST /v1/chat/completions forwards Open WebUI's per-user signed JWT header to the engine (non-streaming)", async () => {
    // The bearer token is Open WebUI's single static token, shared by every one
    // of its users -- the X-OpenWebUI-User-Jwt header must reach the engine as
    // forwardedUserToken so each human resolves to their own subject.
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    await chat(port, { messages: [{ role: "user", content: "scrape https://example.com" }] }, { "x-openwebui-user-jwt": "alices-signed-jwt" });

    expect(engine.invoke).toHaveBeenCalledWith({
      request: "scrape https://example.com",
      forwardedUserToken: "alices-signed-jwt",
    });

    await server.close();
  });

  it("POST /v1/chat/completions omits forwardedUserToken when Open WebUI sends no such header", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    await chat(port, { messages: [{ role: "user", content: "scrape https://example.com" }] });

    expect(engine.invoke).toHaveBeenCalledWith({ request: "scrape https://example.com" });

    await server.close();
  });

  it("POST /v1/chat/completions (streaming) forwards Open WebUI's per-user signed JWT header to the engine", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    await (
      await chat(port, { stream: true, messages: [{ role: "user", content: "scrape https://example.com" }] }, { "x-openwebui-user-jwt": "alices-signed-jwt" })
    ).text();

    expect(engine.invoke).toHaveBeenCalledWith(expect.objectContaining({ forwardedUserToken: "alices-signed-jwt" }));

    await server.close();
  });

  it("POST /v1/chat/completions folds prior conversation turns (e.g. a previously extracted recipe) into the request instead of discarding them", async () => {
    const engine = fakeEngine({ result: "Published!" });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    await chat(port, {
      messages: [
        { role: "user", content: "Extract this recipe: https://example.com/recipe" },
        { role: "assistant", content: "# Pancakes\n\n## Ingredients\n- Flour" },
        { role: "user", content: "publish it!" },
      ],
    });

    expect(engine.invoke).toHaveBeenCalledWith({
      request:
        '<conversation_history>\n<message role="user">\nExtract this recipe: https://example.com/recipe\n</message>\n<message role="assistant">\n# Pancakes\n\n## Ingredients\n- Flour\n</message>\n</conversation_history>\n\npublish it!',
    });

    await server.close();
  });

  it("POST /v1/chat/completions keeps a recipe the USER pasted (not assistant-produced) visible on the next turn", async () => {
    // Regression: the fold used to include only the most recent assistant
    // message, so a user-pasted recipe was silently discarded when the next
    // turn was just "Let's publish it".
    const engine = fakeEngine({ result: "Published!" });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    await chat(port, {
      messages: [
        { role: "user", content: "# Kentucky Butter Cake\n\n## Ingredients\n- Butter" },
        { role: "assistant", content: "What changes would you like to make?" },
        { role: "user", content: "Let's publish it" },
      ],
    });

    const request = engine.invoke.mock.calls[0]![0].request;
    expect(request).toContain("# Kentucky Butter Cake");
    expect(request).toContain('<message role="user">');
    expect(request).toContain("What changes would you like to make?");
    expect(request.endsWith("Let's publish it")).toBe(true);

    await server.close();
  });

  it("POST /v1/chat/completions (non-streaming) maps a turn error to an OpenAI-shaped HTTP error", async () => {
    const server = new InvokeServer(fakeEngine({ error: "unauthorized: could not resolve caller identity" }));
    const port = await listenOn(server);

    const res = await fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ messages: [{ role: "user", content: "do a thing" }] }),
    });

    expect(res.status).toBe(401);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("unauthorized");

    await server.close();
  });

  it("rejects a request with no user message, without starting a turn", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const res = await chat(port, { messages: [{ role: "system", content: "be nice" }] });

    expect(res.status).toBe(400);
    expect(engine.invoke).not.toHaveBeenCalled();

    await server.close();
  });

  it("POST /v1/chat/completions (streaming) relays progress as status steps, then the final result over SSE", async () => {
    const engine = fakeEngine(async (input) => {
      input.progressListener?.("agent", "running Bash");
      return { result: "Argentina won the 2022 World Cup." };
    });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const res = await chat(port, { stream: true, messages: [{ role: "user", content: "who won the world cup?" }] });

    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/event-stream");
    expect(engine.invoke).toHaveBeenCalledWith(
      expect.objectContaining({ request: "who won the world cup?", progressListener: expect.any(Function) }),
    );

    const chunks = (await readSse(res)) as SseChunk[];
    expect(statusesOf(chunks)).toContain("agent: running Bash");
    expect(contentOf(chunks)).toContain("Argentina won the 2022 World Cup.");
    expect(contentOf(chunks)).not.toContain("agent stream ended unexpectedly");
    expect(chunks.filter((c) => c.choices).at(-1)?.choices![0]?.finish_reason).toBe("stop");
    // The step left open by the last progress line is closed before the stream ends.
    expect(chunks.some((c) => c.event?.data?.description === "agent: running Bash" && c.event.data.done === true)).toBe(true);

    await server.close();
  });

  it("POST /v1/chat/completions (streaming) reports a turn that ended with no result, error or tool calls, rather than rendering `undefined`", async () => {
    const engine = fakeEngine(async () => ({}));
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const res = await chat(port, { stream: true, messages: [{ role: "user", content: "hello" }] });

    const chunks = (await readSse(res)) as SseChunk[];
    expect(contentOf(chunks)).toContain("agent stream ended unexpectedly");
    expect(contentOf(chunks)).not.toContain("undefined");

    await server.close();
  });

  it("POST /v1/chat/completions (streaming) surfaces a turn error as the final assistant message", async () => {
    const server = new InvokeServer(fakeEngine({ error: "unauthorized: could not resolve caller identity" }));
    const port = await listenOn(server);

    const res = await chat(port, { stream: true, messages: [{ role: "user", content: "do a thing" }] });

    expect(res.status).toBe(200); // headers already sent by the time the turn settles
    expect(contentOf((await readSse(res)) as SseChunk[])).toContain("❌ unauthorized");

    await server.close();
  });

  it("POST /v1/chat/completions (streaming) surfaces a rejected engine call as the final assistant message", async () => {
    const server = new InvokeServer(
      fakeEngine(async () => {
        throw new Error("temporal engine /invoke failed: 503");
      }),
    );
    const port = await listenOn(server);

    const res = await chat(port, { stream: true, messages: [{ role: "user", content: "do a thing" }] });

    expect(contentOf((await readSse(res)) as SseChunk[])).toContain("❌ temporal engine /invoke failed: 503");
    await server.close();
  });

  it("streams an identity-link prompt as inline chat content, never a truncated status label", async () => {
    const prompt = "Link your GitHub account to continue: https://gateway.example.com/identity-link/start?state=abc";
    const engine = fakeEngine(async (input) => {
      input.progressListener?.("identity-link", prompt);
      return { result: "Done." };
    });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const chunks = (await readSse(await chat(port, { stream: true, messages: [{ role: "user", content: "open a PR" }] }))) as SseChunk[];

    expect(contentOf(chunks)).toContain(prompt);
    expect(statusesOf(chunks).every((d) => !d.includes("identity-link"))).toBe(true);
    await server.close();
  });

  it("streams a Remote Control session URL as inline chat content, never a truncated status label", async () => {
    const rcUrl = "https://claude.ai/code/session_abc123";
    // The engine adapter relays the agent's `remote-control-url` progress
    // through `progressListener`. The chat facade must render it as real,
    // clickable content -- a status label (truncated to 120 chars) would
    // mangle the URL.
    const engine = fakeEngine(async (input) => {
      input.progressListener?.("remote-control-url", rcUrl);
      return { result: "Opened PR #42." };
    });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const chunks = (await readSse(await chat(port, { stream: true, messages: [{ role: "user", content: "fix the bug" }] }))) as SseChunk[];
    const allContent = contentOf(chunks);

    // The full URL is delivered as content with a "watch live" affordance...
    expect(allContent).toContain(rcUrl);
    expect(allContent).toContain("Watch live or take over");
    // ...the actual turn result still follows...
    expect(allContent).toContain("Opened PR #42.");
    // ...and the URL never leaks into a (truncated) status label.
    expect(statusesOf(chunks).every((d) => !d.includes("claude.ai/code"))).toBe(true);

    await server.close();
  });

  it("separates consecutive agent-text narration chunks instead of fusing them into a run-on sentence", async () => {
    // Each "agent-text" progress call is one COMPLETE, independent text block
    // straight from claude-runner.ts's own stream-json parsing -- nothing joins
    // successive blocks with a separator upstream of here, so writing them
    // verbatim back-to-back used to fuse "...repo exists." and "Found it..."
    // into one run-on "...repo exists.Found it...".
    const engine = fakeEngine(async (input) => {
      input.progressListener?.("agent-text", "I'll start by checking whether the repo exists.");
      input.progressListener?.("agent-text", "Found it — bitovi/ai-experiments is accessible.");
      return { result: "Opened PR #42." };
    });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const allContent = contentOf((await readSse(await chat(port, { stream: true, messages: [{ role: "user", content: "fix the bug" }] }))) as SseChunk[]);

    expect(allContent).not.toContain("repo exists.Found it");
    expect(allContent).toMatch(/repo exists\.\s+Found it/);
    expect(allContent).not.toContain("accessible.Opened");

    await server.close();
  });

  it("closes an open status step as soon as real content starts streaming", async () => {
    // A mechanical tool-status step ("agent: running Bash") is opened with
    // done:false. If the agent's remaining work is mostly agent-text (no
    // further tool-status lines), that step must not stay open until the very
    // end of the turn -- Open WebUI's StatusHistory would otherwise show a
    // stale "running Bash" spinner for the rest of the turn.
    const engine = fakeEngine(async (input) => {
      input.progressListener?.("agent", "running Bash");
      input.progressListener?.("agent-text", "Empty directory, so I need to clone the repo fresh.");
      return { result: "Opened PR #42." };
    });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const chunks = (await readSse(await chat(port, { stream: true, messages: [{ role: "user", content: "fix the bug" }] }))) as SseChunk[];
    const statuses = chunks.filter((c) => c.event?.type === "status").map((c) => c.event!.data!);

    // The "running Bash" step must be closed (done:true) BEFORE the agent-text
    // content chunk that follows it -- not left open until the end.
    const runningBashIndex = chunks.findIndex((c) => c.event?.data?.description === "agent: running Bash" && c.event.data.done === false);
    const closedIndex = chunks.findIndex((c) => c.event?.data?.description === "agent: running Bash" && c.event.data.done === true);
    const contentIndex = chunks.findIndex((c) => c.choices?.[0]?.delta.content?.includes("Empty directory"));
    expect(runningBashIndex).toBeGreaterThanOrEqual(0);
    expect(closedIndex).toBeGreaterThan(runningBashIndex);
    expect(closedIndex).toBeLessThan(contentIndex);
    // Exactly one close for that step -- not closed again at the end too.
    expect(statuses.filter((s) => s.description === "agent: running Bash" && s.done === true)).toHaveLength(1);

    await server.close();
  });

  it("carries the chat-id header as the turn's session id", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine, new InMemorySessionStore({ ttlMs: 60_000, maxEntries: 10 }));
    const port = await listenOn(server);

    await chat(port, { messages: [{ role: "user", content: "extract https://example.com" }] }, { "x-openwebui-chat-id": "chat-42" });
    await chat(port, { messages: [{ role: "user", content: "extract https://example.com" }] });

    expect(engine.invoke).toHaveBeenNthCalledWith(1, { request: "extract https://example.com", sessionId: "chat-42" });
    // No header: nothing session-scoped at all.
    expect(engine.invoke).toHaveBeenNthCalledWith(2, { request: "extract https://example.com" });

    await server.close();
  });
});

describe("InvokeServer identity-link flow selection", () => {
  it("threads an explicit identity_link_flow on POST /invoke into the turn", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    await invoke(port, { request: "open a PR", identity_link_flow: "device" });
    await tick();

    expect(engine.invoke).toHaveBeenCalledWith({
      request: "open a PR",
      remoteControlUrlListener: expect.any(Function),
      reportEngineInvocation: expect.any(Function),
      identityLinkFlow: "device",
      traceparent: ANY_TRACEPARENT,
    });

    await server.close();
  });

  it("applies the configured default identity-link flow to a chat-completions turn that names none", async () => {
    // The regression this locks down: Open WebUI sends no `identity_link_flow`
    // field, so before this was configurable every chat turn was pinned to
    // "authcode" -- unusable on a deployment whose GitHub App Callback URL
    // cannot be made to match, with no way to opt into the device flow.
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, undefined, undefined, undefined, 5, "device");
    const port = await listenOn(server);

    await (await chat(port, { stream: true, messages: [{ role: "user", content: "open a PR" }] })).text();

    expect(engine.invoke.mock.calls[0]![0]).toMatchObject({ identityLinkFlow: "device" });

    await server.close();
  });

  // An OpenAI-SDK client with no browser can ask for the device flow per
  // turn, the same field /invoke takes.
  it("threads an explicit identity_link_flow on a chat-completions turn into the turn, streaming or not", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);
    const send = (stream: boolean) =>
      chat(port, { stream, identity_link_flow: "device", messages: [{ role: "user", content: "open a PR" }] }).then((r) => r.text());

    await send(true);
    await send(false);

    expect(engine.invoke.mock.calls[0]![0]).toMatchObject({ identityLinkFlow: "device" });
    expect(engine.invoke.mock.calls[1]![0]).toMatchObject({ identityLinkFlow: "device" });

    await server.close();
  });

  it("lets an explicit chat-completions identity_link_flow override the configured default, and ignores an invalid one", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, undefined, undefined, undefined, 5, "device");
    const port = await listenOn(server);
    const send = (flow: string) =>
      chat(port, { stream: true, identity_link_flow: flow, messages: [{ role: "user", content: "open a PR" }] }).then((r) => r.text());

    await send("authcode");
    await send("carrier-pigeon");

    expect(engine.invoke.mock.calls[0]![0]).toMatchObject({ identityLinkFlow: "authcode" });
    // Invalid falls back to the deployment default rather than failing the turn.
    expect(engine.invoke.mock.calls[1]![0]).toMatchObject({ identityLinkFlow: "device" });

    await server.close();
  });

  it("lets an explicit /invoke identity_link_flow override the configured default", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, undefined, undefined, undefined, undefined, 5, "device");
    const port = await listenOn(server);

    await invoke(port, { request: "open a PR", identity_link_flow: "authcode" });
    await tick();

    expect(engine.invoke).toHaveBeenCalledWith(expect.objectContaining({ identityLinkFlow: "authcode" }));

    await server.close();
  });

  it("silently ignores a missing or invalid identity_link_flow on POST /invoke, without failing the request", async () => {
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);
    const expected = {
      request: "open a PR",
      remoteControlUrlListener: expect.any(Function),
      reportEngineInvocation: expect.any(Function),
      traceparent: ANY_TRACEPARENT,
    };

    const omittedRes = await invoke(port, { request: "open a PR" });
    expect(omittedRes.status).toBe(202);
    await tick();
    expect(engine.invoke).toHaveBeenLastCalledWith(expected);

    const invalidRes = await invoke(port, { request: "open a PR", identity_link_flow: "carrier-pigeon" });
    expect(invalidRes.status).toBe(202);
    await tick();
    expect(engine.invoke).toHaveBeenLastCalledWith(expected);

    await server.close();
  });
});

describe("InvokeServer live-session routes after docs/adr/0053", () => {
  // The engine owns its agent runs, so this process never has a live tunnel to
  // offer. `/sessions/live` still answers a CONFIRMED not-live, because
  // integration-gateway's label reconciler only clears a label on a positive
  // terminal signal (a 404 reads as "unknown").
  it("answers /sessions/live with a confirmed not-live", async () => {
    const server = new InvokeServer(fakeEngine());
    const port = await listenOn(server);

    const res = await fetch(`http://127.0.0.1:${port}/sessions/live?sessionId=${encodeURIComponent("github:acme/widgets#7")}`, {
      headers: { authorization: "Bearer tok-1" },
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ live: false });
    await server.close();
  });

  it("401s /sessions/live without a bearer token", async () => {
    const server = new InvokeServer(fakeEngine());
    const port = await listenOn(server);

    const res = await fetch(`http://127.0.0.1:${port}/sessions/live?sessionId=s1`);
    expect(res.status).toBe(401);
    await server.close();
  });

  it("400s /sessions/live without a sessionId", async () => {
    const server = new InvokeServer(fakeEngine());
    const port = await listenOn(server);

    const res = await fetch(`http://127.0.0.1:${port}/sessions/live`, { headers: { authorization: "Bearer tok-1" } });
    expect(res.status).toBe(400);
    await server.close();
  });

  it("no longer serves the agent-run tunnel routes", async () => {
    const server = new InvokeServer(fakeEngine());
    const port = await listenOn(server);
    const auth = { authorization: "Bearer tok-1" };

    const events = await fetch(`http://127.0.0.1:${port}/agent-runs/run-1/events?sessionId=s1`, { headers: auth });
    const opencode = await fetch(`http://127.0.0.1:${port}/agent-runs/run-1/opencode?sessionId=s1`, {
      method: "POST",
      headers: { ...auth, "content-type": "application/json" },
      body: JSON.stringify({ method: "GET", path: "/global/health" }),
    });
    expect(events.status).toBe(404);
    expect(opencode.status).toBe(404);
    await server.close();
  });
});

describe("InvokeServer signed sender assertion (docs/adr/0030 §6)", () => {
  const SECRET = "assertion-secret";

  async function serverWithAssertions(engine: FakeEngine, secret: string | undefined = SECRET) {
    // Positional order: engine, sessionStore, taskCompleter,
    // eventTriggerRegistry, senderAssertionSecret.
    return new InvokeServer(engine, undefined, undefined, await triggerRegistry([catchAllTrigger]), secret);
  }

  it("threads the envelope's sender.login from POST /invoke into the turn", async () => {
    const engine = fakeEngine();
    const server = await serverWithAssertions(engine, "");
    const port = await listenOn(server);

    await invoke(port, { event: envelope({ sender: { login: "imaustink", isBot: false } }) });
    await tick();

    expect(engine.invoke).toHaveBeenCalledWith(expect.objectContaining({ senderLogin: "imaustink" }));
    await server.close();
  });

  it("accepts a login only from a verified assertion, ignoring the unsigned body field", async () => {
    const engine = fakeEngine();
    const server = await serverWithAssertions(engine);
    const port = await listenOn(server);

    // A DIFFERENT login in the body: with a secret configured this must be
    // ignored outright, not merged or preferred.
    await invoke(
      port,
      { event: envelope({ sender: { login: "victim" } }) },
      { [SENDER_ASSERTION_HEADER]: mintSenderAssertion(SECRET, "imaustink") },
    );
    await tick();

    expect(engine.invoke).toHaveBeenCalledWith(expect.objectContaining({ senderLogin: "imaustink" }));
    await server.close();
  });

  it("drops an unsigned senderLogin entirely when a secret is configured", async () => {
    const engine = fakeEngine();
    const server = await serverWithAssertions(engine);
    const port = await listenOn(server);

    await invoke(port, { event: envelope({ sender: { login: "victim" } }) });
    await tick();

    // No principal rather than the WRONG principal: holding the gateway's
    // token must not be enough to be handed another person's credentials.
    expect(engine.invoke.mock.calls[0]![0].senderLogin).toBeUndefined();
    await server.close();
  });

  it("still trusts the body field when no secret is configured (upgrade compatibility)", async () => {
    const engine = fakeEngine();
    const server = await serverWithAssertions(engine, ""); // "" = no secret configured
    const port = await listenOn(server);

    await invoke(port, { event: envelope({ sender: { login: "imaustink" } }) });
    await tick();

    expect(engine.invoke).toHaveBeenCalledWith(expect.objectContaining({ senderLogin: "imaustink" }));
    await server.close();
  });
});

describe("InvokeServer consumer-supplied tools (docs/adr/0035)", () => {
  /** A valid OpenAI `tools[]` entry. */
  function toolDef(name: string, description = "does a thing") {
    return {
      type: "function",
      function: { name, description, parameters: { type: "object", properties: { q: { type: "string" } } } },
    };
  }

  it("resolves the caller's tools onto the turn", async () => {
    const engine = fakeEngine({ result: "ok" });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    await chat(port, {
      messages: [{ role: "user", content: "what's the weather?" }],
      tools: [toolDef("get_weather", "Look up the weather")],
    });

    expect(engine.invoke).toHaveBeenCalledWith(
      expect.objectContaining({
        callerTools: [
          expect.objectContaining({ name: "get_weather", description: "Look up the weather" }),
        ],
      }),
    );
    await server.close();
  });

  it("leaves the turn input untouched when the caller sends no tools", async () => {
    const engine = fakeEngine({ result: "ok" });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    await chat(port, { messages: [{ role: "user", content: "hi" }] });

    expect(engine.invoke).toHaveBeenCalledWith({ request: "hi" });
    await server.close();
  });

  it("400s a malformed tools array instead of silently ignoring it", async () => {
    // A client that offers tools and silently never gets a tool call can't tell
    // "not chosen" from "never seen".
    const engine = fakeEngine();
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    const res = await chat(port, {
      messages: [{ role: "user", content: "hi" }],
      tools: [{ type: "function", function: { name: "bad name!" } }],
    });

    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { message: string } };
    expect(body.error.message).toContain("must match");
    expect(engine.invoke).not.toHaveBeenCalled();
    await server.close();
  });

  it("(non-streaming) returns tool_calls with finish_reason tool_calls", async () => {
    const server = new InvokeServer(
      fakeEngine({ pendingToolCalls: [{ id: "call_abc", name: "get_weather", arguments: '{"city":"Chicago"}' }] }),
    );
    const port = await listenOn(server);

    const res = await chat(port, { messages: [{ role: "user", content: "weather?" }], tools: [toolDef("get_weather")] });

    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      choices: { message: { content: unknown; tool_calls: { function: { name: string } }[] }; finish_reason: string }[];
    };
    expect(body.choices[0]!.finish_reason).toBe("tool_calls");
    expect(body.choices[0]!.message.content).toBeNull();
    expect(body.choices[0]!.message.tool_calls[0]!.function.name).toBe("get_weather");
    await server.close();
  });

  it("(streaming) emits a tool_calls delta then a tool_calls finish", async () => {
    const server = new InvokeServer(
      fakeEngine({ pendingToolCalls: [{ id: "call_abc", name: "get_weather", arguments: '{"city":"Chicago"}' }] }),
    );
    const port = await listenOn(server);

    const res = await chat(port, { stream: true, messages: [{ role: "user", content: "weather?" }], tools: [toolDef("get_weather")] });

    const withChoices = ((await readSse(res)) as SseChunk[]).filter((c) => c.choices);
    expect(withChoices[0]!.choices![0]!.delta.tool_calls).toEqual([
      { index: 0, id: "call_abc", type: "function", function: { name: "get_weather", arguments: '{"city":"Chicago"}' } },
    ]);
    expect(withChoices.at(-1)!.choices![0]!.finish_reason).toBe("tool_calls");
    await server.close();
  });

  it("passes a client-executed tool result back as seeded actionHistory", async () => {
    // The full round trip: the client ran our tool call and resent the
    // conversation with the result (docs/adr/0035 §1).
    const engine = fakeEngine({ result: "It's 58F and raining." });
    const server = new InvokeServer(engine);
    const port = await listenOn(server);

    await chat(port, {
      messages: [
        { role: "user", content: "weather in Chicago?" },
        {
          role: "assistant",
          content: null,
          tool_calls: [{ id: "call_abc", type: "function", function: { name: "get_weather", arguments: '{"city":"Chicago"}' } }],
        },
        { role: "tool", tool_call_id: "call_abc", content: "58F and raining" },
      ],
      tools: [toolDef("get_weather")],
    });

    expect(engine.invoke).toHaveBeenCalledWith(
      expect.objectContaining({
        request: "weather in Chicago?",
        actionHistory: [{ toolId: "caller:get_weather", toolArgs: '{"city":"Chicago"}', result: "58F and raining" }],
      }),
    );
    await server.close();
  });

  it("never reaches the engine — or emits a tool call — for an Open WebUI housekeeping request", async () => {
    // Open WebUI sends title/tag generation to the SAME endpoint with the same
    // body, tool array included. Emitting a tool call here would have the client
    // execute a real function as a side effect of rendering a chat title
    // (docs/adr/0035 §5).
    const engine = fakeEngine();
    const server = new InvokeServer(engine, undefined, {
      complete: vi.fn().mockResolvedValue("Chicago Weather"),
    });
    const port = await listenOn(server);

    const res = await chat(port, {
      messages: [{ role: "user", content: "### Task:\nGenerate a concise title" }],
      tools: [toolDef("get_weather")],
    });

    expect(engine.invoke).not.toHaveBeenCalled();
    const body = (await res.json()) as { choices: { message: { content: string }; finish_reason: string }[] };
    expect(body.choices[0]!.finish_reason).toBe("stop");
    expect(body.choices[0]!.message.content).toBe("Chicago Weather");
    await server.close();
  });

  it("consults the caller-tool store only above the top-K threshold", async () => {
    // Below it there is nothing to prune, so the JIT index must not be touched
    // at all (docs/adr/0035 §3).
    const callerToolStore = {
      index: vi.fn().mockResolvedValue(undefined),
      search: vi.fn().mockImplementation((_t: string, tools: unknown[], k: number) => Promise.resolve(tools.slice(0, k))),
      prune: vi.fn(),
    };
    const engine = fakeEngine({ result: "ok" });
    const server = new InvokeServer(engine, undefined, undefined, undefined, undefined, callerToolStore, 2);
    const port = await listenOn(server);

    const post = (tools: unknown[]): Promise<Response> => chat(port, { messages: [{ role: "user", content: "hi" }], tools });

    await post([toolDef("a"), toolDef("b")]);
    expect(callerToolStore.index).not.toHaveBeenCalled();

    await post([toolDef("a"), toolDef("b"), toolDef("c")]);
    expect(callerToolStore.index).toHaveBeenCalledTimes(1);
    expect(callerToolStore.search).toHaveBeenCalledWith("hi", expect.arrayContaining([expect.anything()]), 2);
    // Pruned to top-K before it ever reaches the engine.
    expect(engine.invoke.mock.lastCall![0].callerTools).toHaveLength(2);

    await server.close();
  });

  it("surfaces pendingToolCalls on GET /invoke/:id for a programmatic caller", async () => {
    const server = new InvokeServer(fakeEngine({ pendingToolCalls: [{ id: "call_abc", name: "get_weather", arguments: "{}" }] }));
    const port = await listenOn(server);

    const { id } = (await (await invoke(port, { request: "weather?", tools: [toolDef("get_weather")] })).json()) as { id: string };
    await vi.waitFor(async () => {
      const res = await fetch(`http://127.0.0.1:${port}/invoke/${id}`);
      const body = (await res.json()) as { status: string; pendingToolCalls?: { name: string }[] };
      expect(body.status).toBe("succeeded");
      expect(body.pendingToolCalls?.[0]?.name).toBe("get_weather");
    });

    await server.close();
  });
});

describe("InvokeServer accept-phase identity check (docs/adr/0049 §6)", () => {
  const SECRET = "assertion-secret";
  const agentTrigger: EventTriggerResource = {
    metadata: { name: "triage-as-sender" },
    spec: {
      target: { agentRef: "swe", promptTemplate: "triage", identity: "delegated" },
      reply: { github: { repository: "acme/widgets", issueNumber: "7", onIdentityError: true } },
    },
  };
  const claims = { login: "alice", accountId: "583231", subjectStatus: "unlinked" as const };

  async function post(engine: FakeEngine, trigger = agentTrigger): Promise<{ res: Response; server: InvokeServer }> {
    const server = new InvokeServer(engine, undefined, undefined, await triggerRegistry([trigger]), SECRET);
    const port = await listenOn(server);
    const res = await invoke(port, { event: envelope({ sender: { login: "alice" } }) }, {
      [SENDER_ASSERTION_HEADER]: mintSenderAssertion(SECRET, claims),
    });
    return { res, server };
  }

  const refusal: IdentityRefusal = {
    status: "failed",
    error: { code: "identity_link_required", message: "alice: link your GitHub account so swe can act as you: https://gw/x" },
  };

  it("answers 403 with the engine's identity error and the trigger's reply, and runs nothing", async () => {
    const engine = fakeEngine();
    engine.checkIdentity.mockResolvedValue(refusal);
    const { res, server } = await post(engine);

    expect(res.status).toBe(403);
    const body = (await res.json()) as { error: Record<string, unknown> };
    expect(body.error.code).toBe("identity_link_required");
    expect(body.error.trigger).toMatchObject({ name: "triage-as-sender", reply: { github: { onIdentityError: true } } });
    expect(engine.invoke).not.toHaveBeenCalled();

    // The check saw the turn as the engine would: forced agent, signed
    // override, and the gateway's full sender claims.
    expect(engine.checkIdentity).toHaveBeenCalledWith(
      expect.objectContaining({ forcedAgentId: "swe", identityOverride: "delegated", senderLogin: "alice", senderClaims: claims }),
    );
    await server.close();
  });

  it("accepts as before when the engine has no objection, carrying the claims into the run", async () => {
    const engine = fakeEngine();
    const { res, server } = await post(engine);
    expect(res.status).toBe(202);
    await tick();
    expect(engine.invoke).toHaveBeenCalledWith(expect.objectContaining({ senderClaims: claims }));
    await server.close();
  });

  it("does not check a trigger that names no agent", async () => {
    const engine = fakeEngine();
    engine.checkIdentity.mockResolvedValue(refusal);
    const { res, server } = await post(engine, catchAllTrigger);
    expect(res.status).toBe(202);
    expect(engine.checkIdentity).not.toHaveBeenCalled();
    await server.close();
  });
});

describe("InvokeServer trace context (docs/adr/0050 §4)", () => {
  it("continues a valid inbound traceparent into the turn (a child span of it), and roots a new trace for a malformed one", async () => {
    for (const [header, expectedTrace] of [
      ["00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "4bf92f3577b34da6a3ce929d0e0e4736"],
      ["not-a-traceparent", undefined],
    ] as const) {
      const engine = fakeEngine();
      const server = new InvokeServer(engine);
      const port = await listenOn(server);
      await fetch(`http://127.0.0.1:${port}/invoke`, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer t", traceparent: header },
        body: JSON.stringify({ request: "hi" }),
      });
      await tick();
      const arg = engine.invoke.mock.calls[0]![0];
      expect(arg.traceparent).toEqual(ANY_TRACEPARENT);
      // Never the inbound span itself: this hop is a span of its own.
      expect(arg.traceparent).not.toBe(header);
      if (expectedTrace) expect(traceIdOf(arg.traceparent)).toBe(expectedTrace);
      else expect(traceIdOf(arg.traceparent)).not.toBe("4bf92f3577b34da6a3ce929d0e0e4736");
      await server.close();
    }
  });

  it("with export on, hands the turn the context of an exported server span under the inbound one", async () => {
    const exporter = new InMemorySpanExporter();
    const stop = setupTelemetry("agent-orchestrator", { spanProcessor: new SimpleSpanProcessor(exporter) });
    try {
      const engine = fakeEngine();
      const server = new InvokeServer(engine);
      const port = await listenOn(server);
      const res = await fetch(`http://127.0.0.1:${port}/invoke`, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer t", traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" },
        body: JSON.stringify({ request: "hi" }),
      });
      const { id } = (await res.json()) as { id: string };
      await tick();
      const span = exporter.getFinishedSpans().find((s) => s.name === "POST /invoke");
      expect(span?.parentSpanContext?.spanId).toBe("00f067aa0ba902b7");
      expect(engine.invoke.mock.calls[0]![0].traceparent).toBe(`00-4bf92f3577b34da6a3ce929d0e0e4736-${span!.spanContext().spanId}-01`);
      expect(span?.attributes).toMatchObject({ "http.response.status_code": 202, "aktis.invocation_id": id });
      await server.close();
    } finally {
      await stop();
    }
  });
});
