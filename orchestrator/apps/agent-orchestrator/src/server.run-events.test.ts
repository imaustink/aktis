// /invoke behaviour added by docs/adr/0052 (run-completion events): the 202's
// trigger params/onComplete, target.none, and the run outcome on the poll.

import { afterEach, describe, expect, it, vi } from "vitest";
import { SENDER_ASSERTION_HEADER, mintSenderAssertion } from "./rbac/sender-assertion.js";
import { InMemoryInvocationStore } from "./invocation/types.js";
import { MAX_OUTCOME_BYTES, checkOutcome } from "./invocation/outcome.js";
import { InvokeServer, type AgentGraphLike, type InvocationRecord } from "./server.js";
import type { AgentState } from "./agent/graph.js";
import type { Envelope, EventTriggerResource } from "@aktis/event-triggers";
import { CrdEventTriggerRegistry } from "./routing/crd-event-trigger-registry.js";

const nativeFetch = globalThis.fetch;
// Fresh connection per request so server.close() never waits on keep-alive.
const fetch: typeof globalThis.fetch = (input, init = {}) => {
  const headers = new Headers(init.headers);
  headers.set("connection", "close");
  return nativeFetch(input, { ...init, headers });
};

function runEnvelope(overrides: Partial<Envelope> = {}): Envelope {
  return {
    source: "runs",
    kind: "run",
    id: "run-evt-1",
    time: "2026-10-13T00:00:00Z",
    headers: {},
    body: {
      trigger: "github-issue-triage",
      status: "succeeded",
      params: { repository: "acme/widgets", number: "7" },
      outcome: { pullRequest: { number: 12, url: "https://github.com/acme/widgets/pull/12" }, madeNewCommits: true },
      chain: { id: "d-1", depth: 1, counts: { "github-issue-triage": 1 }, origin: { login: "alice", repository: "acme/widgets" } },
    },
    sender: { login: "alice", isBot: false },
    repository: "acme/widgets",
    ...overrides,
  };
}

async function triggerRegistry(triggers: EventTriggerResource[]): Promise<CrdEventTriggerRegistry> {
  const ready = triggers.map((t) => ({ ...t, status: { conditions: [{ type: "Ready", status: "True" as const }] } }));
  const registry = new CrdEventTriggerRegistry("default", "core.aktis.ai", "v1alpha1", {
    listNamespacedCustomObject: vi.fn().mockResolvedValue({ items: ready }),
  });
  await registry.listAll();
  return registry;
}

async function* noStream(): AsyncGenerator<Record<string, Partial<AgentState>>> {}

const servers: InvokeServer[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

async function start(server: InvokeServer): Promise<number> {
  servers.push(server);
  await server.listen(0);
  const address = server["server"]?.address();
  return typeof address === "object" && address ? address.port : 0;
}

async function postEvent(port: number, event: Envelope, headers: Record<string, string> = {}): Promise<Response> {
  return fetch(`http://127.0.0.1:${port}/invoke`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer tok-1", ...headers },
    body: JSON.stringify({ event }),
  });
}

async function poll(port: number, id: string): Promise<InvocationRecord> {
  const res = await fetch(`http://127.0.0.1:${port}/invoke/${id}`);
  expect(res.status).toBe(200);
  return (await res.json()) as InvocationRecord;
}

/** Polls until the record leaves "pending" (the graph runs fire-and-forget). */
async function settled(port: number, id: string): Promise<InvocationRecord> {
  for (let i = 0; i < 100; i++) {
    const record = await poll(port, id);
    if (record.status !== "pending") return record;
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error(`invocation ${id} never settled`);
}

function graphReturning(state: Partial<AgentState>): AgentGraphLike {
  return {
    invoke: vi.fn().mockResolvedValue({ request: "x", authToken: "", skillCandidates: [], result: "done", ...state } as AgentState),
    stream: vi.fn().mockResolvedValue(noStream()),
  };
}

const reviewTrigger: EventTriggerResource = {
  metadata: { name: "factory-review" },
  spec: {
    sourceRef: "runs",
    match: { cel: 'kind == "run" && has(body.outcome.pullRequest)' },
    params: { number: "body.outcome.pullRequest.number", repository: "body.params.repository" },
    target: { agentRef: "claude-code-swe-agent", readOnly: true, promptTemplate: "Review {{repository}}#{{number}}" },
    reply: { github: { repository: "{{repository}}", issueNumber: "{{number}}", addLabels: ["factory:reviewing"] } },
    onComplete: { emitTo: "runs" },
  },
};

const noneTrigger: EventTriggerResource = {
  metadata: { name: "factory-done" },
  spec: {
    sourceRef: "runs",
    params: { number: "body.outcome.pullRequest.number", repository: "body.params.repository", assignee: "sender.login" },
    target: { none: {} },
    reply: {
      github: { repository: "{{repository}}", issueNumber: "{{number}}", addLabels: ["factory:ready"], assignees: ["{{assignee}}"] },
    },
  },
};

describe("/invoke 202 trigger: params and onComplete (docs/adr/0052 §1)", () => {
  it("returns the string params, onComplete and the rendered addLabels", async () => {
    const port = await start(new InvokeServer(graphReturning({}), undefined, undefined, await triggerRegistry([reviewTrigger])));
    const res = await postEvent(port, runEnvelope());
    expect(res.status).toBe(202);
    expect(((await res.json()) as { trigger: unknown }).trigger).toEqual({
      name: "factory-review",
      sessionId: "event:runs:run-evt-1",
      reply: { github: { repository: "acme/widgets", issueNumber: 12, addLabels: ["factory:reviewing"] } },
      params: { number: "12", repository: "acme/widgets" },
      onComplete: { emitTo: "runs" },
    });
  });

  it("omits onComplete when the trigger does not set it", async () => {
    const { onComplete: _drop, ...spec } = reviewTrigger.spec;
    const port = await start(
      new InvokeServer(graphReturning({}), undefined, undefined, await triggerRegistry([{ metadata: reviewTrigger.metadata, spec }])),
    );
    const body = (await (await postEvent(port, runEnvelope())).json()) as { trigger: Record<string, unknown> };
    expect(body.trigger).not.toHaveProperty("onComplete");
    expect(body.trigger.params).toEqual({ number: "12", repository: "acme/widgets" });
  });

  it("rejects an unknown envelope kind but accepts run", async () => {
    const port = await start(new InvokeServer(graphReturning({}), undefined, undefined, await triggerRegistry([reviewTrigger])));
    const bad = await postEvent(port, runEnvelope({ kind: "bogus" as Envelope["kind"] }));
    expect(bad.status).toBe(400);
    expect(((await bad.json()) as { error: string }).error).toContain('"run"');
  });
});

describe("/invoke target.none (docs/adr/0052 §3)", () => {
  it("accepts with 202 + id and runs nothing; the poll is already succeeded with an empty result", async () => {
    const graph = graphReturning({});
    const port = await start(new InvokeServer(graph, undefined, undefined, await triggerRegistry([noneTrigger])));
    const res = await postEvent(port, runEnvelope());
    expect(res.status).toBe(202);
    const body = (await res.json()) as { id: string; status: string; trigger: Record<string, unknown> };
    expect(body.status).toBe("pending");
    expect(body.trigger).toEqual({
      name: "factory-done",
      sessionId: "event:runs:run-evt-1",
      reply: { github: { repository: "acme/widgets", issueNumber: 12, addLabels: ["factory:ready"], assignees: ["alice"] } },
      params: { assignee: "alice", number: "12", repository: "acme/widgets" },
    });
    // Immediately terminal: no wait, no graph.
    expect(await poll(port, body.id)).toEqual({ id: body.id, status: "succeeded", result: "" });
    await new Promise((r) => setTimeout(r, 10));
    expect(graph.invoke).not.toHaveBeenCalled();
    expect(graph.stream).not.toHaveBeenCalled();
  });

  it("never runs the identity pre-flight, so a none trigger cannot 403", async () => {
    const SECRET = "assertion-secret";
    const checkIdentity = vi.fn().mockResolvedValue({
      status: "failed",
      error: { code: "identity_link_required", message: "link first" },
    });
    const graph: AgentGraphLike = { invoke: vi.fn(), stream: vi.fn(), checkIdentity };
    const port = await start(new InvokeServer(graph, undefined, undefined, await triggerRegistry([noneTrigger]), undefined, SECRET));
    const res = await postEvent(port, runEnvelope(), {
      [SENDER_ASSERTION_HEADER]: mintSenderAssertion(SECRET, { login: "alice", accountId: "1", subjectStatus: "unlinked" }),
    });
    expect(res.status).toBe(202);
    expect(checkIdentity).not.toHaveBeenCalled();
    expect(graph.invoke).not.toHaveBeenCalled();
  });
});

describe("GET /invoke/:id outcome (docs/adr/0052 §2)", () => {
  const outcome = { pullRequest: { number: 12, url: "https://github.com/acme/widgets/pull/12" }, madeNewCommits: true };

  async function runTurn(trigger: EventTriggerResource, state: Partial<AgentState>): Promise<InvocationRecord> {
    const port = await start(
      new InvokeServer(
        graphReturning(state), undefined, undefined, await triggerRegistry([trigger]),
        undefined, undefined, undefined, 5, "authcode", new InMemoryInvocationStore(),
      ),
    );
    const res = await postEvent(port, runEnvelope());
    expect(res.status).toBe(202);
    return settled(port, ((await res.json()) as { id: string }).id);
  }

  it("surfaces a forced agentRef run's outcome next to the unchanged result", async () => {
    const record = await runTurn(reviewTrigger, { result: "Reviewed.", agentOutcome: outcome });
    expect(record).toMatchObject({ status: "succeeded", result: "Reviewed.", outcome });
  });

  it("omits outcome when the run reported none", async () => {
    const record = await runTurn(reviewTrigger, { result: "Reviewed." });
    expect(record.status).toBe("succeeded");
    expect(record).not.toHaveProperty("outcome");
  });

  it("omits outcome on a failed turn", async () => {
    const record = await runTurn(reviewTrigger, { error: "boom", agentOutcome: outcome });
    expect(record.status).toBe("failed");
    expect(record).not.toHaveProperty("outcome");
  });

  it("omits outcome when the trigger forced no agent", async () => {
    const skillTrigger: EventTriggerResource = {
      metadata: { name: "skill" },
      spec: { target: { skillRef: "s", promptTemplate: "p" } },
    };
    const record = await runTurn(skillTrigger, { agentOutcome: outcome });
    expect(record).not.toHaveProperty("outcome");
  });

  it.each([
    ["an array", [1, 2]],
    ["a string", "pr 12"],
    ["null", null],
    ["oversized", { blob: "x".repeat(MAX_OUTCOME_BYTES) }],
  ])("drops and logs an outcome that is %s", async (_, bad) => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const record = await runTurn(reviewTrigger, { result: "Reviewed.", agentOutcome: bad });
      expect(record).toMatchObject({ status: "succeeded", result: "Reviewed." });
      expect(record).not.toHaveProperty("outcome");
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("dropping run outcome"));
    } finally {
      warn.mockRestore();
    }
  });
});

describe("checkOutcome", () => {
  it("accepts an object at exactly the byte limit and refuses one byte more", () => {
    // {"k":"<pad>"} is 8 bytes of framing.
    const at = { k: "x".repeat(MAX_OUTCOME_BYTES - 8) };
    expect(Buffer.byteLength(JSON.stringify(at))).toBe(MAX_OUTCOME_BYTES);
    expect(checkOutcome(at)).toEqual({ ok: true, outcome: at });
    expect(checkOutcome({ k: "x".repeat(MAX_OUTCOME_BYTES - 7) }).ok).toBe(false);
  });
  it("measures UTF-8 bytes, not UTF-16 code units", () => {
    // "é" is one code unit but two bytes: this is under the limit in .length
    // and over it in bytes.
    const outcome = { k: "é".repeat((MAX_OUTCOME_BYTES - 8) / 2 + 1) };
    expect(JSON.stringify(outcome).length).toBeLessThan(MAX_OUTCOME_BYTES);
    expect(checkOutcome(outcome).ok).toBe(false);
  });
  it("refuses non-objects and values that do not serialize to an object", () => {
    for (const bad of [undefined, null, 1, "s", true, [], new Date(0)]) expect(checkOutcome(bad).ok).toBe(false);
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(checkOutcome(cyclic).ok).toBe(false);
  });
});
