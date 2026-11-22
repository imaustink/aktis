/**
 * The turn authentication gate (docs/adr/0053 follow-up 1), over real HTTP:
 * nothing about a turn runs until whoever started it is authenticated.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { InvokeServer } from "./server.js";
import { CallerAuthenticator, type CallerAuthMode } from "./rbac/caller-authenticator.js";
import type { IdentityResolver } from "./rbac/types.js";
import type { TurnEngine } from "./engine/types.js";
import type { TaskCompleter } from "./openai/task-completer.js";
import type { EventTriggerSelector } from "./routing/crd-event-trigger-registry.js";

const GATEWAY_TOKEN = "gw-token";
const GOOD_JWT = "good-user-jwt";

function resolverFor(tokens: Record<string, string>): IdentityResolver {
  return { resolve: async (token) => (token in tokens ? { subject: tokens[token]!, roles: ["reader"] } : undefined) };
}

function authenticator(mode: CallerAuthMode = "enforce"): CallerAuthenticator {
  return new CallerAuthenticator(resolverFor({ [GATEWAY_TOKEN]: "client-integration-gateway" }), resolverFor({ [GOOD_JWT]: "openwebui:alice" }), mode);
}

function fakeEngine(): TurnEngine & { invoke: ReturnType<typeof vi.fn>; checkIdentity: ReturnType<typeof vi.fn> } {
  return { invoke: vi.fn().mockResolvedValue({ result: "done" }), checkIdentity: vi.fn().mockResolvedValue(undefined) };
}

const servers: InvokeServer[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

async function start(engine: TurnEngine, auth: CallerAuthenticator, extra: { taskComplete?: ReturnType<typeof vi.fn>; select?: ReturnType<typeof vi.fn> } = {}): Promise<number> {
  const server = new InvokeServer(
    engine,
    undefined,
    extra.taskComplete ? ({ complete: extra.taskComplete } as unknown as TaskCompleter) : undefined,
    extra.select ? ({ select: extra.select } as unknown as EventTriggerSelector) : undefined,
    undefined,
    undefined,
    5,
    "authcode",
    undefined,
    auth,
  );
  servers.push(server);
  await server.listen(0);
  const address = server["server"]?.address();
  return typeof address === "object" && address ? address.port : 0;
}

function post(port: number, path: string, body: unknown, headers: Record<string, string>): Promise<Response> {
  return fetch(`http://127.0.0.1:${port}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", connection: "close", ...headers },
    body: JSON.stringify(body),
  });
}

const bearer = (token: string) => ({ authorization: `Bearer ${token}` });
const chatBody = (content = "hello") => ({ messages: [{ role: "user", content }] });

describe("POST /invoke caller authentication", () => {
  it.each([
    ["no Authorization header", {}],
    ["an unknown bearer token", bearer("not-a-known-token")],
    ["a non-bearer scheme", { authorization: `Basic ${GATEWAY_TOKEN}` }],
  ])("refuses %s with a 401 before the engine runs", async (_label, headers) => {
    const engine = fakeEngine();
    const port = await start(engine, authenticator());
    const res = await post(port, "/invoke", { request: "do something" }, headers);
    expect(res.status).toBe(401);
    expect(res.headers.get("www-authenticate")).toMatch(/^Bearer/);
    expect(await res.json()).toEqual({ error: "unauthorized: could not authenticate the caller" });
    expect(engine.checkIdentity).not.toHaveBeenCalled();
    expect(engine.invoke).not.toHaveBeenCalled();
  });

  it("accepts a bearer token the identity map resolves", async () => {
    const engine = fakeEngine();
    const port = await start(engine, authenticator());
    const res = await post(port, "/invoke", { request: "do something" }, bearer(GATEWAY_TOKEN));
    expect(res.status).toBe(202);
    await vi.waitFor(() => expect(engine.invoke).toHaveBeenCalledTimes(1));
  });

  it("refuses an unauthenticated event envelope before any trigger is matched", async () => {
    // The orchestrator signs a matched trigger's dispatch, so an envelope from
    // an unauthenticated caller must never reach selection.
    const select = vi.fn().mockReturnValue({ status: "ignored" });
    const engine = fakeEngine();
    const port = await start(engine, authenticator(), { select });
    const event = {
      source: "github",
      kind: "webhook",
      id: "delivery-1",
      time: "2026-10-13T00:00:00Z",
      headers: { "x-github-event": "issues" },
      body: { action: "labeled", repository: { full_name: "acme/widgets" } },
      sender: { login: "someone", isBot: false },
      repository: "acme/widgets",
    };
    const res = await post(port, "/invoke", { event }, bearer("forged"));
    expect(res.status).toBe(401);
    expect(select).not.toHaveBeenCalled();
    expect(engine.invoke).not.toHaveBeenCalled();

    // The control: the same envelope from an authenticated caller is a valid
    // one that does reach selection, so the refusal above is the gate's.
    const ok = await post(port, "/invoke", { event }, bearer(GATEWAY_TOKEN));
    expect(ok.status).toBe(200);
    expect(select).toHaveBeenCalledTimes(1);
  });

  it("does not take a forwarded-user JWT on /invoke: only the chat route reads it", async () => {
    const engine = fakeEngine();
    const port = await start(engine, authenticator());
    const res = await post(port, "/invoke", { request: "x" }, { "x-openwebui-user-jwt": GOOD_JWT });
    expect(res.status).toBe(401);
  });

  it("warn mode logs and lets the turn run", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const engine = fakeEngine();
    const port = await start(engine, authenticator("warn"));
    const res = await post(port, "/invoke", { request: "x" }, bearer("not-a-known-token"));
    expect(res.status).toBe(202);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("would reject POST /invoke: bearer token not recognized"));
    // The credential itself is never logged.
    expect(JSON.stringify(warn.mock.calls)).not.toContain("not-a-known-token");
  });

  it("fails closed when a resolver throws", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const throwing = new CallerAuthenticator({ resolve: () => Promise.reject(new Error("jwks down")) }, undefined);
    const engine = fakeEngine();
    const port = await start(engine, throwing);
    const res = await post(port, "/invoke", { request: "x" }, bearer(GATEWAY_TOKEN));
    expect(res.status).toBe(401);
    expect(engine.invoke).not.toHaveBeenCalled();
  });
});

describe("POST /v1/chat/completions caller authentication", () => {
  it("refuses an unknown bearer with no user JWT, in OpenAI's error shape", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const engine = fakeEngine();
    const port = await start(engine, authenticator());
    const res = await post(port, "/v1/chat/completions", chatBody(), bearer("not-a-known-token"));
    expect(res.status).toBe(401);
    expect(await res.json()).toEqual({
      error: { message: "unauthorized: could not authenticate the caller", type: "invalid_request_error", code: "invalid_api_key" },
    });
    expect(engine.invoke).not.toHaveBeenCalled();
  });

  it("authenticates by a verified Open WebUI user JWT, with or without a bearer", async () => {
    const engine = fakeEngine();
    const port = await start(engine, authenticator());
    expect((await post(port, "/v1/chat/completions", chatBody(), { "x-openwebui-user-jwt": GOOD_JWT })).status).toBe(200);
    expect(engine.invoke).toHaveBeenCalledTimes(1);
  });

  it("refuses a forged user JWT even alongside a valid bearer", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const engine = fakeEngine();
    const port = await start(engine, authenticator());
    const res = await post(port, "/v1/chat/completions", chatBody(), { ...bearer(GATEWAY_TOKEN), "x-openwebui-user-jwt": "forged" });
    expect(res.status).toBe(401);
    expect(engine.invoke).not.toHaveBeenCalled();
  });

  it("gates Open WebUI's internal housekeeping completions too", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const taskComplete = vi.fn().mockResolvedValue("A title");
    const port = await start(fakeEngine(), authenticator(), { taskComplete });
    const res = await post(port, "/v1/chat/completions", chatBody("### Task:\nGenerate a concise title"), bearer("nope"));
    expect(res.status).toBe(401);
    expect(taskComplete).not.toHaveBeenCalled();
  });
});
