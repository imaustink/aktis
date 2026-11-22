import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";
import type { Server } from "node:http";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

// The REAL broker, driven with requests shaped exactly like the Go engine's
// (the only client since docs/adr/0053) — the two halves of MCP support agree
// with nothing between them but a URL, a header and two JSON shapes.
import { createMcpBrokerServer, DELEGATED_TOKEN_HEADER } from "../../orchestrator/apps/mcp-broker/src/server.js";
import { StaticMCPServerRegistry } from "../../orchestrator/apps/mcp-broker/src/mcpserver-registry.js";
import { Discovery } from "../../orchestrator/apps/mcp-broker/src/discovery.js";
import { MCPToolWriter, type CustomObjectsWriterApi } from "../../orchestrator/apps/mcp-broker/src/mcptool-writer.js";
import { MCPServerStatusWriter, type StatusPatcherApi } from "../../orchestrator/apps/mcp-broker/src/mcpserver-status.js";
import {
  McpTransportError,
  type DiscoveredTool,
  type McpClient,
  type ToolCallResult,
} from "../../orchestrator/apps/mcp-broker/src/mcp-client.js";
import {
  catalogIdFor,
  GROUP,
  SERVER_LABEL,
  VERSION,
  type MCPServerCustomResource,
  type MCPToolCustomResource,
} from "../../orchestrator/apps/mcp-broker/src/mcp-server-resource.js";

/**
 * MCP tool support across its components and two languages, driven against the
 * REAL broker (ADR 0045). The broker server (TypeScript), its discovery writer,
 * and the Go engine's dispatch activity and catalog reader implement one
 * contract — a URL, an `x-delegated-token` header, a `{arguments}` request, a
 * `{result,isError}` reply, and the MCPTool CR's spec fields — and nothing but
 * agreement makes a tool call reach a server and come back. This is the same
 * guard `broker-contract.e2e.ts` keeps over the connection-broker, for the same
 * reason: every past break in this repo was a cross-component rename that a unit
 * suite, mocking the other half, waved through. It needs no cluster: the MCP
 * server is a fake and the question is purely whether the halves of this repo
 * agree.
 *
 * Four boundaries:
 *   1. The broker serves the route the Go engine calls (route, auth, which
 *      credential is spent, errors), driven with Go-shaped requests.
 *   2. Discovery's WRITE of a derived MCPTool carries the fields the Go
 *      catalog reader decodes.
 *   3. The existence rule (§6) through the real writer.
 *   4. The Go engine's source addresses that same route and reply.
 */

const NAMESPACE = "e2e";
const ORCHESTRATOR_TOKEN = "broker-secret";
const SERVICE_TOKEN = "discovery-service-cred";
const DELEGATED = "alice-delegated-token";
const SERVER = "github-mcp";
// Derived, not hardcoded: the broker hashes a lossy remote name into its id.
const SEARCH_ID = catalogIdFor(SERVER, { remoteToolName: "search_issues", allowedRoles: [] });
const SECRET_ID = catalogIdFor(SERVER, { remoteToolName: "secret_op", allowedRoles: [] });

/** The remote tools the fake MCP server advertises; mutable for the §6 pass. */
interface FakeState {
  tools: DiscoveredTool[];
  reachable: boolean;
  calls: Array<{ url: string; token?: string; name: string; arguments: Record<string, unknown> }>;
}

/**
 * A fake MCP server, injected where the real broker would hold an SdkMcpClient.
 * It records what the broker asked and with which token — which is how a test
 * proves the broker spent the CALLER's token, not the service credential.
 */
function fakeMcpClient(state: FakeState): McpClient {
  return {
    async listTools(opts) {
      if (!state.reachable) throw new McpTransportError(`unreachable: ${opts.url}`);
      return state.tools;
    },
    async callTool(opts): Promise<ToolCallResult> {
      state.calls.push(opts);
      if (opts.name === "explode") {
        return { content: [{ type: "text", text: "the repo does not exist" }], isError: true };
      }
      return {
        content: [{ type: "text", text: `echo:${JSON.stringify(opts.arguments)}` }],
        isError: false,
      };
    },
  };
}

/** An in-memory stand-in for the custom-objects API the broker writes through. */
class FakeK8s implements CustomObjectsWriterApi, StatusPatcherApi {
  readonly tools = new Map<string, MCPToolCustomResource>();
  readonly statuses = new Map<string, Record<string, unknown>>();

  async listNamespacedCustomObject(): Promise<{ items?: unknown[] }> {
    return { items: [...this.tools.values()] };
  }
  async createNamespacedCustomObject(args: { body: unknown }): Promise<unknown> {
    const body = args.body as MCPToolCustomResource;
    this.tools.set(body.metadata.name, body);
    return body;
  }
  async patchNamespacedCustomObject(args: { name: string; body: unknown }): Promise<unknown> {
    const patch = args.body as MCPToolCustomResource;
    this.tools.set(args.name, { ...this.tools.get(args.name), ...patch } as MCPToolCustomResource);
    return patch;
  }
  async deleteNamespacedCustomObject(args: { name: string }): Promise<unknown> {
    this.tools.delete(args.name);
    return {};
  }
  async patchNamespacedCustomObjectStatus(args: { name: string; body: unknown }): Promise<unknown> {
    this.statuses.set(args.name, (args.body as { status: Record<string, unknown> }).status);
    return {};
  }
}

/** An MCPServer CR the broker acts on. */
function serverCR(opts: {
  identityProviders?: string[];
  exposure?: MCPServerCustomResource["spec"]["exposure"];
}): MCPServerCustomResource {
  return {
    apiVersion: `${GROUP}/${VERSION}`,
    kind: "MCPServer",
    metadata: { name: SERVER, namespace: NAMESPACE, uid: "uid-1", generation: 1 },
    spec: {
      transport: "streamable-http",
      url: "http://fake-mcp.e2e.svc:8080/mcp",
      identityProviders: opts.identityProviders,
      exposure: opts.exposure,
    },
  };
}

const caller = { subject: "openwebui:alice" };

describe("the broker serves the route the Go engine calls", () => {
  let server: Server;
  let brokerUrl: string;
  let state: FakeState;

  const exposure = [
    { remoteToolName: "search_issues", allowedRoles: ["engineering"] },
    { remoteToolName: "explode", allowedRoles: ["engineering"] },
  ];

  async function startBroker(identityProviders: string[] | undefined): Promise<void> {
    server = createMcpBrokerServer({
      auth: { orchestratorToken: ORCHESTRATOR_TOKEN },
      registry: new StaticMCPServerRegistry([
        { name: SERVER, cr: serverCR({ identityProviders, exposure }), serviceToken: SERVICE_TOKEN },
      ]),
      client: fakeMcpClient(state),
    });
    await new Promise<void>((resolve) => server.listen(0, () => resolve()));
    brokerUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  }

  /** One tools/call exactly as mcptool.go's callThroughBroker sends it. */
  function call(
    tool: string,
    args: Record<string, unknown>,
    opts: { delegated?: string; brokerToken?: string; server?: string } = {},
  ): Promise<Response> {
    return fetch(
      `${brokerUrl}/servers/${encodeURIComponent(opts.server ?? SERVER)}/tools/${encodeURIComponent(tool)}/call`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${opts.brokerToken ?? ORCHESTRATOR_TOKEN}`,
          ...(opts.delegated ? { [DELEGATED_TOKEN_HEADER]: opts.delegated } : {}),
        },
        body: JSON.stringify({ arguments: args }),
      },
    );
  }

  beforeEach(() => {
    state = {
      reachable: true,
      calls: [],
      tools: [
        { name: "search_issues", description: "Search issues", inputSchema: { type: "object" } },
        { name: "explode", description: "Always errors" },
      ],
    };
  });

  afterEach(() => server?.close());

  it("proxies a call, spending the caller's delegated token upstream", async () => {
    await startBroker(["github"]);

    const res = await call("search_issues", { query: "is:open" }, { delegated: DELEGATED });

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ result: `echo:${JSON.stringify({ query: "is:open" })}`, isError: false });
    // The broker spent the CALLER's delegated token upstream — never the
    // discovery/service credential (ADR 0045 §5).
    expect(state.calls).toHaveLength(1);
    expect(state.calls[0]).toMatchObject({
      token: DELEGATED,
      name: "search_issues",
      arguments: { query: "is:open" },
    });
  });

  it("spends the service credential for a no-identity server called with no delegated header", async () => {
    await startBroker(undefined); // server declares no identityProviders

    const res = await call("search_issues", {});

    expect(res.status).toBe(200);
    expect(state.calls[0]!.token, "a no-identity server runs on the service credential").toBe(SERVICE_TOKEN);
  });

  it("returns a tool-level error as isError, not a failed request", async () => {
    await startBroker(["github"]);

    const res = await call("explode", {}, { delegated: DELEGATED });

    expect(res.status).toBe(200);
    const body = (await res.json()) as { result: string; isError: boolean };
    expect(body.isError).toBe(true);
    expect(body.result).toContain("the repo does not exist");
  });

  it("refuses an unexposed tool with a 404, never reaching the server", async () => {
    await startBroker(["github"]);

    // Advertised by the server but NOT in the exposure map → the broker refuses.
    const res = await call("delete_repo", {}, { delegated: DELEGATED });

    expect(res.status).toBe(404);
    expect(state.calls, "an unexposed tool never reaches the server").toHaveLength(0);
  });

  it("refuses an unknown server with a 404", async () => {
    await startBroker(["github"]);

    const res = await call("search_issues", {}, { delegated: DELEGATED, server: "no-such-server" });

    expect(res.status).toBe(404);
  });

  it("rejects a bad broker token with a 401", async () => {
    await startBroker(["github"]);

    const res = await call("search_issues", {}, { delegated: DELEGATED, brokerToken: "wrong-token" });

    expect(res.status).toBe(401);
    expect(state.calls).toHaveLength(0);
  });
});

describe("discovery's write and the engine's read agree", () => {
  const exposure = [
    { remoteToolName: "search_issues", allowedRoles: ["engineering", "support"], tier: "standard" },
    { remoteToolName: "secret_op", allowedRoles: ["engineering"], hidden: true },
    // Exposed but the server does not advertise it — materializes nothing.
    { remoteToolName: "ghost_tool", allowedRoles: ["engineering"] },
    // Advertised but withdrawn — in discoveredTools as exposed:false, no CR.
    { remoteToolName: "list_repos", expose: false, allowedRoles: ["engineering"] },
  ];

  function discovery(k8s: FakeK8s, client: McpClient): Discovery {
    return new Discovery({
      client,
      toolWriter: new MCPToolWriter({ api: k8s, namespace: NAMESPACE, group: GROUP, version: VERSION }),
      statusWriter: new MCPServerStatusWriter({
        api: k8s,
        namespace: NAMESPACE,
        group: GROUP,
        version: VERSION,
        now: () => new Date("2026-10-02T00:00:00Z"),
      }),
    });
  }

  function liveTools(): DiscoveredTool[] {
    return [
      { name: "search_issues", description: "Search issues", inputSchema: { type: "object" } },
      { name: "secret_op", description: "Privileged" },
      { name: "list_repos", description: "List repositories" },
    ];
  }

  it("materializes only exposed + live tools, sourcing each field correctly", async () => {
    const k8s = new FakeK8s();
    const state: FakeState = { reachable: true, calls: [], tools: liveTools() };
    const server = serverCR({ identityProviders: ["github"], exposure });

    await discovery(k8s, fakeMcpClient(state)).runServer({
      name: SERVER,
      cr: server,
      serviceToken: SERVICE_TOKEN,
    });

    // search_issues and secret_op are materialized; ghost_tool (not advertised)
    // and list_repos (expose:false) are not.
    expect([...k8s.tools.keys()].sort()).toEqual([
      SEARCH_ID,
      SECRET_ID,
    ]);

    const search = k8s.tools.get(SEARCH_ID)!;
    // description/inputSchema from the LIVE tool; roles/tier from the EXPOSURE;
    // identityProviders from the SERVER.
    expect(search.spec).toMatchObject({
      serverRef: SERVER,
      remoteToolName: "search_issues",
      description: "Search issues",
      inputSchema: JSON.stringify({ type: "object" }),
      allowedRoles: ["engineering", "support"],
      tier: "standard",
      identityProviders: ["github"],
    });
    // Owned by its server (cascade delete) and labelled (listable).
    expect(search.metadata.ownerReferences?.[0]).toMatchObject({ kind: "MCPServer", name: SERVER, controller: true });
    expect(search.metadata.labels?.[SERVER_LABEL]).toBe(SERVER);

    // A hidden exposure carries hidden through.
    expect(k8s.tools.get(SECRET_ID)!.spec.hidden).toBe(true);

    // The status is a REPORT of everything advertised, each marked exposed per
    // the map — a grant of nothing.
    const status = k8s.statuses.get(SERVER) as {
      discoveredTools: Array<{ name: string; exposed: boolean }>;
      exposedTools: number;
    };
    expect(status.exposedTools).toBe(2);
    const exposedByName = new Map(status.discoveredTools.map((t) => [t.name, t.exposed]));
    expect(exposedByName.get("search_issues")).toBe(true);
    expect(exposedByName.get("list_repos")).toBe(false); // advertised, withdrawn
    expect(exposedByName.has("secret_op")).toBe(true);

    // The engine READS exactly what the broker WROTE: every spec field the
    // broker sets is one the Go catalog reader decodes (decode.go's
    // mcpToolSpec). Read as source, like the dispatch checks below.
    const decodeGo = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "..", "..", "orchestrator/engines/temporal/internal/catalog/decode.go"),
      "utf8",
    );
    const mcpToolSpecGo = decodeGo.slice(decodeGo.indexOf("type mcpToolSpec struct"));
    for (const field of Object.keys(search.spec)) {
      expect(mcpToolSpecGo, `Go reader decodes spec.${field}`).toMatch(new RegExp(`json:"${field}[",]`));
    }
  });

  it("deletes a materialized tool the server stops advertising (the existence rule §6)", async () => {
    const k8s = new FakeK8s();
    const state: FakeState = { reachable: true, calls: [], tools: liveTools() };
    const server = serverCR({ identityProviders: ["github"], exposure });
    const disco = discovery(k8s, fakeMcpClient(state));

    await disco.runServer({ name: SERVER, cr: server, serviceToken: SERVICE_TOKEN });
    expect(k8s.tools.has(SEARCH_ID)).toBe(true);

    // The server drops search_issues from its live list; the next pass removes
    // the derived tool rather than leaving a tombstone that fails when invoked.
    state.tools = state.tools.filter((t) => t.name !== "search_issues");
    await disco.runServer({ name: SERVER, cr: server, serviceToken: SERVICE_TOKEN });

    expect(k8s.tools.has(SEARCH_ID)).toBe(false);
    expect(k8s.tools.has(SECRET_ID), "the still-live tool stays").toBe(true);
  });

  it("marks a server Degraded and retains its tools when it is unreachable", async () => {
    const k8s = new FakeK8s();
    const state: FakeState = { reachable: true, calls: [], tools: liveTools() };
    const server = serverCR({ identityProviders: ["github"], exposure });
    const disco = discovery(k8s, fakeMcpClient(state));

    await disco.runServer({ name: SERVER, cr: server, serviceToken: SERVICE_TOKEN });
    const before = new Set(k8s.tools.keys());
    expect(before.size).toBe(2);

    // Unreachable is "we do not know", not "nothing exists": the tools stay.
    state.reachable = false;
    await disco.runServer({ name: SERVER, cr: server, serviceToken: SERVICE_TOKEN });

    expect(new Set(k8s.tools.keys())).toEqual(before);
    const status = k8s.statuses.get(SERVER) as { conditions: Array<{ type: string; status: string }> };
    expect(status.conditions[0]).toMatchObject({ type: "Ready", status: "False" });
  });
});

/**
 * The Go engine cannot be driven from here, so its dispatch path is read.
 *
 * Crude on purpose, exactly as `broker-contract.e2e.ts` reads the Go corpus
 * broker: `mcptool.go` is the broker's client and has to address a
 * TypeScript server, with nothing between them but this convention. The string
 * IS the contract; the alternative to checking it is not checking it.
 */
describe("the Go engine addresses the same broker route and reply", () => {
  const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
  const mcptoolGo = readFileSync(
    join(repoRoot, "orchestrator/engines/temporal/internal/temporal/activities/mcptool.go"),
    "utf8",
  );

  it("builds the /servers/:server/tools/:tool/call route the broker serves", () => {
    expect(mcptoolGo).toContain("/servers/");
    expect(mcptoolGo).toContain("/tools/");
    expect(mcptoolGo).toContain("/call");
  });

  it("forwards the caller's token in the x-delegated-token header", () => {
    expect(mcptoolGo).toContain("x-delegated-token");
  });

  it("reads the broker's {result,isError} reply", () => {
    expect(mcptoolGo).toMatch(/json:"result"/);
    expect(mcptoolGo).toMatch(/json:"isError"/);
  });

  it("fails closed on a missing delegated token rather than falling back", () => {
    // A NeedsLink path keyed on IdentityProviders: an unlinked caller gets a
    // link prompt, never a call on some other credential.
    expect(mcptoolGo).toContain("IdentityProviders");
    expect(mcptoolGo).toMatch(/NeedsLink/);
  });
});
