import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";
import type { Server } from "node:http";
import { createHmac } from "node:crypto";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { createBrokerServer, DELEGATED_TOKEN_HEADER } from "../../orchestrator/apps/connection-broker/src/server.js";
import { StaticCorpusRegistry } from "../../orchestrator/apps/connection-broker/src/registry.js";
import { SlackDriver } from "../../orchestrator/apps/connection-broker/src/drivers/slack.js";
import { runGoTestsAsync } from "../support/go-test.js";

/**
 * The connection-broker's routes, driven for real, against the clients that call them.
 *
 * The broker (TypeScript) and its clients — the Go corpus-sync worker's
 * `HTTPSource` (docs/adr/0059) and the Temporal engine's Go `internal/corpus` package,
 * the only retrieval client since docs/adr/0053 removed the in-process engine
 * — agree on nothing but a convention. That agreement has broken twice, both
 * times silently, both times in a release that looked complete:
 *
 *   - The sync worker's client (then TypeScript) asked for `/connections/:name/resources` after
 *     ADR 0043 moved data to `/corpora/`. Every sync request 404'd, so the
 *     write path could not list or fetch anything at all.
 *   - The retrieval prober asked for `/connections/:name/probe` for the same
 *     reason, and read the 404 as a DENIAL, so retrieval answered "No passages
 *     matched" — indistinguishable from an empty corpus.
 *
 * Both survived a full unit suite, because the unit tests asserted the URL each
 * client BUILT. So the sync worker's Go client is driven against the real
 * server here (through `go test`), the routes the Go client calls are exercised with plain requests
 * shaped like its own, and the Go source is checked for the same paths.
 *
 * Like `rbac-parity`, this needs NO cluster and NO credentials — the drivers
 * are fakes and the question is purely whether the two halves of this repo
 * agree. It should turn red in seconds on a laptop rather than after a deploy,
 * which is why it runs in CI while the cluster specs do not.
 */

const CORPUS = "globex-confluence";
const ORCHESTRATOR_TOKEN = "orchestrator-secret";
const SYNC_TOKEN = "sync-secret";

/** A driver that answers everything, so a 404 can only mean a routing fault. */
function fakeDriver() {
  return {
    provider: "confluence",
    validateScope: () => {},
    list: vi.fn().mockResolvedValue({
      resources: [{ id: "1", title: "Runbook", url: "https://wiki/1" }],
      cursor: undefined,
    }),
    fetch: vi.fn().mockResolvedValue({
      id: "1",
      title: "Runbook",
      url: "https://wiki/1",
      markdown: "body",
    }),
    probeGranularity: () => "resource" as const,
    probe: vi.fn().mockResolvedValue({
      allowed: true,
      title: "Runbook",
      url: "https://wiki/1",
      version: "v1",
    }),
    readAsUser: vi.fn().mockResolvedValue({
      id: "1",
      title: "Runbook",
      url: "https://wiki/1",
      markdown: "live body",
    }),
    searchAsUser: vi.fn().mockResolvedValue([
      { id: "1", title: "Runbook", url: "https://wiki/1", excerpt: "deploy steps" },
    ]),
  };
}

let server: Server;
let baseUrl: string;
let driver: ReturnType<typeof fakeDriver>;

beforeAll(async () => {
  driver = fakeDriver();
  server = createBrokerServer({
    auth: {
      orchestratorToken: ORCHESTRATOR_TOKEN,
      syncTokens: new Map([[CORPUS, SYNC_TOKEN]]),
    },
    registry: new StaticCorpusRegistry([
      {
        name: CORPUS,
        connection: "bitovi-confluence",
        driver: driver as never,
        scope: { space: "GLOBEX" },
        allowedRoles: ["reader"],
        serviceToken: "service-cred",
      },
    ]),
  });
  await new Promise<void>((resolve) => server.listen(0, () => resolve()));
  baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(() => server?.close());

/** The caller's own token. Its value is irrelevant; that one is SENT is not. */
const DELEGATED = "user-token";

describe("the sync worker's client", () => {
  // corpus-sync is Go (docs/adr/0059): its HTTPSource is driven against this
  // server by a Go test, asynchronously so this process can answer it.
  it("lists and fetches through the routes the broker serves", async () => {
    await runGoTestsAsync({
      module: "orchestrator/engines/temporal",
      pkg: "./internal/corpussync",
      pattern: "^TestE2EBrokerContract$",
      mustPass: ["TestE2EBrokerContract"],
      env: {
        BROKER_CONTRACT_URL: baseUrl,
        BROKER_CONTRACT_CORPUS: CORPUS,
        BROKER_CONTRACT_SYNC_TOKEN: SYNC_TOKEN,
      },
    });

    expect(driver.list).toHaveBeenCalled();
    expect(driver.fetch).toHaveBeenCalled();
  }, 300_000);
});

/**
 * The routes the Go engine calls, requested the way `internal/corpus` requests
 * them: the orchestrator's bearer plus the caller's own token.
 */
describe("the broker serves the routes the Go engine calls", () => {
  const goHeaders = {
    authorization: `Bearer ${ORCHESTRATOR_TOKEN}`,
    "content-type": "application/json",
    [DELEGATED_TOKEN_HEADER]: DELEGATED,
  };

  it("probes, spending the caller's token rather than the corpus's", async () => {
    const res = await fetch(`${baseUrl}/corpora/${CORPUS}/probe`, {
      method: "POST",
      headers: goHeaders,
      body: JSON.stringify({ sourceId: "1" }),
    });

    expect(res.status).toBe(200);
    expect(((await res.json()) as { allowed: boolean }).allowed).toBe(true);
    const [, credsArg] = driver.probe.mock.calls.at(-1)!;
    expect(credsArg).toEqual({ delegated: DELEGATED });
  });

  it("answers a missing corpus with 404, never the 403 that means a denial", async () => {
    // The distinction that made the routing bug invisible: the Go prober
    // treats only 403 as "you may not see this" (checked below), so a dead
    // route must surface as something else.
    const res = await fetch(`${baseUrl}/corpora/no-such-corpus/probe`, {
      method: "POST",
      headers: goHeaders,
      body: JSON.stringify({ sourceId: "1" }),
    });

    expect(res.status).toBe(404);
  });

  it("reads a document as the caller", async () => {
    const res = await fetch(`${baseUrl}/corpora/${CORPUS}/documents/1`, { headers: goHeaders });

    expect(res.status).toBe(200);
    expect(await res.text()).toContain("live body");
    expect(driver.readAsUser).toHaveBeenCalled();
  });

  it("searches live as the caller", async () => {
    const res = await fetch(`${baseUrl}/corpora/${CORPUS}/search?q=deploy`, { headers: goHeaders });

    expect(res.status).toBe(200);
    expect(await res.text()).toContain("Runbook");
    expect(driver.searchAsUser).toHaveBeenCalled();
  });
});

describe("the ingestion credential never reaches a user-facing read", () => {
  // auth.ts authorizes both user reads as a `fetch`, which a sync worker
  // legitimately performs — so the ROUTE refuses a non-delegated credential
  // rather than trusting every driver to notice.
  it.each([
    ["search", `/corpora/${CORPUS}/search?q=deploy`],
    ["documents", `/corpora/${CORPUS}/documents/1`],
  ])("refuses a sync worker on %s", async (_name, path) => {
    const res = await fetch(`${baseUrl}${path}`, {
      headers: { authorization: `Bearer ${SYNC_TOKEN}` },
    });

    expect(res.status).toBe(403);
  });

  it("refuses the orchestrator with no caller to act as", async () => {
    const res = await fetch(`${baseUrl}/corpora/${CORPUS}/search?q=deploy`, {
      headers: { authorization: `Bearer ${ORCHESTRATOR_TOKEN}` },
    });

    expect(res.status).toBe(403);
  });

  it("lets the orchestrator through with one", async () => {
    const res = await fetch(`${baseUrl}/corpora/${CORPUS}/search?q=deploy`, {
      headers: {
        authorization: `Bearer ${ORCHESTRATOR_TOKEN}`,
        [DELEGATED_TOKEN_HEADER]: DELEGATED,
      },
    });

    expect(res.status).toBe(200);
  });
});

/**
 * The Go engine cannot be driven from here, so its paths are read instead.
 *
 * Crude on purpose. The alternative to checking the string is not checking it,
 * and this exact string is what broke: `internal/corpus/broker.go` is the
 * retrieval prober and has to agree with a server written in TypeScript,
 * with nothing between them but a convention nobody is reminded of.
 */
describe("the Go engine addresses the same routes", () => {
  const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
  const brokerGo = readFileSync(
    join(repoRoot, "orchestrator/engines/temporal/internal/corpus/broker.go"),
    "utf8",
  );

  it("probes /corpora/, not /connections/", () => {
    expect(brokerGo).toContain('"/corpora/"');
    // `/connections/` is webhooks only, and the Go prober never delivers one.
    expect(brokerGo).not.toContain('"/connections/"');
  });

  it("treats only 403 as a denial", () => {
    // StatusNotFound appearing beside StatusForbidden is how the silent drain
    // was written the first time.
    const denialLine = brokerGo
      .split("\n")
      .find((line) => line.includes("StatusForbidden"));
    expect(denialLine).toBeDefined();
    expect(denialLine).not.toContain("StatusNotFound");
  });
});

/**
 * Webhook delivery, end to end through the real server.
 *
 * The only entry point a STRANGER can reach: it is authenticated by the
 * provider's signature rather than by any token of ours, so the signature is
 * the whole boundary between the internet and a broker that spends a client's
 * credential on demand. Nothing else in this suite drives it.
 *
 * The fan-out is the other half. A provider signs and delivers per
 * integration, so one delivery reaches however many Corpora cover what
 * changed (ADR 0043 §4) — and must never reach a Corpus on a different
 * Connection, even when the channel id happens to match.
 */
describe("webhook delivery", () => {
  const SIGNING_SECRET = "e2e-webhook-secret";
  const CONNECTION = "bitovi-slack";

  /** Signed exactly as Slack signs, over the bytes as sent. */
  function sign(rawBody: string, timestamp = Math.floor(Date.now() / 1000)) {
    const mac = createHmac("sha256", SIGNING_SECRET)
      .update(`v0:${timestamp}:${rawBody}`, "utf8")
      .digest("hex");
    return {
      "x-slack-request-timestamp": String(timestamp),
      "x-slack-signature": `v0=${mac}`,
      "content-type": "application/json",
    };
  }

  /** Two Corpora over ONE Connection, plus one over another. */
  function slackServer(onChange: (corpus: string, ids: string[]) => void) {
    const driver = {
      provider: "slack",
      validateScope: () => {},
      list: vi.fn(),
      fetch: vi.fn(),
      probeGranularity: () => "connection" as const,
      probe: vi.fn(),
      parseWebhook: new SlackDriver({}).parseWebhook!.bind(new SlackDriver({})),
    };

    const corpus = (name: string, connection: string, channel: string) => ({
      name,
      connection,
      driver: driver as never,
      scope: { channel },
      allowedRoles: ["reader"],
      serviceToken: "bot",
    });

    return createBrokerServer({
      auth: { orchestratorToken: "orch", syncTokens: new Map() },
      registry: new StaticCorpusRegistry([
        corpus("eng-a", CONNECTION, "CENG"),
        corpus("eng-b", CONNECTION, "CENG"),
        corpus("leads", CONNECTION, "CLEADS"),
        // Same channel id, DIFFERENT Connection: a delivery signed by one
        // workspace must not reach another's corpus.
        corpus("other-workspace", "someone-else-slack", "CENG"),
      ]),
      webhooks: {
        secretFor: (connection) => (connection === CONNECTION ? SIGNING_SECRET : undefined),
        onChange,
      },
    });
  }

  let hookServer: Server;
  let hookUrl: string;
  let changes: { corpus: string; ids: string[] }[];

  beforeEach(async () => {
    changes = [];
    hookServer = slackServer((corpus, ids) => changes.push({ corpus, ids }));
    await new Promise<void>((resolve) => hookServer.listen(0, () => resolve()));
    hookUrl = `http://127.0.0.1:${(hookServer.address() as AddressInfo).port}/connections/${CONNECTION}/webhook`;
  });

  afterEach(() => hookServer?.close());

  const deliver = (body: unknown, headers?: Record<string, string>) => {
    const raw = JSON.stringify(body);
    return fetch(hookUrl, { method: "POST", headers: headers ?? sign(raw), body: raw });
  };

  const message = (channel: string) => ({
    type: "event_callback",
    event: { type: "message", channel, ts: "1700000001.000100" },
  });

  it("fans one delivery out to every Corpus covering that channel", async () => {
    const res = await deliver(message("CENG"));

    expect(res.status).toBe(202);
    expect(changes.map((c) => c.corpus).sort()).toEqual(["eng-a", "eng-b"]);
  });

  it("never crosses to a Corpus on a different Connection", async () => {
    // `other-workspace` scopes the SAME channel id. Routing by channel alone
    // would deliver another workspace's event into a client's corpus.
    await deliver(message("CENG"));

    expect(changes.map((c) => c.corpus)).not.toContain("other-workspace");
  });

  it("leaves Corpora covering another channel alone", async () => {
    await deliver(message("CLEADS"));

    expect(changes.map((c) => c.corpus)).toEqual(["leads"]);
  });

  it("refuses a forged signature", async () => {
    const raw = JSON.stringify(message("CENG"));
    const res = await fetch(hookUrl, {
      method: "POST",
      headers: {
        "x-slack-request-timestamp": String(Math.floor(Date.now() / 1000)),
        "x-slack-signature": "v0=0000000000000000000000000000000000000000000000000000000000000000",
        "content-type": "application/json",
      },
      body: raw,
    });

    expect(res.status).toBe(401);
    expect(changes).toEqual([]);
  });

  it("refuses a replayed delivery, however well signed", async () => {
    // A captured delivery is valid forever without this, and replaying it
    // makes the broker spend a client's credential on demand.
    const old = Math.floor(Date.now() / 1000) - 60 * 60;
    const raw = JSON.stringify(message("CENG"));

    const res = await fetch(hookUrl, { method: "POST", headers: sign(raw, old), body: raw });

    expect(res.status).toBe(401);
    expect(changes).toEqual([]);
  });

  it("refuses a body that was re-serialised after signing", async () => {
    // The signature covers BYTES. A proxy that reformats JSON breaks it, and
    // so does an attacker editing a captured payload.
    const raw = JSON.stringify(message("CENG"));
    const headers = sign(raw);

    const res = await fetch(hookUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(JSON.parse(raw)) + " ",
    });

    expect(res.status).toBe(401);
  });

  it("answers a URL-verification handshake without acting", async () => {
    const res = await deliver({ type: "url_verification", challenge: "abc" });

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ acted: false });
    expect(changes).toEqual([]);
  });

  it("accepts an event about a channel nobody indexes", async () => {
    // The ORDINARY case — most events in a workspace concern channels no
    // Corpus covers. Erroring would make the provider disable the endpoint.
    const res = await deliver(message("CNOBODY"));

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ acted: false });
  });

  it("is refused for a Connection with no signing secret", async () => {
    const raw = JSON.stringify(message("CENG"));
    const res = await fetch(
      hookUrl.replace(CONNECTION, "someone-else-slack"),
      { method: "POST", headers: sign(raw), body: raw },
    );

    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(changes).toEqual([]);
  });
});
