import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

/**
 * MCP tool support across its components (ADR 0045), driven against the REAL
 * broker. The broker (Go, orchestrator/apps/mcp-broker), its discovery writer,
 * and the Temporal engine's dispatch activity and catalog reader implement one
 * contract — a URL, an `x-delegated-token` header, a `{arguments}` request, a
 * `{result,isError}` reply, and the MCPTool CR's spec fields — and nothing but
 * agreement makes a tool call reach a server and come back. This is the same
 * guard `broker-contract.e2e.ts` keeps over the connection-broker, for the same
 * reason: every past break in this repo was a cross-component rename that a
 * unit suite, mocking the other half, waved through. It needs no cluster: the
 * MCP server is a fake and the question is purely whether the halves of this
 * repo agree.
 *
 * The broker is Go, so the four boundaries are driven by its own contract
 * suite (`contract_test.go`, `TestContract*`), which this spec runs:
 *
 *   1. The broker serves the route the Go engine calls (route, auth, which
 *      credential is spent, errors), driven with requests shaped exactly like
 *      mcptool.go's callThroughBroker.
 *   2. Discovery's WRITE of a derived MCPTool carries only fields the Go
 *      catalog reader (decode.go's mcpToolSpec) decodes.
 *   3. The existence rule (§6) through the real writer.
 *   4. The Go engine's source addresses that same route and reply.
 *
 * Below that, the two halves are also read as source from here, so a rename on
 * either side fails this spec even if someone narrows the Go suite.
 *
 * Needs `go` on PATH (the version in orchestrator/apps/mcp-broker/go.mod).
 */

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const brokerDir = join(repoRoot, "orchestrator/apps/mcp-broker");
const read = (path: string) => readFileSync(join(repoRoot, path), "utf8");

describe("the broker's contract suite passes against this checkout", () => {
  it("runs TestContract* in orchestrator/apps/mcp-broker", () => {
    const run = spawnSync("go", ["test", "-count=1", "-run", "^TestContract", "-v", "."], {
      cwd: brokerDir,
      encoding: "utf8",
      timeout: 300_000,
    });
    if (run.error) throw new Error(`could not run go (is it on PATH?): ${run.error.message}`);
    const output = `${run.stdout}\n${run.stderr}`;
    expect(run.status, output).toBe(0);
    // A suite that matched nothing would also exit 0; prove each boundary ran.
    for (const test of [
      "TestContractBrokerServesTheEngineRoute",
      "TestContractDiscoveryWriteMatchesEngineRead",
      "TestContractExistenceRule",
      "TestContractEngineAddressesTheSameRoute",
    ]) {
      expect(output, `${test} did not run`).toContain(`--- PASS: ${test}`);
    }
  }, 320_000);
});

describe("the broker and the Go engine name the same route, header and reply", () => {
  const brokerServerGo = read("orchestrator/apps/mcp-broker/internal/server/server.go");
  const brokerClientGo = read("orchestrator/apps/mcp-broker/internal/mcpclient/mcpclient.go");
  const mcptoolGo = read("orchestrator/engines/temporal/internal/temporal/activities/mcptool.go");

  it("serves the /servers/:server/tools/:tool/call route the engine builds", () => {
    expect(brokerServerGo).toMatch(/parts\[0\] != "servers"/);
    expect(brokerServerGo).toMatch(/parts\[2\] != "tools"/);
    expect(brokerServerGo).toMatch(/parts\[4\] != "call"/);
    expect(mcptoolGo).toContain("/servers/%s/tools/%s/call");
  });

  it("reads the x-delegated-token header the engine forwards", () => {
    expect(brokerServerGo).toContain('DelegatedTokenHeader = "x-delegated-token"');
    expect(mcptoolGo).toContain("x-delegated-token");
  });

  it("replies {result,isError} and refuses with {message}, as the engine decodes", () => {
    expect(brokerClientGo).toMatch(/json:"result"/);
    expect(brokerClientGo).toMatch(/json:"isError"/);
    expect(brokerServerGo).toMatch(/json:"message"/);
    expect(mcptoolGo).toMatch(/json:"result"/);
    expect(mcptoolGo).toMatch(/json:"isError"/);
    expect(mcptoolGo).toMatch(/json:"message"/);
  });

  it("fails closed on a missing delegated token rather than falling back", () => {
    // A NeedsLink path keyed on IdentityProviders: an unlinked caller gets a
    // link prompt, never a call on some other credential.
    expect(mcptoolGo).toContain("IdentityProviders");
    expect(mcptoolGo).toMatch(/NeedsLink/);
  });
});
