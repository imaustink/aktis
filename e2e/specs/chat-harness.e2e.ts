import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import { runGoTests } from "../support/go-test.js";
import {
  assembleSseContent,
  assembleSseToolCalls,
  mintForwardedUserJwt,
  sseFinishReason,
} from "../support/openwebui-jwt.js";

/**
 * The chat harness, checked against the product code it has to agree with.
 *
 * Deliberately NO `requireLocalClusterContext()`: this spec touches no cluster, so
 * it runs anywhere `npm run e2e` runs. That is the point. A harness's signing is
 * the part most likely to be quietly wrong, and when it is, every cluster spec
 * built on it fails with "could not resolve caller identity" -- which reads as a
 * product defect and costs a full stack cycle to disprove. Verifying the
 * agreement here turns that class of failure into a fast, unambiguous red.
 *
 * It checks against the REAL resolver rather than reimplementing its
 * expectations: the claim the harness signs and the claim the orchestrator reads
 * have to be the same claim, and only the resolver itself is authoritative about
 * which that is. The orchestrator is Go (docs/adr/0056), so this spec mints the
 * tokens and the Go test `TestE2EHarnessForwardedUserJWT` (internal/facade)
 * verifies them with the product resolver. Needs `go` on PATH.
 */

/**
 * Real orchestrator SSE output, produced once by `TestE2EHarnessStreams` (the
 * facade's own handler behind a stub engine) for the stream specs below.
 */
let productStreams: { text: string; toolCalls: string };

function harness(streamOut = ""): Record<string, string> {
  const secret = "e2e-openwebui-forward-jwt-secret";
  return {
    E2E_HARNESS_JWTS: JSON.stringify({
      valid: mintForwardedUserJwt(secret, "e2e-chat-user"),
      wrongSecret: mintForwardedUserJwt("not-the-deployments-secret", "e2e-chat-user"),
      fresh: mintForwardedUserJwt(secret, "e2e-chat-user", { expiresInSeconds: 300 }),
      expired: mintForwardedUserJwt(secret, "e2e-chat-user", { expiresInSeconds: -60 }),
      streamOut,
    }),
  };
}

beforeAll(() => {
  const dir = mkdtempSync(join(tmpdir(), "e2e-harness-"));
  try {
    const out = join(dir, "streams.json");
    runGoTests({
      module: "orchestrator/engines/temporal",
      pkg: "./internal/facade",
      pattern: "^TestE2EHarnessStreams$",
      mustPass: ["TestE2EHarnessStreams"],
      env: harness(out),
    });
    productStreams = JSON.parse(readFileSync(out, "utf8")) as typeof productStreams;
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}, 300_000);

describe("chat harness agrees with the orchestrator's forwarded-user resolver", () => {
  it("mints JWTs the resolver accepts (subject, roles, perUser), refuses on the wrong secret, and honors exp", () => {
    // One Go test, four verdicts: TestE2EHarnessForwardedUserJWT fails unless
    // the valid token resolves to openwebui:e2e-chat-user with perUser (without
    // which docs/adr/0031's pre-flight silently tests the degraded path), the
    // wrong-secret token is refused (so acceptance is evidence of agreement, not
    // of a resolver that accepts anything), and exp is honored both ways
    // (specs/identity-continuity.e2e.ts depends on it).
    runGoTests({
      module: "orchestrator/engines/temporal",
      pkg: "./internal/facade",
      pattern: "^TestE2EHarnessForwardedUserJWT$",
      mustPass: ["TestE2EHarnessForwardedUserJWT"],
      env: harness(),
    });
  }, 300_000);

  it("assembles the assistant's text out of an OpenAI-style stream", async () => {
    const body = [
      'data: {"choices":[{"delta":{"role":"assistant"}}]}',
      'data: {"choices":[{"delta":{"content":"To continue, please "}}]}',
      ": keep-alive",
      'data: {"choices":[{"delta":{"content":"link your GitHub account"}}]}',
      'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}',
      "data: [DONE]",
      "",
    ].join("\n");

    // Frames with no content delta, comments and the terminator are skipped
    // rather than parsed strictly -- a spec asserting on what the human saw
    // should not fail over a frame shape it doesn't care about.
    expect(assembleSseContent(body)).toBe("To continue, please link your GitHub account");
  });
});

/**
 * The tool-call half of the same agreement, against the shape the ORCHESTRATOR
 * actually emits (docs/adr/0035).
 *
 * Same reasoning as the JWT tests above: if the harness cannot reconstruct a tool
 * call off the stream, every caller-tool spec fails looking like the product never
 * emitted one. These frames are built from the orchestrator's own
 * `toolCallDeltaChunk` output shape, so a change to that shape that the harness
 * does not follow fails HERE -- fast, and with no cluster -- rather than as eight
 * mysteriously empty `toolCalls` arrays.
 */
describe("chat harness reads tool calls off an OpenAI-style stream", () => {
  const toolCallStream = [
    'data: {"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_abc","type":"function","function":{"name":"get_device_battery","arguments":"{\\"serial\\":\\"SN-4417\\"}"}}]},"finish_reason":null}]}',
    ": keep-alive",
    'data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}',
    "data: [DONE]",
    "",
  ].join("\n");

  it("reconstructs the call a client would execute", () => {
    expect(assembleSseToolCalls(toolCallStream)).toEqual([
      { id: "call_abc", name: "get_device_battery", arguments: '{"serial":"SN-4417"}' },
    ]);
  });

  it("reads the terminal finish_reason that tells a client to execute rather than render", () => {
    // The single most load-bearing field: "stop" here would make every real client
    // show an empty assistant message and run nothing.
    expect(sseFinishReason(toolCallStream)).toBe("tool_calls");
    expect(sseFinishReason('data: {"choices":[{"delta":{},"finish_reason":"stop"}]}')).toBe("stop");
  });

  it("assembles arguments split across deltas, as the wire format permits", () => {
    // This orchestrator emits each call whole, but the format allows chunked
    // arguments and a real client concatenates them. A harness that assumed whole
    // calls would stop being able to tell the difference.
    const split = [
      'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"f","arguments":"{\\"a\\":"}}]}}]}',
      'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]}}]}',
      "",
    ].join("\n");

    expect(assembleSseToolCalls(split)).toEqual([{ id: "call_1", name: "f", arguments: '{"a":1}' }]);
  });

  it("keeps parallel calls separate, keyed by index rather than arrival", () => {
    const parallel = [
      'data: {"choices":[{"delta":{"tool_calls":[{"index":1,"id":"b","function":{"name":"second","arguments":"{}"}}]}}]}',
      'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"first","arguments":"{}"}}]}}]}',
      "",
    ].join("\n");

    expect(assembleSseToolCalls(parallel).map((c) => c.name)).toEqual(["first", "second"]);
  });

  it("finds no tool calls in an ordinary content stream", () => {
    // Guards the negative assertions every caller-tool spec makes ("the gate
    // withheld them", "housekeeping never emits one") from being vacuously true.
    const contentOnly = 'data: {"choices":[{"delta":{"content":"58% remaining"}}]}\ndata: [DONE]\n';
    expect(assembleSseToolCalls(contentOnly)).toEqual([]);
  });

  it("reads the frames the orchestrator ITSELF produces, not a transcription of them", () => {
    // Every test above feeds hand-written frames, which only prove the harness
    // agrees with what its author BELIEVED the product emits. These bodies are
    // the facade's real streaming output (TestE2EHarnessStreams), so a change to
    // the emitted shape fails here instead of silently draining every
    // caller-tool spec's `toolCalls` to empty.
    expect(assembleSseToolCalls(productStreams.toolCalls)).toEqual([
      { id: "call_abc", name: "get_device_battery", arguments: '{"serial":"SN-4417"}' },
    ]);
    expect(sseFinishReason(productStreams.toolCalls)).toBe("tool_calls");
    expect(assembleSseContent(productStreams.text)).toBe("To continue, please link your GitHub account");
    expect(sseFinishReason(productStreams.text)).toBe("stop");
    expect(assembleSseToolCalls(productStreams.text)).toEqual([]);
  });
});
