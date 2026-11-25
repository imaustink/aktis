import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { requireLocalClusterContext } from "../support/guard.js";
import { cleanupAgentRunsSince } from "../support/k8s.js";
import { resetFakeGithub, webhookSecret } from "../support/fixtures.js";
import { seedAllClaudeCredentials } from "../support/credential-store.js";
import { pacingDirective } from "../support/resilience.js";
import { GATEWAY_POLL_BUDGET_MS, IDLE_WINDOW_MS, STUB_REPLY_MARKER, triageTurns } from "../support/triage-turns.js";

requireLocalClusterContext();

const GATEWAY_PORT = 18091;
// This file's own sender, so its credential subject (github:<sender>) is no
// concurrent spec file's: identity-keying needs github:e2e-user EMPTY.
// resilience-rollout.e2e.ts shares it, from a disjoint issue range.
const SENDER = "e2e-resilience-user";
const ISSUE_BASE = 10_000 + (Date.now() % 40_000);

/**
 * What survives infrastructure moving underneath an in-flight agent turn.
 *
 * These exist because of a real incident that no unit test could have caught
 * and no existing e2e spec covers: AgentRun 0f97aa3d ran for 3m54s and
 * SUCCEEDED, while the chat reported
 * `produced no reply within 3660000ms`. Three things combined --
 *
 *   1. the wait's bound was a nats.js FIRST-MESSAGE timeout, cancelled by the
 *      agent's first progress message, so it never actually bounded anything;
 *   2. `shutdown()` drained NATS in the same `Promise.all` as the HTTP close,
 *      destroying the subscription an in-flight turn was parked on;
 *   3. every failure was relabelled as that timeout, quoting a configured
 *      number that was never elapsed time.
 *
 * -- and all three are only observable with a real NATS server, a real
 * orchestrator process receiving a real SIGTERM, and a turn genuinely in
 * flight. Hence: here, not in `apps/*`.
 *
 * Every spec paces the stub agent so the turn is still running when the
 * disruption lands. Without that the turn completes in milliseconds and the
 * whole scenario is untestable -- which is why catalog/agents/stub-agent gained
 * pacing.ts.
 */
describe("resilience: how long an in-flight agent turn may run", () => {
  let secret: string;
  let suiteStartedAt: Date;
  const turns = triageTurns({ sender: SENDER, gatewayPort: GATEWAY_PORT, secret: () => secret, issueBase: ISSUE_BASE });

  beforeAll(async () => {
    suiteStartedAt = new Date();
    secret = await webhookSecret();
    await resetFakeGithub();
    // The stub declares the real agent's identityProviders, so without a
    // seeded credential the gate PARKS and no AgentRun is ever created.
    await seedAllClaudeCredentials(`github:${SENDER}`);
    await turns.warmUp();
  });

  afterAll(async () => {
    await cleanupAgentRunsSince(suiteStartedAt);
  });

  // Paced by a directive in the issue body, so only THESE runs narrate or go
  // silent (support/resilience.ts pacingDirective) and this file can run
  // alongside the rest of the suite. The specs that disrupt infrastructure
  // mid-turn are resilience-rollout.e2e.ts, in the exclusive phase.
  it("a narrating turn outlives the idle window many times over", async () => {
    // 2x the idle window, narrating every 5s, and comfortably inside the
    // gateway's poll budget so the reply assertion below is meaningful. The
    // point is that DURATION is not bounded -- only silence is.
    const narrateForMs = IDLE_WINDOW_MS * 2;
    expect(narrateForMs).toBeLessThan(GATEWAY_POLL_BUDGET_MS);
    const { issueNumber, startedAt } = await turns.trigger({ body: pacingDirective({ narrateForMs, narrateEveryMs: 5000 }) });
    await turns.runCreated(startedAt);
    const run = await turns.runTerminal(startedAt);
    expect(run.phase).toBe("Succeeded");

    const comment = await turns.commentOn(issueNumber);
    // The stub's own reply, not the error comment the gateway posts on failure.
    expect(comment?.body).toContain(STUB_REPLY_MARKER);
  });

  it("a silent turn IS cut off once it exceeds the idle window", async () => {
    // The negative control, and the only proof the window does anything at all.
    // A `tool_call` would legitimately pause the clock; plain silence must not.
    // 2x the window: long enough to trip it, short enough that the orchestrator
    // gives up (at 1x) well inside the gateway's poll budget, so the failure
    // that reaches the issue is the orchestrator's and not the gateway's.
    const { issueNumber, startedAt } = await turns.trigger({ body: pacingDirective({ silentForMs: IDLE_WINDOW_MS * 2 }) });
    await turns.runCreated(startedAt);

    const comment = await turns.commentOn(issueNumber);
    // The orchestrator gave up on the turn, so the gateway relays a failure
    // rather than the stub's reply -- which never got published.
    expect(comment?.body).not.toContain(STUB_REPLY_MARKER);

    // And it said the TRUE thing, on the surface a human actually sees. The
    // orchestrator never logs a turn error -- it returns it, and the gateway
    // renders it into this comment (`Something went wrong processing this: ...`)
    // -- so the comment body is the only place this is observable, and is
    // exactly the surface that reported a fabricated bound during the incident.
    expect(comment?.body).toMatch(new RegExp(`went silent for ${IDLE_WINDOW_MS}ms`));
    expect(comment?.body).not.toMatch(/produced no reply within/);
  });
});
