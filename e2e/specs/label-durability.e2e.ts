import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { requireLocalClusterContext } from "../support/guard.js";
import { cleanupAgentRunsSince } from "../support/k8s.js";
import { resetFakeGithub, webhookSecret } from "../support/fixtures.js";
import { seedAllClaudeCredentials } from "../support/credential-store.js";
import { pacingDirective } from "../support/resilience.js";
import { IDLE_WINDOW_MS, labelDurabilityHarness } from "../support/label-durability.js";

requireLocalClusterContext();

const GATEWAY_PORT = 18093;
// This file's own sender, so its credential subject (github:<sender>) is no
// concurrent spec file's: identity-keying needs github:e2e-user EMPTY.
const SENDER = "e2e-label-user";

/**
 * Proves trigger-label removal is DETERMINISTIC AND DURABLE -- the headline
 * bug: it "only worked ~3/4 of the time" because removal was an in-process
 * `finally` on a detached, whole-turn background promise. If the
 * integration-gateway pod restarted/OOM'd mid-turn, that `finally` never ran
 * and nothing retried, so the label was stranded forever (a re-apply emits no
 * `labeled` event, so the issue could not be re-triaged by hand).
 *
 * Two guarantees, neither observable without a real gateway pod, a real
 * orchestrator, and a real (fake-)GitHub recording the DELETE:
 *
 *   1. the label comes off even on the UNHAPPY path (the run fails) -- here, and
 *   2. the label STILL comes off when the gateway is rolled mid-turn --
 *      label-durability-rollout.e2e.ts, which runs in the exclusive phase
 *      because rolling the gateway disrupts every concurrent spec file.
 *
 * Mirrors resilience.e2e.ts's posture: pace the stub so the turn is still
 * running when the disruption lands, and assert on what fake-github actually
 * recorded.
 */
describe("label durability: a trigger label always comes off, even on the unhappy path", () => {
  let secret: string;
  let suiteStartedAt: Date;
  const harness = labelDurabilityHarness({
    sender: SENDER,
    gatewayPort: GATEWAY_PORT,
    secret: () => secret,
    issueBase: 50_000 + (Date.now() % 40_000),
  });

  beforeAll(async () => {
    suiteStartedAt = new Date();
    secret = await webhookSecret();
    await resetFakeGithub();
    // The stub declares the real agent's identityProviders, so without a seeded
    // credential the gate PARKS and no AgentRun is ever created.
    await seedAllClaudeCredentials(`github:${SENDER}`);
  });

  afterAll(async () => {
    await cleanupAgentRunsSince(suiteStartedAt);
  });

  it("removes the trigger label even when the turn FAILS (the label must come off so a re-apply re-triggers)", async () => {
    // Drive the orchestrator to give up on the turn: plain silence past the idle
    // window is a failure (see resilience.e2e.ts's negative control). A failed
    // run is exactly when someone wants to re-trigger, so the label MUST come off.
    // Paced by a directive in the issue body, so only THIS run goes silent.
    const { issueNumber, startedAt } = await harness.trigger(pacingDirective({ silentForMs: IDLE_WINDOW_MS * 2 }));
    await harness.runCreated(startedAt);

    const deleted = await harness.labelRemoved(issueNumber);
    expect(deleted).toBeDefined();
  });
});
