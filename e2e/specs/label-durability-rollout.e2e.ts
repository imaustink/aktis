import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { requireLocalClusterContext } from "../support/guard.js";
import { cleanupAgentRunsSince } from "../support/k8s.js";
import { resetFakeGithub, webhookSecret } from "../support/fixtures.js";
import { seedAllClaudeCredentials } from "../support/credential-store.js";
import { pacingDirective, rollGateway } from "../support/resilience.js";
import { labelDurabilityHarness } from "../support/label-durability.js";
import { EXCLUSIVE_LOCK, holdLock } from "../support/locks.js";

requireLocalClusterContext();

// Runs in the suite's exclusive phase (vitest.config.ts): this file rolls the
// gateway, which every concurrently running spec file depends on.
holdLock(EXCLUSIVE_LOCK);

const GATEWAY_PORT = 18093;
// Shares label-durability.e2e.ts's sender; the two never run at once (this
// file is in the exclusive phase).
const SENDER = "e2e-label-user";

/**
 * The second half of label-durability.e2e.ts's guarantee: the trigger label
 * STILL comes off when the gateway is rolled mid-turn. The run is a Temporal
 * workflow (docs/adr/0057), so the replacement pod's worker picks up the
 * cancelled poll and finishes the run, label removal included.
 */
describe("label durability: a trigger label comes off across a gateway restart", () => {
  let secret: string;
  let suiteStartedAt: Date;
  const harness = labelDurabilityHarness({
    sender: SENDER,
    gatewayPort: GATEWAY_PORT,
    secret: () => secret,
    issueBase: 10_000 + (Date.now() % 40_000),
  });

  beforeAll(async () => {
    suiteStartedAt = new Date();
    secret = await webhookSecret();
    await resetFakeGithub();
    await seedAllClaudeCredentials(`github:${SENDER}`);
  });

  afterAll(async () => {
    await cleanupAgentRunsSince(suiteStartedAt);
  });

  it("removes a stranded trigger label after the gateway is ROLLED mid-turn (durable self-heal)", async () => {
    // A long narrating run so the turn is still in flight -- and the gateway's
    // in-process label-removal `finally` still pending -- when the pod is rolled
    // out from under it. Rolling the gateway drops that `finally` on the floor:
    // nothing in the OLD pod will ever remove the label.
    const { issueNumber, startedAt } = await harness.trigger(pacingDirective({ narrateForMs: 120_000, narrateEveryMs: 5000 }));
    await harness.runCreated(startedAt);

    // The disruption: the pod holding the only in-process label-removal `finally`
    // for this turn goes away mid-flight.
    await rollGateway();

    // The REPLACEMENT pod finishes it: the old pod's worker cancels the
    // poll activity on shutdown, Temporal retries it on the new pod, and the
    // run's workflow removes the label when the run ends. Generous budget: the
    // run itself narrates for two minutes.
    const deleted = await harness.labelRemoved(issueNumber, 600_000);
    expect(deleted).toBeDefined();

    // Idempotent: the heal must not depend on the original turn, and a second
    // removal of an already-gone label is a 404 == success, never an error that
    // would wedge the sweep.
    expect(deleted?.method).toBe("DELETE");
  });
});
