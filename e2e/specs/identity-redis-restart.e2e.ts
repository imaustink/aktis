import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { requireLocalClusterContext } from "../support/guard.js";
import { agentRunAgentRef, agentRunSecretEnvNames, agentRunsSince, cleanupAgentRunsSince, waitFor, runsMark } from "../support/k8s.js";
import { chatSubject, chatTurn } from "../support/chat.js";
import {
  claudeCredentialSubjects,
  deleteCredentials,
  hasGithubLink,
  seedAllClaudeCredentials,
  seedGithubLink,
} from "../support/credential-store.js";
import { deleteRedisKeys } from "../support/redis.js";
import { bounceRedis } from "../support/resilience.js";
import { resetFakeGithub } from "../support/fixtures.js";
import { EXCLUSIVE_LOCK, holdLock } from "../support/locks.js";

requireLocalClusterContext();

// Split out of identity-keying.e2e.ts so it can run in the suite's exclusive
// phase (vitest.config.ts): deleting the Redis pod drops the session pages and
// conversation state every concurrently running spec is using.
holdLock(EXCLUSIVE_LOCK);

const SENDER = "e2e-user";
const CANONICAL = `github:${SENDER}`;

/**
 * The credential store outlives the infrastructure it runs alongside
 * (docs/adr/0034).
 *
 * This is the spec the reported incident needed and no existing one covered.
 * Every keying spec above seeds a credential and asserts WHERE it is read from,
 * which is exactly the right question and completely blind to the one that
 * actually broke: whether it is still there at all. The store was Redis, that
 * Redis runs with `--save "" --appendonly no` on an emptyDir, its pod restarted,
 * and every credential in the cluster was gone. The pre-flight then behaved
 * perfectly -- resolved the right principal, looked under it, found nothing --
 * and asked a user who had authorized months earlier to authorize again.
 *
 * Which is why the assertion is deliberately made ACROSS a real pod deletion
 * rather than against a store abstraction: nothing observable in-process
 * distinguishes "durable" from "durable until this pod moves". Redis is left
 * ephemeral on purpose. The credentials are not in it any more, and that is the
 * property under test.
 */
describe("linked credentials survive a Redis restart (ADR 0034)", () => {
  const CHAT_USER = "e2e-durability-user";
  const SUBJECT = chatSubject(CHAT_USER);
  const REQUEST = "Delegate this to stub-agent: fix the failing test in e2e-org/e2e-repo";
  /** Either agent proves the point -- both declare the Claude providers the gate resolves. */
  const CREDENTIAL_AGENTS = ["stub-agent", "claude-code-swe-agent"];
  let suiteStartedAt: Date;

  /** Local mirror of the sibling block's helper; the constants above are block-scoped. */
  async function expectCredentialAgent(startedAt: Date, what: string): Promise<{ name: string }> {
    const run = await waitFor(what, async () => (await agentRunsSince(startedAt))[0], { timeoutMs: 420_000 });
    const ref = await agentRunAgentRef(run.name);
    expect(CREDENTIAL_AGENTS, `planner selected ${ref}`).toContain(ref);
    return run;
  }

  beforeAll(() => {
    suiteStartedAt = new Date();
  });

  afterAll(async () => {
    await cleanupAgentRunsSince(suiteStartedAt);
  });

  beforeEach(async () => {
    await resetFakeGithub();
    await deleteCredentials();
    await deleteRedisKeys("sess:*");
  });

  it("keeps both Claude credentials and the GitHub link across the restart", async () => {
    await seedGithubLink(SUBJECT, SENDER);
    await seedAllClaudeCredentials(CANONICAL);

    expect(await claudeCredentialSubjects("setup-token")).toContain(CANONICAL);
    expect(await claudeCredentialSubjects("login")).toContain(CANONICAL);
    expect(await hasGithubLink(SUBJECT)).toBe(true);

    const { oldPod, newPodReadyMs } = await bounceRedis();
    console.log(`  [durability] replaced Redis pod ${oldPod}; new pod Ready in ${newPodReadyMs}ms`);

    // Before ADR 0034 all three of these were empty here, which is the whole
    // incident in three assertions.
    expect(await claudeCredentialSubjects("setup-token")).toContain(CANONICAL);
    expect(await claudeCredentialSubjects("login")).toContain(CANONICAL);
    expect(await hasGithubLink(SUBJECT)).toBe(true);
  });

  it("still authorizes a turn after the restart instead of asking for a re-link", async () => {
    // The behavioural half. Surviving as a stored record is necessary but not
    // sufficient -- the gateway has to still READ it after its own connection to
    // the vanished pod broke, which is the failure mode a store-level assertion
    // alone would miss.
    await seedGithubLink(SUBJECT, SENDER);
    await seedAllClaudeCredentials(CANONICAL);
    await bounceRedis();

    const startedAt = await runsMark();
    await chatTurn(CHAT_USER, REQUEST);

    // A launch is the proof: the gate refuses to launch without both Claude
    // credentials resolved, so reaching an AgentRun means it found them where
    // the restart was supposed to have destroyed them.
    const run = await expectCredentialAgent(
      startedAt,
      "an AgentRun to launch on credentials that outlived the Redis restart",
    );
    const envNames = await agentRunSecretEnvNames(run.name);
    expect(envNames).toContain("CLAUDE_CODE_OAUTH_TOKEN");
    expect(envNames).toContain("CLAUDE_LOGIN_CREDENTIALS_JSON");
  });
});
