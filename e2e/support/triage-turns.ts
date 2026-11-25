import { expect } from "vitest";
import { agentRunsSince, runsMark, waitFor, withPortForward, type AgentRunSummary } from "./k8s.js";
import { issueLabeledPayload, postGithubWebhook } from "./webhook.js";
import { fakeGithubRequests, type RecordedRequest } from "./fixtures.js";

/**
 * Fire-a-triage-webhook-and-watch-the-turn helpers, shared by the resilience
 * specs: the idle-window ones, which run alongside every other file
 * (resilience.e2e.ts), and the rollout ones, which run in the exclusive phase
 * (resilience-rollout.e2e.ts).
 */

export const OWNER = "e2e-org";
export const REPO = "e2e-repo";
export const STUB_REPLY_MARKER = "stub-agent-reply";
// server.ts's upfront "announce" comment (session pages, ADR ...), posted the
// moment a run is seen genuinely running -- BEFORE any terminal outcome
// exists. `commentOn` below must not mistake it for the terminal comment.
const STARTING_WORK_ACK = "Starting work on this now.";

/**
 * The idle window this environment runs with — `temporal-engine.worker.idleTimeoutSeconds` in
 * orchestrator/charts/aktis/values-e2e.yaml. Specs pace turns relative to
 * it, so it lives here as a named constant rather than as scattered magic
 * numbers that would silently stop meaning anything if the value changed.
 */
export const IDLE_WINDOW_MS = 30_000;

/**
 * The integration-gateway's poll budget in this environment
 * (`pollTimeoutMs` in values-e2e.yaml). It is the BINDING constraint on every
 * pacing number: a turn that outruns it gets the gateway's own failure
 * comment regardless of how well the orchestrator handled things, so any spec
 * asserting on the stub's reply must finish inside it.
 */
export const GATEWAY_POLL_BUDGET_MS = 240_000;

/**
 * Budget for the best-effort warm-up turn. Deliberately well under
 * vitest.config.ts's `hookTimeout` (300s) -- a warm-up that outruns the hook
 * skips the entire file.
 */
const WARM_UP_BUDGET_MS = 150_000;

export interface TriageTurns {
  /**
   * Fires a triage webhook and returns the issue number it used. `body` is the
   * issue body (where a per-run pacing directive goes).
   *
   * `onIssue` re-triggers against an issue already used. That matters for
   * resumability: integration-gateway derives its `session_id` from the issue
   * (`github:owner/repo#N`), so a second trigger on the SAME issue is the same
   * conversation to the orchestrator, and therefore the thing that re-attaches
   * to a run a previous turn was cut off from.
   */
  trigger(opts?: { onIssue?: number; body?: string }): Promise<{ issueNumber: number; startedAt: Date }>;
  runCreated(startedAt: Date): Promise<AgentRunSummary>;
  runTerminal(startedAt: Date, timeoutMs?: number): Promise<AgentRunSummary>;
  /** The latest comment on the issue that is not the upfront "Starting work" ack. */
  commentOn(issueNumber: number, timeoutMs?: number): Promise<RecordedRequest>;
  /**
   * Runs one throwaway turn before any timed assertion.
   *
   * The FIRST turn a freshly-started orchestrator serves is far slower than
   * every later one -- it pays for Qdrant collection setup, embedding calls and
   * skill re-derivation before the graph ever reaches the delegate step. That
   * cost landed inside the first spec's "wait for an AgentRun" budget and made
   * it fail on attempt one and pass on retry, which reads exactly like a flaky
   * product bug and is not one. Paying it in setup puts it where it belongs.
   *
   * Best-effort on purpose: a warm-up that fails should not fail the suite,
   * because the specs assert the real thing and will report honestly either
   * way. It logs instead.
   */
  warmUp(body?: string): Promise<void>;
}

/**
 * Issue numbers are a per-run base plus a counter, never `Date.now() % N`: the
 * issue number decides the session id, sessions outlive a spec, and a turn
 * landing on a session that still carries an awaiting-reply anchor RE-ATTACHES
 * to that old run instead of launching a new one (docs/adr/0033). Callers in
 * different files sharing a sender must pass disjoint `issueBase` ranges.
 */
export function triageTurns(opts: {
  sender: string;
  gatewayPort: number;
  secret: () => string;
  issueBase: number;
}): TriageTurns {
  let issueCounter = 0;
  const nextIssueNumber = () => opts.issueBase + issueCounter++ * 137;

  const turns: TriageTurns = {
    async trigger({ onIssue, body } = {}) {
      const issueNumber = onIssue ?? nextIssueNumber();
      const startedAt = await runsMark();
      const status = await withPortForward("agent-controller-integration-gateway", 8090, opts.gatewayPort, async (baseUrl) => {
        const res = await postGithubWebhook(
          baseUrl,
          "issues",
          issueLabeledPayload({
            owner: OWNER,
            repo: REPO,
            issueNumber,
            label: "ai-triage",
            senderLogin: opts.sender,
            ...(body === undefined ? {} : { body }),
          }),
          opts.secret(),
        );
        return res.status;
      });
      expect(status).toBeGreaterThanOrEqual(200);
      expect(status).toBeLessThan(300);
      return { issueNumber, startedAt };
    },

    runCreated: (startedAt) =>
      waitFor("an AgentRun to be created", async () => (await agentRunsSince(startedAt))[0], { timeoutMs: 420_000 }),

    runTerminal: (startedAt, timeoutMs = 300_000) =>
      waitFor(
        "the AgentRun to reach a terminal phase",
        async () => {
          const [current] = await agentRunsSince(startedAt);
          return current?.phase === "Succeeded" || current?.phase === "Failed" ? current : undefined;
        },
        { timeoutMs },
      ),

    commentOn: (issueNumber, timeoutMs = 180_000) =>
      waitFor(
        `a terminal comment on issue #${issueNumber}`,
        async () => {
          // Excludes the upfront "Starting work" ack (server.ts's `announce`,
          // posted the moment the run is seen running, independent of and
          // usually well before any outcome exists) -- without this, a probe
          // landing between that ack and the real terminal comment returns the
          // ack and stops waiting, which is wrong for any spec whose run is
          // still legitimately in flight (e.g. deliberately never reaching
          // Succeeded/Failed, like the idle-timeout negative control).
          const posted = (await fakeGithubRequests()).filter(
            (r) =>
              r.method === "POST" &&
              r.path === `/repos/${OWNER}/${REPO}/issues/${issueNumber}/comments` &&
              !(typeof r.body === "string" && r.body.includes(STARTING_WORK_ACK)),
          );
          return posted.length > 0 ? posted[posted.length - 1] : undefined;
        },
        { timeoutMs },
      ),

    async warmUp(body) {
      try {
        const { startedAt } = await turns.trigger({ body });
        await turns.runTerminal(startedAt, WARM_UP_BUDGET_MS);
        console.log("  [resilience] warm-up turn completed; the stack is warm for the timed specs below");
      } catch (err) {
        console.log(`  [resilience] warm-up did not complete (continuing): ${err instanceof Error ? err.message : String(err)}`);
      }
    },
  };
  return turns;
}
