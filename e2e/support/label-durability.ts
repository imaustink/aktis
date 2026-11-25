import { expect } from "vitest";
import { agentRunsSince, runsMark, waitFor, withPortForward } from "./k8s.js";
import { issueLabeledPayload, postGithubWebhook } from "./webhook.js";
import { fakeGithubRequests } from "./fixtures.js";

/**
 * The trigger-and-observe half of the label-durability specs, shared by the
 * file that runs alongside everything else (label-durability.e2e.ts) and the
 * one that rolls the gateway and so runs in the exclusive phase
 * (label-durability-rollout.e2e.ts).
 */

export const OWNER = "e2e-org";
export const REPO = "e2e-repo";
export const TRIGGER_LABEL = "ai-triage";

/** The idle window this environment runs with (`temporal-engine.worker.idleTimeoutSeconds`, values-e2e.yaml). */
export const IDLE_WINDOW_MS = 30_000;

export interface LabelDurabilityHarness {
  /** Labels a fresh issue (its body is `body`) and returns its number and the start mark. */
  trigger(body?: string): Promise<{ issueNumber: number; startedAt: Date }>;
  runCreated(startedAt: Date): Promise<{ name: string }>;
  /**
   * Waits for fake-github to have recorded a DELETE against this issue's labels
   * endpoint -- i.e. the gateway (its `finally`, its bounded retry, OR the
   * reconciler) actually removed the trigger label. The gateway URL-encodes the
   * label name into the path (github-client.ts), so match the stable prefix.
   */
  labelRemoved(issueNumber: number, timeoutMs?: number): Promise<{ method: string; path: string }>;
}

export function labelDurabilityHarness(opts: {
  sender: string;
  gatewayPort: number;
  secret: () => string;
  issueBase: number;
}): LabelDurabilityHarness {
  let issueCounter = 0;
  return {
    async trigger(body) {
      const issueNumber = opts.issueBase + issueCounter++ * 137;
      const startedAt = await runsMark();
      const status = await withPortForward("agent-controller-integration-gateway", 8090, opts.gatewayPort, async (baseUrl) => {
        const res = await postGithubWebhook(
          baseUrl,
          "issues",
          issueLabeledPayload({
            owner: OWNER,
            repo: REPO,
            issueNumber,
            label: TRIGGER_LABEL,
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
    labelRemoved: (issueNumber, timeoutMs = 300_000) =>
      waitFor(
        `the trigger label to be removed from issue #${issueNumber}`,
        async () => {
          const deletes = (await fakeGithubRequests()).filter(
            (r) =>
              r.method === "DELETE" &&
              typeof r.path === "string" &&
              r.path.startsWith(`/repos/${OWNER}/${REPO}/issues/${issueNumber}/labels/`),
          );
          return deletes.length > 0 ? deletes[deletes.length - 1] : undefined;
        },
        { timeoutMs },
      ),
  };
}
