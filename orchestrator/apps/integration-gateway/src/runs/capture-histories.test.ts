import { mkdir, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { fileURLToPath } from "node:url";
import { WorkflowNotFoundError } from "@temporalio/client";
import { historyToJSON } from "@temporalio/common/proto-utils";
import { describe, expect, it } from "vitest";
import type { Envelope } from "@aktis/event-triggers";
import { HISTORY_TASK_QUEUE, testRuns, type TestRuns } from "../__fixtures__/temporal.js";
import type { AcceptedTrigger, OrchestratorInvokeResult } from "../orchestrator-client.js";
import type { ClaudeCredentialSweepPlan } from "../claude-auth/refresh-contract.js";
import type { RunActivities } from "./contract.js";
import { workflowIds, type IssueRef } from "./model.js";

/**
 * Captures the workflow histories that replay.test.ts replays (docs/adr/0057
 * "Changing a workflow"). Skipped unless CAPTURE_WORKFLOW_HISTORIES is set:
 *
 *   CAPTURE_WORKFLOW_HISTORIES=1 npx vitest run src/runs/capture-histories.test.ts
 *
 * It drives each workflow through the paths a change is most likely to
 * reorder -- queue turns with a release, a handoff and a claim; a held
 * `waitUntilClosed` with a failed publish; an identity-link resume with a
 * retried label removal; a conflict sweep -- and writes each history to
 * src/runs/__histories__/. Re-capture only alongside a change that is meant
 * to break replay (one shipped behind Worker Versioning, say), never to make
 * the replay test pass.
 */

const OUT = fileURLToPath(new URL("./__histories__/", import.meta.url));

const ISSUE: IssueRef = { owner: "acme", repo: "app", number: 7 };
const BLOCKER: IssueRef = { owner: "acme", repo: "app", number: 8 };

function envelope(id: string): Envelope {
  return {
    source: "github",
    kind: "webhook",
    id,
    time: "2026-01-01T00:00:00Z",
    headers: {},
    body: {},
    sender: { login: "octocat" },
    repository: "acme/app",
  };
}

function trigger(name: string, onComplete?: AcceptedTrigger["onComplete"]): AcceptedTrigger {
  return {
    name,
    sessionId: `session-${name}`,
    reply: { github: { repository: "acme/app", issueNumber: ISSUE.number, ack: "on it", result: true, removeLabel: "agent" } },
    ...(onComplete ? { onComplete } : {}),
  };
}

const succeeded: OrchestratorInvokeResult = { status: "succeeded", result: "done" };

/** Every activity a no-op that succeeds; each scenario overrides what it exercises. */
function activities(overrides: Partial<RunActivities>): RunActivities {
  return {
    log: async () => {},
    acknowledge: async () => ({ ackComment: { id: 1, body: "on it" } }),
    trackTurn: async () => ({ turnIndex: 0 }),
    completeTurn: async () => {},
    awaitTurn: async () => succeeded,
    postLinkPrompt: async () => {},
    waitForLink: async () => true,
    resubmit: async () => ({ kind: "accepted", id: "resubmitted" }),
    reportResult: async () => {},
    reportFailure: async () => {},
    reportLinkAbandoned: async () => {},
    removeLabel: async () => {},
    planHold: async () => ({}),
    holdState: async () => "released",
    emitRunEvent: async () => "accepted",
    commentChainDropped: async () => {},
    enqueue: async () => {},
    releaseQueue: async () => {},
    conflictsSource: async () => undefined,
    findConflicts: async () => [],
    emitConflict: async () => true,
    ...overrides,
  };
}

async function until(check: () => boolean | Promise<boolean>, what: string): Promise<void> {
  const deadline = Date.now() + 20_000;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

/** Closed; false while it runs or has not started yet. */
async function closed(t: TestRuns, workflowId: string): Promise<boolean> {
  try {
    return (await t.client.workflow.getHandle(workflowId).describe()).status.name !== "RUNNING";
  } catch (error) {
    if (error instanceof WorkflowNotFoundError) return false;
    throw error;
  }
}

async function save(t: TestRuns, name: string, workflowId: string): Promise<void> {
  const history = await t.client.workflow.getHandle(workflowId).fetchHistory();
  await mkdir(OUT, { recursive: true });
  // Who ran it (pid@host, also in sticky queue names) and where an error was thrown are this machine's, not the workflow's: replay reads neither.
  const scrubbed = JSON.parse(historyToJSON(history), (key, value) => {
    if (key === "identity" && typeof value === "string") return "capture-histories";
    if (key === "stackTrace") return "";
    return typeof value === "string" ? value.replaceAll(hostname(), "capture-host") : value;
  });
  await writeFile(`${OUT}${name}.json`, `${JSON.stringify(scrubbed, null, 2)}\n`);
}

describe.skipIf(!process.env.CAPTURE_WORKFLOW_HISTORIES)("capture workflow histories for replay.test.ts", () => {
  it("serialize: two queue turns (a release, then a handoff a started run claims)", async () => {
    const t = await testRuns({ queueClaimGraceMs: 60_000 }, { taskQueue: HISTORY_TASK_QUEUE });
    const queue = "deploy:acme/app";
    await t.serve(
      activities({
        planHold: async () => ({ queue }),
        enqueue: (hold) => t.runs.enqueue(hold),
        releaseQueue: (hold) => t.runs.releaseQueue(hold),
        emitRunEvent: async (completed, opts) => {
          if (completed.envelope.id === "first") return "not-accepted";
          // The second publish starts its run, then dies without answering: the run claims the handed-off turn.
          await t.runs.startEventRun({ invocationId: "next-hop", envelope: envelope("next-hop"), trigger: trigger("next"), ...(opts.holdsQueue ? { holdsQueue: opts.holdsQueue } : {}) });
          throw new Error("worker died mid-publish");
        },
      }),
    );
    const onComplete = { emitTo: "next", serialize: { group: "deploy", keyParam: "repository" } };
    for (const id of ["first", "second"]) {
      await t.runs.startEventRun({ invocationId: id, envelope: envelope(id), trigger: trigger("build", onComplete) });
    }
    const ids = {
      run: workflowIds.eventRun(t.taskQueue, "first"),
      held: workflowIds.heldRunEvent(t.taskQueue, "second"),
      claimingRun: workflowIds.eventRun(t.taskQueue, "next-hop"),
      queue: workflowIds.runQueue(t.taskQueue, queue),
    };
    await until(async () => (await closed(t, ids.held)) && (await closed(t, ids.claimingRun)), "both turns");
    // The queue has released its last turn and idles for an hour: its history so far is what a replay sees.
    await until(async () => {
      const h = await t.client.workflow.getHandle(ids.queue).fetchHistory();
      return (h.events ?? []).filter((e) => e.workflowExecutionSignaledEventAttributes?.signalName === "release").length >= 2;
    }, "the claiming run's release");
    await new Promise((r) => setTimeout(r, 500));
    await save(t, "eventRun-serialized", ids.run);
    await save(t, "heldRunEvent-serialized-handoff", ids.held);
    await save(t, "eventRun-claims-queue", ids.claimingRun);
    await save(t, "runQueue-release-handoff-claim", ids.queue);
    await t.stop();
  });

  it("waitUntilClosed: an identity-link resume, a retried label removal, a held publish that fails once", async () => {
    const t = await testRuns({}, { taskQueue: HISTORY_TASK_QUEUE });
    let turns = 0;
    let labelTries = 0;
    let checks = 0;
    let emits = 0;
    await t.serve(
      activities({
        awaitTurn: async () =>
          ++turns === 1
            ? { status: "succeeded", result: "link your account", identityLinkPending: true, identityLink: { provider: "github", subject: "octocat" } }
            : succeeded,
        removeLabel: async () => {
          if (++labelTries === 1) throw new Error("GitHub said 502");
        },
        planHold: async () => ({ waitFor: BLOCKER, parkedOn: ISSUE, blocked: true }),
        holdState: async () => (++checks < 3 ? "waiting" : "released"),
        emitRunEvent: async () => (++emits === 1 ? "failed" : "accepted"),
      }),
    );
    await t.runs.startEventRun({
      invocationId: "linked",
      envelope: envelope("linked"),
      trigger: trigger("fix", { emitTo: "review", waitUntilClosed: { repositoryParam: "blockerRepo", numberParam: "blocker" } }),
    });
    const ids = { run: workflowIds.eventRun(t.taskQueue, "linked"), held: workflowIds.heldRunEvent(t.taskQueue, "linked") };
    await until(async () => (await closed(t, ids.run)) && emits >= 2 && (await closed(t, ids.held)), "the held publish");
    expect(labelTries).toBe(2);
    await save(t, "eventRun-identity-link", ids.run);
    await save(t, "heldRunEvent-wait-until-closed", ids.held);
    await t.stop();
  });

  it("conflictSweep: emits once per mark across sweeps, then ends with its source", async () => {
    const t = await testRuns({}, { taskQueue: HISTORY_TASK_QUEUE });
    let sweeps = 0;
    await t.serve(
      activities({
        conflictsSource: async () => (++sweeps <= 3 ? { author: "app/aktis", intervalMs: 20, skipLabels: [] } : undefined),
        findConflicts: async () => [
          {
            owner: "acme",
            repo: "app",
            assignee: { login: "octocat" },
            markKey: "acme/app#9@abc",
            pr: {
              number: 9,
              state: "open",
              draft: false,
              title: "x",
              url: "https://github.com/acme/app/pull/9",
              mergeable: false,
              mergeableState: "dirty",
              baseRef: "main",
              baseSha: "abc",
              headRef: "x",
              headSha: "def",
              labels: [],
              assignees: [{ login: "octocat" }],
            },
          },
        ],
      }),
    );
    await t.runs.ensureConflictSweeps(["conflicts"]);
    const id = workflowIds.conflictSweep(t.taskQueue, "conflicts");
    await until(() => closed(t, id), "the sweep to end");
    await save(t, "conflictSweep-marks", id);
    await t.stop();
  });

  it("claudeCredentialSweep: a child refresh, a subject already refreshing, a failed listing, then a stop", async () => {
    const t = await testRuns({}, { taskQueue: HISTORY_TASK_QUEUE });
    let releaseBob: (() => void) | undefined;
    const plans: ClaudeCredentialSweepPlan[] = [
      { kind: "listed", examined: 3, due: ["github:alice", "github:bob"], unrefreshable: 1, intervalMs: 20, marginMs: 1000 },
      { kind: "failed", detail: "API server unreachable", intervalMs: 20, marginMs: 1000 },
      { kind: "stop", reason: "Claude credential refresh is disabled", intervalMs: 20, marginMs: 1000 },
    ];
    await t.serve({
      ...activities({}),
      claudeCredentialsDue: async () => plans.shift() ?? { kind: "stop", reason: "done", intervalMs: 20, marginMs: 1000 },
      refreshClaudeCredential: async ({ subject }) => {
        if (subject === "github:bob") await new Promise<void>((r) => (releaseBob = r));
        return subject === "github:bob" ? { status: "refreshed", expiresAt: "2026-01-01T08:00:00.000Z", sealedCredentialsJson: "iv:tag:sealed" } : { status: "refreshed", expiresAt: "2026-01-01T08:00:00.000Z" };
      },
    });
    const ids = {
      sweep: workflowIds.claudeCredentialSweep(t.taskQueue),
      alice: workflowIds.claudeCredentialRefresh(t.taskQueue, "github:alice"),
      bob: workflowIds.claudeCredentialRefresh(t.taskQueue, "github:bob"),
    };
    // A read is already refreshing bob when the sweep reaches him.
    await t.client.workflow.start("claudeCredentialRefresh", { taskQueue: t.taskQueue, workflowId: ids.bob, args: [{ subject: "github:bob", marginMs: 1000 }] });
    await until(() => releaseBob !== undefined, "bob's refresh to start");
    await t.client.workflow.start("claudeCredentialSweep", { taskQueue: t.taskQueue, workflowId: ids.sweep, args: [{ intervalMs: 20 }] });
    await until(() => closed(t, ids.sweep), "the sweep to stop");
    releaseBob?.();
    await until(() => closed(t, ids.bob), "bob's refresh");
    await save(t, "claudeCredentialSweep-child-skip-fail-stop", ids.sweep);
    await save(t, "claudeCredentialRefresh-refreshed", ids.alice);
    await save(t, "claudeCredentialRefresh-sealed", ids.bob);
    await t.stop();
  });

  it("claudeCredentialRefresh: an activity that fails is a result, not a retry", async () => {
    const t = await testRuns({}, { taskQueue: HISTORY_TASK_QUEUE });
    await t.serve({
      ...activities({}),
      refreshClaudeCredential: async () => {
        throw new Error("worker lost mid-refresh");
      },
    });
    const id = workflowIds.claudeCredentialRefresh(t.taskQueue, "github:carol");
    await t.client.workflow.start("claudeCredentialRefresh", { taskQueue: t.taskQueue, workflowId: id, args: [{ subject: "github:carol", marginMs: 1000 }] });
    await until(() => closed(t, id), "the refresh");
    await save(t, "claudeCredentialRefresh-failed", id);
    await t.stop();
  });
});
