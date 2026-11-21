# 0052. Run-completion events: chaining EventTriggers into a software factory

Date: 2026-11-21

## Status

Proposed. Extends [0048](0048-generic-event-triggers.md). Builds on the structured
`AgentReply.result` the agent runtime already sends, and on the identity model in
[0049](0049-configurable-agent-identity-mode.md).

## Context

We want a basic software factory, driven by labels and webhook routing:

1. A human applies `ai-triage` to an issue. The SWE agent works the issue and
   opens a PR. (This works today: `github-issue-triage`.)
2. The PR then gets `ai-review` **deterministically**, and a read-only review
   run reviews it and leaves comments.
3. The review decides between two outcomes:
   - **changes needed:** the PR gets `ai-triage`, a fix run addresses the
     review, and the flow goes back to step 2;
   - **ready:** the PR is assigned to the human for final review.

Every transition should be decided by code and trigger config, not by a model
following its prompt (see the "deterministic over prompt" practice behind #278
and #348). Today none of steps 2–3 can be built, for four reasons.

**The bot's own actions never route.** Triage PRs are opened by the GitHub App
(`k5s-bot[bot]` on glyph: every recent PR is `app/k5s-bot`). The gateway drops
every webhook whose sender is a bot:
- `resolveSender` drops it when `dropBots` is true (the default);
- every `IdentityResolver.resolve` returns `undefined` for a bot anyway
  (`integration-gateway/src/identity.ts`), so `dropBots: false` doesn't help.

That drop is the self-trigger guard, and it stays. But it means "the bot opens
a PR / adds a label, and a trigger fires" is impossible, and so is any chain
that crosses GitHub.

**A run reports nothing machine-readable.** A trigger-dispatched run ends as
`{status, result: string}` on `/invoke/:id`. The gateway posts the text and
removes the label; nothing downstream can read "which PR did this open?" or
"what did the reviewer decide?". The runtime already carries a structured
`AgentReply.result` on the wire (`framework/agent-runtime/src/runtime.ts`), but
neither engine passes it through to the invocation record.

**A review can't express its verdict through GitHub.** The App authored the PR,
and GitHub doesn't let a PR's author approve or request changes. Every bot
review on glyph#91 is `COMMENTED`. So GitHub's own review state can't carry the
decision.

**Triggers can only start runs.** `reply.github` can post an ack, post the
result and remove one label. It can't add a label or assign someone, and every
trigger must have a run target, so "assign the PR to a human" has no
expression.

## Decision

Add four features. With them, the factory is a handful of EventTrigger CRs and
no factory-specific code.

### 1. Run-completion events (`EventSource` kind `run`)

A third `EventSource` kind beside `webhook` and `cron`:

```yaml
kind: EventSource
metadata: { name: runs }
spec:
  run: {}
```

An EventTrigger opts in to publishing its runs' completions to it:

```yaml
spec:
  onComplete:
    emitTo: runs          # a run-kind EventSource in the same namespace
```

When an accepted, opted-in run finishes, the gateway synthesizes an envelope and
feeds it through the **same** `relayEvent` → `/invoke` matching path as a
webhook. It does this at the end of `runAcceptedEvent`, which already owns the
run's lifecycle (poll, result comment, label removal):

```
event = {
  source: "runs", kind: "run", id, time, headers: {},
  sender: <the chain's originating human, re-verified>,
  repository: <the chain's repository>,
  body: {
    trigger:   "github-issue-triage",       # the trigger whose run finished
    status:    "succeeded" | "failed" | "timed_out",
    params:    { repository, number, ... }, # that run's rendered params
    outcome:   { ... },                     # §2, {} when none
    chain:     { id, depth, counts: { "<trigger>": n } },
  },
}
```

Triggers match it with ordinary CEL, e.g.
`body.trigger == "github-issue-triage" && body.status == "succeeded" && has(body.outcome.pullRequest)`.

**Identity: the chain keeps its human.** A run event carries the sender of the
webhook that started the chain, never the bot. Before relaying each hop, the
gateway re-runs that login through the source's identity resolver and
`senderClaims`. A human whose write access was revoked mid-chain therefore stops
the chain, and every run still acts as, and is audited to, a verified person.
Because a run event never has a bot sender, the bot-drop guard stays exactly as
it is.

**Loop safety is deterministic, at two levels.**
- **Per trigger, set by the operator:** `body.chain.counts` counts each trigger's
  runs in the chain. A trigger caps itself in CEL, e.g.
  `body.chain.counts["factory-review"] < 3`.
- **Global, set by the gateway:** a hard depth cap (`RUN_EVENT_MAX_DEPTH`,
  default 12) that no trigger config can exceed. Past it, the gateway drops the
  event, logs it, and comments on the issue/PR it would have replied to.

**Opt-in.** Triggers without `onComplete.emitTo` behave exactly as today. Run
events are never published for chat or session-page turns, only for
trigger-dispatched runs.

**Durability (v1).** The continuation lives in the gateway process, like the
poll it follows. If the gateway restarts mid-run, the next hop is lost; the PR
is simply left where it was, and a human re-applies the label (today's manual
path, which keeps working). A durable outbox in the style of
`pendingLabelStore` is a follow-up if this proves to matter.

### 2. Structured run outcomes

`AgentReply.result` becomes the run's **outcome**. It must be a JSON object of at
most 8 KiB; anything else is dropped and logged. Both engines carry it to the
invocation record, and `/invoke/:id` returns it as `outcome`. The gateway passes
it into the run event's `body.outcome`.

**Outcomes are produced by the runner, not the model.** The SWE agents
(claude-code and opencode) report:

- `pullRequest: { number, url }` when the turn ended with an open PR. The runner
  already discovers this for the footer and the `Closes #N` fix (#345, #348).
- `madeNewCommits: bool`.
- `verdict` on read-only (review) runs, below.

**The verdict is the model's judgment, recorded deterministically.** Deciding
whether a PR is ready is inherently the model's call. Recording and routing that
call must not be.
- The runner installs an `aktis-verdict` command on PATH (next to the `gh`
  shim). The review prompt tells the model to run
  `aktis-verdict changes-requested` or `aktis-verdict ready` once, after posting
  its review.
- The command writes the value to a runner-owned file. The runner validates it
  against that enum.
- A missing or invalid verdict becomes `verdict: "none"`. Triggers route only an
  explicit `changes-requested` back to a fix run, so a confused model lands the
  PR with a human instead of looping.

### 3. GitHub actions on triggers, and runless triggers

`reply.github` gains two fields:
- `addLabels: [<template>]`, applied when the run is accepted (alongside the
  ack);
- `assignees: [<template>]`, also applied at acceptance.

Both render from params, like `removeLabel`.

A new target, `target.none: {}`, matches and runs only the reply side effects:
no AgentRun, no skill, no tool. In both engines it returns accepted and
completes immediately, with an empty result. It is how a handoff such as
"assign the PR to the human" is expressed.

Labels the bot adds this way don't re-enter routing (they're bot-authored and
dropped), so they're safe to use as status markers. A human applying the same
label by hand still fires the existing label triggers.

### 4. The factory as config

Shipped as example CRs in `examples/event-triggers/`, and enabled for homelab
in `values-production.yaml`. All four triggers are repo-agnostic, matching any
repository the App is installed on.

| Trigger | Matches (run events on `runs`) | Target | Reply |
| --- | --- | --- | --- |
| `github-issue-triage` (existing) | issue labeled `ai-triage` (webhook) | SWE agent | + `onComplete.emitTo: runs` |
| `factory-review` | `trigger in ["github-issue-triage", "factory-fix"]`, succeeded, has `outcome.pullRequest`, `counts["factory-review"] < 3` | SWE agent, `readOnly` | PR: `addLabels: [ai-review]`, `removeLabel: ai-review`; `emitTo: runs` |
| `factory-fix` | `trigger == "factory-review"`, `outcome.verdict == "changes-requested"`, `counts["factory-review"] < 3` | SWE agent (PR-triage prompt) | PR: `addLabels: [ai-triage]`, `removeLabel: ai-triage`; `emitTo: runs` |
| `factory-handoff` | `trigger == "factory-review"` and not routed to fix (verdict ≠ `changes-requested`, or the cap is reached) | `none` | param `assignee: sender.login`; PR: `assignees: ["{{assignee}}"]`, ack summarising the verdict |

Notes:
- The fix loop runs on the **PR**, not the issue: the review comments live on
  the PR, and issue triage would try to open a second PR.
- The handoff assigns the human who applied the original `ai-triage`, taken from
  the run event's re-verified sender, not a hard-coded login.
- `factory-review` carries the PR forward with
  `number: body.outcome.pullRequest.number`, falling back to
  `body.params.number` for a run that was already on the PR. Its `sessionKey`
  is the PR, so fix runs and re-reviews share one session per PR.

## Consequences

- The chain never crosses GitHub, so it is immune to the bot-drop guard and to
  webhook delivery gaps. GitHub labels become status displays, not the control
  plane.
- Every hop is still a CEL-matched EventTrigger. Changing the factory (another
  round, a different handoff, an extra step such as "run CI first") means
  editing CRs, not code.
- A run event names a real, re-verified human, so per-user credentials, audit
  and the ADR 0049 identity modes apply to every hop unchanged.
- Engines gain a pass-through (`outcome`) and a trivial target (`none`). Their
  CEL evaluation already works for any envelope body, but the new `kind: "run"`
  value joins the cross-engine CEL-parity fixtures.
- The CRD changes ship through the chart's generated `crds/` (`make manifests`).
  No new ClusterRole, so homelab's ci-deployer bootstrap step isn't needed.
- The one prompt-dependent step is the model running `aktis-verdict`, and it
  fails safe: no verdict means a human gets the PR.
- Risk: in v1, a lost hop (gateway restart) silently stalls the chain at the
  last label. Mitigated by labels showing where the PR stopped, and by the
  manual label path.

## Alternatives considered

- **Let the App's own webhooks route** (`dropBots: false` plus an allow-list of
  the App login, attributing events to the service identity). The chain would
  ride real GitHub events. But it removes the self-trigger guard for exactly the
  events that loop. It also loses the originating human, so each hop acts as the
  shared service identity. And the loop counter would have to be reconstructed
  from GitHub state. Rejected.
- **Run triage as the human (delegated identity)** so PRs and labels come from a
  person and existing triggers fire. This needs no new features, but whether the
  next step happens depends on the model choosing to apply a label. It has no
  loop bound, attributes every bot action to the human, and still can't express
  the handoff. Rejected.
- **Conditional reply actions on one trigger** (`reply.when: <cel>`) instead of
  run events. That covers "label or assign" after a review, but not "start the
  next run", which is the core of the loop. Run events subsume it. Rejected.
- **Parse the verdict from the review text** (a marker line in the final
  message). This is the model formatting prose, then a regex. An explicit
  command plus enum validation is the same model judgment with a much narrower
  failure mode. Rejected.

## Rollout

Each step lands green on its own and is inert until the next one uses it.

1. **This ADR.**
2. **Outcome plumbing.**
   - Both engines carry `AgentReply.result` to the invocation record and to
     `/invoke/:id` as `outcome` (object, ≤ 8 KiB).
   - The gateway's `OrchestratorInvokeResult` gains `outcome`.
   - The SWE agents (both) report `pullRequest` and `madeNewCommits`.
3. **Verdict.** `aktis-verdict` and runner validation in both SWE agents, so
   read-only runs report `verdict`. Update the review prompt in the chart's
   PR-review trigger and the examples.
4. **CRDs and engines.**
   - `EventSource.spec.run`, `EventTrigger.spec.onComplete.emitTo`,
     `reply.github.addLabels` / `assignees` and `target.none`, with controller
     validation (`emitTo` must name a `run` source; `none` excludes `readOnly`,
     `identity` and `promptTemplate`).
   - `make manifests generate`.
   - `target.none` in both engines.
   - `kind: "run"` added to the CEL-parity fixtures.
5. **Gateway.**
   - Run-event emission with chain metadata.
   - Per-hop sender re-verification and the global depth cap.
   - `addLabels` / `assignees` execution with the retry policy `removeLabel`
     uses.
6. **Factory config.**
   - Homelab: the `runs` source and the factory triggers in
     `values-production.yaml`, plus `onComplete.emitTo` on
     `github-issue-triage`.
   - The examples.
   - An e2e spec for one full loop (triage → review → fix → review → handoff)
     on both engines, including a check that the cap stops a loop and that
     `verdict: none` hands off.
7. **Bitovi.** After its image pin includes steps 2–5, the same config in
   `bitovi-platform-services`.
