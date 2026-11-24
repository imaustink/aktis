# 0055. Coordinating concurrent software-factory work

Date: 2026-11-23

## Status

Accepted. Extends [0052](0052-run-events-and-software-factory.md).

## Context

The software factory (0052) works tickets concurrently: every `ai-triage`
label starts its own triage → review → fix chain at once. After about a dozen
shipped PRs, the first bottleneck is merge conflicts between sibling factory
PRs. Two tickets touch the same code, both PRs are opened from the same base,
one merges, and the other now conflicts. Nothing notices until a human tries to
merge it.

Two kinds of fix are possible, and they are complementary:

- **Proactive:** before a ticket starts, decide how its work should integrate
  with the work already in flight: go ahead, wait until the overlapping PR
  merges, or stack on it.
- **Reactive:** when a factory PR does end up conflicting, resolve the conflict
  automatically instead of leaving it for the human reviewer.

Whether two tickets overlap is a judgment call, so it is the model's. But
following the practice behind #278 and 0052, everything around that judgment
must be deterministic: what counts as in flight, serializing decisions, holding
a ticket, releasing it, loop bounds. A model following a prompt must never be
what keeps a ticket from starting twice, or from waiting forever.

## Decision

### 1. The conflict sweep (`EventSource` kind `conflicts`)

A fourth source kind beside `webhook`, `cron` and `run`:

```yaml
spec:
  conflicts:
    author: "app/k5s-bot"      # GitHub search author: qualifier
    intervalSeconds: 300       # min 60
    skipLabels: [ai-triage, ai-review, ai-waiting]
```

Every interval, integration-gateway searches the open PRs `author` opened and
reads each PR's merge state. For each one GitHub reports as conflicting
(`mergeable: false`, `mergeable_state: "dirty"`), it emits a `kind: "conflict"`
event through the usual `/invoke` path, **once per (PR, base commit)**.

- **Skipped:** drafts; PRs carrying a `skipLabels` label, because a factory run
  is already on them and syncs the branch itself; PRs whose mergeability is
  still `null`, because GitHub computes it lazily after the base moves and the
  next sweep will see it.
- **Sender:** the PR's first assignee who still verifies through the identity
  resolver. Factory handoff assigns the human who started the chain, so that is
  normally them. It is never the bot and never whoever merged the other PR. A
  PR with no verifying assignee is skipped and logged.
- **Bounded:** the (PR, base sha) mark means a conflict the resync couldn't
  resolve costs one run per merge to the base, not one per sweep. A delivery
  the orchestrator couldn't receive is unmarked and retried. A per-interval
  mark means one gateway replica sweeps per interval.

`factory-resync` (`sourceRef: pr-conflicts`) merges the base into the PR's
branch as that assignee. It merges rather than rebases, because the agents
never force-push. Its completion goes to `factory-review`, or to
`factory-handoff` when it failed or pushed nothing, exactly like a fix.

### 2. Held completions (`onComplete.waitUntilClosed` / `serialize`)

Two gateway primitives on any trigger with `onComplete.emitTo`. Instead of
publishing the completion when the run ends, the gateway records it (Redis,
beside the in-flight runs) and publishes it once every hold is released:

- `waitUntilClosed: {repositoryParam, numberParam}`: until that issue or PR is
  closed or gone.
  - **Cancel:** closing the held run's own issue abandons the wait.
  - **Guards** (each publishes at once and logs why): unusable params, a wait
    on the run's own issue, a target that is already closed, and a **cycle**.
    A cycle is a target that is itself held waiting, directly or transitively,
    on this run's issue. Two tickets that each decided to wait for the other
    would otherwise wait forever.
- `serialize: {group, keyParam}`: one completion in flight per
  `<group>:<params[keyParam]>`.
  - **Holding:** the queue is taken when the completion is published and held
    until *the run that completion started* finishes. It is renewed with that
    run's in-flight lease, by whichever gateway is finishing the run, so a
    dead gateway frees it within minutes.
  - **Order:** oldest first.
  - **Not accepted:** a completion nobody accepted frees the queue at once.

The record is claimed (deleted) before it is published, so of several replicas
exactly one publishes it. The sweep runs every 30s, and also immediately after
a hold or a queue release.

**Depth.** A wait that really blocked (its target was open when the run ended)
restarts the chain's `depth` at 1 when it is published. Progress there was
gated on a human merging something, not a runaway loop. A ticket that waited
a couple of times must still have depth left for its review loop. `counts` are
kept, so trigger caps (at most 5 waits) still bound the chain. A wait that
didn't block (closed target, cycle) does not reset depth.

### 3. The coordinator (factory config)

```
ai-triage ─► factory-intake (none; serialize factory-coordinate:<repo>)
              └─► factory-coordinate (readOnly SWE agent; aktis-coordinate)
                    ├─ proceed / none / failed ─► factory-triage ─► factory-review …
                    ├─ stack <N>                ─► factory-triage-stacked ─► factory-review …
                    └─ wait <N>                 ─► factory-park (none; waitUntilClosed #N + serialize)
                                                    └─► factory-coordinate (again)
```

- **One at a time.** `factory-intake` and `factory-park` both hold their
  completion in the same `factory-coordinate:<repository>` queue. So a
  repository's issues are coordinated one at a time, and each coordinator sees
  the earlier ones as in flight. They keep `ai-triage` until their triage run
  ends, and parked ones carry `ai-waiting`. Without this, two issues labeled
  together would each see an empty board and both proceed: the collision this
  ADR exists to prevent.
- **The decision is recorded like a verdict.** The runner installs
  `aktis-coordinate proceed | wait <N> | stack <N>`. On a `readOnly` run whose
  prompt names that command, the runner reports
  `outcome.coordination: {decision, number?}` instead of a review verdict, and
  does not append the verdict section. A missing or invalid decision is `none`,
  which proceeds, so a confused coordinator costs nothing over having none.
- **Routing is CEL over that enum.** `factory-triage` is the exact complement
  of `factory-park` and `factory-triage-stacked`, so every coordinated issue is
  worked. A coordinator that keeps choosing `wait` proceeds after 5 waits.
- **Wait blocks until merged.** It blocks even on a PR that is waiting for
  human review: merging sibling work first is the point. A PR that is closed
  without merging releases the wait too, and the coordinator then decides
  again with a fresh board.
- **Stacking** branches from PR #N and opens the PR with `--base <#N's branch>`.
  When #N merges, GitHub retargets the stacked PR, and any conflict that leaves
  is the sweep's (§1). The coordinator is told to prefer `wait` unless the
  dependency is real.

### Ownership of `ai-triage`

`factory-intake` leaves the human's label on. `factory-coordinate` re-adds it
(for an issue returning from a wait) and clears `ai-waiting`. `factory-park`
swaps `ai-triage` for `ai-waiting`, and the triage run removes `ai-triage`
when it ends. The labels are status displays, as in 0052: they never route,
because bot-authored label webhooks are dropped.

## Consequences

- Conflicts between factory PRs are prevented where the coordinator sees the
  overlap, and resolved automatically where it doesn't. A human sees a
  conflicted factory PR only when an automated resolution failed, and then it
  arrives assigned with the reason.
- Every new mechanism is generic: a source kind and two `onComplete` holds. The
  factory itself is still only CRs, so changing the policy (another wait cap, a
  different queue key) means editing triggers.
- One coordinator run is added ahead of every factory ticket: minutes of
  latency, and one read-only agent run of cost. One repository's tickets are
  coordinated serially, though triage, review and fix runs still run
  concurrently.
- New GitHub reads per gateway: one search plus one PR read per open factory PR
  per interval, and one issue read per held wait every 30s. These are well
  within App rate limits at factory scale.
- In-memory fallback: without Redis, holds, queues and marks live in memory and
  a restart forgets them. The waiting issues stay labeled `ai-waiting` and need
  `ai-triage` re-applied. Every deployment that runs the factory has Redis.
- The e2e overlay removes `factory-intake` (it outranks the `github-issue-triage`
  the specs drive) and the conflict sweep (it would call GitHub on a timer).
  Coordination routing is covered by a unit test that compiles every example
  trigger and routes each decision (`examples.test.ts`), and by the gateway's
  held-event and sweep tests.

## Alternatives considered

- **Serialize by `sessionKey` in the engine.** Each session is one
  conversation workflow, but concurrent turns in one conversation are not
  serialized. A shared session would also make the coordinator's sticky
  read-only mode and agent continuation span unrelated issues. Rejected in
  favour of an explicit queue.
- **Let the coordinator pause itself** (poll and sleep in its turn). This
  depends on the prompt, holds a pod for days, and loses the wait on any
  restart. Rejected.
- **Only the reactive sweep.** It is cheaper and catches what any prediction
  misses, but it repairs after the fact. Two tickets that genuinely overlap
  then cost a resync, a re-review, and a human reading a merge resolution.
  Both ship together.
- **Fan conflict re-syncs out from the merge webhook.** A merge is a human
  event and would route, but GitHub's `mergeable` is still `null` right after
  the base moves, and webhooks get lost. A periodic sweep is simpler and
  self-healing. Rejected.
