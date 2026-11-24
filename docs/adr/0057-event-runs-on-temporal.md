# 0057. integration-gateway runs each event's run as a Temporal workflow

Date: 2026-11-24

## Status

Proposed. Builds on [0052](0052-run-events-and-software-factory.md) (run
events), [0055](0055-factory-coordination.md) (held completions, serialize
queues, the conflict sweep) and [0056](0056-agent-orchestrator-in-go.md).

## Context

Everything integration-gateway does after the orchestrator accepts an event --
ack, poll the turn, post the result, call back, remove the trigger label,
publish the run-completion event -- used to be one in-process promise. Each
time that promise was lost in production, another Redis store was added to
survive it:

- `label-reconciler.ts`: an outbox of owed label removals and a sweep, after
  labels were stranded by pods dying mid-run ("removal only works ~3/4 of the
  time");
- `inflight-runs.ts`: each accepted run under a renewed lease, step flags
  marking what was done, and a recovery sweep that took over a lapsed lease,
  after a rollout stopped a software-factory chain (glyph#99);
- `deferred-events.ts` (0055): held completions with a claim-before-publish,
  a 24-hour publish retry, serialize queues as TTL locks renewed by the
  running run, and once-marks plus a per-source lock for the conflict sweep.

Together these are a hand-built durable workflow engine: leases, takeover,
idempotency flags, timers, mutexes and dedupe keys, each with its own failure
modes (a lease too short, a lock renewed by a dead pod, a mark that lapses).
The stack already runs Temporal for the engine.

## Decision

1. **Each accepted event's run is a Temporal workflow** (`eventRun`) run by
   integration-gateway's own worker, on the engine's Temporal cluster and
   namespace (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`), task queue
   `integration-gateway`. Its steps are activities over the same code that ran
   them before (`src/runs/effects.ts`). The poll is one heartbeating activity:
   if its worker dies, Temporal retries it on another replica, which polls the
   same invocation -- the orchestrator answers it whichever process asks.
   Label removal is an activity retried for an hour.
2. **Held completions are `heldRunEvent` workflows**: a timer re-checks
   `waitUntilClosed` (every `GATEWAY_HELD_EVENT_STATE_CHECK_INTERVAL_MS`),
   continuing as new so a weeks-long wait keeps a bounded history. The cycle
   guard reads the running holds' memos through Temporal's visibility API.
3. **A serialize queue is a `runQueue` workflow** per `<group>:<key>`: held
   events join it by signal-with-start and get their turn by signal; the run a
   turn started releases it when it finishes. A holder that never releases is
   passed over after a timeout computed from the run settings
   (`queueHoldTimeoutMs`) to outlast any live holder, so two runs from one
   queue never overlap.
4. **A conflicts source is a `conflictSweep` workflow**: one per source,
   started (if not running) by every replica, ending when the source goes
   away. Its once-per-(PR, base commit) marks are workflow state.
5. **Workflow ids are prefixed with the task queue** (`<queue>/event-run:<id>`,
   ...), so two releases sharing a namespace never share a queue or a sweep.
6. **The workflows are TypeScript, in integration-gateway**, not Go in the
   engine. Every step is gateway code (GitHub replies, sender re-verification,
   the orchestrator relay, the session page, the Claude link store), so the
   gateway needs a worker for the activities either way; Go workflows would
   only move GitHub-reply rules into the engine and add a cross-language
   contract on every step.

## Consequences

- `inflight-runs.ts`, `label-reconciler.ts` and `deferred-events.ts` no longer
  record anything. `src/runs/legacy-drain.ts` hands what pre-0057 gateways
  left in Redis to the workflows (lapsed in-flight runs, held events claimed
  as the old sweep claimed them, owed label removals). **Remove the drain and
  those modules one release after this ships.** During the rolling update an
  old pod may still hold a Redis serialize queue while a new run takes the
  Temporal one, so two runs of one queue can overlap once.
- A completion whose publish fails is retried for 24 hours whether or not it
  was held; before, only held ones were, and an immediate publish that failed
  was lost.
- Publishing a run or conflict event is an activity that is **never retried**:
  every attempt submits its own envelope and the orchestrator accepts each as
  a new run, so repeating an attempt that died after `/invoke` accepted would
  start the next hop twice. Such an attempt (a worker crash mid-publish) is
  logged and not published again -- at most once, as before. A publish the
  orchestrator answered as failed is still retried for 24 hours.
- A gateway rollout costs a run nothing: the worker cancels its activities on
  shutdown, and their retries start on the new pods at once.
- **Temporal is required** for the gateway, as it already was for the engine
  (0053). `integration-gateway.temporal.namespace` must name a namespace that
  already exists, normally the engine's: it defaults to `default`, and the
  gateway does not register namespaces. The gateway image carries the Temporal worker's native core
  (prebuilt for glibc; the image is Debian), and the workflow code is bundled
  at build time (`npm run build`), so a pod never runs webpack.
- One worker activity slot per live run (a poll in flight), so the worker
  allows 1000 concurrent activities.
- The gateway's run tests run against a real Temporal dev server
  (`src/__fixtures__/temporal.ts`, downloaded on first use), on a task queue
  per test.
- Redis is still used for what is state, not process: session pages and the
  orchestrator's session and invocation records.

## Alternatives

- **Go workflows in the engine** with TypeScript activities: see Decision 6.
- **Temporal Schedules for conflict sweeps**: a schedule per source would
  have to be created, updated and deleted as the CRs change; a long-running
  workflow that reads its source each pass needs none of that.
- **Keep the Redis stores**: they work, but each new durable behaviour (0055
  added three) re-implemented leases, claims and retries, and each was a new
  way to lose a run.
