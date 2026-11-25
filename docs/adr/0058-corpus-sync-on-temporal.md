# 0058. connection-broker syncs each Corpus from a Temporal workflow

Date: 2026-11-25

## Status

Proposed. Applies [0057](0057-event-runs-on-temporal.md)'s pattern to the
Corpus sync of [0038](0038-connection-crd-scoped-external-resources.md) §4 and
[0043](0043-connection-corpus-knowledgebase.md).

## Context

A Corpus's reconcile pass had two schedulers: an in-process `setTimeout` loop
per Corpus in connection-broker (`SyncScheduler`), and a CronJob per Corpus,
created by core-controller, that POSTed `/corpora/:name/sync`. Each kicked the
same pass, and the CronJob got a 409 whenever the timer's pass was running
(which its Job then counted as success). Beyond the duplication:

- "passes never overlap for one corpus" was an in-memory `Set`, true only at
  `replicaCount: 1`. Two overlapping full passes can each read the other's
  freshly written chunks as absent and delete them;
- a webhook arriving while a pass ran was dropped until the next full
  reconcile;
- an interrupted pass restarted from zero at the next tick, and the timer
  only picked up Corpora that existed when the process started.

## Decision

1. **Each synced Corpus has one long-lived `corpusSync` workflow**, run by
   connection-broker's own worker on the engine's Temporal cluster and
   namespace (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`), task queue
   `connection-broker` (`TEMPORAL_TASK_QUEUE`). Its id is
   `<taskQueue>/corpus-sync:<corpus>` (0057 §5). Every replica starts it (if
   not running) at startup and every minute for each Corpus that is syncable
   -- bound, with a collection, a `reconcileInterval` and a sync token, the
   same filter as before. Since the workflow id is the Corpus's, that is one
   loop however many replicas ask, and its passes never overlap.
2. **The loop**: a full pass when it starts, then wait for the interval or a
   signal; repeat. The Corpus is re-read (an activity) after every pass and
   every five minutes while waiting, so an edited interval applies and a
   deleted or no-longer-syncable Corpus ends its loop. It continues as new
   every 200 turns (or when Temporal suggests it), carrying the time of the
   last full pass and any pending work.
3. **A pass is an activity** over the existing `syncConnection`, heartbeating
   per listed page and fetched resource (2-minute heartbeat timeout, 6-hour
   budget, 3 attempts). A broker that dies mid-pass is noticed within the
   heartbeat timeout and the pass is retried **from the start** on another
   replica: listing completes before any fetch, so a cursor checkpoint would
   also have to carry every ref listed so far, and a repeated pass is
   idempotent (content-hash point ids) and costs no embeddings for what it
   already wrote. The Corpus's status write-back (`CorpusStatusWriter`)
   happens inside the activity, after the pass, as before.
4. **Webhooks are a `sourcesChanged` signal** (signal-with-start, so a
   delivery to a Corpus with no running loop starts one). The named source
   ids accumulate in workflow state until a pass has examined them, so one
   that arrives mid-pass is the next pass's work, not dropped. A burst is
   gathered for 5 seconds into one partial pass. An empty list escalates to a
   full pass. A partial pass still deletes only what it examined (worker.ts
   and reconcile.ts are unchanged); a partial pass that fails leaves its ids
   to the next full pass.
5. **`POST /corpora/:name/sync` is a `syncNow` update** that asks for a full
   pass and returns its result once a pass that *started after the request*
   finishes: 200 `{indexed, removed, full}`, 502 when the pass failed after
   its retries, 404 when the broker does not sync that Corpus. A pass already
   running is not joined -- it may have listed the source before whatever
   prompted the request. There is no 409 any more.
6. **core-controller no longer creates a sync CronJob.** It deletes one an
   earlier release created (named `corpus-sync-<corpus>` and controlled by a
   Corpus of that name) on its next reconcile of the Corpus, so an upgrade
   cleans up. Its `cronjobs` RBAC stays: cron EventSources use it. The
   `core-controller.corpusSync` chart values and the `CONNECTION_BROKER_URL`,
   `CORPUS_SYNC_TOKEN_SECRET` and `CORPUS_SYNC_KICK_IMAGE` controller env are
   gone.
7. **Changing the workflow follows 0057 §7**: `patched()` for any change to
   the commands it issues; `src/sync/replay.test.ts` replays the histories in
   `src/sync/__histories__` (captured by `src/sync/capture-histories.test.ts`)
   and fails on an ungated change.

## Consequences

- **Temporal is required whenever the broker indexes** (`QDRANT_URL`,
  `OPENAI_API_KEY` and at least one `SYNC_TOKEN_*` set): it exits at startup
  if `TEMPORAL_ADDRESS` is unset. A broker that only serves (no index
  configuration) starts no worker and no workflows, as before. The chart sets
  `connection-broker.temporal.{address,namespace,taskQueue}`, defaulting to
  the engine's (`temporal-frontend.temporal.svc:7233`, `default`); the
  namespace must already exist.
- A new syncing Corpus gets its first full pass within about a minute of the
  broker seeing it; before, the timer only knew the Corpora present at
  startup and a new one waited for its CronJob's schedule. A broker restart
  no longer triggers a full pass of every Corpus.
- `replicaCount` above 1 is now safe for indexing: passes for one Corpus are
  serialized by its workflow, and webhook signals and run-now requests reach
  it from any replica.
- The image carries the Temporal worker's native core (glibc; the image was
  already Debian), and the workflow is bundled at build time.
- A worker holds up to 20 passes at once; more wait in the task queue for any
  replica.
- The broker's sync tests run against a real Temporal dev server
  (`src/__fixtures__/temporal.ts`), on a task queue per test.

## Alternatives

- **Temporal Schedules per Corpus**: would have to be created, updated and
  deleted as Corpora change, and would still need something to serialize
  webhook passes against scheduled ones. A long-running workflow per Corpus
  that re-reads its Corpus needs none of that (0057's reasoning for conflict
  sweeps).
- **Keep the CronJob as the only scheduler**: ForbidConcurrent serializes the
  kicks, not the passes (a restarted broker or a second replica still runs
  them concurrently), and webhooks would still need somewhere durable to
  wait.
- **Resume a retried pass from a listing cursor**: see Decision 3.
