# 0058. The remaining hand-built durability moves onto Temporal

Date: 2026-11-25

## Status

Proposed. Continues [0057](0057-event-runs-on-temporal.md), which moved
integration-gateway's event runs off Redis leases and onto Temporal, into the
four other places that still kept work alive by hand. Applies 0057's
conventions throughout: workflow ids prefixed with the task queue (0057 §5),
`patched()` for any change to a workflow's commands with replayed histories
enforcing it (0057 §7), tests against a real Temporal dev server.

§2's workflow moved out of connection-broker to the Go `corpus-sync` worker,
with the same behavior, in [0059](0059-corpus-sync-in-go.md).

## Context

After 0057, four components still did durable work in process memory,
guarded by a Redis record, an in-memory lock or a per-replica timer:

1. **agent-orchestrator's `/invoke`** accepted a turn, answered 202, and
   drove the turn from a goroutine: start it on the engine gateway, poll the
   gateway, write the terminal record to Redis (`inv:<uuid>`). If the pod
   died, a later poll "adopted" the turn from the engine id it had saved.
   A pod that died after the engine accepted the turn but before the engine
   id was saved left a record pending forever: there was nothing to adopt
   by. And every event run was polled three times over: integration-gateway
   polled the facade, the facade polled the engine gateway, the engine
   gateway queried Temporal -- where the turn already was a durable update.
2. **connection-broker's Corpus sync** had two schedulers, an in-process
   `setTimeout` per Corpus and a core-controller CronJob per Corpus that
   POSTed `/corpora/:name/sync` (409 while the timer's pass ran, which the
   Job counted as success). "Passes never overlap for one corpus" was an
   in-memory `Set`, true only at `replicaCount: 1`, and two overlapping full
   passes can each read the other's fresh chunks as absent and delete them.
   A webhook arriving mid-pass was dropped until the next full reconcile; an
   interrupted pass restarted from zero; the timer only knew the Corpora
   present at startup.
3. **integration-gateway's Claude credential refresh.** Anthropic rotates
   the refresh token on every refresh, so two concurrent refreshes of one
   subject invalidate each other and the user has to link again. The guard
   was an in-memory per-subject promise, and the periodic renewal a
   `setInterval` in every replica -- both written for a "single-instance"
   gateway the chart does not guarantee, and rollouts overlap pods anyway.
4. **The engine gateway's caller-tool prune** (0035) was a ticker in every
   gateway replica. Idempotent, so harmless, but N passes per interval.

## Decision

### 1. `/invoke` ids name the engine's turn; nothing is recorded

`POST /invoke` waits for the engine gateway to accept the turn (a Temporal
update, admitted and durable) before answering 202, and the id it returns is
the engine's id with a prefix: `t.<engine id>`. `GET /invoke/:id` asks the
engine once and maps the answer. Any replica answers any poll, a restarted
one included, because the id is all a poll needs; no goroutine drives the
turn, and adoption, the Redis invocation record and the pending-forever
window are gone.

- The session's sticky read-only flag is still written **before** the turn
  starts, as before.
- A turn that cannot start (the engine refused it, unreachable, the session
  flag could not be written) is still a **202**, with a stateless
  `failed.<base64url message>` id that polls as `failed`, so
  integration-gateway's failure path is unchanged. A `target.none` turn gets
  `none.<uuid>`, polling as succeeded with result `""`.
- A poll that cannot get an answer from the engine (transport error, 5xx,
  or a 408/429 from a proxy in front of it) answers `pending` (the caller asks again) rather than failing the turn; an
  id the engine does not know or rejects as malformed (404, 400) is a 404;
  any other 4xx from the engine (a wrong engine token) fails the turn, since
  it would repeat on every poll. Forging an id gains nothing: the caller already holds the `/invoke`
  token, an invented engine id is unknown, and the other shapes only echo
  what they say.
- A run outcome is passed through after `CheckOutcome`. The facade no
  longer gates it on "was this a forced agentRef turn": the engine's
  conversation workflow already clears it on every other turn.
- The chat endpoint is unchanged: it holds its stream open and polls the
  engine itself, so a parked identity link still auto-resumes while the
  user is watching.

### 2. Each synced Corpus has one long-lived `corpusSync` workflow

Run by connection-broker's own worker on the engine's Temporal cluster and
namespace (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`), task queue
`connection-broker` (`TEMPORAL_TASK_QUEUE`), id
`<taskQueue>/corpus-sync:<corpus>`.

- **Started by every replica** (if not running) at startup and every minute
  for each syncable Corpus -- bound, with a collection, a
  `reconcileInterval` and a sync token, the same filter as before. One loop
  per Corpus however many replicas ask, so passes never overlap.
- **The loop**: a full pass when it starts, then wait for the interval or a
  signal; repeat. The Corpus is re-read after every pass and every five
  minutes while waiting, so an edited interval applies and a deleted or
  no-longer-syncable Corpus ends its loop. A replica that cannot see the
  Corpus asks the Kubernetes API before answering "gone"; if the cluster
  still syncs it (the replica's watch lags, or it lacks a token a newer pod
  has) the activity fails retryably instead, so one lagging replica does not
  end the loop for all. Its last attempt answers "gone" all the same, so a
  Corpus no replica holds a token for ends its loop rather than failing the
  workflow; a replica that does see it starts the loop again. A loop started afresh schedules from the Corpus
  status's `lastReconcileTime` rather than passing at once. It continues as
  new every 200 turns (or when Temporal suggests it).
- **A pass is an activity** over the existing `syncConnection`,
  heartbeating per listed page, fetched resource, page of the index read
  and batch embedded and written (2-minute heartbeat timeout, 6 hours an
  attempt, 10 attempts within 12 hours, backoff capped at 5 minutes -- a
  rolling restart costs a running pass an attempt). A retried pass restarts from the
  beginning: listing completes before any fetch, so a cursor checkpoint would
  also have to carry every ref listed so far, and a repeated pass is
  idempotent and re-embeds nothing it already wrote. The status write-back
  happens inside the activity, as before.
- **Webhooks are a `sourcesChanged` signal** (signal-with-start). Named
  source ids stay in workflow state until a pass has examined them, so one
  arriving mid-pass is the next pass's work, not dropped; a burst is
  gathered for 5 seconds; an empty list escalates to a full pass. A partial
  pass still deletes only what it examined.
- **`POST /corpora/:name/sync` is a `syncNow` update**, answered by a full
  pass that *started after the request*: 200 `{indexed, removed, full}`,
  502 when it failed after its retries or the broker is not yet connected
  to Temporal, 404 when the broker does not sync that Corpus (including one
  that stopped being synced while the request waited). No 409 any more.
- **core-controller no longer creates a sync CronJob.** It deletes one an
  earlier release created (`corpus-sync-<corpus>`, controlled by that
  Corpus) on its next reconcile. Its `cronjobs` RBAC stays for cron
  EventSources. The `core-controller.corpusSync` chart values and the
  `CONNECTION_BROKER_URL`, `CORPUS_SYNC_TOKEN_SECRET` and
  `CORPUS_SYNC_KICK_IMAGE` controller env are gone.
- Changes to the workflow are gated as 0057 §7 says;
  `src/sync/replay.test.ts` replays `src/sync/__histories__`.

### 3. Claude credential refresh is serialized by workflow id

- A due refresh is a `claudeCredentialRefresh` workflow on
  integration-gateway's worker, id
  `<queue>/claude-credential-refresh:<URI-encoded subject>`, started with
  `USE_EXISTING` so every replica asking at once attaches to one run. Its one
  activity re-reads the store, re-checks, refreshes and persists, and is
  **never retried**: a retry could spend a token Anthropic already rotated.
- A healthy blob (or one with no refresh token) is served without touching
  Temporal. If Temporal errors or does not answer within 30s, the stored
  blob is served unrefreshed and the failure logged loudly -- never a local,
  unserialized refresh.
- Tokens stay out of history: a persisted refresh is read back from the
  store; only a refresh that could not be persisted -- whose new blob is then
  the only live copy -- returns it, sealed under the credential store's
  AES-GCM key, so the caller on whichever replica can still use it.
- The periodic renewal is one `claudeCredentialSweep` workflow
  (`<queue>/claude-credential-sweep`), started if not running by every
  replica, refreshing each due subject through a child
  `claudeCredentialRefresh` of the same id (a subject already refreshing is
  skipped), continuing as new to bound history, and ending when refresh is
  disabled or the store cannot enumerate subjects.

### 4. The caller-tool prune is one Temporal Schedule

The engine gateway keeps a Schedule, id
`<taskQueue>/caller-tool-prune:<collection>`, every 6 hours with overlap
skipped, whose action is a `CallerToolPruneWorkflow` on the engine task
queue; the worker runs its `PruneCallerTools` activity against the same
Qdrant (the chart gives both the same settings). Every replica creates it,
or updates it in place to its own interval and retention, retrying with a
capped backoff until it succeeds. A failure to set it up is logged, not
fatal: stale definitions only live longer.

agent-orchestrator's own caller-tool store keeps its ticker. It may live in a
different Qdrant with a different TTL, and that process runs no Temporal
worker to hand the pass to; giving it one only for a prune is not worth it.

## Consequences

- **`POST /invoke` waits for the engine's accept** (normally well under a
  second; bounded at 60s), where it used to answer at once. A webhook relay
  therefore spends slightly longer before its own 202.
- **The facade's 30-minute poll budget is gone.** It used to turn a turn
  still running after 30 minutes into a "succeeded" record saying "that's
  taking longer than I can wait"; now the caller sees `pending` until the
  turn ends, and its own poll budget decides. An answer is collectable for
  as long as the engine retains the update.
- **Ids a pre-0058 pod handed out** (bare UUIDs) were answered from Redis,
  read-only, for one release; since then they are a 404 and the facade
  reads no `inv:` records. `AGENT_INVOCATION_TTL_SECONDS` is no longer read.
- **Temporal is required whenever connection-broker indexes** (`QDRANT_URL`,
  `OPENAI_API_KEY` and at least one `SYNC_TOKEN_*`): it exits at startup if
  `TEMPORAL_ADDRESS` is unset. A Temporal that cannot be reached does not
  take the broker down: the worker is started, and restarted, in the
  background, logging loudly, while the probe, retrieval and fetch API keeps
  serving; webhooks that arrive meanwhile are left to the next full pass. A
  serve-only broker starts no worker. The
  chart sets `connection-broker.temporal.{address,namespace,taskQueue}`,
  defaulting to the engine's; the namespace must already exist.
- A new syncing Corpus gets its first full pass within about a minute; a
  broker restart no longer re-syncs every Corpus; `replicaCount` above 1 is
  safe for indexing; a worker holds at most 20 passes at once.
- A due Claude refresh costs a Temporal round trip, and a read may wait up
  to 30s for it. The in-pod CLI's own write-back
  (`POST /claude-auth/api/refresh`) is still not serialized with gateway
  refreshes; that race predates this.
- **During the rollout that ships this**, an old gateway pod's in-memory
  refresh or old broker's timer can still race a new pod's workflow once.
- The engine worker registers `CallerToolPruneWorkflow` unconditionally and
  its activity only with Qdrant; the workflow's ScheduleToClose timeout
  ends a pass no worker can run.

## Alternatives

- **Facade-owned invocation workflows** (a workflow per `/invoke` that
  starts and polls the engine turn): durable, but a second workflow wrapped
  around one that is already durable, plus a worker in the facade. The
  engine's update id already names everything a poll needs.
- **Temporal Schedules per Corpus**: would have to be created, updated and
  deleted as Corpora change, and still need something to serialize webhook
  passes against scheduled ones (0057's reasoning for conflict sweeps).
- **Keep the CronJob as the only Corpus scheduler**: `ForbidConcurrent`
  serializes the kicks, not the passes, and webhooks would still need
  somewhere durable to wait.
- **A Redis lock for Claude refreshes**: works, but is the lease-and-TTL
  machinery 0057 set out to stop rebuilding, and a lock held by a dead pod
  blocks every refresh for that subject until it lapses.
