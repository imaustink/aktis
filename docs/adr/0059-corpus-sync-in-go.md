# 0059. Corpus sync moves to a Go worker; the broker keeps the drivers

Date: 2026-11-26

## Status

Proposed. Moves [0058](0058-more-durable-work-on-temporal.md) §2's
`corpusSync` workflow out of connection-broker and onto the Go Temporal SDK.
Revisits [0056](0056-agent-orchestrator-in-go.md) §4, which kept the whole
broker in TypeScript "for document parsing". Keeps [0038](0038-connection-crd-scoped-external-resources.md)
§3's boundary: the indexer reads sources only through the broker's HTTP API,
with a per-Corpus sync token.

## Context

After 0058, connection-broker was two programs in one process:

1. **The API**: drivers for Confluence, Slack, Drive and JazzHR; PDF, HTML
   and Office extraction (`unpdf`, `parse5`, `fflate`); probes, listing,
   fetch and webhooks. This is the part 0056 kept in TypeScript, and the
   reason still holds. Go has nothing as good as pdf.js for PDF text.
2. **The indexer**: a Temporal worker running one long-lived `corpusSync`
   workflow per Corpus. A pass lists and fetches **through the API above over
   HTTP**, even inside the same process, then chunks, embeds, reconciles
   against Qdrant and writes the Corpus status.

The indexer was the only Temporal workflow code left on the TypeScript SDK.
Everything else, including the engine and agent-orchestrator, uses the Go SDK.
That meant two SDKs with two replay harnesses, two conventions for patching
workflow changes (`patched()` vs `GetVersion`), a webpack bundling step in
the broker's build, and a native Temporal core that forced the broker image
onto glibc. It also gave the indexer the broker's cluster reach: Connections,
and the Secrets holding every client's third-party credentials, which the
indexer never reads.

The indexer touches no parser. The engine module already holds the rest of
what it needs: a Qdrant client whose point ids and payload are the ones the
broker's writer was written to match (`vectorstore.Qdrant`), and the
embedder (`internal/llm`).

## Decision

### 1. The indexer is `corpus-sync`, a Go binary in the engine module

`orchestrator/engines/temporal/cmd/corpus-sync` builds on
`internal/corpussync`, with its own image (`temporal-engine-corpus-sync`,
`Dockerfile.corpus-sync`). It is deployed by the connection-broker subchart as
a second Deployment (`<broker>-sync`), because it is the broker's indexing
feature. It renders whenever the broker indexes: `qdrant.url`, an
embeddings key and `temporal.address` are all set.

The Deployment has **its own ServiceAccount** with get and list on `corpora`
and patch on `corpora/status`, in the knowledge-base namespace. It does not
get Connections or Secrets. The broker's Role loses `corpora/status`, since
the broker no longer writes status.

### 2. Same workflow, new task queue

The workflow (`CorpusSync`) keeps 0058 §2's behavior:

- an immediate first pass, or one scheduled from the status's
  `lastReconcileTime`;
- the 5-minute recheck and the 5-second webhook settle;
- `sourcesChanged` signals and the `syncNow` update;
- continue-as-new every 200 turns or when the server suggests it;
- the same activity timeouts and retry policies.

The payload shapes stay the same: the input `{corpus, lastFullPassAt?,
pendingSourceIds?, fullRequested?}`, the signal argument `string[]`, and the
update result `{indexed, removed, unchanged, failed, failedCount, full}`. Its
failure types are still `SyncFailed` and `CorpusNotSynced`.

The default task queue is `corpus-sync` (`CORPUS_SYNC_TASK_QUEUE`), and
workflow ids are `<taskQueue>/corpus-sync:<corpus>` (0057 §5). Changes to the
workflow are gated by `workflow.GetVersion`, and a replay test runs the
committed histories in `internal/corpussync/testdata/histories`.

### 3. A pass reads the Corpus from the API server, not a watch

`SyncTarget` and `RunPass` GET the Corpus each time they run. That is one
request per recheck, so cost doesn't matter. A Corpus is synced if:

- it has `status.collection`;
- its `spec.sync` has a mode other than `none` and a parseable
  `reconcileInterval`;
- this process has its `SYNC_TOKEN_<CORPUS>`.

A Corpus that is missing or no longer syncable answers "not synced", and its
loop ends. 0058's "this replica cannot see it yet" path
(`CorpusNotVisibleHere`, the `confirm` probe, the last-attempt escape) goes
away. There is no watch cache to lag, and every replica of one Deployment has
the same tokens.

The indexer doesn't know whether the broker could bind the Corpus to its
Connection. A Corpus the broker can't bind fails its passes (the broker
answers its list with 404) instead of quietly having no loop. The failure is
logged each interval and is visible in the workflow's history.

### 4. The broker keeps a Temporal client, not a worker

Webhooks are still `signalWithStart(sourcesChanged)`, and `POST
/corpora/:name/sync` is still `syncNow`, both on the new task queue. The
broker drops `@temporalio/worker`, `/workflow` and `/activity`, the workflow
bundle, the replay histories, and the chunker, reconciler, Qdrant writer,
embedder and status writer. It still treats a Corpus as synced when the
Corpus is bound, has a collection and an interval, and has a sync token, as
long as `TEMPORAL_ADDRESS` is set. The chart sets `TEMPORAL_ADDRESS` only when
the sync Deployment is rendered, so the broker never waits on a queue that
has no worker.

`corpus-sync` makes sure each synced Corpus has a loop: at startup and every
minute, it starts the workflow with `USE_EXISTING`.

The names, the task queue and the payloads above are now a contract across
two languages. Each side declares them once, with a `PARITY` comment pointing
at the other: `internal/corpussync/contract.go` and the broker's
`src/sync/contract.ts`. The knowledge-base e2e specs exercise both directions
through `/sync`.

### 5. Indexing output is the same bytes

Chunking, `contentHash`, point ids and the point payload are unchanged, so
switching over re-embeds nothing. Chunk sizes are counted in UTF-16 code
units, the character cut slices UTF-16, and `\s` and `trim` use
JavaScript's whitespace set. Those rules are ported exactly because they
decide which chunk boundaries and hashes come out. The test is
`testdata/ts-parity.json`, written by the TypeScript implementation before it
was removed: chunking over awkward inputs (CRLF, Unicode whitespace, emoji at
cut points, CJK, oversized lines), hashes, and embedding batch sizes.

The embedder keeps the broker's batching: at most 2048 inputs and an
estimated 250k tokens per request, with a 400 split in half and retried down
to one input. `llm.Embedder` returns a typed `*llm.StatusError` so the split
can tell a 400 from other failures.

Qdrant is reached over gRPC, like the engine. The chart passes the host from
`qdrant.url` and `qdrant.grpcPort` (default 6334).

### 6. Migration

Loops started by a 0058 broker run on the `connection-broker` task queue, and
nothing polls that queue any more. At startup, `corpus-sync` terminates
`<LEGACY_TASK_QUEUE>/corpus-sync:<corpus>` for every Corpus it syncs. The
default is `connection-broker`, and an empty value turns this off. A missing
workflow is not an error. The new loops schedule from `lastReconcileTime`, so
switching over doesn't set off a full pass for every Corpus. Remove the drain
one release after this ships.

## Consequences

- **The broker is a TypeScript HTTP service and nothing else.** No workflow
  code, no native Temporal core and no bundling step. Its image could go back
  to Alpine. The engine module holds all the Temporal workflow code, with one
  replay and versioning convention.
- **Indexing is two images.** Turning it on now means deploying
  `temporal-engine-corpus-sync` as well. The chart does that as part of the
  broker; release publishes it with the engine's images under the
  `temporal-engine` path filter.
- **Indexing and the API scale and fail separately.** A memory spike from PDF
  extraction no longer takes down passes in progress, and an embedding-heavy
  first sync doesn't share a pod with probes. Passes cross the network to the
  broker Service instead of loopback, which a pass that reads a whole source
  hardly notices.
- **The indexer can reach less.** It reads and patches Corpora and holds the
  sync tokens, Qdrant and an embeddings key. The broker holds third-party
  credentials and can no longer write status.
- **Two languages share the names in §4.** That is weaker than 0058, where
  one TypeScript module defined them. The e2e suite is the cross-check.
- **A Corpus the broker can't bind now fails visibly** each interval instead
  of having no loop.

## Alternatives

- **A Go sidecar in the broker pod.** It would keep loopback and share the
  broker's ServiceAccount, which reaches every Connection's Secrets. A
  separate Deployment costs one Service hop and buys least privilege and
  independent scaling.
- **Port the broker whole.** That means the PDF, HTML and Office extraction
  too, where Go's libraries are weaker, and it would change retrieval quality
  in ways no parity fixture could catch. The drivers sit behind an HTTP
  contract, which is where language stops mattering (0056 §4).
- **Replay the TypeScript histories on the Go worker** (same queue and ids).
  The two SDKs don't promise to produce identical commands for the same code,
  and a non-determinism error would wedge every Corpus's loop. Starting fresh
  loops costs nothing, because each schedules from its status.
- **Broker forwards webhooks and `/sync` to `corpus-sync` over HTTP.** That
  would take Temporal out of the broker entirely, but it adds a new
  authenticated endpoint and a third hop for something a Temporal client
  already does directly.
