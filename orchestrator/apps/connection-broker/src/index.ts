/**
 * The connection-broker process.
 *
 * Two jobs in one Deployment, both of which need the same third-party
 * credentials and neither of which may hold cluster RBAC (ADR 0038 §3):
 *
 *   - Serves the HTTP API: per-user authorization probes for retrieval, and
 *     listing/fetch for ingestion.
 *   - Runs each Corpus's reconcile passes, as a Temporal workflow per Corpus
 *     (docs/adr/0058) on this process's own worker.
 */
import * as k8s from "@kubernetes/client-node";
import { createBrokerServer, SyncFailedError } from "./server.js";
import { CrdCorpusRegistry } from "./crd-corpus-registry.js";
import { collectionOf, reconcileIntervalMs, type CorpusCustomResource } from "./corpus-resource.js";
import { EMBEDDING_DIMENSIONS, OpenAIEmbedder } from "./embedder.js";
import { QdrantCorpusWriter } from "./sync/corpus-writer.js";
import { HttpResourceSource } from "./sync/http-source.js";
import { createSyncActivities, type SyncTarget } from "./sync/activities.js";
import type { Worker } from "@temporalio/worker";
import { connectClient, createSyncWorker, CorpusSyncs, SyncPassError } from "./sync/temporal.js";
import { DEFAULT_TASK_QUEUE } from "./sync/model.js";
import { QdrantHttpClient } from "./sync/qdrant-client.js";
import { CorpusStatusWriter } from "./corpus-status.js";
import { CORPUS_PLURAL } from "./crd-corpus-registry.js";

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    // Fail at startup rather than on the first request. A broker running
    // without its orchestrator token would accept nothing and look like a
    // networking problem.
    console.error(`${name} is required`);
    process.exit(1);
  }
  return value;
}

async function main(): Promise<void> {
  const namespace = process.env.NAMESPACE ?? "default";
  const group = process.env.CRD_GROUP ?? "core.aktis.ai";
  const version = process.env.CRD_VERSION ?? "v1alpha1";
  const port = Number(process.env.PORT ?? 8080);

  const kubeConfig = new k8s.KubeConfig();
  kubeConfig.loadFromDefault();

  const registry = CrdCorpusRegistry.fromKubeConfig(
    namespace,
    group,
    version,
    kubeConfig,
    (corpus: string, err: unknown) => {
      // Reported, never fatal: one malformed Corpus must not stop the broker
      // serving every other client's.
      console.error(`corpus ${corpus} could not be bound:`, err);
    },
  );

  await registry.loadAll();
  registry.watch();
  console.log(`bound ${registry.list().length} corpus/corpora in ${namespace}`);

  // Sync tokens are per CORPUS, so a leaked one reaches one client's subset
  // rather than a whole workspace. They arrive as SYNC_TOKEN_<CORPUS>.
  const syncTokens = new Map<string, string>();
  for (const [key, value] of Object.entries(process.env)) {
    const match = /^SYNC_TOKEN_(.+)$/.exec(key);
    if (match && value) syncTokens.set(match[1]!.toLowerCase().replace(/_/g, "-"), value);
  }

  // Webhook signing secrets, one per CONNECTION, as WEBHOOK_SECRET_<CONNECTION>
  // — a provider signs per integration, not per subset (ADR 0043 §4).
  //
  // Separate from the sync tokens on purpose, and not only because the tiers
  // differ: this secret is shared with a third party, and a secret the provider
  // also holds must not also be the thing that authorizes our own worker.
  const webhookSecrets = new Map<string, string>();
  for (const [key, value] of Object.entries(process.env)) {
    const match = /^WEBHOOK_SECRET_(.+)$/.exec(key);
    if (match && value) webhookSecrets.set(match[1]!.toLowerCase().replace(/_/g, "-"), value);
  }

  const status = new CorpusStatusWriter({
    api: kubeConfig.makeApiClient(k8s.CustomObjectsApi) as never,
    namespace,
    group,
    version,
    plural: CORPUS_PLURAL,
    onError: (corpus, err) =>
      // Reported, never fatal: the pass already happened and the chunks are
      // indexed. Failing it because the bookkeeping did not land would throw
      // away real work to protect a timestamp.
      console.error(`could not publish sync status for ${corpus}:`, err),
  });

  // Declared before the server so the webhook route can reach it, and left
  // undefined when this deployment does not index — in which case the route
  // reports not-found rather than accepting notifications it cannot act on.
  let sync: SyncLoop | undefined;

  const server = createBrokerServer({
    auth: { orchestratorToken: required("ORCHESTRATOR_TOKEN"), syncTokens },
    registry,
    // A run-now is the corpus's own workflow's next full pass, so it never
    // overlaps the scheduled one (docs/adr/0058).
    runSync: async (corpus) => {
      if (!sync?.target(corpus)) return undefined;
      const syncs = sync.syncs();
      // Indexing is configured but its Temporal connection is not up yet (or
      // was lost); the API keeps serving and the run-now says why.
      if (!syncs) throw new SyncFailedError("indexing is not connected to Temporal yet; retry shortly");
      try {
        const report = await syncs.runNow(corpus);
        // The Corpus stopped being synced while the request waited.
        if (!report) return undefined;
        return { indexed: report.indexed, removed: report.removed, full: report.full };
      } catch (err) {
        if (err instanceof SyncPassError) throw new SyncFailedError(err.message);
        throw err;
      }
    },
    webhooks: {
      secretFor: (connection) => webhookSecrets.get(connection),
      onChange: (corpus, sourceIds) => {
        // Acknowledged at once: a provider's delivery must be answered
        // promptly or it gets retried and eventually the endpoint disabled.
        // The signal lands in the corpus's workflow, which keeps what it names
        // until a pass has examined it — even while another pass runs.
        if (!sync?.target(corpus)) return;
        const syncs = sync.syncs();
        if (!syncs) {
          // The next full pass, once Temporal is reachable, picks this up.
          console.error(`sync ${corpus}: webhook not handed over, Temporal is not connected yet`);
          return;
        }
        void syncs
          .notify(corpus, sourceIds)
          .catch((err: unknown) => console.error(`sync ${corpus}: could not hand over a webhook:`, err));
      },
    },
  });
  server.listen(port, () => console.log(`connection-broker listening on ${port}`));

  // Asks the cluster, not the watch cache, whether a Corpus this replica
  // cannot see is still meant to be indexed (docs/adr/0058 §2).
  const customObjects = kubeConfig.makeApiClient(k8s.CustomObjectsApi);
  const confirm = async (corpus: string): Promise<"gone" | "syncable"> => {
    try {
      const cr = (await customObjects.getNamespacedCustomObject({
        group,
        version,
        namespace,
        plural: CORPUS_PLURAL,
        name: corpus,
      })) as CorpusCustomResource;
      return collectionOf(cr) && reconcileIntervalMs(cr) ? "syncable" : "gone";
    } catch (err) {
      if ((err as { code?: number }).code === 404) return "gone";
      throw err;
    }
  };

  sync = startSync(registry, syncTokens, port, status, confirm);

  const shutdown = () => {
    void (async () => {
      await sync?.stop();
      registry.stop();
      server.close(() => process.exit(0));
    })();
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

interface SyncLoop {
  /** Undefined until the Temporal connection and worker are up (and while they reconnect). */
  syncs: () => CorpusSyncs | undefined;
  /** The corpus, if this broker syncs it. */
  target: (corpus: string) => SyncTarget | undefined;
  stop: () => Promise<void>;
}

/**
 * Wires the reconcile loop, if this deployment is configured to run one.
 *
 * Absent OPENAI_API_KEY or QDRANT_URL means "serve only": the broker still
 * answers probes against corpora somebody else populated. That is a real
 * arrangement rather than a misconfiguration, so it starts rather than exits —
 * but it says so, because a broker silently indexing nothing looks exactly like
 * a broker whose sources are empty.
 */
function startSync(
  registry: CrdCorpusRegistry,
  syncTokens: Map<string, string>,
  port: number,
  status: CorpusStatusWriter,
  confirm: (corpus: string) => Promise<"gone" | "syncable">,
): SyncLoop | undefined {
  const qdrantUrl = process.env.QDRANT_URL;
  const openaiKey = process.env.OPENAI_API_KEY;
  if (!qdrantUrl || !openaiKey) {
    console.log("QDRANT_URL or OPENAI_API_KEY unset — serving the API only, not indexing");
    return undefined;
  }

  // The worker talks to the broker over HTTP even though both live in this
  // process. The indirection is the boundary being enforced (ADR 0038 §3): the
  // worker is a client of the credential, and routing it through the same
  // authorization path as any other caller keeps it that way rather than
  // letting proximity become privilege.
  if (syncTokens.size === 0) {
    console.log("no SYNC_TOKEN_* configured — serving the API only, not indexing");
    return undefined;
  }

  // Indexing runs on Temporal (docs/adr/0058): without it there is no
  // scheduler at all, and a broker that silently indexed nothing would look
  // exactly like one whose sources are empty.
  const temporalAddress = process.env.TEMPORAL_ADDRESS;
  if (!temporalAddress) {
    console.error(
      "TEMPORAL_ADDRESS is not set: the broker indexes each Corpus from a Temporal workflow (docs/adr/0058). " +
        "Set it, or unset QDRANT_URL/OPENAI_API_KEY to serve the API only.",
    );
    process.exit(1);
  }
  const temporalNamespace = process.env.TEMPORAL_NAMESPACE || "default";
  const taskQueue = process.env.TEMPORAL_TASK_QUEUE || DEFAULT_TASK_QUEUE;

  const qdrant = new QdrantHttpClient({ url: qdrantUrl, apiKey: process.env.QDRANT_API_KEY });
  const embedder = new OpenAIEmbedder({ apiKey: openaiKey, model: process.env.EMBEDDING_MODEL });

  // A corpus whose SYNC_TOKEN_<name> is missing is reported exactly once,
  // not on every re-read of the targets. It is skipped rather than run with some
  // other corpus's token — that token authorizes precisely one corpus's
  // source, so borrowing it would either 403 or read the wrong corpus.
  const warnedMissingToken = new Set<string>();

  const targets = (): SyncTarget[] =>
    registry.listResources().flatMap((cr: CorpusCustomResource) => {
      const binding = registry.get(cr.metadata.name);
      const collection = collectionOf(cr);
      const intervalMs = reconcileIntervalMs(cr);
      // A corpus with no collection has not been reconciled by the
      // controller yet, and one with no interval is not meant to be
      // indexed. Both are ordinary states, not errors.
      if (!binding || !collection || !intervalMs) return [];
      const reconciled = cr.status?.lastReconcileTime ? Date.parse(cr.status.lastReconcileTime) : NaN;
      // A corpus with no sync token cannot be indexed: its source needs
      // its own token and there is no other to borrow. Reported once so a
      // missing SYNC_TOKEN_<name> is visible, then skipped.
      if (!syncTokens.has(binding.name)) {
        if (!warnedMissingToken.has(binding.name)) {
          console.error(`corpus ${binding.name} has no SYNC_TOKEN_* configured — not indexing it`);
          warnedMissingToken.add(binding.name);
        }
        return [];
      }
      return [{ binding, collection, intervalMs, ...(Number.isFinite(reconciled) ? { lastReconcileAt: reconciled } : {}) }];
    });
  const target = (corpus: string) => targets().find((candidate) => candidate.binding.name === corpus);

  const activities = createSyncActivities({
    // One source per corpus, carrying that corpus's own sync token. The
    // broker scopes each token to a single corpus (auth.ts), so a shared
    // source built with one token would 403 on every other corpus's list
    // and fetch — failing their passes and leaving them silently unindexed.
    // `target` guarantees a token exists before a corpus is ever synced, so
    // the lookup here always succeeds.
    sourceFor: (binding) => {
      const token = syncTokens.get(binding.name);
      if (!token) {
        // Unreachable via `target` (it excludes tokenless corpora), but
        // never fall back to another corpus's token.
        throw new Error(`no SYNC_TOKEN_* configured for corpus ${binding.name}`);
      }
      return new HttpResourceSource({ baseUrl: `http://127.0.0.1:${port}`, token });
    },
    // One writer per corpus: every point carries its own corpus's
    // allowedRoles, and a shared writer would stamp one client's roles onto
    // another client's chunks.
    writerFor: (binding) =>
      new QdrantCorpusWriter(qdrant, embedder, {
        allowedRoles: binding.allowedRoles,
        vectorSize: EMBEDDING_DIMENSIONS,
      }),
    target,
    confirm,
    onReport: async (corpus, report) => {
      console.log(
        `sync ${corpus}: indexed=${report.indexed} removed=${report.removed} ` +
          `unchanged=${report.unchanged} failed=${report.failed.length} full=${report.full}`,
      );
      // Publishing this is what makes staleness measurable. Without it every
      // syncing Corpus reports stale forever, because "has never reconciled"
      // is stale by definition.
      await status.record(corpus, report);
    },
  });

  // Temporal is reached in the background, retried for as long as it takes:
  // the probe, retrieval and fetch API this process also serves must not go
  // down because indexing cannot start (docs/adr/0058 §2). A worker that stops
  // on its own is reconnected the same way.
  let syncs: CorpusSyncs | undefined;
  let worker: Worker | undefined;
  let stopped = false;
  /** Ends the wait between connection attempts early (on stop). */
  let wake: (() => void) | undefined;
  const ensure = () => {
    if (syncs) void syncs.ensure(targets().map((t) => t.binding.name));
  };

  const connect = async (): Promise<void> => {
    for (let attempt = 0; !stopped; attempt++) {
      try {
        const client = await connectClient({ address: temporalAddress, namespace: temporalNamespace });
        worker = await createSyncWorker({ address: temporalAddress, namespace: temporalNamespace, taskQueue, activities });
        const running = worker.run();
        syncs = new CorpusSyncs({ client, taskQueue });
        console.log(`syncing corpora on Temporal ${temporalAddress} (namespace ${temporalNamespace}, task queue ${taskQueue})`);
        attempt = 0;
        // Every replica makes sure every synced Corpus has its loop; the
        // workflow id makes that one loop however many replicas ask.
        ensure();
        await running;
        if (stopped) return;
        console.error("sync worker stopped; reconnecting to Temporal");
      } catch (err) {
        if (stopped) return;
        console.error(
          `SYNC IS NOT RUNNING: could not start the Temporal sync worker on ${temporalAddress} ` +
            `(attempt ${attempt + 1}); the API keeps serving, indexing waits:`,
          err,
        );
      }
      syncs = undefined;
      worker = undefined;
      const delayMs = Math.min(60_000, 1_000 * 2 ** Math.min(attempt, 6));
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, delayMs);
        wake = () => {
          clearTimeout(t);
          resolve();
        };
      });
      wake = undefined;
    }
  };
  const connecting = connect();

  // A Corpus that stops being synced ends its own loop (it re-reads the
  // Corpus as it waits); a new one is started here within a minute.
  const timer = setInterval(ensure, 60_000);

  return {
    syncs: () => syncs,
    target,
    stop: async () => {
      stopped = true;
      clearInterval(timer);
      wake?.();
      try {
        worker?.shutdown();
      } catch {
        // Not running yet (still being created): the loop sees `stopped`.
      }
      await connecting;
    },
  };
}

void main().catch((err: unknown) => {
  console.error("connection-broker failed to start:", err);
  process.exit(1);
});
