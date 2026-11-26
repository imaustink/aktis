/**
 * The connection-broker process.
 *
 * Two jobs in one Deployment, both of which need the same third-party
 * credentials and neither of which may hold cluster RBAC (ADR 0038 §3):
 *
 *   - Serves the HTTP API: per-user authorization probes for retrieval, and
 *     listing/fetch for ingestion.
 *   - Hands webhooks and run-nows to each Corpus's sync workflow, which
 *     corpus-sync (Go, docs/adr/0059) runs and which reads through this API.
 */
import * as k8s from "@kubernetes/client-node";
import { createBrokerServer, SyncFailedError } from "./server.js";
import { CrdCorpusRegistry } from "./crd-corpus-registry.js";
import { collectionOf, reconcileIntervalMs, type CorpusCustomResource } from "./corpus-resource.js";
import { Client, Connection } from "@temporalio/client";
import { CorpusSyncs, SyncPassError } from "./sync/corpus-syncs.js";
import { DEFAULT_TASK_QUEUE } from "./sync/contract.js";

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

  // Declared before the server so the webhook route can reach it, and left
  // undefined when this deployment does not index — in which case the route
  // reports not-found rather than accepting notifications it cannot act on.
  let sync: SyncHandoff | undefined;

  const server = createBrokerServer({
    auth: { orchestratorToken: required("ORCHESTRATOR_TOKEN"), syncTokens },
    registry,
    // A run-now is the corpus's own workflow's next full pass, so it never
    // overlaps the scheduled one (docs/adr/0058, run by corpus-sync since 0059).
    runSync: async (corpus) => {
      if (!sync?.target(corpus)) return undefined;
      try {
        const report = await sync.syncs.runNow(corpus);
        // The Corpus stopped being synced while the request waited.
        if (!report) return undefined;
        return { indexed: report.indexed, removed: report.removed, full: report.full };
      } catch (err) {
        if (err instanceof SyncPassError) throw new SyncFailedError(err.message);
        // Temporal unreachable (the lazy connection has not come up, or went
        // away): the API keeps serving and the run-now says why.
        throw new SyncFailedError(`could not reach Temporal: ${err instanceof Error ? err.message : String(err)}`);
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
        // Not handed over (Temporal unreachable): the next full pass, the
        // reconcile backstop, picks the change up.
        void sync.syncs
          .notify(corpus, sourceIds)
          .catch((err: unknown) => console.error(`sync ${corpus}: could not hand over a webhook:`, err));
      },
    },
  });
  server.listen(port, () => console.log(`connection-broker listening on ${port}`));

  sync = startSync(registry, syncTokens);

  const shutdown = () => {
    void (async () => {
      await sync?.close();
      registry.stop();
      server.close(() => process.exit(0));
    })();
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

interface SyncHandoff {
  syncs: CorpusSyncs;
  /** Whether this broker hands the Corpus's sync over at all. */
  target: (corpus: string) => boolean;
  close: () => Promise<void>;
}

/**
 * Wires the hand-off to corpus-sync's workflows (docs/adr/0059), if this
 * deployment indexes. The chart sets TEMPORAL_ADDRESS exactly when it deploys
 * corpus-sync, so without it the broker serves the API only: a real
 * arrangement (another process may own ingestion), said once because a broker
 * silently indexing nothing looks exactly like one whose sources are empty.
 */
function startSync(registry: CrdCorpusRegistry, syncTokens: Map<string, string>): SyncHandoff | undefined {
  const temporalAddress = process.env.TEMPORAL_ADDRESS;
  if (!temporalAddress) {
    console.log("TEMPORAL_ADDRESS unset — serving the API only; nothing indexes through this broker");
    return undefined;
  }
  const namespace = process.env.TEMPORAL_NAMESPACE || "default";
  const taskQueue = process.env.TEMPORAL_TASK_QUEUE || DEFAULT_TASK_QUEUE;

  // Lazy: the API must not wait on Temporal, and a call made while it is
  // unreachable fails on its own (the run-now answers 502, a webhook is left
  // to the next full pass).
  const connection = Connection.lazy({ address: temporalAddress });
  const syncs = new CorpusSyncs({ client: new Client({ connection, namespace }), taskQueue });
  console.log(
    `handing corpus syncs to corpus-sync on Temporal ${temporalAddress} (namespace ${namespace}, task queue ${taskQueue})`,
  );

  // The rule corpus-sync applies (a collection, an interval, a sync token),
  // plus "bound here", so a run-now for a Corpus nobody syncs is a 404 rather
  // than a loop started only to end.
  const target = (corpus: string): boolean => {
    const cr = registry.listResources().find((candidate: CorpusCustomResource) => candidate.metadata.name === corpus);
    return Boolean(cr && registry.get(corpus) && collectionOf(cr) && reconcileIntervalMs(cr) && syncTokens.has(corpus));
  };

  return { syncs, target, close: () => connection.close() };
}

void main().catch((err: unknown) => {
  console.error("connection-broker failed to start:", err);
  process.exit(1);
});
