import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, DefaultLogger, Runtime, type Worker, type WorkflowBundleWithSourceMap } from "@temporalio/worker";
import { afterAll } from "vitest";
import type { SyncActivities } from "../sync/model.js";
import { CorpusSyncs, createSyncWorker } from "../sync/temporal.js";

/**
 * A real Temporal for tests of the Corpus sync workflow (docs/adr/0058): one
 * local dev server and one workflow bundle per test file, and a worker on a
 * fresh task queue per harness -- workflow ids live under their task queue,
 * so nothing one test leaves running reaches the next.
 */

let env: Promise<TestWorkflowEnvironment> | undefined;
let bundle: Promise<WorkflowBundleWithSourceMap> | undefined;

// The SDK's own logging (an activity failing on purpose, say) is noise in test output; errors still show.
Runtime.install({ logger: new DefaultLogger("ERROR") });

function temporal(): Promise<TestWorkflowEnvironment> {
  env ??= TestWorkflowEnvironment.createLocal({ server: { log: { format: "pretty", level: "never" } } });
  return env;
}

/** The workflow bundle every worker here runs, built once per test file (the replay test replays histories against it). */
export function workflows(): Promise<WorkflowBundleWithSourceMap> {
  bundle ??= bundleWorkflowCode({
    workflowsPath: fileURLToPath(new URL("../sync/workflows.ts", import.meta.url)),
    logger: new DefaultLogger("WARN"),
  });
  return bundle;
}

/** The task queue the replay histories (src/sync/__histories__) are captured and replayed on -- production's. */
export const HISTORY_TASK_QUEUE = "connection-broker";

/** Every harness a test started, so teardown can stop the workers a test left running. */
const harnesses = new Set<{ stop(): Promise<void> }>();

afterAll(async () => {
  for (const h of harnesses) await h.stop();
  if (env) await (await env).teardown();
});

export interface TestSyncs {
  syncs: CorpusSyncs;
  /** The dev server's client, for reading back a workflow's history. */
  client: TestWorkflowEnvironment["client"];
  taskQueue: string;
  /** Starts the worker; call once the activities exist. */
  serve(activities: SyncActivities): Promise<void>;
  stop(): Promise<void>;
}

/** A {@link CorpusSyncs} on its own task queue (or `taskQueue`), and its worker. */
export async function testSyncs(queue: { taskQueue?: string } = {}): Promise<TestSyncs> {
  const e = await temporal();
  const taskQueue = queue.taskQueue ?? `test-${randomUUID()}`;
  const syncs = new CorpusSyncs({ client: e.client, taskQueue, log: () => {} });
  let worker: Worker | undefined;
  let running: Promise<void> | undefined;
  const handle: TestSyncs = {
    syncs,
    client: e.client,
    taskQueue,
    async serve(activities) {
      worker = await createSyncWorker({
        address: "",
        namespace: e.namespace ?? "default",
        taskQueue,
        activities,
        connection: e.nativeConnection,
        workflowBundle: await workflows(),
      });
      running = worker.run();
    },
    async stop() {
      harnesses.delete(handle);
      if (!worker) return;
      const w = worker;
      worker = undefined;
      w.shutdown();
      await running;
    },
  };
  harnesses.add(handle);
  return handle;
}
