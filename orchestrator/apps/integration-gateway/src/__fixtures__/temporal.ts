import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Runtime, DefaultLogger, type Worker, type WorkflowBundleWithSourceMap } from "@temporalio/worker";
import { afterAll } from "vitest";
import type { RunActivities } from "../runs/contract.js";
import { DEFAULT_RUN_SETTINGS, type RunSettings } from "../runs/model.js";
import { createRunWorker, TemporalRuns } from "../runs/temporal.js";

/**
 * A real Temporal for tests of the event-run workflows (docs/adr/0057): one
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
    workflowsPath: fileURLToPath(new URL("../../dist/runs/workflows.js", import.meta.url)),
    webpackConfigHook: (c: any) => { c.module.rules = []; return c; },
    logger: new DefaultLogger("WARN"),
  });
  return bundle;
}

/**
 * The task queue the replay histories (src/runs/__histories__) are captured
 * and replayed on -- production's. Workflow ids derive from the task queue
 * (workflowIds), so a replay on any other queue would mismatch every child
 * workflow id.
 */
export const HISTORY_TASK_QUEUE = "integration-gateway";

/** Every worker a test started, so teardown can stop the ones a test left running. */
const workers = new Set<{ stop(): Promise<void> }>();

afterAll(async () => {
  for (const w of workers) await w.stop();
  if (env) await (await env).teardown();
});

export interface TestRuns {
  runs: TemporalRuns;
  /** The dev server's client, for reading back a workflow's history. */
  client: TestWorkflowEnvironment["client"];
  taskQueue: string;
  /** Starts the worker; call once the activities exist. */
  serve(activities: RunActivities): Promise<void>;
  stop(): Promise<void>;
}

/**
 * A {@link TemporalRuns} on its own task queue, and its worker. `taskQueue`
 * names the queue instead (capture-histories.test.ts records on the one
 * production uses).
 */
export async function testRuns(opts: Partial<RunSettings> = {}, queue: { taskQueue?: string } = {}): Promise<TestRuns> {
  const e = await temporal();
  const taskQueue = queue.taskQueue ?? `test-${randomUUID()}`;
  const runs = new TemporalRuns({
    client: e.client,
    taskQueue,
    // Fast enough to test: held events re-check, and failed publishes retry, every few ms.
    settings: { ...DEFAULT_RUN_SETTINGS, heldEventCheckMs: 20, publishRetryMs: 20, publishRetryWindowMs: 60_000, ...opts },
    log: () => {},
    sleep: async () => {},
  });
  let worker: Worker | undefined;
  let running: Promise<void> | undefined;
  const handle: TestRuns = {
    runs,
    client: e.client,
    taskQueue,
    async serve(activities) {
      worker = await createRunWorker({
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
      workers.delete(handle);
      if (!worker) return;
      const w = worker;
      worker = undefined;
      w.shutdown();
      await running;
    },
  };
  workers.add(handle);
  return handle;
}
