import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Client, Connection, WorkflowExecutionAlreadyStartedError, WorkflowNotFoundError } from "@temporalio/client";
import { NativeConnection, Worker, type WorkerOptions } from "@temporalio/worker";
import type { RunActivities } from "./contract.js";
import type { RunStarter } from "./effects.js";
import { queueHoldTimeoutMs, refKey, workflowIds, type EventRunInput, type HeldMemo, type HeldRunEventInput, type QueueHold, type RunSettings } from "./model.js";
import type { conflictSweep, eventRun, heldRunEvent, runQueue } from "./workflows.js";

/**
 * The gateway's side of Temporal (docs/adr/0057): starting runs, the queue
 * signals, and the worker that runs the workflows and their activities.
 */

export interface TemporalRunsOptions {
  client: Client;
  taskQueue: string;
  settings: RunSettings;
  log?: (line: string) => void;
  /** Attempts to start a run before giving up on it (Temporal briefly unreachable). */
  startAttempts?: number;
  /** Injectable for tests. */
  sleep?: (ms: number) => Promise<void>;
}

const DEFAULT_START_ATTEMPTS = 5;

export class TemporalRuns implements RunStarter {
  private readonly log: (line: string) => void;
  private readonly sleep: (ms: number) => Promise<void>;

  constructor(private readonly o: TemporalRunsOptions) {
    this.log = o.log ?? ((l) => console.error(l));
    this.sleep = o.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
  }

  get client(): Client {
    return this.o.client;
  }

  /**
   * Starts one accepted event's run. The workflow id is the invocation's, so
   * starting it twice (a retried publish, the legacy drain racing an old pod)
   * is a no-op. Retried briefly: the orchestrator already accepted the event,
   * so a run that never starts is a run nobody reports on.
   */
  async startEventRun(input: Omit<EventRunInput, "settings">): Promise<void> {
    const attempts = this.o.startAttempts ?? DEFAULT_START_ATTEMPTS;
    for (let attempt = 1; ; attempt++) {
      try {
        await this.o.client.workflow.start<typeof eventRun>("eventRun", {
          taskQueue: this.o.taskQueue,
          workflowId: workflowIds.eventRun(this.o.taskQueue, input.invocationId),
          args: [{ ...input, settings: this.o.settings }],
        });
        return;
      } catch (error) {
        if (error instanceof WorkflowExecutionAlreadyStartedError) return;
        if (attempt >= attempts) throw error;
        this.log(`event run ${input.invocationId}: could not start its workflow (attempt ${attempt}): ${errorText(error)}`);
        await this.sleep(Math.min(10_000, 500 * 2 ** (attempt - 1)));
      }
    }
  }

  /** Starts a held completion's workflow directly -- the legacy drain's way in (legacy-drain.ts). */
  async startHeld(id: string, input: Omit<HeldRunEventInput, "settings">): Promise<void> {
    const memo: HeldMemo = {
      ...(input.parkedOn ? { parkedOn: refKey(input.parkedOn) } : {}),
      ...(input.waitFor ? { waitFor: refKey(input.waitFor) } : {}),
    };
    try {
      await this.o.client.workflow.start<typeof heldRunEvent>("heldRunEvent", {
        taskQueue: this.o.taskQueue,
        workflowId: workflowIds.heldRunEvent(this.o.taskQueue, id),
        memo: memo as Record<string, unknown>,
        args: [{ ...input, settings: this.o.settings }],
      });
    } catch (error) {
      if (!(error instanceof WorkflowExecutionAlreadyStartedError)) throw error;
    }
  }

  /** The memo of every running held event (docs/adr/0055 §2's cycle guard). */
  async listHeld(): Promise<HeldMemo[]> {
    const out: HeldMemo[] = [];
    for await (const w of this.o.client.workflow.list({ query: `WorkflowType = 'heldRunEvent' AND ExecutionStatus = 'Running' AND TaskQueue = '${this.o.taskQueue}'` })) {
      const memo = (w.memo ?? {}) as HeldMemo;
      out.push({
        ...(typeof memo.parkedOn === "string" ? { parkedOn: memo.parkedOn } : {}),
        ...(typeof memo.waitFor === "string" ? { waitFor: memo.waitFor } : {}),
      });
    }
    return out;
  }

  /** Joins a serialize queue, starting its workflow if nobody is queued. */
  async enqueue(hold: QueueHold): Promise<void> {
    await this.o.client.workflow.signalWithStart<typeof runQueue, [string]>("runQueue", {
      taskQueue: this.o.taskQueue,
      workflowId: workflowIds.runQueue(this.o.taskQueue, hold.queue),
      args: [{ queue: hold.queue, holdTimeoutMs: queueHoldTimeoutMs(this.o.settings), claimGraceMs: this.o.settings.queueClaimGraceMs }],
      signal: "enqueue",
      signalArgs: [hold.holder],
    });
  }

  /** Releases a queue turn. A queue that has already closed has nothing to release. */
  async releaseQueue(hold: QueueHold): Promise<void> {
    try {
      await this.o.client.workflow.getHandle(workflowIds.runQueue(this.o.taskQueue, hold.queue)).signal("release", hold.holder);
    } catch (error) {
      if (!(error instanceof WorkflowNotFoundError)) throw error;
    }
  }

  /**
   * Makes sure one sweep workflow runs per `conflicts` source (docs/adr/0055
   * §1); one already running is left alone. `seedMarks` gives a sweep that is
   * not running the marks to start with -- the pre-0057 Redis marks
   * (LegacyRunDrain.conflictMarks), so the cutover does not emit a conflict
   * event a second time. If they can't be read, the sweep is not started
   * this time; the next call tries again.
   */
  async ensureConflictSweeps(sources: string[], seedMarks?: (source: string) => Promise<Record<string, number>>): Promise<void> {
    for (const source of sources) {
      const workflowId = workflowIds.conflictSweep(this.o.taskQueue, source);
      try {
        let marks: Record<string, number> | undefined;
        if (seedMarks) {
          if (await this.isRunning(workflowId)) continue;
          marks = await seedMarks(source);
        }
        await this.o.client.workflow.start<typeof conflictSweep>("conflictSweep", {
          taskQueue: this.o.taskQueue,
          workflowId,
          args: [{ source, ...(marks && Object.keys(marks).length > 0 ? { marks } : {}) }],
          workflowIdConflictPolicy: "USE_EXISTING",
        });
      } catch (error) {
        if (!(error instanceof WorkflowExecutionAlreadyStartedError)) this.log(`conflicts ${source}: could not start its sweep: ${errorText(error)}`);
      }
    }
  }

  private async isRunning(workflowId: string): Promise<boolean> {
    try {
      return (await this.o.client.workflow.getHandle(workflowId).describe()).status.name === "RUNNING";
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) return false;
      throw error;
    }
  }
}

export interface TemporalConnectionOptions {
  address: string;
  namespace: string;
}

export async function connectClient(o: TemporalConnectionOptions): Promise<Client> {
  const connection = await Connection.connect({ address: o.address });
  return new Client({ connection, namespace: o.namespace });
}

/** The workflow code: the bundle the build wrote (npm run build), else the source -- bundled when the worker starts. */
function workflowSource(): Pick<WorkerOptions, "workflowBundle" | "workflowsPath"> {
  const bundle = fileURLToPath(new URL("./workflow-bundle.js", import.meta.url));
  if (existsSync(bundle)) return { workflowBundle: { codePath: bundle } };
  const compiled = fileURLToPath(new URL("./workflows.js", import.meta.url));
  return { workflowsPath: existsSync(compiled) ? compiled : fileURLToPath(new URL("./workflows.ts", import.meta.url)) };
}

export interface RunWorkerOptions extends TemporalConnectionOptions {
  taskQueue: string;
  activities: RunActivities;
  /** Overrides for tests (a prebuilt bundle, a test connection). */
  connection?: NativeConnection;
  workflowBundle?: WorkerOptions["workflowBundle"];
}

/**
 * The worker running this gateway's workflows and activities. Activity slots
 * are mostly a turn being polled -- an HTTP wait, for up to the poll budget --
 * so there are many of them: one slot per live run.
 */
export async function createRunWorker(o: RunWorkerOptions): Promise<Worker> {
  const connection = o.connection ?? (await NativeConnection.connect({ address: o.address }));
  return Worker.create({
    connection,
    namespace: o.namespace,
    taskQueue: o.taskQueue,
    ...(o.workflowBundle ? { workflowBundle: o.workflowBundle } : workflowSource()),
    activities: o.activities,
    maxConcurrentActivityTaskExecutions: 1000,
    shutdownGraceTime: "10 seconds",
  });
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
