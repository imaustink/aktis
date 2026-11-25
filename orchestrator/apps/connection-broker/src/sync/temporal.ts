import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  Client,
  Connection,
  WorkflowExecutionAlreadyStartedError,
  WorkflowNotFoundError,
  WorkflowUpdateFailedError,
} from "@temporalio/client";
import { NativeConnection, Worker, type WorkerOptions } from "@temporalio/worker";
import { SOURCES_CHANGED_SIGNAL, SYNC_NOW_UPDATE, workflowIds, type PassResult, type SyncActivities } from "./model.js";
import type { corpusSync } from "./workflows.js";

/**
 * The broker's side of Temporal (docs/adr/0058): keeping one `corpusSync`
 * workflow running per synced Corpus, handing it webhooks and run-now
 * requests, and the worker that runs it.
 */

export interface CorpusSyncsOptions {
  client: Client;
  taskQueue: string;
  log?: (line: string) => void;
}

/** A run-now whose pass failed, or whose Corpus stopped being synced while it waited. */
export class SyncPassError extends Error {}

export class CorpusSyncs {
  private readonly log: (line: string) => void;

  constructor(private readonly o: CorpusSyncsOptions) {
    this.log = o.log ?? ((l) => console.error(l));
  }

  get client(): Client {
    return this.o.client;
  }

  get taskQueue(): string {
    return this.o.taskQueue;
  }

  workflowId(corpus: string): string {
    return workflowIds.corpusSync(this.o.taskQueue, corpus);
  }

  /**
   * Makes sure every synced Corpus has its loop running; one already running
   * is left alone. Every replica calls this whenever it looks, so a Corpus
   * gets its loop however many brokers there are, and only one.
   */
  async ensure(corpora: string[]): Promise<void> {
    for (const corpus of corpora) {
      try {
        await this.start(corpus);
      } catch (error) {
        this.log(`sync ${corpus}: could not start its workflow: ${errorText(error)}`);
      }
    }
  }

  /**
   * A webhook named these resources (empty: something changed, we do not know
   * what). Starts the loop if it is not running, so a delivery is never
   * dropped for want of one.
   */
  async notify(corpus: string, sourceIds: string[]): Promise<void> {
    await this.o.client.workflow.signalWithStart<typeof corpusSync, [string[]]>("corpusSync", {
      taskQueue: this.o.taskQueue,
      workflowId: this.workflowId(corpus),
      args: [{ corpus }],
      signal: SOURCES_CHANGED_SIGNAL,
      signalArgs: [sourceIds],
    });
  }

  /**
   * Runs a full pass that starts after this call and returns its result. A
   * pass already running is not joined: it may have listed the source before
   * whatever prompted this request.
   */
  async runNow(corpus: string): Promise<PassResult> {
    await this.start(corpus);
    try {
      return await this.o.client.workflow.getHandle(this.workflowId(corpus)).executeUpdate<PassResult, []>(SYNC_NOW_UPDATE);
    } catch (error) {
      if (error instanceof WorkflowUpdateFailedError) throw new SyncPassError(errorText(error.cause ?? error));
      throw error;
    }
  }

  private async start(corpus: string): Promise<void> {
    try {
      await this.o.client.workflow.start<typeof corpusSync>("corpusSync", {
        taskQueue: this.o.taskQueue,
        workflowId: this.workflowId(corpus),
        args: [{ corpus }],
        workflowIdConflictPolicy: "USE_EXISTING",
      });
    } catch (error) {
      if (!(error instanceof WorkflowExecutionAlreadyStartedError)) throw error;
    }
  }

  /** Whether a Corpus's loop is running -- for tests and diagnostics. */
  async isRunning(corpus: string): Promise<boolean> {
    try {
      return (await this.o.client.workflow.getHandle(this.workflowId(corpus)).describe()).status.name === "RUNNING";
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

export interface SyncWorkerOptions extends TemporalConnectionOptions {
  taskQueue: string;
  activities: SyncActivities;
  /** Overrides for tests (a prebuilt bundle, a test connection). */
  connection?: NativeConnection;
  workflowBundle?: WorkerOptions["workflowBundle"];
}

/**
 * The worker running the sync workflows and their passes. A pass holds an
 * activity slot for its whole length, so the slots bound how many corpora
 * index at once on one replica; the rest wait in the queue (or go to another
 * replica).
 */
export async function createSyncWorker(o: SyncWorkerOptions): Promise<Worker> {
  const connection = o.connection ?? (await NativeConnection.connect({ address: o.address }));
  return Worker.create({
    connection,
    namespace: o.namespace,
    taskQueue: o.taskQueue,
    ...(o.workflowBundle ? { workflowBundle: o.workflowBundle } : workflowSource()),
    activities: o.activities,
    maxConcurrentActivityTaskExecutions: 20,
    shutdownGraceTime: "10 seconds",
  });
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
