import { WorkflowNotFoundError, WorkflowUpdateFailedError, type Client } from "@temporalio/client";
import { ApplicationFailure } from "@temporalio/common";
import {
  CORPUS_NOT_SYNCED,
  SOURCES_CHANGED_SIGNAL,
  SYNC_NOW_UPDATE,
  WORKFLOW_NAME,
  workflowId,
  type CorpusSyncInput,
  type PassResult,
} from "./contract.js";

/**
 * Hands webhooks and run-nows to each Corpus's sync workflow, which
 * corpus-sync runs (docs/adr/0059). corpus-sync also keeps every synced
 * Corpus's loop running; the starts here only make sure a delivery or a
 * run-now never waits for that.
 */
export interface CorpusSyncsOptions {
  /** The workflow-facing slice of a Temporal client. */
  client: Pick<Client, "workflow">;
  taskQueue: string;
}

/** A run-now whose pass failed after its retries. */
export class SyncPassError extends Error {}

export class CorpusSyncs {
  constructor(private readonly o: CorpusSyncsOptions) {}

  workflowId(corpus: string): string {
    return workflowId(this.o.taskQueue, corpus);
  }

  /**
   * A webhook named these resources (empty: something changed, we do not
   * know what). Starts the loop if it is not running, so a delivery is never
   * dropped for want of one.
   */
  async notify(corpus: string, sourceIds: string[]): Promise<void> {
    await this.o.client.workflow.signalWithStart(WORKFLOW_NAME, {
      taskQueue: this.o.taskQueue,
      workflowId: this.workflowId(corpus),
      args: [{ corpus } satisfies CorpusSyncInput],
      signal: SOURCES_CHANGED_SIGNAL,
      signalArgs: [sourceIds],
    });
  }

  /**
   * Runs a full pass that starts after this call and returns its result. A
   * pass already running is not joined: it may have listed the source before
   * whatever prompted this request. Undefined when the Corpus is no longer
   * synced -- it went while the request waited, or its loop closed first.
   */
  async runNow(corpus: string): Promise<PassResult | undefined> {
    await this.o.client.workflow.start(WORKFLOW_NAME, {
      taskQueue: this.o.taskQueue,
      workflowId: this.workflowId(corpus),
      args: [{ corpus } satisfies CorpusSyncInput],
      workflowIdConflictPolicy: "USE_EXISTING",
    });
    try {
      return await this.o.client.workflow.getHandle(this.workflowId(corpus)).executeUpdate<PassResult, []>(SYNC_NOW_UPDATE);
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) return undefined;
      if (error instanceof WorkflowUpdateFailedError) {
        const cause = error.cause;
        if (cause instanceof ApplicationFailure && cause.type === CORPUS_NOT_SYNCED) return undefined;
        throw new SyncPassError(errorText(cause ?? error));
      }
      throw error;
    }
  }
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
