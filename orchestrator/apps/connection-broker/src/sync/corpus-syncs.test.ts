import { describe, expect, it, vi } from "vitest";
import { WorkflowNotFoundError, WorkflowUpdateFailedError } from "@temporalio/client";
import { ApplicationFailure } from "@temporalio/common";
import { CorpusSyncs, SyncPassError } from "./corpus-syncs.js";

function fakeClient(executeUpdate: () => Promise<unknown> = async () => ({ indexed: 1, removed: 0, unchanged: 2, failed: [], failedCount: 0, full: true })) {
  const handle = { executeUpdate: vi.fn(executeUpdate) };
  const workflow = {
    start: vi.fn().mockResolvedValue(undefined),
    signalWithStart: vi.fn().mockResolvedValue(undefined),
    getHandle: vi.fn().mockReturnValue(handle),
  };
  return { client: { workflow } as never, workflow, handle };
}

describe("CorpusSyncs (the broker's side of docs/adr/0059)", () => {
  it("hands a webhook to the corpus's loop with signal-with-start, on corpus-sync's queue and id", async () => {
    const { client, workflow } = fakeClient();
    await new CorpusSyncs({ client, taskQueue: "corpus-sync" }).notify("eng", ["p1"]);
    expect(workflow.signalWithStart).toHaveBeenCalledWith("CorpusSync", {
      taskQueue: "corpus-sync",
      workflowId: "corpus-sync/corpus-sync:eng",
      args: [{ corpus: "eng" }],
      signal: "sourcesChanged",
      signalArgs: [["p1"]],
    });
  });

  it("answers a run-now with the update's pass, starting the loop if it is not running", async () => {
    const { client, workflow, handle } = fakeClient();
    const result = await new CorpusSyncs({ client, taskQueue: "q" }).runNow("eng");
    expect(result).toMatchObject({ indexed: 1, full: true });
    expect(workflow.start).toHaveBeenCalledWith("CorpusSync", expect.objectContaining({
      workflowId: "q/corpus-sync:eng",
      taskQueue: "q",
      workflowIdConflictPolicy: "USE_EXISTING",
    }));
    expect(workflow.getHandle).toHaveBeenCalledWith("q/corpus-sync:eng");
    expect(handle.executeUpdate).toHaveBeenCalledWith("syncNow");
  });

  it("answers undefined when the corpus stopped being synced while the run-now waited", async () => {
    const { client } = fakeClient(async () => {
      throw new WorkflowUpdateFailedError("update failed", ApplicationFailure.create({ type: "CorpusNotSynced", message: "gone" }));
    });
    await expect(new CorpusSyncs({ client, taskQueue: "q" }).runNow("eng")).resolves.toBeUndefined();
  });

  it("answers undefined when the loop is already gone", async () => {
    const { client } = fakeClient(async () => {
      throw new WorkflowNotFoundError("not found", "q/corpus-sync:eng", undefined);
    });
    await expect(new CorpusSyncs({ client, taskQueue: "q" }).runNow("eng")).resolves.toBeUndefined();
  });

  it("reports a pass that failed after its retries as a SyncPassError carrying the cause", async () => {
    const { client } = fakeClient(async () => {
      throw new WorkflowUpdateFailedError("update failed", ApplicationFailure.create({ type: "SyncFailed", message: "confluence is down" }));
    });
    const err = await new CorpusSyncs({ client, taskQueue: "q" }).runNow("eng").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(SyncPassError);
    expect((err as Error).message).toBe("confluence is down");
  });
});
