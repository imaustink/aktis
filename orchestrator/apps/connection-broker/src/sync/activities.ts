import { ApplicationFailure, CancelledFailure, Context } from "@temporalio/activity";
import type { CorpusBinding } from "../registry.js";
import { FAILED_IDS_KEPT, type PassResult, type SyncActivities, type SyncTargetInfo } from "./model.js";
import { syncConnection, type CorpusWriter, type ResourceSource, type SyncReport } from "./worker.js";

/** One Corpus the broker syncs: it is bound, has a collection and an interval, and a sync token. */
export interface SyncTarget {
  binding: CorpusBinding;
  collection: string;
  intervalMs: number;
  /** The Corpus status's `lastReconcileTime`, in ms since the epoch. */
  lastReconcileAt?: number;
}

export interface SyncActivityDeps {
  /**
   * A source for one corpus, not one shared source.
   *
   * A source carries a sync token, and the broker scopes each sync token to
   * exactly one corpus (see auth.ts): a source built with corpus A's token
   * gets a 403 the moment it lists or fetches corpus B, which fails B's whole
   * pass so B never indexes. One source per corpus — carrying its own token —
   * is the mirror of `writerFor` below, for the same reason.
   */
  sourceFor: (binding: CorpusBinding) => ResourceSource;
  /**
   * A writer for one corpus, not one shared writer.
   *
   * Every point carries its corpus's allowedRoles, so a single writer would
   * stamp one corpus's roles onto another's chunks — silently widening or
   * narrowing who can retrieve a whole client's material.
   */
  writerFor: (binding: CorpusBinding) => CorpusWriter;
  /** The corpus, if this replica syncs it (read fresh on every call, so edits apply). */
  target: (corpus: string) => SyncTarget | undefined;
  /**
   * Asked when `target` finds nothing: whether the Corpus is really gone (or
   * no longer meant to be indexed), read from the cluster rather than this
   * replica's watch cache. "syncable" means this replica is behind -- its
   * watch has not caught up, or it lacks the sync token a newer pod has --
   * and the activity fails retryably so another attempt, perhaps on another
   * replica, answers. Absent: a miss is taken as gone.
   */
  confirm?: (corpus: string) => Promise<"gone" | "syncable">;
  /**
   * Called for every completed pass. This is where a Corpus's status is
   * written back — see CorpusStatusWriter for why that matters.
   */
  onReport?: (corpus: string, report: SyncReport) => void | Promise<void>;
  log?: (line: string) => void;
}

/**
 * The sync workflow's activities (docs/adr/0058): the same pass the in-process
 * scheduler used to run, now run wherever Temporal hands it.
 */
export function createSyncActivities(deps: SyncActivityDeps): SyncActivities {
  const log = deps.log ?? ((line: string) => console.error(line));
  /** `null` only when the cluster agrees the Corpus is no longer synced. */
  const notHere = async (corpus: string): Promise<null> => {
    if (deps.confirm && (await deps.confirm(corpus)) === "syncable") {
      throw ApplicationFailure.retryable(
        `corpus ${corpus} is synced, but this replica cannot see it yet (watch lag, or no SYNC_TOKEN_* here)`,
        "CorpusNotVisibleHere",
      );
    }
    return null;
  };
  return {
    async syncTarget(corpus): Promise<SyncTargetInfo | null> {
      const target = deps.target(corpus);
      if (!target) return notHere(corpus);
      return {
        intervalMs: target.intervalMs,
        ...(target.lastReconcileAt !== undefined ? { lastReconcileAt: target.lastReconcileAt } : {}),
      };
    },

    async runPass(corpus, sourceIds): Promise<PassResult | null> {
      const target = deps.target(corpus);
      if (!target) return notHere(corpus);
      // Inside an activity only: a direct call (a unit test) has no context.
      // A cancelled pass (the worker shutting down, the heartbeat timing out)
      // stops at its next step, so its retry on another replica does not run
      // alongside it for long.
      const ctx = activityContext();
      const heartbeat = ctx
        ? (details: unknown) => {
            ctx.heartbeat(details);
            if (ctx.cancellationSignal.aborted) throw new CancelledFailure("sync pass cancelled");
          }
        : undefined;
      heartbeat?.({ phase: "start" });
      const report = await syncConnection(
        deps.sourceFor(target.binding),
        deps.writerFor(target.binding),
        { connection: target.binding.name, label: target.binding.name, collection: target.collection },
        {
          ...(sourceIds ? { onlySourceIds: sourceIds } : {}),
          ...(heartbeat ? { onProgress: heartbeat } : {}),
        },
      );
      await deps.onReport?.(corpus, report);
      return {
        indexed: report.indexed,
        removed: report.removed,
        unchanged: report.unchanged,
        failed: report.failed.slice(0, FAILED_IDS_KEPT),
        failedCount: report.failed.length,
        full: report.full,
      };
    },

    async log(line) {
      log(line);
    },
  };
}

function activityContext(): Context | undefined {
  try {
    return Context.current();
  } catch {
    return undefined;
  }
}
