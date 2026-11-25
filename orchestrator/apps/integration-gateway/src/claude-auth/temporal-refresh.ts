import type { Client } from "@temporalio/client";
import { WorkflowExecutionAlreadyStartedError } from "@temporalio/client";
import { decryptField, encryptField } from "../credential-store/field-encryption.js";
import { workflowIds } from "../runs/model.js";
import {
  DEFAULT_SWEEP_INTERVAL_MS,
  DEFAULT_SWEEP_MARGIN_MS,
  type ClaudeCredentialRenewer,
  type CredentialRefreshSerializer,
} from "./credential-refresher.js";
import type { ClaudeCredentialActivities, ClaudeCredentialRefreshResult } from "./refresh-contract.js";
import type { claudeCredentialRefresh, claudeCredentialSweep } from "./workflows.js";

/**
 * The gateway's side of the Claude credential workflows (claude-auth/workflows.ts):
 * starting or attaching to a subject's refresh, keeping the sweep running, and
 * the activities the worker runs for them.
 *
 * Token material never goes into workflow history in the clear. A refresh
 * that persisted leaves its result in the store, and the caller reads it from
 * there; only a refresh that could NOT be persisted has to hand its blob back
 * through the workflow (it is the only live copy, and the caller may be on
 * another replica), and then it is sealed under the credential store's own
 * key, which every replica holds.
 */

/** How long a read waits for a refresh before serving the stored credential. */
export const DEFAULT_REFRESH_WAIT_MS = 30_000;

export interface TemporalClaudeCredentialsOptions {
  client: Client;
  taskQueue: string;
  /** The credential store's encryption key (IDENTITY_LINK_ENCRYPTION_KEY, decoded). */
  key: Buffer;
  /** How long a read waits for the refresh's result. */
  waitMs?: number;
  log?: (line: string) => void;
}

export class TemporalClaudeCredentials implements CredentialRefreshSerializer {
  private readonly log: (line: string) => void;

  constructor(private readonly o: TemporalClaudeCredentialsOptions) {
    this.log = o.log ?? ((l) => console.error(l));
  }

  /**
   * Starts `subject`'s refresh, or attaches to the one running (USE_EXISTING),
   * and waits for its result. A refresh that ended keeps its id free for the
   * next one (the default reuse policy). Throws when Temporal can't be
   * reached or the refresh does not answer within `waitMs`; the read path
   * then serves the stored credential and never refreshes on its own.
   */
  async refresh(subject: string, marginMs: number): Promise<{ credentialsJson?: string }> {
    const handle = await this.o.client.workflow.start<typeof claudeCredentialRefresh>("claudeCredentialRefresh", {
      taskQueue: this.o.taskQueue,
      workflowId: workflowIds.claudeCredentialRefresh(this.o.taskQueue, subject),
      args: [{ subject, marginMs }],
      workflowIdConflictPolicy: "USE_EXISTING",
    });
    const waitMs = this.o.waitMs ?? DEFAULT_REFRESH_WAIT_MS;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const result = await Promise.race([
      handle.result(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`the refresh did not finish within ${waitMs}ms`)), waitMs);
      }),
    ]).finally(() => clearTimeout(timer));
    if (result.status === "failed") throw new Error(`the refresh failed: ${result.detail ?? "no detail"}`);
    return result.sealedCredentialsJson ? { credentialsJson: decryptField(this.o.key, result.sealedCredentialsJson) } : {};
  }

  /**
   * Makes sure the one sweep runs; one already running is left alone. Called
   * by every replica at startup and then periodically, so a sweep that ended
   * (disabled on another replica, say) comes back.
   */
  async ensureSweep(intervalMs = DEFAULT_SWEEP_INTERVAL_MS): Promise<void> {
    try {
      await this.o.client.workflow.start<typeof claudeCredentialSweep>("claudeCredentialSweep", {
        taskQueue: this.o.taskQueue,
        workflowId: workflowIds.claudeCredentialSweep(this.o.taskQueue),
        args: [{ intervalMs }],
        workflowIdConflictPolicy: "USE_EXISTING",
      });
    } catch (error) {
      if (!(error instanceof WorkflowExecutionAlreadyStartedError)) {
        this.log(`[claude-refresh] could not start the credential sweep: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  }
}

export interface ClaudeCredentialActivityOptions {
  /** Absent where Claude auth is not configured: every refresh finds nothing, and the sweep stops. */
  renewer?: ClaudeCredentialRenewer;
  /** The credential store's encryption key, to seal an unpersisted blob. */
  key?: Buffer;
  sweepIntervalMs?: number;
  sweepMarginMs?: number;
}

/** The Claude credential activities. Registered on every worker, configured or not, so a workflow never finds one missing. */
export function createClaudeCredentialActivities(o: ClaudeCredentialActivityOptions): ClaudeCredentialActivities {
  const intervalMs = o.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS;
  const marginMs = o.sweepMarginMs ?? DEFAULT_SWEEP_MARGIN_MS;
  return {
    async refreshClaudeCredential({ subject, marginMs: refreshMarginMs }): Promise<ClaudeCredentialRefreshResult> {
      if (!o.renewer) return { status: "disabled" };
      const r = await o.renewer.renew(subject, refreshMarginMs);
      const out: ClaudeCredentialRefreshResult = {
        status: r.status,
        ...(r.expiresAt ? { expiresAt: r.expiresAt } : {}),
        ...(r.detail ? { detail: r.detail } : {}),
      };
      if (r.status === "refreshed" && r.persisted === false && r.credentialsJson) {
        // The only live copy. Without a key to seal it there is no safe way
        // to hand it on; the renewer has already said this link needs re-linking.
        if (o.key) out.sealedCredentialsJson = encryptField(o.key, r.credentialsJson);
      }
      return out;
    },
    async claudeCredentialsDue() {
      if (!o.renewer) return { kind: "stop", reason: "Claude auth is not configured on this gateway", intervalMs, marginMs };
      return { ...(await o.renewer.due(marginMs)), intervalMs, marginMs };
    },
  };
}
