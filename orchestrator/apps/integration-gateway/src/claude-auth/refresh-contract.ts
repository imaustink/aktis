import type { RenewResult, SweepListing } from "./credential-refresher.js";

/**
 * The Claude credential workflows' activities (claude-auth/workflows.ts,
 * implemented in temporal-refresh.ts). Everything here lands in workflow
 * history, so none of it carries token material in the clear.
 */
export interface ClaudeCredentialActivities {
  /**
   * Re-reads `subject`'s `login` credential and, when it is inside `marginMs`
   * of expiry, refreshes and persists it. NOT idempotent -- a repeat after
   * Anthropic rotated the token would spend a dead one -- so the workflow
   * never retries it.
   */
  refreshClaudeCredential(input: ClaudeCredentialRefreshInput): Promise<ClaudeCredentialRefreshResult>;
  /** The sweep's listing, with this replica's current interval and margin. */
  claudeCredentialsDue(): Promise<ClaudeCredentialSweepPlan>;
}

export interface ClaudeCredentialRefreshInput {
  subject: string;
  marginMs: number;
}

export interface ClaudeCredentialRefreshResult {
  status: RenewResult["status"] | "failed";
  expiresAt?: string;
  detail?: string;
  /**
   * Only when the credential was refreshed but did not persist: the refreshed
   * blob, encrypted under the credential store's key (it is the only live
   * copy, and the caller attached to this workflow may be on another
   * replica). Absent otherwise -- the store has the blob to serve.
   */
  sealedCredentialsJson?: string;
}

export type ClaudeCredentialSweepPlan = SweepListing & {
  /** How long to wait before the next sweep. */
  intervalMs: number;
  /** The sweep margin; each due subject is refreshed with it. */
  marginMs: number;
};

export interface ClaudeCredentialSweepInput {
  /** The interval to wait when a listing could not say (it failed). */
  intervalMs: number;
}
