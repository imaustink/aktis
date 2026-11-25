import type { ClaudeTokenStore } from "./store.js";

/**
 * Refreshing a stored `login` (Remote Control) credential here, in the gateway,
 * rather than leaving it to whatever run happens to pick it up next.
 *
 * Why this has to exist at all. Anthropic ROTATES the refresh token on every
 * refresh, and until now the only thing that ever refreshed was a run's own
 * in-pod CLI. That gave the stored credential two ways to die, and the store
 * was a passive bystander for both:
 *
 *   1. a refresh the pod failed to report back killed the stored copy (its
 *      refresh token had been spent) -- narrowed by the continuous write-back
 *      in claude-code-swe-agent's `credentialsWriteback.ts`, but still only
 *      narrowed, because two concurrent runs for one subject can each refresh
 *      from the same stored token and invalidate the other;
 *   2. nothing refreshed it between runs, so a link nobody exercised aged out
 *      on its own. The CLI itself knows this failure has a name --
 *      `refresh_token_expired` and `refresh_token_expires_in` are both strings
 *      in the shipped binary -- so a refresh token is NOT indefinitely valid
 *      and an idle link is a link on a timer.
 *
 * Doing it here fixes both, because the gateway is the one place that holds
 * the store and can serialize per subject. It is NOT one process, though: the
 * chart allows several replicas, and a rollout overlaps old and new pods. So
 * the serialization lives in Temporal, not in memory (docs/adr/0058 §3): every
 * refresh of a subject runs as that subject's `claudeCredentialRefresh`
 * workflow (claude-auth/workflows.ts), whose id is the subject's, and every
 * caller on every replica attaches to the one that is running. The pieces:
 *
 *   - {@link ClaudeCredentialRefresher}: the read path. Serves a healthy blob
 *     without leaving the process; hands a due one to the workflow.
 *   - {@link ClaudeCredentialRenewer}: what the workflow's activity runs --
 *     re-read, re-check, refresh, persist -- and the sweep's listing.
 *   - claude-auth/temporal-refresh.ts: starting/attaching to the workflows.
 *
 * ONE INVARIANT DOMINATES THIS FILE: the instant Anthropic returns new tokens,
 * the old refresh token is dead. From that moment the new blob is the only
 * living copy of the credential, so it must be persisted (or, failing that,
 * still handed to the caller and complained about loudly) rather than dropped.
 * Every error path below is written so that a refresh we did not complete
 * leaves the stored credential exactly as it was.
 */

/** Claude Code's OAuth token endpoint. Confirmed present in the shipped CLI binary (v2.1.220). */
export const CLAUDE_TOKEN_URL = "https://platform.claude.com/v1/oauth/token";

/**
 * Claude Code's public OAuth client id. Taken from the authorize URL this
 * gateway itself hands users (see the `client_id` in the link it posts), so it
 * is observed rather than assumed.
 */
export const CLAUDE_OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";

/** Refresh when the access token expires within this long. */
export const DEFAULT_REFRESH_MARGIN_MS = 30 * 60_000;

/**
 * The refresh outcome, kept coarse on purpose -- callers only ever need to know
 * "is there a newer blob to use" and "did the credential just become
 * unrecoverable".
 */
export type RefreshStatus =
  | "refreshed"
  /** Still comfortably valid; nothing was sent. */
  | "not-needed"
  /** No refresh token in the blob, or the blob is not a credentials file. */
  | "unrefreshable"
  /** The service rejected the refresh token itself -- the link is dead and needs a human. */
  | "rejected"
  /** Network/5xx/unparseable -- the stored credential is untouched and retrying later is right. */
  | "transient";

export interface RefreshOutcome {
  status: RefreshStatus;
  /** Present only on `"refreshed"`: the new blob, which is now the ONLY live copy. */
  credentialsJson?: string;
  /** Present only on `"refreshed"`: the new expiry, for logging. */
  expiresAt?: string;
  detail?: string;
}

interface OauthFields {
  refreshToken?: string;
  expiresAt?: number;
}

/** The credentials file nests everything under `claudeAiOauth`; tolerate a flat blob too. */
function readOauth(blob: string): { root: Record<string, unknown>; inner: Record<string, unknown> } | null {
  try {
    const root = JSON.parse(blob) as Record<string, unknown>;
    if (typeof root !== "object" || root === null) return null;
    const nested = root.claudeAiOauth;
    const inner = typeof nested === "object" && nested !== null ? (nested as Record<string, unknown>) : root;
    return { root, inner };
  } catch {
    return null;
  }
}

function fields(inner: Record<string, unknown>): OauthFields {
  const rawExpiry = inner.expiresAt;
  const expiresAt =
    typeof rawExpiry === "number" ? rawExpiry : typeof rawExpiry === "string" ? Number(rawExpiry) : Number.NaN;
  return {
    refreshToken: typeof inner.refreshToken === "string" && inner.refreshToken ? inner.refreshToken : undefined,
    expiresAt: Number.isFinite(expiresAt) ? expiresAt : undefined,
  };
}

/** The blob's access-token expiry as an ISO string, or null. Never returns token material. */
export function credentialExpiresAt(blob: string): string | null {
  const parsed = readOauth(blob);
  if (!parsed) return null;
  const { expiresAt } = fields(parsed.inner);
  return expiresAt === undefined ? null : new Date(expiresAt).toISOString();
}

/**
 * True when the access token is inside `marginMs` of expiry (or already past
 * it). A blob with no readable expiry is treated as due: an unknown expiry is
 * far more likely to be an expired credential than a healthy one, and a
 * needless refresh costs one round trip while a skipped one costs a re-link.
 */
export function needsRefresh(blob: string, marginMs = DEFAULT_REFRESH_MARGIN_MS, now = Date.now()): boolean {
  const parsed = readOauth(blob);
  if (!parsed) return false; // not a credentials file at all -- nothing to do
  const { expiresAt } = fields(parsed.inner);
  if (expiresAt === undefined) return true;
  return expiresAt - now <= marginMs;
}

/**
 * True when the blob carries a refresh token at all. A blob without one can
 * never be refreshed, so a due one is served as it is without asking Temporal
 * to serialize a refresh that cannot happen.
 */
export function isRefreshable(blob: string): boolean {
  const parsed = readOauth(blob);
  return parsed !== null && fields(parsed.inner).refreshToken !== undefined;
}

/**
 * Rebuilds the credentials blob with the new tokens, PRESERVING every other
 * field. The file carries things this code has no business understanding
 * (subscription type, scopes, account hints); dropping them would hand runs a
 * blob subtly unlike the one `claude auth login` produces.
 */
function mergeRefreshed(
  blob: string,
  next: { accessToken: string; refreshToken: string; expiresAt: number; scopes?: string[] },
): string {
  const parsed = readOauth(blob);
  const inner = parsed ? { ...parsed.inner } : {};
  inner.accessToken = next.accessToken;
  inner.refreshToken = next.refreshToken;
  inner.expiresAt = next.expiresAt;
  if (next.scopes && next.scopes.length > 0) inner.scopes = next.scopes;
  // Keep the nesting the file actually had.
  if (parsed && parsed.root.claudeAiOauth !== undefined) {
    return JSON.stringify({ ...parsed.root, claudeAiOauth: inner });
  }
  return JSON.stringify(inner);
}

export interface RefreshDeps {
  fetchImpl?: typeof fetch;
  tokenUrl?: string;
  clientId?: string;
  marginMs?: number;
  now?: () => number;
}

/**
 * Exchanges the blob's refresh token for a new pair.
 *
 * The request is the standard OAuth refresh grant against the endpoint and
 * client id above. Both of those are observed (binary / our own authorize URL),
 * but the exact body this service expects is NOT something this repo can prove
 * from the outside, so every non-success path is deliberately inert: on
 * anything other than a 2xx carrying a parseable token pair, the caller keeps
 * using the credential it already had and the run behaves exactly as it does
 * today. The only way this can make things worse is if the service accepts a
 * refresh and we then lose the result -- which is why `ClaudeCredentialRenewer`
 * persists before returning and shouts if it cannot.
 */
export async function refreshCredentialsBlob(blob: string, deps: RefreshDeps = {}): Promise<RefreshOutcome> {
  const parsed = readOauth(blob);
  if (!parsed) return { status: "unrefreshable", detail: "not a credentials file" };
  const { refreshToken } = fields(parsed.inner);
  if (!refreshToken) return { status: "unrefreshable", detail: "blob carries no refreshToken" };

  const fetchImpl = deps.fetchImpl ?? fetch;
  const now = deps.now ?? Date.now;
  let res: Response;
  try {
    res = await fetchImpl(deps.tokenUrl ?? CLAUDE_TOKEN_URL, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded", accept: "application/json" },
      body: new URLSearchParams({
        grant_type: "refresh_token",
        refresh_token: refreshToken,
        client_id: deps.clientId ?? CLAUDE_OAUTH_CLIENT_ID,
      }).toString(),
    });
  } catch (err) {
    return { status: "transient", detail: err instanceof Error ? err.message : String(err) };
  }

  if (!res.ok) {
    let body = "";
    try {
      body = (await res.text()).slice(0, 300);
    } catch {
      /* body is a nicety here, not a requirement */
    }
    // `invalid_grant` is the service saying the refresh token is spent or
    // expired -- the one case where trying again later cannot help, and the
    // case a human has to resolve by re-linking.
    const rejected = res.status === 400 && /invalid_grant|refresh_token_expired/i.test(body);
    return { status: rejected ? "rejected" : "transient", detail: `HTTP ${res.status}${body ? `: ${body}` : ""}` };
  }

  let payload: Record<string, unknown>;
  try {
    payload = (await res.json()) as Record<string, unknown>;
  } catch (err) {
    return { status: "transient", detail: `unparseable response: ${err instanceof Error ? err.message : String(err)}` };
  }

  const accessToken = typeof payload.access_token === "string" ? payload.access_token : "";
  // A rotating server returns a new refresh token; a non-rotating one omits it
  // and the old one stays valid. Carry the old one forward in that case rather
  // than writing `undefined` into the file.
  const nextRefresh = typeof payload.refresh_token === "string" && payload.refresh_token ? payload.refresh_token : refreshToken;
  const expiresIn = typeof payload.expires_in === "number" ? payload.expires_in : Number(payload.expires_in);
  if (!accessToken || !Number.isFinite(expiresIn)) {
    return { status: "transient", detail: "response carried no usable access_token/expires_in" };
  }

  const expiresAt = now() + Math.floor(expiresIn) * 1000;
  const scopes =
    typeof payload.scope === "string" && payload.scope.trim() ? payload.scope.trim().split(/\s+/) : undefined;
  return {
    status: "refreshed",
    credentialsJson: mergeRefreshed(blob, { accessToken, refreshToken: nextRefresh, expiresAt, scopes }),
    expiresAt: new Date(expiresAt).toISOString(),
  };
}

/** What one refresh of a subject came to. Token material is only ever in `credentialsJson`, which never leaves the process (see temporal-refresh.ts). */
export interface RenewResult {
  status: RefreshStatus | "missing" | "disabled";
  /** The refreshed blob, on `"refreshed"` only. */
  credentialsJson?: string;
  /**
   * On `"refreshed"`: whether the blob read back from the store. False means
   * the old refresh token is spent and this blob is the ONLY live copy -- it
   * must still reach the caller.
   */
  persisted?: boolean;
  expiresAt?: string;
  detail?: string;
}

/** One sweep's listing: the subjects due for renewal, or why there is nothing to sweep. */
export type SweepListing =
  /** The sweep has nothing it may do here (refresh disabled, a store that cannot enumerate): it ends. */
  | { kind: "stop"; reason: string }
  /** The listing itself failed; the next sweep tries again. */
  | { kind: "failed"; detail: string }
  | { kind: "listed"; examined: number; due: string[]; unrefreshable: number };

export interface ClaudeCredentialRenewerDeps extends RefreshDeps {
  store: Pick<ClaudeTokenStore, "get" | "set" | "listSubjects">;
  log?: (message: string) => void;
  /** Set false to leave stored credentials strictly alone (an operational off switch). */
  enabled?: boolean;
}

/**
 * Refreshes and persists one subject's stored `login` credential -- the body
 * of the `claudeCredentialRefresh` workflow's one activity, and so the only
 * code in the gateway that ever spends a stored refresh token.
 *
 * It must only ever run inside that workflow: the workflow's id is the
 * subject's, so Temporal runs at most one per subject across every replica,
 * and that is the whole of the serialization. Two callers refreshing the same
 * credential concurrently would each spend the same refresh token, and
 * whichever landed second would invalidate the first.
 *
 * It re-reads the store rather than trusting the caller's copy: by the time a
 * refresh runs, an earlier one (or a run's own write-back) may already have
 * rotated the token, and refreshing from a stale blob would spend a dead token.
 */
export class ClaudeCredentialRenewer {
  /**
   * Belt and braces within one worker: an activity Temporal has already timed
   * out may still be running here, and a new refresh of the subject must not
   * overlap it. Across replicas the workflow id is what serializes.
   */
  private readonly inFlight = new Map<string, Promise<RenewResult>>();
  private readonly log: (message: string) => void;

  constructor(private readonly deps: ClaudeCredentialRenewerDeps) {
    this.log = deps.log ?? ((m) => console.log(m));
  }

  async renew(subject: string, marginMs: number): Promise<RenewResult> {
    for (let pending = this.inFlight.get(subject); pending; pending = this.inFlight.get(subject)) {
      await pending.catch(() => undefined);
    }
    const attempt = this.renewOnce(subject, marginMs).finally(() => this.inFlight.delete(subject));
    this.inFlight.set(subject, attempt);
    return attempt;
  }

  private async renewOnce(subject: string, marginMs: number): Promise<RenewResult> {
    if (this.deps.enabled === false) return { status: "disabled" };
    const now = this.deps.now ?? Date.now;
    const blob = (await this.deps.store.get(subject, "login"))?.credentialsJson;
    if (!blob) return { status: "missing" };
    if (!needsRefresh(blob, marginMs, now())) return { status: "not-needed" };

    const outcome = await refreshCredentialsBlob(blob, this.deps);

    if (outcome.status !== "refreshed" || !outcome.credentialsJson) {
      // Nothing was rotated, so the stored credential is still whatever it was.
      // `rejected` is worth saying out loud (a human will have to re-link) but
      // deleting the record is deliberately NOT done here: the orchestrator's
      // existing re-auth path owns invalidation, and a refresh endpoint hiccup
      // that merely looked like a rejection must not cost a working link.
      this.log(
        `[claude-refresh] ${outcome.status} for ${subject}${outcome.detail ? `: ${outcome.detail}` : ""}; serving the stored credential unchanged`,
      );
      return { status: outcome.status, ...(outcome.detail ? { detail: outcome.detail } : {}) };
    }

    // From here the OLD refresh token is dead and this blob is the only living
    // copy of the credential. Persisting it is not bookkeeping, it is the
    // difference between a durable link and a re-link prompt.
    const refreshed = {
      status: "refreshed" as const,
      credentialsJson: outcome.credentialsJson,
      ...(outcome.expiresAt ? { expiresAt: outcome.expiresAt } : {}),
    };
    try {
      await this.deps.store.set(subject, {
        kind: "login",
        credentialsJson: outcome.credentialsJson,
        createdAt: new Date(now()).toISOString(),
      });
      const readBack = await this.deps.store.get(subject, "login");
      if (readBack?.credentialsJson !== outcome.credentialsJson) {
        // `set` swallows its own storage errors by design, so a silent drop
        // would otherwise be indistinguishable from success -- and it is the
        // one outcome that destroys the link.
        this.log(
          `[claude-refresh] STORED-BUT-UNVERIFIED for ${subject}: the refreshed credential did not read back. ` +
            `The previous refresh token is already spent, so the stored credential is now stale and this link will need re-linking.`,
        );
        return { ...refreshed, persisted: false };
      }
    } catch (err) {
      this.log(
        `[claude-refresh] FAILED TO PERSIST refreshed credential for ${subject} (${err instanceof Error ? err.message : String(err)}). ` +
          `The previous refresh token is already spent, so this link will need re-linking.`,
      );
      // Still hand back the live blob: this caller can at least succeed, and
      // the run's own write-back may yet persist it.
      return { ...refreshed, persisted: false };
    }

    this.log(`[claude-refresh] refreshed ${subject}'s Remote Control credential (expires ${outcome.expiresAt})`);
    return { ...refreshed, persisted: true };
  }

  /**
   * The sweep's listing: every subject whose `login` credential is inside
   * `marginMs` of expiry and can be refreshed at all.
   */
  async due(marginMs: number): Promise<SweepListing> {
    if (this.deps.enabled === false) return { kind: "stop", reason: "Claude credential refresh is disabled" };
    if (!this.deps.store.listSubjects) {
      // Standing down is stated, not silent: a sweeper that quietly did nothing
      // would look exactly like a working one right up until someone's link
      // expired.
      return { kind: "stop", reason: "this credential store cannot enumerate subjects" };
    }
    let subjects: string[];
    try {
      subjects = await this.deps.store.listSubjects("login");
    } catch (err) {
      // A list that failed is not an empty store, and treating it as one is
      // how a sweep does nothing forever: the workflow says so out loud.
      return { kind: "failed", detail: err instanceof Error ? err.message : String(err) };
    }
    const now = this.deps.now ?? Date.now;
    const due: string[] = [];
    let unrefreshable = 0;
    for (const subject of subjects) {
      const blob = (await this.deps.store.get(subject, "login"))?.credentialsJson;
      if (!blob || !needsRefresh(blob, marginMs, now())) continue;
      if (isRefreshable(blob)) due.push(subject);
      else unrefreshable += 1;
    }
    return { kind: "listed", examined: subjects.length, due, unrefreshable };
  }
}

/** Serializes a refresh across every replica: claude-auth/temporal-refresh.ts in production. */
export interface CredentialRefreshSerializer {
  /**
   * Runs (or attaches to the running) refresh of `subject`. Returns the blob
   * to serve when it is one the store does not have (refreshed, but it would
   * not persist); otherwise nothing, meaning "read the store again". Throws
   * when the refresh could not be reached or did not answer in time.
   */
  refresh(subject: string, marginMs: number): Promise<{ credentialsJson?: string }>;
}

export interface ClaudeCredentialRefresherDeps {
  store: Pick<ClaudeTokenStore, "get">;
  serializer: CredentialRefreshSerializer;
  log?: (message: string) => void;
  /** Set false to leave stored credentials strictly alone (an operational off switch). */
  enabled?: boolean;
  marginMs?: number;
  now?: () => number;
}

/**
 * The read path: the freshest `login` blob for a subject, about to be handed
 * to a run.
 *
 * A healthy blob never leaves the process. A due one is refreshed through the
 * {@link CredentialRefreshSerializer} -- the subject's one Temporal workflow --
 * and never here: a refresh here would be serialized only within this
 * process, and the gateway runs as several. Concurrent callers in this process
 * share one in-flight promise first, so a burst of launches costs one workflow
 * start; callers on other replicas attach to the same workflow.
 */
export class ClaudeCredentialRefresher {
  private readonly inFlight = new Map<string, Promise<string | undefined>>();
  private readonly log: (message: string) => void;

  constructor(private readonly deps: ClaudeCredentialRefresherDeps) {
    this.log = deps.log ?? ((m) => console.log(m));
  }

  /**
   * Returns the freshest `login` blob for `subject`, refreshing and persisting
   * first when it is at or near expiry. Never throws and never returns
   * undefined for a credential that exists -- a failed refresh yields the blob
   * that was already stored, so the caller is never worse off than before.
   */
  async ensureFresh(subject: string): Promise<string | undefined> {
    const blob = (await this.deps.store.get(subject, "login"))?.credentialsJson;
    if (!blob) return undefined;
    if (this.deps.enabled === false) return blob;
    const marginMs = this.deps.marginMs ?? DEFAULT_REFRESH_MARGIN_MS;
    if (!needsRefresh(blob, marginMs, (this.deps.now ?? Date.now)())) return blob;
    if (!isRefreshable(blob)) {
      this.log(`[claude-refresh] unrefreshable for ${subject}: blob carries no refreshToken; serving the stored credential unchanged`);
      return blob;
    }

    const pending = this.inFlight.get(subject);
    if (pending) return pending;

    const attempt = this.refresh(subject, blob, marginMs).finally(() => this.inFlight.delete(subject));
    this.inFlight.set(subject, attempt);
    return attempt;
  }

  private async refresh(subject: string, blob: string, marginMs: number): Promise<string | undefined> {
    let result: { credentialsJson?: string };
    try {
      result = await this.deps.serializer.refresh(subject, marginMs);
    } catch (err) {
      // Never fall back to refreshing here: an unserialized refresh is the
      // rotation race this all exists to prevent. The stored blob is what the
      // caller would have had before refresh existed, and it is only inside
      // its margin, not necessarily expired.
      this.log(
        `[claude-refresh] REFRESH UNAVAILABLE for ${subject} (${err instanceof Error ? err.message : String(err)}); ` +
          `serving the stored credential unrefreshed. While this persists, no Claude credential is being renewed.`,
      );
      return blob;
    }
    if (result.credentialsJson) return result.credentialsJson;
    // Refreshed and persisted, refreshed by someone else first, or left alone
    // (transient, rejected): the store has the blob to serve either way.
    try {
      return (await this.deps.store.get(subject, "login"))?.credentialsJson ?? blob;
    } catch {
      return blob;
    }
  }
}

/** How often the background sweep runs. */
export const DEFAULT_SWEEP_INTERVAL_MS = 60 * 60_000;

/**
 * How close to expiry a credential must be for the SWEEP to renew it.
 *
 * Wider than the on-read margin on purpose. The sweep's job is that a link is
 * never found dead, so it should act well before anything needs the credential;
 * the read path's job is only that the blob it is about to hand out is usable.
 */
export const DEFAULT_SWEEP_MARGIN_MS = 4 * 60 * 60_000;
