import { resolveGithubToken, type GithubAuthConfig } from "@aktis/github-app-auth";

/**
 * Marker prefixed onto every comment this gateway posts. Lets
 * `identity.ts`/webhook parsing detect and skip the gateway's own replies
 * even in setups where the sender-type/login check alone isn't reliable
 * (e.g. a PAT-based bot without a distinct GitHub Actor type) -- a second,
 * belt-and-suspenders loop guard.
 */
export const REPLY_MARKER = "<!-- agent-controller:reply -->";

export interface GithubReplyClientOptions extends GithubAuthConfig {
  fetchImpl?: typeof fetch;
  /**
   * How many times {@link GithubReplyClient.removeIssueLabel} attempts the
   * DELETE in total before giving up on a NON-404 failure (a transient 5xx, a
   * secondary-rate-limit 403, a dropped connection). Label removal is the one
   * GitHub call whose failure strands a trigger label -- a human then has to
   * remove-then-re-add it by hand to run again -- so unlike the comment POST it
   * gets a bounded retry rather than failing on the first blip. Defaults to
   * {@link DEFAULT_REMOVE_LABEL_RETRY_ATTEMPTS}. A 404 is still success on the
   * first try and never retried.
   */
  removeLabelRetryAttempts?: number;
  /** Base delay (ms) between {@link removeLabelRetryAttempts}; grows linearly per attempt. Defaults to {@link DEFAULT_REMOVE_LABEL_RETRY_DELAY_MS}. */
  removeLabelRetryDelayMs?: number;
  /** Injectable sleep for the retry backoff; defaults to a real timer. Tests pass a no-op to avoid real waits. */
  sleep?: (ms: number) => Promise<void>;
}

/** A few attempts over a few seconds -- a transient-blip retry, not a general backoff policy (mirrors OrchestratorClient's accept retry). */
const DEFAULT_REMOVE_LABEL_RETRY_ATTEMPTS = 3;
const DEFAULT_REMOVE_LABEL_RETRY_DELAY_MS = 500;

/** Minimal GitHub REST client: posts a reply comment on an issue, and removes a trigger label once a run finishes. */
export class GithubReplyClient {
  private readonly fetchImpl: typeof fetch;
  private readonly removeLabelRetryAttempts: number;
  private readonly removeLabelRetryDelayMs: number;
  private readonly sleep: (ms: number) => Promise<void>;

  constructor(private readonly options: GithubReplyClientOptions) {
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.removeLabelRetryAttempts = options.removeLabelRetryAttempts ?? DEFAULT_REMOVE_LABEL_RETRY_ATTEMPTS;
    this.removeLabelRetryDelayMs = options.removeLabelRetryDelayMs ?? DEFAULT_REMOVE_LABEL_RETRY_DELAY_MS;
    this.sleep = options.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  }

  /**
   * Posts a comment and returns its id, for a later
   * {@link updateIssueComment}. The id is `undefined` when the response body
   * doesn't carry one; the comment was still posted.
   */
  async postIssueComment(owner: string, repo: string, issueNumber: number, body: string): Promise<number | undefined> {
    const token = await resolveGithubToken(this.options);
    const res = await this.fetchImpl(
      `${this.options.githubApiUrl}/repos/${owner}/${repo}/issues/${issueNumber}/comments`,
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          accept: "application/vnd.github+json",
          "content-type": "application/json",
          "x-github-api-version": "2022-11-28",
        },
        body: JSON.stringify({ body: `${REPLY_MARKER}\n${body}` }),
      },
    );
    if (!res.ok) {
      throw new Error(`Failed to post issue comment: ${res.status} ${await res.text()}`);
    }
    try {
      const id = ((await res.json()) as { id?: unknown } | null)?.id;
      return typeof id === "number" && Number.isSafeInteger(id) ? id : undefined;
    } catch {
      return undefined;
    }
  }

  /** Replaces the body of a comment this client posted (keeps the {@link REPLY_MARKER} loop guard). */
  async updateIssueComment(owner: string, repo: string, commentId: number, body: string): Promise<void> {
    const token = await resolveGithubToken(this.options);
    const res = await this.fetchImpl(`${this.options.githubApiUrl}/repos/${owner}/${repo}/issues/comments/${commentId}`, {
      method: "PATCH",
      headers: {
        authorization: `Bearer ${token}`,
        accept: "application/vnd.github+json",
        "content-type": "application/json",
        "x-github-api-version": "2022-11-28",
      },
      body: JSON.stringify({ body: `${REPLY_MARKER}\n${body}` }),
    });
    if (!res.ok) {
      throw new Error(`Failed to update issue comment ${commentId}: ${res.status} ${await res.text()}`);
    }
  }

  /**
   * Removes a single label from an issue or pull request. Called once a
   * label-triggered run finishes so the same label can simply be re-applied
   * to run it again (GitHub emits no `labeled` event for a label that is
   * already present, so without this a re-trigger means remove-then-add by
   * hand). Issues and PRs share one number space and one labels endpoint, so
   * this covers both.
   *
   * A 404 is treated as success: the label already being gone (a human
   * removed it mid-run, or two runs raced) is the desired end state, not an
   * error worth failing the turn over.
   *
   * A non-404 failure (a transient 5xx, a secondary-rate-limit 403, or a
   * dropped connection) is RETRIED a bounded number of times with a short
   * linear backoff before it finally throws, because this is the call whose
   * one-shot failure was the headline bug: the trigger label was only removed
   * "about 3/4 of the time", stranding it so a re-apply emitted no `labeled`
   * event and the issue could not be re-triaged without a manual
   * remove-then-add. The operation is idempotent (a 404 on a later attempt,
   * once a prior attempt or a human got there first, is success), so retrying
   * is always safe. A durable reconciling sweep (label-reconciler.ts) is the
   * backstop for the case this retry cannot reach -- the pod dying before the
   * `finally` ran at all.
   */
  async removeIssueLabel(owner: string, repo: string, issueNumber: number, label: string): Promise<void> {
    const token = await resolveGithubToken(this.options);
    await this.withRetry(
      `remove label "${label}"`,
      () =>
        this.fetchImpl(
          `${this.options.githubApiUrl}/repos/${owner}/${repo}/issues/${issueNumber}/labels/${encodeURIComponent(label)}`,
          {
            method: "DELETE",
            headers: {
              authorization: `Bearer ${token}`,
              accept: "application/vnd.github+json",
              "x-github-api-version": "2022-11-28",
            },
          },
        ),
      // Success, or already-gone: either way the end state is reached.
      (status) => (status === 404 ? "done" : "retry"),
    );
  }

  /**
   * Adds labels to an issue or PR (`reply.github.addLabels`, docs/adr/0052
   * §3) -- the factory's status markers. Same bounded retry as
   * {@link removeIssueLabel}: a lost add would leave the PR showing the wrong
   * stage. Idempotent (re-adding a present label is a no-op), so retrying is
   * safe. A 404/422 (no such issue, invalid input) is final and not retried.
   */
  async addIssueLabels(owner: string, repo: string, issueNumber: number, labels: string[]): Promise<void> {
    await this.postIssueField(owner, repo, issueNumber, "labels", labels);
  }

  /**
   * Assigns logins to an issue or PR (`reply.github.assignees`, docs/adr/0052
   * §3), e.g. the factory's handoff to the human. Same retry policy as
   * {@link addIssueLabels}. GitHub silently skips a login that can't be
   * assigned, so a 2xx does not prove the assignment happened.
   */
  async addIssueAssignees(owner: string, repo: string, issueNumber: number, assignees: string[]): Promise<void> {
    await this.postIssueField(owner, repo, issueNumber, "assignees", assignees);
  }

  private async postIssueField(owner: string, repo: string, issueNumber: number, field: "labels" | "assignees", values: string[]): Promise<void> {
    if (values.length === 0) return;
    const token = await resolveGithubToken(this.options);
    await this.withRetry(
      `add ${field} ${JSON.stringify(values)}`,
      () =>
        this.fetchImpl(`${this.options.githubApiUrl}/repos/${owner}/${repo}/issues/${issueNumber}/${field}`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${token}`,
            accept: "application/vnd.github+json",
            "content-type": "application/json",
            "x-github-api-version": "2022-11-28",
          },
          body: JSON.stringify({ [field]: values }),
        }),
      (status) => (status === 404 || status === 422 ? "fail" : "retry"),
    );
  }

  /**
   * The label calls' shared retry: up to {@link removeLabelRetryAttempts}
   * attempts with linear backoff. A 2xx is success; any other status is
   * classified by `onStatus` ("done" = treat as success, "fail" = throw now,
   * "retry" = transient). A rejected fetch (connection reset/refused) is
   * retryable too -- same transient-transport class the orchestrator poll
   * already rides out.
   */
  private async withRetry(
    what: string,
    send: () => Promise<Response>,
    onStatus: (status: number) => "done" | "fail" | "retry",
  ): Promise<void> {
    let lastError: unknown;
    for (let attempt = 1; attempt <= this.removeLabelRetryAttempts; attempt++) {
      let res: Response | undefined;
      try {
        res = await send();
      } catch (err) {
        lastError = err;
      }
      if (res) {
        if (res.ok) return;
        const verdict = onStatus(res.status);
        if (verdict === "done") return;
        lastError = new Error(`Failed to ${what}: ${res.status} ${await res.text()}`);
        if (verdict === "fail") throw lastError;
      }
      if (attempt < this.removeLabelRetryAttempts) await this.sleep(this.removeLabelRetryDelayMs * attempt);
    }
    throw lastError instanceof Error ? lastError : new Error(`Failed to ${what}: ${String(lastError)}`);
  }
}
