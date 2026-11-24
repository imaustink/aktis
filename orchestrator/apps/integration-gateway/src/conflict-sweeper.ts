import type { CompiledConflicts } from "./event-sources.js";
import type { PullRequestState } from "./github-client.js";
import type { DeferredEventStore } from "./deferred-events.js";

/**
 * The `conflicts` EventSource kind (docs/adr/0055 §1): every `intervalMs`,
 * find the open pull requests `author` opened that now conflict with their
 * base, and hand each to `emit` -- once per (PR, base commit).
 *
 * Deterministic on purpose: which PRs conflict is GitHub's `mergeable_state`,
 * not anyone's judgment. A PR is skipped when it is a draft, carries one of
 * `skipLabels` (the factory's own "a run is on this" labels -- the running
 * chain syncs its branch itself), GitHub hasn't computed mergeability yet
 * (`null` right after the base moved; the next sweep sees it), or it has
 * no assignee who still verifies. The assignee is who the resync runs as:
 * the factory's handoff assigns the human who started the PR's chain, and a
 * run must act as a verified person, never the bot or whoever clicked merge.
 *
 * Once per (PR, base sha): a resync that couldn't resolve the conflict is not
 * retried until the base moves again, so a hard conflict costs one run per
 * merge, not one per sweep.
 */
export interface ConflictCandidate {
  owner: string;
  repo: string;
  pr: PullRequestState;
  assignee: { login: string; accountId?: string };
}

export interface ConflictSweeperOptions {
  /** The current `conflicts` sources, by name. */
  sources: () => Array<{ name: string; conflicts: CompiledConflicts }>;
  github: {
    searchOpenPullRequests(author: string): Promise<Array<{ owner: string; repo: string; number: number }>>;
    getPullRequest(owner: string, repo: string, number: number): Promise<PullRequestState | undefined>;
  };
  store: DeferredEventStore;
  /** True when `login` still verifies against the repository (the run-event per-hop check). */
  verify: (login: string, owner: string, repo: string) => Promise<boolean>;
  /** Relays one conflict event. Resolves whether it was delivered (accepted, ignored or rejected -- anything but a transport failure). */
  emit: (source: string, candidate: ConflictCandidate) => Promise<boolean>;
  log?: (line: string) => void;
  onError?: (error: unknown) => void;
  /** How often to check whether any source is due. */
  tickMs?: number;
  now?: () => number;
}

/** A mark outlives any plausible gap between a conflict and the next base move. */
const MARK_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const DEFAULT_TICK_MS = 30_000;

export class ConflictSweeper {
  private timer: ReturnType<typeof setInterval> | undefined;
  private running = false;
  private readonly lastSwept = new Map<string, number>();
  private readonly log: (line: string) => void;
  private readonly onError: (error: unknown) => void;
  private readonly now: () => number;

  constructor(private readonly options: ConflictSweeperOptions) {
    this.log = options.log ?? ((l) => console.error(l));
    this.onError = options.onError ?? ((e) => console.error(e));
    this.now = options.now ?? (() => Date.now());
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => void this.tick(), this.options.tickMs ?? DEFAULT_TICK_MS);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  /** Sweeps every source that is due. Never throws. */
  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      for (const source of this.options.sources()) {
        const last = this.lastSwept.get(source.name);
        if (last !== undefined && this.now() - last < source.conflicts.intervalMs) continue;
        this.lastSwept.set(source.name, this.now());
        // One replica per interval: the first to set the mark sweeps.
        const slot = Math.floor(this.now() / source.conflicts.intervalMs);
        if (!(await this.options.store.markOnce(`sweep:${source.name}:${slot}`, source.conflicts.intervalMs))) continue;
        await this.sweepSource(source.name, source.conflicts);
      }
    } catch (error) {
      this.onError(error);
    } finally {
      this.running = false;
    }
  }

  /** One source, now. Exported for tests. */
  async sweepSource(name: string, conflicts: CompiledConflicts): Promise<void> {
    let prs: Array<{ owner: string; repo: string; number: number }>;
    try {
      prs = await this.options.github.searchOpenPullRequests(conflicts.author);
    } catch (error) {
      this.onError(error);
      return;
    }
    for (const ref of prs) {
      try {
        await this.consider(name, conflicts, ref.owner, ref.repo, ref.number);
      } catch (error) {
        this.onError(error);
      }
    }
  }

  private async consider(name: string, conflicts: CompiledConflicts, owner: string, repo: string, number: number): Promise<void> {
    const tag = `conflicts ${name}: ${owner}/${repo}#${number}`;
    const pr = await this.options.github.getPullRequest(owner, repo, number);
    if (!pr || pr.state !== "open" || pr.draft) return;
    if (pr.mergeable !== false || pr.mergeableState !== "dirty") return;
    const busy = pr.labels.find((l) => conflicts.skipLabels.includes(l));
    if (busy) {
      this.log(`${tag}: conflicts, but skipped (label ${busy} -- a run is already on it)`);
      return;
    }
    const markKey = `conflict:${name}:${owner}/${repo}#${number}@${pr.baseSha}`;
    let assignee: ConflictCandidate["assignee"] | undefined;
    for (const a of pr.assignees) {
      if (await this.options.verify(a.login, owner, repo)) {
        assignee = { login: a.login, ...(a.id !== undefined ? { accountId: String(a.id) } : {}) };
        break;
      }
    }
    if (!assignee) {
      this.log(`${tag}: conflicts, but skipped (no assignee who verifies to run the resync as)`);
      return;
    }
    if (!(await this.options.store.markOnce(markKey, MARK_TTL_MS))) return;
    this.log(`${tag}: conflicts with ${pr.baseRef}@${pr.baseSha.slice(0, 7)}; emitting a conflict event as ${assignee.login}`);
    let delivered = false;
    try {
      delivered = await this.options.emit(name, { owner, repo, pr, assignee });
    } finally {
      // Undelivered (the orchestrator was unreachable): try again next sweep.
      if (!delivered) await this.options.store.unmark(markKey);
    }
  }
}
