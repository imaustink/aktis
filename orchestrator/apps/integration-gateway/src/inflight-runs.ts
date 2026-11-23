import { Redis } from "ioredis";
import type { Envelope } from "@aktis/event-triggers";
import type { AcceptedTrigger } from "./orchestrator-client.js";
import type { SenderClaims } from "./sender-assertion.js";

/**
 * Durable record of accepted event runs this gateway still owes a finish to.
 *
 * Everything that happens AFTER a run ends -- the result comment, the label
 * removal, the HTTP reply and, above all, the run-completion event that
 * starts the next trigger in a chain (docs/adr/0052 §1) -- used to hang off
 * one detached in-process promise that polls the run for up to 8 hours. A
 * gateway rollout in that window dropped all of it: the run finished, and
 * nothing reported it or started the next step. That is how a software-factory
 * chain stopped after a review that requested changes (glyph#99): the review
 * ran, the gateway that was waiting on it was replaced, and the fix step never
 * started.
 *
 * Each accepted run is recorded here before it is polled and cleared once it
 * is fully finished. The process running it holds a short, renewed lease; a
 * record whose lease lapsed has nobody running it, and
 * {@link InflightRunRecovery} takes it over (on startup and periodically) by
 * re-polling the same invocation -- the orchestrator answers that from the
 * engine, whichever process started it.
 *
 * The lease is what keeps two gateways from finishing the same run during a
 * rolling update, when the old pod is still polling as the new one starts.
 * Steps already done are marked on the record, so a takeover never repeats a
 * result comment or, worse, a run event (a second fix run).
 */
export interface InflightRun {
  /** Stable key: the invocation id the run was accepted with. */
  key: string;
  /** The invocation to poll -- changes when an identity-link resume re-submits the event. */
  invocationId: string;
  envelope: Envelope;
  sender?: SenderClaims;
  trigger: AcceptedTrigger;
  traceparent?: string;
  /** The posted ack, so a takeover can still edit the Remote Control link into it. */
  ackComment?: { id: number; body: string };
  /** Done steps -- a takeover skips them. */
  remoteControlPosted?: boolean;
  resultPosted?: boolean;
  emitted?: boolean;
  /** Epoch ms the run was accepted. */
  recordedAt: number;
}

/**
 * Soft-fails like every other gateway Redis store: a brief Redis outage costs
 * runs their restart-survivability, never the run or the webhook relay.
 */
export interface InflightRunStore {
  /** Inserts or replaces a run. */
  save(run: InflightRun): Promise<void>;
  /** Drops a finished run (and its lease). A no-op if already gone. */
  clear(key: string): Promise<void>;
  /** Every run still owed a finish. */
  list(): Promise<InflightRun[]>;
  /**
   * Takes the run's lease for `owner`, or renews it if `owner` already holds
   * it. False when another owner holds an unexpired lease -- or the store
   * could not be reached, so that an outage never hands a live run to a
   * second gateway.
   */
  lease(key: string, owner: string, ttlMs: number): Promise<boolean>;
  /**
   * Whether any run on `sessionId` is still owed a finish. True when the store
   * cannot be read, so an outage never reads as "finished" (the label
   * reconciler strips a label on false).
   */
  hasRunForSession(sessionId: string): Promise<boolean>;
}

/** In-memory {@link InflightRunStore}: for tests and Redis-less installs (where nothing survives a restart anyway). */
export class InMemoryInflightRunStore implements InflightRunStore {
  private readonly runs = new Map<string, InflightRun>();
  private readonly leases = new Map<string, { owner: string; expiresAt: number }>();

  constructor(private readonly now: () => number = () => Date.now()) {}

  async save(run: InflightRun): Promise<void> {
    this.runs.set(run.key, structuredClone(run));
  }

  async clear(key: string): Promise<void> {
    this.runs.delete(key);
    this.leases.delete(key);
  }

  async list(): Promise<InflightRun[]> {
    return [...this.runs.values()].map((r) => structuredClone(r));
  }

  async lease(key: string, owner: string, ttlMs: number): Promise<boolean> {
    const held = this.leases.get(key);
    if (held && held.owner !== owner && held.expiresAt > this.now()) return false;
    this.leases.set(key, { owner, expiresAt: this.now() + ttlMs });
    return true;
  }

  async hasRunForSession(sessionId: string): Promise<boolean> {
    return [...this.runs.values()].some((r) => r.trigger.sessionId === sessionId);
  }
}

/** Longer than any poll (GATEWAY_POLL_TIMEOUT_MS is 8h in production): a backstop, so an unfinishable record cannot live forever. */
const RECORD_TTL_SECONDS = 2 * 24 * 60 * 60;

/** Take-or-renew: the owner renews its own lease; anyone may take a missing or expired one. */
const LEASE_SCRIPT = `
local held = redis.call('GET', KEYS[1])
if held == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
if not held then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
`;

export class RedisInflightRunStore implements InflightRunStore {
  private readonly redis: Redis;

  constructor(
    url: string,
    private readonly prefix = "inflightRun:",
  ) {
    this.redis = new Redis(url, { maxRetriesPerRequest: 2 });
    this.redis.on("error", (err: Error) => {
      console.error("RedisInflightRunStore connection error:", err.message);
    });
  }

  private runKey(key: string): string {
    return `${this.prefix}run:${key}`;
  }

  private leaseKey(key: string): string {
    return `${this.prefix}lease:${key}`;
  }

  async save(run: InflightRun): Promise<void> {
    try {
      await this.redis.set(this.runKey(run.key), JSON.stringify(run), "EX", RECORD_TTL_SECONDS);
    } catch (err) {
      console.error("RedisInflightRunStore.save failed (ignored):", errorText(err));
    }
  }

  async clear(key: string): Promise<void> {
    try {
      await this.redis.del(this.runKey(key), this.leaseKey(key));
    } catch (err) {
      console.error("RedisInflightRunStore.clear failed (ignored):", errorText(err));
    }
  }

  async list(): Promise<InflightRun[]> {
    try {
      return await this.listOrThrow();
    } catch (err) {
      console.error("RedisInflightRunStore.list failed (treating as empty):", errorText(err));
      return [];
    }
  }

  async hasRunForSession(sessionId: string): Promise<boolean> {
    try {
      return (await this.listOrThrow()).some((r) => r.trigger.sessionId === sessionId);
    } catch (err) {
      console.error("RedisInflightRunStore.hasRunForSession failed (treating as still running):", errorText(err));
      return true;
    }
  }

  private async listOrThrow(): Promise<InflightRun[]> {
    const keys: string[] = [];
    let cursor = "0";
    do {
      const [next, batch] = await this.redis.scan(cursor, "MATCH", `${this.prefix}run:*`, "COUNT", 100);
      cursor = next;
      keys.push(...batch);
    } while (cursor !== "0");
    if (keys.length === 0) return [];
    const raws = await this.redis.mget(keys);
    const runs: InflightRun[] = [];
    for (const raw of raws) {
      if (typeof raw !== "string") continue;
      try {
        runs.push(JSON.parse(raw) as InflightRun);
      } catch {
        // An unreadable record can never be finished; its TTL reaps it.
      }
    }
    return runs;
  }

  async lease(key: string, owner: string, ttlMs: number): Promise<boolean> {
    try {
      return (await this.redis.eval(LEASE_SCRIPT, 1, this.leaseKey(key), owner, String(ttlMs))) === 1;
    } catch (err) {
      console.error("RedisInflightRunStore.lease failed (not taking it):", errorText(err));
      return false;
    }
  }

  async close(): Promise<void> {
    await this.redis.quit();
  }
}

function errorText(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export const DEFAULT_LEASE_MS = 90_000;
const DEFAULT_SWEEP_INTERVAL_MS = 60_000;

export interface InflightRunRecoveryOptions {
  store: InflightRunStore;
  /** This process's lease owner id. */
  owner: string;
  /** Finishes a taken-over run. Runs detached; the lease is renewed while it does. */
  resume: (run: InflightRun) => Promise<void>;
  /** Whether this process is already running the run (its own, or one it took over). */
  isRunning: (key: string) => boolean;
  leaseMs?: number;
  intervalMs?: number;
  onError?: (error: unknown) => void;
}

/**
 * Takes over runs nobody is finishing: on startup and every `intervalMs`, any
 * recorded run whose lease has lapsed (its gateway died) is leased to this
 * process and handed to `resume`.
 */
export class InflightRunRecovery {
  private readonly leaseMs: number;
  private readonly intervalMs: number;
  private readonly onError: (error: unknown) => void;
  private timer: ReturnType<typeof setInterval> | undefined;
  private sweeping = false;

  constructor(private readonly options: InflightRunRecoveryOptions) {
    this.leaseMs = options.leaseMs ?? DEFAULT_LEASE_MS;
    this.intervalMs = options.intervalMs ?? DEFAULT_SWEEP_INTERVAL_MS;
    this.onError = options.onError ?? ((e) => console.error(e));
  }

  /** One pass. Never throws. Resolves once every taken-over run has been STARTED (not finished). */
  async sweepOnce(): Promise<void> {
    let runs: InflightRun[];
    try {
      runs = await this.options.store.list();
    } catch (error) {
      this.onError(error);
      return;
    }
    for (const run of runs) {
      if (this.options.isRunning(run.key)) continue;
      try {
        if (!(await this.options.store.lease(run.key, this.options.owner, this.leaseMs))) continue;
      } catch (error) {
        this.onError(error);
        continue;
      }
      console.log(`integration-gateway: taking over run ${run.key} (trigger ${run.trigger.name}, invocation ${run.invocationId})`);
      this.options.resume(run).catch(this.onError);
    }
  }

  start(): void {
    if (this.timer) return;
    void this.tick();
    this.timer = setInterval(() => void this.tick(), this.intervalMs);
    this.timer.unref?.();
  }

  private async tick(): Promise<void> {
    if (this.sweeping) return;
    this.sweeping = true;
    try {
      await this.sweepOnce();
    } finally {
      this.sweeping = false;
    }
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }
}
