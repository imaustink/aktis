import { Redis } from "ioredis";
import type { Envelope } from "@aktis/event-triggers";
import type { AcceptedTrigger, OrchestratorInvokeResult } from "./orchestrator-client.js";
import type { SenderClaims } from "./sender-assertion.js";

/**
 * DRAIN ONLY (docs/adr/0057): nothing records here any more; runs are Temporal
 * workflows (src/runs/). runs/legacy-drain.ts reads what pre-0057 gateways
 * left behind. Delete this module one release after 0057 ships.
 */
/**
 * Durable state for the coordination primitives of docs/adr/0055:
 *
 *  - **Held run events** (§2). A trigger whose `onComplete` sets
 *    `waitUntilClosed` and/or `serialize` does not publish its completion
 *    when its run ends. The gateway records it here instead, and a sweep
 *    publishes it once every hold is released: the named issue/PR is closed,
 *    and/or no other run holds the same serialize queue.
 *  - **Serialize queues** (§2). A queue key is held from the moment a held
 *    event is published until the run that event started finishes. The run
 *    renews it while it runs (alongside its in-flight lease), so a gateway
 *    that dies mid-run frees the queue after a short TTL instead of hours.
 *  - **Once-marks** (§1). The conflict sweep's "already emitted for this PR at
 *    this base commit" record, and a per-source sweep lock, so several
 *    gateway replicas sweep once between them.
 *
 * Soft-fails like every other gateway Redis store, toward NOT acting: an
 * unreachable store holds an event (it is retried next sweep) and never hands
 * one queue to two runs.
 */
export interface HeldRunEvent {
  id: string;
  /** Epoch ms -- queue order. */
  createdAt: number;
  /** The finished run, exactly as the immediate emit path would publish it. */
  envelope: Envelope;
  sender?: SenderClaims;
  trigger: AcceptedTrigger;
  finished: { status: OrchestratorInvokeResult["status"]; outcome?: Record<string, unknown> };
  traceparent?: string;
  /** `waitUntilClosed`: published once this issue/PR is closed (or gone). */
  waitFor?: IssueRef;
  /** The issue the held run replied on: closing it abandons the wait. */
  parkedOn?: IssueRef;
  /** `serialize`: `<group>:<key>`. */
  queue?: string;
  /**
   * `waitFor` was open when the event was held, so the wait really blocks on
   * a human merging something: the chain's depth restarts when it is
   * published (docs/adr/0055 §2). Decided once, at hold time, so a sweep never
   * re-saves a record another replica may be claiming.
   */
  blocked?: boolean;
  /**
   * Set when a publish attempt failed and the event was held again: epoch ms
   * of the first such failure, and how many attempts have failed since. The
   * retries are bounded by time since the first failure, not by this record's
   * TTL (which every re-save renews).
   */
  publishFailure?: { firstAt: number; attempts: number };
}

export interface IssueRef {
  owner: string;
  repo: string;
  number: number;
}

export interface DeferredEventStore {
  save(event: HeldRunEvent): Promise<void>;
  list(): Promise<HeldRunEvent[]>;
  /** Removes it; true only for the caller that actually removed it -- the claim before publishing, so two replicas never publish one event. */
  claim(id: string): Promise<boolean>;
  /** Take-or-renew a queue for `holder`. False when someone else holds it, or the store is unreachable. */
  acquireQueue(queue: string, holder: string, ttlMs: number): Promise<boolean>;
  /** Frees a queue `holder` holds; a no-op otherwise. */
  releaseQueue(queue: string, holder: string): Promise<void>;
  /** Sets `key` only if unset. True when this call set it; false when it was set (or the store is unreachable). */
  markOnce(key: string, ttlMs: number): Promise<boolean>;
  /** Clears a {@link markOnce} key (an emit that could not be delivered). */
  unmark(key: string): Promise<void>;
}

export class InMemoryDeferredEventStore implements DeferredEventStore {
  private readonly events = new Map<string, HeldRunEvent>();
  private readonly holds = new Map<string, { value: string; expiresAt: number }>();

  constructor(private readonly now: () => number = () => Date.now()) {}

  async save(event: HeldRunEvent): Promise<void> {
    this.events.set(event.id, structuredClone(event));
  }

  async list(): Promise<HeldRunEvent[]> {
    return [...this.events.values()].map((e) => structuredClone(e));
  }

  async claim(id: string): Promise<boolean> {
    return this.events.delete(id);
  }

  private live(key: string): string | undefined {
    const h = this.holds.get(key);
    if (!h) return undefined;
    if (h.expiresAt <= this.now()) {
      this.holds.delete(key);
      return undefined;
    }
    return h.value;
  }

  async acquireQueue(queue: string, holder: string, ttlMs: number): Promise<boolean> {
    const key = `queue:${queue}`;
    const held = this.live(key);
    if (held !== undefined && held !== holder) return false;
    this.holds.set(key, { value: holder, expiresAt: this.now() + ttlMs });
    return true;
  }

  async releaseQueue(queue: string, holder: string): Promise<void> {
    const key = `queue:${queue}`;
    if (this.live(key) === holder) this.holds.delete(key);
  }

  async markOnce(key: string, ttlMs: number): Promise<boolean> {
    const k = `mark:${key}`;
    if (this.live(k) !== undefined) return false;
    this.holds.set(k, { value: "1", expiresAt: this.now() + ttlMs });
    return true;
  }

  async unmark(key: string): Promise<void> {
    this.holds.delete(`mark:${key}`);
  }
}

/** Long backstop: a held event waits on a human merging something, which can take weeks. */
const EVENT_TTL_SECONDS = 90 * 24 * 60 * 60;

const TAKE_OR_RENEW = `
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

const RELEASE_IF_HELD = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

export class RedisDeferredEventStore implements DeferredEventStore {
  private readonly redis: Redis;

  constructor(
    url: string,
    private readonly prefix = "deferredEvent:",
  ) {
    this.redis = new Redis(url, { maxRetriesPerRequest: 2 });
    this.redis.on("error", (err: Error) => {
      console.error("RedisDeferredEventStore connection error:", err.message);
    });
  }

  private eventKey(id: string): string {
    return `${this.prefix}event:${id}`;
  }

  async save(event: HeldRunEvent): Promise<void> {
    try {
      await this.redis.set(this.eventKey(event.id), JSON.stringify(event), "EX", EVENT_TTL_SECONDS);
    } catch (err) {
      console.error("RedisDeferredEventStore.save failed:", errorText(err));
      throw err;
    }
  }

  async list(): Promise<HeldRunEvent[]> {
    try {
      const keys: string[] = [];
      let cursor = "0";
      do {
        const [next, batch] = await this.redis.scan(cursor, "MATCH", `${this.prefix}event:*`, "COUNT", 100);
        cursor = next;
        keys.push(...batch);
      } while (cursor !== "0");
      if (keys.length === 0) return [];
      const out: HeldRunEvent[] = [];
      for (const raw of await this.redis.mget(keys)) {
        if (typeof raw !== "string") continue;
        try {
          out.push(JSON.parse(raw) as HeldRunEvent);
        } catch {
          // Unreadable: its TTL reaps it.
        }
      }
      return out;
    } catch (err) {
      console.error("RedisDeferredEventStore.list failed (treating as empty):", errorText(err));
      return [];
    }
  }

  async claim(id: string): Promise<boolean> {
    try {
      return (await this.redis.del(this.eventKey(id))) === 1;
    } catch (err) {
      console.error("RedisDeferredEventStore.claim failed (not claiming):", errorText(err));
      return false;
    }
  }

  async acquireQueue(queue: string, holder: string, ttlMs: number): Promise<boolean> {
    try {
      return (await this.redis.eval(TAKE_OR_RENEW, 1, `${this.prefix}queue:${queue}`, holder, String(ttlMs))) === 1;
    } catch (err) {
      console.error("RedisDeferredEventStore.acquireQueue failed (not acquiring):", errorText(err));
      return false;
    }
  }

  async releaseQueue(queue: string, holder: string): Promise<void> {
    try {
      await this.redis.eval(RELEASE_IF_HELD, 1, `${this.prefix}queue:${queue}`, holder);
    } catch (err) {
      console.error("RedisDeferredEventStore.releaseQueue failed (its TTL frees it):", errorText(err));
    }
  }

  async markOnce(key: string, ttlMs: number): Promise<boolean> {
    try {
      return (await this.redis.set(`${this.prefix}mark:${key}`, "1", "PX", ttlMs, "NX")) === "OK";
    } catch (err) {
      console.error("RedisDeferredEventStore.markOnce failed (not marking):", errorText(err));
      return false;
    }
  }

  async unmark(key: string): Promise<void> {
    try {
      await this.redis.del(`${this.prefix}mark:${key}`);
    } catch (err) {
      console.error("RedisDeferredEventStore.unmark failed (ignored):", errorText(err));
    }
  }

  async close(): Promise<void> {
    await this.redis.quit();
  }
}

function errorText(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
