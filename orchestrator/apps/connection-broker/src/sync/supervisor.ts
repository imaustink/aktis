/**
 * Keeps the sync worker connected to Temporal (docs/adr/0058 §2).
 *
 * Temporal is reached in the background and retried with backoff for as long
 * as it takes, so the API this process also serves never waits on it. A worker
 * that stops on its own is reconnected the same way. `stop()` ends the loop at
 * any point, including while a worker is still being created.
 */

export interface SupervisedClient {
  connection: { close(): Promise<void> };
}

export interface SupervisedWorker {
  run(): Promise<void>;
  shutdown(): void;
  /** Frees the worker's connection; called once, after `run()` settles or instead of it. */
  close(): Promise<void>;
}

export interface SyncSupervisorOptions<C extends SupervisedClient, W extends SupervisedWorker> {
  connectClient(): Promise<C>;
  createWorker(): Promise<W>;
  /** The client and worker are up; work may be started on `client`. */
  onConnected(client: C): void;
  /** The client is no longer usable (its connection is about to close). */
  onDisconnected(): void;
  /** Logs a failed attempt; `attempt` counts from 1. */
  onError(err: unknown, attempt: number): void;
  /** Logs a worker that stopped without `stop()`. */
  onWorkerStopped(): void;
  /** Backoff ceiling between attempts. Default 1 minute. */
  maxDelayMs?: number;
  /** Backoff floor (first retry). Default 1 second. */
  baseDelayMs?: number;
}

export interface SyncSupervisor {
  stop(): Promise<void>;
}

export function superviseSync<C extends SupervisedClient, W extends SupervisedWorker>(
  o: SyncSupervisorOptions<C, W>,
): SyncSupervisor {
  const maxDelayMs = o.maxDelayMs ?? 60_000;
  const baseDelayMs = o.baseDelayMs ?? 1_000;
  let worker: W | undefined;
  let stopped = false;
  /** Ends the wait between connection attempts early (on stop). */
  let wake: (() => void) | undefined;

  const closeQuietly = (client: C) => client.connection.close().catch(() => {});

  const loop = async (): Promise<void> => {
    for (let attempt = 0; !stopped; attempt++) {
      try {
        const client = await o.connectClient();
        let created: W | undefined;
        try {
          if (stopped) return;
          created = await o.createWorker();
          // stop() ran while the worker was being created, so it had nothing
          // to shut down. Running it now would never end.
          if (stopped) return;
          worker = created;
          const running = worker.run();
          o.onConnected(client);
          attempt = 0;
          await running;
          if (stopped) return;
          o.onWorkerStopped();
        } finally {
          worker = undefined;
          o.onDisconnected();
          // Both connections are closed on every path, so a Temporal that
          // stays half-up does not leak one pair per retry.
          await created?.close().catch(() => {});
          await closeQuietly(client);
        }
      } catch (err) {
        if (stopped) return;
        o.onError(err, attempt + 1);
      }
      if (stopped) return;
      const delayMs = Math.min(maxDelayMs, baseDelayMs * 2 ** Math.min(attempt, 6));
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, delayMs);
        wake = () => {
          clearTimeout(t);
          resolve();
        };
      });
      wake = undefined;
    }
  };
  const running = loop();

  return {
    stop: async () => {
      stopped = true;
      wake?.();
      try {
        worker?.shutdown();
      } catch {
        // Already shutting down.
      }
      await running;
    },
  };
}
