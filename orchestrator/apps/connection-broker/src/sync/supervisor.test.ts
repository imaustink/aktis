import { describe, expect, it, vi } from "vitest";
import { superviseSync, type SupervisedWorker } from "./supervisor.js";

function deferred<T = void>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function client() {
  return { connection: { close: vi.fn(async () => {}) } };
}

/** A worker whose run() ends when shutdown() is called. */
function worker(): SupervisedWorker & { runs: number; shutdowns: number; close: ReturnType<typeof vi.fn> } {
  const done = deferred();
  const w = {
    runs: 0,
    shutdowns: 0,
    close: vi.fn(async () => {}),
    run: () => {
      w.runs++;
      return done.promise;
    },
    shutdown: () => {
      w.shutdowns++;
      done.resolve();
    },
  };
  return w;
}

function hooks() {
  return {
    onConnected: vi.fn(),
    onDisconnected: vi.fn(),
    onError: vi.fn(),
    onWorkerStopped: vi.fn(),
    baseDelayMs: 1,
    maxDelayMs: 5,
  };
}

describe("superviseSync", () => {
  it("stops promptly when stop() arrives while the worker is still being created", async () => {
    const c = client();
    const w = worker();
    const creating = deferred<typeof w>();
    const h = hooks();
    const sup = superviseSync({ connectClient: async () => c, createWorker: () => creating.promise, ...h });

    await vi.waitFor(() => expect(c.connection.close).not.toHaveBeenCalled());
    const stopping = sup.stop();
    creating.resolve(w);
    await stopping;

    expect(w.runs).toBe(0);
    expect(h.onConnected).not.toHaveBeenCalled();
    expect(c.connection.close).toHaveBeenCalledTimes(1);
    // The unrun worker's connection is freed too.
    expect(w.close).toHaveBeenCalledTimes(1);
  });

  it("closes the client when creating the worker fails, then retries", async () => {
    const clients = [client(), client()];
    let n = 0;
    const w = worker();
    const h = hooks();
    const sup = superviseSync({
      connectClient: async () => clients[n]!,
      createWorker: async () => {
        if (n++ === 0) throw new Error("worker create failed");
        return w;
      },
      ...h,
    });

    await vi.waitFor(() => expect(h.onConnected).toHaveBeenCalledWith(clients[1]));
    expect(clients[0]!.connection.close).toHaveBeenCalledTimes(1);
    expect(h.onError).toHaveBeenCalledWith(expect.any(Error), 1);

    await sup.stop();
    expect(w.shutdowns).toBe(1);
    expect(w.close).toHaveBeenCalledTimes(1);
    expect(clients[1]!.connection.close).toHaveBeenCalledTimes(1);
    expect(h.onDisconnected).toHaveBeenCalled();
  });

  it("reconnects with a fresh client when the worker stops on its own", async () => {
    const workers = [worker(), worker()];
    const clients = [client(), client()];
    let n = 0;
    const h = hooks();
    const sup = superviseSync({
      connectClient: async () => clients[n]!,
      createWorker: async () => workers[n++]!,
      ...h,
    });

    await vi.waitFor(() => expect(h.onConnected).toHaveBeenCalledTimes(1));
    workers[0]!.shutdown(); // stops without stop()
    await vi.waitFor(() => expect(h.onConnected).toHaveBeenCalledTimes(2));
    expect(h.onWorkerStopped).toHaveBeenCalledTimes(1);
    expect(clients[0]!.connection.close).toHaveBeenCalledTimes(1);
    expect(workers[0]!.close).toHaveBeenCalledTimes(1);

    await sup.stop();
    expect(clients[1]!.connection.close).toHaveBeenCalledTimes(1);
    expect(workers[1]!.close).toHaveBeenCalledTimes(1);
  });

  it("ends the backoff wait early on stop()", async () => {
    const h = { ...hooks(), baseDelayMs: 60_000, maxDelayMs: 60_000 };
    const sup = superviseSync({
      connectClient: async () => {
        throw new Error("temporal down");
      },
      createWorker: async () => worker(),
      ...h,
    });
    await vi.waitFor(() => expect(h.onError).toHaveBeenCalledTimes(1));
    await sup.stop(); // would take a minute without the wake
    expect(h.onConnected).not.toHaveBeenCalled();
  });
});
