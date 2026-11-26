import { beforeEach, describe, expect, it, vi } from "vitest";

// The SDK is mocked so this runs without a Temporal server or the native binding.
const sdk = vi.hoisted(() => ({
  connect: vi.fn(),
  create: vi.fn(),
}));
vi.mock("@temporalio/worker", () => ({
  NativeConnection: { connect: sdk.connect },
  Worker: { create: sdk.create },
}));

const { createSyncWorker } = await import("./temporal.js");

function nativeConnection() {
  return { close: vi.fn(async () => {}) };
}

/** A fake SDK worker: run() ends when shutdown() is called, and refuses shutdown() before run(). */
function sdkWorker() {
  let end!: () => void;
  const w = {
    running: false,
    run: vi.fn(() => {
      w.running = true;
      return new Promise<void>((resolve) => (end = resolve));
    }),
    shutdown: vi.fn(() => {
      if (!w.running) throw new Error("Not running");
      end();
    }),
  };
  return w;
}

const options = {
  address: "temporal:7233",
  namespace: "default",
  taskQueue: "q",
  activities: {} as never,
  workflowBundle: { code: "" },
};

describe("createSyncWorker", () => {
  beforeEach(() => {
    sdk.connect.mockReset();
    sdk.create.mockReset();
  });

  it("closes the connection it opened when Worker.create fails", async () => {
    const conn = nativeConnection();
    sdk.connect.mockResolvedValue(conn);
    sdk.create.mockRejectedValue(new Error("namespace not found"));

    await expect(createSyncWorker(options)).rejects.toThrow("namespace not found");
    expect(conn.close).toHaveBeenCalledTimes(1);
  });

  it("leaves a passed-in connection open when Worker.create fails", async () => {
    const conn = nativeConnection();
    sdk.create.mockRejectedValue(new Error("boom"));

    await expect(createSyncWorker({ ...options, connection: conn as never })).rejects.toThrow("boom");
    expect(sdk.connect).not.toHaveBeenCalled();
    expect(conn.close).not.toHaveBeenCalled();
  });

  it("close() after run() settles closes the connection it opened", async () => {
    const conn = nativeConnection();
    const w = sdkWorker();
    sdk.connect.mockResolvedValue(conn);
    sdk.create.mockResolvedValue(w);

    const worker = await createSyncWorker(options);
    const running = worker.run();
    worker.shutdown();
    await running;
    await worker.close();

    expect(w.run).toHaveBeenCalledTimes(1);
    expect(conn.close).toHaveBeenCalledTimes(1);
  });

  it("close() on a worker never run drains it, then closes the connection", async () => {
    const conn = nativeConnection();
    const w = sdkWorker();
    sdk.connect.mockResolvedValue(conn);
    sdk.create.mockResolvedValue(w);

    const worker = await createSyncWorker(options);
    await worker.close();

    expect(w.run).toHaveBeenCalledTimes(1);
    expect(w.shutdown).toHaveBeenCalledTimes(1);
    expect(conn.close).toHaveBeenCalledTimes(1);
  });

  it("close() leaves a passed-in connection open", async () => {
    const conn = nativeConnection();
    sdk.create.mockResolvedValue(sdkWorker());

    const worker = await createSyncWorker({ ...options, connection: conn as never });
    await worker.close();
    expect(conn.close).not.toHaveBeenCalled();
  });
});
