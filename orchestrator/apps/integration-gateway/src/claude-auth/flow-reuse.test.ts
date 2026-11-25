import { describe, expect, it } from "vitest";
import { ClaudeLoginFlows } from "./pty-login.js";
import { ClaudeSetupTokenFlows } from "./pty-setup-token.js";

/**
 * One live flow per subject. Every link prompt calls `start()`, and the chat
 * facade re-issues a parked turn's prompt every few seconds; a turn missing
 * both Claude credentials can anchor only one, so the other used to spawn a
 * new ~150 MiB `claude` process on every resume and OOM-kill the gateway.
 */
class FakePty {
  private dataHandlers: ((chunk: string) => void)[] = [];
  private exitHandlers: ((e: { exitCode: number }) => void)[] = [];
  public killed = false;
  onData(handler: (chunk: string) => void) {
    this.dataHandlers.push(handler);
    return { dispose: () => (this.dataHandlers = this.dataHandlers.filter((h) => h !== handler)) };
  }
  onExit(handler: (e: { exitCode: number }) => void) {
    this.exitHandlers.push(handler);
    return { dispose: () => (this.exitHandlers = this.exitHandlers.filter((h) => h !== handler)) };
  }
  write(): void {}
  kill(): void {
    this.killed = true;
  }
  emitData(chunk: string): void {
    for (const h of this.dataHandlers) h(chunk);
  }
  emitExit(exitCode = 0): void {
    for (const h of this.exitHandlers) h({ exitCode });
  }
}

type Spawn = ReturnType<typeof import("node-pty").spawn>;
type Flows = { start(subject: string): Promise<{ flowId: string; authorizeUrl: string }>; submitCode(flowId: string, code: string): Promise<unknown> };

const URL_LINE = "Visit https://claude.ai/oauth/authorize?code_challenge=abc to continue\n";

describe.each([
  ["ClaudeSetupTokenFlows", (spawn: () => Spawn): Flows => new ClaudeSetupTokenFlows(spawn)],
  ["ClaudeLoginFlows", (spawn: () => Spawn): Flows => new ClaudeLoginFlows(spawn)],
])("%s reuses a subject's live flow", (_name, make) => {
  function harness() {
    const spawned: FakePty[] = [];
    const flows = make(() => {
      const fake = new FakePty();
      spawned.push(fake);
      return fake as unknown as Spawn;
    });
    /** Starts a flow and, if it spawned a process, feeds it the authorize URL. */
    const start = async (subject: string) => {
      const before = spawned.length;
      const p = flows.start(subject);
      if (spawned.length > before) spawned.at(-1)!.emitData(URL_LINE);
      return p;
    };
    return { flows, spawned, start };
  }

  it("hands a repeated start for the same subject the SAME flow, spawning once", async () => {
    const { spawned, start } = harness();
    const first = await start("github:alice");
    const again = await start("github:alice");
    const third = await start("github:alice");
    expect(spawned).toHaveLength(1);
    expect(again.flowId).toBe(first.flowId);
    expect(third).toEqual(first);
  });

  it("shares one process between starts that overlap before the URL appears", async () => {
    const { flows, spawned } = harness();
    const a = flows.start("github:alice");
    const b = flows.start("github:alice");
    expect(spawned).toHaveLength(1);
    spawned[0]!.emitData(URL_LINE);
    expect((await a).flowId).toBe((await b).flowId);
  });

  it("gives a different subject its own flow", async () => {
    const { spawned, start } = harness();
    const alice = await start("github:alice");
    const bob = await start("github:bob");
    expect(spawned).toHaveLength(2);
    expect(bob.flowId).not.toBe(alice.flowId);
  });

  it("starts fresh once the subject's flow has exited", async () => {
    const { spawned, start } = harness();
    const first = await start("github:alice");
    spawned[0]!.emitExit(1);
    const next = await start("github:alice");
    expect(spawned).toHaveLength(2);
    expect(next.flowId).not.toBe(first.flowId);
  });

  it("starts fresh once a code was submitted to the subject's flow", async () => {
    const { flows, spawned, start } = harness();
    const first = await start("github:alice");
    void flows.submitCode(first.flowId, "code#state");
    const next = await start("github:alice");
    expect(spawned).toHaveLength(2);
    expect(next.flowId).not.toBe(first.flowId);
  });

  it("starts fresh after a start that failed (process exited before any URL)", async () => {
    const { flows, spawned } = harness();
    const failed = flows.start("github:alice");
    spawned[0]!.emitExit(1);
    await expect(failed).rejects.toThrow();
    const retry = flows.start("github:alice");
    expect(spawned).toHaveLength(2);
    spawned[1]!.emitData(URL_LINE);
    await expect(retry).resolves.toMatchObject({ authorizeUrl: expect.stringContaining("claude.ai/oauth/authorize") });
  });
});
