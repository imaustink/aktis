// Wiring for the run outcome (docs/adr/0052 §2): drives the real main() in
// index.ts with NATS, the opencode server, git and GitHub stubbed, and checks
// the outcome on the final `reply` it publishes. The stubbed model runs the
// real `aktis-verdict` off the environment the runner started `opencode
// serve` with, so this also proves the command reaches the turn.
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AgentDownMessage, AgentUpMessage } from "@aktis/messaging";

const state = vi.hoisted(() => ({
  home: "",
  work: "",
  readOnly: false,
  published: [] as AgentUpMessage[],
  onDown: undefined as undefined | ((m: AgentDownMessage) => void),
  serverEnv: {} as NodeJS.ProcessEnv,
  /** What the stubbed model runs: `aktis-verdict <args>` when set. */
  modelVerdictArgs: null as string[] | null,
  /** What the stubbed model runs: `aktis-coordinate <args>` when set (docs/adr/0055). */
  modelCoordinateArgs: null as string[] | null,
  /** Overrides the run goal when set. */
  goal: null as string | null,
  prompt: "",
  headBefore: "aaa",
  headAfter: "aaa",
  revParseCalls: 0,
  /** Whether the stubbed git reports HEAD on a remote-tracking branch (i.e. pushed). */
  headOnRemote: true,
  discovered: null as null | { repo: string; branch: string; pr: string | null; prUrl: string | null; prCreatedAt: string | null },
}));

vi.mock("@aktis/agent-runtime", async (orig) => ({
  ...(await orig<typeof import("@aktis/agent-runtime")>()),
  loadConfig: () => ({
    runId: "run-1",
    goal: state.goal ?? (state.readOnly ? "review PR #7" : "fix the bug"),
    readOnly: state.readOnly,
    params: {},
  }),
  NatsChannel: {
    connect: async () => ({
      publishUp: async (m: AgentUpMessage) => {
        state.published.push(m);
      },
      onDown: (cb: (m: AgentDownMessage) => void) => {
        state.onDown = cb;
      },
      close: async () => {},
    }),
  },
}));

vi.mock("./config.js", async (orig) => {
  const real = await orig<typeof import("./config.js")>();
  return {
    ...real,
    loadToolConfig: () => ({
      ...real.loadToolConfig({}),
      homeDir: state.home,
      workdir: state.work,
      anthropicApiKey: "test-key",
      githubToken: "",
      identityMode: "agent",
      liveIdleTimeoutMs: 60_000,
    }),
  };
});

vi.mock("./opencode-server.js", () => ({
  startOpencodeServer: (opts: { env: NodeJS.ProcessEnv }) => {
    state.serverEnv = opts.env;
    return { baseUrl: "http://127.0.0.1:0", auth: { username: "u", password: "p" }, waitForHealth: async () => {}, kill() {} };
  },
  createSession: async () => ({ id: "s1" }),
  subscribeEvents: () => new Promise(() => {}),
  narrateOpencodeEvent: () => undefined,
  forwardRequest: async () => ({ status: 200, body: {} }),
  sendMessage: async (_base: string, _id: string, prompt: string) => {
    state.prompt = prompt;
    if (state.modelVerdictArgs) {
      const res = spawnSync("aktis-verdict", state.modelVerdictArgs, { env: state.serverEnv, encoding: "utf8" });
      if (res.error) throw res.error;
    }
    if (state.modelCoordinateArgs) {
      const res = spawnSync("aktis-coordinate", state.modelCoordinateArgs, { env: state.serverEnv, encoding: "utf8" });
      if (res.error) throw res.error;
    }
    return { failed: false, finalMessage: "done", failureDetail: null };
  },
}));

vi.mock("./identityDelegation.js", async (orig) => ({
  ...(await orig<typeof import("./identityDelegation.js")>()),
  identityModeOf: () => "agent",
  resolveUndelegatedToken: async () => "gh-token",
}));

vi.mock("./git.js", async (orig) => ({
  ...(await orig<typeof import("./git.js")>()),
  resolveGitIdentity: async () => ({ name: "t", email: "t@example.com" }),
  setupGitAuth: async () => {},
  findRepoDir: async () => join(state.work, "widgets"),
  countCommitsSince: async () => 0,
  discoverResult: async () => state.discovered,
  appendCoAuthorTrailer: async () => {},
  isHeadOnRemote: async () => state.headOnRemote,
  // Only `git rev-parse HEAD` (before, then after the turn) reaches here.
  runCommand: async () => ({ code: 0, stdout: state.revParseCalls++ === 0 ? state.headBefore : state.headAfter, stderr: "" }),
}));

async function runMain(): Promise<Extract<AgentUpMessage, { type: "reply" }>> {
  vi.resetModules();
  await import("./index.js");
  await vi.waitFor(() => {
    if (!state.published.some((m) => m.type === "reply" || m.type === "failed")) throw new Error("no final message yet");
  });
  const failed = state.published.find((m) => m.type === "failed");
  if (failed) throw new Error(`turn failed: ${JSON.stringify(failed)}`);
  return state.published.find((m) => m.type === "reply") as Extract<AgentUpMessage, { type: "reply" }>;
}

describe("index outcome wiring", () => {
  let exitSpy: ReturnType<typeof vi.spyOn>;
  beforeEach(() => {
    state.home = mkdtempSync(join(tmpdir(), "idx-home-"));
    state.work = mkdtempSync(join(tmpdir(), "idx-work-"));
    state.published = [];
    state.modelVerdictArgs = null;
    state.modelCoordinateArgs = null;
    state.goal = null;
    state.revParseCalls = 0;
    state.headOnRemote = true;
    state.headBefore = state.headAfter = "aaa";
    state.discovered = {
      repo: "acme/widgets",
      branch: "fix-branch",
      pr: "7",
      prUrl: "https://github.com/acme/widgets/pull/7",
      prCreatedAt: null,
    };
    // main() exits the process when the session ends; keep the worker alive.
    exitSpy = vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);
  });
  afterEach(async () => {
    // End the resident session so its idle timer doesn't outlive the test.
    state.onDown?.({ type: "cancel", agent_run_id: "run-1", seq: 0, ts: "" } as AgentDownMessage);
    await vi.waitFor(() => expect(exitSpy).toHaveBeenCalled());
    vi.restoreAllMocks();
    rmSync(state.home, { recursive: true, force: true });
    rmSync(state.work, { recursive: true, force: true });
  });

  it("a review reports its PR and the verdict the model recorded", async () => {
    state.readOnly = true;
    state.modelVerdictArgs = ["ready"];
    const reply = await runMain();
    expect(state.prompt).toContain("aktis-verdict changes-requested");
    expect(reply.outcome).toEqual({
      pullRequest: { number: 7, url: "https://github.com/acme/widgets/pull/7" },
      madeNewCommits: false,
      verdict: "ready",
    });
    expect(reply.result).toBeTypeOf("string"); // the continuation token is unchanged
  });

  it("a review that recorded nothing reports none, even with a stale verdict on disk", async () => {
    state.readOnly = true;
    mkdirSync(join(state.home, ".aktis"), { recursive: true });
    writeFileSync(join(state.home, ".aktis", "verdict"), "changes-requested\n");
    const reply = await runMain();
    expect(reply.outcome?.verdict).toBe("none");
  });

  it("a write run reports its PR and new commits, and never a verdict", async () => {
    state.readOnly = false;
    state.headAfter = "bbb";
    state.modelVerdictArgs = ["ready"];
    const reply = await runMain();
    expect(state.prompt).not.toContain("aktis-verdict");
    expect(reply.outcome).toEqual({
      pullRequest: { number: 7, url: "https://github.com/acme/widgets/pull/7" },
      madeNewCommits: true,
    });
  });

  it("a write run whose commits never reached a remote reports no new commits", async () => {
    state.readOnly = false;
    state.headAfter = "bbb";
    state.headOnRemote = false;
    const reply = await runMain();
    expect(reply.outcome).toEqual({
      pullRequest: { number: 7, url: "https://github.com/acme/widgets/pull/7" },
      madeNewCommits: false,
    });
  });

  // docs/adr/0055: a read-only coordination run reports its decision, not a verdict.
  it("a coordination run that never cloned succeeds and reports the decision, and no verdict", async () => {
    state.readOnly = true;
    state.goal = 'Check issue #5 against in-flight work, then run `aktis-coordinate proceed`, `aktis-coordinate wait <N>` or `aktis-coordinate stack <N>`.';
    state.discovered = null;
    state.modelCoordinateArgs = ["stack", "#9"];
    state.modelVerdictArgs = ["ready"];
    const reply = await runMain();
    expect(state.prompt).not.toContain("aktis-verdict");
    expect(reply.outcome).toEqual({ madeNewCommits: false, coordination: { decision: "stack", number: 9 } });
  });

  it("a coordination run that recorded nothing reports none, even with a stale decision on disk", async () => {
    state.readOnly = true;
    state.goal = 'Check issue #5 against in-flight work, then run `aktis-coordinate proceed`, `aktis-coordinate wait <N>` or `aktis-coordinate stack <N>`.';
    mkdirSync(join(state.home, ".aktis"), { recursive: true });
    writeFileSync(join(state.home, ".aktis", "coordinate"), "proceed\n");
    const reply = await runMain();
    expect(reply.outcome?.coordination).toEqual({ decision: "none" });
  });

  it("a review that left no clone still carries its verdict", async () => {
    state.readOnly = true;
    state.discovered = null;
    state.modelVerdictArgs = ["ready"];
    const reply = await runMain();
    expect(reply.outcome).toEqual({ madeNewCommits: false, verdict: "ready" });
  });
});
