// Wiring for the run outcome (docs/adr/0052 §2): drives the real handler in
// index.ts with the model, git and GitHub stubbed, and checks the outcome on
// the reply it returns. The stubbed model runs the real `aktis-verdict` off
// the PATH/env the runner handed it, so this also proves the command reaches
// the turn.
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AgentReply, AgentSession } from "@aktis/agent-runtime";

const state = vi.hoisted(() => ({
  handler: undefined as undefined | ((s: AgentSession) => Promise<AgentReply>),
  home: "",
  work: "",
  /** What the stubbed model runs: `aktis-verdict <args>` when set. */
  modelVerdictArgs: null as string[] | null,
  prompt: "",
  headBefore: "aaa",
  headAfter: "aaa",
  revParseCalls: 0,
  discovered: null as null | { repo: string; branch: string; pr: string | null; prUrl: string | null; prCreatedAt: string | null },
}));

vi.mock("@aktis/agent-runtime", async (orig) => ({
  ...(await orig<typeof import("@aktis/agent-runtime")>()),
  runAgent: (h: (s: AgentSession) => Promise<AgentReply>) => {
    state.handler = h;
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
    }),
  };
});

vi.mock("./claude-runner.js", () => ({
  runClaudeTurn: async (prompt: string, opts: { env: NodeJS.ProcessEnv }) => {
    state.prompt = prompt;
    if (state.modelVerdictArgs) {
      const res = spawnSync("aktis-verdict", state.modelVerdictArgs, { env: opts.env, encoding: "utf8" });
      if (res.error) throw res.error;
    }
    return { finalMessage: "done", failed: false, failureDetail: null, authError: false, sessionId: null };
  },
  runClaudeTurnRemoteControlled: async () => {
    throw new Error("not used");
  },
}));

vi.mock("./identityDelegation.js", async (orig) => ({
  ...(await orig<typeof import("./identityDelegation.js")>()),
  identityModeOf: () => "agent",
  resolveUndelegatedToken: async () => "gh-token",
}));

vi.mock("./credentialsWriteback.js", async (orig) => ({
  ...(await orig<typeof import("./credentialsWriteback.js")>()),
  createCredentialsWritebackWatcher: () => ({ start() {}, stop: async () => "disabled" }),
}));

vi.mock("./git.js", async (orig) => ({
  ...(await orig<typeof import("./git.js")>()),
  resolveGitIdentity: async () => ({ name: "t", email: "t@example.com" }),
  setupGitAuth: async () => {},
  findRepoDir: async () => join(state.work, "widgets"),
  ensureChangeDeliverable: async () => ({ committed: false, pushed: false, branch: "b" }),
  ensurePullRequest: async () => null,
  countCommitsSince: async () => 0,
  discoverResult: async () => state.discovered,
  appendCoAuthorTrailer: async () => {},
  // Only `git rev-parse HEAD` (before, then after the turn) reaches here.
  runCommand: async () => ({ code: 0, stdout: state.revParseCalls++ === 0 ? state.headBefore : state.headAfter, stderr: "" }),
}));

function session(readOnly: boolean): AgentSession {
  return {
    runId: "run-1",
    goal: readOnly ? "review PR #7" : "fix the bug",
    readOnly,
    params: {},
    signal: new AbortController().signal,
    progress: async () => {},
    warn: async () => {},
  } as unknown as AgentSession;
}

describe("index outcome wiring", () => {
  beforeEach(async () => {
    state.home = mkdtempSync(join(tmpdir(), "idx-home-"));
    state.work = mkdtempSync(join(tmpdir(), "idx-work-"));
    state.modelVerdictArgs = null;
    state.revParseCalls = 0;
    state.headBefore = state.headAfter = "aaa";
    state.discovered = {
      repo: "acme/widgets",
      branch: "fix-branch",
      pr: "7",
      prUrl: "https://github.com/acme/widgets/pull/7",
      prCreatedAt: null,
    };
    vi.resetModules();
    state.handler = undefined;
    await import("./index.js");
  });
  afterEach(() => {
    rmSync(state.home, { recursive: true, force: true });
    rmSync(state.work, { recursive: true, force: true });
  });

  it("a review reports its PR and the verdict the model recorded", async () => {
    state.modelVerdictArgs = ["changes-requested"];
    const reply = await state.handler!(session(true));
    expect(state.prompt).toContain("aktis-verdict changes-requested");
    expect(reply.outcome).toEqual({
      pullRequest: { number: 7, url: "https://github.com/acme/widgets/pull/7" },
      madeNewCommits: false,
      verdict: "changes-requested",
    });
    expect(reply.result).toBeTypeOf("string"); // the continuation token is unchanged
  });

  it("a review that recorded nothing reports none, even with a stale verdict on disk", async () => {
    // A previous turn's verdict, left where this turn will look.
    mkdirSync(join(state.home, ".aktis"), { recursive: true });
    writeFileSync(join(state.home, ".aktis", "verdict"), "ready\n");
    const reply = await state.handler!(session(true));
    expect(reply.outcome?.verdict).toBe("none");
  });

  it("a review whose command call was invalid reports none", async () => {
    state.modelVerdictArgs = ["approve"];
    const reply = await state.handler!(session(true));
    expect(reply.outcome?.verdict).toBe("none");
  });

  it("a write run reports its PR and new commits, and never a verdict", async () => {
    state.headAfter = "bbb";
    state.modelVerdictArgs = ["ready"];
    const reply = await state.handler!(session(false));
    expect(state.prompt).not.toContain("aktis-verdict");
    expect(reply.outcome).toEqual({
      pullRequest: { number: 7, url: "https://github.com/acme/widgets/pull/7" },
      madeNewCommits: true,
    });
  });

  it("a review that left no clone still carries its verdict", async () => {
    state.discovered = null;
    state.modelVerdictArgs = ["ready"];
    const reply = await state.handler!(session(true));
    expect(reply.outcome).toEqual({ madeNewCommits: false, verdict: "ready" });
  });
});
