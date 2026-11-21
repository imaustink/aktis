import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { countCommitsSince, formatPrFooter, type RepoResult, runCommand } from "./git.js";

// "Did this turn commit pushable work?" when the repo was cloned DURING the
// turn (no prior HEAD). Reproduces imaustink/aktis#343: an ai-review run
// cloned the repo, ran `gh pr checkout`, committed nothing, and still posted
// "✅ Opened pull request", because the PR branch sits ahead of origin/HEAD.
describe("countCommitsSince", () => {
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    GIT_AUTHOR_NAME: "t",
    GIT_AUTHOR_EMAIL: "t@example.com",
    GIT_COMMITTER_NAME: "t",
    GIT_COMMITTER_EMAIL: "t@example.com",
  };
  // Commits that existed before the turn: dated well in the past.
  const pastEnv: NodeJS.ProcessEnv = {
    ...env,
    GIT_AUTHOR_DATE: "2026-01-01T00:00:00Z",
    GIT_COMMITTER_DATE: "2026-01-01T00:00:00Z",
  };
  let root: string;
  let workDir: string;
  let turnStartedAt: number;

  beforeAll(async () => {
    root = mkdtempSync(join(tmpdir(), "git-since-test-"));
    const originDir = join(root, "origin");
    workDir = join(root, "work");
    await runCommand("git", ["init", "-b", "main", originDir], { env });
    await runCommand("git", ["-C", originDir, "commit", "--allow-empty", "-m", "base"], { env: pastEnv });
    // An existing PR branch, three commits ahead of main.
    await runCommand("git", ["-C", originDir, "checkout", "-b", "pr-branch"], { env });
    for (const m of ["pr 1", "pr 2", "pr 3"]) {
      await runCommand("git", ["-C", originDir, "commit", "--allow-empty", "-m", m], { env: pastEnv });
    }
    await runCommand("git", ["-C", originDir, "checkout", "main"], { env });
    turnStartedAt = Date.now();
    await runCommand("git", ["clone", originDir, workDir], { env });
  });

  afterAll(() => {
    rmSync(root, { recursive: true, force: true });
  });

  it("is 0 for a fresh clone the turn only read", async () => {
    expect(await countCommitsSince(workDir, turnStartedAt, env)).toBe(0);
  });

  it("is 0 after checking out an existing PR branch that is ahead of the default branch", async () => {
    await runCommand("git", ["-C", workDir, "checkout", "pr-branch"], { env });
    const ahead = await runCommand("git", ["-C", workDir, "rev-list", "--count", "origin/HEAD..HEAD"], { env });
    expect(ahead.stdout.trim()).toBe("3"); // the old signal: "3 new commits"
    expect(await countCommitsSince(workDir, turnStartedAt, env)).toBe(0);
  });

  it("counts commits made during the turn", async () => {
    await runCommand("git", ["-C", workDir, "commit", "--allow-empty", "-m", "fix 1"], { env });
    await runCommand("git", ["-C", workDir, "commit", "--allow-empty", "-m", "fix 2"], { env });
    expect(await countCommitsSince(workDir, turnStartedAt, env)).toBe(2);
  });

  it("returns 0 (rather than throwing) when the path is not a git repo", async () => {
    expect(await countCommitsSince(root, turnStartedAt, env)).toBe(0);
  });
});

describe("formatPrFooter", () => {
  const turnStartedAt = Date.parse("2026-11-21T14:54:41Z");
  const withPr = (prCreatedAt: string | null): RepoResult => ({
    repo: "acme/widgets",
    branch: "feature",
    pr: "343",
    prUrl: "https://github.com/acme/widgets/pull/343",
    prCreatedAt,
  });
  const base = { madeNewCommits: true, turnStartedAt, markerHadPr: false };

  it("is empty for a read-only turn, even with commits and an open PR", () => {
    expect(formatPrFooter({ ...base, readOnly: true, discovered: withPr("2026-11-20T10:00:00Z") })).toBe("");
  });

  it("is empty when the turn made no new commits", () => {
    expect(
      formatPrFooter({ ...base, readOnly: false, madeNewCommits: false, discovered: withPr("2026-11-20T10:00:00Z") }),
    ).toBe("");
  });

  it("says Updated for a PR that existed before the turn, even with no continuation marker", () => {
    const footer = formatPrFooter({ ...base, readOnly: false, discovered: withPr("2026-11-20T10:00:00Z") });
    expect(footer).toContain("✅ Updated pull request: [acme/widgets#343]");
  });

  it("says Opened for a PR GitHub created during the turn", () => {
    const footer = formatPrFooter({ ...base, readOnly: false, discovered: withPr("2026-11-21T14:57:00Z") });
    expect(footer).toContain("✅ Opened pull request: [acme/widgets#343]");
  });

  it("falls back to the continuation marker when createdAt is unknown", () => {
    expect(formatPrFooter({ ...base, readOnly: false, discovered: withPr(null) })).toContain("Opened");
    expect(formatPrFooter({ ...base, readOnly: false, markerHadPr: true, discovered: withPr(null) })).toContain(
      "Updated",
    );
  });

  it("warns when commits were pushed but no PR is open", () => {
    const footer = formatPrFooter({
      ...base,
      readOnly: false,
      discovered: { repo: "acme/widgets", branch: "feature", pr: null, prUrl: null, prCreatedAt: null },
    });
    expect(footer).toContain("⚠️ Work is on `acme/widgets` branch `feature`, but no open pull request was found.");
  });
});
