import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  buildClaudeSettings,
  buildPrompt,
  DENY_BASH_PATTERNS,
  isReadOnly,
  READ_ONLY_DENY_BASH_PATTERNS,
} from "./claude.js";
import { VERDICT_PROMPT_SECTION } from "./outcome.js";

describe("buildClaudeSettings", () => {
  it("bypasses permissions and bakes in the bash deny rules", () => {
    const settings = buildClaudeSettings() as {
      permissions: { defaultMode: string; deny: string[] };
    };
    expect(settings.permissions.defaultMode).toBe("bypassPermissions");
    expect(settings.permissions.deny).toEqual(DENY_BASH_PATTERNS);
  });

  // FIX 1: a read-only run is CODE-ENFORCED -- the deny list, not the
  // prompt, is what stops a push/PR. Without the readOnly branch these
  // assertions fail (buildClaudeSettings used to ignore its argument).
  describe("read-only enforcement", () => {
    const denyOf = (readOnly: boolean): string[] =>
      (buildClaudeSettings(readOnly) as { permissions: { deny: string[] } }).permissions.deny;

    it("denies git push, gh pr create, and gh pr merge on a read-only run", () => {
      const deny = denyOf(true);
      expect(deny).toContain("Bash(git push:*)");
      expect(deny).toContain("Bash(gh pr create:*)");
      expect(deny).toContain("Bash(gh pr merge:*)");
      // and keeps every baseline guardrail
      for (const pattern of DENY_BASH_PATTERNS) expect(deny).toContain(pattern);
      for (const pattern of READ_ONLY_DENY_BASH_PATTERNS) expect(deny).toContain(pattern);
    });

    it("does NOT add the read-only denies on a change run (push/PR-create stay allowed)", () => {
      const deny = denyOf(false);
      expect(deny).not.toContain("Bash(git push:*)");
      expect(deny).not.toContain("Bash(gh pr create:*)");
      expect(deny).not.toContain("Bash(gh pr merge:*)");
      expect(deny).toEqual(DENY_BASH_PATTERNS);
    });
  });
});

// ADR 0048: read-only comes from the structured session.readOnly flag
// (AgentRun.spec.readOnly) and nothing else. The goal is attacker-influenced
// event text, so it must have no say in the mode either way.
describe("isReadOnly", () => {
  const LEGACY_SENTINEL = "SWE-ENFORCED-MODE: review-only";
  const sentinelGoal = `${LEGACY_SENTINEL}\nPull request acme/widgets#7 was labeled "ai-review"\n\nReview this pull request.`;
  const denyFor = (session: { readOnly: boolean; goal: string }): string[] =>
    (buildClaudeSettings(isReadOnly(session)) as { permissions: { deny: string[] } }).permissions.deny;

  it("is enabled by session.readOnly with an ordinary goal", () => {
    const session = { readOnly: true, goal: "Review this pull request." };
    expect(isReadOnly(session)).toBe(true);
    expect(denyFor(session)).toEqual([...DENY_BASH_PATTERNS, ...READ_ONLY_DENY_BASH_PATTERNS]);
  });

  it("is NOT enabled by the old goal sentinel when session.readOnly is false", () => {
    const session = { readOnly: false, goal: sentinelGoal };
    expect(isReadOnly(session)).toBe(false);
    expect(denyFor(session)).not.toContain("Bash(git push:*)");
    expect(denyFor(session)).toEqual(DENY_BASH_PATTERNS);
  });

  it("treats a non-boolean truthy readOnly as a normal run (strict === true)", () => {
    expect(isReadOnly({ readOnly: "true" as unknown as boolean })).toBe(false);
  });

  // Wiring guard: the handler in index.ts can't be imported in a test (it
  // boots runAgent at module load), so assert on its source that every guard
  // is fed from session.readOnly and that the sentinel is gone. Fails if
  // someone reintroduces goal-text detection or drops the session wiring.
  it("index.ts derives read-only from the session, never the goal", () => {
    const src = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "index.ts"), "utf8");
    expect(src).toContain("const readOnly = isReadOnly(session);");
    expect(src).toContain("buildClaudeSettings(readOnly)");
    expect(src).toContain("resolveUndelegatedToken(toolConfig, readOnly)");
    expect(src).toMatch(/resolveDelegatedToken\(toolConfig, [^,]+, readOnly,/);
    expect(src).not.toContain(LEGACY_SENTINEL);
    expect(src).not.toMatch(/isReviewMode|REVIEW_MODE_MARKER/);
  });
});

describe("buildPrompt", () => {
  it("includes continuation context when a marker is present", () => {
    const prompt = buildPrompt("add a health check", {
      repo: "acme/widgets",
      branch: "feature/health-check",
      pr: "12",
      session: "ses_abc123",
    });
    expect(prompt).toContain("CONTINUING work on an existing pull request");
    expect(prompt).toContain("acme/widgets");
    expect(prompt).toContain("feature/health-check");
    expect(prompt).toContain("#12");
  });

  it("omits continuation context with no marker", () => {
    const prompt = buildPrompt("add a health check", null);
    expect(prompt).not.toContain("CONTINUING work");
    expect(prompt).toContain("gh repo create");
  });

  it("puts an invoked skill at the very start, where the CLI expands it", () => {
    const prompt = buildPrompt("the spec in #42", null, "implement");
    expect(prompt.startsWith("/implement You are an autonomous")).toBe(true);
    expect(prompt).toContain("## Task\nthe spec in #42");
    expect(buildPrompt("the spec in #42", null)).toBe(prompt.slice("/implement ".length));
  });

  it("tells the agent to drop --profile when no AWS profiles are declared", () => {
    const prompt = buildPrompt("check bcp-dev", null);
    expect(prompt).toContain("There are no named profiles, so drop any `--profile`.");
  });

  it("lists declared AWS profiles as usable with --profile", () => {
    const prompt = buildPrompt("check bcp-dev", null, null, ["platform", "bitovi-platform"]);
    expect(prompt).toContain("`platform`, `bitovi-platform`");
    expect(prompt).toContain("A `--profile` with one of those names works as written");
    expect(prompt).not.toContain("There are no named profiles");
  });

  it("tells the agent to build an EKS kubeconfig when it has no in-cluster access", () => {
    const prompt = buildPrompt("check the cluster", null);
    expect(prompt).toContain("Build a kubeconfig with `aws eks update-kubeconfig");
    expect(prompt).not.toContain("already connected");
  });

  it("tells the agent kubectl already reaches its own cluster when in-cluster access is granted", () => {
    const prompt = buildPrompt("check the cluster", null, null, [], true);
    expect(prompt).toContain("kubectl is already connected, with no kubeconfig, to the Kubernetes cluster this pod runs in");
    expect(prompt).toContain("Secrets are never readable");
    expect(prompt).toContain("--kubeconfig $HOME/.kube/eks");
    expect(prompt).not.toContain("--alias <context>");
  });

  it("embeds the caller instruction as data under the Task heading", () => {
    const prompt = buildPrompt("add a health check", null);
    expect(prompt).toContain("## Task");
    expect(prompt).toContain("add a health check");
  });

  // Guards the fix for issue #185 ("Claude Agent is Too Eager"): the fixed
  // policy must tell the headless agent to stay in scope and to STOP and
  // surface a blocker rather than improvise a workaround when it is blocked
  // or unsure. These are trusted policy, so they must be present regardless
  // of the caller instruction or whether this is a continuation turn.
  describe("scope-discipline guardrails (issue #185)", () => {
    for (const marker of [
      null,
      { repo: "acme/widgets", branch: "feature/x", pr: "9", session: "ses_1" },
    ] as const) {
      const label = marker ? "on a continuation turn" : "on a fresh turn";

      it(`tells the agent to stay within the task scope ${label}`, () => {
        const prompt = buildPrompt("do the thing", marker);
        expect(prompt).toContain("Stay within the scope of the task as given");
      });

      it(`tells the agent to STOP rather than improvise when blocked or unsure ${label}`, () => {
        const prompt = buildPrompt("do the thing", marker);
        expect(prompt).toContain("When you are blocked or unsure, STOP rather than improvising");
      });

      it(`forbids substituting or creating a repository to work around a block ${label}`, () => {
        const prompt = buildPrompt("do the thing", marker);
        expect(prompt).toContain("Do NOT substitute a different repository");
        expect(prompt).toContain("create a new repository the task didn't call for");
      });

      it(`tells the agent to surface the blocker for a human ${label}`, () => {
        const prompt = buildPrompt("do the thing", marker);
        expect(prompt).toContain("surface the blocker");
        expect(prompt).toContain("so a human can decide and re-trigger you");
      });
    }
  });

  // docs/adr/0052 §2: a review records its decision with aktis-verdict.
  it("ends a read-only prompt with the verdict instruction", () => {
    const prompt = buildPrompt("review PR #7", null, null, [], false, true);
    expect(prompt.endsWith(`\n\n${VERDICT_PROMPT_SECTION}`)).toBe(true);
    expect(prompt).toContain("aktis-verdict changes-requested");
    expect(prompt).toContain("aktis-verdict ready");
    // Still the skill's arguments when a skill was invoked.
    expect(buildPrompt("review PR #7", null, "review", [], false, true)).toBe(`/review ${prompt}`);
  });

  // docs/adr/0055: a coordination run records a decision, not a review verdict.
  it("omits the verdict instruction from a read-only coordination prompt", () => {
    const prompt = buildPrompt("Decide, then run `aktis-coordinate proceed`.", null, null, [], false, true);
    expect(prompt).not.toContain(VERDICT_PROMPT_SECTION);
    expect(prompt).not.toContain("aktis-verdict");
    expect(prompt).toContain("aktis-coordinate proceed");
  });

  it("leaves a write run's prompt unchanged", () => {
    const prompt = buildPrompt("fix the bug", null, null, [], false, false);
    expect(prompt).not.toContain("aktis-verdict");
    expect(prompt).toBe(buildPrompt("fix the bug", null));
  });
});
