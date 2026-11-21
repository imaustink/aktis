/**
 * Makes the pull request a run worked on close the issue that triggered it.
 *
 * An issue-triage EventTrigger passes the issue as the structured
 * `closesIssue` param (AgentRun.spec.params, alongside `repository`). After
 * the turn, the runner asks GitHub whether the PR already closes that issue
 * (`closingIssuesReferences`, GitHub's own parse of the description, so a
 * bare "#12" mention or a keyword GitHub doesn't honor doesn't count) and,
 * if not, appends a `Closes #N` line to the description. This used to be
 * left to the model, which wrote a closing keyword on some PRs and only a
 * mention on others.
 */
import { runCommand } from "./git.js";

export interface ClosesIssue {
  /** owner/repo the issue lives in. */
  repo: string;
  number: string;
}

/**
 * Reads `closesIssue` (+ `repository`) from the run's params. Null when
 * unset or malformed: the number must be a positive integer and the repo an
 * owner/repo, since both end up in a PR description and a `gh` argument.
 */
export function parseClosesIssue(params: Readonly<Record<string, string>>): ClosesIssue | null {
  const number = (params.closesIssue ?? "").trim();
  const repo = (params.repository ?? "").trim();
  if (!/^[1-9][0-9]*$/.test(number)) return null;
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repo)) return null;
  return { repo, number };
}

/** The keyword line to add: repo-relative when the PR is in the issue's repo. */
export function closingLine(issue: ClosesIssue, prRepo: string): string {
  const sameRepo = issue.repo.toLowerCase() === prRepo.toLowerCase();
  return `Closes ${sameRepo ? "" : issue.repo}#${issue.number}`;
}

interface ClosingRefView {
  body?: string;
  closingIssuesReferences?: Array<{ number?: number; url?: string }>;
}

/** True when GitHub already lists `issue` among the PR's closing references. */
export function alreadyCloses(view: ClosingRefView, issue: ClosesIssue): boolean {
  const want = `/${issue.repo.toLowerCase()}/issues/${issue.number}`;
  return (view.closingIssuesReferences ?? []).some(
    (ref) => String(ref.number) === issue.number && (ref.url ?? "").toLowerCase().endsWith(want),
  );
}

export type EnsureClosingResult = "present" | "added" | { error: string };

export async function ensureClosingReference(opts: {
  prRepo: string;
  pr: string;
  issue: ClosesIssue;
  env: NodeJS.ProcessEnv;
  signal?: AbortSignal;
}): Promise<EnsureClosingResult> {
  const { prRepo, pr, issue, env, signal } = opts;
  const view = await runCommand(
    "gh",
    ["pr", "view", pr, "--repo", prRepo, "--json", "body,closingIssuesReferences"],
    { env, signal },
  );
  if (view.code !== 0) return { error: `gh pr view failed: ${view.stderr.trim().slice(0, 300)}` };
  let parsed: ClosingRefView;
  try {
    parsed = JSON.parse(view.stdout) as ClosingRefView;
  } catch {
    return { error: "gh pr view returned invalid JSON" };
  }
  if (alreadyCloses(parsed, issue)) return "present";

  const body = (parsed.body ?? "").trimEnd();
  const line = closingLine(issue, prRepo);
  // closingIssuesReferences stays empty when the PR's base isn't the default
  // branch (GitHub ignores closing keywords there); don't re-append the same
  // line on every later turn.
  if (body.split(/\r?\n/).some((l) => l.trim().toLowerCase() === line.toLowerCase())) return "present";
  const next = `${body}${body ? "\n\n" : ""}${line}`;
  const edit = await runCommand("gh", ["pr", "edit", pr, "--repo", prRepo, "--body", next], { env, signal });
  if (edit.code !== 0) return { error: `gh pr edit failed: ${edit.stderr.trim().slice(0, 300)}` };
  return "added";
}
