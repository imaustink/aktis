/**
 * The run's structured outcome (docs/adr/0052 §2): the facts a chained
 * EventTrigger routes on -- which pull request the turn ended with, whether it
 * pushed new work, and, on a read-only review, the model's verdict.
 *
 * Built by the runner from what it observed, never parsed from the model's
 * prose. The one judgment call in it, the review verdict, is still the
 * model's, but it is RECORDED deterministically: the model runs
 * `aktis-verdict <value>` (a command the runner puts on PATH for the turn),
 * which writes to a runner-owned file, and the runner validates that file
 * against a fixed enum afterwards. Anything else -- no call, a garbled value,
 * a value left over from an earlier turn -- becomes "none", which triggers
 * route to a human rather than back into a fix loop.
 *
 * The factory coordinator (docs/adr/0055) records its decision the same way:
 * a read-only run whose structured `AgentRun.spec.outcome` is `coordination`
 * (AGENT_OUTCOME, from the trigger's `target.outcome`) is told to record it
 * with `aktis-coordinate` instead of a verdict, and reports `coordination` --
 * `proceed`, `wait <N>` or `stack <N>`, validated against a strict pattern, or
 * `none` when nothing valid was recorded (triggers treat that like proceed).
 * Which outcome a run reports comes from that field alone, never from the
 * prompt, so no text in an issue or pull request can switch it.
 *
 * Kept identical between claude-code-swe-agent and opencode-swe-agent.
 */
import { chmod, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

/** The verdicts the model may record. Single source for the CLI and the parser. */
export const VERDICTS = ["changes-requested", "ready"] as const;
export type Verdict = (typeof VERDICTS)[number];
/** What the outcome carries: a recorded verdict, or "none" when there wasn't a valid one. */
export type OutcomeVerdict = Verdict | "none";

/** Env var naming the runner-owned file `aktis-verdict` writes to. */
export const VERDICT_FILE_ENV = "AKTIS_VERDICT_FILE";
export const VERDICT_COMMAND = "aktis-verdict";

/**
 * Appended to a read-only run's prompt. Fixed text, so the command names in it
 * always match {@link VERDICTS}.
 */
export const VERDICT_PROMPT_SECTION = [
  `## Review verdict`,
  `After posting your review, record your decision exactly once by running one of:`,
  `- \`${VERDICT_COMMAND} changes-requested\` -- the pull request needs changes before it can merge.`,
  `- \`${VERDICT_COMMAND} ready\` -- the pull request is ready for a human's final review.`,
].join("\n");

/** The coordination decisions the model may record (docs/adr/0055). */
export const COORDINATE_DECISIONS = ["proceed", "wait", "stack"] as const;
export type CoordinateDecision = (typeof COORDINATE_DECISIONS)[number];
/** Env var naming the runner-owned file `aktis-coordinate` writes to. */
export const COORDINATE_FILE_ENV = "AKTIS_COORDINATE_FILE";
export const COORDINATE_COMMAND = "aktis-coordinate";

/** What the outcome carries for a coordination run. `number` only for wait/stack. */
export type OutcomeCoordination = { decision: CoordinateDecision | "none"; number?: number };

/**
 * Appended to a coordination run's prompt in place of
 * {@link VERDICT_PROMPT_SECTION}. Fixed text, so it always matches the
 * command's grammar.
 */
export const COORDINATE_PROMPT_SECTION = [
  `## Coordination decision`,
  `When you have decided, record it exactly once by running one of:`,
  `- \`${COORDINATE_COMMAND} proceed\` -- start the work now.`,
  `- \`${COORDINATE_COMMAND} wait <number>\` -- hold it until that open pull request or issue is merged or closed.`,
  `- \`${COORDINATE_COMMAND} stack <number>\` -- build it on top of that open pull request's branch.`,
].join("\n");

/** Which structured outcome a run reports (docs/adr/0055). */
export type ReportedOutcome = "verdict" | "coordination";

/**
 * The outcome this run reports, from its structured fields alone
 * (AgentRun.spec.readOnly / spec.outcome): none for a write run; for a
 * read-only run, `coordination` when spec.outcome says so, else a verdict.
 */
export function reportedOutcome(run: { readOnly: boolean; outcome?: string }): ReportedOutcome | null {
  if (!run.readOnly) return null;
  return run.outcome === "coordination" ? "coordination" : "verdict";
}

/** The prompt section that tells the model how to record `report`; none for a write run. */
export function outcomePromptSection(report: ReportedOutcome | null): string | null {
  if (report === "verdict") return VERDICT_PROMPT_SECTION;
  if (report === "coordination") return COORDINATE_PROMPT_SECTION;
  return null;
}

/** Type alias (not an interface) so it is assignable to AgentReply.outcome's Record. */
export type SweOutcome = {
  pullRequest?: { number: number; url: string };
  madeNewCommits: boolean;
  verdict?: OutcomeVerdict;
  coordination?: OutcomeCoordination;
};

/**
 * Source of the `aktis-verdict` command. POSIX sh rather than node so it has
 * no module-format or Node-version dependency. The accepted values are
 * rendered from {@link VERDICTS}, so the command and {@link readVerdict} can't
 * drift. Anything but exactly one valid argument exits non-zero and writes
 * nothing, so the model sees the mistake and can retry.
 */
export function renderVerdictCommand(): string {
  return (
    `#!/bin/sh\n` +
    `# Generated by the SWE agent runner (src/outcome.ts). Records a review\n` +
    `# verdict in the runner-owned file named by $${VERDICT_FILE_ENV}.\n` +
    `usage() { echo "usage: ${VERDICT_COMMAND} ${VERDICTS.join("|")}" >&2; exit 2; }\n` +
    `[ "$#" -eq 1 ] || usage\n` +
    `case "$1" in\n` +
    `  ${VERDICTS.join("|")}) ;;\n` +
    `  *) usage ;;\n` +
    `esac\n` +
    `if [ -z "$${VERDICT_FILE_ENV}" ]; then echo "${VERDICT_COMMAND}: ${VERDICT_FILE_ENV} is not set" >&2; exit 1; fi\n` +
    `printf '%s\\n' "$1" > "$${VERDICT_FILE_ENV}" || exit 1\n` +
    `echo "${VERDICT_COMMAND}: recorded $1"\n`
  );
}

/**
 * Writes `aktis-verdict` into `<homeDir>/bin` (the caller prepends that to the
 * child's PATH and sets {@link VERDICT_FILE_ENV} to `verdictFile`). The file
 * lives under the home directory, not the repo, so recording a verdict works
 * on a read-only run and never shows up as a change in the working tree. The
 * caller clears it with {@link clearVerdict} right before each turn.
 */
export async function installVerdictCommand(
  opts: { homeDir: string },
): Promise<{ binDir: string; verdictFile: string; coordinateFile: string }> {
  const binDir = join(opts.homeDir, "bin");
  await mkdir(binDir, { recursive: true });
  const commands: Array<[string, string]> = [
    [VERDICT_COMMAND, renderVerdictCommand()],
    [COORDINATE_COMMAND, renderCoordinateCommand()],
  ];
  for (const [name, source] of commands) {
    const commandPath = join(binDir, name);
    await writeFile(commandPath, source, { mode: 0o755 });
    await chmod(commandPath, 0o755);
  }
  const stateDir = join(opts.homeDir, ".aktis");
  await mkdir(stateDir, { recursive: true });
  return { binDir, verdictFile: join(stateDir, "verdict"), coordinateFile: join(stateDir, "coordinate") };
}

/**
 * Source of the `aktis-coordinate` command: `proceed`, `wait <N>` or
 * `stack <N>` (N a positive integer; one leading `#` is stripped). Anything
 * else exits non-zero and writes nothing, so the model sees the mistake.
 */
export function renderCoordinateCommand(): string {
  return (
    `#!/bin/sh\n` +
    `# Generated by the SWE agent runner (src/outcome.ts). Records a factory\n` +
    `# coordination decision in the runner-owned file named by $${COORDINATE_FILE_ENV}.\n` +
    `usage() { echo "usage: ${COORDINATE_COMMAND} proceed | wait <number> | stack <number>" >&2; exit 2; }\n` +
    `case "$1" in\n` +
    `  proceed) [ "$#" -eq 1 ] || usage; value="proceed" ;;\n` +
    `  wait|stack)\n` +
    `    [ "$#" -eq 2 ] || usage\n` +
    `    n="\${2#\\#}"\n` +
    `    case "$n" in ''|0*|*[!0-9]*) usage ;; esac\n` +
    `    value="$1 $n" ;;\n` +
    `  *) usage ;;\n` +
    `esac\n` +
    `if [ -z "$${COORDINATE_FILE_ENV}" ]; then echo "${COORDINATE_COMMAND}: ${COORDINATE_FILE_ENV} is not set" >&2; exit 1; fi\n` +
    `printf '%s\\n' "$value" > "$${COORDINATE_FILE_ENV}" || exit 1\n` +
    `echo "${COORDINATE_COMMAND}: recorded $value"\n`
  );
}

/** Removes the coordination file at the start of every turn (see {@link clearVerdict}). */
export async function clearCoordination(coordinateFile: string): Promise<void> {
  await rm(coordinateFile, { force: true });
}

const COORDINATION_LINE = /^(?:(proceed)|(wait|stack) ([1-9][0-9]*))$/;

/** The recorded coordination decision, or `{decision: "none"}` when the file is missing or holds anything else. */
export async function readCoordination(coordinateFile: string): Promise<OutcomeCoordination> {
  let raw: string;
  try {
    raw = await readFile(coordinateFile, "utf8");
  } catch {
    return { decision: "none" };
  }
  const m = COORDINATION_LINE.exec(raw.trim());
  if (!m) return { decision: "none" };
  if (m[1]) return { decision: "proceed" };
  const number = prNumber(m[3]);
  if (number === null) return { decision: "none" };
  return { decision: m[2] as "wait" | "stack", number };
}

/**
 * Removes the verdict file. Called at the start of every turn so a verdict a
 * previous turn recorded can't be reported as this turn's.
 */
export async function clearVerdict(verdictFile: string): Promise<void> {
  await rm(verdictFile, { force: true });
}

/** The recorded verdict, or "none" when the file is missing or holds anything outside {@link VERDICTS}. */
export async function readVerdict(verdictFile: string): Promise<OutcomeVerdict> {
  let raw: string;
  try {
    raw = await readFile(verdictFile, "utf8");
  } catch {
    return "none";
  }
  const value = raw.trim();
  return (VERDICTS as readonly string[]).includes(value) ? (value as Verdict) : "none";
}

/**
 * The PR number as an integer. Discovery hands it over as a string (it's also
 * a `gh` argument); triggers compare it as a JSON number, so anything that
 * isn't a plain positive integer is dropped rather than sent as NaN or text.
 */
export function prNumber(pr: string | number | null | undefined): number | null {
  if (pr === null || pr === undefined) return null;
  const s = String(pr).trim();
  if (!/^[1-9][0-9]*$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;
}

/**
 * Builds the outcome. `pullRequest` only when the runner discovered an open PR
 * with a usable number and URL. `verdict` only when given, which the caller
 * does only on read-only runs (see {@link collectOutcome}).
 */
export function buildOutcome(opts: {
  discovered: { pr: string | null; prUrl: string | null } | null;
  madeNewCommits: boolean;
  verdict?: OutcomeVerdict;
  coordination?: OutcomeCoordination;
}): SweOutcome {
  const outcome: SweOutcome = { madeNewCommits: opts.madeNewCommits };
  const number = prNumber(opts.discovered?.pr);
  const url = opts.discovered?.prUrl;
  if (number !== null && url) outcome.pullRequest = { number, url };
  if (opts.verdict !== undefined) outcome.verdict = opts.verdict;
  if (opts.coordination !== undefined) outcome.coordination = opts.coordination;
  return outcome;
}

/**
 * The end-of-turn outcome. `report` ({@link reportedOutcome}) decides which
 * runner-owned file is read: a verdict run reports only `verdict`, a
 * coordination run only `coordination`, and a write run neither -- whatever
 * command the model actually ran.
 */
export async function collectOutcome(opts: {
  discovered: { pr: string | null; prUrl: string | null } | null;
  madeNewCommits: boolean;
  report: ReportedOutcome | null;
  verdictFile: string;
  coordinateFile: string;
}): Promise<SweOutcome> {
  let extra: { verdict?: OutcomeVerdict; coordination?: OutcomeCoordination } = {};
  if (opts.report === "coordination") {
    extra = { coordination: await readCoordination(opts.coordinateFile) };
  } else if (opts.report === "verdict") {
    extra = { verdict: await readVerdict(opts.verdictFile) };
  }
  return buildOutcome({ discovered: opts.discovered, madeNewCommits: opts.madeNewCommits, ...extra });
}
