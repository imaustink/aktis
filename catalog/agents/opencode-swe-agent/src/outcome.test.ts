import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  buildOutcome,
  clearCoordination,
  clearVerdict,
  COORDINATE_PROMPT_SECTION,
  outcomePromptSection,
  reportedOutcome,
  COORDINATE_COMMAND,
  COORDINATE_FILE_ENV,
  readCoordination,
  collectOutcome,
  installVerdictCommand,
  prNumber,
  readVerdict,
  VERDICT_COMMAND,
  VERDICT_FILE_ENV,
  VERDICT_PROMPT_SECTION,
  VERDICTS,
} from "./outcome.js";

const pr = { pr: "42", prUrl: "https://github.com/acme/widgets/pull/42" };

describe("buildOutcome", () => {
  it("reports the PR the turn ended with, as an integer number", () => {
    expect(buildOutcome({ discovered: pr, madeNewCommits: true })).toEqual({
      pullRequest: { number: 42, url: "https://github.com/acme/widgets/pull/42" },
      madeNewCommits: true,
    });
    expect(typeof buildOutcome({ discovered: pr, madeNewCommits: true }).pullRequest?.number).toBe("number");
  });

  it("omits pullRequest when no open PR was discovered", () => {
    expect(buildOutcome({ discovered: null, madeNewCommits: false })).toEqual({ madeNewCommits: false });
    expect(buildOutcome({ discovered: { pr: null, prUrl: null }, madeNewCommits: true })).toEqual({ madeNewCommits: true });
    // A number with no URL isn't something a trigger can act on.
    expect(buildOutcome({ discovered: { pr: "42", prUrl: null }, madeNewCommits: true })).toEqual({ madeNewCommits: true });
  });

  it("omits pullRequest when the number isn't a plain positive integer", () => {
    for (const bad of ["", "0", "-1", "4.2", "42abc", "#42", "NaN", "99999999999999999999"]) {
      expect(buildOutcome({ discovered: { pr: bad, prUrl: pr.prUrl }, madeNewCommits: true }).pullRequest).toBeUndefined();
    }
  });

  it("includes verdict only when given", () => {
    expect(buildOutcome({ discovered: pr, madeNewCommits: false, verdict: "none" }).verdict).toBe("none");
    expect("verdict" in buildOutcome({ discovered: pr, madeNewCommits: false })).toBe(false);
  });
});

describe("prNumber", () => {
  it("coerces discovery's string to an integer and tolerates whitespace", () => {
    expect(prNumber("7")).toBe(7);
    expect(prNumber(" 7\n")).toBe(7);
    expect(prNumber(7)).toBe(7);
    expect(prNumber(null)).toBeNull();
    expect(prNumber(undefined)).toBeNull();
    expect(prNumber("1e3")).toBeNull();
  });
});

describe("verdict file", () => {
  let home: string;
  let file: string;

  beforeEach(async () => {
    home = mkdtempSync(join(tmpdir(), "verdict-test-"));
    ({ verdictFile: file } = await installVerdictCommand({ homeDir: home }));
  });
  afterEach(() => rmSync(home, { recursive: true, force: true }));

  it("reads each valid verdict", async () => {
    for (const v of VERDICTS) {
      writeFileSync(file, `${v}\n`);
      expect(await readVerdict(file)).toBe(v);
    }
  });

  it("is none when nothing was recorded", async () => {
    expect(existsSync(file)).toBe(false);
    expect(await readVerdict(file)).toBe("none");
  });

  it("is none for anything outside the enum", async () => {
    for (const bad of ["", "approve", "READY", "ready please", "changes_requested", "none", "ready\nchanges-requested"]) {
      writeFileSync(file, bad);
      expect(await readVerdict(file)).toBe("none");
    }
  });

  it("clearVerdict drops a previous turn's verdict", async () => {
    writeFileSync(file, "changes-requested\n");
    await clearVerdict(file);
    expect(await readVerdict(file)).toBe("none");
    await clearVerdict(file); // idempotent when already absent
  });

  it("lives outside the working tree, under the home directory", () => {
    expect(file.startsWith(home)).toBe(true);
  });
});

describe("collectOutcome", () => {
  let home: string;
  let file: string;
  let coordinateFile: string;
  beforeEach(async () => {
    home = mkdtempSync(join(tmpdir(), "collect-test-"));
    ({ verdictFile: file, coordinateFile } = await installVerdictCommand({ homeDir: home }));
  });
  afterEach(() => rmSync(home, { recursive: true, force: true }));

  it("reports the recorded verdict on a read-only run", async () => {
    writeFileSync(file, "changes-requested\n");
    expect(await collectOutcome({ discovered: pr, madeNewCommits: false, report: "verdict", verdictFile: file, coordinateFile })).toEqual({
      pullRequest: { number: 42, url: pr.prUrl },
      madeNewCommits: false,
      verdict: "changes-requested",
    });
  });

  it("reports none on a read-only run that recorded nothing", async () => {
    expect((await collectOutcome({ discovered: pr, madeNewCommits: false, report: "verdict", verdictFile: file, coordinateFile })).verdict).toBe(
      "none",
    );
  });

  it("never reports a verdict on a write run, even if one was recorded", async () => {
    writeFileSync(file, "ready\n");
    const outcome = await collectOutcome({ discovered: pr, madeNewCommits: true, report: null, verdictFile: file, coordinateFile });
    expect("verdict" in outcome).toBe(false);
  });
});

describe(`the ${VERDICT_COMMAND} command`, () => {
  let home: string;
  let binDir: string;
  let file: string;
  const run = (args: string[], env: NodeJS.ProcessEnv = { ...process.env, [VERDICT_FILE_ENV]: file }) =>
    spawnSync(join(binDir, VERDICT_COMMAND), args, { env, encoding: "utf8" });

  beforeEach(async () => {
    home = mkdtempSync(join(tmpdir(), "verdict-cli-"));
    ({ binDir, verdictFile: file } = await installVerdictCommand({ homeDir: home }));
  });
  afterEach(() => rmSync(home, { recursive: true, force: true }));

  it("records each valid verdict, which the runner then reads back", async () => {
    for (const v of VERDICTS) {
      const res = run([v]);
      expect(res.status).toBe(0);
      expect(readFileSync(file, "utf8")).toBe(`${v}\n`);
      expect(await readVerdict(file)).toBe(v);
    }
  });

  it("resolves by name once its directory is on PATH", () => {
    const res = spawnSync(VERDICT_COMMAND, ["ready"], {
      env: { ...process.env, PATH: `${binDir}:${process.env.PATH}`, [VERDICT_FILE_ENV]: file },
      encoding: "utf8",
    });
    expect(res.status).toBe(0);
    expect(readFileSync(file, "utf8")).toBe("ready\n");
  });

  it("rejects anything else with a usage message and writes nothing", () => {
    for (const args of [[], ["approve"], ["READY"], ["ready", "extra"], ["none"], ["ready;touch x"], [""]]) {
      const res = run(args);
      expect(res.status, JSON.stringify(args)).not.toBe(0);
      expect(res.stderr).toContain(`usage: ${VERDICT_COMMAND} changes-requested|ready`);
      expect(existsSync(file), JSON.stringify(args)).toBe(false);
    }
  });

  it("an invalid call doesn't overwrite a verdict already recorded this turn", () => {
    expect(run(["ready"]).status).toBe(0);
    expect(run(["approve"]).status).not.toBe(0);
    expect(readFileSync(file, "utf8")).toBe("ready\n");
  });

  it("fails without the runner's file env rather than writing somewhere else", () => {
    const env = { ...process.env };
    delete env[VERDICT_FILE_ENV];
    const res = run(["ready"], env);
    expect(res.status).not.toBe(0);
    expect(res.stderr).toContain(VERDICT_FILE_ENV);
  });
});

describe("VERDICT_PROMPT_SECTION", () => {
  it("names both commands", () => {
    for (const v of VERDICTS) expect(VERDICT_PROMPT_SECTION).toContain(`\`${VERDICT_COMMAND} ${v}\``);
    expect(VERDICT_PROMPT_SECTION).toContain("exactly once");
  });
});

// docs/adr/0055: the factory coordinator records its decision with aktis-coordinate.
describe(`the ${COORDINATE_COMMAND} command`, () => {
  let home: string;
  let binDir: string;
  let file: string;
  const run = (args: string[], env: NodeJS.ProcessEnv = { ...process.env, [COORDINATE_FILE_ENV]: file }) =>
    spawnSync(join(binDir, COORDINATE_COMMAND), args, { env, encoding: "utf8" });

  beforeEach(async () => {
    home = mkdtempSync(join(tmpdir(), "coordinate-cli-"));
    ({ binDir, coordinateFile: file } = await installVerdictCommand({ homeDir: home }));
  });
  afterEach(() => rmSync(home, { recursive: true, force: true }));

  it("records each valid decision, which the runner then reads back", async () => {
    const cases: Array<[string[], string, unknown]> = [
      [["proceed"], "proceed", { decision: "proceed" }],
      [["wait", "12"], "wait 12", { decision: "wait", number: 12 }],
      [["stack", "#7"], "stack 7", { decision: "stack", number: 7 }],
    ];
    for (const [args, line, parsed] of cases) {
      const res = run(args);
      expect(res.status, JSON.stringify(args)).toBe(0);
      expect(readFileSync(file, "utf8")).toBe(`${line}\n`);
      expect(await readCoordination(file)).toEqual(parsed);
    }
  });

  it("resolves by name once its directory is on PATH", () => {
    const res = spawnSync(COORDINATE_COMMAND, ["wait", "3"], {
      env: { ...process.env, PATH: `${binDir}:${process.env.PATH}`, [COORDINATE_FILE_ENV]: file },
      encoding: "utf8",
    });
    expect(res.status).toBe(0);
    expect(readFileSync(file, "utf8")).toBe("wait 3\n");
  });

  it("rejects anything else with a usage message and writes nothing", () => {
    for (const args of [
      [],
      ["proceed", "1"],
      ["wait"],
      ["wait", "0"],
      ["wait", "012"],
      ["wait", "-3"],
      ["wait", "1.5"],
      ["stack", "abc"],
      ["stack", "1", "2"],
      ["wait", "1;touch x"],
      ["merge", "4"],
      ["PROCEED"],
      [""],
    ]) {
      const res = run(args);
      expect(res.status, JSON.stringify(args)).not.toBe(0);
      expect(res.stderr).toContain(`usage: ${COORDINATE_COMMAND}`);
      expect(existsSync(file), JSON.stringify(args)).toBe(false);
    }
  });

  it("fails without the runner's file env rather than writing somewhere else", () => {
    const env = { ...process.env };
    delete env[COORDINATE_FILE_ENV];
    const res = run(["proceed"], env);
    expect(res.status).not.toBe(0);
    expect(res.stderr).toContain(COORDINATE_FILE_ENV);
  });
});

describe("coordination file", () => {
  let home: string;
  let file: string;
  beforeEach(async () => {
    home = mkdtempSync(join(tmpdir(), "coordinate-file-"));
    ({ coordinateFile: file } = await installVerdictCommand({ homeDir: home }));
  });
  afterEach(() => rmSync(home, { recursive: true, force: true }));

  it("is none when nothing was recorded", async () => {
    expect(await readCoordination(file)).toEqual({ decision: "none" });
  });

  it("is none for anything outside the grammar", async () => {
    for (const raw of ["", "wait", "wait 0", "stack x", "proceed 3", "wait 3 4", "merge 2", "wait 99999999999999999999"]) {
      writeFileSync(file, `${raw}\n`);
      expect(await readCoordination(file), raw).toEqual({ decision: "none" });
    }
  });

  it("clearCoordination drops a previous turn's decision", async () => {
    writeFileSync(file, "wait 4\n");
    await clearCoordination(file);
    expect(await readCoordination(file)).toEqual({ decision: "none" });
  });

  it("lives outside the working tree, under the home directory, next to the verdict", async () => {
    const paths = await installVerdictCommand({ homeDir: home });
    expect(paths.coordinateFile).toBe(join(home, ".aktis", "coordinate"));
    expect(existsSync(join(paths.binDir, COORDINATE_COMMAND))).toBe(true);
  });
});

describe("reportedOutcome", () => {
  it("comes from the structured fields alone", () => {
    expect(reportedOutcome({ readOnly: false })).toBeNull();
    expect(reportedOutcome({ readOnly: false, outcome: "coordination" })).toBeNull();
    expect(reportedOutcome({ readOnly: true })).toBe("verdict");
    expect(reportedOutcome({ readOnly: true, outcome: "verdict" })).toBe("verdict");
    expect(reportedOutcome({ readOnly: true, outcome: "coordination" })).toBe("coordination");
  });

  it("maps to the matching prompt section", () => {
    expect(outcomePromptSection("verdict")).toBe(VERDICT_PROMPT_SECTION);
    expect(outcomePromptSection("coordination")).toBe(COORDINATE_PROMPT_SECTION);
    expect(outcomePromptSection(null)).toBeNull();
    expect(COORDINATE_PROMPT_SECTION).toContain(`${COORDINATE_COMMAND} wait <number>`);
  });
});

describe("collectOutcome with coordination", () => {
  let home: string;
  let verdictFile: string;
  let coordinateFile: string;
  beforeEach(async () => {
    home = mkdtempSync(join(tmpdir(), "collect-coord-"));
    ({ verdictFile, coordinateFile } = await installVerdictCommand({ homeDir: home }));
  });
  afterEach(() => rmSync(home, { recursive: true, force: true }));

  it("reports coordination instead of a verdict on a coordination run", async () => {
    writeFileSync(coordinateFile, "stack 9\n");
    writeFileSync(verdictFile, "ready\n");
    const outcome = await collectOutcome({
      discovered: null,
      madeNewCommits: false,
      report: "coordination",
      verdictFile,
      coordinateFile,
    });
    expect(outcome).toEqual({ madeNewCommits: false, coordination: { decision: "stack", number: 9 } });
  });

  it("reports coordination none when the coordinator recorded nothing", async () => {
    const outcome = await collectOutcome({
      discovered: null,
      madeNewCommits: false,
      report: "coordination",
      verdictFile,
      coordinateFile,
    });
    expect(outcome.coordination).toEqual({ decision: "none" });
    expect("verdict" in outcome).toBe(false);
  });

  it("never reports coordination on a verdict run, even if the file exists", async () => {
    writeFileSync(coordinateFile, "proceed\n");
    writeFileSync(verdictFile, "ready\n");
    const outcome = await collectOutcome({
      discovered: pr,
      madeNewCommits: false,
      report: "verdict",
      verdictFile,
      coordinateFile,
    });
    expect(outcome.verdict).toBe("ready");
    expect("coordination" in outcome).toBe(false);
  });

  it("never reports either on a write run", async () => {
    writeFileSync(coordinateFile, "proceed\n");
    writeFileSync(verdictFile, "ready\n");
    const outcome = await collectOutcome({
      discovered: pr,
      madeNewCommits: true,
      report: null,
      verdictFile,
      coordinateFile,
    });
    expect("coordination" in outcome).toBe(false);
    expect("verdict" in outcome).toBe(false);
  });
});
