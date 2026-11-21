import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { alreadyCloses, closingLine, ensureClosingReference, parseClosesIssue } from "./closingRef.js";

describe("parseClosesIssue", () => {
  it("reads closesIssue + repository from the run params", () => {
    expect(parseClosesIssue({ closesIssue: "49", repository: "imaustink/glyph" })).toEqual({
      repo: "imaustink/glyph",
      number: "49",
    });
  });

  it("is null when the trigger set no closesIssue (PR triage, chat)", () => {
    expect(parseClosesIssue({ number: "49", repository: "imaustink/glyph" })).toBeNull();
    expect(parseClosesIssue({})).toBeNull();
  });

  it("rejects values that aren't a plain issue number or owner/repo", () => {
    for (const closesIssue of ["0", "-3", "12abc", "12 && rm -rf /", "#12", ""]) {
      expect(parseClosesIssue({ closesIssue, repository: "imaustink/glyph" })).toBeNull();
    }
    expect(parseClosesIssue({ closesIssue: "12", repository: "glyph" })).toBeNull();
    expect(parseClosesIssue({ closesIssue: "12", repository: "a/b c" })).toBeNull();
  });
});

describe("closingLine", () => {
  it("is repo-relative when the PR is in the issue's repo (case-insensitive)", () => {
    expect(closingLine({ repo: "imaustink/glyph", number: "49" }, "imaustink/Glyph")).toBe("Closes #49");
  });

  it("names the repo when the PR lives elsewhere", () => {
    expect(closingLine({ repo: "imaustink/glyph", number: "49" }, "imaustink/aktis")).toBe("Closes imaustink/glyph#49");
  });
});

describe("alreadyCloses", () => {
  const issue = { repo: "imaustink/glyph", number: "78" };

  it("is true when GitHub lists the issue as a closing reference", () => {
    expect(
      alreadyCloses(
        { closingIssuesReferences: [{ number: 78, url: "https://github.com/imaustink/glyph/issues/78" }] },
        issue,
      ),
    ).toBe(true);
  });

  it("is false for a body that only mentions the issue (glyph#89's shape)", () => {
    expect(alreadyCloses({ body: "Fixes the bugs from #78 and #79.", closingIssuesReferences: [] }, issue)).toBe(false);
  });

  it("is false when the same number closes an issue in another repo", () => {
    expect(
      alreadyCloses({ closingIssuesReferences: [{ number: 78, url: "https://github.com/other/repo/issues/78" }] }, issue),
    ).toBe(false);
  });
});

describe("ensureClosingReference", () => {
  const env: NodeJS.ProcessEnv = { ...process.env };
  let binDir: string;
  let viewFile: string;
  let editFile: string;
  const issue = { repo: "acme/widgets", number: "12" };

  // Fake `gh`: `pr view` prints viewFile (or fails when it's absent); `pr
  // edit` records its --body argument to editFile.
  beforeAll(() => {
    binDir = mkdtempSync(join(tmpdir(), "fakegh-closes-"));
    viewFile = join(binDir, "view.json");
    editFile = join(binDir, "edit-body");
    const gh = join(binDir, "gh");
    writeFileSync(
      gh,
      `#!/usr/bin/env node\n` +
        `const { existsSync, readFileSync, writeFileSync } = require("node:fs");\n` +
        `const args = process.argv.slice(2);\n` +
        `if (args[0] === "pr" && args[1] === "view") {\n` +
        `  if (!existsSync(${JSON.stringify(viewFile)})) { process.stderr.write("no PR"); process.exit(1); }\n` +
        `  process.stdout.write(readFileSync(${JSON.stringify(viewFile)}, "utf8"));\n` +
        `  process.exit(0);\n` +
        `}\n` +
        `if (args[0] === "pr" && args[1] === "edit") {\n` +
        `  writeFileSync(${JSON.stringify(editFile)}, args[args.indexOf("--body") + 1]);\n` +
        `  process.exit(0);\n` +
        `}\n` +
        `process.exit(1);\n`,
    );
    chmodSync(gh, 0o755);
    env.PATH = `${binDir}:${process.env.PATH}`;
  });

  afterAll(() => {
    rmSync(binDir, { recursive: true, force: true });
  });

  beforeEach(() => {
    rmSync(viewFile, { force: true });
    rmSync(editFile, { force: true });
  });

  it("leaves a PR that already closes the issue untouched", async () => {
    writeFileSync(
      viewFile,
      JSON.stringify({
        body: "Fix.\n\nFixes #12",
        closingIssuesReferences: [{ number: 12, url: "https://github.com/acme/widgets/issues/12" }],
      }),
    );
    expect(await ensureClosingReference({ prRepo: "acme/widgets", pr: "7", issue, env })).toBe("present");
    expect(existsSync(editFile)).toBe(false);
  });

  it("appends a Closes line to a description that only mentions the issue", async () => {
    writeFileSync(viewFile, JSON.stringify({ body: "Fix for #12.\n", closingIssuesReferences: [] }));
    expect(await ensureClosingReference({ prRepo: "acme/widgets", pr: "7", issue, env })).toBe("added");
    expect(readFileSync(editFile, "utf8")).toBe("Fix for #12.\n\nCloses #12");
  });

  it("writes just the Closes line into an empty description", async () => {
    writeFileSync(viewFile, JSON.stringify({ body: "", closingIssuesReferences: [] }));
    expect(await ensureClosingReference({ prRepo: "acme/widgets", pr: "7", issue, env })).toBe("added");
    expect(readFileSync(editFile, "utf8")).toBe("Closes #12");
  });

  it("reports (rather than throws) when the PR can't be read", async () => {
    const result = await ensureClosingReference({ prRepo: "acme/widgets", pr: "7", issue, env });
    expect(result).toEqual({ error: expect.stringContaining("gh pr view failed") });
  });
});
