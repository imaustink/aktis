import { lstat, mkdir, mkdtemp, readdir, readFile, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { installMountedSkills, listInstalledSkills, splitSkillInvocation } from "./skills.js";

/**
 * Lays out a skill the way the kubelet's atomic writer does for a configMap
 * volume with `items`: the real files live in a timestamped `..<ts>` dir,
 * `..data` links to it, and each top-level entry links through `..data`.
 */
async function mountConfigMapSkill(root: string, name: string, files: Record<string, string>): Promise<void> {
  const skillDir = join(root, name);
  const tsDir = join(skillDir, "..2026_11_12_00_00_00.000000001");
  for (const [path, content] of Object.entries(files)) {
    await mkdir(join(tsDir, path, ".."), { recursive: true });
    await writeFile(join(tsDir, path), content);
  }
  await symlink("..2026_11_12_00_00_00.000000001", join(skillDir, "..data"));
  const topLevel = new Set(Object.keys(files).map((p) => p.split("/")[0]!));
  for (const entry of topLevel) await symlink(join("..data", entry), join(skillDir, entry));
}

describe("installMountedSkills", () => {
  it("copies each mounted skill into ~/.claude/skills as real files, nested paths included", async () => {
    const src = await mkdtemp(join(tmpdir(), "skills-src-"));
    const homeDir = await mkdtemp(join(tmpdir(), "skills-home-"));
    await mountConfigMapSkill(src, "tdd", { "SKILL.md": "# tdd", "agents/openai.yaml": "x: 1" });
    await mountConfigMapSkill(src, "implement", { "SKILL.md": "# implement" });

    expect((await installMountedSkills(src, homeDir)).sort()).toEqual(["implement", "tdd"]);

    const tdd = join(homeDir, ".claude/skills/tdd");
    expect((await readdir(tdd)).sort()).toEqual(["SKILL.md", "agents"]);
    expect(await readFile(join(tdd, "agents/openai.yaml"), "utf8")).toBe("x: 1");
    expect((await lstat(join(tdd, "SKILL.md"))).isSymbolicLink()).toBe(false);
    expect((await listInstalledSkills(homeDir)).sort()).toEqual(["implement", "tdd"]);
  });

  it("does nothing when no skills dir was configured or mounted", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "skills-home-"));
    expect(await installMountedSkills("", homeDir)).toEqual([]);
    expect(await installMountedSkills(join(homeDir, "missing"), homeDir)).toEqual([]);
    expect(await listInstalledSkills(homeDir)).toEqual([]);
  });
});

describe("listInstalledSkills", () => {
  it("lists the skill directories under ~/.claude/skills", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "skills-home-"));
    for (const name of ["implement", "tdd"]) {
      await mkdir(join(homeDir, ".claude/skills", name), { recursive: true });
    }
    await writeFile(join(homeDir, ".claude/skills/README.md"), "not a skill");
    expect((await listInstalledSkills(homeDir)).sort()).toEqual(["implement", "tdd"]);
  });

  it("is empty when nothing was installed", async () => {
    expect(await listInstalledSkills(await mkdtemp(join(tmpdir(), "skills-home-")))).toEqual([]);
  });
});

describe("splitSkillInvocation", () => {
  const installed = ["implement", "tdd"];

  it("splits a leading installed skill off the instruction", () => {
    expect(splitSkillInvocation("/implement the spec in #42\nand more", installed)).toEqual({
      skill: "implement",
      instruction: "the spec in #42\nand more",
    });
  });

  it("tolerates leading whitespace and a bare invocation", () => {
    expect(splitSkillInvocation("  /tdd", installed)).toEqual({ skill: "tdd", instruction: "" });
  });

  it("leaves skills that aren't installed, paths, and mid-text slashes alone", () => {
    for (const text of ["/wizard do it", "/etc/hosts is wrong", "/implementation notes", "please /implement this"]) {
      expect(splitSkillInvocation(text, installed)).toEqual({ skill: null, instruction: text });
    }
  });
});
