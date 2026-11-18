import { cp, mkdir, readdir } from "node:fs/promises";
import { basename, join } from "node:path";

/**
 * Copies the skills the core-controller mounted from the Agent CR's
 * `spec.skills` (ADR 0051: one read-only ConfigMap directory per skill under
 * `AKTIS_SKILLS_DIR`) into `$HOME/.claude/skills`, where the Claude Code CLI
 * looks for user skills. Copied rather than mounted there directly: a mount
 * nested in the tmp emptyDir would leave `~/.claude` root-owned and unwritable.
 *
 * A ConfigMap volume exposes each key as a symlink into a hidden `..data`
 * directory (the kubelet's atomic-writer layout), so the copy dereferences
 * links and skips the `..*` bookkeeping entries. Returns the names copied;
 * an unset or missing source just means no skills were configured.
 */
export async function installMountedSkills(sourceDir: string, homeDir: string): Promise<string[]> {
  if (!sourceDir) return [];
  let entries;
  try {
    entries = await readdir(sourceDir, { withFileTypes: true });
  } catch {
    return [];
  }
  const dest = join(homeDir, ".claude", "skills");
  const installed: string[] = [];
  for (const entry of entries) {
    if (entry.name.startsWith(".") || !entry.isDirectory()) continue;
    await mkdir(dest, { recursive: true });
    await cp(join(sourceDir, entry.name), join(dest, entry.name), {
      recursive: true,
      dereference: true,
      force: true,
      filter: (src) => !basename(src).startsWith(".."),
    });
    installed.push(entry.name);
  }
  return installed;
}

/**
 * Names of the Claude Code skills installed under `$HOME/.claude/skills`
 * (by installMountedSkills above, or baked into the image); a missing
 * directory just means none.
 */
export async function listInstalledSkills(homeDir: string): Promise<string[]> {
  try {
    const entries = await readdir(join(homeDir, ".claude", "skills"), { withFileTypes: true });
    return entries.filter((entry) => entry.isDirectory()).map((entry) => entry.name);
  } catch {
    return [];
  }
}

/**
 * Splits a leading `/<skill>` invocation off the caller's instruction when it
 * names an installed skill. The CLI only expands a slash command at the very
 * start of the prompt -- confirmed empirically on the pinned CLI: the same
 * `/skill` under buildPrompt's `## Task` heading is plain text, and a skill
 * with `disable-model-invocation: true` (e.g. `implement`) then can't be
 * reached at all. Anything else -- no slash, or a slash naming something not
 * installed (a path, a typo) -- is left untouched.
 */
export function splitSkillInvocation(
  instruction: string,
  installed: readonly string[],
): { skill: string | null; instruction: string } {
  const match = /^\/([A-Za-z0-9_-]+)(?:\s+|$)/.exec(instruction.trimStart());
  const skill = match?.[1];
  if (!match || !skill || !installed.includes(skill)) return { skill: null, instruction };
  return { skill, instruction: instruction.trimStart().slice(match[0].length) };
}
