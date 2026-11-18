# Vendored Claude Code skills

Each directory here is one Claude Code skill (a `SKILL.md` plus any files it
references). Listing its name in `claudeCodeSweAgent.skills` renders it into a
`claude-code-skill-<name>` ConfigMap, and the core-controller mounts that into
the claude-code-swe-agent run pod (docs/adr/0051-file-based-agent-skills.md).

Skills are instructions the agent follows, so they are vendored, not fetched at
run time: a change in behaviour comes with a reviewable diff and a chart
version bump.

| Skill | Source | Pinned commit |
| --- | --- | --- |
| `implement` | [mattpocock/skills](https://github.com/mattpocock/skills) `skills/engineering/implement` | `d81f3a183412e71a5b1e84ca21bc1a35eea03a60` |
| `tdd` | [mattpocock/skills](https://github.com/mattpocock/skills) `skills/engineering/tdd` | `d81f3a183412e71a5b1e84ca21bc1a35eea03a60` |
| `code-review` | [mattpocock/skills](https://github.com/mattpocock/skills) `skills/engineering/code-review` | `d81f3a183412e71a5b1e84ca21bc1a35eea03a60` |

mattpocock/skills is MIT-licensed; its license is kept alongside as
`LICENSE.mattpocock-skills`.

## Adding or updating a skill

1. Copy the skill's directory here unchanged and add a row above with its
   source and commit.
2. Add its name to `claudeCodeSweAgent.skills` in `values.yaml`.
3. Keep each skill under ConfigMap's 1 MiB limit, with file names made of
   letters, digits, `-`, `_` and `.` (the path goes into a ConfigMap key with
   `/` written as `__`).

Org-specific skills that shouldn't live in this chart go in a ConfigMap you
manage yourself, listed under `claudeCodeSweAgent.extraSkills`.
