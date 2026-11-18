# 0051. File-based skills mounted into agent run pods

Date: 2026-11-12

## Status

Proposed. Replaces the runtime-download init container from PR #262.

## Context

Sub-agents that wrap a coding engine (claude-code-swe-agent today, and
others later) can use that engine's own skills: a directory with a
`SKILL.md` and any files it references, which the engine discovers on disk
(`~/.claude/skills/<name>/` for Claude Code). We want operators to choose which
skills an agent gets without rebuilding its image.

PR #262 did this with an `install-claude-skills` init container in the
community-components chart. On every run it curled a GitHub tarball
(`mattpocock/skills` at a pinned SHA) and copied the selected directories into
the run's `/tmp` HOME. That worked, but:

- **Every run had a network dependency.** If codeload.github.com was slow or
  down, or rate-limited us, agent runs failed before they started. That's an
  egress path the run pod didn't otherwise need.
- **The downloader held every secret the run had.** `buildRunJob` merges the
  run's secretEnv into every init container, so a curl-and-untar step ran with
  the run's GitHub and Claude credentials in its environment.
- **The mechanism lived only in the chart.** Skills were a shell script inside
  one Agent template, not something the platform knows about, so the next agent
  would have to copy the script, and nothing outside Helm could configure it.
- **What the agent was told to do changed outside review.** A skill is
  instructions the agent follows. The pinned SHA helped, but the content itself
  was never in this repo's diff.

The existing `AgentSpec.skillRefs` is a different thing and is not reused here.
It names platform `Skill` CRs, which are planner prompt material plus
`toolRefs`/`agentRefs` that feed RBAC derivation (ADR 0011, 0021). Engine skills
are opaque files the platform never reads.

## Decision

1. **`AgentSpec.skills`**: a list of `{name, configMapName, files[]}`. For each
   entry, `buildRunJob` adds a read-only ConfigMap volume to the **run
   container only** (not to init containers), mounted at
   `/etc/aktis/skills/<name>/`, and sets `AKTIS_SKILLS_DIR=/etc/aktis/skills`.
   `files` maps ConfigMap keys to relative paths, the same as a configMap
   volume's `items`, because ConfigMap keys can't contain `/` and skills have
   nested files (`agents/openai.yaml`, `scripts/…`). The ConfigMap is not
   `optional`: if it is missing, the pod fails to mount, rather than the agent
   quietly running without a skill it was configured with. The builder rejects
   bad names, duplicates, and absolute or `..` paths, which fails the run
   with `InvalidAgentRun`.
2. **The mount is outside HOME on purpose.** If a volume were mounted inside the
   tmp emptyDir (for example at `/tmp/home/.claude/skills/x`), the container
   runtime would create the missing parent directories as root, and the
   non-root agent could then no longer write `~/.claude`. So the platform's
   contract is "skills are under `$AKTIS_SKILLS_DIR`", and each agent image moves
   them where its engine looks. claude-code-swe-agent copies them into
   `~/.claude/skills` at startup. The copy dereferences the kubelet's `..data`
   symlinks and skips those bookkeeping entries.
3. **The chart vendors the skills.** community-components keeps skill
   directories under `skills/claude-code/<name>/`, with provenance and license in
   its README. `claudeCodeSweAgent.skills` lists names, and the chart renders one
   `claude-code-skill-<name>` ConfigMap per name (path `/` encoded as `__` in the
   key) plus the matching `spec.skills` entry. An unknown or invalid name fails
   the render. `claudeCodeSweAgent.extraSkills` passes raw `spec.skills` entries
   for ConfigMaps managed outside the chart.
4. **The controller needs no new RBAC.** The kubelet mounts the ConfigMap. The
   controller never reads it, so the controller still has no cluster-wide
   ConfigMap access.

The `/<skill>` prompt hoist from PR #262 (`skills.ts`'s `splitSkillInvocation`)
stays. It depends only on what is in `~/.claude/skills`, not on how it got
there.

## Consequences

- Runs no longer reach out to GitHub to fetch skills, and no init container
  runs third-party content with the run's secrets.
- Changing or adding a vendored skill is a reviewed diff plus a chart version
  bump. That bump is needed anyway: Argo caches renders by chart version.
- Any agent image can use `spec.skills` by reading `AKTIS_SKILLS_DIR`. The
  engine-specific part is one copy step in that image.
- Limits: ConfigMaps hold at most 1 MiB, so each skill must stay under that.
  File names must be valid ConfigMap keys after `/`→`__` encoding.
  ConfigMap updates don't reach a run that is already going, but runs are short
  one-shot Jobs, so the next run picks them up.
- Not validated at reconcile time: a missing ConfigMap shows up as a pod stuck
  on FailedMount until the Job's deadline. An `Agent` `Ready` check for it would
  need ConfigMap read RBAC, and we're deferring that.
- Future option: Kubernetes `image` volumes (OCI artifacts) would remove the
  1 MiB limit and the key encoding. They're not used because the feature is
  still beta and is off by default on the clusters we deploy to.
