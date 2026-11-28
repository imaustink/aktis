#!/usr/bin/env bash
# Render the software-factory EventSources/EventTriggers from
# examples/event-triggers/ for one deployment (docs/software-factory.md).
#
# The examples are the source of truth; this only substitutes the
# deployment-specific values and, for Helm, escapes {{placeholders}} for the
# aktis chart's `extraManifests` (which runs every entry through `tpl`).
#
# Usage:
#   render-factory.sh --agent <agent name> --app-slug <github app slug> \
#     [--format kubectl|helm] [--no-coordination] [--no-conflict-sweep] \
#     [--no-github-source] [--with-issue-review]
#
#   --format kubectl     multi-doc YAML for `kubectl apply -n <release ns> -f -` (default)
#   --format helm        an `extraManifests:` list to merge into the aktis chart's values
#   --no-coordination    plain ai-triage -> triage (github-issue-triage) instead of
#                        intake -> coordinate -> park/triage/triage-stacked (ADR 0055)
#   --no-conflict-sweep  omit the pr-conflicts source and factory-resync
#   --no-github-source   omit the `github` EventSource (you already have one)
#   --with-issue-review  also install github-issue-review (`ai-review` on an issue)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
ex="$root/examples/event-triggers"

agent="" slug="" format="kubectl" coord=1 conflicts=1 ghsource=1 issuereview=0
while [ $# -gt 0 ]; do
  case "$1" in
    --agent) agent="$2"; shift 2 ;;
    --app-slug) slug="$2"; shift 2 ;;
    --format) format="$2"; shift 2 ;;
    --no-coordination) coord=0; shift ;;
    --no-conflict-sweep) conflicts=0; shift ;;
    --no-github-source) ghsource=0; shift ;;
    --with-issue-review) issuereview=1; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -n "$agent" ] || { echo "--agent is required (e.g. claude-code-swe-agent or opencode-swe-agent)" >&2; exit 2; }
[ -n "$slug" ] || { echo "--app-slug is required (the GitHub App's slug, e.g. k5s-bot)" >&2; exit 2; }
case "$format" in kubectl|helm) ;; *) echo "--format must be kubectl or helm" >&2; exit 2 ;; esac
slug="${slug#app/}"

files=()
[ "$ghsource" = 1 ] && files+=(eventsource-github.yaml)
files+=(eventsource-runs.yaml)
[ "$conflicts" = 1 ] && files+=(eventsource-pr-conflicts.yaml)
if [ "$coord" = 1 ]; then
  files+=(eventtrigger-factory-intake.yaml eventtrigger-factory-coordinate.yaml
          eventtrigger-factory-park.yaml eventtrigger-factory-triage.yaml
          eventtrigger-factory-triage-stacked.yaml)
else
  files+=(eventtrigger-github-issue-triage.yaml)
fi
files+=(eventtrigger-github-pr-triage.yaml eventtrigger-github-pr-review.yaml
        eventtrigger-factory-review.yaml eventtrigger-factory-fix.yaml
        eventtrigger-factory-handoff.yaml)
[ "$conflicts" = 1 ] && files+=(eventtrigger-factory-resync.yaml)
[ "$issuereview" = 1 ] && files+=(eventtrigger-github-issue-review.yaml)

# One manifest: drop the comment header above apiVersion, substitute the agent
# and the App's search author.
body() {
  awk 'f || /^apiVersion:/ { f = 1; print }' "$ex/$1" \
    | sed -e "s|agentRef: \"opencode-swe-agent\"|agentRef: \"$agent\"|" \
          -e "s|author: \"app/k5s-bot\"|author: \"app/$slug\"|"
}

if [ "$format" = kubectl ]; then
  first=1
  for f in "${files[@]}"; do
    [ "$first" = 1 ] || echo "---"
    first=0
    echo "# from examples/event-triggers/$f"
    body "$f"
  done
else
  echo "# Software factory (docs/software-factory.md), rendered from examples/event-triggers/"
  echo "# by .claude/skills/configure-software-factory/render-factory.sh. Merge into the"
  echo "# aktis chart's extraManifests (append to an existing list, don't replace it)."
  echo "extraManifests:"
  for f in "${files[@]}"; do
    echo "  # ${f%.yaml}"
    echo "  - |"
    # Escape every {{placeholder}} as {{`{{placeholder}}`}} so `tpl` leaves it alone.
    body "$f" | sed -e 's/{{\([^{}`]*\)}}/{{`{{\1}}`}}/g' -e 's/^/    /' -e 's/^ *$//'
  done
fi
