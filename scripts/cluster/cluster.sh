#!/usr/bin/env bash
# cluster.sh — the local cluster, whichever one it is. See docs/local-cluster.md.
#
# Usage:
#   ./scripts/cluster/cluster.sh up              start it and point kubectl at it
#   ./scripts/cluster/cluster.sh down [--purge]  stop it (--purge: delete its data)
#   ./scripts/cluster/cluster.sh provider        which provider is selected
#   ./scripts/cluster/cluster.sh context         the kube context it uses
#   ./scripts/cluster/cluster.sh skaffold ...    skaffold, with the provider's flags
#
# The provider comes from AKTIS_CLUSTER (environment, then cluster.local.env),
# defaulting to ferry on an Apple-silicon Mac that has it and minikube
# elsewhere.

set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
cluster_load

cmd="${1:-}"
[[ $# -gt 0 ]] && shift

case "$cmd" in
  up)       cluster_up ;;
  down)     provider_down "$@" ;;
  provider) echo "$AKTIS_CLUSTER" ;;
  context)  provider_context ;;
  skaffold)
    # Refuses the wrong context here too: skaffold deploys to whatever is
    # current, and this is the entry point people will run by hand.
    cluster_require_context
    extra=()
    while IFS= read -r line; do [[ -n "$line" ]] && extra+=("$line"); done < <(provider_skaffold_args)
    sub="${1:-}"
    [[ $# -gt 0 ]] && shift
    cd "$CLUSTER_REPO_ROOT"
    exec skaffold "$sub" ${extra[@]+"${extra[@]}"} "$@"
    ;;
  *)
    sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
    echo "" >&2
    echo "providers: $(cluster_providers | tr '\n' ' ')  (selected: $AKTIS_CLUSTER)" >&2
    exit 2
    ;;
esac
