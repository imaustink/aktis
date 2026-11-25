#!/usr/bin/env bash
# dev-up.sh — one-time/rare cluster prep, then hand off to Skaffold for the
# actual build+deploy (see skaffold.yaml at repo root).
#
# Which local cluster this runs on is a choice (docs/local-cluster.md): ferry
# on an Apple-silicon Mac (it stops with the install command if ferry is
# missing), minikube elsewhere, or whatever AKTIS_CLUSTER names --
# ferry | minikube | kind | custom.
#
# Usage:
#   ./scripts/dev-up.sh              # prep, then `skaffold run` (one-shot deploy)
#   ./scripts/dev-up.sh --dev        # prep, then `skaffold dev` (watch + redeploy loop)
#   ./scripts/dev-up.sh --prep-only  # just the prep steps below, no skaffold invocation
#
# Everything here is either a true one-time prerequisite or something
# Skaffold's helm deployer has no hook mechanism for (only its kubectl
# deployer supports before/after hooks) — see the "Prerequisites this file
# deliberately does NOT own" comment in skaffold.yaml.
#
# Prerequisites (one-time, out-of-band):
#   kubectl -n controller-agent create secret generic agent-orchestrator-secrets \
#     --from-literal=OPENAI_API_KEY=<key> \
#     --from-literal=AGENT_CALLBACK_SECRET=<random-secret>
#
#   kubectl -n controller-agent create secret generic recipe-publisher-secrets \
#     --from-literal=MEALIE_API_TOKEN=<token>
#
#   kubectl -n controller-agent create secret generic agent-orchestrator-openwebui-google-oauth \
#     --from-literal=client-secret=<google-oauth-client-secret>
#
#   kubectl -n controller-agent create secret generic searxng-secrets \
#     --from-literal=secret-key="$(openssl rand -hex 32)"
#
# All four secrets survive a cluster stop/start. If the cluster itself is
# deleted (`./scripts/cluster/cluster.sh down --purge`, `minikube delete`),
# you must re-create the secrets before running this script.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS="controller-agent"
MODE="run"

for arg in "$@"; do
  case "$arg" in
    --dev) MODE="dev" ;;
    --prep-only) MODE="none" ;;
  esac
done

step() { echo ""; echo "▶ $*"; }
die()  { echo "✗ $*" >&2; exit 1; }

# shellcheck source=cluster/lib.sh
source "$REPO_ROOT/scripts/cluster/lib.sh"
cluster_load

# ── 1. Local cluster ─────────────────────────────────────────────────────────
step "Starting the local cluster (provider: $AKTIS_CLUSTER)..."
# Starts it if it is not running and switches kubectl to its context. Whether
# Docker is needed at all is the provider's call: ferry builds images itself.
cluster_up

# Safety check: make sure we're pointing at the local cluster, not a real one.
cluster_require_context

# ── 2. Namespace ─────────────────────────────────────────────────────────────
step "Ensuring namespace '$NS'..."
kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"

# ── 3. Secrets check ─────────────────────────────────────────────────────────
step "Checking required secrets..."
MISSING_SECRETS=()
for secret in agent-orchestrator-secrets recipe-publisher-secrets agent-orchestrator-openwebui-google-oauth searxng-secrets; do
  if ! kubectl -n "$NS" get secret "$secret" >/dev/null 2>&1; then
    MISSING_SECRETS+=("$secret")
  fi
done
if [[ ${#MISSING_SECRETS[@]} -gt 0 ]]; then
  echo ""
  echo "✗ The following secrets are missing from namespace '$NS':" >&2
  for s in "${MISSING_SECRETS[@]}"; do echo "    - $s" >&2; done
  echo "" >&2
  echo "  Create them yourself in a terminal (see script header for the exact commands)," >&2
  echo "  then re-run this script." >&2
  exit 1
fi

# ── 4. ServiceAccounts ───────────────────────────────────────────────────────
step "Ensuring tool ServiceAccounts..."
for sa in recipe-scraper recipe-publisher opencode-swe-agent web-search; do
  kubectl -n "$NS" get serviceaccount "$sa" >/dev/null 2>&1 \
    || kubectl -n "$NS" create serviceaccount "$sa"
done

# ── 5. Nested Helm dependency (Helm doesn't recurse into file:// subcharts) ──
step "Fetching agent-orchestrator's own subchart dependency (qdrant)..."
helm dependency update "$REPO_ROOT/orchestrator/charts/aktis/charts/agent-orchestrator"

# ── 6. CRDs ──────────────────────────────────────────────────────────────────
step "Applying CRDs..."
# Helm's crds/ dir is install-only; upgrades never touch them. Apply manually
# every time so CRD schema changes are always current. These CRDs are what
# the (separately-released) community-components chart's Tool/Skill/Agent CRs
# depend on.
# Source of truth is the controller's generated bases. This previously globbed
# orchestrator/charts/aktis/charts/core-controller/crds/, which no longer exists
# -- and because an unmatched glob expands to nothing, the loop silently
# applied ZERO CRDs instead of failing. A cluster missing a newly-added CRD
# then crashlooped agent-orchestrator with a bare `404 page not found`.
shopt -s nullglob
crds=("$REPO_ROOT"/orchestrator/controllers/core-controller/config/crd/bases/*.yaml)
shopt -u nullglob
if [[ ${#crds[@]} -eq 0 ]]; then
  die "No CRDs found under orchestrator/controllers/core-controller/config/crd/bases -- refusing to deploy without them."
fi
for crd in "${crds[@]}"; do
  kubectl apply -f "$crd" --server-side --force-conflicts >/dev/null
done
echo "  Applied ${#crds[@]} CRDs."

# ── 7. Hand off to Skaffold ───────────────────────────────────────────────────
case "$MODE" in
  run)
    step "Building images and deploying via Skaffold (one-shot)..."
    "$REPO_ROOT/scripts/cluster/cluster.sh" skaffold run
    ;;
  dev)
    step "Building images and deploying via Skaffold (watch mode)..."
    "$REPO_ROOT/scripts/cluster/cluster.sh" skaffold dev
    ;;
  none)
    step "Prep done. Run './scripts/cluster/cluster.sh skaffold run' (or 'dev') from $REPO_ROOT when ready."
    ;;
esac

echo ""
echo "  Port-forward shortcuts:"
echo "    Open WebUI:  kubectl -n $NS port-forward svc/agent-orchestrator-openwebui 8080:80"
echo "    Invoke API:  kubectl -n $NS port-forward svc/agent-orchestrator-invoke 8081:8081"
