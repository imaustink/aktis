#!/usr/bin/env bash
# lib.sh — the local-cluster provider layer. SOURCED, never executed.
#
# Everything that used to say "minikube" by name (scripts/dev-up.sh,
# e2e/scripts/up.sh, skaffold.yaml's builder) now asks this file instead, so
# the local cluster is a choice rather than an assumption. See
# docs/local-cluster.md for the user-facing version.
#
# A provider is one file in providers/ defining:
#
#   provider_context         print the kube context this provider's cluster uses
#   provider_up              create/start the cluster and make that context exist
#   provider_down [--purge]  stop it (--purge: delete it and its data)
#   provider_skaffold_args   print extra `skaffold` flags, one per line
#   provider_build           build an image and make it runnable in the cluster.
#                            Reads BUILD_TAG, BUILD_DOCKERFILE, BUILD_CONTEXT_DIR,
#                            the BUILD_ARGS array, and BUILD_FROM_SKAFFOLD /
#                            BUILD_PUSH (see build-image.sh for the contract).
#
# Selection, first match wins:
#   1. AKTIS_CLUSTER in the environment
#   2. AKTIS_CLUSTER in <repo>/cluster.local.env (gitignored)
#   3. ferry on an Apple-silicon Mac (installed or not -- see below), otherwise minikube
# except `custom`, which only counts when cluster.local.env itself selects it
# (the same rule e2e/support/guard.ts applies).
#
# CI never reaches this file: no workflow starts a local cluster through these
# scripts (validate-crds.yml and core-controller's own e2e each create their
# kind cluster directly).

CLUSTER_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_REPO_ROOT="$(cd "$CLUSTER_LIB_DIR/../.." && pwd)"
CLUSTER_LOCAL_ENV="$CLUSTER_REPO_ROOT/cluster.local.env"

cluster_die() { echo "✗ $*" >&2; exit 1; }

# On Apple silicon the default is ferry whether or not it is installed yet:
# provider_up then stops with the install command. Falling back to minikube
# instead would quietly launch Docker Desktop (cluster_ensure_docker) and put
# the dev and e2e stacks on a cluster nobody chose. Pick minikube/kind
# deliberately with AKTIS_CLUSTER if that is what you want.
cluster_default_provider() {
  if [[ "$(uname -s)" == "Darwin" && "$(uname -m)" == "arm64" ]]; then
    echo ferry
  else
    echo minikube
  fi
}

cluster_load() {
  local from_env="${AKTIS_CLUSTER:-}"
  # The `custom` provider's settings are honoured from the file ONLY. The
  # context one of them names is where these scripts create and delete
  # objects; a stray exported variable must not be able to aim that at a
  # cluster nobody chose. e2e/support/guard.ts reads the same file for the
  # same reason.
  unset AKTIS_CLUSTER AKTIS_CLUSTER_CONTEXT AKTIS_CLUSTER_REGISTRY AKTIS_CLUSTER_LOAD_CMD
  local from_file=""
  if [[ -f "$CLUSTER_LOCAL_ENV" ]]; then
    # shellcheck disable=SC1090
    source "$CLUSTER_LOCAL_ENV"
    from_file="${AKTIS_CLUSTER:-}"
  fi
  AKTIS_CLUSTER="${from_env:-${from_file:-$(cluster_default_provider)}}"
  # Same rule as guard.ts's customClusterContext: a custom context is only
  # trusted when cluster.local.env itself selects AKTIS_CLUSTER=custom.
  if [[ "$AKTIS_CLUSTER" == custom && "$from_file" != custom ]]; then
    cluster_die "AKTIS_CLUSTER=custom must be set in cluster.local.env, not only in the environment (docs/local-cluster.md)."
  fi
  local provider_file="$CLUSTER_LIB_DIR/providers/$AKTIS_CLUSTER.sh"
  [[ -f "$provider_file" ]] || cluster_die "unknown cluster provider '$AKTIS_CLUSTER' (have: $(cluster_providers | tr '\n' ' '))"
  # shellcheck disable=SC1090
  source "$provider_file"
}

cluster_providers() {
  local f
  for f in "$CLUSTER_LIB_DIR"/providers/*.sh; do basename "$f" .sh; done
}

# Fails unless kubectl currently points at the selected provider's cluster.
# The scripts that call this create and delete objects, and the default context
# on a maintainer's machine can be a live cluster.
cluster_require_context() {
  local want current
  want="$(provider_context)"
  current="$(kubectl config current-context 2>/dev/null || true)"
  if [[ "$current" != "$want" ]]; then
    echo "✗ kubectl context is '$current', not '$want' (cluster provider: $AKTIS_CLUSTER). Refusing to touch it." >&2
    echo "  Start and select the local cluster with:  ./scripts/cluster/cluster.sh up" >&2
    exit 1
  fi
}

# Start the cluster and point kubectl at it. Switching the context is loud on
# purpose: skaffold, helm and the e2e suite all act on the current context.
cluster_up() {
  local want
  provider_up
  want="$(provider_context)"
  if [[ "$(kubectl config current-context 2>/dev/null || true)" != "$want" ]]; then
    kubectl config use-context "$want" >/dev/null
    echo "  kubectl context is now '$want'"
  fi
}

# Docker-backed providers share this: Docker Desktop is not running by default
# on a Mac that has moved to ferry.
cluster_ensure_docker() {
  command -v docker >/dev/null 2>&1 || cluster_die "docker is not installed, and the '$AKTIS_CLUSTER' cluster provider builds images with it."
  docker info >/dev/null 2>&1 && return 0
  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "  Docker is not running. Launching Docker Desktop..."
    open -a Docker
    until docker info >/dev/null 2>&1; do sleep 2; done
    echo "  Docker is up."
  else
    cluster_die "the Docker daemon is not running."
  fi
}

# `docker build` from the BUILD_* contract. Used by every provider whose images
# come from a Docker daemon.
cluster_docker_build() {
  local args=(build -t "$BUILD_TAG" -f "$BUILD_DOCKERFILE")
  local a
  for a in ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}; do args+=(--build-arg "$a"); done
  docker "${args[@]}" "$BUILD_CONTEXT_DIR"
}
