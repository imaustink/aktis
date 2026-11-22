#!/usr/bin/env bash
# kind — a cluster named `aktis` (context `kind-aktis`), so it cannot be
# mistaken for, or collide with, the throwaway kind clusters CI and
# core-controller's own e2e create.

KIND_CLUSTER_NAME="aktis"

provider_context() { echo "kind-$KIND_CLUSTER_NAME"; }

provider_up() {
  command -v kind >/dev/null 2>&1 || cluster_die "kind is not installed."
  cluster_ensure_docker
  if ! kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER_NAME"; then
    kind create cluster --name "$KIND_CLUSTER_NAME"
  fi
  # A stopped Docker Desktop leaves the node container stopped too.
  docker start "$KIND_CLUSTER_NAME-control-plane" >/dev/null 2>&1 || true
  kind export kubeconfig --name "$KIND_CLUSTER_NAME" >/dev/null
}

provider_down() {
  if [[ "${1:-}" == "--purge" ]]; then
    kind delete cluster --name "$KIND_CLUSTER_NAME"
  else
    docker stop "$KIND_CLUSTER_NAME-control-plane" >/dev/null
  fi
}

# None: skaffold recognises `kind-*` contexts as local -- it never pushes, and
# it `kind load`s every image it built before deploying.
provider_skaffold_args() { :; }

provider_build() {
  cluster_docker_build
  # skaffold loads its own artifacts; anything built outside it has to be
  # loaded by hand or the kubelet cannot see it.
  if [[ "${BUILD_FROM_SKAFFOLD:-}" != "1" ]]; then
    kind load docker-image "$BUILD_TAG" --name "$KIND_CLUSTER_NAME"
  fi
}
