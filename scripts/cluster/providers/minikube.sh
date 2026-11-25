#!/usr/bin/env bash
# minikube — the original local cluster, and the default wherever ferry cannot
# run (Linux, Intel Macs). On an Apple-silicon Mac it is only used when
# selected deliberately with AKTIS_CLUSTER=minikube.

# Sized up front, because minikube has to be. 6GB/4 CPUs holds the demo stack;
# the e2e stack is tight in it (see values-e2e.yaml's notes on request sizes).
# Only applied when the profile is created: flags are ignored on a restart.
MINIKUBE_MEMORY="${MINIKUBE_MEMORY:-6144}"
MINIKUBE_CPUS="${MINIKUBE_CPUS:-4}"

provider_context() { echo "minikube"; }

provider_up() {
  command -v minikube >/dev/null 2>&1 || cluster_die "minikube is not installed."
  cluster_ensure_docker
  minikube start --memory="$MINIKUBE_MEMORY" --cpus="$MINIKUBE_CPUS" --driver=docker
}

provider_down() {
  if [[ "${1:-}" == "--purge" ]]; then minikube delete; else minikube stop; fi
}

# None: skaffold recognises the `minikube` context as a local cluster by
# itself -- it never pushes, and it points every build at minikube's own
# Docker daemon.
provider_skaffold_args() { :; }

provider_build() {
  if [[ "${BUILD_FROM_SKAFFOLD:-}" == "1" ]]; then
    # skaffold has already exported minikube's docker-env into this process,
    # so a plain build lands in the daemon the kubelet reads.
    cluster_docker_build
    return
  fi
  local args=(image build -t "$BUILD_TAG" -f "$BUILD_DOCKERFILE")
  local a
  for a in ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}; do args+=(--build-opt "build-arg=$a"); done
  minikube "${args[@]}" "$BUILD_CONTEXT_DIR"
}
