#!/usr/bin/env bash
# custom — bring your own Kubernetes: Apple's `container` cluster, k3d,
# OrbStack, Docker Desktop's, a VM you already run. These scripts do not create
# it or stop it; they only need to know which context it is and how an image
# built on this machine reaches it.
#
# Configured in <repo>/cluster.local.env (gitignored), and ONLY there:
#
#   AKTIS_CLUSTER=custom
#   AKTIS_CLUSTER_CONTEXT=<kube context of a cluster you can throw away>
#   # ...and one of:
#   AKTIS_CLUSTER_REGISTRY=<registry the cluster pulls from, e.g. localhost:5000>
#   AKTIS_CLUSTER_LOAD_CMD='<command that loads a local Docker image; {image} is the tag>'
#
# With neither, images are only `docker build`t -- right when the cluster's
# nodes share this machine's Docker image store (Docker Desktop's Kubernetes).

provider_context() {
  [[ -n "${AKTIS_CLUSTER_CONTEXT:-}" ]] \
    || cluster_die "AKTIS_CLUSTER=custom needs AKTIS_CLUSTER_CONTEXT set in cluster.local.env (docs/local-cluster.md)."
  echo "$AKTIS_CLUSTER_CONTEXT"
}

provider_up() {
  local ctx; ctx="$(provider_context)"
  kubectl --context "$ctx" get --raw /readyz >/dev/null 2>&1 \
    || cluster_die "the cluster behind context '$ctx' is not answering. The custom provider does not start clusters: bring it up, then re-run."
  cluster_ensure_docker
}

provider_down() {
  echo "  the custom provider does not own the cluster's lifecycle; stop it with its own tool."
}

provider_skaffold_args() {
  if [[ -n "${AKTIS_CLUSTER_REGISTRY:-}" ]]; then
    echo "--default-repo=$AKTIS_CLUSTER_REGISTRY"
  else
    # Without a registry there is nothing to push to, and skaffold would
    # otherwise push to any cluster it does not recognise as local. This
    # profile (skaffold.yaml) turns that off; the image reaches the cluster
    # through AKTIS_CLUSTER_LOAD_CMD in provider_build instead.
    echo "--profile=no-push"
  fi
}

provider_build() {
  cluster_docker_build
  # Under skaffold with a registry, the tag already carries the registry and
  # skaffold asked for a push.
  if [[ "${BUILD_FROM_SKAFFOLD:-}" == "1" && "${BUILD_PUSH:-}" == "true" ]]; then
    docker push "$BUILD_TAG"
    return
  fi
  if [[ -n "${AKTIS_CLUSTER_LOAD_CMD:-}" ]]; then
    # {image} is substituted textually; the tag is one skaffold or these
    # scripts generated, never user input.
    bash -c "${AKTIS_CLUSTER_LOAD_CMD//\{image\}/$BUILD_TAG}"
  elif [[ -n "${AKTIS_CLUSTER_REGISTRY:-}" ]]; then
    # An image built outside skaffold is referenced by its bare name
    # (`web-fetch:latest`), which a registry only satisfies if the nodes
    # mirror it. Push it anyway and say so, rather than leave a pod in
    # ImagePullBackOff with nothing naming the cause.
    docker tag "$BUILD_TAG" "$AKTIS_CLUSTER_REGISTRY/$BUILD_TAG"
    docker push "$AKTIS_CLUSTER_REGISTRY/$BUILD_TAG"
    echo "  pushed $AKTIS_CLUSTER_REGISTRY/$BUILD_TAG -- the cluster must resolve the bare name '$BUILD_TAG' to it (a registry mirror), or set AKTIS_CLUSTER_LOAD_CMD." >&2
  fi
}
