#!/usr/bin/env bash
# build-image.sh — build one image so the local cluster can run it.
#
# Two callers, one contract:
#
#   skaffold (every artifact in skaffold.yaml is a `custom` build that runs
#   this). Skaffold passes the tag as $IMAGE, the build context as
#   $BUILD_CONTEXT and whether it expects a push as $PUSH_IMAGE; the Dockerfile
#   and build args come as flags from the artifact's buildCommand:
#
#       build-image.sh -f Dockerfile [--build-arg K=V]...
#
#   a script that needs an image skaffold does not build (e2e/scripts/up.sh's
#   web-fetch), which names the tag and context itself:
#
#       build-image.sh -t web-fetch:latest -f catalog/tools/web-fetch/Dockerfile .
#
# HOW the image is built and reaches the cluster is the provider's business
# (providers/*.sh: `ferry image build`, minikube's daemon, `kind load`, a
# registry push). This file only normalises the arguments.

set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
cluster_load

BUILD_TAG=""
BUILD_DOCKERFILE=""
BUILD_CONTEXT_DIR=""
BUILD_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t) BUILD_TAG="$2"; shift 2 ;;
    -f) BUILD_DOCKERFILE="$2"; shift 2 ;;
    --build-arg) BUILD_ARGS+=("$2"); shift 2 ;;
    -*) cluster_die "build-image.sh: unknown flag $1" ;;
    *) BUILD_CONTEXT_DIR="$1"; shift ;;
  esac
done

BUILD_FROM_SKAFFOLD=0
if [[ -z "$BUILD_TAG" ]]; then
  [[ -n "${IMAGE:-}" ]] || cluster_die "build-image.sh: no -t <tag>, and no \$IMAGE from skaffold."
  BUILD_TAG="$IMAGE"
  BUILD_FROM_SKAFFOLD=1
fi
BUILD_PUSH="${PUSH_IMAGE:-false}"
BUILD_CONTEXT_DIR="${BUILD_CONTEXT_DIR:-${BUILD_CONTEXT:-.}}"
[[ -n "$BUILD_DOCKERFILE" ]] || cluster_die "build-image.sh: -f <Dockerfile> is required."
# Skaffold runs the command from the artifact's context, and a Dockerfile path
# in skaffold.yaml is relative to that context.
if [[ "$BUILD_DOCKERFILE" != /* && "$BUILD_FROM_SKAFFOLD" == "1" ]]; then
  BUILD_DOCKERFILE="$BUILD_CONTEXT_DIR/$BUILD_DOCKERFILE"
fi

export BUILD_TAG BUILD_DOCKERFILE BUILD_CONTEXT_DIR BUILD_FROM_SKAFFOLD BUILD_PUSH
provider_build
