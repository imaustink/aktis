#!/usr/bin/env bash
# ferry — https://github.com/imaustink/ferry. The default on Apple silicon.
#
# Why it is the default: nothing is sized up front. minikube and kind run
# inside a Linux VM whose memory and CPUs are fixed before the first pod, and
# this stack kept outgrowing whatever was chosen (see the 6GB note that used to
# live in dev-up.sh). Here pods land on "machines" that ferry creates to fit
# them and removes when they empty, and no Docker Desktop is involved at all:
# images are built by `ferry image build` inside the cluster.
#
# The cluster is its own ferry PROFILE, so it never shares state, ports or a
# pod network with any other ferry cluster on this Mac, and its kube context
# (`ferry-aktis`) is one that exists only to be thrown away.

FERRY_PROFILE_NAME="aktis"
# Every `ferry` call below, and every one skaffold makes through
# build-image.sh, must name the profile: without it ferry acts on the default
# profile, i.e. on somebody's other cluster.
export FERRY_PROFILE="$FERRY_PROFILE_NAME"

FERRY_STATE_DIR="$HOME/.ferry-$FERRY_PROFILE_NAME"
FERRY_RUN_DIR="/tmp/ferry-run-$FERRY_PROFILE_NAME"

provider_context() { echo "ferry-$FERRY_PROFILE_NAME"; }

ferry_api_ready() {
  [[ -f "$FERRY_STATE_DIR/admin.conf" ]] \
    && kubectl --kubeconfig "$FERRY_STATE_DIR/admin.conf" get --raw /readyz >/dev/null 2>&1
}

provider_up() {
  [[ "$(uname -s)" == "Darwin" && "$(uname -m)" == "arm64" ]] \
    || cluster_die "ferry only runs on Apple-silicon Macs. Pick another provider: AKTIS_CLUSTER=minikube|kind|custom (docs/local-cluster.md)."
  command -v ferry >/dev/null 2>&1 \
    || cluster_die "ferry is not installed. Install it with:  curl -sfL https://get.ferry.kurpuis.com | FERRY_SKIP_START=1 FERRY_SKIP_SERVICE=1 sh -"

  if [[ ! -f "$FERRY_STATE_DIR/config.yaml" ]]; then
    # ferry-shared: a pod that names no RuntimeClass is a container on a
    # machine (a Linux node VM ferry sizes to fit and reclaims when idle)
    # rather than a VM of its own -- ~14 MiB a pod instead of ~133 MiB, which
    # is what makes this many-small-services stack cheap. process-crash
    # durability because this cluster is recreated from a script; nothing in
    # it is worth an fsync per etcd commit.
    ferry init --purpose dev --durability process-crash --machines true \
      --default-runtime ferry-shared --yes
  fi

  if ferry_api_ready; then
    echo "  ferry profile '$FERRY_PROFILE_NAME' is already up"
  else
    ferry up
  fi
  ferry_fix_nodepool_host_label

  # `ferry kubeconfig --merge` backs up ~/.kube/config every time it runs, so
  # only merge when the context is missing or stale (a purged cluster comes
  # back with new certificates under the same context name).
  local ctx; ctx="$(provider_context)"
  if ! kubectl --context "$ctx" get --raw /readyz >/dev/null 2>&1; then
    ferry kubeconfig --merge
  fi
}

# ferry v0.15.0's Karpenter cannot make a machine for a pod with a volume.
# Every `ferry-local` PersistentVolume is pinned to the Mac it lives on with
# nodeAffinity `ferry.dev/host In [<node>]`, and ferry's default NodePool never
# says machines carry that label (they do: ferry-node sets it). Karpenter only
# provisions for labels a NodePool declares, so it gives up with
#   incompatible volume requirements, label "ferry.dev/host" does not have known values
# and the pod stays Pending once the existing machines are full. For this stack
# that is Open WebUI and Qdrant -- the stateful pods, exactly when the cluster
# needs to grow.
#
# Declaring the label in the NodePool's template fixes it. ferry re-applies its
# NodePool from a file on every `ferry up`, so this runs after every up. The
# first time it changes the NodePool, Karpenter treats existing machines as
# drifted and replaces them once (their pods reschedule; fine on a dev cluster).
ferry_fix_nodepool_host_label() {
  local kc="$FERRY_STATE_DIR/admin.conf" host current
  host="$(cat "$FERRY_RUN_DIR/node-name" 2>/dev/null || true)"
  [[ -n "$host" ]] || return 0
  kubectl --kubeconfig "$kc" get nodepool default >/dev/null 2>&1 || return 0   # machines off
  current="$(kubectl --kubeconfig "$kc" get nodepool default \
    -o jsonpath='{.spec.template.metadata.labels.ferry\.dev/host}' 2>/dev/null || true)"
  [[ "$current" == "$host" ]] && return 0
  kubectl --kubeconfig "$kc" patch nodepool default --type merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"labels\":{\"ferry.dev/host\":\"$host\"}}}}}" >/dev/null \
    && echo "  NodePool now declares ferry.dev/host=$host, so pods with volumes can get machines (see scripts/cluster/providers/ferry.sh)"
}

provider_down() {
  if [[ "${1:-}" == "--purge" ]]; then ferry down --purge; else ferry down; fi
}

# The port of this profile's ferry-registry: the read-only registry every
# `ferry image build` lands in, which every node (the Mac's and each machine's)
# asks before the real one. Read from the running process rather than
# recomputed, because the port is shifted per profile by a number ferry
# allocates.
ferry_registry_port() {
  local pid port
  pid="$(cat "$FERRY_RUN_DIR/ferry-registry.pid" 2>/dev/null || true)"
  [[ -n "$pid" ]] || cluster_die "ferry-registry is not running for profile '$FERRY_PROFILE_NAME' (is the cluster up? ./scripts/cluster/cluster.sh up)"
  port="$(ps -o args= -p "$pid" 2>/dev/null | sed -n 's/.*--listen [^ ]*:\([0-9][0-9]*\).*/\1/p')"
  [[ -n "$port" ]] || cluster_die "could not read ferry-registry's port from pid $pid"
  echo "$port"
}

ferry_registry() { echo "localhost:$(ferry_registry_port)"; }

provider_skaffold_args() {
  # Skaffold needs somewhere to read a built image's digest back from. With no
  # Docker daemon that has to be a registry, and ferry already runs one:
  # naming it as the default repo makes skaffold tag `localhost:<port>/<name>`
  # and look the digest up there after provider_build has built `<name>`.
  echo "--default-repo=$(ferry_registry)"
}

# ferry's builder is buildkit in a pod VM. As of ferry v0.15.0 buildkit's state
# sits on that pod's overlay root, where buildkit cannot use overlayfs itself
# and falls back to the "native" snapshotter: every Dockerfile step is a full
# copy of the filesystem so far. One of this repo's Node images costs ~12 GiB
# of the builder's 16 GiB disk that way, so the second build on a warm builder
# dies with "no space left on device".
#
# The fix is one volume: with an emptyDir (an ext4 disk of its own on ferry) at
# /var/lib/buildkit, buildkit picks overlayfs. Measured here: the same image
# 30 s cold and 1 s warm, 1.9 GiB for two images instead of 12 GiB for one.
#
# So a builder found on the native snapshotter is recreated from ferry's own
# manifest (its last-applied annotation) with that volume added. ferry reuses
# any builder pod that answers with its certificates, so `ferry image build`
# carries on with the replacement. The check is on the snapshotter, which
# makes this a no-op the day ferry's builder ships with the volume itself.
ferry_builder_kubectl() { kubectl --kubeconfig "$FERRY_STATE_DIR/admin.conf" -n kube-system "$@"; }

ferry_prepare_builder() {
  local state
  state="$(ferry_builder_kubectl exec ferry-builder -- sh -c \
    'if [ -d /var/lib/buildkit/runc-native ]; then echo native; else df -P /var/lib/buildkit | awk "NR==2 {print \$5}"; fi' 2>/dev/null || true)"
  case "$state" in
    native)
      echo "  moving ferry's builder onto the overlayfs snapshotter (see scripts/cluster/providers/ferry.sh)" >&2
      local manifest; manifest="$(mktemp "${TMPDIR:-/tmp}/ferry-builder.XXXXXX")"
      ferry_builder_kubectl apply view-last-applied pod/ferry-builder -o yaml > "$manifest"
      ferry_builder_kubectl patch --local -f "$manifest" --type=json -o yaml -p '[
        {"op":"add","path":"/spec/volumes/-","value":{"name":"buildkit-state","emptyDir":{}}},
        {"op":"add","path":"/spec/containers/0/volumeMounts/-","value":{"name":"buildkit-state","mountPath":"/var/lib/buildkit"}}
      ]' > "$manifest.patched"
      ferry_builder_kubectl delete pod ferry-builder --wait=true >/dev/null
      ferry_builder_kubectl apply -f "$manifest.patched" >/dev/null
      ferry_builder_kubectl wait --for=condition=Ready pod/ferry-builder --timeout=120s >/dev/null
      rm -f "$manifest" "$manifest.patched"
      ;;
    *%)
      # buildkit garbage-collects its cache, but not mid-build. If the disk is
      # nearly full anyway, start over rather than fail the build halfway.
      if (( ${state%\%} > 85 )); then
        echo "  ferry's builder disk is ${state} full; restarting it" >&2
        ferry image build --stop >/dev/null
      fi
      ;;
    *) ;; # no builder yet: ferry creates one for this build, and the next call moves it
  esac
}

provider_build() {
  ferry_prepare_builder
  local args=(-f "$BUILD_DOCKERFILE")
  local a
  for a in ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}; do args+=(--build-arg "$a"); done
  # Nothing to load or push afterwards: the build runs in the cluster and its
  # result is in the registry every node reads.
  ferry image build -t "$BUILD_TAG" "${args[@]}" "$BUILD_CONTEXT_DIR"

  # Under skaffold the tag is `localhost:<port>/<name>:<tag>`, and that one
  # name has to resolve for two different readers of the same registry:
  #
  #   a machine's containerd, pulling `localhost:<port>/<name>:<tag>` through
  #   its mirror, asks for <name> with `?ns=localhost:<port>` -- found only
  #   under the QUALIFIED name, which is what the build above stored;
  #
  #   skaffold, reading the digest back from localhost:<port> on this Mac,
  #   asks for <name> with no `ns` -- found only under the BARE name.
  #
  # So the image is stored under both. The second build is a cache hit: same
  # layers, same manifest digest, a second name. (Pulling by tag alone, with
  # only the bare name stored, fails on machines with a plain "not found" --
  # which is every chart value set from IMAGE_REPO/IMAGE_TAG rather than a
  # digest.)
  local registry bare
  registry="$(ferry_registry)"
  bare="${BUILD_TAG#"$registry"/}"
  if [[ "$bare" != "$BUILD_TAG" ]]; then
    ferry image build --quiet -t "$bare" "${args[@]}" "$BUILD_CONTEXT_DIR"
  fi
}
