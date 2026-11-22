# The local cluster

`scripts/dev-up.sh` (the demo stack) and `e2e/scripts/up.sh` (the e2e stack)
both need a Kubernetes cluster on your machine. Which one is a choice:

| provider | context | when |
| --- | --- | --- |
| `ferry` | `ferry-aktis` | **the default on an Apple-silicon Mac** with [ferry](https://github.com/imaustink/ferry) installed |
| `minikube` | `minikube` | the default everywhere else |
| `kind` | `kind-aktis` | if you prefer it |
| `custom` | yours | any other Kubernetes: Apple's `container` cluster, k3d, OrbStack, Docker Desktop's |

CI is not on this list and is not affected by it: no workflow brings a cluster
up through these scripts.

```sh
./scripts/cluster/cluster.sh up        # start it, point kubectl at it
./scripts/dev-up.sh                    # ...or just this: it starts the cluster too
./e2e/scripts/up.sh && npm run e2e -w e2e
./scripts/cluster/cluster.sh down      # stop it; --purge deletes its data as well
```

To pick a provider other than the default, set `AKTIS_CLUSTER` for one command
or write it down once, in `cluster.local.env` at the repo root (gitignored):

```sh
AKTIS_CLUSTER=kind ./scripts/dev-up.sh

echo 'AKTIS_CLUSTER=minikube' > cluster.local.env
```

## Why ferry

minikube and kind run inside a Linux VM whose memory and CPUs are fixed before
the first pod starts. This stack is ~25 pods plus a Job per agent run, and it
kept outgrowing whatever was chosen: runs stuck `Pending` on a 6GB minikube,
OOM-killed agents, a resilience spec that was flaky only locally. The usual fix
was to guess a bigger number and recreate the cluster.

ferry has nothing to size. The cluster is created in **ferry-shared** mode: a
pod is a container on a *machine*, a Linux node VM that ferry creates to fit
the pods that are pending and removes about a minute after it empties. The
pods' own requests are the size. It also runs no Docker Desktop: the control
plane is native macOS processes, and images are built by `ferry image build`
inside the cluster, so an idle cluster costs a few hundred MiB rather than a
reserved VM.

Install it without starting anything (the scripts create the cluster):

```sh
curl -sfL https://get.ferry.kurpuis.com | FERRY_SKIP_START=1 FERRY_SKIP_SERVICE=1 sh -
```

What the `ferry` provider sets up, so you know what is on your Mac:

- A ferry **profile** named `aktis`: its own state (`~/.ferry-aktis`), ports and
  pod network, so it never collides with another ferry cluster. Run ferry
  against it yourself with `FERRY_PROFILE=aktis ferry status`.
- `machines: true`, `defaultRuntime: ferry-shared`, `durability: process-crash`
  (fast, and fine for a cluster a script recreates). `ferry init` is only run
  when the profile has no config, so `FERRY_PROFILE=aktis ferry config set ...`
  sticks.
- All machines together are capped at ferry's defaults (8 CPUs, 32Gi). On a
  smaller Mac: `FERRY_PROFILE=aktis ferry config set machineLimitMemoryGi 12`.
- A pod that must not share a kernel can still ask for a VM of its own with
  `runtimeClassName: ferry-vm`. Nothing in this repo does.

Things that behave differently from minikube and are worth knowing:

- **Images never touch Docker.** Skaffold tags them
  `localhost:<port>/<name>:<tag>`, where the port is ferry's own registry for
  the profile. `imagePullPolicy` must be `IfNotPresent` (or `Always`), never
  `Never`: a machine pulls from that registry, it does not share an image
  store with anything.
- **The first pod of a kind waits ~15s** for a machine to be created. After
  that pods start as fast as containers do.
- **No Docker daemon is needed**, so Docker Desktop can stay quit.
- **The provider patches ferry's image builder.** ferry v0.15.0 keeps
  BuildKit's state on a filesystem where BuildKit cannot use overlayfs, so one
  of this repo's Node images fills most of the builder's 16 GiB disk and the
  next build fails with `no space left on device`. `ferry_prepare_builder` in
  `scripts/cluster/providers/ferry.sh` recreates the builder pod with a volume
  for that state, which fixes it (and makes a warm rebuild take seconds). It
  checks which snapshotter the builder is on, so it stops doing anything once
  ferry ships the volume itself. `FERRY_PROFILE=aktis ferry image build --stop`
  throws the builder and its cache away if you ever need to.

## Using your own cluster (`custom`)

For anything the built-in providers do not manage. The scripts will not create
or stop it; they need to know which context it is, and how an image built on
your machine gets into it. Both go in `cluster.local.env`:

```sh
AKTIS_CLUSTER=custom
AKTIS_CLUSTER_CONTEXT=my-cluster          # must be a cluster you can throw away

# then ONE of these.
# a registry the cluster pulls from -- images are pushed there:
AKTIS_CLUSTER_REGISTRY=localhost:5000
# or a command that loads a locally built Docker image; {image} is the tag:
AKTIS_CLUSTER_LOAD_CMD='k3d image import {image} -c my-cluster'
```

With neither, images are only `docker build`t, which is right when the nodes
share your Docker image store (Docker Desktop's built-in Kubernetes).

`AKTIS_CLUSTER_CONTEXT` is read from that file only, never from the
environment. The e2e suite creates and deletes objects in whatever context it
is allowed to run against, so the allowed list is hardcoded
(`e2e/support/guard.ts`) and this file is the single, deliberate way to add one
more. Do not point it at a cluster you care about.

A cluster used this way needs a default StorageClass (qdrant, NATS and Redis
claim volumes) and nodes that can run `linux/arm64` or `linux/amd64` images
matching your machine.

## Adding a provider

A provider is one file, `scripts/cluster/providers/<name>.sh`, defining five
functions; `scripts/cluster/lib.sh` documents the contract and `kind.sh` is the
shortest example. If its context is a fixed, throwaway name, add it to
`LOCAL_CLUSTER_CONTEXTS` in `e2e/support/guard.ts` too.

The pieces, for when something needs debugging:

| file | role |
| --- | --- |
| `scripts/cluster/cluster.sh` | the CLI: `up`, `down`, `context`, `provider`, `skaffold ...` |
| `scripts/cluster/lib.sh` | provider selection, the context safety check |
| `scripts/cluster/build-image.sh` | builds one image for the cluster; every skaffold artifact is a `custom` build that runs it |
| `scripts/cluster/providers/*.sh` | what each of those means on ferry, minikube, kind, or your own |

Run skaffold through the wrapper (`./scripts/cluster/cluster.sh skaffold dev`)
rather than bare: the provider supplies flags skaffold needs, such as ferry's
registry as the default repo.
