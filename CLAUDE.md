# CLAUDE.md

The project instructions for every coding agent live in one place, shared with
GitHub Copilot. Read them, and keep them there rather than here:

@.github/copilot-instructions.md

## The one rule worth repeating: e2e runs on ferry

On an Apple-silicon Mac, local e2e tests run on **ferry** (kube context
`ferry-aktis`), not Docker Desktop, minikube or kind. Use
`./e2e/scripts/up.sh` then `npm run e2e -w e2e`. Don't start Docker Desktop,
don't `docker build` for the cluster, and don't switch kubectl to another
context to get past `e2e/support/guard.ts`. If ferry isn't installed, stop and
ask the user. Details: "Local cluster and e2e tests" in the file above, and
[docs/local-cluster.md](docs/local-cluster.md).
