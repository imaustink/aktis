import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

/**
 * The ONLY kubectl contexts these tests may touch: one per built-in local
 * cluster provider (scripts/cluster/providers/, docs/local-cluster.md).
 *
 * Every name here is a cluster that exists only to be destroyed, and each is
 * the context that provider's `provider_context` prints:
 *
 *   ferry-aktis  ferry's `aktis` profile -- its own state, ports and pod
 *                network, isolated from any other ferry cluster on the Mac
 *   minikube     the default minikube profile
 *   kind-aktis   the kind cluster named `aktis`
 *
 * Deliberately a hardcoded list with no environment-variable override. The
 * maintainer's default context is a live cluster running the real deployment,
 * these tests create and delete namespaced objects, and the failure mode of
 * "ran the suite against the wrong cluster" is destructive and not obviously
 * recoverable. An override variable would exist purely to be set by accident.
 */
const LOCAL_CLUSTER_CONTEXTS = ["ferry-aktis", "minikube", "kind-aktis"];

/** Gitignored, per checkout. The same file scripts/cluster/lib.sh reads. */
const CLUSTER_LOCAL_ENV = fileURLToPath(new URL("../../cluster.local.env", import.meta.url));

/**
 * The one way to add a context without editing this file: the `custom`
 * provider, for a cluster the built-in providers do not manage (Apple's
 * `container` cluster, k3d, ...).
 *
 * It is read from `cluster.local.env` and nowhere else -- not the
 * environment -- and only when that file also selects `AKTIS_CLUSTER=custom`.
 * That keeps the property the hardcoded list has: naming a cluster is a
 * deliberate, written-down act in a file that is never committed and never
 * exists in CI, and it names ONE exact context rather than switching the check
 * off.
 */
export function customClusterContext(envFileText: string): string | undefined {
  const values = new Map<string, string>();
  for (const raw of envFileText.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const match = /^(?:export\s+)?([A-Z_][A-Z0-9_]*)=(.*)$/.exec(line);
    if (!match) continue;
    values.set(match[1]!, match[2]!.trim().replace(/^(['"])(.*)\1$/, "$2"));
  }
  if (values.get("AKTIS_CLUSTER") !== "custom") return undefined;
  return values.get("AKTIS_CLUSTER_CONTEXT") || undefined;
}

function allowedContexts(): string[] {
  let text: string;
  try {
    text = readFileSync(CLUSTER_LOCAL_ENV, "utf8");
  } catch {
    return LOCAL_CLUSTER_CONTEXTS;
  }
  const custom = customClusterContext(text);
  return custom ? [...LOCAL_CLUSTER_CONTEXTS, custom] : LOCAL_CLUSTER_CONTEXTS;
}

let verified: string | undefined;

/**
 * Aborts unless kubectl is pointed at a local throwaway cluster, and returns
 * the context it verified. Every spec file calls this at module scope, before
 * any fixture allocates anything, so a misconfigured run fails on import rather
 * than midway through creating objects somewhere it shouldn't.
 *
 * Memoized: the check shells out, and every spec calling it would otherwise
 * pay for it repeatedly in a serial suite.
 *
 * Because it is memoized, the RETURNED name is the guarantee, not the check:
 * every kubectl spawn must pass it as `--context` (support/k8s.ts's
 * `kubectlArgs`). `current-context` is machine-wide state that any other
 * terminal or session can change mid-run, and a memoized "it was fine at
 * import" does not stop the next call from following the switch. That
 * happened: a run kept going against `kind-substrate` after another session
 * selected it, with a teardown that deletes AgentRuns and Secrets.
 */
export function requireLocalClusterContext(): string {
  if (verified) return verified;

  let current: string;
  try {
    current = execFileSync("kubectl", ["config", "current-context"], { encoding: "utf8" }).trim();
  } catch (err) {
    throw new Error(
      `e2e: could not determine the kubectl context (is kubectl installed and configured?): ${
        err instanceof Error ? err.message : String(err)
      }`,
    );
  }

  const allowed = allowedContexts();
  if (!allowed.includes(current)) {
    throw new Error(
      [
        `e2e: refusing to run against kubectl context "${current}".`,
        `These tests create and delete cluster objects and may ONLY run against: ${
          allowed.map((c) => `"${c}"`).join(", ")
        }.`,
        `Start and select the local cluster with:  ./scripts/cluster/cluster.sh up`,
        `(docs/local-cluster.md covers using a cluster of your own.)`,
      ].join("\n"),
    );
  }

  verified = current;
  return current;
}
