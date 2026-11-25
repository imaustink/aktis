import { defineConfig } from "vitest/config";

/**
 * Spec files that cannot share the cluster with anything: they restart shared
 * services (orchestrator, gateway, NATS, Redis) or assert that nothing at all
 * launched. They run in a second phase that starts once every other file has
 * finished, one at a time (each holds support/locks.ts EXCLUSIVE_LOCK).
 *
 * Keep this list short -- it is the part of the suite that does not overlap.
 * A spec that only needs a slow or silent stub turn paces that one run with a
 * directive (support/resilience.ts pacingDirective) and stays out of it; split
 * a disruptive test into its own file rather than listing a whole file here.
 */
const EXCLUSIVE = [
  "specs/resilience-rollout.e2e.ts",
  "specs/label-durability-rollout.e2e.ts",
  "specs/event-triggers-verification.e2e.ts",
  "specs/identity-redis-restart.e2e.ts",
];

export default defineConfig({
  test: {
    // Registers the global afterEach that lets a FAILED spec file keep its
    // AgentRuns instead of having teardown delete the evidence
    // (support/retain-on-failure.ts).
    setupFiles: ["support/retain-on-failure.ts"],
    // Files run CONCURRENTLY against one cluster; tests within a file stay
    // serial. What makes that safe is in the support helpers, not here: each
    // file sees only the AgentRuns it launched (k8s.ts claimRuns), reads
    // fake-github and fake-sink from its own position (fixtures.ts), deletes
    // only its own credentials (credential-store.ts), forwards on its own
    // local ports (k8s.ts localPortFor), and the few files that share state
    // anyway hold a named lock (support/locks.ts). Most of this suite's time
    // is waiting -- for a pod, an agent, an idle window -- so files overlap
    // well; the cluster, not the host, is the bound on how many.
    //
    // A file waiting on a lock still occupies a worker, so leave headroom.
    maxWorkers: 8,
    maxConcurrency: 1,
    // A real triage turn launches a Job, pulls an image and runs an agent.
    // The per-step budgets in `waitFor` are the meaningful deadlines; this is
    // just a backstop well above them.
    testTimeout: 600_000,
    hookTimeout: 300_000,
    // One retry: cluster-level flakes (image pull backoff, a port-forward
    // dropped by a busy apiserver) are real and not worth a red build. Kept
    // at 1 so a genuinely broken assertion still fails fast rather than
    // being retried into looking intermittent.
    retry: 1,
    // ...and because a retry absorbs the failure, the default output barely
    // shows it happened. This lists every pass-after-retry with the error its
    // failed attempt threw (support/flaky-reporter.ts).
    reporters: ["default", "./support/flaky-reporter.ts"],
    projects: [
      {
        extends: true,
        test: {
          name: "concurrent",
          include: ["specs/**/*.e2e.ts"],
          exclude: EXCLUSIVE,
          sequence: { groupOrder: 0 },
        },
      },
      {
        extends: true,
        test: {
          name: "exclusive",
          include: EXCLUSIVE,
          sequence: { groupOrder: 1 },
        },
      },
    ],
  },
});
