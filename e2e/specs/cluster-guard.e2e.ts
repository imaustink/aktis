import { describe, expect, it } from "vitest";
import { customClusterContext } from "../support/guard.js";

/**
 * The one door in the context guard that is not a hardcoded name, checked with
 * no cluster.
 *
 * `cluster.local.env` lets a contributor point the suite at a cluster of their
 * own (docs/local-cluster.md). Everything the suite does after that is create
 * and delete objects there, so what this parser ACCEPTS is the whole safety
 * property: it must yield a context only when the file both selects the
 * `custom` provider and names one, and never fall back to "anything goes".
 *
 * Deliberately no `requireLocalClusterContext()`: this touches nothing.
 */
describe("the context guard's custom-cluster door", () => {
  it("yields the named context when the file selects the custom provider", () => {
    expect(customClusterContext("AKTIS_CLUSTER=custom\nAKTIS_CLUSTER_CONTEXT=my-k3d\n")).toBe("my-k3d");
    expect(
      customClusterContext('# mine\nexport AKTIS_CLUSTER="custom"\n\nAKTIS_CLUSTER_CONTEXT=\'apple-container\'\n'),
    ).toBe("apple-container");
  });

  it("yields nothing when a built-in provider is selected, even with a context present", () => {
    // A leftover AKTIS_CLUSTER_CONTEXT from an earlier experiment must not
    // widen the guard once the file is back on ferry/minikube/kind.
    expect(customClusterContext("AKTIS_CLUSTER=ferry\nAKTIS_CLUSTER_CONTEXT=production\n")).toBeUndefined();
    expect(customClusterContext("AKTIS_CLUSTER_CONTEXT=production\n")).toBeUndefined();
  });

  it("yields nothing when custom is selected without a context", () => {
    expect(customClusterContext("AKTIS_CLUSTER=custom\n")).toBeUndefined();
    expect(customClusterContext("AKTIS_CLUSTER=custom\nAKTIS_CLUSTER_CONTEXT=\n")).toBeUndefined();
    expect(customClusterContext("")).toBeUndefined();
  });

  it("ignores a commented-out selection", () => {
    expect(customClusterContext("# AKTIS_CLUSTER=custom\nAKTIS_CLUSTER_CONTEXT=production\n")).toBeUndefined();
  });
});
