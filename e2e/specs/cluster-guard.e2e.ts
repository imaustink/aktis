import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { builtInContexts, customClusterContext, fileClusterProvider, hostArch } from "../support/guard.js";

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
 * Deliberately no `requireLocalClusterContext()`: this touches nothing (the
 * context-switch block below stubs kubectl on PATH).
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

/**
 * On an Apple-silicon Mac the local cluster is ferry, and the suite must not
 * wander onto a leftover Docker Desktop-backed `minikube` or `kind-aktis`
 * context just because it is still current. Off the Mac nothing changes.
 */
describe("the built-in contexts allowed on this machine", () => {
  const mac = { platform: "darwin", arch: "arm64" };

  it("is only ferry on an Apple-silicon Mac by default", () => {
    expect(builtInContexts({ ...mac, provider: undefined })).toEqual(["ferry-aktis"]);
    expect(builtInContexts({ ...mac, provider: "" })).toEqual(["ferry-aktis"]);
    expect(builtInContexts({ ...mac, provider: "ferry" })).toEqual(["ferry-aktis"]);
  });

  it("is only the deliberately selected provider's context on a Mac", () => {
    expect(builtInContexts({ ...mac, provider: "minikube" })).toEqual(["minikube"]);
    expect(builtInContexts({ ...mac, provider: "kind" })).toEqual(["kind-aktis"]);
    // custom adds its own context through customClusterContext, nothing here;
    // an unknown name allows nothing rather than everything.
    expect(builtInContexts({ ...mac, provider: "custom" })).toEqual([]);
    expect(builtInContexts({ ...mac, provider: "production" })).toEqual([]);
  });

  it("is every built-in context elsewhere", () => {
    const all = ["ferry-aktis", "minikube", "kind-aktis"];
    expect(builtInContexts({ platform: "linux", arch: "x64", provider: undefined })).toEqual(all);
    expect(builtInContexts({ platform: "linux", arch: "arm64", provider: "ferry" })).toEqual(all);
    expect(builtInContexts({ platform: "darwin", arch: "x64", provider: undefined })).toEqual(all);
  });

  it("judges Apple silicon by the hardware, so Rosetta does not re-allow Docker contexts", () => {
    // x64 Node under Rosetta on an M-series Mac: the sysctl still says arm64.
    expect(hostArch("darwin", "x64", () => "1")).toBe("arm64");
    expect(builtInContexts({ platform: "darwin", arch: hostArch("darwin", "x64", () => "1"), provider: undefined }))
      .toEqual(["ferry-aktis"]);
    // Intel Mac: no hw.optional.arm64 (or 0).
    expect(hostArch("darwin", "x64", () => undefined)).toBe("x64");
    expect(hostArch("darwin", "x64", () => "0")).toBe("x64");
    // Native arm64 and non-Mac never consult sysctl.
    const never = () => {
      throw new Error("sysctl read off the Rosetta path");
    };
    expect(hostArch("darwin", "arm64", never)).toBe("arm64");
    expect(hostArch("linux", "x64", never)).toBe("x64");
    expect(hostArch("linux", "arm64", never)).toBe("arm64");
  });

  it("reads the provider from cluster.local.env", () => {
    expect(fileClusterProvider("AKTIS_CLUSTER=minikube\n")).toBe("minikube");
    expect(fileClusterProvider('export AKTIS_CLUSTER="kind"\n')).toBe("kind");
    expect(fileClusterProvider("# AKTIS_CLUSTER=kind\n")).toBeUndefined();
    expect(fileClusterProvider("")).toBeUndefined();
  });
});

/**
 * The guard's verdict must still bind the LAST kubectl call of a run, not just
 * the first. `current-context` is machine-wide: another terminal or session can
 * select a different cluster mid-run, and the guard is memoized, so re-running
 * it per call proves nothing after the first. A run did exactly this -- kept
 * going against `kind-substrate` after another session switched to it, with a
 * teardown that deletes AgentRuns and Secrets. The defence is that every spawn
 * carries the verified context as `--context`, so a switch is simply ignored.
 *
 * Checked by stubbing `kubectl` at the process boundary: the stub answers
 * `config current-context` from a file the test rewrites, and records every
 * other invocation's arguments.
 */
describe("a mid-run context switch cannot redirect the suite", () => {
  it("keeps passing the verified context after current-context changes", async () => {
    const dir = mkdtempSync(join(tmpdir(), "e2e-kubectl-ctx-"));
    const ctxFile = join(dir, "ctx");
    const calls = join(dir, "calls");
    writeFileSync(ctxFile, "ferry-aktis\n");
    writeFileSync(
      join(dir, "kubectl"),
      `#!/bin/sh\ncase "$*" in\n  *"config current-context"*) cat '${ctxFile}' ;;\n  *) echo "$*" >> '${calls}'; echo '{}' ;;\nesac\n`,
      { mode: 0o755 },
    );
    const original = process.env.PATH;
    process.env.PATH = `${dir}:${original ?? ""}`;
    try {
      // Imported here, after PATH points at the stub, so the guard's one real
      // check reads the stub's answer.
      const { kubectl } = await import("../support/k8s.js");
      await kubectl(["get", "agentruns"]);
      writeFileSync(ctxFile, "kind-substrate\n");
      await kubectl(["delete", "agentrun", "after-the-switch"]);

      const recorded = readFileSync(calls, "utf8").trim().split("\n");
      expect(recorded).toHaveLength(2);
      for (const line of recorded) expect(line).toMatch(/^--context ferry-aktis /);
      expect(recorded[1]).toContain("delete agentrun after-the-switch");
    } finally {
      process.env.PATH = original;
    }
  });
});
