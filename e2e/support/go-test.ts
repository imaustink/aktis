import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

/** The repository root, from this file's location. */
export const REPO_ROOT = fileURLToPath(new URL("../../", import.meta.url));

/**
 * Runs `go test -run <pattern>` in a Go module of this repo and requires every
 * test in `mustPass` to have PASSED — not skipped, not filtered out by a typo
 * in the pattern. A Go test that skips because its environment variable did not
 * arrive must read as a failure here, or the spec would be green while checking
 * nothing.
 *
 * Used where the product code a spec has to agree with is Go (docs/adr/0055):
 * the spec supplies what only it can (minted tokens, a port-forward), and the
 * Go test exercises the real implementation.
 *
 * Needs `go` on PATH, at the version in the module's go.mod.
 */
export function runGoTests(options: {
  /** Module-relative directory of the package, e.g. "./internal/facade". */
  pkg: string;
  /** The module's directory, repo-relative. */
  module: string;
  pattern: string;
  mustPass: string[];
  env?: Record<string, string>;
  timeoutMs?: number;
}): string {
  const run = spawnSync("go", ["test", "-count=1", "-v", "-run", options.pattern, options.pkg], {
    cwd: `${REPO_ROOT}${options.module}`,
    env: { ...process.env, ...options.env },
    encoding: "utf8",
    timeout: options.timeoutMs ?? 300_000,
  });
  if (run.error) throw new Error(`could not run go (is it on PATH?): ${run.error.message}`);
  const output = `${run.stdout ?? ""}${run.stderr ?? ""}`;
  if (run.status !== 0) throw new Error(`go test ${options.pattern} failed (exit ${run.status}):\n${output}`);
  for (const test of options.mustPass) {
    if (!output.includes(`--- PASS: ${test} `)) throw new Error(`${test} did not pass (skipped or not run):\n${output}`);
  }
  return output;
}
