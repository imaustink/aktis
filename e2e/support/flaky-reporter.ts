/**
 * Names every test that only passed on its retry, with the error its failed
 * attempt threw, at the end of the run.
 *
 * `retry: 1` (vitest.config.ts) absorbs genuine cluster flakes, and it also
 * hides them: the default reporter prints a pass-after-retry as a green tick
 * with a small "(retry x1)" suffix and never shows the first attempt's error,
 * and the JSON reporter drops the retry entirely. A timing pass over this suite
 * found three such tests reading as merely "slow" -- each was paying for a full
 * failed attempt plus a second one, every run, with nobody seeing why.
 *
 * Listed in vitest.config.ts next to the default reporter. A `--reporter` flag
 * on the command line replaces both, so pass this one again if you use it.
 */

import type { Reporter, TestCase } from "vitest/node";

export default class FlakyReporter implements Reporter {
  private readonly flaky: TestCase[] = [];

  onTestCaseResult(testCase: TestCase): void {
    if (testCase.diagnostic()?.flaky) this.flaky.push(testCase);
  }

  onTestRunEnd(): void {
    if (this.flaky.length === 0) return;
    const lines = [``, `⚠ ${this.flaky.length} test(s) passed ONLY on retry -- each paid for a failed attempt first:`];
    for (const testCase of this.flaky) {
      const seconds = Math.round((testCase.diagnostic()?.duration ?? 0) / 1000);
      lines.push(``, `  ${testCase.module.moduleId.replace(/^.*\/specs\//, "specs/")} > ${testCase.fullName}  (${seconds}s total)`);
      for (const error of testCase.result().errors ?? []) {
        const first = (error.message ?? String(error)).split("\n").slice(0, 4).join("\n      ");
        lines.push(`    first attempt: ${first}`);
        // The message abbreviates its operands (`expected [ Array(1) ] to
        // deeply equal []`); the diff is where the unexpected value -- e.g. a
        // stray run's agent and goal -- is actually spelled out.
        if (error.diff) {
          const diff = error.diff.split("\n").slice(0, 40).join("\n      ");
          lines.push(`      ${diff}`);
        }
      }
    }
    console.log(lines.join("\n"));
  }
}
