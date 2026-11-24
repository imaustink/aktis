import { describe, expect, it } from "vitest";
import { DEFAULT_RUN_SETTINGS, PUBLISH_RETRY_MAX_MS, queueHoldTimeoutMs, type RunSettings } from "./model.js";

describe("queueHoldTimeoutMs (docs/adr/0055 §2)", () => {
  const HOUR = 60 * 60 * 1000;

  it("outlasts a held event publishing for its whole retry window, then a run doing the same", () => {
    const s = DEFAULT_RUN_SETTINGS;
    // The old fixed 9 hours was shorter than one 24-hour publish window.
    expect(queueHoldTimeoutMs(s)).toBeGreaterThan(2 * (s.publishRetryWindowMs + PUBLISH_RETRY_MAX_MS) + 10 * s.turnTimeoutMs + HOUR);
    expect(queueHoldTimeoutMs(s)).toBeGreaterThan(48 * HOUR);
  });

  it("grows with every setting a live holder can spend", () => {
    const base = queueHoldTimeoutMs(DEFAULT_RUN_SETTINGS);
    const longer = (o: Partial<RunSettings>) => queueHoldTimeoutMs({ ...DEFAULT_RUN_SETTINGS, ...o });
    expect(longer({ publishRetryWindowMs: DEFAULT_RUN_SETTINGS.publishRetryWindowMs + HOUR })).toBe(base + 2 * HOUR);
    expect(longer({ turnTimeoutMs: DEFAULT_RUN_SETTINGS.turnTimeoutMs + 1 })).toBeGreaterThan(base);
    expect(longer({ linkWaitTimeoutMs: DEFAULT_RUN_SETTINGS.linkWaitTimeoutMs + 1 })).toBeGreaterThan(base);
  });
});
