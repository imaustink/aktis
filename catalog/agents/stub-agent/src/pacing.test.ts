import { describe, expect, it } from "vitest";
import { isImmediate, readPacing } from "./pacing.js";

describe("readPacing", () => {
  it("defaults to replying immediately, so specs that set nothing are unaffected", () => {
    const p = readPacing({});
    expect(p).toEqual({ narrateForMs: 0, narrateEveryMs: 2000, silentForMs: 0 });
    expect(isImmediate(p)).toBe(true);
  });

  it("reads each knob", () => {
    const p = readPacing({ STUB_NARRATE_FOR_MS: "5000", STUB_NARRATE_EVERY_MS: "500", STUB_SILENT_FOR_MS: "60000" });
    expect(p).toEqual({ narrateForMs: 5000, narrateEveryMs: 500, silentForMs: 60000 });
    expect(isImmediate(p)).toBe(false);
  });

  it("clamps to a ceiling, so a typo cannot hang a suite for hours", () => {
    expect(readPacing({ STUB_SILENT_FOR_MS: "999999999" }).silentForMs).toBe(10 * 60 * 1000);
  });

  it("falls back on junk rather than producing NaN waits", () => {
    for (const raw of ["", "abc", "-1", "NaN"]) {
      expect(readPacing({ STUB_NARRATE_FOR_MS: raw }).narrateForMs).toBe(0);
    }
  });

  it("never yields a zero narration cadence, which would busy-loop", () => {
    expect(readPacing({ STUB_NARRATE_EVERY_MS: "0" }).narrateEveryMs).toBe(2000);
  });

  it("treats a silent-only turn as paced", () => {
    expect(isImmediate(readPacing({ STUB_SILENT_FOR_MS: "1000" }))).toBe(false);
  });
});

describe("readPacing with a per-run directive", () => {
  it("reads the directive out of the turn's text", () => {
    const text = "Steps to reproduce.\n[stub-pacing narrateForMs=120000 narrateEveryMs=5000]\nThanks";
    expect(readPacing({}, text)).toEqual({ narrateForMs: 120000, narrateEveryMs: 5000, silentForMs: 0 });
  });

  it("REPLACES the env pacing, so another spec's CR patch cannot leak into this run", () => {
    const env = { STUB_NARRATE_FOR_MS: "50000", STUB_SILENT_FOR_MS: "90000" };
    expect(readPacing(env, "[stub-pacing silentForMs=1000]")).toEqual({
      narrateForMs: 0,
      narrateEveryMs: 2000,
      silentForMs: 1000,
    });
  });

  it("falls back to the env vars when the turn carries no directive", () => {
    expect(readPacing({ STUB_SILENT_FOR_MS: "1000" }, "a plain goal").silentForMs).toBe(1000);
  });

  it("clamps directive values like env values", () => {
    expect(readPacing({}, "[stub-pacing silentForMs=999999999]").silentForMs).toBe(10 * 60 * 1000);
  });

  it("treats a bare directive as immediate, overriding a paced CR", () => {
    expect(isImmediate(readPacing({ STUB_SILENT_FOR_MS: "1000" }, "[stub-pacing]"))).toBe(true);
  });
});
