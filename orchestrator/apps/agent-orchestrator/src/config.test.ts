import { afterEach, describe, expect, it, vi } from "vitest";

/**
 * `config` reads the environment once at module load, so each case loads a
 * fresh copy with vi.resetModules after setting env.
 */
async function loadConfig(env: Record<string, string | undefined>) {
  vi.resetModules();
  for (const [key, value] of Object.entries(env)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  return (await import("./config.js")).config;
}

describe("engine selection (docs/adr/0053)", () => {
  afterEach(() => {
    delete process.env.AGENT_ENGINE;
    delete process.env.AGENT_TEMPORAL_ENGINE_URL;
  });

  // index.ts refuses to start on anything but "temporal" or unset, so the
  // value must reach it as set -- including a stale "langgraph".
  it("keeps AGENT_ENGINE as set so startup can refuse a removed engine", async () => {
    expect((await loadConfig({ AGENT_ENGINE: "langgraph" })).requestedEngine).toBe("langgraph");
    expect((await loadConfig({ AGENT_ENGINE: "temporal" })).requestedEngine).toBe("temporal");
  });

  it("treats an empty AGENT_ENGINE or engine URL as unset", async () => {
    const config = await loadConfig({ AGENT_ENGINE: "", AGENT_TEMPORAL_ENGINE_URL: "" });
    expect(config.requestedEngine).toBeUndefined();
    expect(config.temporalEngineUrl).toBeUndefined();
  });
});
