import { describe, expect, it } from "vitest";
import { loadConfig } from "./config.js";

describe("loadConfig event sources (ADR 0048)", () => {
  it("defaults the CRD group/version; namespace has no default", () => {
    const config = loadConfig({});
    expect(config.crdNamespace).toBe("");
    expect(config.crdGroup).toBe("core.aktis.ai");
    expect(config.crdVersion).toBe("v1alpha1");
  });

  it("reads CRD_NAMESPACE / CRD_GROUP / CRD_VERSION", () => {
    const config = loadConfig({
      CRD_NAMESPACE: "agents",
      CRD_GROUP: "core.aktis.ai",
      CRD_VERSION: "v1beta1",
    });
    expect([config.crdNamespace, config.crdGroup, config.crdVersion]).toEqual(["agents", "core.aktis.ai", "v1beta1"]);
  });
});

describe("loadConfig RUN_EVENT_MAX_DEPTH (docs/adr/0052 §1)", () => {
  it("defaults to 12 and reads a positive integer", () => {
    expect(loadConfig({}).runEventMaxDepth).toBe(12);
    expect(loadConfig({ RUN_EVENT_MAX_DEPTH: "5" }).runEventMaxDepth).toBe(5);
  });

  it.each(["0", "-3", "2.5", "twelve", ""])("falls back to the default for %j rather than disabling the cap", (raw) => {
    expect(loadConfig({ RUN_EVENT_MAX_DEPTH: raw }).runEventMaxDepth).toBe(12);
  });
});

describe("loadConfig GitHub host resolution", () => {
  it("defaults the OAuth host to github.com, so existing deployments are unaffected", () => {
    const config = loadConfig({});
    expect(config.githubBaseUrl).toBe("https://github.com");
    expect(config.githubApiUrl).toBe("https://api.github.com");
  });

  it("reads GITHUB_BASE_URL independently of GITHUB_API_URL", () => {
    // GitHub Enterprise Server splits these: the REST API lives under
    // /api/v3 while the OAuth routes stay on the bare host. A single value
    // cannot serve both, which is why this is a separate setting rather than
    // something derived from githubApiUrl.
    const config = loadConfig({
      GITHUB_API_URL: "https://ghe.example.com/api/v3",
      GITHUB_BASE_URL: "https://ghe.example.com",
    });
    expect(config.githubApiUrl).toBe("https://ghe.example.com/api/v3");
    expect(config.githubBaseUrl).toBe("https://ghe.example.com");
  });

  it("does not infer the OAuth host from GITHUB_API_URL", () => {
    // Setting only the API URL leaves the OAuth host at its default. Guessing
    // (e.g. stripping "/api/v3") would silently send a GHES deployment's users
    // to a host nobody configured, and it cannot be right for both GHES and
    // the github.com/api.github.com split.
    const config = loadConfig({ GITHUB_API_URL: "https://ghe.example.com/api/v3" });
    expect(config.githubBaseUrl).toBe("https://github.com");
  });
});

describe("loadConfig GATEWAY_HELD_EVENT_STATE_CHECK_INTERVAL_MS (docs/adr/0055 §2)", () => {
  it("is unset by default (the gateway's default applies) and reads a non-negative number", () => {
    expect(loadConfig({}).heldEventStateCheckIntervalMs).toBeUndefined();
    expect(loadConfig({ GATEWAY_HELD_EVENT_STATE_CHECK_INTERVAL_MS: "60000" }).heldEventStateCheckIntervalMs).toBe(60000);
    expect(loadConfig({ GATEWAY_HELD_EVENT_STATE_CHECK_INTERVAL_MS: "0" }).heldEventStateCheckIntervalMs).toBe(0);
    expect(loadConfig({ GATEWAY_HELD_EVENT_STATE_CHECK_INTERVAL_MS: "soon" }).heldEventStateCheckIntervalMs).toBeUndefined();
  });
});
