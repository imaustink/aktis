import { mkdtemp, readFile, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { parseAwsProfiles, renderAwsConfig, writeAwsConfig } from "./awsConfig.js";

describe("parseAwsProfiles", () => {
  it("splits, trims and de-duplicates a comma-separated list", () => {
    expect(parseAwsProfiles(" platform, bitovi-platform ,,platform")).toEqual({
      profiles: ["platform", "bitovi-platform"],
      rejected: [],
    });
  });

  it("treats unset or empty as no profiles", () => {
    expect(parseAwsProfiles(undefined)).toEqual({ profiles: [], rejected: [] });
    expect(parseAwsProfiles("")).toEqual({ profiles: [], rejected: [] });
  });

  it("rejects names that could inject INI syntax", () => {
    expect(parseAwsProfiles("ok,bad]\n[profile x,with space")).toEqual({
      profiles: ["ok"],
      rejected: ["bad]\n[profile x", "with space"],
    });
  });
});

describe("renderAwsConfig", () => {
  it("writes a region-only default and one section per profile", () => {
    expect(renderAwsConfig(["platform"], "us-east-1")).toBe(
      "[default]\nregion = us-east-1\n\n[profile platform]\nregion = us-east-1\n",
    );
  });

  it("doesn't duplicate [default] when it's listed as a profile", () => {
    expect(renderAwsConfig(["default"], "us-east-1")).toBe("[default]\nregion = us-east-1\n");
  });

  it("declares empty profiles when no region is set", () => {
    expect(renderAwsConfig(["platform"], "")).toBe("[default]\n\n[profile platform]\n");
  });
});

describe("writeAwsConfig", () => {
  it("writes ~/.aws/config readable only by the owner", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "aws-home-"));
    const path = await writeAwsConfig({ homeDir, profiles: ["platform"], region: "us-east-1" });
    expect(path).toBe(join(homeDir, ".aws", "config"));
    expect(await readFile(path!, "utf8")).toContain("[profile platform]");
    expect((await stat(path!)).mode & 0o777).toBe(0o600);
  });

  it("writes nothing when there are no profiles and no region", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "aws-home-"));
    expect(await writeAwsConfig({ homeDir, profiles: [], region: "" })).toBeUndefined();
  });

  it("refuses a region that isn't a plain identifier", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "aws-home-"));
    await expect(writeAwsConfig({ homeDir, profiles: [], region: "us-east-1\n[x]" })).rejects.toThrow(
      /SWE_AWS_REGION/,
    );
  });
});
