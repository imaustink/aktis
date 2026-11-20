import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

/**
 * Named AWS CLI profiles for the agent's own `aws` use.
 *
 * The pod's AWS credentials, when the deployment grants any, come from its
 * ServiceAccount (EKS Pod Identity / IRSA) through the CLI's default
 * credential chain. A task written for a person's laptop says
 * `aws --profile <name>`, and the CLI fails with "The config profile (<name>)
 * could not be found" when that profile isn't in ~/.aws/config, even though
 * the credentials would work. A profile that sets nothing but a region still
 * resolves credentials through the same chain, so declaring the expected names
 * makes `--profile <name>` mean "this pod's own credentials".
 */

const PROFILE_NAME = /^[A-Za-z0-9._-]+$/;

/**
 * Parses a comma-separated profile list (`SWE_AWS_PROFILES`). Names that
 * aren't plain identifiers are dropped rather than written, since one would
 * otherwise be able to inject INI syntax into the config file; they're
 * returned separately so the caller can log them.
 */
export function parseAwsProfiles(raw: string | undefined): { profiles: string[]; rejected: string[] } {
  const profiles: string[] = [];
  const rejected: string[] = [];
  for (const name of (raw ?? "").split(",").map((s) => s.trim())) {
    if (!name) continue;
    if (!PROFILE_NAME.test(name)) rejected.push(name);
    else if (!profiles.includes(name)) profiles.push(name);
  }
  return { profiles, rejected };
}

/** Renders ~/.aws/config: `[default]` plus one region-only section per profile. */
export function renderAwsConfig(profiles: string[], region: string): string {
  const body = region ? `region = ${region}\n` : "";
  const sections = [`[default]\n${body}`];
  for (const name of profiles) {
    if (name !== "default") sections.push(`[profile ${name}]\n${body}`);
  }
  return sections.join("\n");
}

/**
 * Writes `<homeDir>/.aws/config` when there's anything to declare. Returns the
 * path written, or `undefined` when neither profiles nor a region were set.
 */
export async function writeAwsConfig(opts: {
  homeDir: string;
  profiles: string[];
  region: string;
}): Promise<string | undefined> {
  if (opts.profiles.length === 0 && !opts.region) return undefined;
  if (opts.region && !PROFILE_NAME.test(opts.region)) {
    throw new Error(`SWE_AWS_REGION is not a valid region name: ${JSON.stringify(opts.region)}`);
  }
  const dir = join(opts.homeDir, ".aws");
  await mkdir(dir, { recursive: true });
  const path = join(dir, "config");
  await writeFile(path, renderAwsConfig(opts.profiles, opts.region), { mode: 0o600 });
  return path;
}
