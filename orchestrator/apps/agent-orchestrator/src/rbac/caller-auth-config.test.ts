import { beforeAll, describe, expect, it } from "vitest";
import { SignJWT, createLocalJWKSet, exportJWK, generateKeyPair, type JWTVerifyGetKey } from "jose";
import { buildCallerAuthenticator, type CallerAuthSettings } from "./caller-auth-config.js";
import type { IdentityResolver } from "./types.js";

const ISSUER = "https://idp.example.com";
const AUDIENCE = "https://agent-orchestrator.example";
const STATIC = JSON.stringify({ "owui-token": { subject: "open-webui", roles: ["reader"] } });

function settings(overrides: Partial<CallerAuthSettings> = {}): CallerAuthSettings {
  return {
    callerAuthMode: undefined,
    identityResolverKind: "static",
    staticIdentities: STATIC,
    oidcIssuer: undefined,
    oidcJwksUri: undefined,
    oidcAudience: undefined,
    oidcRolesClaim: "roles",
    ...overrides,
  };
}

function built(result: ReturnType<typeof buildCallerAuthenticator>) {
  if ("error" in result) throw new Error(`unexpected build error: ${result.error}`);
  return result;
}

describe("buildCallerAuthenticator", () => {
  let jwks: JWTVerifyGetKey;
  let privateKey: Awaited<ReturnType<typeof generateKeyPair>>["privateKey"];

  beforeAll(async () => {
    const pair = await generateKeyPair("RS256");
    privateKey = pair.privateKey;
    jwks = createLocalJWKSet({ keys: [{ ...(await exportJWK(pair.publicKey)), kid: "k1", alg: "RS256", use: "sig" }] });
  });

  function oidcToken(sub: string, issuer = ISSUER): Promise<string> {
    return new SignJWT({ roles: ["reader"] })
      .setProtectedHeader({ alg: "RS256", kid: "k1" })
      .setSubject(sub)
      .setIssuer(issuer)
      .setAudience(AUDIENCE)
      .setExpirationTime("5m")
      .sign(privateKey);
  }

  it("defaults to enforce and refuses an unsupported mode instead of quietly not enforcing", () => {
    expect(built(buildCallerAuthenticator(settings(), undefined)).authenticator.mode).toBe("enforce");
    expect(built(buildCallerAuthenticator(settings({ callerAuthMode: "warn" }), undefined)).authenticator.mode).toBe("warn");
    for (const mode of ["off", "Enforce", "disabled"]) {
      expect(buildCallerAuthenticator(settings({ callerAuthMode: mode }), undefined)).toEqual({
        error: expect.stringContaining(`AGENT_CALLER_AUTH="${mode}" is not supported`),
      });
    }
  });

  it("warns loudly in warn mode", () => {
    expect(built(buildCallerAuthenticator(settings({ callerAuthMode: "warn" }), undefined)).warnings).toEqual([
      expect.stringContaining("NOT refused"),
    ]);
  });

  it("static: authenticates exactly the tokens in AGENT_STATIC_IDENTITIES", async () => {
    const { authenticator, warnings } = built(buildCallerAuthenticator(settings(), undefined));
    expect(warnings).toEqual([]);
    expect(await authenticator.authenticate({ bearer: "owui-token" })).toMatchObject({ ok: true, subject: "open-webui" });
    expect(await authenticator.authenticate({ bearer: "other" })).toMatchObject({ ok: false });
  });

  it("static with an empty map warns, naming what will be refused", () => {
    const jwtVerifier: IdentityResolver = { resolve: async () => undefined };
    expect(built(buildCallerAuthenticator(settings({ staticIdentities: undefined }), undefined)).warnings).toEqual([
      expect.stringContaining("every turn will be refused"),
    ]);
    expect(built(buildCallerAuthenticator(settings({ staticIdentities: undefined }), jwtVerifier)).warnings).toEqual([
      expect.stringContaining("every /invoke caller"),
    ]);
  });

  it("oidc: requires issuer and JWKS URI", () => {
    expect(buildCallerAuthenticator(settings({ identityResolverKind: "oidc" }), undefined)).toEqual({
      error: "AGENT_OIDC_ISSUER and AGENT_OIDC_JWKS_URI are required when AGENT_IDENTITY_RESOLVER=oidc",
    });
  });

  it("oidc: verifies JWTs, and falls back to the static map only for the tokens in it (homelab's shape)", async () => {
    const { authenticator } = built(
      buildCallerAuthenticator(
        settings({ identityResolverKind: "oidc", oidcIssuer: ISSUER, oidcJwksUri: "https://idp.example.com/jwks", oidcAudience: AUDIENCE }),
        undefined,
        () => jwks,
      ),
    );
    expect(await authenticator.authenticate({ bearer: await oidcToken("integration-gateway") })).toMatchObject({
      ok: true,
      subject: "integration-gateway",
    });
    expect(await authenticator.authenticate({ bearer: "owui-token" })).toMatchObject({ ok: true, subject: "open-webui" });
    expect(await authenticator.authenticate({ bearer: await oidcToken("x", "https://evil.example.com") })).toMatchObject({ ok: false });
    expect(await authenticator.authenticate({ bearer: "other" })).toMatchObject({ ok: false });
  });

  it("oidc without a static map accepts only verified JWTs", async () => {
    const { authenticator } = built(
      buildCallerAuthenticator(
        settings({ identityResolverKind: "oidc", staticIdentities: undefined, oidcIssuer: ISSUER, oidcJwksUri: "https://idp.example.com/jwks", oidcAudience: AUDIENCE }),
        undefined,
        () => jwks,
      ),
    );
    expect(await authenticator.authenticate({ bearer: "owui-token" })).toMatchObject({ ok: false });
    expect(await authenticator.authenticate({ bearer: await oidcToken("integration-gateway") })).toMatchObject({ ok: true });
  });
});
