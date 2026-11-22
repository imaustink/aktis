import { createRemoteJWKSet, type JWTVerifyGetKey } from "jose";
import { CallerAuthenticator, type CallerAuthMode } from "./caller-authenticator.js";
import { CompositeIdentityResolver } from "./composite-identity-resolver.js";
import { OidcIdentityResolver } from "./oidc-identity-resolver.js";
import { StaticIdentityResolver, loadStaticIdentitiesFromEnv } from "./static-identity-resolver.js";
import type { IdentityResolver } from "./types.js";

export interface CallerAuthSettings {
  callerAuthMode: string | undefined;
  identityResolverKind: "static" | "oidc";
  staticIdentities: string | undefined;
  oidcIssuer: string | undefined;
  oidcJwksUri: string | undefined;
  oidcAudience: string | undefined;
  oidcRolesClaim: string;
}

export type CallerAuthBuild =
  | {
      authenticator: CallerAuthenticator;
      /** Startup warnings worth a loud log line, but not worth refusing to start. */
      warnings: string[];
    }
  | { error: string };

/**
 * Builds the turn authentication gate from config, or the startup error that
 * should stop the pod. `forwardedUserResolver` is the bare Open WebUI JWT
 * verifier, when `AGENT_OPENWEBUI_USER_JWT_SECRET` is set.
 *
 * The bearer resolver is built the way the removed in-process engine built it
 * (docs/adr/0053), so deployment values that worked then work unchanged:
 * `oidc` verifies JWTs against the issuer's JWKS and falls back to the static
 * map for the tokens in it (callers like Open WebUI that cannot refresh an
 * OIDC token); `static` uses the map alone.
 */
export function buildCallerAuthenticator(
  settings: CallerAuthSettings,
  forwardedUserResolver: IdentityResolver | undefined,
  jwksFor: (uri: string) => JWTVerifyGetKey = (uri) => createRemoteJWKSet(new URL(uri)),
): CallerAuthBuild {
  const mode = settings.callerAuthMode ?? "enforce";
  if (mode !== "enforce" && mode !== "warn") {
    return { error: `AGENT_CALLER_AUTH=${JSON.stringify(mode)} is not supported: use "enforce" (the default) or "warn"` };
  }

  const staticIdentities = loadStaticIdentitiesFromEnv(settings.staticIdentities);
  let bearerResolver: IdentityResolver;
  if (settings.identityResolverKind === "oidc") {
    if (!settings.oidcIssuer || !settings.oidcJwksUri) {
      return { error: "AGENT_OIDC_ISSUER and AGENT_OIDC_JWKS_URI are required when AGENT_IDENTITY_RESOLVER=oidc" };
    }
    const oidc = new OidcIdentityResolver({
      issuer: settings.oidcIssuer,
      audience: settings.oidcAudience,
      rolesClaim: settings.oidcRolesClaim,
      jwks: jwksFor(settings.oidcJwksUri),
    });
    bearerResolver = staticIdentities.size > 0 ? new CompositeIdentityResolver(oidc, new StaticIdentityResolver(staticIdentities)) : oidc;
  } else {
    bearerResolver = new StaticIdentityResolver(staticIdentities);
  }

  const warnings: string[] = [];
  if (mode === "warn") {
    warnings.push(
      "AGENT_CALLER_AUTH=warn: unauthenticated turns are logged, NOT refused. " +
        "Use only while confirming callers' tokens, then remove it to enforce.",
    );
  }
  if (settings.identityResolverKind === "static" && staticIdentities.size === 0) {
    warnings.push(
      forwardedUserResolver
        ? "AGENT_STATIC_IDENTITIES is empty: only chat turns with a verified Open WebUI user JWT can authenticate; " +
            "every /invoke caller (integration-gateway included) will be refused."
        : "AGENT_STATIC_IDENTITIES is empty and no other resolver is configured: every turn will be refused.",
    );
  }
  return { authenticator: new CallerAuthenticator(bearerResolver, forwardedUserResolver, mode as CallerAuthMode), warnings };
}
