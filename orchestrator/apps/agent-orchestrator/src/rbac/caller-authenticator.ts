import type { IdentityResolver } from "./types.js";

/**
 * `enforce` refuses an unauthenticated turn with a 401. `warn` logs that it
 * would have, and lets the turn run: a rollout aid for a deployment that has
 * not yet confirmed every caller's token is in its identity map. Never a
 * steady state.
 */
export type CallerAuthMode = "enforce" | "warn";

/** What authenticated the caller, or why nothing did. Never carries a token. */
export type CallerAuthResult =
  | { ok: true; via: "forwarded-user" | "bearer"; subject: string }
  | { ok: false; reason: string };

export interface CallerCredentials {
  /** The `Authorization: Bearer` value, or "" when absent or not a bearer. */
  bearer: string;
  /**
   * Open WebUI's per-request signed user JWT (`X-OpenWebUI-User-Jwt`). Only
   * the chat route reads it; `/invoke` callers authenticate by bearer alone.
   */
  forwardedUserToken?: string;
}

/**
 * Authenticates whoever is starting a turn (docs/adr/0053 follow-up 1).
 *
 * This is the gate the removed in-process engine's `resolveIdentity` node
 * applied, restored with the same rule:
 *
 * - A chat turn carrying a forwarded-user JWT, with a resolver configured for
 *   it, is authenticated by that JWT alone. It is HMAC-signed with a secret
 *   only Open WebUI holds, and it fails the turn if it does not verify, even
 *   when the bearer would have resolved.
 * - Every other turn must present a bearer token the identity map (static or
 *   OIDC, `AGENT_IDENTITY_RESOLVER`) resolves.
 *
 * Authentication only. Who a turn runs AS is decided elsewhere and is
 * unchanged: the forwarded-user JWT or the signed sender assertion, else the
 * engine's own token subject (see temporal-engine.ts's `start` for why a
 * bearer-resolved identity is deliberately not forwarded).
 */
export class CallerAuthenticator {
  constructor(
    private readonly bearerResolver: IdentityResolver,
    /**
     * Verifies the forwarded-user JWT. Pass the bare verifier, not a
     * resolver with side effects (PrincipalRecordingResolver): the engine
     * adapter resolves the same token again to decide who the turn runs as.
     */
    private readonly forwardedUserResolver: IdentityResolver | undefined,
    readonly mode: CallerAuthMode = "enforce",
  ) {}

  async authenticate(credentials: CallerCredentials): Promise<CallerAuthResult> {
    if (credentials.forwardedUserToken && this.forwardedUserResolver) {
      const identity = await this.forwardedUserResolver.resolve(credentials.forwardedUserToken);
      return identity
        ? { ok: true, via: "forwarded-user", subject: identity.subject }
        : { ok: false, reason: "forwarded user JWT did not verify" };
    }
    if (!credentials.bearer) return { ok: false, reason: "no bearer token" };
    const identity = await this.bearerResolver.resolve(credentials.bearer);
    return identity
      ? { ok: true, via: "bearer", subject: identity.subject }
      : { ok: false, reason: "bearer token not recognized" };
  }
}
