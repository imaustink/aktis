import { describe, expect, it, vi } from "vitest";
import { CallerAuthenticator } from "./caller-authenticator.js";
import type { IdentityResolver } from "./types.js";

/** Resolves exactly the listed tokens. */
function resolverFor(tokens: Record<string, string>): IdentityResolver & { resolve: ReturnType<typeof vi.fn> } {
  return {
    resolve: vi.fn(async (token: string) => (token in tokens ? { subject: tokens[token]!, roles: [] } : undefined)),
  };
}

describe("CallerAuthenticator", () => {
  it("accepts a bearer token the map resolves", async () => {
    const auth = new CallerAuthenticator(resolverFor({ "gw-token": "client-integration-gateway" }), undefined);
    expect(await auth.authenticate({ bearer: "gw-token" })).toEqual({
      ok: true,
      via: "bearer",
      subject: "client-integration-gateway",
    });
  });

  it("refuses an unknown bearer and an absent one", async () => {
    const bearer = resolverFor({ "gw-token": "gw" });
    const auth = new CallerAuthenticator(bearer, undefined);
    expect(await auth.authenticate({ bearer: "nope" })).toEqual({ ok: false, reason: "bearer token not recognized" });
    expect(await auth.authenticate({ bearer: "" })).toEqual({ ok: false, reason: "no bearer token" });
    // An absent bearer never reaches the resolver: a static map with an ""
    // key must not authenticate a request that sent no token at all.
    expect(bearer.resolve).toHaveBeenCalledTimes(1);
  });

  it("authenticates a chat turn by a verified forwarded-user JWT alone, as the removed engine did", async () => {
    const auth = new CallerAuthenticator(resolverFor({}), resolverFor({ "good-jwt": "openwebui:alice" }));
    expect(await auth.authenticate({ bearer: "", forwardedUserToken: "good-jwt" })).toEqual({
      ok: true,
      via: "forwarded-user",
      subject: "openwebui:alice",
    });
  });

  it("refuses a forwarded-user JWT that does not verify, even when the bearer would have resolved", async () => {
    const bearer = resolverFor({ "owui-token": "open-webui" });
    const auth = new CallerAuthenticator(bearer, resolverFor({ "good-jwt": "openwebui:alice" }));
    expect(await auth.authenticate({ bearer: "owui-token", forwardedUserToken: "forged" })).toEqual({
      ok: false,
      reason: "forwarded user JWT did not verify",
    });
    expect(bearer.resolve).not.toHaveBeenCalled();
  });

  it("falls back to the bearer when a JWT arrives but no JWT resolver is configured", async () => {
    const auth = new CallerAuthenticator(resolverFor({ "owui-token": "open-webui" }), undefined);
    expect(await auth.authenticate({ bearer: "owui-token", forwardedUserToken: "anything" })).toMatchObject({
      ok: true,
      via: "bearer",
    });
    expect(await auth.authenticate({ bearer: "", forwardedUserToken: "anything" })).toMatchObject({ ok: false });
  });

  it("defaults to enforce", () => {
    expect(new CallerAuthenticator(resolverFor({}), undefined).mode).toBe("enforce");
  });
});
