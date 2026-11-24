package facade

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// Identity is a resolved caller.
type Identity struct {
	// Subject is the identity as the ENTRY POINT resolved it: openwebui:<id>
	// for chat, an IdP sub for OIDC, a static map entry's subject.
	Subject string
	Roles   []string
	// PerUser is true only when Subject provably identifies ONE human. Set
	// only by the Open WebUI forwarded-user resolver (docs/adr/0031).
	PerUser bool
	// Email, when read from a SIGNED claim. Never used for authorization; it
	// lets the Connections page find this subject (docs/adr/0046).
	Email string
}

// IdentityResolver turns a caller-supplied token into an Identity. It MUST
// fail closed: nil on any verification failure, never a guessed identity.
type IdentityResolver interface {
	Resolve(ctx context.Context, token string) *Identity
}

// StaticIdentityResolver looks a bearer token up in a fixed map. No
// cryptographic verification: dev/test, or a deployment's known service
// callers.
type StaticIdentityResolver struct {
	tokens map[string]Identity
}

func (r *StaticIdentityResolver) Resolve(_ context.Context, token string) *Identity {
	id, ok := r.tokens[token]
	if !ok {
		return nil
	}
	return &id
}

// Len is how many tokens the map holds.
func (r *StaticIdentityResolver) Len() int { return len(r.tokens) }

// LoadStaticIdentities parses AGENT_STATIC_IDENTITIES:
// {"<token>": {"subject": "...", "roles": ["..."]}}. Missing or invalid input
// is an EMPTY map (fail closed); an entry without a string subject and an
// array of roles is skipped, and non-string roles are dropped.
func LoadStaticIdentities(raw string) *StaticIdentityResolver {
	r := &StaticIdentityResolver{tokens: map[string]Identity{}}
	if raw == "" {
		return r
	}
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return r
	}
	for token, value := range parsed {
		var entry map[string]any
		if json.Unmarshal(value, &entry) != nil || entry == nil {
			continue
		}
		subject, ok := entry["subject"].(string)
		if !ok {
			continue
		}
		rawRoles, ok := entry["roles"].([]any)
		if !ok {
			continue
		}
		roles := []string{}
		for _, role := range rawRoles {
			if s, ok := role.(string); ok {
				roles = append(roles, s)
			}
		}
		r.tokens[token] = Identity{Subject: subject, Roles: roles}
	}
	return r
}

// CompositeIdentityResolver tries primary, then fallback. Built for OIDC with
// a static escape hatch for callers that cannot refresh a token (Open WebUI's
// static bearer), so every other caller keeps strict verification.
type CompositeIdentityResolver struct {
	Primary, Fallback IdentityResolver
}

func (r CompositeIdentityResolver) Resolve(ctx context.Context, token string) *Identity {
	if id := r.Primary.Resolve(ctx, token); id != nil {
		return id
	}
	return r.Fallback.Resolve(ctx, token)
}

// OIDCIdentityResolver verifies bearer tokens as JWTs against an issuer's
// JWKS. Any failure — signature, expiry, issuer, audience, a missing sub — is
// nil. A missing or malformed roles claim is NOT a failure: it resolves with
// zero roles, which downstream role checks then deny.
type OIDCIdentityResolver struct {
	Issuer string
	// Audience, when set, must appear in aud.
	Audience string
	// RolesClaim is a dot path, e.g. "roles" or "realm_access.roles".
	RolesClaim string
	Keyfunc    jwt.Keyfunc
}

func (r *OIDCIdentityResolver) Resolve(_ context.Context, token string) *Identity {
	// exp/nbf are verified when present, as jose did for the TS resolver.
	opts := []jwt.ParserOption{jwt.WithIssuer(r.Issuer)}
	if r.Audience != "" {
		opts = append(opts, jwt.WithAudience(r.Audience))
	}
	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, r.Keyfunc, opts...)
	if err != nil || !parsed.Valid {
		return nil
	}
	subject, ok := claims["sub"].(string)
	if !ok || subject == "" {
		return nil
	}
	roles := []string{}
	if list, ok := readClaimPath(claims, r.RolesClaim).([]any); ok {
		for _, role := range list {
			if s, ok := role.(string); ok {
				roles = append(roles, s)
			}
		}
	}
	return &Identity{Subject: subject, Roles: roles}
}

func readClaimPath(payload map[string]any, path string) any {
	var value any = payload
	for _, key := range strings.Split(path, ".") {
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = object[key]
	}
	return value
}

// OpenWebUIForwardedUserResolver verifies Open WebUI's per-request signed
// X-OpenWebUI-User-Jwt (HS256, FORWARD_USER_INFO_HEADER_JWT_SECRET) instead
// of its Authorization bearer, which is one static value shared by every
// user. The subject is namespaced (openwebui:<id>) so it can never collide
// with an IdP subject.
type OpenWebUIForwardedUserResolver struct {
	Secret []byte
	// Roles are this system's RBAC roles, granted to every verified user.
	// Open WebUI's own role claim is a different vocabulary.
	Roles []string
}

func (r *OpenWebUIForwardedUserResolver) Resolve(_ context.Context, token string) *Identity {
	if token == "" {
		return nil
	}
	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) {
		return r.Secret, nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil || !parsed.Valid {
		return nil
	}
	// The payload shape is not a published contract: accept whichever of
	// these is present, in this order (TS: payload.id ?? payload.sub ?? payload.email).
	var rawID any
	for _, key := range []string{"id", "sub", "email"} {
		if v, present := claims[key]; present && v != nil {
			rawID = v
			break
		}
	}
	id, ok := rawID.(string)
	if !ok || id == "" {
		return nil
	}
	identity := &Identity{Subject: "openwebui:" + id, Roles: r.Roles, PerUser: true}
	if email, ok := claims["email"].(string); ok && strings.Contains(email, "@") {
		identity.Email = email
	}
	return identity
}

// CallerAuthResult is what authenticated a caller, or why nothing did. Never
// carries a credential.
type CallerAuthResult struct {
	OK      bool
	Via     string // "forwarded-user" | "bearer"
	Subject string
	Reason  string
}

// CallerAuthenticator is the gate in front of every turn (docs/adr/0053
// follow-up 1):
//
//   - a chat turn carrying a forwarded-user JWT, with a resolver configured
//     for it, is authenticated by that JWT ALONE — it fails even if the bearer
//     would have resolved;
//   - every other turn needs a bearer the static/OIDC map resolves.
//
// Authentication only: who a turn runs AS is decided elsewhere.
type CallerAuthenticator struct {
	Bearer IdentityResolver
	// ForwardedUser is the BARE JWT verifier, never one with side effects
	// (PrincipalRecordingResolver): the engine client resolves the same token
	// again to decide who the turn runs as.
	ForwardedUser IdentityResolver
	// Mode is "enforce" or "warn".
	Mode string
}

func (a *CallerAuthenticator) Authenticate(ctx context.Context, bearer, forwardedUserToken string) CallerAuthResult {
	if forwardedUserToken != "" && a.ForwardedUser != nil {
		if id := a.ForwardedUser.Resolve(ctx, forwardedUserToken); id != nil {
			return CallerAuthResult{OK: true, Via: "forwarded-user", Subject: id.Subject}
		}
		return CallerAuthResult{Reason: "forwarded user JWT did not verify"}
	}
	if bearer == "" {
		return CallerAuthResult{Reason: "no bearer token"}
	}
	if id := a.Bearer.Resolve(ctx, bearer); id != nil {
		return CallerAuthResult{OK: true, Via: "bearer", Subject: id.Subject}
	}
	return CallerAuthResult{Reason: "bearer token not recognized"}
}

// BuildCallerAuthenticator builds the gate from config, or the startup error
// that should stop the pod. jwksFor builds the OIDC key function for a JWKS
// URI; injectable so tests need no network.
func BuildCallerAuthenticator(cfg Config, forwardedUser IdentityResolver, jwksFor func(uri string) (jwt.Keyfunc, error)) (*CallerAuthenticator, []string, error) {
	mode := cfg.CallerAuthMode
	if mode == "" {
		mode = "enforce"
	}
	if mode != "enforce" && mode != "warn" {
		return nil, nil, fmt.Errorf(`AGENT_CALLER_AUTH=%q is not supported: use "enforce" (the default) or "warn"`, mode)
	}

	static := LoadStaticIdentities(cfg.StaticIdentities)
	var bearer IdentityResolver
	if cfg.IdentityResolverKind == "oidc" {
		if cfg.OIDCIssuer == "" || cfg.OIDCJWKSURI == "" {
			return nil, nil, fmt.Errorf("AGENT_OIDC_ISSUER and AGENT_OIDC_JWKS_URI are required when AGENT_IDENTITY_RESOLVER=oidc")
		}
		keyfunc, err := jwksFor(cfg.OIDCJWKSURI)
		if err != nil {
			return nil, nil, fmt.Errorf("AGENT_OIDC_JWKS_URI: %w", err)
		}
		oidc := &OIDCIdentityResolver{Issuer: cfg.OIDCIssuer, Audience: cfg.OIDCAudience, RolesClaim: cfg.OIDCRolesClaim, Keyfunc: keyfunc}
		if static.Len() > 0 {
			bearer = CompositeIdentityResolver{Primary: oidc, Fallback: static}
		} else {
			bearer = oidc
		}
	} else {
		bearer = static
	}

	var warnings []string
	if mode == "warn" {
		warnings = append(warnings, "AGENT_CALLER_AUTH=warn: unauthenticated turns are logged, NOT refused. "+
			"Use only while confirming callers' tokens, then remove it to enforce.")
	}
	if cfg.IdentityResolverKind == "static" && static.Len() == 0 {
		if forwardedUser != nil {
			warnings = append(warnings, "AGENT_STATIC_IDENTITIES is empty: only chat turns with a verified Open WebUI user JWT can authenticate; "+
				"every /invoke caller (integration-gateway included) will be refused.")
		} else {
			warnings = append(warnings, "AGENT_STATIC_IDENTITIES is empty and no other resolver is configured: every turn will be refused.")
		}
	}
	return &CallerAuthenticator{Bearer: bearer, ForwardedUser: forwardedUser, Mode: mode}, warnings, nil
}
