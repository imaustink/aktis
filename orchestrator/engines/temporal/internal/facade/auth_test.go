package facade

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

// Ported from rbac/*.test.ts: static, composite, OIDC and Open WebUI
// forwarded-user resolvers, the caller authenticator and its config.

var bg = context.Background()

// ── static ─────────────────────────────────────────────────────────────────

func TestLoadStaticIdentitiesFailsClosedOnMissingOrInvalidInput(t *testing.T) {
	for _, raw := range []string{"", "not json", "[]", "null", `"x"`} {
		require.Zero(t, LoadStaticIdentities(raw).Len(), raw)
	}
}

func TestLoadStaticIdentitiesParsesAValidMap(t *testing.T) {
	r := LoadStaticIdentities(`{"tok-1": {"subject": "alice", "roles": ["reader"]}}`)
	require.Equal(t, &Identity{Subject: "alice", Roles: []string{"reader"}}, r.Resolve(bg, "tok-1"))
}

func TestLoadStaticIdentitiesSkipsMalformedEntries(t *testing.T) {
	r := LoadStaticIdentities(`{
		"tok-1": {"subject": "alice"},
		"tok-2": {"subject": "bob", "roles": ["x", 3, null]},
		"tok-3": {"subject": 7, "roles": []},
		"tok-4": "not an object"
	}`)
	require.Nil(t, r.Resolve(bg, "tok-1"))
	// Non-string roles are dropped, the entry kept.
	require.Equal(t, &Identity{Subject: "bob", Roles: []string{"x"}}, r.Resolve(bg, "tok-2"))
	require.Nil(t, r.Resolve(bg, "tok-3"))
	require.Nil(t, r.Resolve(bg, "tok-4"))
	require.Equal(t, 1, r.Len())
}

func TestStaticResolverFailsClosedForAnUnknownToken(t *testing.T) {
	require.Nil(t, LoadStaticIdentities("").Resolve(bg, "nope"))
	require.Nil(t, LoadStaticIdentities(`{"tok-1": {"subject": "a", "roles": []}}`).Resolve(bg, ""))
}

// ── composite ──────────────────────────────────────────────────────────────

type fixedResolver struct{ id *Identity }

func (f fixedResolver) Resolve(context.Context, string) *Identity { return f.id }

func TestCompositeResolver(t *testing.T) {
	alice := &Identity{Subject: "alice", Roles: []string{"reader"}}
	r := CompositeIdentityResolver{Primary: fixedResolver{alice}, Fallback: fixedResolver{&Identity{Subject: "should-not-be-used"}}}
	require.Equal(t, alice, r.Resolve(bg, "tok"))

	r = CompositeIdentityResolver{Primary: LoadStaticIdentities(""), Fallback: LoadStaticIdentities(`{"tok-1": {"subject": "openwebui", "roles": ["reader","writer"]}}`)}
	require.Equal(t, &Identity{Subject: "openwebui", Roles: []string{"reader", "writer"}}, r.Resolve(bg, "tok-1"))
	require.Nil(t, r.Resolve(bg, "nope"))
}

// ── OIDC ───────────────────────────────────────────────────────────────────

const (
	oidcIssuer   = "https://issuer.example.com"
	oidcAudience = "agent-orchestrator"
	oidcKID      = "test-key"
)

type oidcFixture struct {
	key     *rsa.PrivateKey
	keyfunc jwt.Keyfunc
}

// newOIDCFixture builds a local JWKS (the same keyfunc library production
// uses against a JWKS URI) over a fresh RSA key.
func newOIDCFixture(t *testing.T) *oidcFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwks, err := json.Marshal(map[string]any{"keys": []any{map[string]any{
		"kty": "RSA", "kid": oidcKID, "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}})
	require.NoError(t, err)
	k, err := keyfunc.NewJWKSetJSON(jwks)
	require.NoError(t, err)
	return &oidcFixture{key: key, keyfunc: k.Keyfunc}
}

type signOpts struct {
	issuer, audience string
	expired          bool
	key              *rsa.PrivateKey
}

func (f *oidcFixture) sign(t *testing.T, claims jwt.MapClaims, opts signOpts) string {
	t.Helper()
	full := jwt.MapClaims{"iat": time.Now().Unix(), "iss": oidcIssuer, "aud": oidcAudience, "exp": time.Now().Add(time.Hour).Unix()}
	if opts.issuer != "" {
		full["iss"] = opts.issuer
	}
	if opts.audience != "" {
		full["aud"] = opts.audience
	}
	if opts.expired {
		full["exp"] = time.Now().Add(-10 * time.Second).Unix()
	}
	for k, v := range claims {
		full[k] = v
	}
	key := f.key
	if opts.key != nil {
		key = opts.key
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, full)
	token.Header["kid"] = oidcKID
	signed, err := token.SignedString(key)
	require.NoError(t, err)
	return signed
}

func (f *oidcFixture) resolver(rolesClaim string) *OIDCIdentityResolver {
	return &OIDCIdentityResolver{Issuer: oidcIssuer, Audience: oidcAudience, RolesClaim: rolesClaim, Keyfunc: f.keyfunc}
}

func TestOIDCResolver(t *testing.T) {
	f := newOIDCFixture(t)
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	t.Run("resolves a valid token's subject and roles", func(t *testing.T) {
		token := f.sign(t, jwt.MapClaims{"sub": "alice", "roles": []string{"reader", "writer"}}, signOpts{})
		require.Equal(t, &Identity{Subject: "alice", Roles: []string{"reader", "writer"}}, f.resolver("roles").Resolve(bg, token))
	})
	t.Run("supports a nested roles claim path", func(t *testing.T) {
		token := f.sign(t, jwt.MapClaims{"sub": "bob", "realm_access": map[string]any{"roles": []string{"admin"}}}, signOpts{})
		require.Equal(t, &Identity{Subject: "bob", Roles: []string{"admin"}}, f.resolver("realm_access.roles").Resolve(bg, token))
	})
	t.Run("zero roles when the claim is missing or malformed", func(t *testing.T) {
		token := f.sign(t, jwt.MapClaims{"sub": "carol"}, signOpts{})
		require.Equal(t, &Identity{Subject: "carol", Roles: []string{}}, f.resolver("roles").Resolve(bg, token))
		token = f.sign(t, jwt.MapClaims{"sub": "carol", "roles": "admin"}, signOpts{})
		require.Equal(t, &Identity{Subject: "carol", Roles: []string{}}, f.resolver("roles").Resolve(bg, token))
		token = f.sign(t, jwt.MapClaims{"sub": "carol", "realm_access": "flat"}, signOpts{})
		require.Equal(t, &Identity{Subject: "carol", Roles: []string{}}, f.resolver("realm_access.roles").Resolve(bg, token))
	})
	t.Run("filters non-string roles", func(t *testing.T) {
		token := f.sign(t, jwt.MapClaims{"sub": "dave", "roles": []any{"reader", 42, nil}}, signOpts{})
		require.Equal(t, &Identity{Subject: "dave", Roles: []string{"reader"}}, f.resolver("roles").Resolve(bg, token))
	})
	t.Run("skips audience verification when none is configured", func(t *testing.T) {
		r := f.resolver("roles")
		r.Audience = ""
		token := f.sign(t, jwt.MapClaims{"sub": "alice", "roles": []string{"reader"}}, signOpts{audience: "anything"})
		require.Equal(t, &Identity{Subject: "alice", Roles: []string{"reader"}}, r.Resolve(bg, token))
	})
	for name, token := range map[string]func() string{
		"an expired token": func() string { return f.sign(t, jwt.MapClaims{"sub": "alice"}, signOpts{expired: true}) },
		"the wrong issuer": func() string {
			return f.sign(t, jwt.MapClaims{"sub": "alice"}, signOpts{issuer: "https://evil.example.com"})
		},
		"the wrong audience": func() string { return f.sign(t, jwt.MapClaims{"sub": "alice"}, signOpts{audience: "someone-else"}) },
		"a malformed token":  func() string { return "not-a-jwt" },
		"an untrusted key":   func() string { return f.sign(t, jwt.MapClaims{"sub": "alice"}, signOpts{key: other}) },
		"a missing sub":      func() string { return f.sign(t, jwt.MapClaims{"roles": []string{"reader"}}, signOpts{}) },
		"a non-string sub":   func() string { return f.sign(t, jwt.MapClaims{"sub": 123}, signOpts{}) },
		"an HS256 token": func() string {
			return hs256(t, jwt.MapClaims{"sub": "alice", "iss": oidcIssuer, "aud": oidcAudience}, "secret")
		},
	} {
		t.Run("fails closed for "+name, func(t *testing.T) {
			require.Nil(t, f.resolver("roles").Resolve(bg, token()))
		})
	}
}

func hs256(t *testing.T, claims jwt.MapClaims, secret string) string {
	t.Helper()
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	require.NoError(t, err)
	return signed
}

// ── Open WebUI forwarded user ──────────────────────────────────────────────

const owuiSecret = "test-forward-user-secret"

func owuiResolver(roles ...string) *OpenWebUIForwardedUserResolver {
	return &OpenWebUIForwardedUserResolver{Secret: []byte(owuiSecret), Roles: roles}
}

func TestOpenWebUIResolverNamespacesADistinctSubjectPerUser(t *testing.T) {
	r := owuiResolver("reader", "writer")
	require.Equal(t, &Identity{Subject: "openwebui:alice-id", Roles: []string{"reader", "writer"}, PerUser: true},
		r.Resolve(bg, hs256(t, jwt.MapClaims{"id": "alice-id", "role": "user"}, owuiSecret)))
	require.Equal(t, &Identity{Subject: "openwebui:bob-id", Roles: []string{"reader", "writer"}, PerUser: true},
		r.Resolve(bg, hs256(t, jwt.MapClaims{"id": "bob-id", "role": "admin"}, owuiSecret)))
}

// Open WebUI's own role vocabulary is unrelated to this system's RBAC.
func TestOpenWebUIResolverGrantsTheConfiguredRolesRegardlessOfItsRoleClaim(t *testing.T) {
	id := owuiResolver("reader", "writer").Resolve(bg, hs256(t, jwt.MapClaims{"id": "alice-id", "role": "admin"}, owuiSecret))
	require.Equal(t, []string{"reader", "writer"}, id.Roles)
}

func TestOpenWebUIResolverFallsBackToSubThenEmail(t *testing.T) {
	r := owuiResolver("reader", "writer")
	require.Equal(t, &Identity{Subject: "openwebui:sub-id", Roles: []string{"reader", "writer"}, PerUser: true},
		r.Resolve(bg, hs256(t, jwt.MapClaims{"sub": "sub-id"}, owuiSecret)))
	require.Equal(t, &Identity{Subject: "openwebui:alice@example.com", Roles: []string{"reader", "writer"}, PerUser: true, Email: "alice@example.com"},
		r.Resolve(bg, hs256(t, jwt.MapClaims{"email": "alice@example.com"}, owuiSecret)))
	// id wins over sub.
	require.Equal(t, "openwebui:the-id", r.Resolve(bg, hs256(t, jwt.MapClaims{"id": "the-id", "sub": "the-sub"}, owuiSecret)).Subject)
}

// The shape Open WebUI actually mints: sub is the id, email rides along.
func TestOpenWebUIResolverCarriesTheSignedEmail(t *testing.T) {
	token := hs256(t, jwt.MapClaims{"sub": "u-1", "email": "ada@example.com", "name": "Ada", "role": "user", "iss": "open-webui"}, owuiSecret)
	require.Equal(t, &Identity{Subject: "openwebui:u-1", Roles: []string{"reader"}, PerUser: true, Email: "ada@example.com"}, owuiResolver("reader").Resolve(bg, token))
	// Something that is not an email is not carried as one.
	require.Empty(t, owuiResolver("reader").Resolve(bg, hs256(t, jwt.MapClaims{"sub": "u-1", "email": "nope"}, owuiSecret)).Email)
}

func TestOpenWebUIResolverFailsClosed(t *testing.T) {
	r := owuiResolver("reader", "writer")
	wrongAlg, err := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{"id": "alice-id"}).SignedString([]byte(owuiSecret))
	require.NoError(t, err)
	for name, token := range map[string]string{
		"bad signature": hs256(t, jwt.MapClaims{"id": "alice-id"}, "wrong-secret"),
		"no usable id":  hs256(t, jwt.MapClaims{"role": "user"}, owuiSecret),
		"non-string id": hs256(t, jwt.MapClaims{"id": 7}, owuiSecret),
		"empty id":      hs256(t, jwt.MapClaims{"id": ""}, owuiSecret),
		"expired":       hs256(t, jwt.MapClaims{"id": "alice-id", "exp": time.Now().Add(-time.Minute).Unix()}, owuiSecret),
		"not HS256":     wrongAlg,
		"empty token":   "",
	} {
		require.Nil(t, r.Resolve(bg, token), name)
	}
}

// ── caller authenticator ───────────────────────────────────────────────────

func TestCallerAuthenticatorAcceptsAResolvedBearer(t *testing.T) {
	auth := &CallerAuthenticator{Bearer: &mapResolver{tokens: map[string]string{"gw-token": "client-integration-gateway"}}}
	require.Equal(t, CallerAuthResult{OK: true, Via: "bearer", Subject: "client-integration-gateway"}, auth.Authenticate(bg, "gw-token", ""))
}

func TestCallerAuthenticatorRefusesAnUnknownOrAbsentBearer(t *testing.T) {
	b := &mapResolver{tokens: map[string]string{"gw-token": "gw", "": "empty-key"}}
	auth := &CallerAuthenticator{Bearer: b}
	require.Equal(t, CallerAuthResult{Reason: "bearer token not recognized"}, auth.Authenticate(bg, "nope", ""))
	require.Equal(t, CallerAuthResult{Reason: "no bearer token"}, auth.Authenticate(bg, "", ""))
	// An absent bearer never reaches the resolver: a map with an "" key must
	// not authenticate a request that sent no token at all.
	require.Equal(t, 1, b.callCount())
}

func TestCallerAuthenticatorAuthenticatesByAForwardedUserJWTAlone(t *testing.T) {
	auth := &CallerAuthenticator{Bearer: &mapResolver{}, ForwardedUser: &mapResolver{tokens: map[string]string{"good-jwt": "openwebui:alice"}}}
	require.Equal(t, CallerAuthResult{OK: true, Via: "forwarded-user", Subject: "openwebui:alice"}, auth.Authenticate(bg, "", "good-jwt"))
}

func TestCallerAuthenticatorRefusesAnUnverifiedJWTEvenWithAGoodBearer(t *testing.T) {
	b := &mapResolver{tokens: map[string]string{"owui-token": "open-webui"}}
	auth := &CallerAuthenticator{Bearer: b, ForwardedUser: &mapResolver{tokens: map[string]string{"good-jwt": "openwebui:alice"}}}
	require.Equal(t, CallerAuthResult{Reason: "forwarded user JWT did not verify"}, auth.Authenticate(bg, "owui-token", "forged"))
	require.Zero(t, b.callCount())
}

func TestCallerAuthenticatorFallsBackToTheBearerWithoutAJWTResolver(t *testing.T) {
	auth := &CallerAuthenticator{Bearer: &mapResolver{tokens: map[string]string{"owui-token": "open-webui"}}}
	require.Equal(t, CallerAuthResult{OK: true, Via: "bearer", Subject: "open-webui"}, auth.Authenticate(bg, "owui-token", "anything"))
	require.False(t, auth.Authenticate(bg, "", "anything").OK)
}

func TestBearerTokenParsing(t *testing.T) {
	for header, want := range map[string]string{
		"Bearer abc":     "abc",
		"bearer abc":     "abc",
		"BEARER abc def": "abc",
		"Basic abc":      "",
		"Bearer":         "",
		"Bearer ":        "",
		"":               "",
		"abc":            "",
	} {
		require.Equal(t, want, bearerToken(header), header)
	}
}

// ── BuildCallerAuthenticator ───────────────────────────────────────────────

const staticJSON = `{"owui-token": {"subject": "open-webui", "roles": ["reader"]}}`

func authConfig(mutate func(*Config)) Config {
	c := Config{IdentityResolverKind: "static", StaticIdentities: staticJSON, OIDCRolesClaim: "roles"}
	if mutate != nil {
		mutate(&c)
	}
	return c
}

func noJWKS(string) (jwt.Keyfunc, error) { return nil, errors.New("no JWKS in this test") }

func TestBuildCallerAuthenticatorDefaultsToEnforceAndRefusesUnsupportedModes(t *testing.T) {
	auth, _, err := BuildCallerAuthenticator(authConfig(nil), nil, noJWKS)
	require.NoError(t, err)
	require.Equal(t, "enforce", auth.Mode)
	auth, _, err = BuildCallerAuthenticator(authConfig(func(c *Config) { c.CallerAuthMode = "warn" }), nil, noJWKS)
	require.NoError(t, err)
	require.Equal(t, "warn", auth.Mode)
	for _, mode := range []string{"off", "Enforce", "disabled"} {
		_, _, err := BuildCallerAuthenticator(authConfig(func(c *Config) { c.CallerAuthMode = mode }), nil, noJWKS)
		require.ErrorContains(t, err, fmt.Sprintf(`AGENT_CALLER_AUTH=%q is not supported`, mode))
	}
}

func TestBuildCallerAuthenticatorWarnsLoudlyInWarnMode(t *testing.T) {
	_, warnings, err := BuildCallerAuthenticator(authConfig(func(c *Config) { c.CallerAuthMode = "warn" }), nil, noJWKS)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "NOT refused")
}

func TestBuildCallerAuthenticatorStaticAuthenticatesExactlyTheMap(t *testing.T) {
	auth, warnings, err := BuildCallerAuthenticator(authConfig(nil), nil, noJWKS)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, CallerAuthResult{OK: true, Via: "bearer", Subject: "open-webui"}, auth.Authenticate(bg, "owui-token", ""))
	require.False(t, auth.Authenticate(bg, "other", "").OK)
}

func TestBuildCallerAuthenticatorStaticWithAnEmptyMapWarns(t *testing.T) {
	_, warnings, err := BuildCallerAuthenticator(authConfig(func(c *Config) { c.StaticIdentities = "" }), nil, noJWKS)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "every turn will be refused")

	_, warnings, err = BuildCallerAuthenticator(authConfig(func(c *Config) { c.StaticIdentities = "" }), fixedResolver{}, noJWKS)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "every /invoke caller")
}

func TestBuildCallerAuthenticatorOIDCRequiresIssuerAndJWKSURI(t *testing.T) {
	_, _, err := BuildCallerAuthenticator(authConfig(func(c *Config) { c.IdentityResolverKind = "oidc" }), nil, noJWKS)
	require.EqualError(t, err, "AGENT_OIDC_ISSUER and AGENT_OIDC_JWKS_URI are required when AGENT_IDENTITY_RESOLVER=oidc")

	_, _, err = BuildCallerAuthenticator(authConfig(func(c *Config) {
		c.IdentityResolverKind, c.OIDCIssuer, c.OIDCJWKSURI = "oidc", oidcIssuer, "https://idp.example.com/jwks"
	}), nil, noJWKS)
	require.ErrorContains(t, err, "AGENT_OIDC_JWKS_URI: no JWKS in this test")
}

func oidcConfig(static string) Config {
	return authConfig(func(c *Config) {
		c.IdentityResolverKind = "oidc"
		c.StaticIdentities = static
		c.OIDCIssuer = oidcIssuer
		c.OIDCJWKSURI = "https://idp.example.com/jwks"
		c.OIDCAudience = oidcAudience
	})
}

// homelab's shape: JWTs verified, the static map only for the tokens in it.
func TestBuildCallerAuthenticatorOIDCFallsBackToTheStaticMap(t *testing.T) {
	f := newOIDCFixture(t)
	var askedFor string
	auth, _, err := BuildCallerAuthenticator(oidcConfig(staticJSON), nil, func(uri string) (jwt.Keyfunc, error) {
		askedFor = uri
		return f.keyfunc, nil
	})
	require.NoError(t, err)
	require.Equal(t, "https://idp.example.com/jwks", askedFor)
	require.Equal(t, "integration-gateway", auth.Authenticate(bg, f.sign(t, jwt.MapClaims{"sub": "integration-gateway"}, signOpts{}), "").Subject)
	require.Equal(t, "open-webui", auth.Authenticate(bg, "owui-token", "").Subject)
	require.False(t, auth.Authenticate(bg, f.sign(t, jwt.MapClaims{"sub": "x"}, signOpts{issuer: "https://evil.example.com"}), "").OK)
	require.False(t, auth.Authenticate(bg, "other", "").OK)
}

func TestBuildCallerAuthenticatorOIDCWithoutAStaticMapAcceptsOnlyJWTs(t *testing.T) {
	f := newOIDCFixture(t)
	auth, warnings, err := BuildCallerAuthenticator(oidcConfig(""), nil, func(string) (jwt.Keyfunc, error) { return f.keyfunc, nil })
	require.NoError(t, err)
	require.Empty(t, warnings, "an empty map warns only for the static resolver")
	require.False(t, auth.Authenticate(bg, "owui-token", "").OK)
	require.True(t, auth.Authenticate(bg, f.sign(t, jwt.MapClaims{"sub": "integration-gateway"}, signOpts{}), "").OK)
}
