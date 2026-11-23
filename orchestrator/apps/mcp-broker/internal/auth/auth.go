// Package auth is caller authentication for the broker's invocation API
// (docs/adr/0045 §5).
//
// The broker proxies a tools/call to an externally-controlled MCP server, under
// a credential the caller supplies. That makes it a confused deputy by
// construction (same posture as connection-broker, ADR 0038 §3), and this
// package is the first gate: only the agent engines, holding the shared
// ORCHESTRATOR_TOKEN, may ask the broker to invoke anything at all.
//
// There is exactly ONE caller class here — the orchestrator/engine. The broker
// never spends a per-connection credential of its own on a caller's behalf:
// invocation runs as the user, via the delegated token the caller forwards (see
// the server package). So the only question this package answers is "is this
// the engine", and it answers it fail-closed.
package auth

import (
	"crypto/subtle"
	"regexp"
	"strings"
)

// UnauthorizedError is a rejected caller; the server maps it to 401.
type UnauthorizedError struct{ msg string }

func (e *UnauthorizedError) Error() string { return e.msg }

// Config is the shared secret the agent engines present. Mirrors
// connection-broker.
type Config struct {
	OrchestratorToken string
}

// SecretsMatch is a constant-time compare that tolerates differing lengths
// (returning false) rather than failing.
func SecretsMatch(presented, expected string) bool {
	if len(presented) != len(expected) {
		// Still burn a comparison so the failure is not distinguishable by timing.
		subtle.ConstantTimeCompare([]byte(presented), []byte(presented))
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(expected)) == 1
}

var bearerPrefix = regexp.MustCompile(`(?i)^Bearer\s+`)

// Authenticate accepts the caller only if it presents the orchestrator token.
//
// Fails closed: a missing, empty or unrecognized bearer is rejected rather than
// treated as an anonymous caller with reduced powers — there are no reduced
// powers here, every accepted caller may cause a tools/call to run.
func Authenticate(config Config, authorization string) error {
	token := strings.TrimSpace(bearerPrefix.ReplaceAllString(authorization, ""))
	if token == "" {
		return &UnauthorizedError{"missing bearer token"}
	}
	if config.OrchestratorToken != "" && SecretsMatch(token, config.OrchestratorToken) {
		return nil
	}
	return &UnauthorizedError{"unrecognized bearer token"}
}
