package auth

import (
	"errors"
	"testing"
)

var config = Config{OrchestratorToken: "orchestrator-secret"}

func TestAuthenticate(t *testing.T) {
	cases := []struct {
		name          string
		authorization string
		ok            bool
	}{
		{"accepts the orchestrator token", "Bearer orchestrator-secret", true},
		{"tolerates whitespace and a case-insensitive scheme", "bearer   orchestrator-secret  ", true},
		// An unrecognized caller is never an anonymous one with reduced powers —
		// every accepted caller may cause a tools/call to run.
		{"fails closed on a missing token", "", false},
		{"fails closed on an empty bearer", "Bearer ", false},
		{"fails closed on an unknown token", "Bearer nope", false},
		// A timing-safe compare must also be a length-safe one: a prefix of the
		// secret is not the secret.
		{"does not accept a prefix", "Bearer orchestrator-secre", false},
		{"does not accept an extension", "Bearer orchestrator-secretX", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Authenticate(config, tc.authorization)
			if tc.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.ok {
				var unauthorized *UnauthorizedError
				if !errors.As(err, &unauthorized) {
					t.Fatalf("want UnauthorizedError, got %v", err)
				}
			}
		})
	}

	t.Run("fails closed when no orchestrator token is configured", func(t *testing.T) {
		if err := Authenticate(Config{}, "Bearer anything"); err == nil {
			t.Fatal("accepted a caller with no token configured")
		}
	})

	t.Run("names the missing case distinctly", func(t *testing.T) {
		if err := Authenticate(config, ""); err.Error() != "missing bearer token" {
			t.Fatalf("message = %q", err.Error())
		}
		if err := Authenticate(config, "Bearer nope"); err.Error() != "unrecognized bearer token" {
			t.Fatalf("message = %q", err.Error())
		}
	})
}

func TestSecretsMatch(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"abc", "abc", true},
		{"abc", "abd", false},
		// Differing lengths must return false rather than fail.
		{"abc", "abcd", false},
		{"", "", true},
	}
	for _, tc := range cases {
		if got := SecretsMatch(tc.a, tc.b); got != tc.want {
			t.Errorf("SecretsMatch(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
