package facade

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
)

// harnessEnv carries tokens minted by the e2e chat harness
// (e2e/support/openwebui-jwt.ts) to TestE2EHarness*, which
// e2e/specs/chat-harness.e2e.ts runs. The harness's JWT signing is the part
// most likely to be quietly wrong, and when it is every chat spec fails as
// "could not resolve caller identity" — a product defect, apparently, costing
// a full stack cycle to disprove. Checked here, against the REAL resolver,
// with no cluster. Without the variable these tests skip.
const harnessEnv = "E2E_HARNESS_JWTS"

// harnessSecret is the deployment secret the e2e values configure.
const harnessSecret = "e2e-openwebui-forward-jwt-secret"

type harnessTokens struct {
	Valid       string `json:"valid"`
	WrongSecret string `json:"wrongSecret"`
	Fresh       string `json:"fresh"`
	Expired     string `json:"expired"`
	// StreamOut, when set, is where TestE2EHarnessStreams writes the facade's
	// real SSE output for the harness's parsers to read back.
	StreamOut string `json:"streamOut"`
}

func loadHarness(t *testing.T) harnessTokens {
	t.Helper()
	raw := os.Getenv(harnessEnv)
	if raw == "" {
		t.Skipf("%s not set: run by e2e/specs/chat-harness.e2e.ts", harnessEnv)
	}
	var tokens harnessTokens
	require.NoError(t, json.Unmarshal([]byte(raw), &tokens))
	return tokens
}

func TestE2EHarnessForwardedUserJWT(t *testing.T) {
	tokens := loadHarness(t)
	resolver := &OpenWebUIForwardedUserResolver{Secret: []byte(harnessSecret), Roles: []string{"reader", "writer"}}
	ctx := context.Background()

	id := resolver.Resolve(ctx, tokens.Valid)
	require.NotNil(t, id, "the resolver refused the harness's JWT")
	require.Equal(t, "openwebui:e2e-chat-user", id.Subject)
	require.Equal(t, []string{"reader", "writer"}, id.Roles)
	// Without PerUser the pre-flight refuses to establish or adopt anything
	// (docs/adr/0031): a harness that lost it would test the degraded path.
	require.True(t, id.PerUser)

	require.Nil(t, resolver.Resolve(ctx, tokens.WrongSecret), "the resolver is not verifying signatures")
	require.NotNil(t, resolver.Resolve(ctx, tokens.Fresh))
	require.Nil(t, resolver.Resolve(ctx, tokens.Expired), "the resolver ignored the harness's exp")
}

// TestE2EHarnessStreams writes what the facade REALLY streams for a text
// answer and for a tool-call turn, so the harness's SSE parsers are checked
// against product output rather than a hand-copied shape.
func TestE2EHarnessStreams(t *testing.T) {
	tokens := loadHarness(t)
	if tokens.StreamOut == "" {
		t.Skip("no streamOut")
	}
	result := "To continue, please link your GitHub account"
	outcomes := []TurnOutcome{
		{Result: &result},
		{PendingToolCalls: []callertools.PendingCall{{ID: "call_abc", Name: "get_device_battery", Arguments: `{"serial":"SN-4417"}`}}},
	}
	streams := map[string]string{}
	for i, key := range []string{"text", "toolCalls"} {
		outcome := outcomes[i]
		server := &Server{Engine: harnessEngine{outcome: outcome}}
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
			strings.NewReader(`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		streams[key] = rec.Body.String()
	}
	payload, err := json.Marshal(streams)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tokens.StreamOut, payload, 0o600))
}

type harnessEngine struct{ outcome TurnOutcome }

func (e harnessEngine) Invoke(context.Context, TurnInput) (TurnOutcome, error) { return e.outcome, nil }
func (e harnessEngine) Adopt(context.Context, string, TurnInput) (TurnOutcome, error) {
	return e.outcome, nil
}
func (e harnessEngine) CheckIdentity(context.Context, TurnInput) *IdentityRefusal { return nil }
