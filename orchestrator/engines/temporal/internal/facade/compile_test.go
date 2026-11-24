package facade

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// POST /events/compile: which of a source's sender expressions do not
// compile, so integration-gateway can 404 that source's routes.

func TestCompileReportsOnlyBrokenExpressions(t *testing.T) {
	resp := Compile(CompileRequest{Expressions: []string{"body.sender.login", "body.sender.(", `body.sender.type == "Bot"`}})
	require.Len(t, resp.Errors, 1)
	require.True(t, strings.Contains(resp.Errors["body.sender.("], "Syntax error"), resp.Errors["body.sender.("])
}

func TestCompileEndpoint(t *testing.T) {
	base := prefilterServer(t, nil)

	res := do(t, http.MethodPost, base+"/events/compile", map[string]any{"expressions": []string{"body.a", "body.("}}, bearer(gatewayToken))
	require.Equal(t, http.StatusOK, res.StatusCode)
	body := decode[CompileResponse](t, res)
	require.Len(t, body.Errors, 1)
	require.Contains(t, body.Errors, "body.(")

	res = do(t, http.MethodPost, base+"/events/compile", map[string]any{"expressions": []string{}}, bearer(gatewayToken))
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Empty(t, decode[CompileResponse](t, res).Errors)

	res = do(t, http.MethodPost, base+"/events/compile", map[string]any{}, bearer(gatewayToken))
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	res = do(t, http.MethodPost, base+"/events/compile", map[string]any{"expressions": []string{"body.a"}}, nil)
	require.Equal(t, http.StatusUnauthorized, res.StatusCode)
}
