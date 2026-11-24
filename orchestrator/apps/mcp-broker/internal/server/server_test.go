package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/auth"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/mcpclient"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/registry"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
)

func ptr[T any](v T) *T { return &v }

func serverCR(identityProviders []string, expose bool) *resource.MCPServer {
	return &resource.MCPServer{
		Metadata: resource.ObjectMeta{Name: "github-mcp", UID: "u"},
		Spec: resource.MCPServerSpec{
			Transport:         "streamable-http",
			URL:               "https://mcp.example/",
			IdentityProviders: identityProviders,
			Exposure: []resource.MCPToolExposure{
				{RemoteToolName: "search_issues", AllowedRoles: []string{"eng"}, Expose: ptr(expose)},
			},
		},
	}
}

// fakeClient records every tools/call and answers with a fixed result or error.
type fakeClient struct {
	mu     sync.Mutex
	calls  []mcpclient.CallRequest
	result mcpclient.ToolCallResult
	err    error
}

func (c *fakeClient) ListTools(context.Context, string, string) ([]mcpclient.DiscoveredTool, error) {
	return nil, nil
}

func (c *fakeClient) CallTool(_ context.Context, req mcpclient.CallRequest) (mcpclient.ToolCallResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, req)
	return c.result, c.err
}

func okClient() *fakeClient {
	return &fakeClient{result: mcpclient.ToolCallResult{Content: []mcpclient.ContentPart{{Type: "text", Text: "ok"}}}}
}

func start(t *testing.T, cr *resource.MCPServer, client *fakeClient, serviceToken string) string {
	t.Helper()
	ts := httptest.NewServer(New(Options{
		Auth:     auth.Config{OrchestratorToken: "orch"},
		Registry: registry.NewStatic(registry.Binding{Name: cr.Metadata.Name, CR: cr, ServiceToken: serviceToken}),
		Client:   client,
	}))
	t.Cleanup(ts.Close)
	return ts.URL
}

type request struct {
	method  string
	path    string
	headers map[string]string
	body    *string
}

type response struct {
	status int
	body   map[string]any
	ctype  string
}

func do(t *testing.T, base string, req request) response {
	t.Helper()
	method := req.method
	if method == "" {
		method = http.MethodPost
	}
	body := `{"arguments":{"q":"bug"}}`
	if req.body != nil {
		body = *req.body
	}
	r, err := http.NewRequest(method, base+req.path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("authorization", "Bearer orch")
	for k, v := range req.headers {
		if v == "" {
			r.Header.Del(k)
		} else {
			r.Header.Set(k, v)
		}
	}
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var parsed map[string]any
	_ = json.Unmarshal(raw, &parsed)
	return response{status: res.StatusCode, body: parsed, ctype: res.Header.Get("content-type")}
}

const callPath = "/servers/github-mcp/tools/search_issues/call"

func TestHealthAndAuth(t *testing.T) {
	client := okClient()
	base := start(t, serverCR(nil, true), client, "svc-token")

	cases := []struct {
		name   string
		req    request
		status int
		body   map[string]any
	}{
		{"serves health without a token", request{method: http.MethodGet, path: "/healthz", headers: map[string]string{"authorization": ""}},
			200, map[string]any{"ok": true}},
		{"rejects a missing bearer with 401", request{path: callPath, headers: map[string]string{"authorization": ""}},
			401, map[string]any{"message": "missing bearer token"}},
		{"rejects an unknown bearer with 401", request{path: callPath, headers: map[string]string{"authorization": "Bearer nope"}},
			401, map[string]any{"message": "unrecognized bearer token"}},
		{"404s a non-POST", request{method: http.MethodGet, path: callPath}, 404, map[string]any{"message": "not found"}},
		{"404s an unrecognized route", request{path: "/servers/github-mcp/nope"}, 404, map[string]any{"message": "not found"}},
		{"404s an unrecognized route before auth", request{path: "/nope", headers: map[string]string{"authorization": ""}},
			404, map[string]any{"message": "not found"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := do(t, base, tc.req)
			if res.status != tc.status || !reflect.DeepEqual(res.body, tc.body) {
				t.Fatalf("got %d %v, want %d %v", res.status, res.body, tc.status, tc.body)
			}
			if res.ctype != "application/json" {
				t.Fatalf("content-type = %q", res.ctype)
			}
		})
	}
	if len(client.calls) != 0 {
		t.Fatalf("calls = %+v", client.calls)
	}
}

func TestLookup(t *testing.T) {
	cases := []struct {
		name    string
		expose  bool
		path    string
		message string
	}{
		{"404s an unknown server", true, "/servers/other/tools/search_issues/call", "no such server: other"},
		// The operator's exposure map is truth for permission (§6): the broker
		// refuses to proxy a tool no exposure entry covers.
		{"404s a tool that is not exposed, even if the server advertises it", false, callPath,
			"no such tool on github-mcp: search_issues"},
		{"404s a tool name not in the exposure map at all", true, "/servers/github-mcp/tools/delete_repo/call",
			"no such tool on github-mcp: delete_repo"},
		{"decodes percent-encoded segments", true, "/servers/github-mcp/tools/a%2Fb/call", "no such tool on github-mcp: a/b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := okClient()
			base := start(t, serverCR(nil, tc.expose), client, "svc-token")
			res := do(t, base, request{path: tc.path})
			if res.status != 404 || res.body["message"] != tc.message {
				t.Fatalf("got %d %v", res.status, res.body)
			}
			if len(client.calls) != 0 {
				t.Fatalf("calls = %+v", client.calls)
			}
		})
	}

	t.Run("tolerates doubled slashes", func(t *testing.T) {
		client := okClient()
		base := start(t, serverCR(nil, true), client, "svc-token")
		if res := do(t, base, request{path: "/servers//github-mcp/tools/search_issues/call/"}); res.status != 200 {
			t.Fatalf("got %d %v", res.status, res.body)
		}
	})
}

func TestDelegatedTokenFailClosed(t *testing.T) {
	t.Run("403s a per-user server when no delegated token is sent", func(t *testing.T) {
		for _, delegated := range []string{"", "   "} {
			client := okClient()
			base := start(t, serverCR([]string{"github"}, true), client, "svc-token")
			headers := map[string]string{}
			if delegated != "" {
				headers[DelegatedTokenHeader] = delegated
			}
			res := do(t, base, request{path: callPath, headers: headers})
			if res.status != 403 {
				t.Fatalf("status = %d", res.status)
			}
			// Crucially, it did NOT fall back to the service credential.
			if len(client.calls) != 0 {
				t.Fatalf("calls = %+v", client.calls)
			}
			want := "server github-mcp runs tools as the calling user, but no x-delegated-token was provided; " +
				"refusing to fall back to the discovery credential"
			if res.body["message"] != want {
				t.Fatalf("message = %v", res.body["message"])
			}
		}
	})

	cases := []struct {
		name              string
		identityProviders []string
		serviceToken      string
		delegated         string
		wantToken         string
	}{
		{"spends the DELEGATED token for a per-user server", []string{"github"}, "svc-token", " user-tok ", "user-tok"},
		// A shared server spends the service token and ignores any forwarded
		// user token — identity does not matter to it.
		{"spends the SERVICE credential for a server with no identityProviders", nil, "svc-token", "ignored", "svc-token"},
		{"calls unauthenticated when a shared server has no service credential", nil, "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := okClient()
			base := start(t, serverCR(tc.identityProviders, true), client, tc.serviceToken)
			headers := map[string]string{}
			if tc.delegated != "" {
				headers[DelegatedTokenHeader] = tc.delegated
			}
			res := do(t, base, request{path: callPath, headers: headers})
			if res.status != 200 {
				t.Fatalf("status = %d %v", res.status, res.body)
			}
			want := mcpclient.CallRequest{
				URL: "https://mcp.example/", Token: tc.wantToken, Name: "search_issues",
				Arguments: map[string]any{"q": "bug"},
			}
			if len(client.calls) != 1 || !reflect.DeepEqual(client.calls[0], want) {
				t.Fatalf("calls = %+v", client.calls)
			}
		})
	}
}

func TestResultMapping(t *testing.T) {
	cases := []struct {
		name   string
		client *fakeClient
		status int
		body   map[string]any
	}{
		{"flattens text content and passes isError through",
			&fakeClient{result: mcpclient.ToolCallResult{Content: []mcpclient.ContentPart{
				{Type: "text", Text: "line 1\n"}, {Type: "text", Text: "line 2"},
			}}},
			200, map[string]any{"result": "line 1\nline 2", "isError": false}},
		{"notes non-text content by its type and reports isError",
			&fakeClient{result: mcpclient.ToolCallResult{Content: []mcpclient.ContentPart{
				{Type: "text", Text: "see image: "}, {Type: "image"},
			}, IsError: true}},
			200, map[string]any{"result": "see image: [image]", "isError": true}},
		{"502s a transport/protocol failure",
			&fakeClient{err: &mcpclient.TransportError{Message: "MCP request to x failed: connection refused"}},
			502, map[string]any{"message": "MCP request to x failed: connection refused"}},
		{"500s any other failure without leaking it",
			&fakeClient{err: io.ErrUnexpectedEOF},
			500, map[string]any{"message": "internal error"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := start(t, serverCR(nil, true), tc.client, "svc-token")
			res := do(t, base, request{path: callPath})
			if res.status != tc.status || !reflect.DeepEqual(res.body, tc.body) {
				t.Fatalf("got %d %v, want %d %v", res.status, res.body, tc.status, tc.body)
			}
		})
	}
}

func TestArgumentsBody(t *testing.T) {
	big := `{"arguments":{"pad":"` + strings.Repeat("x", maxBodyBytes) + `"}}`
	cases := []struct {
		name   string
		body   string
		status int
		args   map[string]any
	}{
		{"tolerates a malformed body", "not json", 200, map[string]any{}},
		{"tolerates an empty body", "", 200, map[string]any{}},
		{"tolerates a non-object body", `[1,2]`, 200, map[string]any{}},
		{"tolerates a null body", `null`, 200, map[string]any{}},
		{"tolerates missing arguments", `{}`, 200, map[string]any{}},
		{"tolerates array arguments", `{"arguments":[1]}`, 200, map[string]any{}},
		{"tolerates null arguments", `{"arguments":null}`, 200, map[string]any{}},
		{"tolerates scalar arguments", `{"arguments":"x"}`, 200, map[string]any{}},
		{"passes nested arguments through, numbers verbatim", `{"arguments":{"n":12345678901234567890,"o":{"a":[true]}}}`, 200,
			map[string]any{"n": json.Number("12345678901234567890"), "o": map[string]any{"a": []any{true}}}},
		{"refuses an over-limit body", big, 500, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := okClient()
			base := start(t, serverCR(nil, true), client, "svc-token")
			body := tc.body
			res := do(t, base, request{path: callPath, body: &body})
			if res.status != tc.status {
				t.Fatalf("status = %d %v", res.status, res.body)
			}
			if tc.args == nil {
				if len(client.calls) != 0 {
					t.Fatalf("calls = %+v", client.calls)
				}
				return
			}
			if len(client.calls) != 1 || !reflect.DeepEqual(client.calls[0].Arguments, tc.args) {
				t.Fatalf("arguments = %#v", client.calls)
			}
		})
	}
}

func TestParseRoute(t *testing.T) {
	cases := []struct {
		path    string
		want    *route
		wantErr bool
	}{
		{"/servers/s/tools/t/call", &route{"s", "t"}, false},
		{"/servers/s%2Fx/tools/t%20u/call", &route{"s/x", "t u"}, false},
		{"//servers/s/tools/t/call//", &route{"s", "t"}, false},
		{"/servers/s/tools/t", nil, false},
		{"/servers/s/tool/t/call", nil, false},
		{"/servers/s/tools/t/call/extra", nil, false},
		// Unreachable over HTTP (net/http rejects the request line with a 400
		// first) but kept an error rather than a guess.
		{"/servers/a%zz/tools/t/call", nil, true},
	}
	for _, tc := range cases {
		got, err := parseRoute(tc.path)
		if (err != nil) != tc.wantErr || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parseRoute(%q) = %+v, %v", tc.path, got, err)
		}
	}
}
