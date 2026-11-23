package mcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type echoArgs struct {
	Text string `json:"text"`
}

// startServer runs a real MCP server over streamable HTTP — the same shape as
// orchestrator/apps/fake-mcp-server — recording the Authorization header of
// every request so a test can see which credential the broker spent.
func startServer(t *testing.T, stateless bool) (url string, authHeaders func() []string) {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "echo", Description: "Echo text"},
		func(_ context.Context, _ *mcp.CallToolRequest, in echoArgs) (*mcp.CallToolResult, any, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{
				&mcp.TextContent{Text: "echo:" + in.Text},
				&mcp.ImageContent{Data: []byte{1}, MIMEType: "image/png"},
			}}, nil, nil
		})
	mcp.AddTool(server, &mcp.Tool{Name: "boom", Description: "Always fails"},
		func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, any, error) {
			return nil, nil, errors.New("kaboom")
		})

	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{Stateless: stateless})

	var mu sync.Mutex
	var seen []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts.URL, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func TestSDKClientAgainstARealServer(t *testing.T) {
	for _, stateless := range []bool{false, true} {
		name := "stateful"
		if stateless {
			name = "stateless"
		}
		t.Run(name, func(t *testing.T) {
			url, headers := startServer(t, stateless)
			client := &SDKClient{}
			ctx := context.Background()

			tools, err := client.ListTools(ctx, url, "svc-token")
			if err != nil {
				t.Fatalf("ListTools: %v", err)
			}
			if len(tools) != 2 || tools[0].Name != "boom" && tools[0].Name != "echo" {
				t.Fatalf("tools = %+v", tools)
			}
			var echo DiscoveredTool
			for _, tool := range tools {
				if tool.Name == "echo" {
					echo = tool
				}
			}
			if echo.Description != "Echo text" || echo.InputSchema == nil {
				t.Fatalf("echo = %+v", echo)
			}
			schema, _ := json.Marshal(echo.InputSchema)
			if !strings.Contains(string(schema), `"text"`) {
				t.Fatalf("schema = %s", schema)
			}
			for _, h := range headers() {
				if h != "Bearer svc-token" {
					t.Fatalf("discovery sent Authorization %q", h)
				}
			}

			result, err := client.CallTool(ctx, CallRequest{URL: url, Token: "user-tok", Name: "echo", Arguments: map[string]any{"text": "hi"}})
			if err != nil {
				t.Fatalf("CallTool: %v", err)
			}
			if got := FlattenToolResult(result); got != (FlattenedResult{Result: "echo:hi[image]", IsError: false}) {
				t.Fatalf("flattened = %+v", got)
			}
			all := headers()
			if last := all[len(all)-1]; last != "Bearer user-tok" {
				t.Fatalf("invocation sent Authorization %q", last)
			}

			// A tool-level failure is a result with isError, not a transport error.
			failed, err := client.CallTool(ctx, CallRequest{URL: url, Name: "boom"})
			if err != nil {
				t.Fatalf("CallTool boom: %v", err)
			}
			if !failed.IsError || !strings.Contains(FlattenToolResult(failed).Result, "kaboom") {
				t.Fatalf("boom = %+v", failed)
			}
		})
	}
}

func TestSDKClientOmitsTheHeaderWithNoToken(t *testing.T) {
	url, headers := startServer(t, true)
	if _, err := (&SDKClient{}).ListTools(context.Background(), url, ""); err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	for _, h := range headers() {
		if h != "" {
			t.Fatalf("sent Authorization %q with no token", h)
		}
	}
}

func TestSDKClientWrapsFailuresAsTransportErrors(t *testing.T) {
	cases := []struct {
		name string
		url  func(t *testing.T) string
	}{
		{"unreachable", func(t *testing.T) string {
			ts := httptest.NewServer(http.NotFoundHandler())
			ts.Close()
			return ts.URL
		}},
		{"not an MCP server", func(t *testing.T) string {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "nope", http.StatusInternalServerError)
			}))
			t.Cleanup(ts.Close)
			return ts.URL
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := tc.url(t)
			_, err := (&SDKClient{}).ListTools(context.Background(), url, "")
			var transport *TransportError
			if !errors.As(err, &transport) {
				t.Fatalf("want TransportError, got %T %v", err, err)
			}
			if !strings.HasPrefix(transport.Message, "MCP request to "+url+" failed: ") {
				t.Fatalf("message = %q", transport.Message)
			}
		})
	}
}

func TestFlattenToolResult(t *testing.T) {
	cases := []struct {
		name string
		in   ToolCallResult
		want FlattenedResult
	}{
		{"concatenates text parts in order",
			ToolCallResult{Content: []ContentPart{{Type: "text", Text: "line 1\n"}, {Type: "text", Text: "line 2"}}},
			FlattenedResult{Result: "line 1\nline 2"}},
		{"notes non-text content by type and passes isError",
			ToolCallResult{Content: []ContentPart{{Type: "text", Text: "see image: "}, {Type: "image"}}, IsError: true},
			FlattenedResult{Result: "see image: [image]", IsError: true}},
		{"empty content", ToolCallResult{}, FlattenedResult{Result: ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FlattenToolResult(tc.in); got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}
