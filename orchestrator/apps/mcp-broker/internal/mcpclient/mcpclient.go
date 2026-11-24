// Package mcpclient is the broker's thin wrapper over the MCP client SDK — the
// ONE place that speaks the protocol (ADR 0045 §3). Everything above this
// package deals in plain tool inventories and tool-call results; the session
// handshake, the transport and the SDK framing stay quarantined here.
//
// Two operations, each opening a FRESH client session and closing it (ADR 0045
// §5): ListTools for discovery (under the shared service credential) and
// CallTool for invocation (under the caller's per-user token). The broker holds
// no per-user session — a shared session fanned across callers is exactly the
// shared-subject identity collapse this design refuses.
package mcpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// DiscoveredTool is one tool as the server advertises it in tools/list.
type DiscoveredTool struct {
	Name        string
	Description string
	// InputSchema is the raw JSON Schema value the server returned, or nil.
	InputSchema any
}

// ContentPart is one block of a tools/call result's content.
type ContentPart struct {
	Type string
	Text string
}

// ToolCallResult is a tools/call result, flattened by FlattenToolResult.
type ToolCallResult struct {
	Content []ContentPart
	IsError bool
}

// CallRequest is one tools/call. Token "" means no Authorization header.
type CallRequest struct {
	URL       string
	Token     string
	Name      string
	Arguments map[string]any
}

// Client is the protocol surface the rest of the broker depends on. Tests
// inject a fake.
type Client interface {
	ListTools(ctx context.Context, url, token string) ([]DiscoveredTool, error)
	CallTool(ctx context.Context, req CallRequest) (ToolCallResult, error)
}

// TransportError is a transport/protocol failure talking to a server —
// unreachable, a bad handshake, a malformed frame, a JSON-RPC error.
//
// Named distinctly so the HTTP surface can map it to 502 (the server is at
// fault) and keep it separate from an authorization decision the broker itself
// made. One bad server must never look like a broker bug.
type TransportError struct{ Message string }

func (e *TransportError) Error() string { return e.Message }

const (
	clientName    = "mcp-broker"
	clientVersion = "0.1.0"
	// protocolVersion pins the classic initialize handshake — what the
	// TypeScript SDK client this replaced spoke. The Go SDK's default (the
	// stateless 2026-07-28 revision) opens with a server/discover probe first,
	// which a stateful pre-2026 server may answer with an HTTP error rather than
	// a JSON-RPC "method not found"; servers in the wild today speak the
	// initialize handshake, and the server still negotiates down from here.
	protocolVersion = "2025-11-25"
	// requestTimeout bounds each phase (connect, then the one operation), the
	// TypeScript SDK's default per-request timeout.
	requestTimeout = 60 * time.Second
)

// SDKClient is the real SDK-backed client.
type SDKClient struct {
	// HTTPClient is the base client requests go through; nil uses
	// http.DefaultTransport.
	HTTPClient *http.Client
}

func (c *SDKClient) ListTools(ctx context.Context, url, token string) ([]DiscoveredTool, error) {
	var out []DiscoveredTool
	err := c.withSession(ctx, url, token, func(ctx context.Context, session *mcp.ClientSession) error {
		// One page, as the TypeScript client's listTools() returned.
		result, err := session.ListTools(ctx, nil)
		if err != nil {
			return err
		}
		out = make([]DiscoveredTool, 0, len(result.Tools))
		for _, tool := range result.Tools {
			out = append(out, DiscoveredTool{
				Name:        tool.Name,
				Description: tool.Description,
				InputSchema: tool.InputSchema,
			})
		}
		return nil
	})
	return out, err
}

func (c *SDKClient) CallTool(ctx context.Context, req CallRequest) (ToolCallResult, error) {
	var out ToolCallResult
	err := c.withSession(ctx, req.URL, req.Token, func(ctx context.Context, session *mcp.ClientSession) error {
		args := req.Arguments
		if args == nil {
			args = map[string]any{}
		}
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: req.Name, Arguments: args})
		if err != nil {
			return err
		}
		out = ToolCallResult{Content: make([]ContentPart, 0, len(result.Content)), IsError: result.IsError}
		for _, part := range result.Content {
			out.Content = append(out.Content, contentPart(part))
		}
		return nil
	})
	return out, err
}

// withSession opens a fresh session, runs one operation, and always closes it.
//
// Any failure to connect or converse becomes a TransportError, so callers never
// see raw SDK or net/http errors and can map the whole class to one HTTP
// status. The Authorization header is omitted entirely when no token is given —
// a server that needs no credential to list its tools gets an unauthenticated
// request, not an empty bearer.
func (c *SDKClient) withSession(
	ctx context.Context,
	url, token string,
	run func(context.Context, *mcp.ClientSession) error,
) error {
	base := http.DefaultTransport
	if c.HTTPClient != nil && c.HTTPClient.Transport != nil {
		base = c.HTTPClient.Transport
	}
	transport := &mcp.StreamableClientTransport{
		Endpoint:   url,
		HTTPClient: &http.Client{Transport: &bearerTransport{base: base, token: token}},
		// Request/response only: a one-shot session has no use for the
		// server-initiated stream, and a server that mishandles the GET would
		// otherwise turn into a spurious failure.
		DisableStandaloneSSE: true,
	}
	// Advertise no client capabilities, as the TypeScript client did.
	client := mcp.NewClient(
		&mcp.Implementation{Name: clientName, Version: clientVersion},
		&mcp.ClientOptions{Capabilities: &mcp.ClientCapabilities{}},
	)

	connectCtx, cancelConnect := context.WithTimeout(ctx, requestTimeout)
	session, err := client.Connect(connectCtx, transport, &mcp.ClientSessionOptions{ProtocolVersion: protocolVersion})
	cancelConnect()
	if err != nil {
		return transportError(url, err)
	}
	// Best-effort: the call already succeeded or failed; a close error must not
	// mask that outcome.
	defer func() { _ = session.Close() }()

	opCtx, cancelOp := context.WithTimeout(ctx, requestTimeout)
	defer cancelOp()
	if err := run(opCtx, session); err != nil {
		return transportError(url, err)
	}
	return nil
}

func transportError(url string, err error) error {
	return &TransportError{Message: fmt.Sprintf("MCP request to %s failed: %s", url, err.Error())}
}

// bearerTransport adds the caller-chosen bearer to every request of a session.
type bearerTransport struct {
	base  http.RoundTripper
	token string
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.token == "" {
		return t.base.RoundTrip(req)
	}
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(clone)
}

// contentPart reduces one SDK content block to its wire type and, for text,
// its text.
func contentPart(content mcp.Content) ContentPart {
	if text, ok := content.(*mcp.TextContent); ok {
		return ContentPart{Type: "text", Text: text.Text}
	}
	raw, err := content.MarshalJSON()
	if err != nil {
		return ContentPart{Type: "unknown"}
	}
	var wire struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil || wire.Type == "" {
		return ContentPart{Type: "unknown"}
	}
	return ContentPart{Type: wire.Type}
}

// FlattenedResult is the broker's reply body for a successful proxy.
type FlattenedResult struct {
	Result  string `json:"result"`
	IsError bool   `json:"isError"`
}

// FlattenToolResult flattens an MCP CallToolResult's content to a single text
// string.
//
// Text parts are concatenated in order; a non-text part (image, resource, …)
// is noted by its type in brackets rather than dropped silently, so a caller
// sees that content existed it cannot render as text. IsError passes through.
func FlattenToolResult(result ToolCallResult) FlattenedResult {
	var b strings.Builder
	for _, part := range result.Content {
		if part.Type == "text" {
			b.WriteString(part.Text)
		} else {
			b.WriteString("[" + part.Type + "]")
		}
	}
	return FlattenedResult{Result: b.String(), IsError: result.IsError}
}
