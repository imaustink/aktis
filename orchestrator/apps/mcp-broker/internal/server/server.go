// Package server is the broker's invocation surface (ADR 0045 §3, §5):
//
//	POST /servers/:server/tools/:remoteTool/call
//	GET  /healthz
//
// It proxies ONE tools/call to the named server and maps the result back. The
// engines never speak MCP; they call this, and the protocol stays quarantined
// behind it.
//
// The auth model is deliberately strict and fails closed in two places:
//
//   - The caller must present the ORCHESTRATOR_TOKEN (package auth), or 401.
//   - If the server declares identityProviders, the call runs as the USER: the
//     caller must forward that user's token in `x-delegated-token`, or the call
//     is refused 403. It NEVER falls back to the discovery/service credential —
//     a per-user call quietly becoming a shared-identity call is exactly the
//     failure this design forbids (§5).
//
// All non-2xx bodies are `{ "message": "..." }`.
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/auth"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/mcpclient"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/registry"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
)

// DelegatedTokenHeader carries the calling user's per-user delegated token,
// per request.
const DelegatedTokenHeader = "x-delegated-token"

// maxBodyBytes bounds a request body: tool arguments are small; anything larger
// is not a tool call we proxy.
const maxBodyBytes = 1024 * 1024

// Options wires the handler.
type Options struct {
	Auth     auth.Config
	Registry registry.Registry
	Client   mcpclient.Client
}

// New returns the broker's HTTP handler.
func New(opts Options) http.Handler {
	return &handler{opts: opts}
}

type handler struct {
	opts Options
}

type message struct {
	Message string `json:"message"`
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("mcp-broker request failed: %v", p)
			send(w, http.StatusInternalServerError, message{"internal error"})
		}
	}()
	if err := h.handle(w, r); err != nil {
		log.Printf("mcp-broker request failed: %v", err)
		send(w, http.StatusInternalServerError, message{"internal error"})
	}
}

// handle serves one request; a returned error is an unexpected failure and
// becomes a 500.
func (h *handler) handle(w http.ResponseWriter, r *http.Request) error {
	if r.URL.Path == "/healthz" {
		send(w, http.StatusOK, map[string]bool{"ok": true})
		return nil
	}

	route, err := parseRoute(r.URL.EscapedPath())
	if err != nil {
		return err
	}
	if route == nil || r.Method != http.MethodPost {
		send(w, http.StatusNotFound, message{"not found"})
		return nil
	}

	// 1. Is this the engine at all? Fail closed on any other bearer.
	if err := auth.Authenticate(h.opts.Auth, r.Header.Get("Authorization")); err != nil {
		var unauthorized *auth.UnauthorizedError
		if errors.As(err, &unauthorized) {
			send(w, http.StatusUnauthorized, message{err.Error()})
			return nil
		}
		return err
	}

	// 2. Known server?
	binding, ok := h.opts.Registry.Get(route.server)
	if !ok {
		send(w, http.StatusNotFound, message{"no such server: " + route.server})
		return nil
	}
	server := binding.CR

	// 3. Known, EXPOSED tool? The operator's exposure map is truth for
	//    permission (§6): the broker refuses to proxy a tool the operator did
	//    not expose, even if the server advertises it — defense in depth behind
	//    the catalog's own RBAC filter.
	entry, ok := resource.ExposureByRemoteName(server).Get(route.remoteTool)
	if !ok || !resource.IsExposed(entry) {
		send(w, http.StatusNotFound, message{fmt.Sprintf("no such tool on %s: %s", route.server, route.remoteTool)})
		return nil
	}

	// 4. Which credential, and fail closed when a per-user call has no user.
	needsUser := len(server.Spec.IdentityProviders) > 0
	delegated := strings.TrimSpace(r.Header.Get(DelegatedTokenHeader))
	if needsUser && delegated == "" {
		send(w, http.StatusForbidden, message{fmt.Sprintf(
			"server %s runs tools as the calling user, but no %s was provided; "+
				"refusing to fall back to the discovery credential",
			route.server, DelegatedTokenHeader)})
		return nil
	}
	// A per-user server spends the delegated token; a shared server spends the
	// service credential (which may be empty, i.e. an unauthenticated call).
	token := binding.ServiceToken
	if needsUser {
		token = delegated
	}

	// 5. The arguments object from the body.
	args, err := readArguments(r)
	if err != nil {
		return err
	}

	// 6. Proxy the single tools/call.
	result, err := h.opts.Client.CallTool(r.Context(), mcpclient.CallRequest{
		URL:       server.Spec.URL,
		Token:     token,
		Name:      route.remoteTool,
		Arguments: args,
	})
	if err != nil {
		// A transport/protocol failure is the server's fault, not the caller's.
		var transport *mcpclient.TransportError
		if errors.As(err, &transport) {
			send(w, http.StatusBadGateway, message{transport.Message})
			return nil
		}
		return err
	}
	send(w, http.StatusOK, mcpclient.FlattenToolResult(result))
	return nil
}

type route struct {
	server     string
	remoteTool string
}

// parseRoute parses `/servers/:server/tools/:remoteTool/call` from the raw
// (still percent-encoded) path, so an encoded `/` stays inside its segment.
// Empty segments are ignored. It returns nil for any other path, and an error
// for a segment that is not valid percent-encoding.
func parseRoute(escapedPath string) (*route, error) {
	var parts []string
	for _, p := range strings.Split(escapedPath, "/") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) != 5 || parts[0] != "servers" || parts[2] != "tools" || parts[4] != "call" {
		return nil, nil
	}
	server, err := url.PathUnescape(parts[1])
	if err != nil {
		return nil, err
	}
	tool, err := url.PathUnescape(parts[3])
	if err != nil {
		return nil, err
	}
	return &route{server: server, remoteTool: tool}, nil
}

// readArguments returns the body's `arguments` object. A missing, malformed or
// non-object body (or `arguments`) yields an empty object; an over-limit body
// is an error.
func readArguments(r *http.Request) (map[string]any, error) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxBodyBytes {
		return nil, errors.New("request body too large")
	}
	var body struct {
		Arguments json.RawMessage `json:"arguments"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &body) != nil {
		return map[string]any{}, nil
	}
	// Numbers are kept as written (json.Number), so an argument reaches the
	// server exactly as the engine sent it.
	var args map[string]any
	dec := json.NewDecoder(bytes.NewReader(body.Arguments))
	dec.UseNumber()
	if len(body.Arguments) == 0 || dec.Decode(&args) != nil || args == nil {
		return map[string]any{}, nil
	}
	return args, nil
}

func send(w http.ResponseWriter, status int, body any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(body); err != nil {
		status = http.StatusInternalServerError
		buf.Reset()
		buf.WriteString(`{"message":"internal error"}`)
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(bytes.TrimRight(buf.Bytes(), "\n"))
}
