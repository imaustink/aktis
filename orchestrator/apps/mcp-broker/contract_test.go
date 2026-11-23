package main

// The broker's contract with the Go engine (ADR 0045), held hermetically: no
// cluster, no real MCP server. e2e/specs/mcp-broker-contract.e2e.ts runs these
// (`go test -run '^TestContract'`), so they are the cross-component guard the
// TypeScript spec used to be when the broker was TypeScript.
//
// The broker server, its discovery writer, and the Go engine's dispatch
// activity and catalog reader implement one contract — a URL, an
// `x-delegated-token` header, a `{arguments}` request, a `{result,isError}`
// reply, and the MCPTool CR's spec fields — and nothing but agreement makes a
// tool call reach a server and come back. Every past break in this repo was a
// cross-component rename that a unit suite, mocking the other half, waved
// through.
//
// Four boundaries:
//  1. The broker serves the route the Go engine calls (route, auth, which
//     credential is spent, errors), driven with requests shaped exactly like
//     mcptool.go's callThroughBroker.
//  2. Discovery's WRITE of a derived MCPTool carries only fields the Go
//     catalog reader decodes.
//  3. The existence rule (§6) through the real writer.
//  4. The Go engine's source addresses that same route and reply.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/auth"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/discovery"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/mcpclient"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/registry"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/server"
)

const (
	contractNS     = "e2e"
	brokerToken    = "broker-secret"
	serviceToken   = "discovery-service-cred"
	delegatedToken = "alice-delegated-token"
	contractServer = "github-mcp"
)

// engineRoot is the Temporal engine, read as source for boundaries 2 and 4.
var engineRoot = filepath.Join("..", "..", "engines", "temporal")

// fakeMCP stands in for an MCP server where the real broker would hold an
// SDKClient. It records what the broker asked and with which token — which is
// how a test proves the broker spent the CALLER's token, not the service
// credential.
type fakeMCP struct {
	mu        sync.Mutex
	tools     []mcpclient.DiscoveredTool
	reachable bool
	calls     []mcpclient.CallRequest
}

func (f *fakeMCP) ListTools(_ context.Context, u, _ string) ([]mcpclient.DiscoveredTool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.reachable {
		return nil, &mcpclient.TransportError{Message: "unreachable: " + u}
	}
	return append([]mcpclient.DiscoveredTool(nil), f.tools...), nil
}

func (f *fakeMCP) CallTool(_ context.Context, req mcpclient.CallRequest) (mcpclient.ToolCallResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	if req.Name == "explode" {
		return mcpclient.ToolCallResult{Content: []mcpclient.ContentPart{{Type: "text", Text: "the repo does not exist"}}, IsError: true}, nil
	}
	args, _ := json.Marshal(req.Arguments)
	return mcpclient.ToolCallResult{Content: []mcpclient.ContentPart{{Type: "text", Text: "echo:" + string(args)}}}, nil
}

func contractCR(identityProviders []string, exposure ...resource.MCPToolExposure) *resource.MCPServer {
	gen := int64(1)
	return &resource.MCPServer{
		APIVersion: resource.Group + "/" + resource.Version,
		Kind:       resource.MCPServerKind,
		Metadata:   resource.ObjectMeta{Name: contractServer, Namespace: contractNS, UID: "uid-1", Generation: &gen},
		Spec: resource.MCPServerSpec{
			Transport:         "streamable-http",
			URL:               "http://fake-mcp.e2e.svc:8080/mcp",
			IdentityProviders: identityProviders,
			Exposure:          exposure,
		},
	}
}

func roles(r ...string) []string { return r }

// engineCall sends one tools/call exactly as mcptool.go's callThroughBroker
// does: url-escaped path segments, a JSON {arguments} body, the broker bearer,
// and x-delegated-token only when the engine has one.
func engineCall(t *testing.T, base, srv, tool string, args map[string]any, delegated, token string) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"arguments": args})
	req, _ := http.NewRequest(http.MethodPost,
		base+"/servers/"+url.PathEscape(srv)+"/tools/"+url.PathEscape(tool)+"/call", bytes.NewReader(body))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if delegated != "" {
		req.Header.Set("x-delegated-token", delegated)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var parsed map[string]any
	_ = json.NewDecoder(res.Body).Decode(&parsed)
	return res.StatusCode, parsed
}

func TestContractBrokerServesTheEngineRoute(t *testing.T) {
	exposure := []resource.MCPToolExposure{
		{RemoteToolName: "search_issues", AllowedRoles: roles("engineering")},
		{RemoteToolName: "explode", AllowedRoles: roles("engineering")},
	}
	start := func(t *testing.T, identityProviders []string) (string, *fakeMCP) {
		mcp := &fakeMCP{reachable: true}
		ts := httptest.NewServer(server.New(server.Options{
			Auth: auth.Config{OrchestratorToken: brokerToken},
			Registry: registry.NewStatic(registry.Binding{
				Name: contractServer, CR: contractCR(identityProviders, exposure...), ServiceToken: serviceToken,
			}),
			Client: mcp,
		}))
		t.Cleanup(ts.Close)
		return ts.URL, mcp
	}

	t.Run("proxies a call, spending the caller's delegated token upstream", func(t *testing.T) {
		base, mcp := start(t, []string{"github"})
		status, body := engineCall(t, base, contractServer, "search_issues", map[string]any{"query": "is:open"}, delegatedToken, brokerToken)
		if status != 200 || body["result"] != `echo:{"query":"is:open"}` || body["isError"] != false {
			t.Fatalf("got %d %v", status, body)
		}
		// The broker spent the CALLER's delegated token upstream — never the
		// discovery/service credential (ADR 0045 §5).
		if len(mcp.calls) != 1 || mcp.calls[0].Token != delegatedToken || mcp.calls[0].Name != "search_issues" ||
			mcp.calls[0].Arguments["query"] != "is:open" {
			t.Fatalf("calls = %+v", mcp.calls)
		}
	})

	t.Run("spends the service credential for a no-identity server called with no delegated header", func(t *testing.T) {
		base, mcp := start(t, nil)
		if status, _ := engineCall(t, base, contractServer, "search_issues", map[string]any{}, "", brokerToken); status != 200 {
			t.Fatalf("status = %d", status)
		}
		if mcp.calls[0].Token != serviceToken {
			t.Fatalf("a no-identity server runs on the service credential; spent %q", mcp.calls[0].Token)
		}
	})

	t.Run("returns a tool-level error as isError, not a failed request", func(t *testing.T) {
		base, _ := start(t, []string{"github"})
		status, body := engineCall(t, base, contractServer, "explode", map[string]any{}, delegatedToken, brokerToken)
		if status != 200 || body["isError"] != true || !strings.Contains(body["result"].(string), "the repo does not exist") {
			t.Fatalf("got %d %v", status, body)
		}
	})

	refusals := []struct {
		name, server, tool, delegated, token string
		status                               int
	}{
		// Advertised by the server but NOT in the exposure map → refused.
		{"refuses an unexposed tool with a 404, never reaching the server", contractServer, "delete_repo", delegatedToken, brokerToken, 404},
		{"refuses an unknown server with a 404", "no-such-server", "search_issues", delegatedToken, brokerToken, 404},
		{"rejects a bad broker token with a 401", contractServer, "search_issues", delegatedToken, "wrong-token", 401},
		{"fails closed with a 403 when a per-user call carries no user", contractServer, "search_issues", "", brokerToken, 403},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			base, mcp := start(t, []string{"github"})
			status, body := engineCall(t, base, tc.server, tc.tool, map[string]any{}, tc.delegated, tc.token)
			if status != tc.status {
				t.Fatalf("status = %d, want %d", status, tc.status)
			}
			// The engine surfaces `message` as prose; every refusal carries one.
			if msg, _ := body["message"].(string); msg == "" {
				t.Fatalf("refusal without a message: %v", body)
			}
			if len(mcp.calls) != 0 {
				t.Fatalf("a refused call reached the server: %+v", mcp.calls)
			}
		})
	}
}

// contractK8s is the in-memory API discovery writes through.
func contractK8s(t *testing.T, srv *resource.MCPServer) *dynamicfake.FakeDynamicClient {
	t.Helper()
	raw, _ := json.Marshal(srv)
	var obj map[string]any
	_ = json.Unmarshal(raw, &obj)
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		resource.MCPServerGVR(resource.Group, resource.Version): "MCPServerList",
		resource.MCPToolGVR(resource.Group, resource.Version):   "MCPToolList",
	}, &unstructured.Unstructured{Object: obj})
}

func contractDiscovery(client *dynamicfake.FakeDynamicClient, mcp mcpclient.Client) *discovery.Discovery {
	return discovery.New(discovery.Options{
		Client: mcp,
		ToolWriter: discovery.NewToolWriter(discovery.ToolWriterOptions{
			Dynamic: client, Namespace: contractNS, Group: resource.Group, Version: resource.Version,
		}),
		StatusWriter: discovery.NewStatusWriter(discovery.StatusWriterOptions{
			Dynamic: client, Namespace: contractNS, Group: resource.Group, Version: resource.Version,
			Now: func() time.Time { return time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC) },
		}),
	})
}

func toolsIn(t *testing.T, client *dynamicfake.FakeDynamicClient) map[string]map[string]any {
	t.Helper()
	list, err := client.Resource(resource.MCPToolGVR(resource.Group, resource.Version)).Namespace(contractNS).
		List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]any{}
	for _, item := range list.Items {
		out[item.GetName()] = item.Object
	}
	return out
}

func keys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var (
	contractExposure = []resource.MCPToolExposure{
		{RemoteToolName: "search_issues", AllowedRoles: roles("engineering", "support"), Tier: ptrTo("standard")},
		{RemoteToolName: "secret_op", AllowedRoles: roles("engineering"), Hidden: ptrTo(true)},
		// Exposed but the server does not advertise it — materializes nothing.
		{RemoteToolName: "ghost_tool", AllowedRoles: roles("engineering")},
		// Advertised but withdrawn — in discoveredTools as exposed:false, no CR.
		{RemoteToolName: "list_repos", Expose: ptrTo(false), AllowedRoles: roles("engineering")},
	}
	searchID = resource.CatalogIDFor(contractServer, resource.MCPToolExposure{RemoteToolName: "search_issues"})
	secretID = resource.CatalogIDFor(contractServer, resource.MCPToolExposure{RemoteToolName: "secret_op"})
)

func ptrTo[T any](v T) *T { return &v }

func contractLiveTools() []mcpclient.DiscoveredTool {
	return []mcpclient.DiscoveredTool{
		{Name: "search_issues", Description: "Search issues", InputSchema: map[string]any{"type": "object"}},
		{Name: "secret_op", Description: "Privileged"},
		{Name: "list_repos", Description: "List repositories"},
	}
}

func TestContractDiscoveryWriteMatchesEngineRead(t *testing.T) {
	srv := contractCR([]string{"github"}, contractExposure...)
	client := contractK8s(t, srv)
	mcp := &fakeMCP{reachable: true, tools: contractLiveTools()}
	contractDiscovery(client, mcp).RunServer(context.Background(), registry.Binding{Name: contractServer, CR: srv, ServiceToken: serviceToken})

	// search_issues and secret_op are materialized; ghost_tool (not advertised)
	// and list_repos (expose:false) are not.
	tools := toolsIn(t, client)
	want := []string{searchID, secretID}
	sort.Strings(want)
	if got := keys(tools); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("materialized %v, want %v", got, want)
	}

	search := tools[searchID]
	spec := search["spec"].(map[string]any)
	// description/inputSchema from the LIVE tool; roles/tier from the EXPOSURE;
	// identityProviders from the SERVER.
	wantSpec := map[string]any{
		"serverRef": contractServer, "remoteToolName": "search_issues", "description": "Search issues",
		"inputSchema": `{"type":"object"}`, "allowedRoles": []any{"engineering", "support"}, "tier": "standard",
		"identityProviders": []any{"github"},
	}
	specJSON, _ := json.Marshal(spec)
	wantJSON, _ := json.Marshal(wantSpec)
	if string(specJSON) != string(wantJSON) {
		t.Fatalf("spec = %s\nwant   %s", specJSON, wantJSON)
	}
	// Owned by its server (cascade delete) and labelled (listable).
	meta := search["metadata"].(map[string]any)
	owner := meta["ownerReferences"].([]any)[0].(map[string]any)
	if owner["kind"] != "MCPServer" || owner["name"] != contractServer || owner["controller"] != true {
		t.Fatalf("owner = %v", owner)
	}
	if meta["labels"].(map[string]any)[resource.ServerLabel] != contractServer {
		t.Fatalf("labels = %v", meta["labels"])
	}
	// A hidden exposure carries hidden through.
	if tools[secretID]["spec"].(map[string]any)["hidden"] != true {
		t.Fatalf("secret_op spec = %v", tools[secretID]["spec"])
	}

	// The status is a REPORT of everything advertised, each marked exposed per
	// the map — a grant of nothing.
	got, _ := client.Resource(resource.MCPServerGVR(resource.Group, resource.Version)).Namespace(contractNS).
		Get(context.Background(), contractServer, metav1.GetOptions{})
	status := got.Object["status"].(map[string]any)
	if status["exposedTools"] != int64(2) && status["exposedTools"] != float64(2) {
		t.Fatalf("exposedTools = %#v", status["exposedTools"])
	}
	exposed := map[string]any{}
	for _, d := range status["discoveredTools"].([]any) {
		entry := d.(map[string]any)
		exposed[entry["name"].(string)] = entry["exposed"]
	}
	if exposed["search_issues"] != true || exposed["list_repos"] != false {
		t.Fatalf("exposed = %v", exposed)
	}
	if _, ok := exposed["secret_op"]; !ok {
		t.Fatalf("secret_op missing from discoveredTools: %v", exposed)
	}

	// The engine READS exactly what the broker WROTE: every spec field the
	// broker sets is one the Go catalog reader decodes (decode.go's
	// mcpToolSpec). Read as source, like boundary 4.
	decodeGo := readSource(t, filepath.Join(engineRoot, "internal", "catalog", "decode.go"))
	structAt := strings.Index(decodeGo, "type mcpToolSpec struct")
	if structAt < 0 {
		t.Fatal("decode.go no longer declares mcpToolSpec")
	}
	mcpToolSpecGo := decodeGo[structAt:]
	for field := range spec {
		if !regexp.MustCompile(`json:"` + regexp.QuoteMeta(field) + `[",]`).MatchString(mcpToolSpecGo) {
			t.Errorf("the Go reader does not decode spec.%s", field)
		}
	}
}

func TestContractExistenceRule(t *testing.T) {
	t.Run("deletes a materialized tool the server stops advertising (§6)", func(t *testing.T) {
		srv := contractCR([]string{"github"}, contractExposure...)
		client := contractK8s(t, srv)
		mcp := &fakeMCP{reachable: true, tools: contractLiveTools()}
		disco := contractDiscovery(client, mcp)
		binding := registry.Binding{Name: contractServer, CR: srv, ServiceToken: serviceToken}

		disco.RunServer(context.Background(), binding)
		if _, ok := toolsIn(t, client)[searchID]; !ok {
			t.Fatal("search_issues was not materialized")
		}
		// The server drops search_issues from its live list; the next pass
		// removes the derived tool rather than leaving a tombstone that fails
		// when invoked.
		mcp.tools = mcp.tools[1:]
		disco.RunServer(context.Background(), binding)
		tools := toolsIn(t, client)
		if _, ok := tools[searchID]; ok {
			t.Fatal("search_issues survived its removal from the live list")
		}
		if _, ok := tools[secretID]; !ok {
			t.Fatal("the still-live tool was removed")
		}
	})

	t.Run("marks a server Ready=False and retains its tools when it is unreachable", func(t *testing.T) {
		srv := contractCR([]string{"github"}, contractExposure...)
		client := contractK8s(t, srv)
		mcp := &fakeMCP{reachable: true, tools: contractLiveTools()}
		disco := contractDiscovery(client, mcp)
		binding := registry.Binding{Name: contractServer, CR: srv, ServiceToken: serviceToken}

		disco.RunServer(context.Background(), binding)
		before := keys(toolsIn(t, client))
		if len(before) != 2 {
			t.Fatalf("tools = %v", before)
		}
		// Unreachable is "we do not know", not "nothing exists": the tools stay.
		mcp.reachable = false
		disco.RunServer(context.Background(), binding)
		if after := keys(toolsIn(t, client)); strings.Join(after, ",") != strings.Join(before, ",") {
			t.Fatalf("tools changed: %v -> %v", before, after)
		}
		got, _ := client.Resource(resource.MCPServerGVR(resource.Group, resource.Version)).Namespace(contractNS).
			Get(context.Background(), contractServer, metav1.GetOptions{})
		cond := got.Object["status"].(map[string]any)["conditions"].([]any)[0].(map[string]any)
		if cond["type"] != "Ready" || cond["status"] != "False" {
			t.Fatalf("condition = %v", cond)
		}
	})
}

// The Go engine is the broker's only client (docs/adr/0053); its dispatch path
// is read as source. Crude on purpose: the string IS the contract, and the
// alternative to checking it is not checking it.
func TestContractEngineAddressesTheSameRoute(t *testing.T) {
	mcptoolGo := readSource(t, filepath.Join(engineRoot, "internal", "temporal", "activities", "mcptool.go"))
	checks := []struct {
		name    string
		pattern string
	}{
		{"builds the /servers/:server/tools/:tool/call route the broker serves", `/servers/%s/tools/%s/call"`},
		{"forwards the caller's token in the header the broker reads", `"` + server.DelegatedTokenHeader + `"`},
		{"reads the broker's result field", `json:"result"`},
		{"reads the broker's isError field", `json:"isError"`},
		{"reads the broker's refusal message", `json:"message`},
		// A NeedsLink path keyed on IdentityProviders: an unlinked caller gets a
		// link prompt, never a call on some other credential.
		{"keys the per-user path on IdentityProviders", `IdentityProviders`},
		{"fails closed into a link prompt", `NeedsLink`},
	}
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			if !strings.Contains(mcptoolGo, c.pattern) {
				t.Fatalf("mcptool.go does not contain %s", c.pattern)
			}
		})
	}

	// And the reply the broker sends decodes into the field names the engine
	// reads: a 200 body is exactly {result,isError}.
	reply, _ := json.Marshal(mcpclient.FlattenToolResult(mcpclient.ToolCallResult{}))
	if string(reply) != `{"result":"","isError":false}` {
		t.Fatalf("reply shape = %s", reply)
	}
}

func readSource(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s is missing: the contract test reads the engine from this repo checkout", path)
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
