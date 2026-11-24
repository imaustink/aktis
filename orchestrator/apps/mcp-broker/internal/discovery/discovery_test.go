package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/mcpclient"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/registry"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
)

const (
	testNS      = "ns"
	testGroup   = resource.Group
	testVersion = resource.Version
)

// searchIssuesID is the materialized id for the server+tool used throughout —
// derived, not hardcoded, since CatalogIDFor hashes a lossy remote name.
var searchIssuesID = resource.CatalogIDFor("github-mcp", resource.MCPToolExposure{RemoteToolName: "search_issues"})

func ptr[T any](v T) *T { return &v }

// fakeK8s is the in-memory API both writers go through: the dynamic client's
// fake object tracker, with the MCPServers seeded so their status subresource
// can be patched.
type fakeK8s struct {
	t      *testing.T
	client *dynamicfake.FakeDynamicClient
}

func newFakeK8s(t *testing.T, servers ...*resource.MCPServer) *fakeK8s {
	t.Helper()
	var objs []runtime.Object
	for _, s := range servers {
		u, err := toUnstructured(s)
		if err != nil {
			t.Fatal(err)
		}
		u.SetNamespace(testNS)
		objs = append(objs, u)
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		resource.MCPServerGVR(testGroup, testVersion): "MCPServerList",
		resource.MCPToolGVR(testGroup, testVersion):   "MCPToolList",
	}, objs...)
	return &fakeK8s{t: t, client: client}
}

func (f *fakeK8s) toolNames() []string {
	f.t.Helper()
	list, err := f.client.Resource(resource.MCPToolGVR(testGroup, testVersion)).Namespace(testNS).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		f.t.Fatal(err)
	}
	var names []string
	for _, item := range list.Items {
		names = append(names, item.GetName())
	}
	sort.Strings(names)
	return names
}

func (f *fakeK8s) tool(name string) *resource.MCPTool {
	f.t.Helper()
	u, err := f.client.Resource(resource.MCPToolGVR(testGroup, testVersion)).Namespace(testNS).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		f.t.Fatalf("get MCPTool %s: %v", name, err)
	}
	raw, _ := json.Marshal(u.Object)
	var tool resource.MCPTool
	if err := json.Unmarshal(raw, &tool); err != nil {
		f.t.Fatal(err)
	}
	return &tool
}

// rawSpec is a tool's spec as stored, so a test can see which keys exist.
func (f *fakeK8s) rawSpec(name string) map[string]any {
	f.t.Helper()
	u, err := f.client.Resource(resource.MCPToolGVR(testGroup, testVersion)).Namespace(testNS).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		f.t.Fatal(err)
	}
	spec, _, _ := unstructured.NestedMap(u.Object, "spec")
	return spec
}

type testStatus struct {
	DiscoveredTools []struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		InputSchema string `json:"inputSchema"`
		Exposed     *bool  `json:"exposed"`
	} `json:"discoveredTools"`
	ExposedTools       *int                 `json:"exposedTools"`
	ObservedGeneration *int64               `json:"observedGeneration"`
	Conditions         []resource.Condition `json:"conditions"`
}

func (f *fakeK8s) status(server string) testStatus {
	f.t.Helper()
	u, err := f.client.Resource(resource.MCPServerGVR(testGroup, testVersion)).Namespace(testNS).Get(context.Background(), server, metav1.GetOptions{})
	if err != nil {
		f.t.Fatal(err)
	}
	raw, _ := json.Marshal(u.Object["status"])
	var s testStatus
	_ = json.Unmarshal(raw, &s)
	return s
}

// fakeClient returns a fixed inventory, or fails to simulate unreachable.
type fakeClient struct {
	tools func(url string) ([]mcpclient.DiscoveredTool, error)
}

func (c fakeClient) ListTools(_ context.Context, url, _ string) ([]mcpclient.DiscoveredTool, error) {
	return c.tools(url)
}

func (fakeClient) CallTool(context.Context, mcpclient.CallRequest) (mcpclient.ToolCallResult, error) {
	return mcpclient.ToolCallResult{}, errors.New("not used in discovery tests")
}

func listing(tools []mcpclient.DiscoveredTool) fakeClient {
	return fakeClient{func(string) ([]mcpclient.DiscoveredTool, error) { return tools, nil }}
}

func unreachable(message string) fakeClient {
	return fakeClient{func(string) ([]mcpclient.DiscoveredTool, error) {
		return nil, &mcpclient.TransportError{Message: message}
	}}
}

func makeServer(identityProviders []string, exposure ...resource.MCPToolExposure) *resource.MCPServer {
	return &resource.MCPServer{
		APIVersion: "core.aktis.ai/v1alpha1",
		Kind:       "MCPServer",
		Metadata:   resource.ObjectMeta{Name: "github-mcp", UID: "srv-uid", Generation: ptr(int64(3))},
		Spec: resource.MCPServerSpec{
			Transport:         "streamable-http",
			URL:               "https://mcp.example/",
			IdentityProviders: identityProviders,
			Exposure:          exposure,
		},
	}
}

func discoveryWith(k *fakeK8s, client mcpclient.Client) *Discovery {
	return New(Options{
		Client: client,
		ToolWriter: NewToolWriter(ToolWriterOptions{
			Dynamic: k.client, Namespace: testNS, Group: testGroup, Version: testVersion,
		}),
		StatusWriter: NewStatusWriter(StatusWriterOptions{
			Dynamic: k.client, Namespace: testNS, Group: testGroup, Version: testVersion,
		}),
	})
}

func bind(cr *resource.MCPServer) registry.Binding {
	return registry.Binding{Name: cr.Metadata.Name, CR: cr, ServiceToken: "svc"}
}

var liveTools = []mcpclient.DiscoveredTool{
	{
		Name:        "search_issues",
		Description: "Search issues",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}},
	},
	{Name: "get_pull_request", Description: "Get a PR"},
}

func eng(name string, mutate ...func(*resource.MCPToolExposure)) resource.MCPToolExposure {
	e := resource.MCPToolExposure{RemoteToolName: name, AllowedRoles: []string{"eng"}}
	for _, m := range mutate {
		m(&e)
	}
	return e
}

var ctx = context.Background()

func TestDiscoveryMaterializes(t *testing.T) {
	t.Run("copies the server's defaultIdentity onto each MCPTool (docs/adr/0049 step 5)", func(t *testing.T) {
		server := makeServer([]string{"github"}, eng("search_issues"))
		server.Spec.DefaultIdentity = "agent"
		k := newFakeK8s(t, server)
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))
		if got := k.tool(searchIssuesID).Spec.DefaultIdentity; got != "agent" {
			t.Fatalf("defaultIdentity = %q", got)
		}
	})

	t.Run("materializes an exposed tool that the server advertises", func(t *testing.T) {
		server := makeServer([]string{"github"}, resource.MCPToolExposure{
			RemoteToolName: "search_issues", AllowedRoles: []string{"eng", "support"}, Tier: ptr("standard"),
		})
		k := newFakeK8s(t, server)
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))

		tool := k.tool(searchIssuesID)
		// description/inputSchema from the LIVE tool; roles/tier from the
		// EXPOSURE entry; identityProviders from the SERVER.
		spec := tool.Spec
		if spec.ServerRef != "github-mcp" || spec.RemoteToolName != "search_issues" || spec.Description != "Search issues" ||
			!reflect.DeepEqual(spec.AllowedRoles, []string{"eng", "support"}) || spec.Tier == nil || *spec.Tier != "standard" ||
			!reflect.DeepEqual(spec.IdentityProviders, []string{"github"}) {
			t.Fatalf("spec = %+v", spec)
		}
		var schemaValue any
		if spec.InputSchema == nil || json.Unmarshal([]byte(*spec.InputSchema), &schemaValue) != nil {
			t.Fatalf("inputSchema = %v", spec.InputSchema)
		}
		if !reflect.DeepEqual(schemaValue, liveTools[0].InputSchema) {
			t.Fatalf("inputSchema = %s", *spec.InputSchema)
		}
		// Owned by its server (cascade delete) and labeled for listing.
		if tool.Metadata.Labels[resource.ServerLabel] != "github-mcp" {
			t.Fatalf("labels = %v", tool.Metadata.Labels)
		}
		owner := tool.Metadata.OwnerReferences[0]
		if owner.Kind != "MCPServer" || owner.Name != "github-mcp" || owner.UID != "srv-uid" || !owner.Controller ||
			!owner.BlockOwnerDeletion || owner.APIVersion != "core.aktis.ai/v1alpha1" {
			t.Fatalf("owner = %+v", owner)
		}
		if tool.APIVersion != "core.aktis.ai/v1alpha1" || tool.Kind != "MCPTool" || tool.Metadata.Namespace != testNS {
			t.Fatalf("tool = %+v", tool)
		}
	})

	t.Run("copies approval from the exposure entry onto the materialized MCPTool (ADR 0003)", func(t *testing.T) {
		server := makeServer([]string{"github"}, eng("search_issues", func(e *resource.MCPToolExposure) { e.Approval = ptr("always") }))
		k := newFakeK8s(t, server)
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))
		if got := k.tool(searchIssuesID).Spec.Approval; got == nil || *got != "always" {
			t.Fatalf("approval = %v", got)
		}
	})

	t.Run("omits every optional field the exposure entry and server leave unset", func(t *testing.T) {
		// Existing servers are unaffected by fields they never set: the stored
		// spec carries no approval/hidden/tier/identityProviders/defaultIdentity
		// key at all, and an absent inputSchema stays absent.
		server := makeServer(nil, eng("get_pull_request"))
		k := newFakeK8s(t, server)
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))
		id := resource.CatalogIDFor("github-mcp", eng("get_pull_request"))
		spec := k.rawSpec(id)
		var keys []string
		for key := range spec {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if want := "allowedRoles,description,remoteToolName,serverRef"; strings.Join(keys, ",") != want {
			t.Fatalf("spec keys = %v, want %s", keys, want)
		}
	})

	t.Run("keeps an explicit hidden:false and an empty allowedRoles", func(t *testing.T) {
		server := makeServer(nil, resource.MCPToolExposure{RemoteToolName: "get_pull_request", Hidden: ptr(false)})
		k := newFakeK8s(t, server)
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))
		spec := k.rawSpec(resource.CatalogIDFor("github-mcp", server.Spec.Exposure[0]))
		if hidden, ok := spec["hidden"]; !ok || hidden != false {
			t.Fatalf("hidden = %v (present %v)", hidden, ok)
		}
		if roles, ok := spec["allowedRoles"].([]any); !ok || len(roles) != 0 {
			t.Fatalf("allowedRoles = %#v", spec["allowedRoles"])
		}
	})

	t.Run("falls back to the remote name when the live tool has no description", func(t *testing.T) {
		server := makeServer(nil, eng("bare"))
		k := newFakeK8s(t, server)
		discoveryWith(k, listing([]mcpclient.DiscoveredTool{{Name: "bare"}})).RunServer(ctx, bind(server))
		if got := k.tool(resource.CatalogIDFor("github-mcp", eng("bare"))).Spec.Description; got != "bare" {
			t.Fatalf("description = %q", got)
		}
	})
}

func TestDiscoveryExposureRules(t *testing.T) {
	t.Run("does not materialize an expose:false entry, but records it on status", func(t *testing.T) {
		server := makeServer(nil, eng("search_issues", func(e *resource.MCPToolExposure) { e.Expose = ptr(false) }))
		k := newFakeK8s(t, server)
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))

		if names := k.toolNames(); len(names) != 0 {
			t.Fatalf("tools = %v", names)
		}
		status := k.status("github-mcp")
		if status.ExposedTools == nil || *status.ExposedTools != 0 {
			t.Fatalf("exposedTools = %v", status.ExposedTools)
		}
		// The tool is still surfaced on status, marked not-exposed.
		for _, d := range status.DiscoveredTools {
			if d.Name == "search_issues" && (d.Exposed == nil || *d.Exposed) {
				t.Fatalf("search_issues exposed = %v", d.Exposed)
			}
		}
	})

	t.Run("does not materialize an exposure entry the server does not advertise", func(t *testing.T) {
		server := makeServer(nil, eng("no_such_tool"))
		k := newFakeK8s(t, server)
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))
		if names := k.toolNames(); len(names) != 0 {
			t.Fatalf("tools = %v", names)
		}
		if got := k.status("github-mcp").ExposedTools; got == nil || *got != 0 {
			t.Fatalf("exposedTools = %v", got)
		}
	})

	t.Run("writes the full inventory with exposed flags and the exposed count", func(t *testing.T) {
		server := makeServer(nil, eng("search_issues"))
		k := newFakeK8s(t, server)
		now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
		d := discoveryWith(k, listing(liveTools))
		d.opts.StatusWriter.opts.Now = func() time.Time { return now }
		d.RunServer(ctx, bind(server))

		status := k.status("github-mcp")
		var names []string
		exposed := map[string]bool{}
		for _, t := range status.DiscoveredTools {
			names = append(names, t.Name)
			exposed[t.Name] = t.Exposed != nil && *t.Exposed
		}
		if strings.Join(names, ",") != "search_issues,get_pull_request" {
			t.Fatalf("names = %v", names)
		}
		if !exposed["search_issues"] || exposed["get_pull_request"] {
			t.Fatalf("exposed = %v", exposed)
		}
		if status.DiscoveredTools[0].Description != "Search issues" ||
			status.DiscoveredTools[0].InputSchema != `{"properties":{"q":{"type":"string"}},"type":"object"}` {
			t.Fatalf("discovered[0] = %+v", status.DiscoveredTools[0])
		}
		if status.DiscoveredTools[1].InputSchema != "" {
			t.Fatalf("a tool with no schema got %q", status.DiscoveredTools[1].InputSchema)
		}
		if *status.ExposedTools != 1 || status.ObservedGeneration == nil || *status.ObservedGeneration != 3 {
			t.Fatalf("status = %+v", status)
		}
		want := resource.Condition{
			Type: "Ready", Status: "True", Reason: "DiscoverySucceeded", Message: "Listed tools from the server",
			LastTransitionTime: "2026-10-02T00:00:00.000Z", ObservedGeneration: ptr(int64(3)),
		}
		if !reflect.DeepEqual(status.Conditions, []resource.Condition{want}) {
			t.Fatalf("conditions = %+v", status.Conditions)
		}
	})
}

func TestDiscoveryExistenceRule(t *testing.T) {
	t.Run("deletes an owned tool the server no longer advertises (§6)", func(t *testing.T) {
		server := makeServer(nil, eng("search_issues"), eng("get_pull_request"))
		k := newFakeK8s(t, server)
		// First pass: both tools live and exposed.
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))
		if n := len(k.toolNames()); n != 2 {
			t.Fatalf("after first pass: %v", k.toolNames())
		}
		// Second pass: the server dropped get_pull_request from tools/list.
		discoveryWith(k, listing(liveTools[:1])).RunServer(ctx, bind(server))
		if names := k.toolNames(); !reflect.DeepEqual(names, []string{searchIssuesID}) {
			t.Fatalf("after second pass: %v", names)
		}
	})

	t.Run("deletes an owned tool whose exposure entry was withdrawn", func(t *testing.T) {
		exposed := makeServer(nil, eng("search_issues"))
		k := newFakeK8s(t, exposed)
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(exposed))
		if n := len(k.toolNames()); n != 1 {
			t.Fatalf("tools = %v", k.toolNames())
		}
		// Operator sets expose:false — the tool still lives on the server, but
		// permission was withdrawn, so the derived record must go.
		withdrawn := makeServer(nil, eng("search_issues", func(e *resource.MCPToolExposure) { e.Expose = ptr(false) }))
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(withdrawn))
		if names := k.toolNames(); len(names) != 0 {
			t.Fatalf("tools = %v", names)
		}
	})

	t.Run("patches a still-desired tool in place", func(t *testing.T) {
		server := makeServer(nil, eng("search_issues"))
		k := newFakeK8s(t, server)
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))

		changed := []mcpclient.DiscoveredTool{{Name: "search_issues", Description: "Search issues, v2"}}
		discoveryWith(k, listing(changed)).RunServer(ctx, bind(server))
		if got := k.tool(searchIssuesID).Spec.Description; got != "Search issues, v2" {
			t.Fatalf("description = %q", got)
		}
	})

	t.Run("leaves tools other servers own alone", func(t *testing.T) {
		server := makeServer(nil, eng("search_issues"))
		other := makeServer(nil, eng("get_pull_request"))
		other.Metadata.Name = "other-mcp"
		k := newFakeK8s(t, server, other)
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(other))
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))
		if n := len(k.toolNames()); n != 2 {
			t.Fatalf("tools = %v", k.toolNames())
		}
	})
}

func TestDiscoveryUnreachable(t *testing.T) {
	t.Run("records Ready=False and keeps existing tools when the server is unreachable", func(t *testing.T) {
		server := makeServer(nil, eng("search_issues"))
		k := newFakeK8s(t, server)
		// Seed a successful pass.
		discoveryWith(k, listing(liveTools)).RunServer(ctx, bind(server))
		if n := len(k.toolNames()); n != 1 {
			t.Fatalf("tools = %v", k.toolNames())
		}

		// Now the server cannot be reached: tools must NOT be deleted (unreachable
		// is "we do not know", not "nothing exists").
		var reported []string
		d := discoveryWith(k, unreachable("connection refused"))
		d.opts.OnError = func(s string, _ error) { reported = append(reported, s) }
		d.RunServer(ctx, bind(server))

		if n := len(k.toolNames()); n != 1 {
			t.Fatalf("tools = %v", k.toolNames())
		}
		status := k.status("github-mcp")
		c := status.Conditions[0]
		if c.Type != "Ready" || c.Status != "False" || c.Reason != "DiscoveryFailed" || c.Message != "connection refused" {
			t.Fatalf("condition = %+v", c)
		}
		// The last known inventory survives the blip.
		if len(status.DiscoveredTools) != 2 {
			t.Fatalf("discoveredTools = %+v", status.DiscoveredTools)
		}
		if !reflect.DeepEqual(reported, []string{"github-mcp"}) {
			t.Fatalf("reported = %v", reported)
		}
	})

	t.Run("prefixes a non-transport failure", func(t *testing.T) {
		server := makeServer(nil, eng("search_issues"))
		k := newFakeK8s(t, server)
		client := fakeClient{func(string) ([]mcpclient.DiscoveredTool, error) { return nil, errors.New("weird") }}
		discoveryWith(k, client).RunServer(ctx, bind(server))
		if got := k.status("github-mcp").Conditions[0].Message; got != "discovery failed: weird" {
			t.Fatalf("message = %q", got)
		}
	})

	t.Run("RunAll continues past a failing server", func(t *testing.T) {
		good := makeServer(nil, eng("search_issues"))
		bad := &resource.MCPServer{
			APIVersion: "core.aktis.ai/v1alpha1", Kind: "MCPServer",
			Metadata: resource.ObjectMeta{Name: "bad-mcp", UID: "bad-uid", Generation: ptr(int64(1))},
			Spec: resource.MCPServerSpec{
				Transport: "streamable-http", URL: "https://bad.example/",
				Exposure: []resource.MCPToolExposure{eng("search_issues")},
			},
		}
		k := newFakeK8s(t, good, bad)
		// Fails only for the bad server's url; the good one still materializes.
		client := fakeClient{func(url string) ([]mcpclient.DiscoveredTool, error) {
			if url == "https://bad.example/" {
				return nil, &mcpclient.TransportError{Message: "down"}
			}
			return liveTools, nil
		}}
		discoveryWith(k, client).RunAll(ctx, []registry.Binding{bind(bad), bind(good)})

		if names := k.toolNames(); !reflect.DeepEqual(names, []string{searchIssuesID}) {
			t.Fatalf("tools = %v", names)
		}
		if got := k.status("bad-mcp").Conditions[0].Status; got != "False" {
			t.Fatalf("bad status = %q", got)
		}
		if got := k.status("github-mcp").Conditions[0].Status; got != "True" {
			t.Fatalf("good status = %q", got)
		}
	})
}

func TestWritersReportRatherThanFail(t *testing.T) {
	// A tool name that already exists but is NOT owned by this server (no
	// label) collides on create; the failure is reported for that tool and the
	// status write still lands.
	server := makeServer(nil, eng("search_issues"))
	k := newFakeK8s(t, server)
	squatter := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "core.aktis.ai/v1alpha1", "kind": "MCPTool",
		"metadata": map[string]any{"name": searchIssuesID, "namespace": testNS},
		"spec":     map[string]any{"serverRef": "someone-else"},
	}}
	if _, err := k.client.Resource(resource.MCPToolGVR(testGroup, testVersion)).Namespace(testNS).Create(ctx, squatter, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var failed []string
	d := New(Options{
		Client: listing(liveTools),
		ToolWriter: NewToolWriter(ToolWriterOptions{
			Dynamic: k.client, Namespace: testNS, Group: testGroup, Version: testVersion,
			OnError: func(tool string, _ error) { failed = append(failed, tool) },
		}),
		StatusWriter: NewStatusWriter(StatusWriterOptions{Dynamic: k.client, Namespace: testNS, Group: testGroup, Version: testVersion}),
	})
	d.RunServer(ctx, bind(server))
	if !reflect.DeepEqual(failed, []string{searchIssuesID}) {
		t.Fatalf("failed = %v", failed)
	}
	if got := k.status("github-mcp").Conditions[0].Status; got != "True" {
		t.Fatalf("status = %q", got)
	}

	// A status patch against a server that does not exist is reported too.
	var statusFailed []string
	w := NewStatusWriter(StatusWriterOptions{
		Dynamic: k.client, Namespace: testNS, Group: testGroup, Version: testVersion,
		OnError: func(s string, _ error) { statusFailed = append(statusFailed, s) },
	})
	ghost := makeServer(nil)
	ghost.Metadata.Name = "ghost"
	w.RecordDegraded(ctx, ghost, "x")
	if !reflect.DeepEqual(statusFailed, []string{"ghost"}) {
		t.Fatalf("statusFailed = %v", statusFailed)
	}
}

func TestSerializeSchema(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want *string
	}{
		{"nil stays absent", nil, nil},
		{"object", map[string]any{"type": "object"}, ptr(`{"type":"object"}`)},
		{"does not HTML-escape", map[string]any{"description": "a<b & c>d"}, ptr(`{"description":"a<b & c>d"}`)},
		{"unserializable is dropped", map[string]any{"bad": func() {}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := serializeSchema(tc.in)
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Fatalf("serializeSchema = %v, want %v", deref(got), deref(tc.want))
			}
		})
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
