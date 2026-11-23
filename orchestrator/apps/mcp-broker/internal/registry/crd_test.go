package registry

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
)

const ns = "mcp"

func mcpServer(name string, generation int64, secretName string) *unstructured.Unstructured {
	spec := map[string]any{
		"transport": "streamable-http",
		"url":       "https://" + name + ".example/",
		"exposure":  []any{map[string]any{"remoteToolName": "t", "allowedRoles": []any{"eng"}}},
	}
	if secretName != "" {
		spec["secretEnv"] = []any{
			map[string]any{"name": "TOKEN", "secretRef": map[string]any{"name": secretName, "key": "token"}},
			map[string]any{"name": "OTHER", "secretRef": map[string]any{"name": "ignored", "key": "token"}},
		}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "core.aktis.ai/v1alpha1",
		"kind":       "MCPServer",
		"metadata":   map[string]any{"name": name, "namespace": ns, "generation": generation, "uid": name + "-uid"},
		"spec":       spec,
	}}
}

func secret(name, value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string][]byte{"token": []byte(value)},
	}
}

type recorder struct {
	mu      sync.Mutex
	changes []string
	deletes []string
	errors  []string
}

func (r *recorder) snapshot() (changes, deletes, errs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.changes...), append([]string(nil), r.deletes...), append([]string(nil), r.errors...)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func setup(t *testing.T, objects ...runtime.Object) (*CRD, *dynamicfake.FakeDynamicClient, *kubefake.Clientset, *recorder) {
	t.Helper()
	var servers []runtime.Object
	var secrets []runtime.Object
	for _, o := range objects {
		if _, ok := o.(*corev1.Secret); ok {
			secrets = append(secrets, o)
		} else {
			servers = append(servers, o)
		}
	}
	gvr := resource.MCPServerGVR(resource.Group, resource.Version)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{gvr: "MCPServerList"}, servers...)
	kube := kubefake.NewClientset(secrets...)
	rec := &recorder{}
	reg := NewCRD(CRDOptions{
		Namespace: ns, Group: resource.Group, Version: resource.Version,
		Dynamic: dyn, Secrets: KubeSecretReader(kube),
		OnChange: func(b Binding) { rec.mu.Lock(); rec.changes = append(rec.changes, b.Name); rec.mu.Unlock() },
		OnDelete: func(n string) { rec.mu.Lock(); rec.deletes = append(rec.deletes, n); rec.mu.Unlock() },
		OnError: func(s string, err error) {
			rec.mu.Lock()
			rec.errors = append(rec.errors, s+": "+err.Error())
			rec.mu.Unlock()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := reg.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return reg, dyn, kube, rec
}

func TestCRDRegistryInitialLoad(t *testing.T) {
	reg, _, _, rec := setup(t,
		mcpServer("with-secret", 1, "svc"),
		mcpServer("no-secret", 1, ""),
		mcpServer("missing-secret", 1, "absent"),
		secret("svc", "discovery-cred"),
		secret("ignored", "never-used"),
	)

	cases := []struct {
		server, token string
	}{
		// The FIRST secretEnv entry is the discovery credential.
		{"with-secret", "discovery-cred"},
		// No secretEnv is legitimate: bound with no token.
		{"no-secret", ""},
		// An unresolvable secret still binds (with no token) rather than vanishing.
		{"missing-secret", ""},
	}
	for _, tc := range cases {
		t.Run(tc.server, func(t *testing.T) {
			b, ok := reg.Get(tc.server)
			if !ok {
				t.Fatalf("%s not bound", tc.server)
			}
			if b.ServiceToken != tc.token {
				t.Fatalf("token = %q, want %q", b.ServiceToken, tc.token)
			}
			if b.CR.Spec.URL != "https://"+tc.server+".example/" {
				t.Fatalf("cr = %+v", b.CR)
			}
		})
	}

	changes, _, errs := rec.snapshot()
	// The initial list does not fire OnChange: the caller runs the first sweep.
	if len(changes) != 0 {
		t.Fatalf("initial load fired OnChange: %v", changes)
	}
	if len(errs) != 1 || !strings.Contains(errs[0], "MCPServer missing-secret: Secret absent/token is missing or empty") {
		t.Fatalf("errors = %v", errs)
	}
	if n := len(reg.List()); n != 3 {
		t.Fatalf("List() = %d bindings", n)
	}
}

func TestCRDRegistryWatch(t *testing.T) {
	reg, dyn, kube, rec := setup(t, mcpServer("a", 1, ""))
	ctx := context.Background()
	gvr := resource.MCPServerGVR(resource.Group, resource.Version)
	client := dyn.Resource(gvr).Namespace(ns)

	// A server created after startup is bound and fires OnChange.
	if _, err := kube.CoreV1().Secrets(ns).Create(ctx, secret("b-cred", "b-token"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Create(ctx, mcpServer("b", 1, "b-cred"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b bound", func() bool {
		b, ok := reg.Get("b")
		return ok && b.ServiceToken == "b-token"
	})
	eventually(t, "OnChange(b)", func() bool { c, _, _ := rec.snapshot(); return len(c) == 1 && c[0] == "b" })

	// A status-only update (same generation) re-binds quietly: publishing
	// status must never re-trigger discovery.
	statusOnly := mcpServer("a", 1, "")
	statusOnly.Object["status"] = map[string]any{"exposedTools": int64(1)}
	if _, err := client.Update(ctx, statusOnly, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	// A spec change (generation bump) fires.
	changed := mcpServer("a", 2, "")
	changed.Object["spec"].(map[string]any)["url"] = "https://moved.example/"
	if _, err := client.Update(ctx, changed, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a re-bound with the new url", func() bool {
		b, _ := reg.Get("a")
		return b.CR.Spec.URL == "https://moved.example/"
	})
	eventually(t, "OnChange(a) once", func() bool { c, _, _ := rec.snapshot(); return len(c) == 2 })
	if c, _, _ := rec.snapshot(); c[1] != "a" {
		t.Fatalf("changes = %v", c)
	}

	// Deleting a server unbinds it.
	if err := client.Delete(ctx, "b", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b unbound", func() bool { _, ok := reg.Get("b"); return !ok })
	if _, d, _ := rec.snapshot(); len(d) != 1 || d[0] != "b" {
		t.Fatalf("deletes = %v", d)
	}
}

func TestCRDRegistryRefreshPicksUpALateSecret(t *testing.T) {
	reg, _, kube, _ := setup(t, mcpServer("late", 1, "late-cred"))
	if b, _ := reg.Get("late"); b.ServiceToken != "" {
		t.Fatalf("token before the Secret exists = %q", b.ServiceToken)
	}
	if _, err := kube.CoreV1().Secrets(ns).Create(context.Background(), secret("late-cred", "now-here"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	reg.Refresh(context.Background())
	if b, _ := reg.Get("late"); b.ServiceToken != "now-here" {
		t.Fatalf("token after Refresh = %q", b.ServiceToken)
	}
}

func TestSpecChanged(t *testing.T) {
	gen := func(g int64) *int64 { return &g }
	base := resource.MCPServer{Spec: resource.MCPServerSpec{URL: "u"}}
	cases := []struct {
		name     string
		old, new resource.MCPServer
		want     bool
	}{
		{"same generation", withGen(base, gen(1)), withGen(base, gen(1)), false},
		{"bumped generation", withGen(base, gen(1)), withGen(base, gen(2)), true},
		{"no generation, same spec", base, base, false},
		{"no generation, different spec", base, resource.MCPServer{Spec: resource.MCPServerSpec{URL: "v"}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := specChanged(&tc.old, &tc.new); got != tc.want {
				t.Fatalf("specChanged = %v, want %v", got, tc.want)
			}
		})
	}
}

func withGen(s resource.MCPServer, g *int64) resource.MCPServer {
	s.Metadata.Generation = g
	return s
}

func TestCRDRegistryFailsLoudlyWhenItCannotList(t *testing.T) {
	gvr := resource.MCPServerGVR(resource.Group, resource.Version)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{gvr: "MCPServerList"})
	// The shape of a Role missing the mcpservers verbs.
	dyn.PrependReactor("list", "mcpservers", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(gvr.GroupResource(), "", errors.New("rbac"))
	})
	reported := make(chan string, 16)
	reg := NewCRD(CRDOptions{
		Namespace: ns, Group: resource.Group, Version: resource.Version,
		Dynamic: dyn, Secrets: KubeSecretReader(kubefake.NewClientset()),
		OnError: func(server string, _ error) {
			select {
			case reported <- server:
			default:
			}
		},
		SyncTimeout: 300 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := reg.Start(ctx)
	if err == nil || !strings.Contains(err.Error(), "could not list mcpservers in mcp") {
		t.Fatalf("Start = %v", err)
	}
	// The cause is reported, not swallowed by the retry loop.
	select {
	case server := <-reported:
		if server != "(watch mcpservers)" {
			t.Fatalf("reported as %q", server)
		}
	default:
		t.Fatal("the list failure was never reported")
	}
}
