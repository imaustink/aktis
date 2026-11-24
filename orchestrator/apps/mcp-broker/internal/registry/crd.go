package registry

import (
	"context"
	"fmt"
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
)

// SecretReader reads one key from one Secret, returning "" when the Secret or
// the key does not exist. Injected so secret resolution stays testable.
type SecretReader func(ctx context.Context, namespace, name, key string) (string, error)

// KubeSecretReader reads Secrets through the core API.
func KubeSecretReader(client kubernetes.Interface) SecretReader {
	return func(ctx context.Context, namespace, name, key string) (string, error) {
		secret, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return "", nil
			}
			return "", err
		}
		return string(secret.Data[key]), nil
	}
}

// CRDOptions configures a CRD-backed registry.
type CRDOptions struct {
	Namespace string
	Group     string
	Version   string
	Dynamic   dynamic.Interface
	Secrets   SecretReader
	// OnChange is called after a server's binding changes (after the initial
	// list), so discovery can re-run for it.
	OnChange func(Binding)
	// OnDelete is called when a server is deleted.
	OnDelete func(name string)
	// OnError is reported to rather than failing: one broken server must not
	// take the others down.
	OnError func(server string, err error)
	// SyncTimeout bounds the initial list; 0 means defaultSyncTimeout. A
	// broker that cannot list MCPServers (CRD missing, RBAC 403) exits loudly
	// rather than sitting unready with the reason buried in a retry loop.
	SyncTimeout time.Duration
}

const defaultSyncTimeout = time.Minute

// CRD serves the broker's bindings from MCPServer custom resources (ADR 0045).
//
// Each binding carries the CR and the resolved DISCOVERY credential. The token
// is resolved from the server's `secretEnv` — the FIRST entry, used as the
// bearer for tools/list — so a Secret read never lands on the request path. A
// server with no `secretEnv` binds with no token, which is legitimate: a server
// that needs no credential to list its tools, or one that only ever runs as the
// user via `identityProviders`.
//
// A server that cannot resolve its secret is still BOUND (with no token) rather
// than omitted: discovery will then record it Degraded when tools/list fails,
// which is more legible than the server silently not existing — and invocation
// under `identityProviders` does not need the discovery credential at all.
//
// The watch is a client-go informer: it relists and rewatches on its own across
// apiserver watch timeouts, which is what makes the MCPServer catalog
// hot-reloadable (ADR 0020 posture) — a server created after startup is
// discovered as soon as its ADDED event arrives, no restart required.
type CRD struct {
	*Static
	opts CRDOptions
}

func NewCRD(opts CRDOptions) *CRD {
	return &CRD{Static: NewStatic(), opts: opts}
}

// Start loads every MCPServer and keeps the bindings current until ctx is done.
//
// It returns once the initial list has been bound. Servers bound from that
// initial list do NOT fire OnChange (the caller runs the first discovery sweep
// itself); every later add, and every update that changes a server's spec,
// does. An update that changes only status or metadata — including the status
// discovery itself writes — re-binds the CR quietly, so publishing status never
// re-triggers discovery.
func (r *CRD) Start(ctx context.Context) error {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(r.opts.Dynamic, 0, r.opts.Namespace, nil)
	informer := factory.ForResource(resource.MCPServerGVR(r.opts.Group, r.opts.Version)).Informer()

	if err := informer.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		r.reportError("(watch mcpservers)", err)
	}); err != nil {
		return fmt.Errorf("set mcpservers watch error handler: %w", err)
	}

	registration, err := informer.AddEventHandler(cache.ResourceEventHandlerDetailedFuncs{
		AddFunc: func(obj any, isInInitialList bool) {
			if server := r.decode(obj); server != nil {
				r.bind(ctx, server, !isInInitialList)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			server := r.decode(newObj)
			if server == nil {
				return
			}
			old := r.decode(oldObj)
			if old != nil && !specChanged(old, server) {
				r.rebindQuietly(server)
				return
			}
			r.bind(ctx, server, true)
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			u, ok := obj.(*unstructured.Unstructured)
			if !ok || u.GetName() == "" {
				return
			}
			r.remove(u.GetName())
			if r.opts.OnDelete != nil {
				r.opts.OnDelete(u.GetName())
			}
		},
	})
	if err != nil {
		return fmt.Errorf("add mcpservers event handler: %w", err)
	}

	factory.Start(ctx.Done())
	timeout := r.opts.SyncTimeout
	if timeout <= 0 {
		timeout = defaultSyncTimeout
	}
	syncCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), registration.HasSynced) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("could not list mcpservers in %s within %s (is the CRD installed and the Role bound?)",
			r.opts.Namespace, timeout)
	}
	return nil
}

// Refresh re-resolves every binding's discovery credential, so a Secret
// created or rotated after its server was bound is picked up by the next
// discovery pass rather than only on the next edit to the CR.
func (r *CRD) Refresh(ctx context.Context) {
	for _, b := range r.List() {
		token, err := r.resolveServiceToken(ctx, b.CR)
		if err != nil {
			r.reportError(b.Name, err)
		}
		// Only if the watch has not swapped in a newer CR meanwhile: that bind
		// resolved its own token.
		r.setTokenIfCurrent(b.Name, b.CR, token)
	}
}

func (r *CRD) decode(obj any) *resource.MCPServer {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok || u.GetName() == "" {
		return nil
	}
	server, err := resource.ServerFromUnstructured(u)
	if err != nil {
		r.reportError(u.GetName(), fmt.Errorf("decode MCPServer: %w", err))
		return nil
	}
	return server
}

func (r *CRD) bind(ctx context.Context, server *resource.MCPServer, fireChange bool) {
	token, err := r.resolveServiceToken(ctx, server)
	if err != nil {
		// Bound anyway, with no token (see the type comment): discovery will
		// surface the failure as Degraded rather than the server vanishing.
		r.reportError(server.Metadata.Name, err)
	}
	binding := Binding{Name: server.Metadata.Name, CR: server, ServiceToken: token}
	r.set(binding)
	if fireChange && r.opts.OnChange != nil {
		r.opts.OnChange(binding)
	}
}

// rebindQuietly swaps in the newer CR, keeping the resolved token — the spec
// (and so the secret reference) did not change.
func (r *CRD) rebindQuietly(server *resource.MCPServer) {
	token := ""
	if existing, ok := r.Get(server.Metadata.Name); ok {
		token = existing.ServiceToken
	}
	r.set(Binding{Name: server.Metadata.Name, CR: server, ServiceToken: token})
}

// resolveServiceToken returns the DISCOVERY credential: the FIRST `secretEnv`
// entry's value, read from a Secret in the server's OWN namespace (never a
// caller-supplied one). The first entry wins because the broker needs exactly
// one bearer for tools/list; a server that needs none leaves `secretEnv` empty
// and binds with no token.
func (r *CRD) resolveServiceToken(ctx context.Context, server *resource.MCPServer) (string, error) {
	if len(server.Spec.SecretEnv) == 0 {
		return "", nil
	}
	entry := server.Spec.SecretEnv[0]
	namespace := server.Metadata.Namespace
	if namespace == "" {
		namespace = r.opts.Namespace
	}
	value, err := r.opts.Secrets(ctx, namespace, entry.SecretRef.Name, entry.SecretRef.Key)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", fmt.Errorf("MCPServer %s: Secret %s/%s is missing or empty",
			server.Metadata.Name, entry.SecretRef.Name, entry.SecretRef.Key)
	}
	return value, nil
}

func (r *CRD) reportError(server string, err error) {
	if r.opts.OnError != nil {
		r.opts.OnError(server, err)
	}
}

// specChanged reports whether an update touched the server's desired state.
// The apiserver bumps metadata.generation on every spec change and never on a
// status-only one; with no generation recorded, the specs are compared.
func specChanged(old, updated *resource.MCPServer) bool {
	if old.Metadata.Generation != nil && updated.Metadata.Generation != nil {
		return *old.Metadata.Generation != *updated.Metadata.Generation
	}
	return !reflect.DeepEqual(old.Spec, updated.Spec)
}
