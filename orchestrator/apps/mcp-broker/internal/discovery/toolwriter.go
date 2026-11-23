package discovery

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
)

// DesiredTool is one MCPTool discovery wants to exist: its catalog id and the
// spec to stamp.
type DesiredTool struct {
	CatalogID string
	Spec      resource.MCPToolSpec
}

// ToolWriterOptions configures a ToolWriter.
type ToolWriterOptions struct {
	Dynamic   dynamic.Interface
	Namespace string
	Group     string
	Version   string
	// OnError is reported to, never fatal: one tool's write must not stop the
	// others (ADR 0045).
	OnError func(tool string, err error)
}

// ToolWriter creates, updates and deletes the derived MCPTool CRs an MCPServer
// owns (ADR 0045 §1, §6).
//
// Reconcile is the whole contract: given the set of tools discovery wants for
// one server, it makes the cluster match — creating the missing, updating the
// changed, and DELETING any MCPTool this server owns that is no longer desired.
// That delete is the existence rule (§6): a remote tool the server renamed,
// removed, or that the operator stopped exposing must leave the catalog rather
// than linger as a tombstone that fails only when invoked.
//
// Every derived tool carries an ownerReference to its MCPServer (so deleting
// the server cascades its tools) and a ServerLabel naming that server (so its
// tools can be listed without scanning ownerReferences).
type ToolWriter struct {
	opts ToolWriterOptions
}

func NewToolWriter(opts ToolWriterOptions) *ToolWriter { return &ToolWriter{opts: opts} }

func (w *ToolWriter) client() dynamic.ResourceInterface {
	return w.opts.Dynamic.Resource(resource.MCPToolGVR(w.opts.Group, w.opts.Version)).Namespace(w.opts.Namespace)
}

// Reconcile makes the server's owned MCPTools match desired. It fails only
// when the owned set cannot be listed; individual writes are reported and
// skipped.
func (w *ToolWriter) Reconcile(ctx context.Context, server *resource.MCPServer, desired []DesiredTool) error {
	owned, err := w.listOwned(ctx, server.Metadata.Name)
	if err != nil {
		return fmt.Errorf("list MCPTools owned by %s: %w", server.Metadata.Name, err)
	}
	ownedNames := map[string]bool{}
	for _, name := range owned {
		ownedNames[name] = true
	}
	desiredNames := map[string]bool{}
	for _, d := range desired {
		desiredNames[d.CatalogID] = true
	}

	// Deletes first (the existence rule): a tool no longer desired is removed
	// whether the server dropped it or the operator un-exposed it.
	for _, name := range owned {
		if !desiredNames[name] {
			w.delete(ctx, name)
		}
	}

	// Then upserts. A name already present is patched (the live tool's schema or
	// the operator's roles may have changed); a new one is created.
	for _, want := range desired {
		body := w.toolObject(server, want)
		if ownedNames[want.CatalogID] {
			w.patch(ctx, want.CatalogID, body)
		} else {
			w.create(ctx, want.CatalogID, body)
		}
	}
	return nil
}

// listOwned returns the names of every MCPTool in the namespace this server
// owns, by its label.
func (w *ToolWriter) listOwned(ctx context.Context, server string) ([]string, error) {
	list, err := w.client().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, item := range list.Items {
		if item.GetLabels()[resource.ServerLabel] == server {
			names = append(names, item.GetName())
		}
	}
	return names, nil
}

func (w *ToolWriter) toolObject(server *resource.MCPServer, want DesiredTool) resource.MCPTool {
	ownerAPIVersion := server.APIVersion
	if ownerAPIVersion == "" {
		ownerAPIVersion = resource.Group + "/" + resource.Version
	}
	return resource.MCPTool{
		APIVersion: w.opts.Group + "/" + w.opts.Version,
		Kind:       resource.MCPToolKind,
		Metadata: resource.MCPToolMeta{
			Name:      want.CatalogID,
			Namespace: w.opts.Namespace,
			Labels:    map[string]string{resource.ServerLabel: server.Metadata.Name},
			OwnerReferences: []resource.OwnerReference{{
				APIVersion:         ownerAPIVersion,
				Kind:               resource.MCPServerKind,
				Name:               server.Metadata.Name,
				UID:                server.Metadata.UID,
				Controller:         true,
				BlockOwnerDeletion: true,
			}},
		},
		Spec: want.Spec,
	}
}

func (w *ToolWriter) create(ctx context.Context, name string, body resource.MCPTool) {
	obj, err := toUnstructured(body)
	if err == nil {
		_, err = w.client().Create(ctx, obj, metav1.CreateOptions{})
	}
	w.report(name, err)
}

func (w *ToolWriter) patch(ctx context.Context, name string, body resource.MCPTool) {
	// A merge patch of metadata+spec: labels and ownerReferences are restamped
	// (so a hand-edit that stripped them is corrected) and the spec is brought
	// to the desired state. Status is a subresource and is left untouched.
	patch, err := json.Marshal(struct {
		Metadata resource.MCPToolMeta `json:"metadata"`
		Spec     resource.MCPToolSpec `json:"spec"`
	}{body.Metadata, body.Spec})
	if err == nil {
		_, err = w.client().Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	}
	w.report(name, err)
}

func (w *ToolWriter) delete(ctx context.Context, name string) {
	w.report(name, w.client().Delete(ctx, name, metav1.DeleteOptions{}))
}

func (w *ToolWriter) report(name string, err error) {
	if err != nil && w.opts.OnError != nil {
		w.opts.OnError(name, err)
	}
}

func toUnstructured(v any) (*unstructured.Unstructured, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: obj}, nil
}
