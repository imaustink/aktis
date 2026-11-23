package discovery

import (
	"context"
	"encoding/json"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
)

// StatusWriterOptions configures a StatusWriter.
type StatusWriterOptions struct {
	Dynamic   dynamic.Interface
	Namespace string
	Group     string
	Version   string
	// Now defaults to time.Now.
	Now     func() time.Time
	OnError func(server string, err error)
}

// StatusWriter publishes discovery results back onto an MCPServer's status
// (ADR 0045 §4).
//
// `discoveredTools` is the full inventory the server advertised — a REPORT,
// not a grant — each marked `exposed` per the operator's map. A successful pass
// sets a Ready condition; a server the broker could not reach or list gets a
// Ready=False condition with the reason, and NO change to discoveredTools, so
// the last known inventory is not thrown away because the server blipped.
//
// A failure to write status is REPORTED, never fatal: the discovery already
// happened and the MCPTools are already materialized; failing the pass
// because the bookkeeping did not land would throw away real work to protect a
// status blob (same discipline as connection-broker).
//
// Every patch is an explicit JSON merge patch on the status subresource.
type StatusWriter struct {
	opts StatusWriterOptions
}

func NewStatusWriter(opts StatusWriterOptions) *StatusWriter { return &StatusWriter{opts: opts} }

// readyStatus is the status a successful pass writes.
type readyStatus struct {
	DiscoveredTools    []resource.MCPDiscoveredTool `json:"discoveredTools"`
	ExposedTools       int                          `json:"exposedTools"`
	ObservedGeneration *int64                       `json:"observedGeneration,omitempty"`
	Conditions         []resource.Condition         `json:"conditions"`
}

// degradedStatus leaves discoveredTools and exposedTools alone.
type degradedStatus struct {
	ObservedGeneration *int64               `json:"observedGeneration,omitempty"`
	Conditions         []resource.Condition `json:"conditions"`
}

// RecordReady records a successful discovery.
func (w *StatusWriter) RecordReady(ctx context.Context, server *resource.MCPServer, discovered []resource.MCPDiscoveredTool, exposedTools int) {
	if discovered == nil {
		discovered = []resource.MCPDiscoveredTool{}
	}
	w.patch(ctx, server, readyStatus{
		DiscoveredTools:    discovered,
		ExposedTools:       exposedTools,
		ObservedGeneration: server.Metadata.Generation,
		Conditions: []resource.Condition{
			w.condition(server, "True", "DiscoverySucceeded", "Listed tools from the server"),
		},
	})
}

// RecordDegraded records a server the broker could not reach or list.
func (w *StatusWriter) RecordDegraded(ctx context.Context, server *resource.MCPServer, message string) {
	w.patch(ctx, server, degradedStatus{
		ObservedGeneration: server.Metadata.Generation,
		Conditions:         []resource.Condition{w.condition(server, "False", "DiscoveryFailed", message)},
	})
}

func (w *StatusWriter) condition(server *resource.MCPServer, status, reason, message string) resource.Condition {
	now := time.Now
	if w.opts.Now != nil {
		now = w.opts.Now
	}
	return resource.Condition{
		Type:               "Ready",
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: FormatTime(now()),
		ObservedGeneration: server.Metadata.Generation,
	}
}

// FormatTime renders t as an RFC 3339 UTC timestamp with milliseconds — the
// form the TypeScript broker wrote (Date.toISOString()).
func FormatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func (w *StatusWriter) patch(ctx context.Context, server *resource.MCPServer, status any) {
	body, err := json.Marshal(map[string]any{"status": status})
	if err == nil {
		_, err = w.opts.Dynamic.
			Resource(resource.MCPServerGVR(w.opts.Group, w.opts.Version)).
			Namespace(w.opts.Namespace).
			Patch(ctx, server.Metadata.Name, types.MergePatchType, body, metav1.PatchOptions{}, "status")
	}
	if err != nil && w.opts.OnError != nil {
		w.opts.OnError(server.Metadata.Name, err)
	}
}
