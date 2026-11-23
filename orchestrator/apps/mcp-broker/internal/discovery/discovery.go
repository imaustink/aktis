// Package discovery is the controller half of the broker (ADR 0045 §3, §4,
// §6).
//
// For each MCPServer — on load, on change, and on a periodic resync — it opens
// an MCP client under the server's DISCOVERY credential, lists the server's
// tools, and does two things with the result:
//
//  1. Writes the full inventory to `status.discoveredTools`, each tool marked
//     `exposed` per the operator's map. This grants nothing; it only surfaces
//     what the server offers so an operator can decide what to expose.
//  2. Materializes an MCPTool for every exposure entry (expose != false) whose
//     remote tool is actually in the live list, and DELETES any owned MCPTool
//     whose remote tool left the list or whose exposure was withdrawn.
//
// The division of authority is the point (§6): the LIVE SERVER decides what
// exists, the OPERATOR decides what is permitted. So a tool materializes only
// when both agree — the server still advertises it AND an exposure entry names
// it — and a reappearing tool re-enters discoveredTools but is re-materialized
// only if the exposure map still covers it.
//
// One bad server must never stop the others: every per-server failure is
// caught, recorded as Ready=False, and left behind. Crucially, a server the
// broker could not reach does NOT have its tools deleted — unreachable is "we
// do not know", not "nothing exists", and deleting on a blip would flap the
// whole catalog.
package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/mcpclient"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/registry"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
)

// Options configures a Discovery.
type Options struct {
	Client       mcpclient.Client
	ToolWriter   *ToolWriter
	StatusWriter *StatusWriter
	OnError      func(server string, err error)
}

// Discovery runs discovery passes. Passes for one server are serialized (a
// watch event and the periodic resync can otherwise race on the same CRs);
// passes for different servers are independent.
type Discovery struct {
	opts  Options
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func New(opts Options) *Discovery {
	return &Discovery{opts: opts, locks: map[string]*sync.Mutex{}}
}

// RunAll runs discovery for every bound server, never failing for any one of
// them.
func (d *Discovery) RunAll(ctx context.Context, bindings []registry.Binding) {
	for _, binding := range bindings {
		if ctx.Err() != nil {
			return
		}
		d.RunServer(ctx, binding)
	}
}

// RunServer runs one discovery pass for one server.
func (d *Discovery) RunServer(ctx context.Context, binding registry.Binding) {
	lock := d.lockFor(binding.Name)
	lock.Lock()
	defer lock.Unlock()

	server := binding.CR
	name := server.Metadata.Name

	tools, err := d.opts.Client.ListTools(ctx, server.Spec.URL, binding.ServiceToken)
	if err != nil {
		// Unreachable or protocol error: record Ready=False and STOP. The
		// existing MCPTools are deliberately left in place — the live server is
		// truth for existence, and right now we cannot see the live server.
		message := "discovery failed: " + err.Error()
		var transport *mcpclient.TransportError
		if errors.As(err, &transport) {
			message = transport.Message
		}
		d.opts.StatusWriter.RecordDegraded(ctx, server, message)
		d.report(name, err)
		return
	}

	exposure := resource.ExposureByRemoteName(server)

	// The inventory, in the order the server reported it, each marked exposed
	// only when an entry names it AND that entry is not withdrawn.
	discovered := make([]resource.MCPDiscoveredTool, 0, len(tools))
	byName := map[string]mcpclient.DiscoveredTool{}
	for _, tool := range tools {
		entry, ok := exposure.Get(tool.Name)
		schema := serializeSchema(tool.InputSchema)
		item := resource.MCPDiscoveredTool{
			Name:        tool.Name,
			Description: tool.Description,
			Exposed:     ok && resource.IsExposed(entry),
		}
		if schema != nil {
			item.InputSchema = *schema
		}
		discovered = append(discovered, item)
		byName[tool.Name] = tool
	}

	// What should exist: an exposure entry that is exposed AND whose remote
	// tool the server actually advertised right now. An entry naming a tool the
	// server does not list materializes nothing (and is not in discoveredTools).
	var desired []DesiredTool
	for _, remoteToolName := range exposure.Keys() {
		entry, _ := exposure.Get(remoteToolName)
		if !resource.IsExposed(entry) {
			continue
		}
		live, ok := byName[remoteToolName]
		if !ok {
			continue
		}
		desired = append(desired, DesiredTool{
			CatalogID: resource.CatalogIDFor(name, entry),
			Spec:      desiredSpec(server, entry, live),
		})
	}

	// Materialize/delete first so the catalog is current, then publish status.
	if err := d.opts.ToolWriter.Reconcile(ctx, server, desired); err != nil {
		d.report(name, err)
		return
	}
	d.opts.StatusWriter.RecordReady(ctx, server, discovered, len(desired))
}

// desiredSpec builds one derived MCPTool's spec. description/inputSchema come
// from the LIVE tool (what it is); allowedRoles/hidden/tier/approval from the
// EXPOSURE entry (what the operator permits); identityProviders and
// defaultIdentity from the SERVER (how to run as the user).
func desiredSpec(server *resource.MCPServer, entry resource.MCPToolExposure, live mcpclient.DiscoveredTool) resource.MCPToolSpec {
	description := live.Description
	if description == "" {
		description = live.Name
	}
	allowedRoles := entry.AllowedRoles
	if allowedRoles == nil {
		allowedRoles = []string{}
	}
	spec := resource.MCPToolSpec{
		ServerRef:       server.Metadata.Name,
		RemoteToolName:  live.Name,
		Description:     description,
		InputSchema:     serializeSchema(live.InputSchema),
		AllowedRoles:    allowedRoles,
		Hidden:          entry.Hidden,
		Tier:            entry.Tier,
		Approval:        entry.Approval,
		DefaultIdentity: server.Spec.DefaultIdentity,
	}
	if len(server.Spec.IdentityProviders) > 0 {
		spec.IdentityProviders = server.Spec.IdentityProviders
	}
	return spec
}

// serializeSchema serializes a tool's input schema to the JSON string the CRD
// stores.
//
// nil stays nil (the field is optional); anything else is marshaled verbatim
// so an operator sees what the server advertised. A value that cannot be
// serialized is dropped rather than failing — a broken schema must not fail the
// whole server's discovery.
func serializeSchema(schema any) *string {
	if schema == nil {
		return nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(schema); err != nil {
		return nil
	}
	out := string(bytes.TrimRight(buf.Bytes(), "\n"))
	if out == "null" {
		return nil
	}
	return &out
}

func (d *Discovery) lockFor(name string) *sync.Mutex {
	d.mu.Lock()
	defer d.mu.Unlock()
	lock, ok := d.locks[name]
	if !ok {
		lock = &sync.Mutex{}
		d.locks[name] = lock
	}
	return lock
}

func (d *Discovery) report(server string, err error) {
	if d.opts.OnError != nil {
		d.opts.OnError(server, err)
	}
}
