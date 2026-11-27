// Package resource holds the MCPServer and MCPTool custom resources as the
// broker sees them, mirroring
// orchestrator/controllers/core-controller/api/v1alpha1/mcpserver_types.go and
// mcptool_types.go. Only the fields the BROKER reads or writes are declared.
//
// MCPServer is operator-authored desired state; MCPTool is derived state the
// broker WRITES (ADR 0045 §1). The split is the whole point — the exposure map
// on the server is the operator's permission grant, and the materialized tool
// is machine output gated by it — so this package keeps the two shapes and the
// rules that turn one into the other in one place.
//
// Optional fields whose PRESENCE matters (an explicit `hidden: false`, an
// explicit `tier: ""`) are pointers, so "absent" and "zero" stay distinct
// exactly as `undefined` and a value did in the TypeScript broker this
// replaced: the derived MCPTool carries a field iff the exposure entry set it.
package resource

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	Group           = "core.aktis.ai"
	Version         = "v1alpha1"
	MCPServerPlural = "mcpservers"
	MCPToolPlural   = "mcptools"
	MCPServerKind   = "MCPServer"
	MCPToolKind     = "MCPTool"

	// ServerLabel is stamped on every derived MCPTool, so a server's tools can
	// be listed without scanning ownerReferences.
	ServerLabel = "mcpserver.core.aktis.ai/name"

	maxK8sName = 253
)

// MCPServerGVR and MCPToolGVR are the resources for a given group/version
// (CRD_GROUP / CRD_VERSION are configurable, so these are functions).
func MCPServerGVR(group, version string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Version: version, Resource: MCPServerPlural}
}

func MCPToolGVR(group, version string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Version: version, Resource: MCPToolPlural}
}

// SecretKeySelector is a Secret key reference, as on every CR that carries
// `secretEnv`.
type SecretKeySelector struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type SecretEnvVar struct {
	Name      string            `json:"name"`
	SecretRef SecretKeySelector `json:"secretRef"`
}

// MCPToolExposure is one entry in MCPServer.spec.exposure: the operator's
// decision to expose a remote tool, and with which roles (ADR 0045 §4).
// Default-DENY — a remote tool is callable only because an entry names it.
type MCPToolExposure struct {
	RemoteToolName string `json:"remoteToolName"`
	// Expose defaults to true (matches the CRD's +kubebuilder:default=true).
	Expose       *bool    `json:"expose,omitempty"`
	AllowedRoles []string `json:"allowedRoles"`
	// AllowedPrincipals privately scopes the materialized tool to specific
	// callers on top of AllowedRoles (ADR 0060). Empty means public.
	AllowedPrincipals []string `json:"allowedPrincipals,omitempty"`
	// ToolID overrides the materialized MCPTool's metadata.name. Used VERBATIM
	// (see CatalogIDFor) — the broker never sanitizes it, because a silent
	// rename would break the id a Skill references in toolRefs. The CRD
	// enforces a valid DNS-1123 name on this field.
	ToolID string  `json:"toolID,omitempty"`
	Hidden *bool   `json:"hidden,omitempty"`
	Tier   *string `json:"tier,omitempty"`
	// Approval is the tool-approval policy (ADR 0003) carried onto the
	// materialized MCPTool — "never" | "always" | "auto". Mirrors Tier.
	Approval *string `json:"approval,omitempty"`
}

type MCPServerSpec struct {
	Transport   string `json:"transport"`
	URL         string `json:"url"`
	DisplayName string `json:"displayName,omitempty"`
	// SecretEnv is the DISCOVERY (service) credential, for tools/list.
	SecretEnv []SecretEnvVar `json:"secretEnv,omitempty"`
	// IdentityProviders, when non-empty, makes invocation require a per-user
	// delegated token (ADR 0045 §5).
	IdentityProviders []string `json:"identityProviders,omitempty"`
	// DefaultIdentity (docs/adr/0049 step 5): "delegated" (default) or
	// "agent"; copied onto each MCPTool.
	DefaultIdentity string            `json:"defaultIdentity,omitempty"`
	Exposure        []MCPToolExposure `json:"exposure,omitempty"`
}

type ObjectMeta struct {
	Name       string `json:"name"`
	Namespace  string `json:"namespace,omitempty"`
	UID        string `json:"uid,omitempty"`
	Generation *int64 `json:"generation,omitempty"`
}

type MCPServer struct {
	APIVersion string        `json:"apiVersion,omitempty"`
	Kind       string        `json:"kind,omitempty"`
	Metadata   ObjectMeta    `json:"metadata"`
	Spec       MCPServerSpec `json:"spec"`
}

// MCPDiscoveredTool is one entry of status.discoveredTools.
type MCPDiscoveredTool struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	// InputSchema is the tool's JSON Schema serialized as a JSON string
	// (matches the CRD).
	InputSchema string `json:"inputSchema,omitempty"`
	Exposed     bool   `json:"exposed"`
}

// Condition is the metav1.Condition shape, as the CRDs use it.
type Condition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason"`
	Message            string `json:"message"`
	LastTransitionTime string `json:"lastTransitionTime"`
	ObservedGeneration *int64 `json:"observedGeneration,omitempty"`
}

// MCPToolSpec is the spec of a derived MCPTool (ADR 0045 §2 — the mcpExec
// catalog record). Field order and omission match what the TypeScript broker
// wrote, and every field is one the Go engine's catalog reader decodes.
type MCPToolSpec struct {
	ServerRef         string   `json:"serverRef"`
	RemoteToolName    string   `json:"remoteToolName"`
	Description       string   `json:"description"`
	InputSchema       *string  `json:"inputSchema,omitempty"`
	AllowedRoles      []string `json:"allowedRoles"`
	AllowedPrincipals []string `json:"allowedPrincipals,omitempty"`
	Hidden            *bool    `json:"hidden,omitempty"`
	Tier              *string  `json:"tier,omitempty"`
	Approval          *string  `json:"approval,omitempty"`
	IdentityProviders []string `json:"identityProviders,omitempty"`
	DefaultIdentity   string   `json:"defaultIdentity,omitempty"`
}

type OwnerReference struct {
	APIVersion         string `json:"apiVersion"`
	Kind               string `json:"kind"`
	Name               string `json:"name"`
	UID                string `json:"uid"`
	Controller         bool   `json:"controller"`
	BlockOwnerDeletion bool   `json:"blockOwnerDeletion"`
}

type MCPToolMeta struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	OwnerReferences []OwnerReference  `json:"ownerReferences,omitempty"`
}

type MCPTool struct {
	APIVersion string      `json:"apiVersion,omitempty"`
	Kind       string      `json:"kind,omitempty"`
	Metadata   MCPToolMeta `json:"metadata"`
	Spec       MCPToolSpec `json:"spec"`
}

// ServerFromUnstructured decodes an MCPServer from the dynamic client's form.
// Decoding goes through JSON so pointer-typed optional fields keep the
// "absent vs. set" distinction.
func ServerFromUnstructured(obj *unstructured.Unstructured) (*MCPServer, error) {
	raw, err := json.Marshal(obj.Object)
	if err != nil {
		return nil, err
	}
	var server MCPServer
	if err := json.Unmarshal(raw, &server); err != nil {
		return nil, err
	}
	return &server, nil
}

// IsExposed reports whether an exposure entry currently materializes its tool.
//
// `expose` defaults TRUE (the CRD default): an entry that merely names a tool
// exposes it. `expose: false` keeps the entry — and its role assignment — on
// record while withdrawing the tool from the catalog.
func IsExposed(entry MCPToolExposure) bool {
	return entry.Expose == nil || *entry.Expose
}

// Exposure is the exposure map keyed by remote tool name, iterable in the
// order the entries first appeared.
type Exposure struct {
	order   []string
	entries map[string]MCPToolExposure
}

func (e *Exposure) Get(name string) (MCPToolExposure, bool) {
	entry, ok := e.entries[name]
	return entry, ok
}

func (e *Exposure) Len() int { return len(e.order) }

// Keys returns the remote tool names in first-appearance order.
func (e *Exposure) Keys() []string { return append([]string(nil), e.order...) }

// ExposureByRemoteName builds the exposure map.
//
// `+listMapKey=remoteToolName` on the CRD means a server cannot carry two
// entries for one tool, so a plain last-wins map is faithful to the API. An
// entry with no remoteToolName is skipped.
func ExposureByRemoteName(server *MCPServer) *Exposure {
	e := &Exposure{entries: map[string]MCPToolExposure{}}
	for _, entry := range server.Spec.Exposure {
		if entry.RemoteToolName == "" {
			continue
		}
		if _, seen := e.entries[entry.RemoteToolName]; !seen {
			e.order = append(e.order, entry.RemoteToolName)
		}
		e.entries[entry.RemoteToolName] = entry
	}
	return e
}

// CatalogIDFor is the catalog id (metadata.name) of the MCPTool materialized
// for one exposure.
//
// `toolID` wins when the operator set one, giving a stable, friendly id they
// control. Otherwise it is a deterministic `mcp-<server>-<remoteTool>`,
// sanitized to a legal k8s name — so the same (server, tool) always maps to the
// same object and discovery updates rather than duplicates it.
func CatalogIDFor(server string, entry MCPToolExposure) string {
	if id := strings.TrimSpace(entry.ToolID); id != "" {
		return id
	}

	base := SanitizeName("mcp-" + server + "-" + entry.RemoteToolName)
	// SanitizeName is not injective — it collapses case, underscores and other
	// characters to `-`, so two distinct remote tool names on ONE server (e.g.
	// `get_user` and `get-user`, or `getUser` and `get-user`) can sanitize to
	// the same id. Both would then land in discovery's desired set with one
	// catalog id; reconcile creates the first and the second 409s into a
	// swallowed error, so an exposed tool silently never materializes. When
	// sanitizing the remote tool name was lossy, disambiguate with a short
	// deterministic hash of the ORIGINAL name: distinct names get distinct ids,
	// while a name already a clean k8s segment stays readable (no suffix).
	if SanitizeName(entry.RemoteToolName) != entry.RemoteToolName {
		suffix := "-" + shortHash(entry.RemoteToolName)
		return trimTrailingDashes(truncate(base, maxK8sName-len(suffix))) + suffix
	}
	return base
}

var (
	illegalRun = regexp.MustCompile(`[^a-z0-9-]+`)
	dashRun    = regexp.MustCompile(`-+`)
)

// SanitizeName coerces an arbitrary string into a legal k8s object name:
// lowercase, only `[a-z0-9-]`, no leading/trailing or doubled separators.
//
// A remote tool name is the SERVER's to choose, so it can contain anything —
// underscores, slashes, capitals, unicode — all of which collapse to `-`. The
// result stays readable (`mcp-github-search-issues`). Only when the name does
// not fit a k8s object name — over the length limit, or sanitized to nothing —
// is a short hash of the ORIGINAL appended, truncating the base to make room,
// so an over-long or exotic name still gets a stable, unique, legal id.
func SanitizeName(raw string) string {
	cleaned := strings.ToLower(raw)
	cleaned = illegalRun.ReplaceAllString(cleaned, "-")
	cleaned = dashRun.ReplaceAllString(cleaned, "-")
	cleaned = strings.Trim(cleaned, "-")

	if cleaned != "" && len(cleaned) <= maxK8sName {
		return cleaned
	}

	suffix := "-" + shortHash(raw)
	base := cleaned
	if base == "" {
		base = "mcp"
	}
	return trimTrailingDashes(truncate(base, maxK8sName-len(suffix))) + suffix
}

// shortHash is the first 8 hex chars of the SHA-1 of s (UTF-8), the same
// digest the TypeScript broker used, so ids are stable across the port.
func shortHash(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:])[:8]
}

// truncate cuts s to at most n bytes. Every caller passes an already
// sanitized (ASCII-only) string, so bytes and characters coincide.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func trimTrailingDashes(s string) string { return strings.TrimRight(s, "-") }
