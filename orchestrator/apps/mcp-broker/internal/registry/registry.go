// Package registry holds what the broker needs to act on each MCPServer: the
// CR itself (for its url, exposure map and identityProviders) and the resolved
// DISCOVERY credential.
package registry

import (
	"sync"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
)

// Binding is one MCPServer as the broker acts on it.
//
// The service token is resolved once, at bind time, from the server's
// `secretEnv` (ADR 0045 §5) — not on the request path, which would put a Secret
// read in front of every discovery pass and every invocation. It is used ONLY
// for tools/list and for a tools/call to a server that declares no
// identityProviders; a server that runs as the user never has it spent on a
// call (see the server package).
type Binding struct {
	Name string
	CR   *resource.MCPServer
	// ServiceToken is the discovery credential, or "" when the server carries
	// no secretEnv (or it could not be resolved).
	ServiceToken string
}

// Registry serves bindings by server name.
type Registry interface {
	Get(name string) (Binding, bool)
	List() []Binding
}

// Static is an in-memory registry, for tests and for wiring that supplies its
// own CRs.
type Static struct {
	mu       sync.RWMutex
	order    []string
	bindings map[string]Binding
}

func NewStatic(bindings ...Binding) *Static {
	s := &Static{bindings: map[string]Binding{}}
	for _, b := range bindings {
		s.set(b)
	}
	return s
}

func (s *Static) Get(name string) (Binding, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.bindings[name]
	return b, ok
}

// List returns the bindings in the order they were first bound.
func (s *Static) List() []Binding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Binding, 0, len(s.order))
	for _, name := range s.order {
		out = append(out, s.bindings[name])
	}
	return out
}

func (s *Static) set(b Binding) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.bindings[b.Name]; !ok {
		s.order = append(s.order, b.Name)
	}
	s.bindings[b.Name] = b
}

// setTokenIfCurrent replaces a binding's token only while cr is still the
// bound CR.
func (s *Static) setTokenIfCurrent(name string, cr *resource.MCPServer, token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.bindings[name]; ok && b.CR == cr {
		b.ServiceToken = token
		s.bindings[name] = b
	}
}

func (s *Static) remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.bindings[name]; !ok {
		return
	}
	delete(s.bindings, name)
	for i, n := range s.order {
		if n == name {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}
