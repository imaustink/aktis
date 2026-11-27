package corpussync

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
)

// Synced is one Corpus this worker indexes, read fresh from the API server.
type Synced struct {
	Name         string
	Collection   string
	IntervalMs   int64
	AllowedRoles []string
	// LastReconcileAt is the status's lastReconcileTime in ms, zero if none.
	LastReconcileAt int64
	// Token is the Corpus's own sync token: the broker scopes each to one Corpus.
	Token string
}

// Corpora reads Corpus CRs straight from the API server -- no watch cache
// to lag behind (docs/adr/0059 §3) -- and records each pass on the Corpus's
// status.
type Corpora struct {
	Client    dynamic.Interface
	Namespace string
	// Tokens maps a Corpus name to its sync token (SyncTokensFromEnv).
	Tokens map[string]string
	Now    func() time.Time
	// OnMissingToken is told, once per Corpus, that a syncable Corpus has no
	// token here and so is not indexed.
	OnMissingToken func(corpus string)

	mu     sync.Mutex
	warned map[string]bool
}

func (c *Corpora) resource() dynamic.ResourceInterface {
	return c.Client.Resource(catalog.CorpusGVR).Namespace(c.Namespace)
}

// Get returns the Corpus if it is synced here, nil if it is gone or not
// meant to be indexed.
func (c *Corpora) Get(ctx context.Context, name string) (*Synced, error) {
	obj, err := c.resource().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get corpus %s: %w", name, err)
	}
	return c.synced(obj), nil
}

// List is the names of every Corpus synced here, sorted.
func (c *Corpora) List(ctx context.Context) ([]string, error) {
	list, err := c.resource().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list corpora in %s: %w", c.Namespace, err)
	}
	var names []string
	for i := range list.Items {
		if s := c.synced(&list.Items[i]); s != nil {
			names = append(names, s.Name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// ListAll is the name of every Corpus in the namespace, synced or not.
func (c *Corpora) ListAll(ctx context.Context) ([]string, error) {
	list, err := c.resource().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list corpora in %s: %w", c.Namespace, err)
	}
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].GetName())
	}
	sort.Strings(names)
	return names, nil
}

// synced applies the broker's rule for "this Corpus is indexed": a
// collection published by the controller, a sync mode other than none with a
// parseable interval (a default here would reconcile on a cadence nobody
// chose), and a sync token. Missing any is an ordinary state, not an error.
func (c *Corpora) synced(obj *unstructured.Unstructured) *Synced {
	name := obj.GetName()
	collection, _, _ := unstructured.NestedString(obj.Object, "status", "collection")
	mode, _, _ := unstructured.NestedString(obj.Object, "spec", "sync", "mode")
	interval, _, _ := unstructured.NestedString(obj.Object, "spec", "sync", "reconcileInterval")
	_, hasSync, _ := unstructured.NestedMap(obj.Object, "spec", "sync")
	if collection == "" || !hasSync || mode == "none" {
		return nil
	}
	intervalMs, ok := ParseDurationMs(interval)
	if !ok {
		return nil
	}
	token := c.Tokens[name]
	if token == "" {
		c.warnMissingToken(name)
		return nil
	}
	roles, _, _ := unstructured.NestedStringSlice(obj.Object, "spec", "allowedRoles")
	s := &Synced{Name: name, Collection: collection, IntervalMs: intervalMs, AllowedRoles: roles, Token: token}
	if at, _, _ := unstructured.NestedString(obj.Object, "status", "lastReconcileTime"); at != "" {
		if t, err := time.Parse(time.RFC3339Nano, at); err == nil {
			s.LastReconcileAt = t.UnixMilli()
		}
	}
	return s
}

func (c *Corpora) warnMissingToken(name string) {
	if c.OnMissingToken == nil {
		return
	}
	c.mu.Lock()
	first := !c.warned[name]
	if c.warned == nil {
		c.warned = map[string]bool{}
	}
	c.warned[name] = true
	c.mu.Unlock()
	if first {
		c.OnMissingToken(name)
	}
}

// RecordPass publishes a pass onto the Corpus's status. lastSyncTime moves on
// any pass (material did arrive); lastReconcileTime only on a full one, the
// kind that could have noticed a deletion -- it is what the KnowledgeBase
// controller measures staleness by (ADR 0038 §4).
func (c *Corpora) RecordPass(ctx context.Context, name string, report Report) error {
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	at := now().UTC().Format("2006-01-02T15:04:05.000Z07:00")
	status := map[string]any{
		"resources":    report.Indexed + report.Unchanged,
		"lastSyncTime": at,
	}
	if report.Full {
		status["lastReconcileTime"] = at
	}
	patch, err := json.Marshal(map[string]any{"status": status})
	if err != nil {
		return err
	}
	if _, err := c.resource().Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status"); err != nil {
		return fmt.Errorf("record sync status on corpus %s: %w", name, err)
	}
	return nil
}

var durationPattern = regexp.MustCompile(`^(\d+(?:\.\d+)?)(ms|s|m|h)$`)

// ParseDurationMs reads a reconcileInterval ("6h", "30m", "90s", "1.5h") as
// the broker did: one number and one unit, nothing else.
func ParseDurationMs(value string) (int64, bool) {
	m := durationPattern.FindStringSubmatch(strings.TrimSpace(value))
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, false
	}
	scale := map[string]float64{"ms": 1, "s": 1e3, "m": 60e3, "h": 3600e3}[m[2]]
	ms := int64(n * scale)
	if ms <= 0 {
		return 0, false
	}
	return ms, true
}

// SyncTokensFromEnv reads SYNC_TOKEN_<CORPUS> the way the broker does:
// lower-cased, underscores as dashes.
func SyncTokensFromEnv() map[string]string {
	tokens := map[string]string{}
	for _, kv := range os.Environ() {
		key, value, ok := strings.Cut(kv, "=")
		if !ok || value == "" || !strings.HasPrefix(key, "SYNC_TOKEN_") || len(key) == len("SYNC_TOKEN_") {
			continue
		}
		tokens[strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(key, "SYNC_TOKEN_")), "_", "-")] = value
	}
	return tokens
}
