package corpussync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
)

func corpusCR(name string, spec, status map[string]any) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": "core.aktis.ai/v1alpha1",
		"kind":       "Corpus",
		"metadata":   map[string]any{"name": name, "namespace": "kb"},
		"spec":       spec,
	}
	if status != nil {
		obj["status"] = status
	}
	return &unstructured.Unstructured{Object: obj}
}

func syncedSpec(interval string) map[string]any {
	return map[string]any{
		"connectionRef": "conf",
		"allowedRoles":  []any{"eng"},
		"sync":          map[string]any{"mode": "webhook+reconcile", "reconcileInterval": interval},
	}
}

func fakeCorpora(objs ...*unstructured.Unstructured) (*Corpora, *dynamicfake.FakeDynamicClient) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{catalog.CorpusGVR: "CorpusList"})
	// Tracked under the real resource: the fake would guess "corpuses" from the kind.
	for _, obj := range objs {
		if err := client.Tracker().Create(catalog.CorpusGVR, obj, "kb"); err != nil {
			panic(err)
		}
	}
	return &Corpora{
		Client:    client,
		Namespace: "kb",
		Tokens:    map[string]string{"eng": "tok-eng", "sales": "tok-sales", "none": "tok-none", "fresh": "tok-fresh"},
	}, client
}

func TestCorporaAppliesTheBrokersSyncedRule(t *testing.T) {
	var warned []string
	corpora, _ := fakeCorpora(
		corpusCR("eng", syncedSpec("6h"), map[string]any{"collection": "corpus-eng", "lastReconcileTime": "2026-11-25T10:00:00.500Z"}),
		corpusCR("fresh", syncedSpec("30m"), nil), // no collection yet
		corpusCR("none", map[string]any{"sync": map[string]any{"mode": "none", "reconcileInterval": "1h"}}, map[string]any{"collection": "c"}),
		corpusCR("nosync", map[string]any{}, map[string]any{"collection": "c"}),
		corpusCR("bad", syncedSpec("1h30m"), map[string]any{"collection": "c"}),
		corpusCR("notoken", syncedSpec("1h"), map[string]any{"collection": "c"}),
		corpusCR("sales", syncedSpec("90s"), map[string]any{"collection": "corpus-sales"}),
	)
	corpora.OnMissingToken = func(c string) { warned = append(warned, c) }

	names, err := corpora.List(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"eng", "sales"}, names)
	_, err = corpora.List(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"notoken"}, warned, "a missing token is reported once")

	all, err := corpora.ListAll(context.Background())
	require.NoError(t, err)
	require.Len(t, all, 7)

	eng, err := corpora.Get(context.Background(), "eng")
	require.NoError(t, err)
	require.Equal(t, &Synced{
		Name: "eng", Collection: "corpus-eng", IntervalMs: 6 * 3600_000, AllowedRoles: []string{"eng"},
		LastReconcileAt: time.Date(2026, 11, 25, 10, 0, 0, 500e6, time.UTC).UnixMilli(), Token: "tok-eng",
	}, eng)

	gone, err := corpora.Get(context.Background(), "missing")
	require.NoError(t, err)
	require.Nil(t, gone)
	fresh, err := corpora.Get(context.Background(), "fresh")
	require.NoError(t, err)
	require.Nil(t, fresh)
}

func TestRecordPassPatchesTheStatusSubresource(t *testing.T) {
	corpora, client := fakeCorpora(corpusCR("eng", syncedSpec("6h"), map[string]any{"collection": "c"}))
	corpora.Now = func() time.Time { return time.Date(2026, 11, 26, 7, 8, 9, 10e6, time.FixedZone("x", 3600)) }

	require.NoError(t, corpora.RecordPass(context.Background(), "eng", Report{Indexed: 2, Unchanged: 3, Full: false}))
	require.NoError(t, corpora.RecordPass(context.Background(), "eng", Report{Indexed: 1, Unchanged: 1, Full: true}))

	var patches []map[string]map[string]any
	for _, a := range client.Actions() {
		p, ok := a.(k8stesting.PatchAction)
		if !ok {
			continue
		}
		require.Equal(t, "status", p.GetSubresource())
		var body map[string]map[string]any
		require.NoError(t, json.Unmarshal(p.GetPatch(), &body))
		patches = append(patches, body)
	}
	require.Len(t, patches, 2)
	// A partial pass moves lastSyncTime only; a full one moves lastReconcileTime too.
	require.Equal(t, map[string]any{"resources": float64(5), "lastSyncTime": "2026-11-26T06:08:09.010Z"}, patches[0]["status"])
	require.Equal(t, map[string]any{
		"resources": float64(2), "lastSyncTime": "2026-11-26T06:08:09.010Z", "lastReconcileTime": "2026-11-26T06:08:09.010Z",
	}, patches[1]["status"])
}

func TestParseDurationMsAcceptsOnlyWhatTheBrokerDid(t *testing.T) {
	for in, want := range map[string]int64{"6h": 6 * 3600_000, "30m": 1800_000, "90s": 90_000, "250ms": 250, "1.5h": 5400_000, " 2m ": 120_000} {
		got, ok := ParseDurationMs(in)
		require.True(t, ok, in)
		require.Equal(t, want, got, in)
	}
	for _, in := range []string{"", "1h30m", "5", "-1h", "0s", "1d", "h"} {
		_, ok := ParseDurationMs(in)
		require.False(t, ok, in)
	}
}

func TestSyncTokensFromEnvMapsNamesAsTheBrokerDoes(t *testing.T) {
	t.Setenv("SYNC_TOKEN_ENG_WIKI", "a")
	t.Setenv("SYNC_TOKEN_SALES", "b")
	t.Setenv("SYNC_TOKEN_EMPTY", "")
	tokens := SyncTokensFromEnv()
	require.Equal(t, "a", tokens["eng-wiki"])
	require.Equal(t, "b", tokens["sales"])
	_, ok := tokens["empty"]
	require.False(t, ok)
}
