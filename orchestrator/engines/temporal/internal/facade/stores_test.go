package facade

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
)

// ── MemorySessionStore (session/in-memory-session-store.test.ts) ───────────

func memStore(ttl time.Duration, maxEntries int) (*MemorySessionStore, func(time.Duration)) {
	now := time.UnixMilli(1_000)
	s := &MemorySessionStore{TTL: ttl, MaxEntries: maxEntries, Now: func() time.Time { return now }}
	return s, func(d time.Duration) { now = now.Add(d) }
}

func TestMemorySessionStoreUnknownIDIsAMiss(t *testing.T) {
	s, _ := memStore(time.Minute, 10)
	require.Nil(t, s.Get(bg, "nope"))
}

func TestMemorySessionStoreRoundTripsAndStampsUpdatedAt(t *testing.T) {
	s, _ := memStore(time.Minute, 10)
	require.NoError(t, s.Set(bg, "chat-1", true))
	require.Equal(t, &SessionRecord{ReadOnly: true, UpdatedAt: 1_000}, s.Get(bg, "chat-1"))
}

func TestMemorySessionStoreExpiresEntriesIdlePastTheTTL(t *testing.T) {
	s, advance := memStore(time.Second, 10)
	require.NoError(t, s.Set(bg, "chat-1", true))
	advance(1000 * time.Millisecond)
	require.NotNil(t, s.Get(bg, "chat-1"), "exactly at the TTL is still live")
	advance(time.Millisecond)
	require.Nil(t, s.Get(bg, "chat-1"))
}

func TestMemorySessionStoreSlidesTheTTLOnSet(t *testing.T) {
	s, advance := memStore(time.Second, 10)
	require.NoError(t, s.Set(bg, "chat-1", true))
	advance(900 * time.Millisecond)
	require.NoError(t, s.Set(bg, "chat-1", false))
	advance(900 * time.Millisecond)
	require.Equal(t, &SessionRecord{UpdatedAt: 1_900}, s.Get(bg, "chat-1"))
}

func TestMemorySessionStoreEvictsTheLeastRecentlyUpdatedPastMaxEntries(t *testing.T) {
	s, _ := memStore(time.Minute, 2)
	require.NoError(t, s.Set(bg, "chat-1", true))
	require.NoError(t, s.Set(bg, "chat-2", true))
	require.NoError(t, s.Set(bg, "chat-1", true)) // touch: chat-2 is now oldest
	require.NoError(t, s.Set(bg, "chat-3", true))
	require.Nil(t, s.Get(bg, "chat-2"))
	require.NotNil(t, s.Get(bg, "chat-1"))
	require.NotNil(t, s.Get(bg, "chat-3"))
}

func TestMemoryInvocationStoreRoundTrips(t *testing.T) {
	s := &MemoryInvocationStore{}
	require.Nil(t, s.Get(bg, "x"))
	require.NoError(t, s.Set(bg, InvocationRecord{ID: "x", Status: "pending", EngineInvocationID: "e"}))
	require.Equal(t, &InvocationRecord{ID: "x", Status: "pending", EngineInvocationID: "e"}, s.Get(bg, "x"))
}

// ── Redis stores ───────────────────────────────────────────────────────────

func newMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

func TestRedisSessionStoreKeysTTLAndRoundTrip(t *testing.T) {
	mr, client := newMiniredis(t)
	s := &RedisSessionStore{Client: client, TTL: 30 * time.Minute}
	require.Nil(t, s.Get(bg, "chat-1"))

	before := time.Now().UnixMilli()
	require.NoError(t, s.Set(bg, "chat-1", true))
	require.True(t, mr.Exists("sess:chat-1"), "the TS process's key space")
	require.Equal(t, 30*time.Minute, mr.TTL("sess:chat-1"))

	var raw map[string]any
	value, err := mr.Get("sess:chat-1")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(value), &raw))
	require.Equal(t, true, raw["readOnly"])
	require.GreaterOrEqual(t, int64(raw["updatedAt"].(float64)), before)

	got := s.Get(bg, "chat-1")
	require.True(t, got.ReadOnly)

	// readOnly=false is omitted, as TS left it undefined.
	require.NoError(t, s.Set(bg, "chat-2", false))
	value, _ = mr.Get("sess:chat-2")
	require.NotContains(t, value, "readOnly")
}

// TTL slides on every write, never on a read.
func TestRedisSessionStoreTTLSlidesOnWriteOnly(t *testing.T) {
	mr, client := newMiniredis(t)
	s := &RedisSessionStore{Client: client, TTL: time.Minute}
	require.NoError(t, s.Set(bg, "chat-1", true))
	mr.FastForward(40 * time.Second)
	s.Get(bg, "chat-1")
	require.Equal(t, 20*time.Second, mr.TTL("sess:chat-1"))
	require.NoError(t, s.Set(bg, "chat-1", true))
	require.Equal(t, time.Minute, mr.TTL("sess:chat-1"))
	mr.FastForward(61 * time.Second)
	require.Nil(t, s.Get(bg, "chat-1"))
}

// Records written before 0053 carry more fields; they still read.
func TestRedisSessionStoreReadsLegacyRecordsAndTreatsGarbageAsAMiss(t *testing.T) {
	captureLog(t)
	mr, client := newMiniredis(t)
	s := &RedisSessionStore{Client: client, TTL: time.Minute, Prefix: "p:"}
	require.NoError(t, mr.Set("p:old", `{"readOnly":true,"updatedAt":5,"history":[1,2],"agentRuns":{}}`))
	require.Equal(t, &SessionRecord{ReadOnly: true, UpdatedAt: 5}, s.Get(bg, "old"))
	require.NoError(t, mr.Set("p:bad", `not json`))
	require.Nil(t, s.Get(bg, "bad"))
}

func TestRedisSessionStoreSwallowsErrors(t *testing.T) {
	logs := captureLog(t)
	mr, client := newMiniredis(t)
	s := &RedisSessionStore{Client: client, TTL: time.Minute}
	mr.Close()
	require.Nil(t, s.Get(bg, "chat-1"))
	require.NoError(t, s.Set(bg, "chat-1", true))
	require.Contains(t, logs(), "RedisSessionStore.get failed (treating as miss)")
	require.Contains(t, logs(), "RedisSessionStore.set failed (ignored)")
}

func TestRedisInvocationStoreKeysTTLAndBookkeepingRoundTrip(t *testing.T) {
	mr, client := newMiniredis(t)
	s := &RedisInvocationStore{Client: client, TTL: time.Hour}
	require.Nil(t, s.Get(bg, "inv-1"))

	result := "done"
	record := InvocationRecord{
		ID: "inv-1", Status: "succeeded", Result: &result, RemoteControlURL: "https://rc",
		PendingToolCalls:   []callertools.PendingCall{{ID: "c", Name: "f", Arguments: "{}"}},
		Outcome:            map[string]any{"verdict": "ok"},
		EngineInvocationID: "engine-1", ForcedAgent: true,
	}
	require.NoError(t, s.Set(bg, record))
	require.True(t, mr.Exists("inv:inv-1"), "the TS process's key space")
	require.Equal(t, time.Hour, mr.TTL("inv:inv-1"))

	value, err := mr.Get("inv:inv-1")
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal([]byte(value), &raw))
	// The wire keys integration-gateway and the TS process share.
	require.Equal(t, map[string]any{
		"id": "inv-1", "status": "succeeded", "result": "done", "remoteControlUrl": "https://rc",
		"pendingToolCalls":   []any{map[string]any{"id": "c", "name": "f", "arguments": "{}"}},
		"outcome":            map[string]any{"verdict": "ok"},
		"engineInvocationId": "engine-1", "forcedAgent": true,
	}, raw)

	got := s.Get(bg, "inv-1")
	require.Equal(t, &record, got)
	// What a caller sees has the bookkeeping stripped.
	visible, err := json.Marshal(got.visible())
	require.NoError(t, err)
	require.NotContains(t, string(visible), "engineInvocationId")
	require.NotContains(t, string(visible), "forcedAgent")
	require.Contains(t, string(visible), `"remoteControlUrl":"https://rc"`)
}

// An empty result (target.none) is kept, not omitted.
func TestRedisInvocationStoreKeepsAnEmptyResult(t *testing.T) {
	mr, client := newMiniredis(t)
	s := &RedisInvocationStore{Client: client, TTL: time.Hour}
	empty := ""
	require.NoError(t, s.Set(bg, InvocationRecord{ID: "n", Status: "succeeded", Result: &empty}))
	value, _ := mr.Get("inv:n")
	require.JSONEq(t, `{"id":"n","status":"succeeded","result":""}`, value)
}

func TestRedisInvocationStoreTTLIsAbsolutePerWrite(t *testing.T) {
	mr, client := newMiniredis(t)
	s := &RedisInvocationStore{Client: client, TTL: time.Minute, Prefix: "x:"}
	require.NoError(t, s.Set(bg, InvocationRecord{ID: "i", Status: "pending"}))
	require.True(t, mr.Exists("x:i"))
	mr.FastForward(61 * time.Second)
	require.Nil(t, s.Get(bg, "i"))
}

// Get is a miss on an error (the caller polls again); Set SURFACES it (a lost
// terminal record costs the caller its answer).
func TestRedisInvocationStoreGetSwallowsButSetSurfacesErrors(t *testing.T) {
	captureLog(t)
	mr, client := newMiniredis(t)
	s := &RedisInvocationStore{Client: client, TTL: time.Hour}
	require.NoError(t, mr.Set("inv:bad", "not json"))
	require.Nil(t, s.Get(bg, "bad"))
	mr.Close()
	require.Nil(t, s.Get(bg, "inv-1"))
	require.Error(t, s.Set(bg, InvocationRecord{ID: "inv-1", Status: "pending"}))
}

// End to end: a Redis-backed pair of servers shares records.
func TestRedisInvocationStoreBacksTheServer(t *testing.T) {
	_, client := newMiniredis(t)
	store := &RedisInvocationStore{Client: client, TTL: time.Hour}
	a := serve(t, &Server{Engine: engineReturning(resultOutcome("done")), Invocations: store})
	b := serve(t, &Server{Engine: newFakeEngine(), Invocations: store})
	id := acceptedID(t, postInvoke(t, a, map[string]any{"request": "x"}, nil))
	require.Equal(t, map[string]any{"id": id, "status": "succeeded", "result": "done"}, settled(t, b, id))
}
