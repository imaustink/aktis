package facade

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
)

// ── conversation sessions ──────────────────────────────────────────────────

// SessionRecord is what this process remembers about a conversation between
// turns: only the sticky read-only flag. The engine's workflow holds
// everything else (docs/adr/0036, 0053). Records written before 0053 carry
// more fields; they are ignored and expire with their TTL.
type SessionRecord struct {
	// ReadOnly: the conversation ran a read-only trigger turn, so every later
	// non-trigger turn in it is read-only too. Only a writable trigger turn
	// clears it.
	ReadOnly bool `json:"readOnly,omitempty"`
	// UpdatedAt is ms since epoch; sliding TTL.
	UpdatedAt int64 `json:"updatedAt"`
}

// SessionStore persists SessionRecords. Implementations are soft-failing by
// contract: an outage reads as a miss and a write is dropped, never an error.
type SessionStore interface {
	Get(ctx context.Context, sessionID string) *SessionRecord
	Set(ctx context.Context, sessionID string, readOnly bool) error
}

// MemorySessionStore is the single-replica default: sliding TTL, and the
// least-recently-written session evicted past maxEntries.
type MemorySessionStore struct {
	TTL        time.Duration
	MaxEntries int
	Now        func() time.Time

	mu    sync.Mutex
	order []string
	items map[string]SessionRecord
}

func (s *MemorySessionStore) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *MemorySessionStore) Get(_ context.Context, sessionID string) *SessionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.items[sessionID]
	if !ok {
		return nil
	}
	if s.now().UnixMilli()-record.UpdatedAt > s.TTL.Milliseconds() {
		s.remove(sessionID)
		return nil
	}
	return &record
}

func (s *MemorySessionStore) Set(_ context.Context, sessionID string, readOnly bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items == nil {
		s.items = map[string]SessionRecord{}
	}
	s.remove(sessionID)
	s.items[sessionID] = SessionRecord{ReadOnly: readOnly, UpdatedAt: s.now().UnixMilli()}
	s.order = append(s.order, sessionID)
	for len(s.order) > s.MaxEntries && len(s.order) > 0 {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.items, oldest)
	}
	return nil
}

func (s *MemorySessionStore) remove(sessionID string) {
	if _, ok := s.items[sessionID]; !ok {
		return
	}
	delete(s.items, sessionID)
	for i, id := range s.order {
		if id == sessionID {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// RedisSessionStore shares session records across replicas (docs/adr/0016).
// Soft-failing: a Redis error is a miss on read and ignored on write. TTL
// slides on every write, never on read.
type RedisSessionStore struct {
	Client *redis.Client
	TTL    time.Duration
	// Prefix defaults to "sess:" — the key space the TS process used, so a
	// rollout between the two reads the same records.
	Prefix string
}

func (s *RedisSessionStore) key(id string) string {
	if s.Prefix == "" {
		return "sess:" + id
	}
	return s.Prefix + id
}

func (s *RedisSessionStore) Get(ctx context.Context, sessionID string) *SessionRecord {
	raw, err := s.Client.Get(ctx, s.key(sessionID)).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Printf("RedisSessionStore.get failed (treating as miss): %v", err)
		}
		return nil
	}
	var record SessionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		log.Printf("RedisSessionStore.get failed (treating as miss): %v", err)
		return nil
	}
	return &record
}

func (s *RedisSessionStore) Set(ctx context.Context, sessionID string, readOnly bool) error {
	payload, _ := json.Marshal(SessionRecord{ReadOnly: readOnly, UpdatedAt: time.Now().UnixMilli()})
	if err := s.Client.Set(ctx, s.key(sessionID), payload, s.TTL).Err(); err != nil {
		log.Printf("RedisSessionStore.set failed (ignored): %v", err)
	}
	return nil
}

// ── /invoke accept-then-poll records ───────────────────────────────────────

// InvocationRecord is one /invoke turn as a caller polls it. The JSON keys
// are the contract integration-gateway reads, and match what the TS process
// wrote to the same Redis keys.
type InvocationRecord struct {
	ID     string `json:"id"`
	Status string `json:"status"` // pending | succeeded | failed
	// Result is the turn's answer; "" for a target.none turn.
	Result *string `json:"result,omitempty"`
	Error  string  `json:"error,omitempty"`
	// RemoteControlURL is the delegated agent's live Remote Control session,
	// the most recent one seen.
	RemoteControlURL string `json:"remoteControlUrl,omitempty"`
	// PendingToolCalls: the turn is asking the CALLER to run these
	// (docs/adr/0035); such a record has no result.
	PendingToolCalls []callertools.PendingCall `json:"pendingToolCalls,omitempty"`
	// Outcome is a forced agentRef run's structured outcome (docs/adr/0052 §2),
	// already accepted by CheckOutcome.
	Outcome map[string]any `json:"outcome,omitempty"`

	// EngineInvocationID lets ANY process finish the turn if the one that
	// started it dies. Internal: never returned to a caller.
	EngineInvocationID string `json:"engineInvocationId,omitempty"`
	// ForcedAgent: a forced agentRef dispatch, the only kind whose outcome is
	// reported. Internal.
	ForcedAgent bool `json:"forcedAgent,omitempty"`
}

// visible is the record without this process's bookkeeping.
func (r InvocationRecord) visible() InvocationRecord {
	r.EngineInvocationID = ""
	r.ForcedAgent = false
	return r
}

// InvocationStore holds /invoke records.
//
// Get treats a store error as a miss (the caller polls again). Set surfaces
// its error: a lost terminal record costs the caller its answer with no way
// to ask again.
type InvocationStore interface {
	Get(ctx context.Context, id string) *InvocationRecord
	Set(ctx context.Context, record InvocationRecord) error
}

// MemoryInvocationStore is the single-replica default: records die with the
// process, and a second replica 404s polls it did not accept.
type MemoryInvocationStore struct {
	mu      sync.Mutex
	records map[string]InvocationRecord
}

func (s *MemoryInvocationStore) Get(_ context.Context, id string) *InvocationRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if !ok {
		return nil
	}
	return &record
}

func (s *MemoryInvocationStore) Set(_ context.Context, record InvocationRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = map[string]InvocationRecord{}
	}
	s.records[record.ID] = record
	return nil
}

// RedisInvocationStore lets any replica answer any poll. TTL is absolute per
// write and must outlast the longest poll budget a caller uses.
type RedisInvocationStore struct {
	Client *redis.Client
	TTL    time.Duration
	// Prefix defaults to "inv:", the TS process's key space.
	Prefix string
}

func (s *RedisInvocationStore) key(id string) string {
	if s.Prefix == "" {
		return "inv:" + id
	}
	return s.Prefix + id
}

func (s *RedisInvocationStore) Get(ctx context.Context, id string) *InvocationRecord {
	raw, err := s.Client.Get(ctx, s.key(id)).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Printf("RedisInvocationStore.get failed (treating as miss): %v", err)
		}
		return nil
	}
	var record InvocationRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		log.Printf("RedisInvocationStore.get failed (treating as miss): %v", err)
		return nil
	}
	return &record
}

func (s *RedisInvocationStore) Set(ctx context.Context, record InvocationRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.Client.Set(ctx, s.key(record.ID), payload, s.TTL).Err()
}
