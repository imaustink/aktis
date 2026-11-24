package facade

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Ported from identity-link/*.test.ts. Only RecordPrincipal survived the
// port of the gateway client: the device/authcode start, poll, token and
// wait calls belong to the engine now, and are tested there.

type recordedPrincipal struct{ email, subject string }

type sinkStub struct {
	mu    sync.Mutex
	calls []recordedPrincipal
	err   error
}

func (s *sinkStub) RecordPrincipal(_ context.Context, email, subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, recordedPrincipal{email, subject})
	return s.err
}

func (s *sinkStub) recorded() []recordedPrincipal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedPrincipal(nil), s.calls...)
}

// seqIdentityResolver answers its identities in order, repeating the last.
type seqIdentityResolver struct {
	mu  sync.Mutex
	ids []*Identity
	i   int
}

func (s *seqIdentityResolver) Resolve(context.Context, string) *Identity {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.ids[min(s.i, len(s.ids)-1)]
	s.i++
	return id
}

func ada() *Identity {
	return &Identity{Subject: "openwebui:1", Roles: []string{"reader"}, PerUser: true, Email: "Ada@Example.com"}
}

func TestPrincipalRecordingPassesThroughAndRecordsTheNormalizedEmailOnce(t *testing.T) {
	sink := &sinkStub{}
	identity := ada()
	r := &PrincipalRecordingResolver{Inner: fixedResolver{identity}, Sink: sink}
	require.Same(t, identity, r.Resolve(bg, "jwt"))
	waitFor(t, func() bool { return len(sink.recorded()) == 1 })
	r.Resolve(bg, "jwt")
	stillFalse(t, func() bool { return len(sink.recorded()) > 1 })
	require.Equal(t, []recordedPrincipal{{"ada@example.com", "openwebui:1"}}, sink.recorded())
}

func TestPrincipalRecordingRecordsAgainWhenTheEmailChanges(t *testing.T) {
	sink := &sinkStub{}
	changed := ada()
	changed.Email = "ada@new.example"
	r := &PrincipalRecordingResolver{Inner: &seqIdentityResolver{ids: []*Identity{ada(), changed}}, Sink: sink}
	r.Resolve(bg, "a")
	waitFor(t, func() bool { return len(sink.recorded()) == 1 })
	r.Resolve(bg, "b")
	waitFor(t, func() bool { return len(sink.recorded()) == 2 })
	require.Equal(t, recordedPrincipal{"ada@new.example", "openwebui:1"}, sink.recorded()[1])
}

// A shared subject mapped to one email would hand that person everyone's
// connections.
func TestPrincipalRecordingNeverRecordsASharedIdentity(t *testing.T) {
	sink := &sinkStub{}
	shared := ada()
	shared.PerUser = false
	(&PrincipalRecordingResolver{Inner: fixedResolver{shared}, Sink: sink}).Resolve(bg, "jwt")
	stillFalse(t, func() bool { return len(sink.recorded()) > 0 })
}

func TestPrincipalRecordingDoesNothingWithoutAnEmailOrAnIdentity(t *testing.T) {
	sink := &sinkStub{}
	noEmail := ada()
	noEmail.Email = ""
	(&PrincipalRecordingResolver{Inner: fixedResolver{noEmail}, Sink: sink}).Resolve(bg, "jwt")
	require.Nil(t, (&PrincipalRecordingResolver{Inner: fixedResolver{nil}, Sink: sink}).Resolve(bg, "jwt"))
	stillFalse(t, func() bool { return len(sink.recorded()) > 0 })
}

func TestPrincipalRecordingNeverFailsResolutionAndBacksOffBeforeRetrying(t *testing.T) {
	captureLog(t)
	var mu sync.Mutex
	now := time.Unix(0, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	sink := &sinkStub{err: errors.New("gateway down")}
	identity := ada()
	r := &PrincipalRecordingResolver{Inner: fixedResolver{identity}, Sink: sink, Now: clock}

	require.Same(t, identity, r.Resolve(bg, "jwt"))
	waitFor(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		_, failed := r.failedAt["openwebui:1"]
		return failed
	})
	r.Resolve(bg, "jwt")
	stillFalse(t, func() bool { return len(sink.recorded()) > 1 })

	mu.Lock()
	now = now.Add(6 * time.Minute)
	mu.Unlock()
	r.Resolve(bg, "jwt")
	waitFor(t, func() bool { return len(sink.recorded()) == 2 })
}

func TestIdentityLinkGatewayClientRecordPrincipal(t *testing.T) {
	type seen struct {
		method, path, auth, contentType string
		body                            map[string]string
	}
	var got seen
	status := http.StatusNoContent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		got = seen{r.Method, r.URL.Path, r.Header.Get("authorization"), r.Header.Get("content-type"), nil}
		_ = json.Unmarshal(raw, &got.body)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `{"error":"boom"}`)
	}))
	t.Cleanup(srv.Close)
	client := &IdentityLinkGatewayClient{BaseURL: srv.URL + "/", Token: "tok"}

	require.NoError(t, client.RecordPrincipal(bg, "ada@example.com", "openwebui:1"))
	require.Equal(t, seen{http.MethodPut, "/connections/api/principals", "Bearer tok", "application/json",
		map[string]string{"email": "ada@example.com", "subject": "openwebui:1"}}, got)

	status = http.StatusInternalServerError
	err := client.RecordPrincipal(bg, "ada@example.com", "openwebui:1")
	require.ErrorContains(t, err, "500")
	require.ErrorContains(t, err, "boom")

	unreachable := &IdentityLinkGatewayClient{BaseURL: "http://127.0.0.1:1", Token: "tok"}
	require.Error(t, unreachable.RecordPrincipal(bg, "a@b", "s"))
}
