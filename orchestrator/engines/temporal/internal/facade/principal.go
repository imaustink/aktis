package facade

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// principalRetryAfter is how long a subject whose record failed waits before
// being tried again.
const principalRetryAfter = 5 * time.Minute

// PrincipalSink receives verified (email, subject) pairs.
type PrincipalSink interface {
	RecordPrincipal(ctx context.Context, email, subject string) error
}

// PrincipalRecordingResolver wraps the Open WebUI resolver so every verified
// caller's email -> subject pair reaches integration-gateway's Connections
// page directory (docs/adr/0046). That page signs people in against the IdP
// and learns only an email; this is how it finds their openwebui:<id>.
//
// Never delays or fails resolution: the record is fire-and-forget, sent once
// per (subject, email) per process, retried after a back-off on failure.
// Only PerUser identities are recorded — a shared subject mapped to one
// person's email would hand them every other caller's connections.
type PrincipalRecordingResolver struct {
	Inner IdentityResolver
	Sink  PrincipalSink
	Now   func() time.Time

	mu       sync.Mutex
	recorded map[string]string    // subject -> email recorded (or in flight)
	failedAt map[string]time.Time // subject -> last failure
}

func (r *PrincipalRecordingResolver) Resolve(ctx context.Context, token string) *Identity {
	identity := r.Inner.Resolve(ctx, token)
	if identity != nil && identity.PerUser && identity.Email != "" {
		r.maybeRecord(strings.ToLower(strings.TrimSpace(identity.Email)), identity.Subject)
	}
	return identity
}

func (r *PrincipalRecordingResolver) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *PrincipalRecordingResolver) maybeRecord(email, subject string) {
	r.mu.Lock()
	if r.recorded == nil {
		r.recorded = map[string]string{}
		r.failedAt = map[string]time.Time{}
	}
	if r.recorded[subject] == email {
		r.mu.Unlock()
		return
	}
	if failed, ok := r.failedAt[subject]; ok && r.now().Sub(failed) < principalRetryAfter {
		r.mu.Unlock()
		return
	}
	r.recorded[subject] = email
	r.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := r.Sink.RecordPrincipal(ctx, email, subject)
		r.mu.Lock()
		defer r.mu.Unlock()
		if err != nil {
			delete(r.recorded, subject)
			r.failedAt[subject] = r.now()
			log.Printf("connections: could not record principal for %s: %v", subject, err)
			return
		}
		delete(r.failedAt, subject)
	}()
}

// IdentityLinkGatewayClient is the one call this process makes to
// integration-gateway's internal identity-link API.
type IdentityLinkGatewayClient struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

func (c *IdentityLinkGatewayClient) RecordPrincipal(ctx context.Context, email, subject string) error {
	body, _ := json.Marshal(map[string]string{"email": email, "subject": subject})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, strings.TrimRight(c.BaseURL, "/")+"/connections/api/principals", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+c.Token)
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		text, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return fmt.Errorf("connections principal record failed: %d %s", res.StatusCode, text)
	}
	return nil
}
