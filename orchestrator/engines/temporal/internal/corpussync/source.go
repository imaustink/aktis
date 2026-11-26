package corpussync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// SourceErrorKind is how the broker classified a failure (its status codes
// are a classification the driver already made, so they are mapped back
// rather than re-decided).
type SourceErrorKind string

const (
	// SourceDenied (403): the service credential cannot read the resource.
	SourceDenied SourceErrorKind = "denied"
	// SourcePermanent (401, 500): retrying the same request cannot help. A
	// 401 is this worker's own token, which must never be read as "this
	// resource is inaccessible".
	SourcePermanent SourceErrorKind = "permanent"
	// SourceTransient: unreachable, 503, anything else -- we could not find out.
	SourceTransient SourceErrorKind = "transient"
)

// SourceError is a failed list or fetch.
type SourceError struct {
	Kind    SourceErrorKind
	Message string
}

func (e *SourceError) Error() string { return e.Message }

// HTTPSource reads one Corpus's resources through connection-broker's API
// with that Corpus's sync token (docs/adr/0038 §3). The broker scopes a sync
// token to exactly one Corpus, so a source is per Corpus, never shared.
type HTTPSource struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

// NewHTTPSource returns a source for one Corpus's token.
func NewHTTPSource(baseURL, token string) *HTTPSource {
	return &HTTPSource{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Token:   token,
		// Generous: a fetch may be a large PDF the broker extracts first.
		Client: &http.Client{Timeout: 5 * time.Minute},
	}
}

func (s *HTTPSource) List(ctx context.Context, corpus, cursor string) ([]ResourceRef, string, error) {
	u := s.BaseURL + "/corpora/" + url.PathEscape(corpus) + "/resources"
	if cursor != "" {
		u += "?cursor=" + url.QueryEscape(cursor)
	}
	var body struct {
		Resources []ResourceRef `json:"resources"`
		Cursor    string        `json:"cursor"`
	}
	if err := s.get(ctx, u, &body); err != nil {
		return nil, "", err
	}
	return body.Resources, body.Cursor, nil
}

func (s *HTTPSource) Fetch(ctx context.Context, corpus, id string) (Document, error) {
	u := s.BaseURL + "/corpora/" + url.PathEscape(corpus) + "/resources/" + url.PathEscape(id)
	var doc Document
	if err := s.get(ctx, u, &doc); err != nil {
		return Document{}, err
	}
	return doc, nil
}

func (s *HTTPSource) get(ctx context.Context, u string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return &SourceError{Kind: SourcePermanent, Message: fmt.Sprintf("build request %s: %v", u, err)}
	}
	req.Header.Set("Authorization", "Bearer "+s.Token)
	req.Header.Set("Accept", "application/json")

	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return &SourceError{Kind: SourceTransient, Message: fmt.Sprintf("connection-broker unreachable: %v", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return &SourceError{Kind: SourceTransient, Message: fmt.Sprintf("decode %s: %v", u, err)}
		}
		return nil
	}

	detail := ""
	if raw, _ := io.ReadAll(io.LimitReader(resp.Body, 300)); len(raw) > 0 {
		detail = ": " + strings.TrimSpace(string(raw))
	}
	switch resp.StatusCode {
	case http.StatusForbidden:
		return &SourceError{Kind: SourceDenied, Message: fmt.Sprintf("broker refused %s%s", u, detail)}
	case http.StatusUnauthorized:
		return &SourceError{Kind: SourcePermanent, Message: "connection-broker rejected this worker's token" + detail}
	case http.StatusInternalServerError:
		return &SourceError{Kind: SourcePermanent, Message: "connection-broker failed permanently" + detail}
	default:
		return &SourceError{Kind: SourceTransient, Message: fmt.Sprintf("connection-broker returned %d%s", resp.StatusCode, detail)}
	}
}
