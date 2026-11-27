package corpussync

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func brokerStub(t *testing.T, handler http.HandlerFunc) *HTTPSource {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewHTTPSource(srv.URL+"/", "sync-token")
}

func TestHTTPSourceListsWithTheCorpusTokenAndCursor(t *testing.T) {
	var gotPath, gotQuery, gotAuth string
	src := brokerStub(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotAuth = r.URL.EscapedPath(), r.URL.RawQuery, r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]any{"resources": []map[string]any{{"id": "p1", "title": "t", "url": "u"}}, "cursor": "next"})
	})
	refs, next, err := src.List(context.Background(), "a b/c", "x&y=1")
	require.NoError(t, err)
	require.Equal(t, []ResourceRef{{ID: "p1"}}, refs)
	require.Equal(t, "next", next)
	require.Equal(t, "/corpora/a%20b%2Fc/resources", gotPath)
	require.Equal(t, "cursor=x%26y%3D1", gotQuery)
	require.Equal(t, "Bearer sync-token", gotAuth)
}

func TestHTTPSourceTreatsMissingResourcesAndCursorAsTheEnd(t *testing.T) {
	src := brokerStub(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"cursor":null}`)) })
	refs, next, err := src.List(context.Background(), "eng", "")
	require.NoError(t, err)
	require.Empty(t, refs)
	require.Empty(t, next)
}

func TestHTTPSourceFetchesOneDocumentWithAnEscapedID(t *testing.T) {
	var gotPath string
	src := brokerStub(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		_, _ = w.Write([]byte(`{"id":"../etc","title":"T","url":"U","markdown":"# hi","acl":{"principals":["user:1"],"permissive":false}}`))
	})
	doc, err := src.Fetch(context.Background(), "eng", "../etc")
	require.NoError(t, err)
	require.Equal(t, "/corpora/eng/resources/..%2Fetc", gotPath)
	require.Equal(t, "# hi", doc.Markdown)
	require.Equal(t, []string{"user:1"}, doc.ACL.Principals)
	require.NotNil(t, doc.ACL.Permissive)
}

func TestHTTPSourceClassifiesTheBrokersStatusCodes(t *testing.T) {
	for status, kind := range map[int]SourceErrorKind{
		403: SourceDenied,
		401: SourcePermanent,
		500: SourcePermanent,
		503: SourceTransient,
		404: SourceTransient,
	} {
		src := brokerStub(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("why it failed"))
		})
		_, err := src.Fetch(context.Background(), "eng", "p1")
		var se *SourceError
		require.True(t, errors.As(err, &se), "%d", status)
		require.Equal(t, kind, se.Kind, "%d", status)
		require.Contains(t, se.Message, "why it failed", "%d", status)
	}
}

func TestHTTPSourceTreatsAnUnreachableBrokerAsTransient(t *testing.T) {
	src := NewHTTPSource("http://127.0.0.1:1", "t")
	_, _, err := src.List(context.Background(), "eng", "")
	var se *SourceError
	require.True(t, errors.As(err, &se))
	require.Equal(t, SourceTransient, se.Kind)
}
