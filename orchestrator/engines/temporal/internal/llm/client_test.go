package llm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImagesBecomeMultipartContent(t *testing.T) {
	var got struct {
		Messages []json.RawMessage `json:"messages"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &got))
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "k", "m")
	_, err := c.Complete(context.Background(), []Message{
		{Role: "system", Content: "sys"},
		{Role: "user", Content: "what is this?", Images: []string{"data:image/png;base64,AAAA"}},
	})
	require.NoError(t, err)
	require.Len(t, got.Messages, 2)
	assert.JSONEq(t, `{"role":"system","content":"sys"}`, string(got.Messages[0]))
	assert.JSONEq(t, `{"role":"user","content":[
		{"type":"text","text":"what is this?"},
		{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}
	]}`, string(got.Messages[1]))

	// A Message itself never serializes its images (it rides in Temporal payloads).
	raw, err := json.Marshal(Message{Role: "user", Content: "x", Images: []string{"data:..."}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"role":"user","content":"x"}`, string(raw))
}
