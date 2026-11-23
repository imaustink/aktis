package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"

	"github.com/aktisai/orchestrator/engines/temporal/internal/attachments"
	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/workflows"
)

var onePixelPNG, _ = base64.StdEncoding.DecodeString(
	"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")

func pngDataURL() string {
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(onePixelPNG)
}

type memStore struct {
	objects map[string][]byte
	fail    bool
}

func (m *memStore) Put(_ context.Context, key, _ string, data []byte) error {
	if m.fail {
		return errors.New("bucket unreachable")
	}
	if m.objects == nil {
		m.objects = map[string][]byte{}
	}
	m.objects[key] = data
	return nil
}
func (m *memStore) Get(context.Context, string) ([]byte, error) { return nil, errors.New("unused") }
func (m *memStore) Presign(context.Context, string, time.Duration) (string, error) {
	return "", errors.New("unused")
}

// capturingClient records the turn the gateway hands the conversation
// workflow, and answers update-with-start as accepted.
func capturingClient(t *testing.T) (*mocks.Client, *workflows.TurnInput) {
	c := mocks.NewClient(t)
	handle := mocks.NewWorkflowUpdateHandle(t)
	// A blocking chat turn waits on the handle; the captured input is what
	// these tests are about, so the wait just ends.
	handle.On("Get", mock.Anything, mock.Anything).Return(errors.New("not under test")).Maybe()
	var got workflows.TurnInput
	c.On("NewWithStartWorkflowOperation", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	c.On("UpdateWithStartWorkflow", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			opts := args.Get(1).(client.UpdateWithStartWorkflowOptions)
			got = opts.UpdateOptions.Args[0].(workflows.TurnInput)
		}).
		Return(handle, nil).Maybe()
	return c, &got
}

func postJSON(s *Server, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func TestInvokeUploadsImagesAndForwardsOnlyRefs(t *testing.T) {
	c, got := capturingClient(t)
	store := &memStore{}
	s := NewServer(c, "tq", nil, WithAttachments(store, "attachments"))

	body, _ := json.Marshal(map[string]any{
		"request":   "Extract this recipe: https://example.com/r",
		"sessionId": "chat/1",
		"images":    []map[string]string{{"url": pngDataURL()}, {"url": "https://example.com/p.jpg"}},
	})
	rec := postJSON(s, "/invoke", string(body))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	require.Len(t, got.Images, 2)
	require.True(t, strings.HasPrefix(got.Images[0].Key, "attachments/chat-1/"), got.Images[0].Key)
	require.Equal(t, "image/png", got.Images[0].MediaType)
	require.Equal(t, onePixelPNG, store.objects[got.Images[0].Key])
	require.Equal(t, attachments.Ref{URL: "https://example.com/p.jpg"}, got.Images[1])

	// No bytes ride on the workflow input.
	raw, err := json.Marshal(got)
	require.NoError(t, err)
	require.NotContains(t, string(raw), base64.StdEncoding.EncodeToString(onePixelPNG))
}

func TestInvokeAcceptsAnImageWithNoText(t *testing.T) {
	c, got := capturingClient(t)
	s := NewServer(c, "tq", nil, WithAttachments(&memStore{}, "attachments"))
	body, _ := json.Marshal(map[string]any{"images": []map[string]string{{"url": pngDataURL()}}})
	rec := postJSON(s, "/invoke", string(body))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	require.Len(t, got.Images, 1)
}

// Nil Temporal client throughout: each of these must be refused before any
// workflow is touched.
func TestInvokeImageRefusals(t *testing.T) {
	image := []map[string]string{{"url": pngDataURL()}}

	t.Run("attachments not configured", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"request": "what is this?", "images": image})
		rec := postJSON(NewServer(nil, "tq", nil), "/invoke", string(body))
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "not enabled")
	})
	t.Run("invalid image", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"request": "x", "images": []map[string]string{{"url": "http://insecure/a.png"}}})
		rec := postJSON(NewServer(nil, "tq", nil, WithAttachments(&memStore{}, "a")), "/invoke", string(body))
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
	t.Run("store outage is a 502", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"request": "x", "images": image})
		rec := postJSON(NewServer(nil, "tq", nil, WithAttachments(&memStore{fail: true}, "a")), "/invoke", string(body))
		require.Equal(t, http.StatusBadGateway, rec.Code)
	})
	t.Run("images on an event", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"event": prEnvelope("ai-review"), "images": image})
		rec := postJSON(NewServer(nil, "tq", nil, WithEventTriggers(reviewTriggers(t))), "/invoke", string(body))
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestChatCompletionsUploadsTheUserTurnsImages(t *testing.T) {
	c, got := capturingClient(t)
	s := NewServer(c, "tq", nil, WithAttachments(&memStore{}, "attachments"))
	body, _ := json.Marshal(map[string]any{
		"messages": []map[string]any{{
			"role": "user",
			"content": []map[string]any{
				{"type": "text", "text": "Extract this recipe"},
				{"type": "image_url", "image_url": map[string]string{"url": pngDataURL()}},
			},
		}},
	})
	// Blocking mode waits on the update handle, which the mock cannot
	// complete; the captured turn is what this test is about.
	_ = postJSON(s, "/v1/chat/completions", string(body))
	require.Equal(t, "Extract this recipe", got.Message)
	require.Len(t, got.Images, 1)
}

func TestSplitMessagesAcceptsAnImageOnlyTurn(t *testing.T) {
	messages := []callertools.WireMessage{{
		Role:    "user",
		Content: json.RawMessage(`[{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}]`),
	}}
	text, _, idx, err := splitMessages(messages)
	require.NoError(t, err)
	require.Equal(t, "", text)
	require.Equal(t, []attachments.Image{{URL: "https://example.com/a.png"}}, messageImages(messages[idx].Content))
}

// bigPNGDataURL is a data URL of a valid PNG padded to about size bytes, so
// it is large enough to be streamed out of a chat body rather than buffered.
func bigPNGDataURL(size int) string {
	data := append(append([]byte(nil), onePixelPNG...), make([]byte, size)...)
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(data)
}

func imageMessage(role, text, url string) map[string]any {
	return map[string]any{"role": role, "content": []map[string]any{
		{"type": "text", "text": text},
		{"type": "image_url", "image_url": map[string]string{"url": url}},
	}}
}

// A chat client resends every earlier image inline. Together they may far
// exceed one turn's cap; the turn must still go through, with only the last
// user message's image uploaded.
func TestChatCompletionsToleratesEarlierImagesBeyondTheTurnCap(t *testing.T) {
	c, got := capturingClient(t)
	store := &memStore{}
	s := NewServer(c, "tq", nil, WithAttachments(store, "attachments"))

	var messages []map[string]any
	earlier := bigPNGDataURL(4 << 20)
	for i := 0; i < 8; i++ { // 8 × ~5.3 MB of base64: well past MaxRequestBytes
		messages = append(messages,
			imageMessage("user", "and this one?", earlier),
			map[string]any{"role": "assistant", "content": "A photo."})
	}
	current := bigPNGDataURL(64 << 10)
	messages = append(messages, imageMessage("user", "and the last one?", current))
	body, _ := json.Marshal(map[string]any{"messages": messages})
	require.Greater(t, len(body), attachments.MaxRequestBytes)

	_ = postJSON(s, "/v1/chat/completions", string(body))
	require.Equal(t, "and the last one?", got.Message)
	require.Len(t, got.Images, 1)
	_, want, err := attachments.DecodeDataURL(current)
	require.NoError(t, err)
	require.Equal(t, want, store.objects[got.Images[0].Key], "the LAST user message's image, not an earlier one")
	require.Len(t, got.SeedHistory, maxSeedHistoryMessages)
}

// An image on the current turn that is over the per-image limit still gets
// a clear 400, not a JSON bind error.
func TestChatCompletionsRefusesAnOversizedCurrentImage(t *testing.T) {
	s := NewServer(nil, "tq", nil, WithAttachments(&memStore{}, "attachments"))
	body, _ := json.Marshal(map[string]any{"messages": []map[string]any{
		imageMessage("user", "what is this?", bigPNGDataURL(attachments.MaxImageBytes+1)),
	}})
	rec := postJSON(s, "/v1/chat/completions", string(body))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "image too large")
}

// Text beyond the in-memory cap is a 413 that says so.
func TestChatCompletionsRefusesTooMuchText(t *testing.T) {
	s := NewServer(nil, "tq", nil)
	body, _ := json.Marshal(map[string]any{"messages": []map[string]any{
		{"role": "user", "content": strings.Repeat("x", maxChatTextBytes+1)},
	}})
	rec := postJSON(s, "/v1/chat/completions", string(body))
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	require.Contains(t, rec.Body.String(), "too large")
}

func TestReadChatBodyLeavesOrdinaryStringsAlone(t *testing.T) {
	in := `{"a":"data:image/png;base64,short","b":"x\"y\\","c":"` + strings.Repeat("data:", 400) + `"}`
	out, _, err := readChatBody(strings.NewReader(in))
	require.NoError(t, err)
	require.Equal(t, in, string(out))
}

// Some encoders escape "/" as "\/"; a stashed image is unescaped as JSON.
func TestReadChatBodyRestoresEscapedImages(t *testing.T) {
	url := bigPNGDataURL(4 << 10)
	escaped := strings.ReplaceAll(url, "/", `\/`)
	out, stash, err := readChatBody(strings.NewReader(`{"u":"` + escaped + `"}`))
	require.NoError(t, err)
	require.Equal(t, `{"u":"`+stashPrefix+`0"}`, string(out))
	images, err := stash.resolve([]attachments.Image{{URL: stashPrefix + "0"}})
	require.NoError(t, err)
	require.Equal(t, []attachments.Image{{URL: url}}, images)
}
