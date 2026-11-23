package attachments

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A real 1x1 PNG, so content sniffing sees genuine image bytes.
var pngBytes, _ = base64.StdEncoding.DecodeString(
	"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")

func pngDataURL() string {
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(pngBytes)
}

type memStore struct {
	objects map[string][]byte
	types   map[string]string
}

func newMemStore() *memStore {
	return &memStore{objects: map[string][]byte{}, types: map[string]string{}}
}

func (m *memStore) Put(_ context.Context, key, mediaType string, data []byte) error {
	m.objects[key] = data
	m.types[key] = mediaType
	return nil
}

func (m *memStore) Get(_ context.Context, key string) ([]byte, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return data, nil
}

func (m *memStore) Presign(_ context.Context, key string, ttl time.Duration) (string, error) {
	return fmt.Sprintf("https://bucket.example/%s?ttl=%s", key, ttl), nil
}

func seqID() func() string {
	n := 0
	return func() string { n++; return fmt.Sprintf("id%d", n) }
}

func TestUploadStoresDataURLsAndPassesHTTPSThrough(t *testing.T) {
	store := newMemStore()
	refs, err := Upload(context.Background(), store, "attachments/chat-1", seqID(), []Image{
		{URL: pngDataURL()},
		{URL: "https://example.com/photo.jpg"},
	})
	require.NoError(t, err)
	require.Equal(t, []Ref{
		{Key: "attachments/chat-1/id1.png", MediaType: "image/png"},
		{URL: "https://example.com/photo.jpg"},
	}, refs)
	assert.Equal(t, pngBytes, store.objects["attachments/chat-1/id1.png"])
	assert.Equal(t, "image/png", store.types["attachments/chat-1/id1.png"])
}

func TestUploadWithoutStoreIsLoud(t *testing.T) {
	_, err := Upload(context.Background(), nil, "p", seqID(), []Image{{URL: pngDataURL()}})
	assert.ErrorIs(t, err, ErrNotConfigured)

	// An external URL needs no store.
	refs, err := Upload(context.Background(), nil, "p", seqID(), []Image{{URL: "https://example.com/a.png"}})
	require.NoError(t, err)
	assert.Len(t, refs, 1)
}

func TestUploadRejects(t *testing.T) {
	cases := map[string]string{
		"plain http":      "http://example.com/a.png",
		"file scheme":     "file:///etc/passwd",
		"not base64":      "data:image/png,rawbytes",
		"svg":             "data:image/svg+xml;base64," + base64.StdEncoding.EncodeToString([]byte("<svg/>")),
		"mislabelled":     "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(pngBytes),
		"garbage payload": "data:image/png;base64,!!!",
		"empty":           "data:image/png;base64,",
	}
	for name, u := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Upload(context.Background(), newMemStore(), "p", seqID(), []Image{{URL: u}})
			assert.Error(t, err)
		})
	}

	t.Run("too many", func(t *testing.T) {
		images := make([]Image, MaxImagesPerTurn+1)
		for i := range images {
			images[i] = Image{URL: pngDataURL()}
		}
		_, err := Upload(context.Background(), newMemStore(), "p", seqID(), images)
		assert.ErrorContains(t, err, "too many images")
	})

	t.Run("too large", func(t *testing.T) {
		big := append(append([]byte{}, pngBytes...), make([]byte, MaxImageBytes)...)
		_, err := Upload(context.Background(), newMemStore(), "p", seqID(),
			[]Image{{URL: "data:image/png;base64," + base64.StdEncoding.EncodeToString(big)}})
		assert.ErrorContains(t, err, "too large")
	})
}

func TestDecodeDataURLToleratesUnpaddedBase64(t *testing.T) {
	unpadded := strings.TrimRight(base64.StdEncoding.EncodeToString(pngBytes), "=")
	mediaType, data, err := DecodeDataURL("data:image/png;base64," + unpadded)
	require.NoError(t, err)
	assert.Equal(t, "image/png", mediaType)
	assert.Equal(t, pngBytes, data)
}

func TestResolver(t *testing.T) {
	store := newMemStore()
	refs, err := Upload(context.Background(), store, "p", seqID(), []Image{
		{URL: pngDataURL()},
		{URL: "https://example.com/b.webp"},
	})
	require.NoError(t, err)

	r := &Resolver{Store: store, PresignTTL: time.Hour}
	inline, err := r.DataURLs(context.Background(), refs)
	require.NoError(t, err)
	assert.Equal(t, []string{pngDataURL(), "https://example.com/b.webp"}, inline)

	links, err := r.DownloadURLs(context.Background(), refs)
	require.NoError(t, err)
	assert.Equal(t, []string{"https://bucket.example/p/id1.png?ttl=1h0m0s", "https://example.com/b.webp"}, links)

	_, err = (&Resolver{}).DataURLs(context.Background(), refs)
	assert.ErrorIs(t, err, ErrNotConfigured)
}

func TestNoteAndDownloadBlock(t *testing.T) {
	assert.Equal(t, "[The user attached 1 image to this message.]", Note(1))
	assert.Equal(t, "[The user attached 3 images to this message.]", Note(3))
	assert.Empty(t, DownloadBlock(nil))
	assert.Contains(t, DownloadBlock([]string{"https://x/y"}), "- https://x/y\n</attached_images>")
}
