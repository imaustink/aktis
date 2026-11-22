// Package attachments carries user-supplied images (chat attachments) through
// the engine without putting their bytes in Temporal history.
//
// The gateway uploads each image ONCE, at the edge, and only a small Ref
// travels on the turn input, in conversation state and in activity inputs.
// Temporal caps a single payload (2 MB by default) and a workflow's whole
// history (50 MB), and the agent loop hands the request to several LLM
// activities per turn: inlined base64 would be re-recorded on every one of
// them. Activities resolve a Ref back to bytes (to inline for a vision model)
// or to a presigned URL (for an agent pod) only at the point of use.
//
// An https:// image URL a client sends is kept as an external Ref, never
// fetched here: the engine does not dereference caller-chosen URLs (SSRF),
// and the model provider can fetch a public URL itself.
package attachments

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// The limits are sized to the engine's memory budget as much as to the
// models': the gateway holds a whole request body while it uploads, and the
// worker holds every context image base64-inlined while it calls the model.
const (
	// MaxImagesPerTurn bounds how many images one message may carry.
	MaxImagesPerTurn = 4
	// MaxImageBytes bounds one decoded image. 5 MB is the strictest common
	// provider limit (Anthropic's), and far above what a vision model keeps
	// after its own downscaling.
	MaxImageBytes = 5 << 20
	// MaxContextImages bounds how many images (this turn's plus earlier ones
	// still in the conversation window) are shown to a model in one call.
	MaxContextImages = 3
	// MaxRequestBytes bounds a gateway request body that may carry images:
	// MaxImagesPerTurn base64-encoded images plus room for the text.
	MaxRequestBytes = MaxImagesPerTurn*MaxImageBytes*4/3 + 4<<20
)

// Ref is a reference to one image. Exactly one of Key and URL is set.
type Ref struct {
	// Key is the object key in the attachment store.
	Key string `json:"key,omitempty"`
	// URL is an external https:// image URL passed through as sent.
	URL string `json:"url,omitempty"`
	// MediaType is the image's MIME type (image/png, image/jpeg, ...).
	MediaType string `json:"mediaType,omitempty"`
}

// Image is one image as a client sent it, before upload.
type Image struct {
	// URL is a data: URL (base64 image) or an https:// URL.
	URL string `json:"url"`
}

// Store persists image bytes. The S3 implementation is the production one;
// tests use an in-memory fake.
type Store interface {
	Put(ctx context.Context, key, mediaType string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Presign(ctx context.Context, key string, ttl time.Duration) (string, error)
}

// ErrNotConfigured is returned when a turn carries images but this deployment
// has no attachment store. Loud on purpose: silently dropping the image would
// have the agent answer as if it had seen something it never received.
var ErrNotConfigured = errors.New("image attachments are not enabled on this deployment (no attachment store configured)")

// InvalidError is an image the caller sent that cannot be accepted (wrong
// type, too large, malformed) — fixable by the caller, unlike a store outage.
type InvalidError struct{ msg string }

func (e *InvalidError) Error() string { return e.msg }

func invalid(format string, args ...any) error {
	return &InvalidError{msg: fmt.Sprintf(format, args...)}
}

// allowedMediaTypes are the formats every major vision model accepts.
var allowedMediaTypes = map[string]string{
	"image/png":  "png",
	"image/jpeg": "jpg",
	"image/gif":  "gif",
	"image/webp": "webp",
}

// Upload validates images and stores the data: URL ones, returning a Ref for
// each in order. keyPrefix scopes the objects (e.g. by conversation); newID
// supplies a unique object name. A nil store with any data: URL image is
// ErrNotConfigured.
func Upload(ctx context.Context, store Store, keyPrefix string, newID func() string, images []Image) ([]Ref, error) {
	if len(images) == 0 {
		return nil, nil
	}
	if len(images) > MaxImagesPerTurn {
		return nil, invalid("too many images: %d (at most %d per message)", len(images), MaxImagesPerTurn)
	}
	refs := make([]Ref, 0, len(images))
	for i, img := range images {
		raw := strings.TrimSpace(img.URL)
		switch {
		case strings.HasPrefix(raw, "data:"):
			mediaType, data, err := DecodeDataURL(raw)
			if err != nil {
				return nil, invalid("image %d: %v", i+1, err)
			}
			if store == nil {
				return nil, ErrNotConfigured
			}
			key := strings.TrimSuffix(keyPrefix, "/") + "/" + newID() + "." + allowedMediaTypes[mediaType]
			if err := store.Put(ctx, key, mediaType, data); err != nil {
				return nil, fmt.Errorf("store image %d: %w", i+1, err)
			}
			refs = append(refs, Ref{Key: key, MediaType: mediaType})
		case strings.HasPrefix(raw, "https://"):
			if _, err := url.ParseRequestURI(raw); err != nil {
				return nil, invalid("image %d: invalid URL", i+1)
			}
			refs = append(refs, Ref{URL: raw})
		default:
			return nil, invalid("image %d: only data: and https:// image URLs are supported", i+1)
		}
	}
	return refs, nil
}

// DecodeDataURL parses a base64 data: URL holding an allowed image type. The
// declared type is checked against the bytes' sniffed type, so a mislabelled
// payload cannot reach the model or the bucket as something it is not.
func DecodeDataURL(raw string) (mediaType string, data []byte, err error) {
	header, payload, ok := strings.Cut(strings.TrimPrefix(raw, "data:"), ",")
	if !ok {
		return "", nil, errors.New("malformed data URL")
	}
	params := strings.Split(header, ";")
	mediaType = strings.ToLower(strings.TrimSpace(params[0]))
	isBase64 := false
	for _, p := range params[1:] {
		if strings.EqualFold(strings.TrimSpace(p), "base64") {
			isBase64 = true
		}
	}
	if !isBase64 {
		return "", nil, errors.New("data URL must be base64-encoded")
	}
	if _, ok := allowedMediaTypes[mediaType]; !ok {
		return "", nil, fmt.Errorf("unsupported image type %q (png, jpeg, gif or webp)", mediaType)
	}
	if base64.StdEncoding.DecodedLen(len(payload)) > MaxImageBytes+3 {
		return "", nil, fmt.Errorf("image too large (at most %d MB)", MaxImageBytes>>20)
	}
	data, err = base64.StdEncoding.DecodeString(payload)
	if err != nil {
		// Some clients drop padding.
		if data, err = base64.RawStdEncoding.DecodeString(strings.TrimRight(payload, "=")); err != nil {
			return "", nil, errors.New("data URL is not valid base64")
		}
	}
	if len(data) == 0 {
		return "", nil, errors.New("empty image")
	}
	if len(data) > MaxImageBytes {
		return "", nil, fmt.Errorf("image too large (at most %d MB)", MaxImageBytes>>20)
	}
	if sniffed := http.DetectContentType(data); sniffed != mediaType {
		return "", nil, fmt.Errorf("image content is %s, not the declared %s", sniffed, mediaType)
	}
	return mediaType, data, nil
}

// Resolver turns Refs back into something a consumer can use. A nil Store is
// allowed: external Refs still resolve, stored ones fail.
type Resolver struct {
	Store Store
	// PresignTTL is how long an agent pod's download URL stays valid.
	PresignTTL time.Duration
}

// DataURLs returns each Ref as a URL a vision model can take: stored images
// inlined as base64 data URLs (the provider may not be able to reach the
// bucket — a private MinIO, say), external ones as-is.
func (r *Resolver) DataURLs(ctx context.Context, refs []Ref) ([]string, error) {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref.URL != "" {
			out = append(out, ref.URL)
			continue
		}
		if r == nil || r.Store == nil {
			return nil, ErrNotConfigured
		}
		data, err := r.Store.Get(ctx, ref.Key)
		if err != nil {
			return nil, fmt.Errorf("load attachment %s: %w", ref.Key, err)
		}
		mediaType := ref.MediaType
		if mediaType == "" {
			mediaType = http.DetectContentType(data)
		}
		var b bytes.Buffer
		b.WriteString("data:")
		b.WriteString(mediaType)
		b.WriteString(";base64,")
		b.WriteString(base64.StdEncoding.EncodeToString(data))
		out = append(out, b.String())
	}
	return out, nil
}

// DownloadURLs returns each Ref as a URL an agent pod can fetch with a plain
// HTTP GET: stored images presigned for PresignTTL, external ones as-is.
func (r *Resolver) DownloadURLs(ctx context.Context, refs []Ref) ([]string, error) {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref.URL != "" {
			out = append(out, ref.URL)
			continue
		}
		if r == nil || r.Store == nil {
			return nil, ErrNotConfigured
		}
		ttl := r.PresignTTL
		if ttl <= 0 {
			ttl = 24 * time.Hour
		}
		u, err := r.Store.Presign(ctx, ref.Key, ttl)
		if err != nil {
			return nil, fmt.Errorf("presign attachment %s: %w", ref.Key, err)
		}
		out = append(out, u)
	}
	return out, nil
}

// Note is the deterministic text marker added to a message that carries
// images, so text-only consumers (retrieval, routing, the transcript) know
// they exist even though they cannot see them.
func Note(n int) string {
	if n == 1 {
		return "[The user attached 1 image to this message.]"
	}
	return fmt.Sprintf("[The user attached %d images to this message.]", n)
}

// DownloadBlock renders download URLs for an agent's goal text: an agent pod
// receives text only, so the images reach it as links it can fetch and view.
func DownloadBlock(urls []string) string {
	if len(urls) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("\n\n<attached_images>\n")
	b.WriteString("The user attached the image(s) below to this request. Download each one (e.g. with curl) and view it before acting; the links expire.\n")
	for _, u := range urls {
		b.WriteString("- ")
		b.WriteString(u)
		b.WriteString("\n")
	}
	b.WriteString("</attached_images>")
	return b.String()
}
