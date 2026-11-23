package gateway

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/aktisai/orchestrator/engines/temporal/internal/attachments"
)

// An OpenAI-style chat client (Open WebUI, say) resends the WHOLE transcript
// on every request, earlier messages' base64 images included, but a turn only
// uses the last user message's images (handleChatCompletions): earlier ones
// already live in the conversation workflow as Refs. So the body of
// /v1/chat/completions grows with every photo turn while what the gateway
// needs from it does not.
//
// readChatBody therefore streams the body once and keeps only what it needs:
// every large data:image/ string is lifted out and replaced by a short
// placeholder, and only the most recent few are kept. Memory is bounded by
// maxChatTextBytes of everything else plus MaxImagesPerTurn images, however
// many images the transcript carries.
const (
	// maxChatWireBytes bounds a /v1/chat/completions body on the wire. Older
	// images are discarded as they stream past, so this is not held in
	// memory; it only stops a runaway client.
	maxChatWireBytes = 256 << 20
	// maxChatTextBytes bounds the rest of the body — text, tool calls and
	// placeholders — which IS held in memory. Far beyond any model's context.
	maxChatTextBytes = 8 << 20
	// maxStashedRawBytes bounds one image string as held: a MaxImageBytes
	// image base64-encoded, with headroom for its data: header and for
	// JSON-escaped slashes (\/) some encoders emit.
	maxStashedRawBytes = attachments.MaxImageBytes*4/3 + attachments.MaxImageBytes/8 + 4<<10
	// stashThreshold is how long a string starting with data:image/ must be
	// before it is lifted out. Shorter strings stay inline untouched, so
	// ordinary text is never rewritten.
	stashThreshold = 1 << 10
	// stashPrefix marks a placeholder. It cannot collide with a client's
	// image URL: only data: and https:// URLs are accepted.
	stashPrefix = "aktis-stashed-image:"
)

// errChatBodyTooLarge is a body whose non-image content exceeds
// maxChatTextBytes, or whose wire size exceeds maxChatWireBytes.
var errChatBodyTooLarge = fmt.Errorf(
	"request body too large: at most %d MB of message text, and %d MB in total",
	maxChatTextBytes>>20, maxChatWireBytes>>20)

type stashedImage struct {
	raw      []byte // the JSON string's contents, still escaped
	tooLarge bool
}

// chatImageStash holds the most recent images lifted out of a chat body.
type chatImageStash struct {
	images map[int]stashedImage
	next   int
}

func (s *chatImageStash) add(img stashedImage) int {
	if s.images == nil {
		s.images = map[int]stashedImage{}
	}
	id := s.next
	s.next++
	s.images[id] = img
	// Keep only the newest MaxImagesPerTurn: the last user message is at (or
	// near) the end of the transcript, and it may carry no more than that.
	delete(s.images, id-attachments.MaxImagesPerTurn)
	return id
}

// resolve maps a placeholder back to the image URL the client sent. A URL
// that is not a placeholder is returned as-is.
func (s *chatImageStash) resolve(images []attachments.Image) ([]attachments.Image, error) {
	out := make([]attachments.Image, 0, len(images))
	for i, img := range images {
		idStr, ok := strings.CutPrefix(img.URL, stashPrefix)
		if !ok {
			out = append(out, img)
			continue
		}
		id, err := strconv.Atoi(idStr)
		stashed, kept := s.images[id]
		if err != nil || !kept {
			// Only the newest MaxImagesPerTurn images are kept, so this is a
			// last user message carrying more than that (or images placed
			// after it in the transcript).
			return nil, attachments.Invalidf("image %d: too many images (at most %d per message)", i+1, attachments.MaxImagesPerTurn)
		}
		if stashed.tooLarge {
			return nil, attachments.Invalidf("image %d: image too large (at most %d MB)", i+1, attachments.MaxImageBytes>>20)
		}
		var url string
		if err := json.Unmarshal(append(append([]byte{'"'}, stashed.raw...), '"'), &url); err != nil {
			return nil, attachments.Invalidf("image %d: malformed image URL", i+1)
		}
		out = append(out, attachments.Image{URL: url})
	}
	return out, nil
}

// readChatBody reads a chat request body, lifting large data:image/ strings
// out into the returned stash (see the comment at the top of this file). It
// does not validate the JSON: the decoder that reads the result does.
func readChatBody(body io.Reader) ([]byte, *chatImageStash, error) {
	const (
		outside   = iota         // between strings: copied
		undecided                // in a string that may still turn out to be an image
		plain                    // in a string that is not an image: copied
		image                    // in an image string: stashed
		prefix    = "data:image" // no slash: some encoders escape it as \/
	)
	r := bufio.NewReaderSize(body, 64<<10)
	stash := &chatImageStash{}
	var out bytes.Buffer
	write := func(b ...byte) error {
		if out.Len()+len(b) > maxChatTextBytes {
			return errChatBodyTooLarge
		}
		out.Write(b)
		return nil
	}

	state := outside
	escaped := false
	var pending []byte // an undecided string's bytes so far
	var img stashedImage
	for {
		b, err := r.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			if maxErr := new(http.MaxBytesError); errors.As(err, &maxErr) {
				return nil, nil, errChatBodyTooLarge
			}
			return nil, nil, err
		}
		if state == outside {
			if err := write(b); err != nil {
				return nil, nil, err
			}
			if b == '"' {
				state, escaped, pending = undecided, false, pending[:0]
			}
			continue
		}
		closing := b == '"' && !escaped
		escaped = b == '\\' && !escaped
		switch state {
		case undecided:
			if closing {
				// Short string: left inline, untouched.
				if err := write(append(pending, '"')...); err != nil {
					return nil, nil, err
				}
				state = outside
				continue
			}
			pending = append(pending, b)
			if n := min(len(pending), len(prefix)); !bytes.EqualFold(pending[:n], []byte(prefix[:n])) {
				if err := write(pending...); err != nil {
					return nil, nil, err
				}
				state = plain
			} else if len(pending) >= stashThreshold {
				img, state = stashedImage{raw: append([]byte(nil), pending...)}, image
			}
		case plain:
			if err := write(b); err != nil {
				return nil, nil, err
			}
			if closing {
				state = outside
			}
		case image:
			if closing {
				id := stash.add(img)
				img = stashedImage{}
				if err := write([]byte(stashPrefix + strconv.Itoa(id) + `"`)...); err != nil {
					return nil, nil, err
				}
				state = outside
				continue
			}
			if img.tooLarge {
				continue
			}
			if len(img.raw) >= maxStashedRawBytes {
				img = stashedImage{tooLarge: true} // drop the bytes; remember why
				continue
			}
			img.raw = append(img.raw, b)
		}
	}
	if state == undecided {
		// Unterminated string: keep it, so the decoder reports the syntax error.
		if err := write(pending...); err != nil {
			return nil, nil, err
		}
	}
	return out.Bytes(), stash, nil
}
