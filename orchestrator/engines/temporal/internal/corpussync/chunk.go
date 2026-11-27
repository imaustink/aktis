package corpussync

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode/utf16"
)

// Chunking, which is where retrieval quality actually comes from
// (docs/adr/0039 §6): cut on the boundaries the source already has --
// headings first, then paragraphs -- and only then on lines, words and length.
//
// BYTE-COMPATIBLE WITH THE TYPESCRIPT CHUNKER THIS REPLACES (docs/adr/0059 §5).
// Chunk boundaries decide content hashes, and content hashes are point ids: a
// chunk cut one character differently is a new point, re-embedded, with the
// old one deleted. So sizes are counted in UTF-16 code units (JavaScript's
// string length), the character cut slices UTF-16, and whitespace means
// JavaScript's `\s` / `trim()` set, not Go's unicode.IsSpace (which differ on
// U+0085 and U+FEFF). testdata/ts-parity.json, written by the TS
// implementation, holds this file to that.

// Document is one resource with its body, as the broker serves it
// (GET /corpora/:name/resources/:id).
type Document struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	URL       string `json:"url"`
	Version   string `json:"version,omitempty"`
	UpdatedAt string `json:"updatedAt,omitempty"`
	ACL       *ACL   `json:"acl,omitempty"`
	Markdown  string `json:"markdown"`
}

// ACL is the driver's mirror of a resource's read restrictions (ADR 0040).
type ACL struct {
	Principals []string `json:"principals"`
	Permissive *bool    `json:"permissive,omitempty"`
}

// Chunk is one passage to embed. The JSON names are the descriptor's
// (corpus.Chunk) plus nothing else.
type Chunk struct {
	ConnectionID    string `json:"connectionId"`
	ConnectionLabel string `json:"connectionLabel"`
	SourceID        string `json:"sourceId"`
	SourceURL       string `json:"sourceUrl"`
	Title           string `json:"title"`
	UpdatedAt       string `json:"updatedAt,omitempty"`
	Version         string `json:"version,omitempty"`
	// ContentHash is sha256 over the normalized text plus the source it came
	// from, and doubles as the point id (docs/adr/0039 §7): the same chunk of
	// the same document is stable across syncs, so a re-sync re-embeds only
	// what changed.
	ContentHash   string   `json:"contentHash"`
	ACLPrincipals []string `json:"aclPrincipals,omitempty"`
	ACLPermissive *bool    `json:"aclPermissive,omitempty"`
	Text          string   `json:"text"`
}

// ChunkOptions overrides the defaults; nil fields take them.
type ChunkOptions struct {
	// MaxTokens is the target upper bound, in approximate tokens.
	MaxTokens *int `json:"maxTokens,omitempty"`
	// Overlap is how much adjacent chunks share, so a passage split across a
	// seam is still findable from either side.
	Overlap *int `json:"overlap,omitempty"`
}

const (
	defaultMaxTokens = 800
	defaultOverlap   = 100
	// maxChunkTokens caps the effective target well under the embedding
	// model's 8192-token input limit: one over-long chunk 400s the embed, and
	// nothing can rescue a lone over-cap input, so it would fail the whole
	// corpus. A larger configured target is clamped, and a piece still over
	// budget is force-split even if that severs a sentence.
	maxChunkTokens = 6000
)

// ChunkDocument splits a document into chunks: first on Markdown headings
// (the author's own statement of where one idea ends), then any section
// still over budget on paragraphs with overlap. A section under budget is
// left whole even if tiny; merging across a heading would undo the boundary.
func ChunkDocument(connectionID, connectionLabel string, doc Document, opts ChunkOptions) []Chunk {
	maxTokens := defaultMaxTokens
	if opts.MaxTokens != nil {
		maxTokens = *opts.MaxTokens
	}
	maxTokens = min(maxTokens, maxChunkTokens)
	overlap := defaultOverlap
	if opts.Overlap != nil {
		overlap = *opts.Overlap
	}

	var pieces []string
	for _, section := range splitOnHeadings(doc.Markdown) {
		if approxTokens(section) <= maxTokens {
			pieces = append(pieces, section)
			continue
		}
		pieces = append(pieces, splitOnParagraphs(section, maxTokens, overlap)...)
	}

	var chunks []Chunk
	for _, piece := range pieces {
		text := jsTrim(piece)
		if text == "" {
			continue
		}
		c := Chunk{
			ConnectionID:    connectionID,
			ConnectionLabel: connectionLabel,
			SourceID:        doc.ID,
			SourceURL:       doc.URL,
			Title:           doc.Title,
			UpdatedAt:       doc.UpdatedAt,
			Version:         doc.Version,
			ContentHash:     HashChunk(connectionID, doc.ID, text),
			Text:            text,
		}
		if doc.ACL != nil {
			c.ACLPrincipals = doc.ACL.Principals
			c.ACLPermissive = doc.ACL.Permissive
		}
		chunks = append(chunks, c)
	}
	return chunks
}

// HashChunk is the content hash over whitespace-normalized text and its
// source. Normalizing first means a reflowed paragraph that says the same
// thing does not read as a change and force a re-embed.
func HashChunk(connectionID, sourceID, text string) string {
	normalized := jsTrim(collapseJSSpace(text))
	sum := sha256.Sum256([]byte(connectionID + "\x00" + sourceID + "\x00" + normalized))
	return hex.EncodeToString(sum[:])
}

// splitOnHeadings keeps each heading with the body beneath it: the heading
// is usually the most retrievable sentence in a section.
func splitOnHeadings(markdown string) []string {
	var sections []string
	var current []string
	for _, line := range strings.Split(markdown, "\n") {
		if isHeading(line) && anyNonBlank(current) {
			sections = append(sections, strings.Join(current, "\n"))
			current = []string{line}
			continue
		}
		current = append(current, line)
	}
	if len(current) > 0 {
		sections = append(sections, strings.Join(current, "\n"))
	}
	if len(sections) == 0 {
		return []string{markdown}
	}
	return sections
}

// splitOnParagraphs splits an over-budget section on blank lines, carrying
// `overlap` tokens' worth of the previous chunk's tail into the next, and
// repeating the section's heading onto every piece rather than orphaning it
// as a contentless chunk of its own.
func splitOnParagraphs(section string, maxTokens, overlap int) []string {
	paragraphs := splitBlankLines(section)

	heading := ""
	hasHeading := len(paragraphs) > 0 && isHeading(jsTrim(paragraphs[0]))
	body := paragraphs
	if hasHeading {
		heading = paragraphs[0]
		body = paragraphs[1:]
	}
	withHeading := func(text string) string {
		if hasHeading {
			return heading + "\n\n" + text
		}
		return text
	}

	var chunks []string
	var current []string
	flush := func() {
		if len(current) == 0 {
			return
		}
		chunks = append(chunks, withHeading(strings.Join(current, "\n\n")))
		if overlap > 0 {
			current = tail(current, overlap)
		} else {
			current = nil
		}
	}

	for _, paragraph := range body {
		units := []string{paragraph}
		if approxTokens(paragraph) > maxTokens {
			units = forceSplit(paragraph, maxTokens)
		}
		for _, unit := range units {
			candidate := strings.Join(append(append([]string{}, current...), unit), "\n\n")
			if len(current) > 0 && approxTokens(candidate) > maxTokens {
				flush()
			}
			current = append(current, unit)
		}
	}
	if len(current) > 0 {
		chunks = append(chunks, withHeading(strings.Join(current, "\n\n")))
	}
	if len(chunks) == 0 {
		return []string{section}
	}
	return chunks
}

// forceSplit is the last resort for a paragraph with no blank-line seam:
// lines, then words, then a hard cut by length. Every piece is at most
// maxTokens.
func forceSplit(text string, maxTokens int) []string {
	if approxTokens(text) <= maxTokens {
		return []string{text}
	}

	for _, sep := range []string{"\n", " "} {
		if !strings.Contains(text, sep) {
			continue
		}
		parts := strings.Split(text, sep)
		if len(parts) < 2 {
			continue
		}
		var out []string
		buf := ""
		for _, part := range parts {
			if approxTokens(part) > maxTokens {
				if buf != "" {
					out = append(out, buf)
					buf = ""
				}
				out = append(out, forceSplit(part, maxTokens)...)
				continue
			}
			candidate := part
			if buf != "" {
				candidate = buf + sep + part
			}
			if buf != "" && approxTokens(candidate) > maxTokens {
				out = append(out, buf)
				buf = part
			} else {
				buf = candidate
			}
		}
		if buf != "" {
			out = append(out, buf)
		}
		return out
	}

	// An unbroken run with no line or space: the only seam left is length,
	// measured as JavaScript measured it. A surrogate pair cut in two becomes
	// U+FFFD on each side, which is also what the TS implementation hashed
	// (Node encodes a lone surrogate as U+FFFD).
	units := utf16.Encode([]rune(text))
	window := max(1, maxTokens*4)
	var windows []string
	for i := 0; i < len(units); i += window {
		windows = append(windows, string(utf16.Decode(units[i:min(i+window, len(units))])))
	}
	return windows
}

// tail is the trailing paragraphs of a chunk, up to roughly `overlap` tokens.
func tail(paragraphs []string, overlap int) []string {
	var kept []string
	budget := overlap
	for i := len(paragraphs) - 1; i >= 0 && budget > 0; i-- {
		kept = append([]string{paragraphs[i]}, kept...)
		budget -= approxTokens(paragraphs[i])
	}
	return kept
}

// approxTokens is a deliberately cheap estimate: a quarter of the UTF-16 length.
func approxTokens(text string) int {
	return (utf16Len(text) + 3) / 4
}

func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		if r >= 0x10000 {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// isHeading is /^#{1,6}\s/: one to six '#' and then whitespace.
func isHeading(line string) bool {
	n := 0
	for n < len(line) && line[n] == '#' {
		n++
	}
	if n < 1 || n > 6 {
		return false
	}
	for _, r := range line[n:] {
		return isJSSpace(r)
	}
	return false
}

func anyNonBlank(lines []string) bool {
	for _, l := range lines {
		if jsTrim(l) != "" {
			return true
		}
	}
	return false
}

// splitBlankLines is String.prototype.split(/\n{2,}/).
func splitBlankLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); {
		if s[i] == '\n' && i+1 < len(s) && s[i+1] == '\n' {
			out = append(out, s[start:i])
			j := i
			for j < len(s) && s[j] == '\n' {
				j++
			}
			i, start = j, j
			continue
		}
		i++
	}
	return append(out, s[start:])
}

// isJSSpace is JavaScript's \s: WhiteSpace and LineTerminator.
func isJSSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0x00a0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
		return true
	}
	return r >= 0x2000 && r <= 0x200a
}

// jsTrim is String.prototype.trim.
func jsTrim(s string) string {
	return strings.TrimFunc(s, isJSSpace)
}

// collapseJSSpace is s.replace(/\s+/g, " ").
func collapseJSSpace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSpace := false
	for _, r := range s {
		if isJSSpace(r) {
			if !inSpace {
				b.WriteByte(' ')
			}
			inSpace = true
			continue
		}
		inSpace = false
		b.WriteRune(r)
	}
	return b.String()
}
