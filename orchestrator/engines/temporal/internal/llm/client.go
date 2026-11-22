// Package llm is a minimal OpenAI-compatible chat-completions client.
//
// It is deliberately dependency-free: milestone 1 only needs plain
// completions, and a hand-rolled client keeps the base URL overridable
// (LiteLLM, Ollama, etc.). Swap for the official SDK if/when structured
// outputs get complex enough to warrant it.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	// Images are image URLs (data: or https://) shown to a vision model
	// alongside Content. Never serialized as part of Message itself — a
	// Message also travels in Temporal payloads, where image bytes must not —
	// only folded into the wire request by wireMessages.
	Images []string `json:"-"`
}

// wireMessage is one message as the chat-completions API takes it: Content
// is a plain string, or the multi-part array when the message has images.
type wireMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type contentPart struct {
	Type     string    `json:"type"`
	Text     string    `json:"text,omitempty"`
	ImageURL *imageURL `json:"image_url,omitempty"`
}

type imageURL struct {
	URL string `json:"url"`
}

func wireMessages(messages []Message) []wireMessage {
	out := make([]wireMessage, len(messages))
	for i, m := range messages {
		if len(m.Images) == 0 {
			out[i] = wireMessage{Role: m.Role, Content: m.Content}
			continue
		}
		parts := make([]contentPart, 0, len(m.Images)+1)
		if m.Content != "" {
			parts = append(parts, contentPart{Type: "text", Text: m.Content})
		}
		for _, u := range m.Images {
			parts = append(parts, contentPart{Type: "image_url", ImageURL: &imageURL{URL: u}})
		}
		out[i] = wireMessage{Role: m.Role, Content: parts}
	}
	return out
}

type Client struct {
	baseURL string
	apiKey  string
	model   string
	http    *http.Client
}

func New(baseURL, apiKey, model string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		http:    &http.Client{Timeout: 120 * time.Second},
	}
}

func (c *Client) Model() string { return c.model }

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []wireMessage `json:"messages"`
}

type chatResponse struct {
	Choices []struct {
		Message Message `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
	// Usage is token accounting, recorded on the call's generation span
	// (docs/adr/0050). Absent from some OpenAI-compatible servers.
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage,omitempty"`
}

// Complete sends one chat-completions request and returns the assistant text.
func (c *Client) Complete(ctx context.Context, messages []Message) (_ string, err error) {
	ctx, span := c.startGeneration(ctx, "chat", "")
	defer func() { endGeneration(span, err) }()

	body, err := json.Marshal(chatRequest{Model: c.model, Messages: wireMessages(messages)})
	if err != nil {
		return "", fmt.Errorf("marshal chat request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build chat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("chat request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read chat response: %w", err)
	}

	var parsed chatResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("decode chat response (status %d): %w", resp.StatusCode, err)
	}
	recordUsage(span, parsed)
	if resp.StatusCode != http.StatusOK {
		msg := string(raw)
		if parsed.Error != nil {
			msg = parsed.Error.Message
		}
		return "", fmt.Errorf("chat completions returned %d: %s", resp.StatusCode, msg)
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("chat completions returned no choices")
	}
	return parsed.Choices[0].Message.Content, nil
}
