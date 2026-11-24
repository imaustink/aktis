package facade

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// OpenAITaskCompleter answers Open WebUI's housekeeping completions (title,
// tags, query generation) with one direct, non-agentic chat completion. They
// bypass the engine entirely, so a background UI call can never be misrouted
// into skill/agent delegation — into a privileged agent that would open real
// pull requests for what should be a cheap text completion.
type OpenAITaskCompleter struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

func (c *OpenAITaskCompleter) Complete(ctx context.Context, messages any, model string) (string, error) {
	payload, err := json.Marshal(map[string]any{"model": model, "temperature": 0, "messages": messages})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+c.APIKey)
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		text, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return "", fmt.Errorf("openai chat completion: %d %s", res.StatusCode, text)
	}
	var parsed struct {
		Choices []struct {
			Message struct {
				Content *string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return "", err
	}
	if len(parsed.Choices) == 0 || parsed.Choices[0].Message.Content == nil {
		return "", nil
	}
	return *parsed.Choices[0].Message.Content, nil
}
