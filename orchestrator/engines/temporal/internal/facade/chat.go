package facade

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/google/uuid"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
)

// ModelID is the one model this facade advertises.
const ModelID = "agent-orchestrator"

// Bounds on the prior conversation folded into a chat turn's request.
const (
	historyMaxMessages = 8
	historyMaxChars    = 24_000
)

// SelfImprovementFooter is what the removed in-process engine appended to an
// ad-hoc reply. Nothing produces it any more, but chats that predate the
// switch still carry it, and left in their folded history its "no existing
// skill matched" wording biases the next turn's selection toward "no match".
const SelfImprovementFooter = "\n\n---\nNo existing skill or agent matched this request, so it was handled ad-hoc. " +
	"Ask me to run the self-improvement skill if you'd like a permanent skill added for this next time."

// ChatImage is one image on a chat message: a data: or https:// URL.
type ChatImage struct {
	URL string `json:"url"`
}

// AgentRequest is what BuildAgentRequest extracts from a messages array.
type AgentRequest struct {
	// Request is the text the engine sees, with any folded history.
	Request string
	// PriorToolCalls are calls the CALLER already executed for the exchange in
	// flight, paired with their results (docs/adr/0035 §1).
	PriorToolCalls []callertools.PriorCall
	// Images on the new user message only.
	Images []ChatImage
}

// MessageText is a message's text: the string content, or the text parts of
// an array content joined by newlines. Treating only the string form as text
// once made every image-bearing message look empty.
func MessageText(content any) string {
	switch c := content.(type) {
	case string:
		return c
	case []any:
		var parts []string
		for _, raw := range c {
			part, ok := raw.(map[string]any)
			if !ok || part["type"] != "text" {
				continue
			}
			if text, ok := part["text"].(string); ok {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

// MessageImages is a message's image_url parts, in order. OpenAI's shape is
// {url}; some clients send the bare string.
func MessageImages(content any) []ChatImage {
	parts, ok := content.([]any)
	if !ok {
		return nil
	}
	var images []ChatImage
	for _, raw := range parts {
		part, ok := raw.(map[string]any)
		if !ok || part["type"] != "image_url" {
			continue
		}
		var url string
		switch v := part["image_url"].(type) {
		case string:
			url = v
		case map[string]any:
			url, _ = v["url"].(string)
		}
		if url = strings.TrimSpace(url); url != "" {
			images = append(images, ChatImage{URL: url})
		}
	}
	return images
}

func lastMessageIndex(messages []any, role string) int {
	for i := len(messages) - 1; i >= 0; i-- {
		m, ok := messages[i].(map[string]any)
		if !ok || m["role"] != role {
			continue
		}
		if strings.TrimSpace(MessageText(m["content"])) != "" || len(MessageImages(m["content"])) > 0 {
			return i
		}
	}
	return -1
}

// BuildAgentRequest turns an OpenAI messages array into the engine's single
// request string. OpenAI-style clients send the WHOLE conversation; a bounded
// window of prior user/assistant turns is folded ahead of the new user
// message in a <conversation_history> tag (both roles: content a user pasted
// two turns ago matters as much as what the agent said). Caller-executed tool
// calls are returned separately, never folded into prose. nil when there is
// no non-empty user message.
func BuildAgentRequest(rawMessages any) *AgentRequest {
	messages, ok := rawMessages.([]any)
	if !ok {
		return nil
	}
	userIdx := lastMessageIndex(messages, "user")
	if userIdx == -1 {
		return nil
	}
	user := messages[userIdx].(map[string]any)
	userContent := MessageText(user["content"])
	result := &AgentRequest{
		Images:         MessageImages(user["content"]),
		PriorToolCalls: collectPriorToolCalls(messages, userIdx),
	}

	type folded struct{ role, content string }
	var prior []folded
	for i := userIdx - 1; i >= 0 && len(prior) < historyMaxMessages; i-- {
		m, ok := messages[i].(map[string]any)
		if !ok {
			continue
		}
		role, _ := m["role"].(string)
		if role != "user" && role != "assistant" {
			continue
		}
		text := MessageText(m["content"])
		if strings.TrimSpace(text) == "" {
			continue
		}
		if role == "assistant" {
			text = strings.TrimSuffix(text, SelfImprovementFooter)
		}
		prior = append([]folded{{role, text}}, prior...)
	}
	total := 0
	for _, m := range prior {
		total += jsLen(m.content)
	}
	for len(prior) > 0 && total > historyMaxChars {
		total -= jsLen(prior[0].content)
		prior = prior[1:]
	}
	if len(prior) == 0 {
		result.Request = userContent
		return result
	}
	var history []string
	for _, m := range prior {
		history = append(history, fmt.Sprintf("<message role=%q>\n%s\n</message>", m.role, m.content))
	}
	result.Request = "<conversation_history>\n" + strings.Join(history, "\n") + "\n</conversation_history>\n\n" + userContent
	return result
}

// jsLen is a string's length in UTF-16 code units, which is what the TS
// facade's character budgets measured.
func jsLen(s string) int { return len(utf16.Encode([]rune(s))) }

// collectPriorToolCalls pairs assistant.tool_calls with their role:"tool"
// results, scoped to messages AFTER the last user turn (the exchange in
// flight). An unmatched result is skipped rather than guessed at.
func collectPriorToolCalls(messages []any, userIdx int) []callertools.PriorCall {
	type requested struct{ name, arguments string }
	calls := map[string]requested{}
	for i := userIdx + 1; i < len(messages); i++ {
		m, ok := messages[i].(map[string]any)
		if !ok || m["role"] != "assistant" {
			continue
		}
		list, ok := m["tool_calls"].([]any)
		if !ok {
			continue
		}
		for _, raw := range list {
			call, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			id, ok := call["id"].(string)
			if !ok {
				continue
			}
			fn, _ := call["function"].(map[string]any)
			name, ok := fn["name"].(string)
			if !ok {
				continue
			}
			args, isString := fn["arguments"].(string)
			if !isString {
				value := fn["arguments"]
				if value == nil {
					value = map[string]any{}
				}
				args = jsonString(value)
			}
			calls[id] = requested{name, args}
		}
	}
	if len(calls) == 0 {
		return nil
	}
	var out []callertools.PriorCall
	for i := userIdx + 1; i < len(messages); i++ {
		m, ok := messages[i].(map[string]any)
		if !ok || m["role"] != "tool" {
			continue
		}
		id, ok := m["tool_call_id"].(string)
		if !ok {
			continue
		}
		req, ok := calls[id]
		if !ok {
			continue
		}
		content, isString := m["content"].(string)
		if !isString {
			content = jsonString(m["content"])
		}
		out = append(out, callertools.PriorCall{ID: id, Name: req.name, Arguments: req.arguments, Result: content})
	}
	return out
}

// jsonString is JSON.stringify for decoded JSON: compact, no HTML escaping.
func jsonString(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	return strings.TrimSuffix(buf.String(), "\n")
}

// IsInternalUITask: Open WebUI's housekeeping completions (title, tags,
// search query, follow-ups) start with "### Task:". They must never reach
// skill/agent delegation.
func IsInternalUITask(userContent string) bool {
	return callertools.IsInternalUITask(userContent)
}

// RenderResult renders a turn result as chat content.
func RenderResult(result any) string {
	if s, ok := result.(string); ok {
		return s
	}
	pretty, _ := json.MarshalIndent(result, "", "  ")
	return "```json\n" + string(pretty) + "\n```"
}

func chatCompletionID() string { return "chatcmpl-" + uuid.NewString() }

func nowUnix() int64 { return time.Now().Unix() }

// ── wire shapes ────────────────────────────────────────────────────────────

func listModelsResponse() map[string]any {
	return map[string]any{
		"object": "list",
		"data":   []any{map[string]any{"id": ModelID, "object": "model", "created": nowUnix(), "owned_by": "aktis"}},
	}
}

func chatCompletionChunk(id, model string, delta map[string]any, finishReason *string) map[string]any {
	var finish any
	if finishReason != nil {
		finish = *finishReason
	}
	return map[string]any{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": nowUnix(),
		"model":   model,
		"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}},
	}
}

func toolCallsPayload(calls []callertools.PendingCall, withIndex bool) []any {
	out := make([]any, len(calls))
	for i, call := range calls {
		entry := map[string]any{
			"id":       call.ID,
			"type":     "function",
			"function": map[string]any{"name": call.Name, "arguments": call.Arguments},
		}
		if withIndex {
			entry["index"] = i
		}
		out[i] = entry
	}
	return out
}

// chatCompletionToolCallResponse is a blocking tool-call turn: content null
// and finish_reason "tool_calls" are what tell a client to execute something.
func chatCompletionToolCallResponse(id, model string, calls []callertools.PendingCall) map[string]any {
	return map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": nowUnix(),
		"model":   model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": nil, "tool_calls": toolCallsPayload(calls, false)},
			"finish_reason": "tool_calls",
		}},
		"usage": zeroUsage(),
	}
}

// toolCallDeltaChunk is the streaming counterpart: one delta with the whole
// array (arguments are never streamed partially); index is how a streaming
// client assembles more than one call.
func toolCallDeltaChunk(id, model string, calls []callertools.PendingCall) map[string]any {
	return chatCompletionChunk(id, model, map[string]any{"role": "assistant", "tool_calls": toolCallsPayload(calls, true)}, nil)
}

func chatCompletionResponse(id, model, content, finishReason string) map[string]any {
	return map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": nowUnix(),
		"model":   model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": content},
			"finish_reason": finishReason,
		}},
		// Zeroed rather than estimated, so clients don't mistake it for usage.
		"usage": zeroUsage(),
	}
}

func zeroUsage() map[string]any {
	return map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
}

// openAIError is OpenAI's error envelope, which clients (Open WebUI) special-case.
func openAIError(message, code string) map[string]any {
	return map[string]any{"error": map[string]any{"message": message, "type": "invalid_request_error", "code": code}}
}

// errorStatusAndCode maps a turn failure to the non-streaming response's
// status and OpenAI code. A streaming response has already sent 200, so it
// renders the error as the final assistant message instead.
func errorStatusAndCode(message string) (int, string) {
	switch {
	case strings.HasPrefix(message, "unauthorized"):
		return http.StatusUnauthorized, "unauthorized"
	case strings.HasPrefix(message, "no matching skill"):
		return http.StatusUnprocessableEntity, "no_skill_available"
	case strings.HasPrefix(message, "skill has no usable tools"):
		return http.StatusUnprocessableEntity, "no_tool_available"
	case strings.HasPrefix(message, "planner selected a tool outside"):
		return http.StatusInternalServerError, "internal_error"
	case strings.HasPrefix(message, "tool failed"):
		return http.StatusBadGateway, "tool_failed"
	}
	return http.StatusInternalServerError, "internal_error"
}

// renderProgressLabel is the Open WebUI status label for a mechanical step:
// "<stage>: <message>" with the message clamped to 120 characters, else the
// bare stage, else a placeholder. Byte-for-byte what
// @aktis/messaging's renderTurnEvent produced for a tool-progress event.
func renderProgressLabel(stage, message string) string {
	if message != "" {
		units := utf16.Encode([]rune(message))
		if len(units) > 120 {
			message = string(utf16.Decode(units[:120]))
		}
		if stage != "" {
			return stage + ": " + message
		}
		return message
	}
	if stage != "" {
		return stage
	}
	return "working…"
}
