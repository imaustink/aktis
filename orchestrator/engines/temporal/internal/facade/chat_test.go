package facade

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
)

// Ported from openai/chat-completions.test.ts.

// msgs decodes a messages array the way the server does (from JSON), so the
// shapes match what a real request carries.
func msgs(t *testing.T, raw string) any {
	t.Helper()
	var v any
	require.NoError(t, json.Unmarshal([]byte(raw), &v))
	return v
}

func TestBuildAgentRequestReturnsTheLatestUserMessage(t *testing.T) {
	require.Equal(t, &AgentRequest{Request: "hello"}, BuildAgentRequest(msgs(t, `[{"role":"user","content":"hello"}]`)))
}

func TestBuildAgentRequestReadsTextAndImagesFromAMultiPartMessage(t *testing.T) {
	built := BuildAgentRequest(msgs(t, `[{"role":"user","content":[
		{"type":"text","text":"Extract this recipe: https://www.tiktok.com/t/x/"},
		{"type":"text","text":"I also attached a photo of the ingredients."},
		{"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgo="}}
	]}]`))
	require.Equal(t, &AgentRequest{
		Request: "Extract this recipe: https://www.tiktok.com/t/x/\nI also attached a photo of the ingredients.",
		Images:  []ChatImage{{URL: "data:image/png;base64,iVBORw0KGgo="}},
	}, built)
}

func TestBuildAgentRequestAcceptsAnImageWithNoText(t *testing.T) {
	built := BuildAgentRequest(msgs(t, `[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}]}]`))
	require.Equal(t, &AgentRequest{Request: "", Images: []ChatImage{{URL: "https://example.com/a.png"}}}, built)
}

// Some clients send image_url as a bare string; blank URLs are dropped.
func TestMessageImagesAcceptsABareStringAndDropsBlanks(t *testing.T) {
	images := MessageImages(msgs(t, `[{"type":"image_url","image_url":" https://e/a.png "},{"type":"image_url","image_url":{"url":"  "}},{"type":"text","text":"x"}]`))
	require.Equal(t, []ChatImage{{URL: "https://e/a.png"}}, images)
	require.Nil(t, MessageImages("plain"))
}

func TestBuildAgentRequestForwardsOnlyTheLatestUserImages(t *testing.T) {
	built := BuildAgentRequest(msgs(t, `[
		{"role":"user","content":[{"type":"text","text":"what is this?"},{"type":"image_url","image_url":{"url":"https://example.com/old.png"}}]},
		{"role":"assistant","content":"A shrimp orzo ingredient list."},
		{"role":"user","content":"convert it to metric"}
	]`))
	require.Empty(t, built.Images)
	require.Contains(t, built.Request, "<message role=\"user\">\nwhat is this?\n</message>")
}

func TestBuildAgentRequestIsNilWithoutAUsableUserMessage(t *testing.T) {
	require.Nil(t, BuildAgentRequest(nil))
	require.Nil(t, BuildAgentRequest("not an array"))
	require.Nil(t, BuildAgentRequest(msgs(t, `[]`)))
	require.Nil(t, BuildAgentRequest(msgs(t, `[{"role":"assistant","content":"hi"}]`)))
	require.Nil(t, BuildAgentRequest(msgs(t, `[{"role":"user","content":"   "}]`)))
}

func TestBuildAgentRequestFoldsPriorTurns(t *testing.T) {
	built := BuildAgentRequest(msgs(t, `[{"role":"user","content":"first"},{"role":"assistant","content":"answer"},{"role":"user","content":"second"}]`))
	require.Equal(t, "<conversation_history>\n<message role=\"user\">\nfirst\n</message>\n<message role=\"assistant\">\nanswer\n</message>\n</conversation_history>\n\nsecond", built.Request)
}

// A later empty user message (e.g. a tool-only turn) does not hide the real one.
func TestBuildAgentRequestSkipsSystemToolAndEmptyMessagesInTheFold(t *testing.T) {
	built := BuildAgentRequest(msgs(t, `[
		{"role":"system","content":"be nice"},
		{"role":"user","content":"first"},
		{"role":"assistant","content":"   "},
		{"role":"tool","tool_call_id":"x","content":"ignored"},
		{"role":"user","content":"second"},
		{"role":"user","content":""}
	]`))
	require.Equal(t, "<conversation_history>\n<message role=\"user\">\nfirst\n</message>\n</conversation_history>\n\nsecond", built.Request)
}

func TestBuildAgentRequestBoundsTheFoldedHistory(t *testing.T) {
	var parts []string
	for i := 0; i < 12; i++ {
		parts = append(parts, `{"role":"user","content":"m`+string(rune('a'+i))+`"}`)
	}
	parts = append(parts, `{"role":"user","content":"now"}`)
	built := BuildAgentRequest(msgs(t, "["+strings.Join(parts, ",")+"]"))
	require.Equal(t, historyMaxMessages, strings.Count(built.Request, "<message role="))
	require.NotContains(t, built.Request, "\nmd\n", "the oldest messages are dropped first")
	require.Contains(t, built.Request, "\nme\n")

	// The character budget drops the OLDEST until the rest fits, measured in
	// UTF-16 units as the TS facade measured it.
	big := strings.Repeat("é", historyMaxChars-1)
	built = BuildAgentRequest([]any{
		map[string]any{"role": "user", "content": "old"},
		map[string]any{"role": "assistant", "content": big},
		map[string]any{"role": "user", "content": "now"},
	})
	require.NotContains(t, built.Request, "\nold\n")
	require.Contains(t, built.Request, big)
	require.Equal(t, 2, jsLen("😀"))
}

func TestBuildAgentRequestStripsTheLegacySelfImprovementFooter(t *testing.T) {
	built := BuildAgentRequest([]any{
		map[string]any{"role": "user", "content": "q"},
		map[string]any{"role": "assistant", "content": "answer" + SelfImprovementFooter},
		map[string]any{"role": "user", "content": "next"},
	})
	require.Contains(t, built.Request, "\nanswer\n</message>")
	require.NotContains(t, built.Request, "No existing skill or agent matched")
}

// ── caller-executed tool results (docs/adr/0035 §1) ────────────────────────

const resumed = `[
	{"role":"user","content":"what's the weather in Chicago?"},
	{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Chicago\"}"}}]},
	{"role":"tool","tool_call_id":"call_1","content":"58F and raining"}
]`

func TestPriorToolCallsPairAResultBackToItsCall(t *testing.T) {
	require.Equal(t, []callertools.PriorCall{{ID: "call_1", Name: "get_weather", Arguments: `{"city":"Chicago"}`, Result: "58F and raining"}},
		BuildAgentRequest(msgs(t, resumed)).PriorToolCalls)
}

func TestPriorToolCallsStayOutOfTheProseRequest(t *testing.T) {
	built := BuildAgentRequest(msgs(t, resumed))
	require.Equal(t, "what's the weather in Chicago?", built.Request)
	require.NotContains(t, built.Request, "58F")
}

func TestPriorToolCallsCollectsParallelCalls(t *testing.T) {
	built := BuildAgentRequest(msgs(t, `[
		{"role":"user","content":"compare Chicago and Denver"},
		{"role":"assistant","content":null,"tool_calls":[
			{"id":"a","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Chicago\"}"}},
			{"id":"b","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Denver\"}"}}]},
		{"role":"tool","tool_call_id":"a","content":"58F"},
		{"role":"tool","tool_call_id":"b","content":"71F"}
	]`))
	var results []string
	for _, c := range built.PriorToolCalls {
		results = append(results, c.Result)
	}
	require.Equal(t, []string{"58F", "71F"}, results)
}

func TestPriorToolCallsIgnoreAnAlreadyCompletedEarlierExchange(t *testing.T) {
	built := BuildAgentRequest(msgs(t, `[
		{"role":"user","content":"weather?"},
		{"role":"assistant","content":null,"tool_calls":[{"id":"old","type":"function","function":{"name":"get_weather","arguments":"{}"}}]},
		{"role":"tool","tool_call_id":"old","content":"58F"},
		{"role":"assistant","content":"It's 58F and raining."},
		{"role":"user","content":"thanks, and tomorrow?"}
	]`))
	require.Empty(t, built.PriorToolCalls)
	require.Contains(t, built.Request, "It's 58F and raining.")
}

func TestPriorToolCallsSkipAResultWithNoMatchingCall(t *testing.T) {
	built := BuildAgentRequest(msgs(t, `[{"role":"user","content":"weather?"},{"role":"tool","tool_call_id":"orphan","content":"58F"}]`))
	require.Empty(t, built.PriorToolCalls)
}

func TestPriorToolCallsStringifyNonStringResultsAndArguments(t *testing.T) {
	built := BuildAgentRequest(msgs(t, `[
		{"role":"user","content":"weather?"},
		{"role":"assistant","content":null,"tool_calls":[
			{"id":"c","type":"function","function":{"name":"f","arguments":{"city":"<Oslo>"}}},
			{"id":"d","type":"function","function":{"name":"g"}}]},
		{"role":"tool","tool_call_id":"c","content":{"tempF":58}},
		{"role":"tool","tool_call_id":"d","content":"ok"}
	]`))
	require.Equal(t, []callertools.PriorCall{
		// JSON.stringify: compact, and no HTML escaping of <>.
		{ID: "c", Name: "f", Arguments: `{"city":"<Oslo>"}`, Result: `{"tempF":58}`},
		{ID: "d", Name: "g", Arguments: `{}`, Result: "ok"},
	}, built.PriorToolCalls)
}

func TestPriorToolCallsEmptyForAnOrdinaryTurn(t *testing.T) {
	require.Empty(t, BuildAgentRequest(msgs(t, `[{"role":"user","content":"hi"}]`)).PriorToolCalls)
}

func TestIsInternalUITask(t *testing.T) {
	require.True(t, IsInternalUITask("### Task:\nGenerate a title"))
	require.True(t, IsInternalUITask("  \n### Task:\nGenerate tags"))
	require.False(t, IsInternalUITask("what's the weather?"))
}

func TestRenderResult(t *testing.T) {
	require.Equal(t, "plain", RenderResult("plain"))
	require.Equal(t, "```json\n{\n  \"a\": 1\n}\n```", RenderResult(map[string]any{"a": 1}))
}

// ── tool_calls response shapes ─────────────────────────────────────────────

var shapeCalls = []callertools.PendingCall{{ID: "call_1", Name: "get_weather", Arguments: `{"city":"Chicago"}`}}

func roundTrip(t *testing.T, v any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func TestToolCallResponseHasNullContentAndToolCallsFinish(t *testing.T) {
	body := roundTrip(t, chatCompletionToolCallResponse("id-1", "agent-orchestrator", shapeCalls))
	choice := body["choices"].([]any)[0].(map[string]any)
	require.Equal(t, "tool_calls", choice["finish_reason"])
	message := choice["message"].(map[string]any)
	require.Contains(t, message, "content")
	require.Nil(t, message["content"])
	require.Equal(t, []any{map[string]any{"id": "call_1", "type": "function", "function": map[string]any{"name": "get_weather", "arguments": `{"city":"Chicago"}`}}}, message["tool_calls"])
}

func TestToolCallDeltaChunkIndexesEachCall(t *testing.T) {
	chunk := roundTrip(t, toolCallDeltaChunk("id-1", "agent-orchestrator", append(append([]callertools.PendingCall{}, shapeCalls...), callertools.PendingCall{ID: "call_2", Name: "f", Arguments: "{}"})))
	require.Equal(t, "chat.completion.chunk", chunk["object"])
	choice := chunk["choices"].([]any)[0].(map[string]any)
	require.Contains(t, choice, "finish_reason")
	require.Nil(t, choice["finish_reason"])
	var pairs [][2]any
	for _, c := range choice["delta"].(map[string]any)["tool_calls"].([]any) {
		call := c.(map[string]any)
		pairs = append(pairs, [2]any{call["index"], call["id"]})
	}
	require.Equal(t, [][2]any{{float64(0), "call_1"}, {float64(1), "call_2"}}, pairs)
}

// ── OpenAITaskCompleter ────────────────────────────────────────────────────

func TestOpenAITaskCompleter(t *testing.T) {
	var got map[string]any
	var auth string
	reply := `{"choices":[{"message":{"content":"A title"}}]}`
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/chat/completions", r.URL.Path)
		auth = r.Header.Get("authorization")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &got)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, reply)
	}))
	t.Cleanup(srv.Close)
	c := &OpenAITaskCompleter{BaseURL: srv.URL + "/v1", APIKey: "sk"}

	out, err := c.Complete(context.Background(), msgs(t, `[{"role":"user","content":"### Task: title"}]`), "gpt-x")
	require.NoError(t, err)
	require.Equal(t, "A title", out)
	require.Equal(t, "Bearer sk", auth)
	require.Equal(t, map[string]any{"model": "gpt-x", "temperature": float64(0), "messages": []any{map[string]any{"role": "user", "content": "### Task: title"}}}, got)

	reply = `{"choices":[]}`
	out, err = c.Complete(context.Background(), nil, "m")
	require.NoError(t, err)
	require.Empty(t, out)

	status, reply = http.StatusTooManyRequests, `{"error":"slow down"}`
	_, err = c.Complete(context.Background(), nil, "m")
	require.ErrorContains(t, err, "429")
}
