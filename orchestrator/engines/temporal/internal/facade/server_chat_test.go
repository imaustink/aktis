package facade

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
)

// Ported from server.test.ts: "OpenAI-compatible chat completions (ADR
// 0007)", "identity-link flow selection" and "consumer-supplied tools".

func userMessages(content any) map[string]any {
	return map[string]any{"messages": []any{map[string]any{"role": "user", "content": content}}}
}

func with(body map[string]any, extra map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range body {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func TestListModels(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine()})
	res := do(t, http.MethodGet, base+"/v1/models", nil, nil)
	require.Equal(t, http.StatusOK, res.StatusCode)
	body := decode[struct {
		Object string `json:"object"`
		Data   []struct {
			ID     string `json:"id"`
			Object string `json:"object"`
		} `json:"data"`
	}](t, res)
	require.Equal(t, "list", body.Object)
	require.Len(t, body.Data, 1)
	require.Equal(t, "agent-orchestrator", body.Data[0].ID)
}

func TestChatBlockingReturnsAChatCompletionWithTheResult(t *testing.T) {
	engine := engineReturning(resultOutcome("# Pancakes"))
	base := serve(t, &Server{Engine: engine})
	res := postChat(t, base, with(userMessages("scrape https://example.com"), map[string]any{"model": "agent-orchestrator"}), nil)

	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, []TurnInput{{Request: "scrape https://example.com"}}, engine.invoked())
	body := decode[map[string]any](t, res)
	require.Equal(t, "chat.completion", body["object"])
	require.Regexp(t, `^chatcmpl-`, body["id"])
	choice := body["choices"].([]any)[0].(map[string]any)
	require.Equal(t, "stop", choice["finish_reason"])
	require.Equal(t, map[string]any{"role": "assistant", "content": "# Pancakes"}, choice["message"])
	require.Equal(t, map[string]any{"prompt_tokens": float64(0), "completion_tokens": float64(0), "total_tokens": float64(0)}, body["usage"])
}

func TestChatForwardsTheOpenWebUIUserJWTToTheEngine(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine})
	postChat(t, base, userMessages("scrape https://example.com"), map[string]string{"x-openwebui-user-jwt": "alices-signed-jwt"})
	require.Equal(t, []TurnInput{{Request: "scrape https://example.com", ForwardedUserToken: "alices-signed-jwt"}}, engine.invoked())
}

func TestChatOmitsTheForwardedUserTokenWithoutTheHeader(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine})
	postChat(t, base, userMessages("scrape https://example.com"), nil)
	require.Equal(t, []TurnInput{{Request: "scrape https://example.com"}}, engine.invoked())
}

func TestChatStreamingForwardsTheOpenWebUIUserJWT(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine})
	res := postChat(t, base, with(userMessages("scrape https://example.com"), map[string]any{"stream": true}), map[string]string{"x-openwebui-user-jwt": "alices-signed-jwt"})
	readSSE(t, res)
	require.Equal(t, "alices-signed-jwt", engine.invoked()[0].ForwardedUserToken)
}

// Regression: an image attachment used to be refused as "no user message".
func TestChatForwardsAnImageAttachmentInBothModes(t *testing.T) {
	engine := engineReturning(resultOutcome("ok"))
	base := serve(t, &Server{Engine: engine})
	dataURL := "data:image/png;base64,iVBORw0KGgo="
	content := []any{
		map[string]any{"type": "text", "text": "Extract this recipe: https://www.tiktok.com/t/ZP8TbWREM/"},
		map[string]any{"type": "image_url", "image_url": map[string]any{"url": dataURL}},
	}
	require.Equal(t, http.StatusOK, postChat(t, base, userMessages(content), nil).StatusCode)
	streaming := postChat(t, base, with(userMessages(content), map[string]any{"stream": true}), nil)
	require.Equal(t, http.StatusOK, streaming.StatusCode)
	readSSE(t, streaming)

	calls := engine.invoked()
	require.Len(t, calls, 2)
	for _, in := range calls {
		require.Equal(t, "Extract this recipe: https://www.tiktok.com/t/ZP8TbWREM/", in.Request)
		require.Equal(t, []ChatImage{{URL: dataURL}}, in.Images)
	}
}

func TestChatFoldsPriorTurnsIntoTheRequest(t *testing.T) {
	engine := engineReturning(resultOutcome("Published!"))
	base := serve(t, &Server{Engine: engine})
	postChat(t, base, map[string]any{"messages": []any{
		map[string]any{"role": "user", "content": "Extract this recipe: https://example.com/recipe"},
		map[string]any{"role": "assistant", "content": "# Pancakes\n\n## Ingredients\n- Flour"},
		map[string]any{"role": "user", "content": "publish it!"},
	}}, nil)
	require.Equal(t, []TurnInput{{Request: "<conversation_history>\n<message role=\"user\">\nExtract this recipe: https://example.com/recipe\n</message>\n" +
		"<message role=\"assistant\">\n# Pancakes\n\n## Ingredients\n- Flour\n</message>\n</conversation_history>\n\npublish it!"}}, engine.invoked())
}

// Regression: the fold used to keep only the latest assistant message.
func TestChatKeepsARecipeTheUserPastedVisibleOnTheNextTurn(t *testing.T) {
	engine := engineReturning(resultOutcome("Published!"))
	base := serve(t, &Server{Engine: engine})
	postChat(t, base, map[string]any{"messages": []any{
		map[string]any{"role": "user", "content": "# Kentucky Butter Cake\n\n## Ingredients\n- Butter"},
		map[string]any{"role": "assistant", "content": "What changes would you like to make?"},
		map[string]any{"role": "user", "content": "Let's publish it"},
	}}, nil)
	request := engine.invoked()[0].Request
	require.Contains(t, request, "# Kentucky Butter Cake")
	require.Contains(t, request, `<message role="user">`)
	require.Contains(t, request, "What changes would you like to make?")
	require.True(t, strings.HasSuffix(request, "Let's publish it"))
}

func TestChatBlockingMapsTurnErrorsToOpenAIShapedHTTPErrors(t *testing.T) {
	cases := []struct {
		message string
		status  int
		code    string
	}{
		{"unauthorized: could not resolve caller identity", http.StatusUnauthorized, "unauthorized"},
		{"no matching skill for this request", http.StatusUnprocessableEntity, "no_skill_available"},
		{"skill has no usable tools", http.StatusUnprocessableEntity, "no_tool_available"},
		{"planner selected a tool outside the skill", http.StatusInternalServerError, "internal_error"},
		{"tool failed: 500", http.StatusBadGateway, "tool_failed"},
		{"something else", http.StatusInternalServerError, "internal_error"},
	}
	for _, tc := range cases {
		t.Run(tc.code+"/"+tc.message, func(t *testing.T) {
			base := serve(t, &Server{Engine: engineReturning(TurnOutcome{Error: tc.message})})
			res := do(t, http.MethodPost, base+"/v1/chat/completions", userMessages("do a thing"), nil)
			require.Equal(t, tc.status, res.StatusCode)
			require.Equal(t, map[string]any{"error": map[string]any{"message": tc.message, "type": "invalid_request_error", "code": tc.code}}, decode[map[string]any](t, res))
		})
	}
	t.Run("a failed engine call", func(t *testing.T) {
		engine := &fakeEngine{invokeFn: func(context.Context, TurnInput) (TurnOutcome, error) {
			return TurnOutcome{}, errors.New("temporal engine /invoke failed: 503")
		}}
		base := serve(t, &Server{Engine: engine})
		res := postChat(t, base, userMessages("do a thing"), nil)
		require.Equal(t, http.StatusInternalServerError, res.StatusCode)
		require.Equal(t, "internal_error", decode[map[string]any](t, res)["error"].(map[string]any)["code"])
	})
}

func TestChatRejectsARequestWithNoUserMessage(t *testing.T) {
	for name, body := range map[string]any{
		"system only": map[string]any{"messages": []any{map[string]any{"role": "system", "content": "be nice"}}},
		"no messages": map[string]any{},
		"blank user":  userMessages("   "),
	} {
		t.Run(name, func(t *testing.T) {
			engine := newFakeEngine()
			base := serve(t, &Server{Engine: engine})
			res := postChat(t, base, body, nil)
			require.Equal(t, http.StatusBadRequest, res.StatusCode)
			require.Equal(t, "invalid_request", decode[map[string]any](t, res)["error"].(map[string]any)["code"])
			require.Empty(t, engine.invoked())
		})
	}
	t.Run("invalid JSON", func(t *testing.T) {
		engine := newFakeEngine()
		base := serve(t, &Server{Engine: engine})
		res := postChat(t, base, "{nope", nil)
		require.Equal(t, http.StatusBadRequest, res.StatusCode)
		require.Equal(t, "body must be valid JSON", decode[map[string]any](t, res)["error"].(map[string]any)["message"])
	})
}

// ── streaming ──────────────────────────────────────────────────────────────

func streamChat(t *testing.T, engine Engine, content string) ([]sseChunk, *http.Response) {
	t.Helper()
	base := serve(t, &Server{Engine: engine})
	res := postChat(t, base, with(userMessages(content), map[string]any{"stream": true}), nil)
	chunks, _ := readSSE(t, res)
	return chunks, res
}

func progressEngine(result string, steps ...[2]string) *fakeEngine {
	return &fakeEngine{invokeFn: func(_ context.Context, in TurnInput) (TurnOutcome, error) {
		for _, s := range steps {
			in.ProgressListener(s[0], s[1])
		}
		return resultOutcome(result), nil
	}}
}

func TestChatStreamingRelaysProgressAsStatusStepsThenTheResult(t *testing.T) {
	engine := progressEngine("Argentina won the 2022 World Cup.", [2]string{"agent", "running Bash"})
	base := serve(t, &Server{Engine: engine})
	res := postChat(t, base, with(userMessages("who won the world cup?"), map[string]any{"stream": true}), nil)

	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Contains(t, res.Header.Get("content-type"), "text/event-stream")
	chunks, raw := readSSE(t, res)
	require.True(t, strings.HasSuffix(raw, "data: [DONE]\n\n"))

	in := engine.invoked()[0]
	require.Equal(t, "who won the world cup?", in.Request)
	require.NotNil(t, in.ProgressListener)

	require.Contains(t, statusesOf(chunks), "agent: running Bash")
	require.Contains(t, contentOf(chunks), "Argentina won the 2022 World Cup.")
	require.NotContains(t, contentOf(chunks), "agent stream ended unexpectedly")
	last := withChoices(chunks)
	require.Equal(t, "stop", *last[len(last)-1].Choices[0].FinishReason)
	// The step left open by the last progress line is closed before the end.
	closed := false
	for _, c := range chunks {
		if c.Event != nil && c.Event.Data.Description == "agent: running Bash" && c.Event.Data.Done {
			closed = true
		}
	}
	require.True(t, closed)
}

func TestChatStreamingReportsATurnThatEndedWithNothing(t *testing.T) {
	chunks, _ := streamChat(t, engineReturning(TurnOutcome{}), "hello")
	require.Contains(t, contentOf(chunks), "agent stream ended unexpectedly")
	require.NotContains(t, contentOf(chunks), "undefined")
	require.NotContains(t, contentOf(chunks), "<nil>")
}

func TestChatStreamingSurfacesATurnErrorAsTheFinalMessage(t *testing.T) {
	chunks, res := streamChat(t, engineReturning(TurnOutcome{Error: "unauthorized: could not resolve caller identity"}), "do a thing")
	require.Equal(t, http.StatusOK, res.StatusCode) // headers already sent
	require.Contains(t, contentOf(chunks), "❌ unauthorized")
}

func TestChatStreamingSurfacesAFailedEngineCallAsTheFinalMessage(t *testing.T) {
	engine := &fakeEngine{invokeFn: func(context.Context, TurnInput) (TurnOutcome, error) {
		return TurnOutcome{}, errors.New("temporal engine /invoke failed: 503")
	}}
	chunks, _ := streamChat(t, engine, "do a thing")
	require.Contains(t, contentOf(chunks), "❌ temporal engine /invoke failed: 503")
}

func TestChatStreamingSurfacesAPanickingEngineAsTheFinalMessage(t *testing.T) {
	engine := &fakeEngine{invokeFn: func(context.Context, TurnInput) (TurnOutcome, error) { panic("kaboom") }}
	chunks, _ := streamChat(t, engine, "do a thing")
	require.Contains(t, contentOf(chunks), "❌ kaboom")
}

func TestChatStreamingRendersAnIdentityLinkPromptAsContent(t *testing.T) {
	prompt := "Link your GitHub account to continue: https://gateway.example.com/identity-link/start?state=abc"
	chunks, _ := streamChat(t, progressEngine("Done.", [2]string{"identity-link", prompt}), "open a PR")
	require.Contains(t, contentOf(chunks), prompt)
	for _, s := range statusesOf(chunks) {
		require.NotContains(t, s, "identity-link")
	}
}

func TestChatStreamingRendersARemoteControlURLAsContent(t *testing.T) {
	rcURL := "https://claude.ai/code/session_abc123"
	chunks, _ := streamChat(t, progressEngine("Opened PR #42.", [2]string{"remote-control-url", rcURL}), "fix the bug")
	all := contentOf(chunks)
	require.Contains(t, all, rcURL)
	require.Contains(t, all, "Watch live or take over")
	require.Contains(t, all, "Opened PR #42.")
	for _, s := range statusesOf(chunks) {
		require.NotContains(t, s, "claude.ai/code")
	}
}

func TestChatStreamingSeparatesConsecutiveAgentTextBlocks(t *testing.T) {
	chunks, _ := streamChat(t, progressEngine("Opened PR #42.",
		[2]string{"agent-text", "I'll start by checking whether the repo exists."},
		[2]string{"agent-text", "Found it — bitovi/ai-experiments is accessible."},
	), "fix the bug")
	all := contentOf(chunks)
	require.NotContains(t, all, "repo exists.Found it")
	require.Regexp(t, regexp.MustCompile(`repo exists\.\s+Found it`), all)
	require.NotContains(t, all, "accessible.Opened")
}

// A block that already ends (or the next one starts) with whitespace needs
// no inserted break.
func TestChatStreamingAddsNoSeparatorWhereWhitespaceAlreadyIs(t *testing.T) {
	chunks, _ := streamChat(t, progressEngine("Done.",
		[2]string{"agent-text", "First line.\n"},
		[2]string{"agent-text", "Second."},
	), "go")
	require.Equal(t, "First line.\nSecond.\n\nDone.", contentOf(chunks))
}

func TestChatStreamingClosesAnOpenStatusStepWhenContentStarts(t *testing.T) {
	chunks, _ := streamChat(t, progressEngine("Opened PR #42.",
		[2]string{"agent", "running Bash"},
		[2]string{"agent-text", "Empty directory, so I need to clone the repo fresh."},
	), "fix the bug")

	index := func(match func(sseChunk) bool) int {
		for i, c := range chunks {
			if match(c) {
				return i
			}
		}
		return -1
	}
	isBash := func(done bool) func(sseChunk) bool {
		return func(c sseChunk) bool {
			return c.Event != nil && c.Event.Data.Description == "agent: running Bash" && c.Event.Data.Done == done
		}
	}
	opened := index(isBash(false))
	closed := index(isBash(true))
	content := index(func(c sseChunk) bool {
		return len(c.Choices) > 0 && c.Choices[0].Delta.Content != nil && strings.Contains(*c.Choices[0].Delta.Content, "Empty directory")
	})
	require.GreaterOrEqual(t, opened, 0)
	require.Greater(t, closed, opened)
	require.Less(t, closed, content)
	closes := 0
	for _, c := range chunks {
		if isBash(true)(c) {
			closes++
		}
	}
	require.Equal(t, 1, closes, "closed exactly once, not again at the end")
}

func TestChatStreamingClampsStatusLabelsAndFallsBack(t *testing.T) {
	long := strings.Repeat("é", 130)
	chunks, _ := streamChat(t, progressEngine("ok",
		[2]string{"clone", long},
		[2]string{"", "bare message"},
		[2]string{"stage-only", ""},
		[2]string{"", ""},
		// An empty agent-text message is bookkeeping, not content.
		[2]string{"agent-text", ""},
	), "go")
	var opened []string
	for _, c := range chunks {
		if c.Event != nil && !c.Event.Data.Done {
			opened = append(opened, c.Event.Data.Description)
		}
	}
	require.Equal(t, []string{"clone: " + strings.Repeat("é", 120), "bare message", "stage-only", "working…", "agent-text"}, opened)
}

// The SSE connection gets a comment heartbeat while a turn is silent.
func TestChatStreamingSendsHeartbeatsWhileTheTurnIsSilent(t *testing.T) {
	prev := heartbeatInterval
	heartbeatInterval = 5 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = prev })

	engine := &fakeEngine{invokeFn: func(ctx context.Context, _ TurnInput) (TurnOutcome, error) {
		time.Sleep(60 * time.Millisecond)
		return resultOutcome("ok"), nil
	}}
	base := serve(t, &Server{Engine: engine})
	res := postChat(t, base, with(userMessages("slow"), map[string]any{"stream": true}), nil)
	chunks, raw := readSSE(t, res)
	require.Contains(t, raw, ": keep-alive\n\n")
	require.Equal(t, "ok", contentOf(chunks))
	// Heartbeats are comments, never data a client would parse.
	require.Less(t, strings.Index(raw, ": keep-alive"), strings.Index(raw, `"content":"ok"`))
}

func TestChatCarriesTheChatIDHeaderAsTheSessionID(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Sessions: memSessions()})
	postChat(t, base, userMessages("extract https://example.com"), map[string]string{"x-openwebui-chat-id": "chat-42"})
	postChat(t, base, userMessages("extract https://example.com"), nil)
	require.Equal(t, []TurnInput{
		{Request: "extract https://example.com", SessionID: "chat-42"},
		// No header: nothing session-scoped at all.
		{Request: "extract https://example.com"},
	}, engine.invoked())
}

// ── identity-link flow selection ───────────────────────────────────────────

func TestIdentityLinkFlowExplicitOnInvoke(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine})
	postInvoke(t, base, map[string]any{"request": "open a PR", "identity_link_flow": "device"}, nil)
	requireInvokeTurn(t, TurnInput{Request: "open a PR", IdentityLinkFlow: "device"}, engine.waitInvokes(t, 1)[0])
}

func TestIdentityLinkFlowDefaultAppliesToAChatTurnThatNamesNone(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, DefaultIdentityLinkFlow: "device"})
	readSSE(t, postChat(t, base, with(userMessages("open a PR"), map[string]any{"stream": true}), nil))
	require.Equal(t, "device", engine.invoked()[0].IdentityLinkFlow)
}

func TestIdentityLinkFlowExplicitOnChatStreamingOrNot(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine})
	for _, stream := range []bool{true, false} {
		res := postChat(t, base, with(userMessages("open a PR"), map[string]any{"stream": stream, "identity_link_flow": "device"}), nil)
		require.Equal(t, http.StatusOK, res.StatusCode)
		readSSE(t, res)
	}
	calls := engine.invoked()
	require.Equal(t, "device", calls[0].IdentityLinkFlow)
	require.Equal(t, "device", calls[1].IdentityLinkFlow)
}

func TestIdentityLinkFlowChatOverrideAndInvalidFallsBackToDefault(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, DefaultIdentityLinkFlow: "device"})
	for _, flow := range []string{"authcode", "carrier-pigeon"} {
		readSSE(t, postChat(t, base, with(userMessages("open a PR"), map[string]any{"stream": true, "identity_link_flow": flow}), nil))
	}
	calls := engine.invoked()
	require.Equal(t, "authcode", calls[0].IdentityLinkFlow)
	require.Equal(t, "device", calls[1].IdentityLinkFlow)
}

func TestIdentityLinkFlowInvokeOverridesTheDefault(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, DefaultIdentityLinkFlow: "device"})
	postInvoke(t, base, map[string]any{"request": "open a PR", "identity_link_flow": "authcode"}, nil)
	require.Equal(t, "authcode", engine.waitInvokes(t, 1)[0].IdentityLinkFlow)
}

// An "authcode" default is the engine's own default, so it is not sent.
func TestIdentityLinkFlowAuthcodeDefaultIsNotForwarded(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, DefaultIdentityLinkFlow: "authcode"})
	postChat(t, base, userMessages("open a PR"), nil)
	require.Empty(t, engine.invoked()[0].IdentityLinkFlow)
}

func TestIdentityLinkFlowMissingOrInvalidOnInvokeIsIgnored(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine})
	require.Equal(t, http.StatusAccepted, postInvoke(t, base, map[string]any{"request": "open a PR"}, nil).StatusCode)
	requireInvokeTurn(t, TurnInput{Request: "open a PR"}, engine.waitInvokes(t, 1)[0])
	require.Equal(t, http.StatusAccepted, postInvoke(t, base, map[string]any{"request": "open a PR", "identity_link_flow": "carrier-pigeon"}, nil).StatusCode)
	requireInvokeTurn(t, TurnInput{Request: "open a PR"}, engine.waitInvokes(t, 2)[1])
}

// ── consumer-supplied tools (docs/adr/0035) ────────────────────────────────

func toolDef(name, description string) map[string]any {
	return map[string]any{"type": "function", "function": map[string]any{
		"name": name, "description": description,
		"parameters": map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}},
	}}
}

func TestCallerToolsAreResolvedOntoTheTurn(t *testing.T) {
	engine := engineReturning(resultOutcome("ok"))
	base := serve(t, &Server{Engine: engine})
	postChat(t, base, with(userMessages("what's the weather?"), map[string]any{"tools": []any{toolDef("get_weather", "Look up the weather")}}), nil)
	tools := engine.invoked()[0].CallerTools
	require.Len(t, tools, 1)
	require.Equal(t, "get_weather", tools[0].Name)
	require.Equal(t, "Look up the weather", tools[0].Description)
	require.NotEmpty(t, tools[0].Hash)
	require.False(t, engine.invoked()[0].CallerToolRequired)
}

func TestCallerToolChoiceRequiredRidesAsADirective(t *testing.T) {
	engine := engineReturning(resultOutcome("ok"))
	base := serve(t, &Server{Engine: engine})
	postChat(t, base, with(userMessages("weather?"), map[string]any{"tools": []any{toolDef("get_weather", "x")}, "tool_choice": "required"}), nil)
	require.True(t, engine.invoked()[0].CallerToolRequired)

	postChat(t, base, with(userMessages("weather?"), map[string]any{"tools": []any{toolDef("get_weather", "x")}, "tool_choice": "none"}), nil)
	require.Empty(t, engine.invoked()[1].CallerTools)
}

func TestCallerToolsLeaveTheTurnUntouchedWhenNoneAreSent(t *testing.T) {
	engine := engineReturning(resultOutcome("ok"))
	base := serve(t, &Server{Engine: engine})
	postChat(t, base, userMessages("hi"), nil)
	require.Equal(t, []TurnInput{{Request: "hi"}}, engine.invoked())
}

func TestCallerToolsMalformedArrayIs400(t *testing.T) {
	cases := map[string]struct {
		extra map[string]any
		want  string
	}{
		"bad name":            {map[string]any{"tools": []any{map[string]any{"type": "function", "function": map[string]any{"name": "bad name!"}}}}, "must match"},
		"not an array":        {map[string]any{"tools": "nope"}, "tools must be an array"},
		"entry not an object": {map[string]any{"tools": []any{1}}, "tools[0] must be an object"},
		"wrong type":          {map[string]any{"tools": []any{map[string]any{"type": "x", "function": map[string]any{"name": "a"}}}}, `tools[0].type must be "function"`},
		"no function":         {map[string]any{"tools": []any{map[string]any{"type": "function"}}}, "tools[0].function must be an object"},
		"non-string name":     {map[string]any{"tools": []any{map[string]any{"function": map[string]any{"name": 3}}}}, "tools[0].function.name must be a non-empty string"},
		"bad description":     {map[string]any{"tools": []any{map[string]any{"function": map[string]any{"name": "a", "description": 3}}}}, "tools[0].function.description must be a string"},
		"bad parameters":      {map[string]any{"tools": []any{map[string]any{"function": map[string]any{"name": "a", "parameters": "x"}}}}, "tools[0].function.parameters must be a JSON Schema object"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			engine := newFakeEngine()
			base := serve(t, &Server{Engine: engine})
			res := postChat(t, base, with(userMessages("hi"), tc.extra), nil)
			require.Equal(t, http.StatusBadRequest, res.StatusCode)
			require.Contains(t, decode[map[string]any](t, res)["error"].(map[string]any)["message"], tc.want)
			require.Empty(t, engine.invoked())
		})
	}
	t.Run("on /invoke too", func(t *testing.T) {
		engine := newFakeEngine()
		base := serve(t, &Server{Engine: engine})
		res := postInvoke(t, base, map[string]any{"request": "hi", "tools": "nope"}, nil)
		require.Equal(t, http.StatusBadRequest, res.StatusCode)
		require.Equal(t, map[string]any{"error": "tools must be an array"}, decode[map[string]any](t, res))
		stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
	})
}

var weatherCall = []callertools.PendingCall{{ID: "call_abc", Name: "get_weather", Arguments: `{"city":"Chicago"}`}}

func TestCallerToolCallsBlockingReturnToolCallsWithFinishReasonToolCalls(t *testing.T) {
	base := serve(t, &Server{Engine: engineReturning(TurnOutcome{PendingToolCalls: weatherCall})})
	res := postChat(t, base, with(userMessages("weather?"), map[string]any{"tools": []any{toolDef("get_weather", "x")}}), nil)
	require.Equal(t, http.StatusOK, res.StatusCode)
	choice := decode[map[string]any](t, res)["choices"].([]any)[0].(map[string]any)
	require.Equal(t, "tool_calls", choice["finish_reason"])
	message := choice["message"].(map[string]any)
	require.Contains(t, message, "content")
	require.Nil(t, message["content"])
	require.Equal(t, []any{map[string]any{"id": "call_abc", "type": "function", "function": map[string]any{"name": "get_weather", "arguments": `{"city":"Chicago"}`}}}, message["tool_calls"])
}

func TestCallerToolCallsStreamingEmitADeltaThenAToolCallsFinish(t *testing.T) {
	base := serve(t, &Server{Engine: engineReturning(TurnOutcome{PendingToolCalls: weatherCall})})
	res := postChat(t, base, with(userMessages("weather?"), map[string]any{"stream": true, "tools": []any{toolDef("get_weather", "x")}}), nil)
	chunks, _ := readSSE(t, res)
	choices := withChoices(chunks)
	require.Equal(t, []map[string]any{{"index": float64(0), "id": "call_abc", "type": "function", "function": map[string]any{"name": "get_weather", "arguments": `{"city":"Chicago"}`}}}, choices[0].Choices[0].Delta.ToolCalls)
	require.Equal(t, "tool_calls", *choices[len(choices)-1].Choices[0].FinishReason)
}

func TestCallerToolResultIsPassedBackAsAPriorCall(t *testing.T) {
	engine := engineReturning(resultOutcome("It's 58F and raining."))
	base := serve(t, &Server{Engine: engine})
	postChat(t, base, map[string]any{
		"messages": []any{
			map[string]any{"role": "user", "content": "weather in Chicago?"},
			map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "call_abc", "type": "function", "function": map[string]any{"name": "get_weather", "arguments": `{"city":"Chicago"}`}}}},
			map[string]any{"role": "tool", "tool_call_id": "call_abc", "content": "58F and raining"},
		},
		"tools": []any{toolDef("get_weather", "x")},
	}, nil)
	in := engine.invoked()[0]
	require.Equal(t, "weather in Chicago?", in.Request)
	require.Equal(t, []callertools.PriorCall{{ID: "call_abc", Name: "get_weather", Arguments: `{"city":"Chicago"}`, Result: "58F and raining"}}, in.PriorCallerToolCalls)
}

type stubTaskCompleter struct {
	mu    sync.Mutex
	calls int
	reply string
	err   error
}

func (s *stubTaskCompleter) Complete(context.Context, any, string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.reply, s.err
}

func (s *stubTaskCompleter) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

func TestHousekeepingRequestsNeverReachTheEngineOrEmitToolCalls(t *testing.T) {
	engine := newFakeEngine()
	completer := &stubTaskCompleter{reply: "Chicago Weather"}
	base := serve(t, &Server{Engine: engine, TaskCompleter: completer})
	res := postChat(t, base, with(userMessages("### Task:\nGenerate a concise title"), map[string]any{"tools": []any{toolDef("get_weather", "x")}}), nil)

	require.Empty(t, engine.invoked())
	choice := decode[map[string]any](t, res)["choices"].([]any)[0].(map[string]any)
	require.Equal(t, "stop", choice["finish_reason"])
	require.Equal(t, "Chicago Weather", choice["message"].(map[string]any)["content"])
	require.Equal(t, 1, completer.count())
}

func TestHousekeepingRequestsStreamAndDegradeToEmpty(t *testing.T) {
	completer := &stubTaskCompleter{reply: "A title"}
	base := serve(t, &Server{Engine: newFakeEngine(), TaskCompleter: completer})
	chunks, raw := readSSE(t, postChat(t, base, with(userMessages("### Task:\ntitle"), map[string]any{"stream": true}), nil))
	require.Equal(t, "A title", contentOf(chunks))
	require.True(t, strings.HasSuffix(raw, "data: [DONE]\n\n"))

	failing := serve(t, &Server{Engine: newFakeEngine(), TaskCompleter: &stubTaskCompleter{err: errors.New("openai down")}})
	choice := decode[map[string]any](t, postChat(t, failing, userMessages("### Task:\ntitle"), nil))["choices"].([]any)[0].(map[string]any)
	require.Equal(t, "", choice["message"].(map[string]any)["content"])
}

type fakeToolStore struct {
	mu       sync.Mutex
	indexed  int
	searches []struct {
		text string
		n, k int
	}
}

func (s *fakeToolStore) Index(context.Context, []callertools.Descriptor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexed++
	return nil
}

func (s *fakeToolStore) Search(_ context.Context, text string, tools []callertools.Descriptor, k int) ([]callertools.Descriptor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.searches = append(s.searches, struct {
		text string
		n, k int
	}{text, len(tools), k})
	return tools[:k], nil
}

func (s *fakeToolStore) Prune(context.Context, int64) (int, error) { return 0, nil }

func TestCallerToolStoreIsConsultedOnlyAboveTopK(t *testing.T) {
	store := &fakeToolStore{}
	engine := engineReturning(resultOutcome("ok"))
	base := serve(t, &Server{Engine: engine, CallerTools: store, CallerToolTopK: 2})
	post := func(names ...string) {
		var tools []any
		for _, n := range names {
			tools = append(tools, toolDef(n, "does a thing"))
		}
		postChat(t, base, with(userMessages("hi"), map[string]any{"tools": tools}), nil)
	}

	post("a", "b")
	require.Zero(t, store.indexed)

	post("a", "b", "c")
	require.Equal(t, 1, store.indexed)
	require.Len(t, store.searches, 1)
	require.Equal(t, "hi", store.searches[0].text)
	require.Equal(t, 3, store.searches[0].n)
	require.Equal(t, 2, store.searches[0].k)
	calls := engine.invoked()
	require.Len(t, calls[len(calls)-1].CallerTools, 2)
}

func TestPendingToolCallsSurfaceOnTheInvokePoll(t *testing.T) {
	base := serve(t, &Server{Engine: engineReturning(TurnOutcome{PendingToolCalls: []callertools.PendingCall{{ID: "call_abc", Name: "get_weather", Arguments: "{}"}}})})
	id := acceptedID(t, postInvoke(t, base, map[string]any{"request": "weather?", "tools": []any{toolDef("get_weather", "x")}}, nil))
	record := settled(t, base, id)
	require.Equal(t, "succeeded", record["status"])
	require.NotContains(t, record, "result")
	require.Equal(t, []any{map[string]any{"id": "call_abc", "name": "get_weather", "arguments": "{}"}}, record["pendingToolCalls"])
}
