package facade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
	"github.com/aktisai/orchestrator/engines/temporal/internal/rbac"
)

// heartbeatInterval keeps an idle SSE connection from being cut upstream
// while a turn runs with no progress line. A var only so tests can shorten it.
var heartbeatInterval = 15 * time.Second

const (
	// chatIDHeader is Open WebUI's conversation id (ENABLE_FORWARD_USER_INFO_HEADERS).
	chatIDHeader = "X-OpenWebUI-Chat-Id"
	// forwardedUserJWTHeader is Open WebUI's per-request signed user JWT.
	forwardedUserJWTHeader = "X-OpenWebUI-User-Jwt"

	// maxBodyBytes bounds any request body. Chat bodies carry inline images.
	maxBodyBytes = 64 << 20
)

// TaskCompleter answers Open WebUI's housekeeping completions directly.
type TaskCompleter interface {
	Complete(ctx context.Context, messages any, model string) (string, error)
}

// TriggerSelector is the live EventTrigger table.
type TriggerSelector interface {
	Match(e catalog.Envelope, callerSessionID string) (*catalog.TriggerMatch, error)
	CouldMatchBeforeSender(e catalog.Envelope) bool
}

// Server is agent-orchestrator's HTTP surface.
type Server struct {
	Engine Engine
	// Sessions holds the sticky read-only flag. Nil: no turn inherits it.
	Sessions SessionStore
	// TaskCompleter answers housekeeping requests. Nil: they get "".
	TaskCompleter TaskCompleter
	// Triggers is the EventTrigger table. Nil: every event is ignored.
	Triggers *catalog.EventTriggerRegistry
	// SenderAssertionSecret: when set, a webhook turn's sender login is
	// accepted ONLY from a verified assertion.
	SenderAssertionSecret string
	// CallerTools ranks a large caller tool array. Nil: truncation.
	CallerTools    callertools.Store
	CallerToolTopK int
	// DefaultIdentityLinkFlow applies to any turn that names none.
	DefaultIdentityLinkFlow string
	Invocations             InvocationStore
	// CallerAuth gates every turn-starting route. Nil: no check (tests).
	CallerAuth *CallerAuthenticator

	draining sync.Map // "draining" -> true
	driving  sync.Map // invocation id -> struct{}
	// background is the context the fire-and-forget turns run under: a
	// turn outlives the request that started it.
	background context.Context

	httpServer *http.Server
}

func (s *Server) isDraining() bool { _, ok := s.draining.Load("draining"); return ok }

// BeginDraining stops accepting new turns. Called at the top of shutdown,
// before the drain: in-flight work finishes, but a caller asking for new work
// is told to retry (503) and reaches the replacement pod instead of being
// handed a turn whose poll dies with this process.
func (s *Server) BeginDraining() { s.draining.Store("draining", true) }

func (s *Server) bg() context.Context {
	if s.background != nil {
		return s.background
	}
	return context.Background()
}

func (s *Server) invocations() InvocationStore {
	if s.Invocations == nil {
		s.Invocations = &MemoryInvocationStore{}
	}
	return s.Invocations
}

func (s *Server) topK() int {
	if s.CallerToolTopK > 0 {
		return s.CallerToolTopK
	}
	return 5
}

var invocationPath = regexp.MustCompile(`^/invoke/([^/]+)$`)

// ServeHTTP routes. Unknown routes 404 with an empty body.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("agent-orchestrator: panic serving %s %s: %v", r.Method, r.URL.Path, rec)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()
	path := r.URL.Path
	switch {
	case r.Method == http.MethodPost && path == "/invoke":
		s.handleInvoke(w, r)
	case r.Method == http.MethodGet && invocationPath.MatchString(path):
		s.handleGetInvocation(w, r, invocationPath.FindStringSubmatch(path)[1])
	case r.Method == http.MethodGet && path == "/v1/models":
		writeJSON(w, http.StatusOK, listModelsResponse())
	case r.Method == http.MethodPost && path == "/v1/chat/completions":
		s.handleChatCompletions(w, r)
	case r.Method == http.MethodGet && path == "/sessions/live":
		s.handleSessionLive(w, r)
	case r.Method == http.MethodPost && path == "/events/prefilter":
		s.handlePrefilter(w, r)
	case r.Method == http.MethodPost && path == "/events/compile":
		s.handleCompile(w, r)
	case r.Method == http.MethodGet && path == "/healthz":
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	payload, _ := json.Marshal(body)
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func bearerToken(header string) string {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "bearer") || token == "" {
		return ""
	}
	// The TS split(" ") took exactly the second space-separated field.
	token, _, _ = strings.Cut(token, " ")
	return token
}

func headerValue(r *http.Request, name string) string {
	values := r.Header.Values(name)
	if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
		return ""
	}
	return values[0]
}

func readBody(r *http.Request) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
}

// admitCaller is the authentication gate in front of every turn-starting
// route. On a refusal it has already written the 401, before anything about
// the turn runs. Logs the reason, never the credential.
func (s *Server) admitCaller(w http.ResponseWriter, r *http.Request, route, forwardedUserToken string) bool {
	if s.CallerAuth == nil {
		return true
	}
	result := s.CallerAuth.Authenticate(r.Context(), bearerToken(r.Header.Get("Authorization")), forwardedUserToken)
	if result.OK {
		return true
	}
	if s.CallerAuth.Mode == "warn" {
		log.Printf("agent-orchestrator: AGENT_CALLER_AUTH=warn: would reject POST %s: %s", route, result.Reason)
		return true
	}
	log.Printf("agent-orchestrator: rejected %s %s: %s", r.Method, route, result.Reason)
	message := "unauthorized: could not authenticate the caller"
	w.Header().Set("www-authenticate", `Bearer realm="agent-orchestrator"`)
	if route == "/v1/chat/completions" {
		writeJSON(w, http.StatusUnauthorized, openAIError(message, "invalid_api_key"))
	} else {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": message})
	}
	return false
}

// handleSessionLive always answers {live:false}. The live-session tunnel
// proxied into a Pod the removed in-process engine launched; the Temporal
// engine owns its runs, so there is never one here. integration-gateway's
// label reconciler must not read this as "the turn finished".
func (s *Server) handleSessionLive(w http.ResponseWriter, r *http.Request) {
	if bearerToken(r.Header.Get("Authorization")) == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if r.URL.Query().Get("sessionId") == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "sessionId query param is required"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"live": false})
}

// handlePrefilter answers integration-gateway's pre-sender question about a
// webhook delivery (docs/event-triggers.md §8): could any trigger match, and
// what do the source's sender expressions evaluate to. The gateway calls it
// with its own /invoke credential.
func (s *Server) handlePrefilter(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "could not read body"})
		return
	}
	if !s.admitCaller(w, r, "/events/prefilter", "") {
		return
	}
	var req PrefilterRequest
	if err := json.Unmarshal(body, &req); err != nil || len(req.Envelope) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": `body must be JSON: {"envelope": <envelope>, "sender"?: {...}}`})
		return
	}
	resp, err := Prefilter(s.Triggers, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleCompile reports which of a set of CEL expressions do not compile.
// integration-gateway asks before serving an EventSource whose sender block
// carries them, so a source with a broken expression 404s exactly as it did
// when the gateway compiled CEL itself (docs/event-triggers.md §8).
func (s *Server) handleCompile(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "could not read body"})
		return
	}
	if !s.admitCaller(w, r, "/events/compile", "") {
		return
	}
	var req CompileRequest
	if err := json.Unmarshal(body, &req); err != nil || req.Expressions == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": `body must be JSON: {"expressions": [<cel>, ...]}`})
		return
	}
	if len(req.Expressions) > maxCompileExpressions {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("at most %d expressions per request", maxCompileExpressions)})
		return
	}
	writeJSON(w, http.StatusOK, Compile(req))
}

// ── /invoke ────────────────────────────────────────────────────────────────

// triggerResponse is the 202's `trigger` object for an event turn
// (docs/event-triggers.md §5, docs/adr/0052 §1/§3).
type triggerResponse struct {
	Name      string                 `json:"name"`
	SessionID string                 `json:"sessionId"`
	Reply     *catalog.RenderedReply `json:"reply"`
	Params    map[string]string      `json:"params"`
	// OnComplete is present only when the trigger sets onComplete.emitTo, with
	// its waitUntilClosed / serialize holds (docs/adr/0055) passed through.
	OnComplete *catalog.EventTriggerOnComplete `json:"onComplete,omitempty"`
}

func newTriggerResponse(m *catalog.TriggerMatch) *triggerResponse {
	t := &triggerResponse{Name: m.Trigger.Name, SessionID: m.SessionID, Reply: m.Reply, Params: m.StringParams}
	if t.Params == nil {
		t.Params = map[string]string{}
	}
	if oc := m.Trigger.Spec.OnComplete; oc != nil && oc.EmitTo != "" {
		echo := *oc
		t.OnComplete = &echo
	}
	return t
}

// callerToolTurn is everything one turn knows about consumer-supplied tools.
type callerToolTurn struct {
	tools      []callertools.Descriptor
	required   bool
	priorCalls []callertools.PriorCall
}

func (s *Server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	span := StartSpan("POST /invoke", headerValue(r, TraceparentHeader), spanKindServer(), map[string]any{
		"http.request.method": "POST", "http.route": "/invoke",
	})
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	defer func() {
		span.Set(map[string]any{"http.response.status_code": rec.status})
		span.End()
	}()
	s.handleInvokeTraced(rec, r, span)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

const invalidInvokeBody = `body must be JSON: { "request": "<non-empty string>" } or { "event": <envelope> }`

func (s *Server) handleInvokeTraced(w http.ResponseWriter, r *http.Request, span *Span) {
	rawBody, err := readBody(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": invalidInvokeBody})
		return
	}
	if !s.admitCaller(w, r, "/invoke", "") {
		span.Fail("unauthorized")
		return
	}

	var parsed any = map[string]any{}
	if len(rawBody) > 0 {
		if err := json.Unmarshal(rawBody, &parsed); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": invalidInvokeBody})
			return
		}
	}
	body, ok := parsed.(map[string]any)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": invalidInvokeBody})
		return
	}

	// An event envelope (ADR 0048): `request` is optional (the trigger's
	// rendered prompt replaces it) and a malformed envelope is a 400, never a
	// conversational turn over whatever `request` says.
	var envelope *catalog.Envelope
	if rawEvent, present := body["event"]; present {
		env, err := ParseEnvelope(rawEvent)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		envelope = &env
	}
	request, isString := body["request"].(string)
	if envelope == nil && (!isString || strings.TrimSpace(request) == "") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": invalidInvokeBody})
		return
	}
	sessionID := ""
	if raw, ok := body["session_id"].(string); ok && strings.TrimSpace(raw) != "" {
		sessionID = raw
	}
	// Optional; an invalid value is silently ignored.
	identityLinkFlow := ""
	if flow, _ := body["identity_link_flow"].(string); flow == "device" || flow == "authcode" {
		identityLinkFlow = flow
	}

	// WHERE the sender login is trusted from depends on configuration
	// (docs/adr/0030 §6): with a secret, ONLY a signed assertion — the
	// envelope's sender.login is ignored, or anything holding the /invoke
	// token could name a login and be handed that person's credentials.
	var senderLogin string
	var senderClaims *rbac.SenderClaims
	if s.SenderAssertionSecret != "" {
		senderClaims = rbac.VerifySenderClaims(s.SenderAssertionSecret, headerValue(r, rbac.SenderAssertionHeader), time.Now())
		if senderClaims != nil {
			senderLogin = senderClaims.Login
		}
	} else if envelope != nil {
		if login, ok := envelope.Sender["login"].(string); ok && strings.TrimSpace(login) != "" {
			senderLogin = login
		}
	}

	// ── EventTrigger selection (docs/event-triggers.md §4/§5) ──
	var trigger *TurnInput // dispatch fields only
	var triggerResp *triggerResponse
	targetRepository := ""
	if envelope != nil {
		var match *catalog.TriggerMatch
		var matchErr error
		if s.Triggers != nil {
			match, matchErr = s.Triggers.Match(*envelope, sessionID)
		}
		switch {
		case matchErr != nil:
			span.Set(map[string]any{"aktis.event.outcome": "failed"})
			var me *catalog.MatchError
			message := matchErr.Error()
			if errors.As(matchErr, &me) {
				message = fmt.Sprintf("event trigger %s: %v", me.Trigger, me.Err)
			}
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"status": "failed", "error": message})
			return
		case match == nil:
			// A clean no-op: no turn, no record, and no retrieval fallback — a
			// CI webhook that matches nothing must not be read as free text.
			span.Set(map[string]any{"aktis.event.outcome": "ignored"})
			writeJSON(w, http.StatusOK, map[string]any{"status": "ignored"})
			return
		}
		span.Set(map[string]any{"aktis.event.outcome": "matched"})
		triggerResp = newTriggerResponse(match)
		target := match.Trigger.Spec.Target
		if target.Kind() == catalog.TargetNone {
			s.acceptNoneTurn(w, span, triggerResp)
			return
		}
		// A toolRef trigger needs no prompt, but the turn still needs a
		// request — a short descriptive one, never text from the event body.
		if match.Prompt != "" {
			request = match.Prompt
		} else {
			request = strings.TrimSpace("Run tool " + target.ToolRef)
		}
		sessionID = match.SessionID
		targetRepository = envelope.Repository
		trigger = &TurnInput{
			TriggerName:      match.Trigger.Name,
			ForcedSkillID:    target.SkillRef,
			ForcedAgentID:    target.AgentRef,
			ReadOnly:         target.ReadOnly,
			Outcome:          target.Outcome,
			Params:           match.StringParams,
			IdentityOverride: target.Identity,
		}
		if target.ToolRef != "" {
			trigger.ForcedToolID = target.ToolRef
			trigger.ToolInput = match.ToolInput
		}
	}

	// /invoke has no prior-tool-call history: a single request string has
	// nowhere to have put a result. Offering works; resuming is chat-only.
	tools, toolErr := s.resolveCallerToolTurn(r.Context(), request, body, nil)
	if toolErr != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": toolErr.Error()})
		return
	}

	if s.isDraining() {
		w.Header().Set("retry-after", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "orchestrator is shutting down; retry"})
		return
	}

	// Accept-phase identity check (docs/adr/0049 §6): a trigger turn whose
	// agent must act as a sender who cannot be acted as is refused NOW, while
	// the webhook caller is still listening.
	if trigger != nil && trigger.ForcedAgentID != "" && senderLogin != "" {
		check := *trigger
		check.Request = request
		check.SessionID = sessionID
		check.SenderLogin = senderLogin
		check.SenderClaims = senderClaims
		check.TargetRepository = targetRepository
		check.Traceparent = span.Traceparent
		if refusal := s.Engine.CheckIdentity(r.Context(), check); refusal != nil {
			code, _ := refusal.Error["code"].(string)
			span.Set(map[string]any{"aktis.identity_error": code})
			span.Fail(code)
			errBody := map[string]any{}
			for k, v := range refusal.Error {
				errBody[k] = v
			}
			if triggerResp != nil {
				errBody["trigger"] = triggerResp
			}
			resp := map[string]any{}
			for k, v := range refusal.Body {
				resp[k] = v
			}
			resp["error"] = errBody
			writeJSON(w, http.StatusForbidden, resp)
			return
		}
	}

	id := uuid.NewString()
	if err := s.invocations().Set(r.Context(), InvocationRecord{ID: id, Status: "pending", ForcedAgent: trigger != nil && trigger.ForcedAgentID != ""}); err != nil {
		log.Printf("agent-orchestrator: could not write invocation record %s: %v", id, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	s.driving.Store(id, struct{}{})

	input := s.buildTurnInput(r.Context(), turnRequest{
		request:          request,
		sessionID:        sessionID,
		identityLinkFlow: identityLinkFlow,
		senderLogin:      senderLogin,
		callerTools:      tools,
		trigger:          trigger,
	})
	input.TargetRepository = targetRepository
	input.SenderClaims = senderClaims
	input.Traceparent = span.Traceparent
	forcedAgent := trigger != nil && trigger.ForcedAgentID != ""

	go s.runInvocation(id, input, sessionID, trigger != nil, forcedAgent)

	attrs := map[string]any{"aktis.invocation_id": id}
	if triggerResp != nil {
		attrs["aktis.trigger"] = triggerResp.Name
	}
	span.Set(attrs)
	resp := map[string]any{"id": id, "status": "pending"}
	if triggerResp != nil {
		resp["trigger"] = triggerResp
	}
	w.Header().Set("location", "/invoke/"+id)
	writeJSON(w, http.StatusAccepted, resp)
}

// runInvocation drives an accepted /invoke turn to its terminal record. It
// never panics out: a lost goroutine would leave the record pending forever.
func (s *Server) runInvocation(id string, input TurnInput, sessionID string, isTrigger, forcedAgent bool) {
	defer s.driving.Delete(id)
	ctx := s.bg()
	defer func() {
		if rec := recover(); rec != nil {
			s.persistTerminal(ctx, InvocationRecord{ID: id, Status: "failed", Error: fmt.Sprint(rec)})
		}
	}()

	// Tracks the latest Remote Control URL onto the still-pending record.
	input.RemoteControlURLListener = func(url string) {
		if current := s.invocations().Get(ctx, id); current != nil && current.Status == "pending" {
			current.RemoteControlURL = url
			_ = s.invocations().Set(ctx, *current)
		}
	}
	// Persist the turn's read-only mode BEFORE anything launches: its job is
	// to make later turns read-only, and a turn that can't record that must
	// not go on to launch runs.
	if sessionID != "" && (input.ReadOnly || isTrigger) {
		if err := s.setSessionReadOnly(ctx, sessionID, input.ReadOnly); err != nil {
			s.persistTerminal(ctx, InvocationRecord{ID: id, Status: "failed", Error: err.Error()})
			return
		}
	}
	input.ReportEngineInvocation = func(engineID string) error {
		current := s.invocations().Get(ctx, id)
		if current == nil || current.Status != "pending" {
			return nil
		}
		current.EngineInvocationID = engineID
		return s.invocations().Set(ctx, *current)
	}
	outcome, err := s.Engine.Invoke(ctx, input)
	if err != nil {
		record := InvocationRecord{ID: id, Status: "failed", Error: err.Error()}
		if current := s.invocations().Get(ctx, id); current != nil {
			record.RemoteControlURL = current.RemoteControlURL
		}
		s.persistTerminal(ctx, record)
		return
	}
	s.persistTerminalOutcome(ctx, id, outcome, forcedAgent)
}

// acceptNoneTurn accepts a target.none trigger (docs/adr/0052 §3): its reply
// side effects and onComplete are real, but nothing runs — no engine turn,
// and so no identity pre-flight. The record is already succeeded.
func (s *Server) acceptNoneTurn(w http.ResponseWriter, span *Span, trigger *triggerResponse) {
	if s.isDraining() {
		w.Header().Set("retry-after", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "orchestrator is shutting down; retry"})
		return
	}
	id := uuid.NewString()
	empty := ""
	if err := s.invocations().Set(context.Background(), InvocationRecord{ID: id, Status: "succeeded", Result: &empty}); err != nil {
		log.Printf("agent-orchestrator: could not write invocation record %s: %v", id, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	span.Set(map[string]any{"aktis.invocation_id": id, "aktis.trigger": trigger.Name, "aktis.trigger.target": "none"})
	w.Header().Set("location", "/invoke/"+id)
	writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "status": "pending", "trigger": trigger})
}

func (s *Server) handleGetInvocation(w http.ResponseWriter, r *http.Request, id string) {
	record := s.invocations().Get(r.Context(), id)
	if record == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if record.Status == "pending" && !s.isDraining() {
		s.adoptInvocation(*record)
	}
	writeJSON(w, http.StatusOK, record.visible())
}

// adoptInvocation finishes a pending turn whose process died: the engine
// still holds it, so polling by the engine id the dead process recorded gets
// the same answer. Two processes adopting the same turn during a rollout is
// harmless — both write the same terminal record.
func (s *Server) adoptInvocation(record InvocationRecord) {
	if record.EngineInvocationID == "" {
		return
	}
	if _, already := s.driving.LoadOrStore(record.ID, struct{}{}); already {
		return
	}
	log.Printf("agent-orchestrator: adopting orphaned invocation %s (engine %s)", record.ID, record.EngineInvocationID)
	go func() {
		defer s.driving.Delete(record.ID)
		ctx := s.bg()
		input := TurnInput{RemoteControlURLListener: func(url string) {
			if current := s.invocations().Get(ctx, record.ID); current != nil && current.Status == "pending" {
				current.RemoteControlURL = url
				_ = s.invocations().Set(ctx, *current)
			}
		}}
		outcome, err := s.Engine.Adopt(ctx, record.EngineInvocationID, input)
		if err != nil {
			s.persistTerminal(ctx, InvocationRecord{ID: record.ID, Status: "failed", Error: err.Error()})
			return
		}
		s.persistTerminalOutcome(ctx, record.ID, outcome, record.ForcedAgent)
	}()
}

// persistTerminalOutcome writes a settled turn's record, carrying forward the
// Remote Control URL the listener recorded (this write replaces the record).
func (s *Server) persistTerminalOutcome(ctx context.Context, id string, outcome TurnOutcome, forcedAgent bool) {
	record := InvocationRecord{ID: id, Status: "succeeded", Result: outcome.Result}
	if outcome.Error != "" {
		record.Status = "failed"
		record.Error = outcome.Error
	}
	if current := s.invocations().Get(ctx, id); current != nil {
		record.RemoteControlURL = current.RemoteControlURL
	}
	if outcome.Error == "" && forcedAgent && len(outcome.RunOutcome) > 0 && string(outcome.RunOutcome) != "null" {
		if checked, err := CheckOutcome(outcome.RunOutcome); err == nil {
			record.Outcome = checked
		} else {
			log.Printf("agent-orchestrator: dropping run outcome for invocation %s: %v", id, err)
		}
	}
	if len(outcome.PendingToolCalls) > 0 {
		record.PendingToolCalls = outcome.PendingToolCalls
	}
	s.persistTerminal(ctx, record)
}

// persistTerminal never fails the caller: a Redis blip at terminal-write time
// costs THIS record its answer, logged, not the whole process.
func (s *Server) persistTerminal(ctx context.Context, record InvocationRecord) {
	if err := s.invocations().Set(ctx, record); err != nil {
		log.Printf("agent-orchestrator: failed to persist terminal invocation record %s: %v", record.ID, err)
	}
}

// ── turn input ─────────────────────────────────────────────────────────────

type turnRequest struct {
	request            string
	sessionID          string
	progressListener   func(stage, message string)
	identityLinkFlow   string
	forwardedUserToken string
	senderLogin        string
	callerTools        *callerToolTurn
	trigger            *TurnInput
}

// buildTurnInput folds in the session's sticky read-only flag. A turn no
// trigger produced inherits it, so a follow-up can only be MORE restricted; a
// trigger turn brings its own mode, writable included. Honored whoever wrote
// the record: honoring a restriction leaks nothing.
func (s *Server) buildTurnInput(ctx context.Context, t turnRequest) TurnInput {
	input := TurnInput{
		Request:            t.request,
		SenderLogin:        t.senderLogin,
		ProgressListener:   t.progressListener,
		ForwardedUserToken: t.forwardedUserToken,
		SessionID:          t.sessionID,
	}
	if t.callerTools != nil {
		if len(t.callerTools.tools) > 0 {
			input.CallerTools = t.callerTools.tools
		}
		input.CallerToolRequired = t.callerTools.required
		if len(t.callerTools.priorCalls) > 0 {
			input.PriorCallerToolCalls = t.callerTools.priorCalls
		}
	}
	flow := t.identityLinkFlow
	if flow == "" && s.DefaultIdentityLinkFlow != "" && s.DefaultIdentityLinkFlow != "authcode" {
		flow = s.DefaultIdentityLinkFlow
	}
	input.IdentityLinkFlow = flow
	if t.trigger != nil {
		input.TriggerName = t.trigger.TriggerName
		input.ForcedSkillID = t.trigger.ForcedSkillID
		input.ForcedAgentID = t.trigger.ForcedAgentID
		input.ForcedToolID = t.trigger.ForcedToolID
		input.ToolInput = t.trigger.ToolInput
		input.ReadOnly = t.trigger.ReadOnly
		input.Outcome = t.trigger.Outcome
		if len(t.trigger.Params) > 0 {
			input.Params = t.trigger.Params
		}
		input.IdentityOverride = t.trigger.IdentityOverride
	}
	if t.sessionID == "" || s.Sessions == nil {
		return input
	}
	if record := s.Sessions.Get(ctx, t.sessionID); record != nil && record.ReadOnly && t.trigger == nil {
		input.ReadOnly = true
	}
	return input
}

// setSessionReadOnly records a turn's mode on its session before it runs.
func (s *Server) setSessionReadOnly(ctx context.Context, sessionID string, readOnly bool) error {
	if s.Sessions == nil {
		return nil
	}
	existing := s.Sessions.Get(ctx, sessionID)
	if (existing != nil && existing.ReadOnly) == readOnly {
		return nil
	}
	return s.Sessions.Set(ctx, sessionID, readOnly)
}

// resolveCallerToolTurn validates tools/tool_choice and decides which reach
// the engine (docs/adr/0035): the just-in-time ranking step, skipped when
// there is nothing to prune.
func (s *Server) resolveCallerToolTurn(ctx context.Context, request string, body map[string]any, prior []callertools.PriorCall) (*callerToolTurn, error) {
	tools, choice, err := ParseCallerTools(body["tools"], body["tool_choice"])
	if err != nil {
		return nil, err
	}
	turn := &callerToolTurn{priorCalls: prior}
	if len(tools) == 0 {
		return turn, nil
	}
	turn.tools = callertools.Resolve(ctx, request, tools, choice, s.topK(), s.CallerTools)
	turn.required = choice.Kind == callertools.ChoiceAuto && choice.Required
	return turn, nil
}

// ── chat completions ───────────────────────────────────────────────────────

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	rawBody, err := readBody(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, openAIError("body must be valid JSON", "invalid_request"))
		return
	}
	forwardedUserToken := headerValue(r, forwardedUserJWTHeader)
	// Before everything else, housekeeping completions included: those spend
	// model tokens too.
	if !s.admitCaller(w, r, "/v1/chat/completions", forwardedUserToken) {
		return
	}
	var parsed any = map[string]any{}
	if len(rawBody) > 0 {
		if err := json.Unmarshal(rawBody, &parsed); err != nil {
			writeJSON(w, http.StatusBadRequest, openAIError("body must be valid JSON", "invalid_request"))
			return
		}
	}
	body, _ := parsed.(map[string]any)
	if body == nil {
		body = map[string]any{}
	}

	built := BuildAgentRequest(body["messages"])
	if built == nil {
		writeJSON(w, http.StatusBadRequest, openAIError(`messages must include a non-empty "user" message`, "invalid_request"))
		return
	}
	model := ModelID
	if m, ok := body["model"].(string); ok && m != "" {
		model = m
	}
	sessionID := headerValue(r, chatIDHeader)
	stream := body["stream"] == true
	identityLinkFlow := ""
	if flow, _ := body["identity_link_flow"].(string); flow == "device" || flow == "authcode" {
		identityLinkFlow = flow
	}

	// Housekeeping completions never reach the engine, and stay AHEAD of
	// caller-tool parsing: a title request carrying the chat's tool array
	// must come back as prose, never a tool call the client would execute.
	if IsInternalUITask(built.Request) {
		s.handleInternalUITask(w, body["messages"], model, stream)
		return
	}

	tools, toolErr := s.resolveCallerToolTurn(r.Context(), built.Request, body, built.PriorToolCalls)
	if toolErr != nil {
		writeJSON(w, http.StatusBadRequest, openAIError(toolErr.Error(), "invalid_request"))
		return
	}

	t := turnRequest{
		request:            built.Request,
		sessionID:          sessionID,
		identityLinkFlow:   identityLinkFlow,
		forwardedUserToken: forwardedUserToken,
		callerTools:        tools,
	}
	if stream {
		s.chatStreaming(w, r, t, model, built.Images)
		return
	}
	s.chatBlocking(w, r, t, model, built.Images)
}

func (s *Server) handleInternalUITask(w http.ResponseWriter, messages any, model string, stream bool) {
	content := ""
	if s.TaskCompleter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if reply, err := s.TaskCompleter.Complete(ctx, messages, model); err == nil {
			content = reply
		}
		cancel()
	}
	id := chatCompletionID()
	if !stream {
		writeJSON(w, http.StatusOK, chatCompletionResponse(id, model, content, "stop"))
		return
	}
	sse := newSSE(w)
	sse.chunk(chatCompletionChunk(id, model, map[string]any{"content": content}, nil))
	stop := "stop"
	sse.chunk(chatCompletionChunk(id, model, map[string]any{}, &stop))
	sse.done()
}

// chatBlocking: a Remote Control URL is deliberately not surfaced here — a
// blocking response returns once the turn is over, with nothing live to join.
func (s *Server) chatBlocking(w http.ResponseWriter, r *http.Request, t turnRequest, model string, images []ChatImage) {
	input := s.buildTurnInput(r.Context(), t)
	input.Images = images
	outcome, err := s.Engine.Invoke(r.Context(), input)
	if err != nil {
		outcome = TurnOutcome{Error: err.Error()}
	}
	if outcome.Error != "" {
		status, code := errorStatusAndCode(outcome.Error)
		writeJSON(w, status, openAIError(outcome.Error, code))
		return
	}
	id := chatCompletionID()
	if len(outcome.PendingToolCalls) > 0 {
		writeJSON(w, http.StatusOK, chatCompletionToolCallResponse(id, model, outcome.PendingToolCalls))
		return
	}
	content := ""
	if outcome.Result != nil {
		content = *outcome.Result
	}
	writeJSON(w, http.StatusOK, chatCompletionResponse(id, model, content, "stop"))
}

type sseWriter struct {
	mu      sync.Mutex
	w       http.ResponseWriter
	flusher http.Flusher
	closed  bool
}

func newSSE(w http.ResponseWriter) *sseWriter {
	h := w.Header()
	h.Set("content-type", "text/event-stream")
	h.Set("cache-control", "no-cache")
	h.Set("connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	s := &sseWriter{w: w, flusher: flusher}
	s.flush()
	return s
}

func (s *sseWriter) flush() {
	if s.flusher != nil {
		s.flusher.Flush()
	}
}

func (s *sseWriter) raw(text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	_, _ = io.WriteString(s.w, text)
	s.flush()
}

func (s *sseWriter) chunk(payload any) { s.raw("data: " + jsonString(payload) + "\n\n") }

// status writes an Open WebUI status event: a collapsible StatusHistory step
// above the message rather than inline text. done=false is a spinner.
func (s *sseWriter) status(description string, done bool) {
	s.chunk(map[string]any{"event": map[string]any{"type": "status", "data": map[string]any{"description": description, "done": done}}})
}

func (s *sseWriter) comment(text string) { s.raw(": " + text + "\n\n") }

func (s *sseWriter) done() {
	s.raw("data: [DONE]\n\n")
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

var leadingSpace = regexp.MustCompile(`^\s`)
var trailingSpace = regexp.MustCompile(`\s$`)

func (s *Server) chatStreaming(w http.ResponseWriter, r *http.Request, t turnRequest, model string, images []ChatImage) {
	id := chatCompletionID()
	sse := newSSE(w)

	var mu sync.Mutex
	// The open in-progress status step, closed out before the stream ends or
	// real content starts — otherwise Open WebUI's spinner sticks on it.
	openStatus := ""
	hasOpenStatus := false
	// Each agent-text block is complete and independent; written back to back
	// "...account." and "Found it" fuse. Insert a paragraph break when needed.
	lastEndedWithSpace := true
	withSeparator := func(content string) string {
		needs := !lastEndedWithSpace && !leadingSpace.MatchString(content)
		if content != "" {
			lastEndedWithSpace = trailingSpace.MatchString(content)
		}
		if needs {
			return "\n\n" + content
		}
		return content
	}
	closeStatus := func() {
		if hasOpenStatus {
			sse.status(openStatus, true)
			hasOpenStatus = false
			openStatus = ""
		}
	}
	stop := "stop"
	finish := func(content string) {
		mu.Lock()
		defer mu.Unlock()
		closeStatus()
		sse.chunk(chatCompletionChunk(id, model, map[string]any{"content": withSeparator(content)}, nil))
		sse.chunk(chatCompletionChunk(id, model, map[string]any{}, &stop))
		sse.done()
	}
	finishWithToolCalls := func(calls []callertools.PendingCall) {
		mu.Lock()
		defer mu.Unlock()
		closeStatus()
		reason := "tool_calls"
		sse.chunk(toolCallDeltaChunk(id, model, calls))
		sse.chunk(chatCompletionChunk(id, model, map[string]any{}, &reason))
		sse.done()
	}

	t.progressListener = func(stage, message string) {
		mu.Lock()
		defer mu.Unlock()
		// Real content: the agent's own narration, the one-time link prompt,
		// and a live Remote Control link. A truncated status label would
		// mangle their markdown/URLs.
		if stage == "remote-control-url" && message != "" {
			closeStatus()
			sse.chunk(chatCompletionChunk(id, model, map[string]any{
				"content": withSeparator("🤖 Watch live or take over this session here: " + message + "\n\n"),
			}, nil))
			return
		}
		if (stage == "agent-text" || stage == "identity-link") && message != "" {
			closeStatus()
			sse.chunk(chatCompletionChunk(id, model, map[string]any{"content": withSeparator(message)}, nil))
			return
		}
		// Everything else is mechanical bookkeeping: a status step.
		label := renderProgressLabel(stage, message)
		openStatus, hasOpenStatus = label, true
		sse.status(label, false)
	}

	input := s.buildTurnInput(r.Context(), t)
	input.Images = images

	type result struct {
		outcome TurnOutcome
		err     error
	}
	done := make(chan result, 1)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				done <- result{err: fmt.Errorf("%v", rec)}
			}
		}()
		outcome, err := s.Engine.Invoke(r.Context(), input)
		done <- result{outcome, err}
	}()

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-heartbeat.C:
			sse.comment("keep-alive")
		case res := <-done:
			switch {
			case res.err != nil:
				finish("❌ " + res.err.Error())
			case res.outcome.Error != "":
				finish("❌ " + res.outcome.Error)
			case len(res.outcome.PendingToolCalls) > 0:
				finishWithToolCalls(res.outcome.PendingToolCalls)
			case res.outcome.Result != nil:
				finish(RenderResult(*res.outcome.Result))
			default:
				finish("❌ agent stream ended unexpectedly")
			}
			return
		}
	}
}

// ── lifecycle ──────────────────────────────────────────────────────────────

// Listen serves on port until Shutdown. Background turns run under ctx.
func (s *Server) Listen(ctx context.Context, port int) error {
	s.background = ctx
	s.httpServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           s,
		ReadHeaderTimeout: 30 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return context.Background() },
	}
	err := s.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown waits for in-flight requests, bounded by ctx. Idle keep-alive
// connections close immediately.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}
