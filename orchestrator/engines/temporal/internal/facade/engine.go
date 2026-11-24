package facade

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
	"github.com/aktisai/orchestrator/engines/temporal/internal/rbac"
)

// TurnInput is everything the facade hands the engine for one turn. The
// engine holds the conversation's own state, so this is only what was
// resolved from the request.
type TurnInput struct {
	Request string
	Images  []ChatImage
	// ForwardedUserToken is Open WebUI's per-request signed user JWT.
	ForwardedUserToken string
	SessionID          string
	// CallerTools are already parsed, validated and top-K-pruned.
	CallerTools          []callertools.Descriptor
	CallerToolRequired   bool
	PriorCallerToolCalls []callertools.PriorCall
	// IdentityLinkFlow "" means the engine's own default (authcode).
	IdentityLinkFlow string
	// ProgressListener is set only by the SSE streaming handler. Its presence
	// is the "this caller has somewhere to wait" signal: it is what lets a
	// turn parked on an account link auto-resume (see autoResumeLink).
	ProgressListener func(stage, message string)
	// RemoteControlURLListener is told each new Remote Control session URL.
	// Separate from ProgressListener so a fire-and-forget caller can record
	// the URL without also opting into the auto-resume hold.
	RemoteControlURLListener func(url string)
	// SenderLogin is the verified webhook sender (or the unsigned envelope
	// field, without an assertion secret).
	SenderLogin  string
	SenderClaims *rbac.SenderClaims
	// TargetRepository is the webhook event's repository, for the engine's
	// read gate.
	TargetRepository string
	// The EventTrigger-resolved dispatch, when there is one.
	TriggerName   string
	ForcedSkillID string
	ForcedAgentID string
	ForcedToolID  string
	ToolInput     string
	ReadOnly      bool
	// Outcome is the trigger's target.outcome (docs/adr/0055): the structured
	// outcome the forced agent's run reports. Trigger turns only, never
	// sticky, and forwarded only inside the signed dispatch assertion.
	Outcome          string
	Params           map[string]string
	IdentityOverride string
	Traceparent      string
	// ReportEngineInvocation records the engine's id for this turn as soon as
	// the engine accepts it, so another process can finish it.
	ReportEngineInvocation func(engineInvocationID string) error
}

// TurnOutcome is how a turn ended: an answer, an error, or tool calls for
// the caller to run.
type TurnOutcome struct {
	Result           *string
	Error            string
	PendingToolCalls []callertools.PendingCall
	// RunOutcome is a forced agentRef run's raw outcome; the server checks it.
	RunOutcome json.RawMessage
}

// IdentityRefusal is the engine's 403 body for an accept-phase identity
// refusal (docs/adr/0049 §6): {"status":"failed","error":{code,message,...}}.
// Body is the whole object, so whatever the engine reports reaches the caller
// intact; Error is its `error` member.
type IdentityRefusal struct {
	Body  map[string]any
	Error map[string]any
}

// Engine runs turns. An interface so the server is testable without HTTP.
type Engine interface {
	Invoke(ctx context.Context, input TurnInput) (TurnOutcome, error)
	// Adopt finishes a turn a now-dead process started, by its engine id.
	Adopt(ctx context.Context, engineInvocationID string, input TurnInput) (TurnOutcome, error)
	// CheckIdentity asks the engine's accept-phase identity check before a
	// trigger turn is accepted. nil is no objection, including whenever the
	// check cannot be made.
	CheckIdentity(ctx context.Context, input TurnInput) *IdentityRefusal
}

// TemporalEngine is the HTTP client for the engine gateway's accept-then-poll
// /invoke. It is deliberately not a Temporal client: the gateway already
// implements this contract, and Temporal credentials stay out of this pod.
type TemporalEngine struct {
	BaseURL               string
	Token                 string
	SenderAssertionSecret string
	// ForwardedUserResolver resolves Open WebUI's per-request JWT into the
	// identity a chat turn runs as. Deliberately NOT a general bearer
	// resolver: every other caller of this process's /invoke shares one token
	// that says nothing about who is asking.
	ForwardedUserResolver IdentityResolver
	// Timeout bounds one turn's poll.
	Timeout         time.Duration
	AutoResumeMax   time.Duration
	AutoResumeEvery time.Duration
	PollInterval    time.Duration
	HTTP            *http.Client
	Now             func() time.Time
}

const (
	defaultEnginePollInterval = 500 * time.Millisecond
	defaultEngineTimeout      = 30 * time.Minute
	// defaultAutoResumeMax covers a human actually going and linking (a
	// GitHub device code lasts 15 minutes) without pinning a resume loop for
	// a caller who abandoned the tab.
	defaultAutoResumeMax = 10 * time.Minute
	// defaultAutoResumeEvery spaces out resumes: each one is a brand-new turn.
	defaultAutoResumeEvery = 4 * time.Second
)

// contentNarrationPrefixes are stages the gateway flattens into a narration
// line as "<stage>: <message>" that the SSE handler renders as real chat
// content rather than a truncated status label.
var contentNarrationPrefixes = []string{"agent-text", "identity-link", "remote-control-url"}

func splitNarrationLine(line string) (string, string) {
	for _, stage := range contentNarrationPrefixes {
		if rest, ok := strings.CutPrefix(line, stage+": "); ok {
			return stage, rest
		}
	}
	return "", line
}

// forwardedCaller is the per-user identity a turn carries across the hop.
type forwardedCaller struct {
	subject string
	roles   []string
}

var errUnresolvedCaller = errors.New("unresolved forwarded caller")

type engineRecord struct {
	ID               string                    `json:"id"`
	Status           string                    `json:"status"`
	Result           *string                   `json:"result,omitempty"`
	Error            string                    `json:"error,omitempty"`
	ToolCalls        []callertools.PendingCall `json:"toolCalls,omitempty"`
	Progress         []string                  `json:"progress,omitempty"`
	Path             string                    `json:"path,omitempty"`
	RemoteControlURL string                    `json:"remoteControlUrl,omitempty"`
	Outcome          json.RawMessage           `json:"outcome,omitempty"`
}

func (e *TemporalEngine) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

func (e *TemporalEngine) client() *http.Client {
	if e.HTTP != nil {
		return e.HTTP
	}
	return http.DefaultClient
}

func (e *TemporalEngine) base() string { return strings.TrimRight(e.BaseURL, "/") }

func durationOr(d, fallback time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return fallback
}

func (e *TemporalEngine) Invoke(ctx context.Context, input TurnInput) (TurnOutcome, error) {
	caller, err := e.resolveForwardedCaller(ctx, input)
	if errors.Is(err, errUnresolvedCaller) {
		// Sending the turn on without the header would run it as the
		// gateway's shared bearer subject, with that subject's credentials.
		return TurnOutcome{Error: "unauthorized: could not resolve caller identity"}, nil
	}
	if input.TriggerName != "" && e.SenderAssertionSecret == "" {
		return TurnOutcome{Error: "temporal engine: an event-triggered turn needs AGENT_SENDER_ASSERTION_SECRET to sign its dispatch"}, nil
	}

	id, err := e.start(ctx, input, caller)
	if err != nil {
		return TurnOutcome{}, err
	}
	// Best effort: a failure only costs this turn its rollout survivability.
	if input.ReportEngineInvocation != nil {
		if err := input.ReportEngineInvocation(id); err != nil {
			log.Printf("temporal engine: could not record engine invocation %s: %v", id, err)
		}
	}
	record, err := e.poll(ctx, id, input)
	if err != nil {
		return TurnOutcome{}, err
	}
	if record.Status == "succeeded" && record.Path == "link-required" {
		if record, err = e.autoResumeLink(ctx, input, record, caller); err != nil {
			return TurnOutcome{}, err
		}
	}
	return toOutcome(record), nil
}

// Adopt polls an engine id another process recorded. No link auto-resume:
// that needs a live progress listener, which died with its process.
func (e *TemporalEngine) Adopt(ctx context.Context, engineInvocationID string, input TurnInput) (TurnOutcome, error) {
	record, err := e.poll(ctx, engineInvocationID, input)
	if err != nil {
		return TurnOutcome{}, err
	}
	return toOutcome(record), nil
}

func toOutcome(record engineRecord) TurnOutcome {
	if record.Status == "failed" {
		message := record.Error
		if message == "" {
			message = "the turn failed"
		}
		return TurnOutcome{Error: message}
	}
	if len(record.ToolCalls) > 0 {
		return TurnOutcome{PendingToolCalls: record.ToolCalls}
	}
	return TurnOutcome{Result: record.Result, RunOutcome: record.Outcome}
}

// autoResumeLink re-submits a turn parked on an outstanding account link to
// the SAME session every few seconds, so a live caller's chat picks up once
// the human links, without them sending another message. Gated on a live
// ProgressListener (a webhook relay must not block here) and a session id.
//
// Every resume carries the identity Invoke resolved ONCE. Open WebUI's JWT
// expires after 300s; re-verifying it on each resume once made the fifth
// minute's resume run on the gateway's shared subject.
func (e *TemporalEngine) autoResumeLink(ctx context.Context, input TurnInput, first engineRecord, caller *forwardedCaller) (engineRecord, error) {
	if input.ProgressListener == nil || input.SessionID == "" {
		return first, nil
	}
	firstResult := ""
	if first.Result != nil {
		firstResult = *first.Result
	}
	input.ProgressListener("identity-link", firstResult)

	record := first
	deadline := e.now().Add(durationOr(e.AutoResumeMax, defaultAutoResumeMax))
	interval := durationOr(e.AutoResumeEvery, defaultAutoResumeEvery)
	for record.Status == "succeeded" && record.Path == "link-required" && e.now().Before(deadline) {
		if err := sleepCtx(ctx, interval); err != nil {
			return record, err
		}
		id, err := e.start(ctx, input, caller)
		if err != nil {
			return record, err
		}
		if record, err = e.poll(ctx, id, input); err != nil {
			return record, err
		}
	}
	if record.Status == "succeeded" && record.Path == "link-required" {
		// Gave up waiting, not gave up: the flow is likely still live
		// server-side, and the next message resumes it. The prompt itself was
		// already shown, so it is not repeated.
		waiting := "Still waiting for you to finish linking your account. " +
			"I'll pick this back up automatically once you have, or send any message to check now."
		record.Result = &waiting
	}
	return record, nil
}

// resolveForwardedCaller: nil = no per-user identity to forward (the gateway
// resolves from the bearer as before); errUnresolvedCaller = the turn DID
// carry one and it can't be verified or signed, so the turn must fail.
func (e *TemporalEngine) resolveForwardedCaller(ctx context.Context, input TurnInput) (*forwardedCaller, error) {
	if input.ForwardedUserToken == "" || e.ForwardedUserResolver == nil {
		return nil, nil
	}
	identity := e.ForwardedUserResolver.Resolve(ctx, input.ForwardedUserToken)
	if identity == nil {
		return nil, errUnresolvedCaller
	}
	if e.SenderAssertionSecret == "" {
		log.Printf("temporal engine: a chat turn carried a per-user identity but AGENT_SENDER_ASSERTION_SECRET is unset, " +
			"so it cannot be forwarded; refusing the turn rather than running it as the gateway's shared subject")
		return nil, errUnresolvedCaller
	}
	return &forwardedCaller{subject: identity.Subject, roles: identity.Roles}, nil
}

func (e *TemporalEngine) start(ctx context.Context, input TurnInput, caller *forwardedCaller) (string, error) {
	hop := StartSpan("engine POST /invoke", input.Traceparent, spanKindClient(), map[string]any{
		"http.request.method": "POST", "http.route": "/invoke", "aktis.trigger": input.TriggerName,
	})
	defer hop.End()
	header, body := e.invokeRequest(input, caller, hop.Traceparent)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.base()+"/invoke", bytes.NewReader(body))
	if err != nil {
		hop.Fail("engine_unreachable")
		return "", err
	}
	req.Header = header
	res, err := e.client().Do(req)
	if err != nil {
		hop.Fail("engine_unreachable")
		return "", err
	}
	defer res.Body.Close()
	hop.Set(map[string]any{"http.response.status_code": res.StatusCode})
	if res.StatusCode < 200 || res.StatusCode > 299 {
		hop.Fail(fmt.Sprintf("http_%d", res.StatusCode))
		// A 400 is something the user can fix (an image the engine refuses),
		// so its reason is worth showing; anything else stays the bare status.
		reason := ""
		if res.StatusCode == http.StatusBadRequest {
			reason = engineErrorMessage(res.Body)
		}
		if reason != "" {
			return "", fmt.Errorf("temporal engine /invoke failed: %d (%s)", res.StatusCode, reason)
		}
		return "", fmt.Errorf("temporal engine /invoke failed: %d", res.StatusCode)
	}
	var accepted struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&accepted); err != nil {
		hop.Fail(fmt.Sprintf("http_%d", res.StatusCode))
		return "", fmt.Errorf("temporal engine /invoke: %w", err)
	}
	hop.Set(map[string]any{"aktis.invocation_id": accepted.ID})
	return accepted.ID, nil
}

// engineErrorMessage reads {"error":{"message":...}} or {"error":"..."}.
func engineErrorMessage(body io.Reader) string {
	var parsed struct {
		Error json.RawMessage `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(body, 1<<20)).Decode(&parsed) != nil {
		return ""
	}
	var s string
	if json.Unmarshal(parsed.Error, &s) == nil {
		return strings.TrimSpace(s)
	}
	var obj struct {
		Message any `json:"message"`
	}
	if json.Unmarshal(parsed.Error, &obj) == nil {
		if m, ok := obj.Message.(string); ok {
			return strings.TrimSpace(m)
		}
	}
	return ""
}

func (e *TemporalEngine) CheckIdentity(ctx context.Context, input TurnInput) *IdentityRefusal {
	if input.TriggerName != "" && e.SenderAssertionSecret == "" {
		return nil
	}
	hop := StartSpan("engine POST /invoke/check", input.Traceparent, spanKindClient(), map[string]any{
		"http.request.method": "POST", "http.route": "/invoke/check", "aktis.trigger": input.TriggerName,
	})
	defer hop.End()
	header, body := e.invokeRequest(input, nil, hop.Traceparent)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.base()+"/invoke/check", bytes.NewReader(body))
	if err != nil {
		hop.Fail("engine_unreachable")
		return nil
	}
	req.Header = header
	res, err := e.client().Do(req)
	if err != nil {
		hop.Fail("engine_unreachable")
		return nil
	}
	defer res.Body.Close()
	hop.Set(map[string]any{"http.response.status_code": res.StatusCode})
	if res.StatusCode != http.StatusForbidden {
		return nil
	}
	var refusal map[string]any
	if json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&refusal) != nil {
		return nil
	}
	errObj, _ := refusal["error"].(map[string]any)
	code, okCode := errObj["code"].(string)
	_, okMessage := errObj["message"].(string)
	if !okCode || !okMessage {
		return nil
	}
	hop.Set(map[string]any{"aktis.identity_error": code})
	return &IdentityRefusal{Body: refusal, Error: errObj}
}

type enginePriorCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
	Result    string `json:"result"`
}

type engineInvokeBody struct {
	Request              string                   `json:"request"`
	SessionID            string                   `json:"sessionId,omitempty"`
	Images               []ChatImage              `json:"images,omitempty"`
	ReadOnly             bool                     `json:"readOnly,omitempty"`
	IdentityLinkFlow     string                   `json:"identityLinkFlow,omitempty"`
	TargetRepository     string                   `json:"targetRepository,omitempty"`
	CallerTools          []callertools.Descriptor `json:"callerTools,omitempty"`
	CallerToolRequired   bool                     `json:"callerToolRequired,omitempty"`
	PriorCallerToolCalls []enginePriorCall        `json:"priorCallerToolCalls,omitempty"`
}

func (e *TemporalEngine) headers() http.Header {
	h := http.Header{}
	h.Set("content-type", "application/json")
	if e.Token != "" {
		h.Set("authorization", "Bearer "+e.Token)
	}
	return h
}

// invokeRequest builds the engine call. Identity and dispatch travel as
// SIGNED assertions, never body fields: the sender login (which credentials
// a run gets), this process's per-user caller identity, and the resolved
// trigger dispatch (which the engine refuses from the body).
func (e *TemporalEngine) invokeRequest(input TurnInput, caller *forwardedCaller, traceparent string) (http.Header, []byte) {
	h := e.headers()
	h.Set(TraceparentHeader, traceparent)
	now := time.Now()
	secret := e.SenderAssertionSecret

	if input.SenderLogin != "" && secret != "" {
		if input.SenderClaims != nil && input.SenderClaims.Login == input.SenderLogin {
			h.Set(rbac.SenderAssertionHeader, rbac.MintSenderClaims(secret, *input.SenderClaims, rbac.DefaultAssertionTTL, now))
		} else {
			h.Set(rbac.SenderAssertionHeader, rbac.MintSenderAssertion(secret, input.SenderLogin, rbac.DefaultAssertionTTL, now))
		}
	}
	// Minted fresh on every start, so a late auto-resume still carries an
	// unexpired assertion.
	if caller != nil && secret != "" {
		h.Set(rbac.CallerIdentityHeader, rbac.MintCallerIdentityAssertion(secret, caller.subject, caller.roles, true, rbac.DefaultAssertionTTL, now))
	}
	if input.TriggerName != "" && secret != "" {
		d := rbac.Dispatch{
			TriggerName:   input.TriggerName,
			ForcedSkillID: input.ForcedSkillID,
			ForcedAgentID: input.ForcedAgentID,
			ReadOnly:      input.ReadOnly,
			Outcome:       input.Outcome,
			Identity:      input.IdentityOverride,
		}
		if input.ForcedToolID != "" {
			d.ForcedToolID = input.ForcedToolID
			d.ToolInput = input.ToolInput
		}
		if len(input.Params) > 0 {
			d.Params = input.Params
		}
		h.Set(rbac.DispatchAssertionHeader, rbac.MintDispatchAssertion(secret, d, input.SessionID, input.Request, rbac.DefaultAssertionTTL, now))
	}

	body := engineInvokeBody{
		Request:            input.Request,
		SessionID:          input.SessionID,
		Images:             input.Images,
		ReadOnly:           input.ReadOnly,
		IdentityLinkFlow:   input.IdentityLinkFlow,
		TargetRepository:   input.TargetRepository,
		CallerTools:        input.CallerTools,
		CallerToolRequired: input.CallerToolRequired,
	}
	// The engine's own callertools.ID re-adds the caller: namespace, so a
	// prefixed name here would double it.
	for _, call := range input.PriorCallerToolCalls {
		body.PriorCallerToolCalls = append(body.PriorCallerToolCalls, enginePriorCall{
			Name: callertools.NameFromID(call.Name), Arguments: call.Arguments, Result: call.Result,
		})
	}
	payload, _ := json.Marshal(body)
	return h, payload
}

// poll waits for a terminal engine record. A transport failure is retried
// until the deadline (an engine rollout produces exactly that, and the turn
// is still alive); a non-2xx answer is fatal. At the deadline the turn is
// reported as a resumable pause, because the record IS the workflow.
func (e *TemporalEngine) poll(ctx context.Context, id string, input TurnInput) (engineRecord, error) {
	deadline := e.now().Add(durationOr(e.Timeout, defaultEngineTimeout))
	interval := durationOr(e.PollInterval, defaultEnginePollInterval)
	seen := 0
	announced := ""
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.base()+"/invoke/"+url.PathEscape(id), nil)
		if err != nil {
			return engineRecord{}, err
		}
		req.Header = e.headers()
		res, err := e.client().Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return engineRecord{}, ctx.Err()
			}
			if !e.now().Before(deadline) {
				return engineRecord{}, fmt.Errorf("temporal engine /invoke/%s unreachable for the poll budget: %v", id, err)
			}
			if err := sleepCtx(ctx, interval); err != nil {
				return engineRecord{}, err
			}
			continue
		}
		if res.StatusCode < 200 || res.StatusCode > 299 {
			res.Body.Close()
			return engineRecord{}, fmt.Errorf("temporal engine /invoke/%s failed: %d", id, res.StatusCode)
		}
		var record engineRecord
		err = json.NewDecoder(res.Body).Decode(&record)
		res.Body.Close()
		if err != nil {
			return engineRecord{}, fmt.Errorf("temporal engine /invoke/%s: %w", id, err)
		}

		// Checked before the terminal return: the engine also sets it on the
		// finished record, so a short turn polled once still reports it.
		if record.RemoteControlURL != "" && record.RemoteControlURL != announced {
			announced = record.RemoteControlURL
			if input.RemoteControlURLListener != nil {
				input.RemoteControlURLListener(record.RemoteControlURL)
			}
		}
		if record.Status != "pending" {
			return record, nil
		}
		// The engine resends the WHOLE narration buffer each poll.
		if input.ProgressListener != nil && len(record.Progress) > seen {
			for _, line := range record.Progress[seen:] {
				input.ProgressListener(splitNarrationLine(line))
			}
			seen = len(record.Progress)
		}
		if !e.now().Before(deadline) {
			pause := "That's taking longer than I can wait here — it's still running, " +
				"and I'll have the answer on your next message."
			return engineRecord{ID: id, Status: "succeeded", Result: &pause}, nil
		}
		if err := sleepCtx(ctx, interval); err != nil {
			return engineRecord{}, err
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
