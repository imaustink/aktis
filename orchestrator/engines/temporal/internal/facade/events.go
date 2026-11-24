package facade

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
)

// ParseEnvelope validates /invoke's `event` field as a normalized envelope
// (docs/event-triggers.md §2). Strict on everything selection and the session
// id depend on, so CEL here sees the same shape the engine and the gateway
// produced. body is any JSON; an absent one is {} (a cron source with no
// payload).
func ParseEnvelope(raw any) (catalog.Envelope, error) {
	obj, ok := raw.(map[string]any)
	if !ok {
		return catalog.Envelope{}, fmt.Errorf("event must be an object")
	}
	str := func(key string) (string, bool) {
		s, ok := obj[key].(string)
		return s, ok && s != ""
	}
	source, ok := str("source")
	if !ok {
		return catalog.Envelope{}, fmt.Errorf("event.source must be a non-empty string")
	}
	kind, _ := obj["kind"].(string)
	if kind != "webhook" && kind != "cron" && kind != "run" {
		return catalog.Envelope{}, fmt.Errorf(`event.kind must be "webhook", "cron" or "run"`)
	}
	id, ok := str("id")
	if !ok {
		return catalog.Envelope{}, fmt.Errorf("event.id must be a non-empty string")
	}
	eventTime, ok := str("time")
	if !ok {
		return catalog.Envelope{}, fmt.Errorf("event.time must be a non-empty string")
	}

	headers := map[string]string{}
	if rawHeaders, present := obj["headers"]; present {
		m, ok := rawHeaders.(map[string]any)
		if !ok {
			return catalog.Envelope{}, fmt.Errorf("event.headers must be an object")
		}
		for name, value := range m {
			s, ok := value.(string)
			if !ok {
				return catalog.Envelope{}, fmt.Errorf("event.headers[%s] must be a string", strconv.Quote(name))
			}
			headers[name] = s
		}
	}
	sender := map[string]any{}
	if rawSender, present := obj["sender"]; present {
		m, ok := rawSender.(map[string]any)
		if !ok {
			return catalog.Envelope{}, fmt.Errorf("event.sender must be an object")
		}
		sender = m
	}
	repository := ""
	if rawRepo, present := obj["repository"]; present {
		s, ok := rawRepo.(string)
		if !ok {
			return catalog.Envelope{}, fmt.Errorf("event.repository must be a string")
		}
		repository = s
	}
	body, present := obj["body"]
	if !present {
		body = map[string]any{}
	}
	return catalog.Envelope{
		Source: source, Kind: kind, ID: id, Time: eventTime,
		Headers: headers, Body: body, Sender: sender, Repository: repository,
	}, nil
}

// MaxOutcomeBytes bounds a run outcome's compact JSON — the same number the
// engine enforces. It rides in the invocation record, the run-completion
// event and every CEL evaluation over it.
const MaxOutcomeBytes = 8192

// CheckOutcome accepts raw only as a JSON object whose compact serialization
// is at most MaxOutcomeBytes. Anything else is refused, never truncated: a
// half outcome could still satisfy a CEL predicate it should not.
func CheckOutcome(raw json.RawMessage) (map[string]any, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("outcome is not serializable: %v", err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("outcome must be a JSON object, got %s", jsonKind(value))
	}
	if size := len(compactJSON(object)); size > MaxOutcomeBytes {
		return nil, fmt.Errorf("outcome is %d bytes of JSON, over the %d-byte limit", size, MaxOutcomeBytes)
	}
	return object, nil
}

// compactJSON is JSON.stringify's bytes: compact, no HTML escaping.
func compactJSON(v any) []byte { return []byte(jsonString(v)) }

func jsonKind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case []any:
		return "array"
	case string:
		return "string"
	case bool:
		return "boolean"
	case float64:
		return "number"
	}
	return "object"
}

// ── /events/prefilter ──────────────────────────────────────────────────────

// SenderExpressions are an EventSource's spec.sender CEL fields.
type SenderExpressions struct {
	LoginFrom      string `json:"loginFrom"`
	IsBotFrom      string `json:"isBotFrom,omitempty"`
	RepositoryFrom string `json:"repositoryFrom,omitempty"`
	AccountIDFrom  string `json:"accountIdFrom,omitempty"`
}

// PrefilterRequest is integration-gateway's question about one webhook
// delivery, asked before it spends any provider API call on the sender.
type PrefilterRequest struct {
	// Envelope is the PRELIMINARY envelope: sender {} and repository "".
	Envelope json.RawMessage    `json:"envelope"`
	Sender   *SenderExpressions `json:"sender,omitempty"`
}

// EvalResult is one CEL evaluation: a value and its CEL type name, or why
// there is none. Ints travel as decimal strings so an account id never meets
// a JSON number's 2^53 limit.
type EvalResult struct {
	OK    bool   `json:"ok"`
	Type  string `json:"type,omitempty"`
	Value any    `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

// PrefilterResponse answers a PrefilterRequest.
type PrefilterResponse struct {
	// CouldMatch false means no trigger can fire for this delivery whoever
	// sent it: the gateway drops it without resolving the sender.
	CouldMatch bool `json:"couldMatch"`
	// Sender holds each requested sender expression's result, keyed by field
	// name (loginFrom, ...), evaluated over the preliminary envelope.
	Sender map[string]EvalResult `json:"sender,omitempty"`
}

// Prefilter answers a PrefilterRequest against the live trigger table.
func Prefilter(triggers *catalog.EventTriggerRegistry, req PrefilterRequest) (PrefilterResponse, error) {
	var raw any
	if err := json.Unmarshal(req.Envelope, &raw); err != nil {
		return PrefilterResponse{}, fmt.Errorf("envelope must be JSON: %v", err)
	}
	envelope, err := ParseEnvelope(raw)
	if err != nil {
		return PrefilterResponse{}, err
	}
	resp := PrefilterResponse{}
	if triggers != nil {
		resp.CouldMatch = triggers.CouldMatchBeforeSender(envelope)
	}
	if req.Sender != nil && resp.CouldMatch {
		resp.Sender = map[string]EvalResult{}
		for field, expr := range map[string]string{
			"loginFrom":      req.Sender.LoginFrom,
			"isBotFrom":      req.Sender.IsBotFrom,
			"repositoryFrom": req.Sender.RepositoryFrom,
			"accountIdFrom":  req.Sender.AccountIDFrom,
		} {
			if expr == "" {
				continue
			}
			resp.Sender[field] = evaluate(expr, envelope)
		}
	}
	return resp, nil
}

func evaluate(expr string, envelope catalog.Envelope) EvalResult {
	value, err := catalog.EvalEventExpr(expr, envelope)
	if err != nil {
		return EvalResult{Error: err.Error()}
	}
	result := EvalResult{OK: true, Type: catalog.ValueType(value), Value: value}
	if n, ok := value.(int64); ok {
		result.Value = strconv.FormatInt(n, 10)
	}
	return result
}

// maxCompileExpressions bounds one /events/compile request. A sender block
// has at most four expressions; the gateway sends one source's at a time.
const maxCompileExpressions = 64

// CompileRequest is POST /events/compile's body.
type CompileRequest struct {
	Expressions []string `json:"expressions"`
}

// CompileResponse maps each expression that does NOT compile to the
// compiler's message. An expression absent from Errors compiled.
type CompileResponse struct {
	Errors map[string]string `json:"errors"`
}

// Compile checks expressions with the same compiler Prefilter evaluates them
// with, so "compiles" here and "can be evaluated over a delivery" agree.
func Compile(req CompileRequest) CompileResponse {
	resp := CompileResponse{Errors: map[string]string{}}
	for _, expr := range req.Expressions {
		if _, err := catalog.CompileEventExpr(expr); err != nil {
			resp.Errors[expr] = err.Error()
		}
	}
	return resp
}
