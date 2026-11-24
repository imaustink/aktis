package facade

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"regexp"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/aktisai/orchestrator/engines/temporal/internal/telemetry"
)

// TraceparentHeader is the W3C trace-context header (docs/adr/0050 §4).
const TraceparentHeader = "traceparent"

// safeAttribute is the allow-list of span attributes: aktis.* plus a few
// standard HTTP ones. Nothing else is recorded, so a span can never carry a
// prompt, a token or a login by accident.
var safeAttribute = regexp.MustCompile(`^(aktis\.[a-z0-9_.]+|http\.request\.method|http\.route|http\.response\.status_code|error\.type)$`)

// Span wraps an OpenTelemetry span with the traceparent string the next hop
// carries. With tracing off it still yields a valid traceparent (the inbound
// trace id, or a fresh one), so the engine's spans join whoever does export.
type Span struct {
	span        trace.Span
	Traceparent string
	ctx         context.Context
}

// StartSpan starts a span continuing parent (a traceparent header value, may
// be empty or invalid).
func StartSpan(name, parent string, kind trace.SpanKind, attrs map[string]any) *Span {
	ctx := context.Background()
	if parent != "" {
		ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{TraceparentHeader: parent})
	}
	ctx, span := telemetry.Tracer().Start(ctx, name, trace.WithSpanKind(kind))
	s := &Span{span: span, ctx: ctx}
	carrier := propagation.MapCarrier{}
	if span.SpanContext().IsValid() && span.IsRecording() {
		propagation.TraceContext{}.Inject(ctx, carrier)
		s.Traceparent = carrier[TraceparentHeader]
	}
	if s.Traceparent == "" {
		s.Traceparent = nextTraceparent(parent)
	}
	s.Set(attrs)
	return s
}

// Set records the allow-listed, non-empty attributes.
func (s *Span) Set(attrs map[string]any) {
	for key, value := range attrs {
		if !safeAttribute.MatchString(key) {
			continue
		}
		switch v := value.(type) {
		case string:
			if v != "" {
				s.span.SetAttributes(attribute.String(key, v))
			}
		case int:
			s.span.SetAttributes(attribute.Int(key, v))
		case bool:
			s.span.SetAttributes(attribute.Bool(key, v))
		}
	}
}

// Fail marks the span errored with a short machine code.
func (s *Span) Fail(code string) {
	s.span.SetAttributes(attribute.String("error.type", code))
	s.span.SetStatus(codes.Error, "")
}

func (s *Span) End() { s.span.End() }

var traceparentPattern = regexp.MustCompile(`^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$`)

// nextTraceparent keeps the inbound trace id when valid, with a fresh span id.
func nextTraceparent(inbound string) string {
	traceID := ""
	if m := traceparentPattern.FindStringSubmatch(inbound); m != nil &&
		m[1] != "00000000000000000000000000000000" && m[2] != "0000000000000000" {
		traceID = m[1]
	}
	if traceID == "" {
		traceID = randomHex(16)
	}
	return "00-" + traceID + "-" + randomHex(8) + "-01"
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func spanKindServer() trace.SpanKind { return trace.SpanKindServer }
func spanKindClient() trace.SpanKind { return trace.SpanKindClient }

func headerOf(r *http.Request, name string) string { return firstNonEmpty(r.Header.Values(name)) }

func firstNonEmpty(values []string) string {
	if len(values) == 0 {
		return ""
	}
	v := values[0]
	for _, c := range v {
		if c != ' ' && c != '\t' {
			return v
		}
	}
	return ""
}
