package catalog

// CouldMatchBeforeSender reports whether any trigger could match e, an
// envelope whose `sender` and `repository` are NOT resolved yet
// (integration-gateway's preliminary envelope, docs/event-triggers.md §8).
//
// Resolving a sender costs provider API calls, and most deliveries (push,
// check_run, ...) can fire no trigger, so the gateway asks this first and
// drops the delivery when the answer is false. It only ever drops: anything
// that survives is matched for real by Match once the sender is resolved.
//
// A trigger whose match expression references `sender` or `repository`
// cannot be decided yet, so it counts as a possible match. Every other trigger
// is evaluated exactly as Match would (Ready, compiled, same source filter, an
// evaluation error or a non-bool is a non-match).
func (r *EventTriggerRegistry) CouldMatchBeforeSender(e Envelope) bool {
	return CouldMatchBeforeSender(r.snapshot(), e)
}

// CouldMatchBeforeSender is the registry method over an explicit list.
func CouldMatchBeforeSender(triggers []*EventTriggerDescriptor, e Envelope) bool {
	activation := e.activation()
	for _, d := range triggers {
		if !d.Ready || d.invalid != nil {
			continue
		}
		if d.Spec.SourceRef != "" && d.Spec.SourceRef != e.Source {
			continue
		}
		if d.match == nil || d.match.readsResolved {
			return true
		}
		verdict, err := d.match.eval(activation)
		if err != nil {
			continue
		}
		if matched, ok := verdict.(bool); ok && matched {
			return true
		}
	}
	return false
}
