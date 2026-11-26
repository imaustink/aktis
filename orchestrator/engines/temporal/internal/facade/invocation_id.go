package facade

import (
	"encoding/base64"
	"strings"
)

// /invoke ids carry everything a poll needs, so no process keeps a record of
// them (docs/adr/0058 §1). Three shapes, told apart by prefix:
//
//   - "t.<engine id>": a turn the engine accepted. The engine id names a
//     Temporal update, so any replica answers the poll from Temporal.
//   - "none.<uuid>": a target.none acceptance, succeeded with result "".
//   - "failed.<base64url message>": a turn that never reached the engine.
//
// Any other id, a bare UUID a pre-0058 process handed out included, is a 404.
//
// Forging any of these gains nothing: the caller already holds the /invoke
// token, a made-up engine id is unknown to the engine (404), and the other
// two only echo back what the id itself says.
const (
	turnInvocationPrefix   = "t."
	noneInvocationPrefix   = "none."
	failedInvocationPrefix = "failed."

	// maxFailedMessageBytes keeps a failed id a reasonable URL path segment.
	maxFailedMessageBytes = 1024
)

func failedInvocationID(message string) string {
	if len(message) > maxFailedMessageBytes {
		message = message[:maxFailedMessageBytes]
	}
	return failedInvocationPrefix + base64.RawURLEncoding.EncodeToString([]byte(message))
}

func decodeFailedInvocationID(id string) (string, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(id, failedInvocationPrefix))
	if err != nil {
		return "", false
	}
	return string(raw), true
}
