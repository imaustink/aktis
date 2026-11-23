package main

import (
	"bytes"
	"errors"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

// Ported from retry.test.ts where retry's fixed delays allow: the delay
// schedule and exhaustion cases would sleep for real (1s doubling to 15s over
// 12 attempts), since retry has no injectable sleep.

func TestRetryReturnsImmediatelyWhenTheFirstAttemptSucceeds(t *testing.T) {
	calls := 0
	require.NoError(t, retry("op", func() error { calls++; return nil }))
	require.Equal(t, 1, calls)
}

// One real retry: a single 1s backoff.
func TestRetryRetriesUntilTheOperationSucceeds(t *testing.T) {
	if testing.Short() {
		t.Skip("sleeps for one backoff")
	}
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })

	calls := 0
	err := retry("qdrant", func() error {
		calls++
		if calls < 2 {
			return errors.New("fetch failed")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Contains(t, buf.String(), "qdrant failed (attempt 1/12): fetch failed -- retrying in 1s")
}
