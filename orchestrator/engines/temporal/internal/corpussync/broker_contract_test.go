package corpussync

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestE2EBrokerContract drives HTTPSource against the REAL connection-broker
// server (TypeScript), which e2e/specs/broker-contract.e2e.ts runs in-process
// with a fake driver and points this test at. The two sides agree on nothing
// but a URL convention, and that agreement has broken silently before (a
// route rename that every unit test, asserting only the URL its own side
// built, missed). Skipped outside that spec, which requires it to PASS.
func TestE2EBrokerContract(t *testing.T) {
	base := os.Getenv("BROKER_CONTRACT_URL")
	if base == "" {
		t.Skip("run by e2e/specs/broker-contract.e2e.ts")
	}
	corpus := os.Getenv("BROKER_CONTRACT_CORPUS")
	source := NewHTTPSource(base, os.Getenv("BROKER_CONTRACT_SYNC_TOKEN"))
	ctx := context.Background()

	refs, next, err := source.List(ctx, corpus, "")
	require.NoError(t, err)
	require.Equal(t, []ResourceRef{{ID: "1"}}, refs)
	require.Empty(t, next)

	doc, err := source.Fetch(ctx, corpus, "1")
	require.NoError(t, err)
	require.Equal(t, "body", doc.Markdown)
	require.Equal(t, "Runbook", doc.Title)

	// A wrong token is this worker's misconfiguration, never "the resource
	// is inaccessible": it must not read as a denial a reconcile could act on.
	_, _, err = NewHTTPSource(base, "not-the-token").List(ctx, corpus, "")
	var se *SourceError
	require.ErrorAs(t, err, &se)
	require.Equal(t, SourcePermanent, se.Kind)
}
