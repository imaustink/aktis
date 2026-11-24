package callertools

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// While pre-0056 TypeScript points exist, the Go point (pointID) and the TS
// point (legacyPointID) can both carry the same hash. Search must return that
// tool once, at its best rank, and the repeat must not take a top-k slot.
func TestResolveHitsReturnsEachHashOnce(t *testing.T) {
	alpha := mustTool(t, "alpha_search", "Search the alpha corpus")
	beta := mustTool(t, "beta_lookup", "Look up a beta record")
	gamma := mustTool(t, "gamma_fetch", "Fetch a gamma document")
	tools := []Descriptor{alpha, beta, gamma}

	hits := []string{beta.Hash, beta.Hash, alpha.Hash, gamma.Hash}
	require.Equal(t, []Descriptor{beta, alpha}, resolveHits(hits, tools, 2))
	require.Equal(t, []Descriptor{beta, alpha, gamma}, resolveHits(hits, tools, 5))
}

func TestResolveHitsDropsHashesNotInTheRequest(t *testing.T) {
	alpha := mustTool(t, "alpha_search", "Search the alpha corpus")
	other := mustTool(t, "other_tool", "Not in this request")
	require.Equal(t, []Descriptor{alpha}, resolveHits([]string{other.Hash, alpha.Hash}, []Descriptor{alpha}, 5))
}
