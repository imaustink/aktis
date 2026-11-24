package workflows

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/authz"
)

// docs/adr/0055: only the agent a trigger forced reports the trigger's
// outcome, and an identity-link resume keeps the one it parked with.
func TestLaunchOutcome(t *testing.T) {
	forced := TurnInput{ForcedAgentID: "swe", Outcome: "coordination"}
	require.Equal(t, "coordination", launchOutcome(forced, "swe", nil))
	require.Empty(t, launchOutcome(forced, "other", nil), "a planner-chosen agent keeps the default")
	require.Empty(t, launchOutcome(TurnInput{Outcome: "coordination"}, "swe", nil), "no forced agent, no outcome")

	// The resume is a different turn: its own trigger fields say nothing
	// about the parked agent.
	resume := TurnInput{Message: "done, linked it"}
	require.Equal(t, "coordination", launchOutcome(resume, "swe", &authz.PendingLink{Outcome: "coordination"}))
	require.Empty(t, launchOutcome(forced, "swe", &authz.PendingLink{}))
}
