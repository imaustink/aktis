package workflows

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/activities"
)

func TestCallerToolPruneWorkflowPrunesTheNamedCollection(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	var got activities.PruneCallerToolsInput
	env.RegisterActivityWithOptions(func(_ context.Context, in activities.PruneCallerToolsInput) (int, error) {
		got = in
		return 4, nil
	}, activity.RegisterOptions{Name: activities.PruneCallerToolsActivityName})

	in := activities.PruneCallerToolsInput{Collection: "caller_tools", RetentionSeconds: 60}
	env.ExecuteWorkflow(CallerToolPruneWorkflow, in)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var removed int
	require.NoError(t, env.GetWorkflowResult(&removed))
	require.Equal(t, 4, removed)
	require.Equal(t, in, got)
}

func TestCallerToolPruneWorkflowRetriesThenFails(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	calls := 0
	env.RegisterActivityWithOptions(func(context.Context, activities.PruneCallerToolsInput) (int, error) {
		calls++
		return 0, errors.New("qdrant unavailable")
	}, activity.RegisterOptions{Name: activities.PruneCallerToolsActivityName})

	env.ExecuteWorkflow(CallerToolPruneWorkflow, activities.PruneCallerToolsInput{Collection: "c", RetentionSeconds: 1})
	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "qdrant unavailable")
	require.Equal(t, 3, calls)
}
