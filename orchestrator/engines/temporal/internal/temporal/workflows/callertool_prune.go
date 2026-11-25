package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/activities"
)

// CallerToolPruneWorkflowName is one pass over the caller-tool collection,
// started by a Temporal Schedule the gateway keeps (docs/adr/0058 §4). The
// schedule, not a ticker in each gateway replica, is what makes it one pass
// per interval however many replicas run.
const CallerToolPruneWorkflowName = "CallerToolPruneWorkflow"

// CallerToolPruneWorkflow runs the prune activity. It is the whole workflow:
// a schedule action has to be a workflow, and the work itself is one
// idempotent call.
func CallerToolPruneWorkflow(ctx workflow.Context, in activities.PruneCallerToolsInput) (int, error) {
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// ScheduleToClose, not only StartToClose: a worker without Qdrant
		// never registers the activity, and the pass must give up rather
		// than wait for one forever.
		ScheduleToCloseTimeout: 30 * time.Minute,
		StartToCloseTimeout:    10 * time.Minute,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var removed int
	if err := workflow.ExecuteActivity(actx, activities.PruneCallerToolsActivityName, in).Get(ctx, &removed); err != nil {
		return 0, err
	}
	if removed > 0 {
		workflow.GetLogger(ctx).Info("caller-tool prune reclaimed definitions", "removed", removed, "collection", in.Collection)
	}
	return removed, nil
}
