package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/activities"
	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/workflows"
)

// ScheduleClient is the part of client.ScheduleClient the prune schedule
// uses; client.Client.ScheduleClient() satisfies it.
type ScheduleClient interface {
	Create(ctx context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error)
	GetHandle(ctx context.Context, scheduleID string) client.ScheduleHandle
}

// CallerToolPruneScheduleID is the one schedule per task queue and
// collection: every gateway replica asks for the same id, so there is one
// prune per interval however many replicas run (docs/adr/0058 §4). The task
// queue prefix keeps two releases sharing a namespace apart, as ADR 0057 §5
// does for the gateway's workflows.
func CallerToolPruneScheduleID(taskQueue, collection string) string {
	return taskQueue + "/caller-tool-prune:" + collection
}

// EnsureCallerToolPruneSchedule creates the prune schedule, or brings an
// existing one's interval, retention and task queue up to date. Overlap is
// skipped: a pass still running when the next is due needs no company.
func EnsureCallerToolPruneSchedule(ctx context.Context, schedules ScheduleClient, taskQueue, collection string, every, retention time.Duration) error {
	id := CallerToolPruneScheduleID(taskQueue, collection)
	spec := client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: every}}}
	action := &client.ScheduleWorkflowAction{
		ID:        id,
		Workflow:  workflows.CallerToolPruneWorkflowName,
		TaskQueue: taskQueue,
		Args: []any{activities.PruneCallerToolsInput{
			Collection:       collection,
			RetentionSeconds: int64(retention.Seconds()),
		}},
		WorkflowExecutionTimeout: time.Hour,
	}
	_, err := schedules.Create(ctx, client.ScheduleOptions{
		ID:      id,
		Spec:    spec,
		Action:  action,
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	})
	if err == nil {
		return nil
	}
	if !errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return fmt.Errorf("create schedule %s: %w", id, err)
	}
	// Another replica (or an earlier release) made it. Bring it in line with
	// this one's settings: during a rollout the last to start wins, which is
	// the newer release.
	return schedules.GetHandle(ctx, id).Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			schedule := in.Description.Schedule
			schedule.Spec = &spec
			schedule.Action = action
			if schedule.Policy == nil {
				schedule.Policy = &client.SchedulePolicies{}
			}
			schedule.Policy.Overlap = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
			return &client.ScheduleUpdate{Schedule: &schedule}, nil
		},
	})
}
