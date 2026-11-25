package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/activities"
	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/workflows"
)

type fakeSchedules struct {
	createErr error
	created   []client.ScheduleOptions
	handle    *fakeScheduleHandle
}

func (f *fakeSchedules) Create(_ context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error) {
	f.created = append(f.created, options)
	return nil, f.createErr
}

func (f *fakeSchedules) GetHandle(_ context.Context, id string) client.ScheduleHandle {
	f.handle.id = id
	return f.handle
}

// fakeScheduleHandle implements only Update; the rest panic if called.
type fakeScheduleHandle struct {
	client.ScheduleHandle
	id       string
	existing client.Schedule
	updated  *client.Schedule
}

func (h *fakeScheduleHandle) Update(_ context.Context, options client.ScheduleUpdateOptions) error {
	update, err := options.DoUpdate(client.ScheduleUpdateInput{Description: client.ScheduleDescription{Schedule: h.existing}})
	if err != nil {
		return err
	}
	h.updated = update.Schedule
	return nil
}

func TestPruneScheduleIsCreatedOncePerQueueAndCollection(t *testing.T) {
	schedules := &fakeSchedules{}
	require.NoError(t, EnsureCallerToolPruneSchedule(context.Background(), schedules, "aktis", "caller_tools", 6*time.Hour, 30*24*time.Hour))

	require.Len(t, schedules.created, 1)
	got := schedules.created[0]
	require.Equal(t, "aktis/caller-tool-prune:caller_tools", got.ID)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, got.Overlap)
	require.Equal(t, []client.ScheduleIntervalSpec{{Every: 6 * time.Hour}}, got.Spec.Intervals)
	action := got.Action.(*client.ScheduleWorkflowAction)
	require.Equal(t, workflows.CallerToolPruneWorkflowName, action.Workflow)
	require.Equal(t, "aktis", action.TaskQueue)
	require.Equal(t, []any{activities.PruneCallerToolsInput{Collection: "caller_tools", RetentionSeconds: 30 * 24 * 3600}}, action.Args)
}

// A schedule another replica made is brought up to this one's settings, not
// duplicated and not left on stale ones.
func TestPruneScheduleThatExistsIsUpdatedInPlace(t *testing.T) {
	schedules := &fakeSchedules{
		createErr: temporal.ErrScheduleAlreadyRunning,
		handle: &fakeScheduleHandle{existing: client.Schedule{
			Spec:  &client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: time.Minute}}},
			State: &client.ScheduleState{Note: "kept"},
		}},
	}
	require.NoError(t, EnsureCallerToolPruneSchedule(context.Background(), schedules, "aktis", "caller_tools", 6*time.Hour, time.Hour))

	require.Equal(t, "aktis/caller-tool-prune:caller_tools", schedules.handle.id)
	updated := schedules.handle.updated
	require.NotNil(t, updated)
	require.Equal(t, []client.ScheduleIntervalSpec{{Every: 6 * time.Hour}}, updated.Spec.Intervals)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, updated.Policy.Overlap)
	require.Equal(t, "kept", updated.State.Note)
	action := updated.Action.(*client.ScheduleWorkflowAction)
	require.Equal(t, []any{activities.PruneCallerToolsInput{Collection: "caller_tools", RetentionSeconds: 3600}}, action.Args)
}

func TestPruneScheduleSurfacesOtherCreateErrors(t *testing.T) {
	schedules := &fakeSchedules{createErr: errors.New("namespace not found")}
	err := EnsureCallerToolPruneSchedule(context.Background(), schedules, "aktis", "caller_tools", time.Hour, time.Hour)
	require.ErrorContains(t, err, "namespace not found")
}
