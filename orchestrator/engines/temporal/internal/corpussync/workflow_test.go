package corpussync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

var wfStart = time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC)

// scriptedSync stands in for the activities: the Corpus's settings come from
// `info`, and each pass is recorded with the workflow-clock time it ran at.
type scriptedSync struct {
	mu     sync.Mutex
	env    *testsuite.TestWorkflowEnvironment
	info   func(now time.Time) *TargetInfo
	pass   func(in RunPassInput, n int) (*PassResult, error)
	passes []passRecord
	reads  int
}

type passRecord struct {
	at  time.Duration
	in  RunPassInput
	err bool
}

func (s *scriptedSync) syncTarget(_ context.Context, _ string) (*TargetInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	return s.info(s.env.Now()), nil
}

func (s *scriptedSync) runPass(_ context.Context, in RunPassInput) (*PassResult, error) {
	s.mu.Lock()
	n := len(s.passes) + 1
	s.mu.Unlock()
	res, err := &PassResult{Indexed: n, Failed: []string{}, Full: in.Full}, error(nil)
	if s.pass != nil {
		res, err = s.pass(in, n)
	}
	s.mu.Lock()
	s.passes = append(s.passes, passRecord{at: s.env.Now().Sub(wfStart), in: in, err: err != nil})
	s.mu.Unlock()
	return res, err
}

func newSyncEnv(t *testing.T, s *scriptedSync) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(wfStart)
	env.RegisterWorkflowWithOptions(CorpusSyncWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	env.RegisterActivityWithOptions(s.syncTarget, activity.RegisterOptions{Name: SyncTargetActivityName})
	env.RegisterActivityWithOptions(s.runPass, activity.RegisterOptions{Name: RunPassActivityName})
	s.env = env
	return env
}

// syncedUntil keeps the Corpus synced with `interval` until `end`.
func syncedUntil(interval, end time.Duration) func(time.Time) *TargetInfo {
	return func(now time.Time) *TargetInfo {
		if now.Sub(wfStart) >= end {
			return nil
		}
		return &TargetInfo{IntervalMs: interval.Milliseconds()}
	}
}

func (s *scriptedSync) times() []time.Duration {
	var out []time.Duration
	for _, p := range s.passes {
		out = append(out, p.at)
	}
	return out
}

func TestCorpusSyncPassesAtOnceThenOnTheInterval(t *testing.T) {
	s := &scriptedSync{info: syncedUntil(time.Hour, 150*time.Minute)}
	env := newSyncEnv(t, s)
	env.ExecuteWorkflow(WorkflowName, Input{Corpus: "eng"})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []time.Duration{0, time.Hour, 2 * time.Hour}, s.times())
	for _, p := range s.passes {
		require.True(t, p.in.Full)
	}
}

func TestCorpusSyncRereadsAnEditedInterval(t *testing.T) {
	s := &scriptedSync{info: func(now time.Time) *TargetInfo {
		elapsed := now.Sub(wfStart)
		switch {
		case elapsed >= 95*time.Minute: // read at the 95-minute recheck
			return nil
		case elapsed >= 10*time.Minute:
			// Shortened while the loop waited out the first hour: seen at the
			// next five-minute recheck, not after the old interval.
			return &TargetInfo{IntervalMs: (20 * time.Minute).Milliseconds()}
		default:
			return &TargetInfo{IntervalMs: time.Hour.Milliseconds()}
		}
	}}
	env := newSyncEnv(t, s)
	env.ExecuteWorkflow(WorkflowName, Input{Corpus: "eng"})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []time.Duration{0, 20 * time.Minute, 40 * time.Minute, 60 * time.Minute, 80 * time.Minute}, s.times())
}

func TestCorpusSyncKeepsAWebhookThatArrivesMidPassForAPartialPass(t *testing.T) {
	s := &scriptedSync{info: syncedUntil(time.Hour, 50*time.Minute)}
	env := newSyncEnv(t, s)
	s.pass = func(in RunPassInput, n int) (*PassResult, error) {
		if n == 1 {
			// Delivered while the first (full) pass runs.
			env.SignalWorkflow(SourcesChangedSignal, []string{"p2", "p1", "p2"})
		}
		return &PassResult{Failed: []string{}, Full: in.Full}, nil
	}
	env.ExecuteWorkflow(WorkflowName, Input{Corpus: "eng"})
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, s.passes, 2)
	require.True(t, s.passes[0].in.Full)
	require.Equal(t, RunPassInput{Corpus: "eng", SourceIDs: []string{"p2", "p1"}}, s.passes[1].in)
	require.Equal(t, WebhookSettle, s.passes[1].at, "a burst is gathered first")
}

func TestCorpusSyncEscalatesAnEmptyWebhookToAFullPass(t *testing.T) {
	s := &scriptedSync{info: syncedUntil(time.Hour, 50*time.Minute)}
	env := newSyncEnv(t, s)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SourcesChangedSignal, []string{"p1"}) }, 10*time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SourcesChangedSignal, []string{}) }, 10*time.Minute+time.Second)
	env.ExecuteWorkflow(WorkflowName, Input{Corpus: "eng"})
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, s.passes, 2)
	require.Equal(t, RunPassInput{Corpus: "eng", Full: true}, s.passes[1].in, "the named id is covered by the full pass")
}

func TestCorpusSyncAnswersARunNowWithAPassThatStartedAfterIt(t *testing.T) {
	s := &scriptedSync{info: syncedUntil(time.Hour, 50*time.Minute)}
	env := newSyncEnv(t, s)
	var answered PassResult
	var updateErr error
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(SyncNowUpdate, "run-now", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { updateErr = err },
			OnAccept:   func() {},
			OnComplete: func(res interface{}, err error) { answered, updateErr = res.(PassResult), err },
		})
	}, 10*time.Minute)
	env.ExecuteWorkflow(WorkflowName, Input{Corpus: "eng"})
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, updateErr)
	require.Equal(t, []time.Duration{0, 10 * time.Minute}, s.times())
	require.Equal(t, 2, answered.Indexed, "the second pass's result, not the first's")
	require.True(t, answered.Full)
}

func TestCorpusSyncReportsAFailedPassToTheRunNowAndKeepsLooping(t *testing.T) {
	s := &scriptedSync{info: syncedUntil(time.Hour, 60*time.Minute)}
	env := newSyncEnv(t, s)
	s.pass = func(in RunPassInput, n int) (*PassResult, error) {
		if n == 2 {
			return nil, temporal.NewNonRetryableApplicationError("confluence is down", "Boom", nil)
		}
		return &PassResult{Failed: []string{}, Full: in.Full}, nil
	}
	var updateErr error
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(SyncNowUpdate, "run-now", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { updateErr = err },
			OnAccept:   func() {},
			OnComplete: func(_ interface{}, err error) { updateErr = err },
		})
	}, 10*time.Minute)
	env.ExecuteWorkflow(WorkflowName, Input{Corpus: "eng"})
	require.NoError(t, env.GetWorkflowError())
	var app *temporal.ApplicationError
	require.True(t, errors.As(updateErr, &app), "%v", updateErr)
	require.Equal(t, ErrTypeSyncFailed, app.Type())
	require.Contains(t, app.Message(), "confluence is down")
	// The failed full pass waits out its interval like a successful one
	// (the Corpus goes at 60 minutes, before it is due again).
	require.Equal(t, []time.Duration{0, 10 * time.Minute}, s.times())
	require.True(t, s.passes[1].err)
}

func TestCorpusSyncEndsWhenItsCorpusGoesAndAnswersAWaitingRunNow(t *testing.T) {
	s := &scriptedSync{}
	s.info = func(now time.Time) *TargetInfo {
		if s.reads > 1 {
			return nil
		}
		return &TargetInfo{IntervalMs: time.Hour.Milliseconds()}
	}
	env := newSyncEnv(t, s)
	s.pass = func(_ RunPassInput, _ int) (*PassResult, error) { return nil, nil } // gone mid-pass
	var updateErr error
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(SyncNowUpdate, "run-now", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { updateErr = err },
			OnAccept:   func() {},
			OnComplete: func(_ interface{}, err error) { updateErr = err },
		})
	}, 0)
	env.ExecuteWorkflow(WorkflowName, Input{Corpus: "eng"})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var app *temporal.ApplicationError
	require.True(t, errors.As(updateErr, &app), "%v", updateErr)
	require.Equal(t, ErrTypeCorpusNotSynced, app.Type())
}

func TestCorpusSyncFreshLoopSchedulesFromTheLastRecordedFullPass(t *testing.T) {
	last := wfStart.Add(-40 * time.Minute).UnixMilli()
	s := &scriptedSync{}
	s.info = func(now time.Time) *TargetInfo {
		if now.Sub(wfStart) >= 30*time.Minute {
			return nil
		}
		return &TargetInfo{IntervalMs: time.Hour.Milliseconds(), LastReconcileAt: last}
	}
	env := newSyncEnv(t, s)
	env.ExecuteWorkflow(WorkflowName, Input{Corpus: "eng"})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []time.Duration{20 * time.Minute}, s.times(), "due an interval after the status's last full pass")
}

func TestCorpusSyncContinuesAsNewCarryingItsState(t *testing.T) {
	s := &scriptedSync{info: syncedUntil(time.Minute, 1000*time.Hour)}
	env := newSyncEnv(t, s)
	env.ExecuteWorkflow(WorkflowName, Input{Corpus: "eng", PendingSourceIDs: []string{"kept"}})
	err := env.GetWorkflowError()
	var can *workflow.ContinueAsNewError
	require.True(t, errors.As(err, &can), "%v", err)
	require.Equal(t, WorkflowName, can.WorkflowType.Name)
	require.Len(t, s.passes, cyclesPerRun/2, "every other turn is a wait")

	var next Input
	require.NoError(t, converterDecode(can, &next))
	require.Equal(t, "eng", next.Corpus)
	require.NotZero(t, next.LastFullPassAt)
}

func converterDecode(can *workflow.ContinueAsNewError, out any) error {
	return converter.GetDefaultDataConverter().FromPayloads(can.Input, out)
}
