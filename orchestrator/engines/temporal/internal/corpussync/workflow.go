package corpussync

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// One Corpus's sync loop (docs/adr/0058 §2, in Go since docs/adr/0059): a
// full reconcile pass every reconcileInterval, a partial pass for what
// webhooks name in between, and a full pass on demand. One workflow per
// Corpus, so its passes never overlap however many workers run: two
// concurrent full passes can each conclude the other's fresh chunks are
// absent, and a full pass deletes what it believes absent.
//
// The reconcile is the source of truth (ADR 0038 §4); a webhook only adds
// work, and what one names while a pass runs is kept for the next pass.
//
// CHANGING THIS WORKFLOW: it runs for as long as its Corpus exists, and a new
// pod replays it from its history. A change to the commands it issues -- an
// activity, timer, signal or update handler added, removed or reordered --
// must keep the old path behind workflow.GetVersion. replay_test.go replays
// the committed histories in testdata/histories and fails on a change that is
// not gated; fix the change, don't re-capture them.

const (
	// RecheckInterval is how often a waiting loop re-reads its Corpus, so an
	// edited interval or a deleted Corpus is seen without waiting out the old
	// interval.
	RecheckInterval = 5 * time.Minute
	// WebhookSettle gathers a burst of deliveries into one partial pass.
	WebhookSettle = 5 * time.Second
	// cyclesPerRun bounds a long-lived Corpus's history.
	cyclesPerRun = 200
)

// settingsOptions: reading a Corpus is cheap, and retried for a few minutes
// (across a rollout, or an API server blip) before the loop gives up.
// Retry options are not replayed, so they may change without a version gate.
var settingsOptions = workflow.ActivityOptions{
	StartToCloseTimeout: time.Minute,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 10, MaximumInterval: time.Minute},
}

// passOptions: a pass runs as long as a large corpus needs, heartbeating, so
// a worker that dies mid-pass is noticed within the heartbeat timeout and the
// pass retried (from the start) elsewhere. Every rolling restart a pass spans
// costs it an attempt, so it has enough to outlast several, within an overall
// budget; backoff is capped so a source that is down is retried every few
// minutes, not hammered. A pass heartbeats between fetches, not during one, so
// the heartbeat timeout sits above the source's per-fetch HTTP timeout (5m, see
// NewHTTPSource): a slow fetch of a large PDF must not fail every attempt.
var passOptions = workflow.ActivityOptions{
	StartToCloseTimeout:    6 * time.Hour,
	ScheduleToCloseTimeout: 12 * time.Hour,
	HeartbeatTimeout:       6 * time.Minute,
	RetryPolicy: &temporal.RetryPolicy{
		MaximumAttempts:    10,
		InitialInterval:    10 * time.Second,
		BackoffCoefficient: 3,
		MaximumInterval:    5 * time.Minute,
	},
}

type passOutcome struct {
	n      int
	result *PassResult
	err    string
}

// CorpusSyncWorkflow is registered as WorkflowName.
func CorpusSyncWorkflow(ctx workflow.Context, in Input) error {
	corpus := in.Corpus
	logger := workflow.GetLogger(ctx)

	// Pending source ids in arrival order: workflow code must be
	// deterministic, and map iteration is not.
	var pending []string
	pendingSet := map[string]bool{}
	addPending := func(ids []string) {
		for _, id := range ids {
			if !pendingSet[id] {
				pendingSet[id] = true
				pending = append(pending, id)
			}
		}
	}
	addPending(in.PendingSourceIDs)
	fullRequested := in.FullRequested
	lastFullPassAt := in.LastFullPassAt
	// Passes started in this run; a run-now waits for the next one.
	started := 0
	var finished *passOutcome
	// Why the loop ended, once the Corpus is no longer synced.
	ended := ""

	onSourcesChanged := func(ids []string) {
		// Empty means "something changed, we do not know what": a full pass,
		// or a deletion that sent no event would never be noticed.
		if len(ids) == 0 {
			fullRequested = true
			return
		}
		addPending(ids)
	}
	signals := workflow.GetSignalChannel(ctx, SourcesChangedSignal)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var ids []string
			signals.Receive(ctx, &ids)
			onSourcesChanged(ids)
		}
	})

	// Answers with a full pass that STARTS after the request: one already
	// running may have listed the source before whatever prompted it.
	if err := workflow.SetUpdateHandler(ctx, SyncNowUpdate, func(ctx workflow.Context) (PassResult, error) {
		n := started + 1
		fullRequested = true
		if err := workflow.Await(ctx, func() bool {
			return ended != "" || (finished != nil && finished.n >= n)
		}); err != nil {
			return PassResult{}, err
		}
		if finished != nil && finished.n >= n {
			if finished.result != nil {
				return *finished.result, nil
			}
			return PassResult{}, temporal.NewNonRetryableApplicationError(finished.err, ErrTypeSyncFailed, nil)
		}
		return PassResult{}, temporal.NewNonRetryableApplicationError(ended, ErrTypeCorpusNotSynced, nil)
	}); err != nil {
		return err
	}

	settingsCtx := workflow.WithActivityOptions(ctx, settingsOptions)
	passCtx := workflow.WithActivityOptions(ctx, passOptions)
	readTarget := func() (*TargetInfo, error) {
		var target *TargetInfo
		err := workflow.ExecuteActivity(settingsCtx, SyncTargetActivityName, corpus).Get(ctx, &target)
		return target, err
	}
	now := func() int64 { return workflow.Now(ctx).UnixMilli() }

	target, err := readTarget()
	if err != nil {
		return err
	}
	// A loop started afresh schedules from the last full pass the Corpus's
	// status records, rather than passing at once.
	if lastFullPassAt == 0 && target != nil && target.LastReconcileAt != 0 {
		lastFullPassAt = target.LastReconcileAt
	}

	for cycle := 0; target != nil; cycle++ {
		if (cycle >= cyclesPerRun || workflow.GetInfo(ctx).GetContinueAsNewSuggested()) && workflow.AllHandlersFinished(ctx) {
			// Signals delivered but not yet handed to the receiver above
			// would be lost with this run.
			for {
				var ids []string
				if !signals.ReceiveAsync(&ids) {
					break
				}
				onSourcesChanged(ids)
			}
			return workflow.NewContinueAsNewError(ctx, WorkflowName, Input{
				Corpus:           corpus,
				LastFullPassAt:   lastFullPassAt,
				PendingSourceIDs: pending,
				FullRequested:    fullRequested,
			})
		}

		dueAt := now()
		if lastFullPassAt != 0 {
			dueAt = lastFullPassAt + target.IntervalMs
		}
		due := now() >= dueAt
		if !due && !fullRequested && len(pending) == 0 {
			wait := min(time.Duration(dueAt-now())*time.Millisecond, RecheckInterval)
			signalled, err := workflow.AwaitWithTimeout(ctx, wait, func() bool { return fullRequested || len(pending) > 0 })
			if err != nil {
				return err
			}
			// Woken by the clock before the pass is due: re-read the Corpus,
			// so an edited interval applies and a deleted one ends its loop.
			if !signalled && now() < dueAt {
				if target, err = readTarget(); err != nil {
					return err
				}
			}
			continue
		}

		full := due || fullRequested
		// A partial pass waits a moment, so a burst becomes one pass.
		if !full {
			if err := workflow.Sleep(ctx, WebhookSettle); err != nil {
				return err
			}
		}
		// A full pass examines everything, the named resources included;
		// anything named from here on is for the next pass.
		isFull := full || fullRequested
		pass := RunPassInput{Corpus: corpus, Full: isFull}
		if isFull {
			fullRequested = false
		} else {
			pass.SourceIDs = pending
		}
		pending, pendingSet = nil, map[string]bool{}

		started++
		n := started
		var result *PassResult
		err := workflow.ExecuteActivity(passCtx, RunPassActivityName, pass).Get(ctx, &result)
		if err == nil {
			if result == nil {
				break
			}
			finished = &passOutcome{n: n, result: result}
		} else {
			if temporal.IsCanceledError(err) {
				return err
			}
			// Retried already (see passOptions). A failed partial pass's
			// resources are the next full pass's: the reconcile is the
			// backstop for exactly this.
			message := failureMessage(err)
			finished = &passOutcome{n: n, err: message}
			kind := "partial"
			if isFull {
				kind = "full"
			}
			logger.Error(fmt.Sprintf("sync %s: %s pass failed: %s", corpus, kind, message))
		}
		// A failed full pass also waits out the interval: retrying at once
		// would hammer a source that is down.
		if isFull {
			lastFullPassAt = now()
		}
		if target, err = readTarget(); err != nil {
			return err
		}
	}

	ended = fmt.Sprintf("corpus %s is no longer synced", corpus)
	return workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
}

// failureMessage is what went wrong inside an activity's failure, rather than
// the activity wrapper's own description of having failed.
func failureMessage(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Message()
	}
	if cause := errors.Unwrap(err); cause != nil {
		return cause.Error()
	}
	return err.Error()
}
