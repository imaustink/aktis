package corpussync

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

// These run against a real Temporal dev server (the `temporal` CLI on PATH,
// else one the SDK downloads), because what they check is the server's:
// workflow-id conflict handling, signal-with-start and updates from a
// separate client (the broker's side), termination, and the histories the
// replay test holds the workflow to.
//
// CAPTURE_CORPUS_SYNC_HISTORIES=1 rewrites testdata/histories from these
// runs. Only for a NEW history covering a new path -- never to make the
// replay test pass after a change; gate the change with GetVersion instead.

var (
	devServerOnce sync.Once
	devServer     *testsuite.DevServer
	devServerErr  error
)

// TestMain stops the dev server, if a test started one: a running child
// holds the test's output open and `go test` fails waiting for it.
func TestMain(m *testing.M) {
	code := m.Run()
	if devServer != nil {
		_ = devServer.Stop()
	}
	os.Exit(code)
}

func devServerClient(t *testing.T) client.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("needs a Temporal dev server")
	}
	devServerOnce.Do(func() {
		opts := testsuite.DevServerOptions{LogLevel: "error"}
		if path, err := exec.LookPath("temporal"); err == nil {
			opts.ExistingPath = path
		}
		devServer, devServerErr = testsuite.StartDevServer(context.Background(), opts)
	})
	require.NoError(t, devServerErr, "start a Temporal dev server")
	return devServer.Client()
}

// liveSync is the activities, scripted per test, on a real worker.
type liveSync struct {
	synced   atomic.Bool
	interval time.Duration
	passes   atomic.Int32
	mu       sync.Mutex
	inputs   []RunPassInput
}

func (l *liveSync) syncTarget(context.Context, string) (*TargetInfo, error) {
	if !l.synced.Load() {
		return nil, nil
	}
	return &TargetInfo{IntervalMs: l.interval.Milliseconds()}, nil
}

func (l *liveSync) runPass(_ context.Context, in RunPassInput) (*PassResult, error) {
	if !l.synced.Load() {
		return nil, nil
	}
	n := l.passes.Add(1)
	l.mu.Lock()
	l.inputs = append(l.inputs, in)
	l.mu.Unlock()
	return &PassResult{Indexed: int(n), Failed: []string{}, Full: in.Full}, nil
}

func (l *liveSync) passInputs() []RunPassInput {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]RunPassInput{}, l.inputs...)
}

func startLiveWorker(t *testing.T, c client.Client, queue string, l *liveSync) {
	t.Helper()
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflowWithOptions(CorpusSyncWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	w.RegisterActivityWithOptions(l.syncTarget, activity.RegisterOptions{Name: SyncTargetActivityName})
	w.RegisterActivityWithOptions(l.runPass, activity.RegisterOptions{Name: RunPassActivityName})
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)
}

func uniqueQueue(t *testing.T) string {
	return fmt.Sprintf("corpus-sync-test-%d", time.Now().UnixNano())
}

func running(t *testing.T, c client.Client, id string) bool {
	t.Helper()
	desc, err := c.DescribeWorkflowExecution(context.Background(), id, "")
	if err != nil {
		return false
	}
	return desc.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}

func TestLiveOneLoopPerCorpusHoweverManyAskAndTheBrokersSignalAndUpdate(t *testing.T) {
	c := devServerClient(t)
	queue := uniqueQueue(t)
	l := &liveSync{interval: time.Hour}
	l.synced.Store(true)
	startLiveWorker(t, c, queue, l)
	ctx := context.Background()

	ensurer := &Ensurer{
		Client: c, TaskQueue: queue,
		Synced: func(context.Context) ([]string, error) { return []string{"eng"}, nil },
		Logf:   t.Logf,
	}
	// Several replicas ensuring at once: still one loop.
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() { defer wg.Done(); ensurer.Tick(ctx) }()
	}
	wg.Wait()
	id := WorkflowID(queue, "eng")
	require.Eventually(t, func() bool { return l.passes.Load() == 1 }, 10*time.Second, 50*time.Millisecond)
	ensurer.Tick(ctx)
	time.Sleep(300 * time.Millisecond)
	require.EqualValues(t, 1, l.passes.Load(), "re-ensuring a running loop starts no pass")

	// A webhook, as the broker sends it: signal-with-start.
	_, err := c.SignalWithStartWorkflow(ctx, id, SourcesChangedSignal, []string{"p1"},
		client.StartWorkflowOptions{ID: id, TaskQueue: queue}, WorkflowName, Input{Corpus: "eng"})
	require.NoError(t, err)

	// A run-now, as the broker sends it: an update whose answer is a full
	// pass that started after the request.
	handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: id, UpdateName: SyncNowUpdate, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	require.NoError(t, err)
	var result PassResult
	require.NoError(t, handle.Get(ctx, &result))
	require.True(t, result.Full)

	inputs := l.passInputs()
	require.GreaterOrEqual(t, len(inputs), 2)
	require.True(t, inputs[len(inputs)-1].Full)
	if len(inputs) == 3 {
		// The partial pass ran before the run-now's full pass.
		require.Equal(t, []string{"p1"}, inputs[1].SourceIDs)
	}

	// The Corpus goes: the loop ends, and a run-now to it is not-synced.
	l.synced.Store(false)
	require.NoError(t, c.SignalWorkflow(ctx, id, "", SourcesChangedSignal, []string{}))
	require.Eventually(t, func() bool { return !running(t, c, id) }, 15*time.Second, 100*time.Millisecond)
}

func TestLiveTheLegacyDrainTerminatesLoopsOnTheOldQueue(t *testing.T) {
	c := devServerClient(t)
	queue, legacy := uniqueQueue(t), uniqueQueue(t)+"-legacy"
	ctx := context.Background()
	// A 0058 loop: nothing polls its queue any more.
	_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: WorkflowID(legacy, "eng"), TaskQueue: legacy}, "corpusSync", map[string]string{"corpus": "eng"})
	require.NoError(t, err)

	l := &liveSync{interval: time.Hour}
	l.synced.Store(true)
	startLiveWorker(t, c, queue, l)
	ensurer := &Ensurer{
		Client: c, TaskQueue: queue, LegacyTaskQueue: legacy,
		Synced: func(context.Context) ([]string, error) { return []string{"eng"}, nil },
		All:    func(context.Context) ([]string, error) { return []string{"eng", "never-had-one"}, nil },
		Logf:   t.Logf,
	}
	ensurer.Tick(ctx)
	require.False(t, running(t, c, WorkflowID(legacy, "eng")))
	require.Eventually(t, func() bool { return running(t, c, WorkflowID(queue, "eng")) }, 5*time.Second, 50*time.Millisecond)
	// A second tick finds nothing left to drain and says nothing.
	ensurer.Tick(ctx)
}

// TestLiveCaptureHistories records the histories replay_test.go replays.
func TestLiveCaptureHistories(t *testing.T) {
	if os.Getenv("CAPTURE_CORPUS_SYNC_HISTORIES") != "1" {
		t.Skip("set CAPTURE_CORPUS_SYNC_HISTORIES=1 to re-record testdata/histories")
	}
	c := devServerClient(t)
	ctx := context.Background()
	queue := uniqueQueue(t)
	l := &liveSync{interval: 2 * time.Second}
	l.synced.Store(true)
	startLiveWorker(t, c, queue, l)

	// The whole life of a loop: a pass at once, interval passes, a webhook's
	// partial pass, an empty webhook's full pass, a run-now, and the end.
	id := WorkflowID(queue, "lifecycle")
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: queue}, WorkflowName, Input{Corpus: "lifecycle"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return l.passes.Load() >= 2 }, 20*time.Second, 50*time.Millisecond)
	l.interval = time.Hour
	time.Sleep(3 * time.Second)
	require.NoError(t, c.SignalWorkflow(ctx, id, "", SourcesChangedSignal, []string{"p1", "p2"}))
	n := l.passes.Load()
	require.Eventually(t, func() bool { return l.passes.Load() > n }, 20*time.Second, 50*time.Millisecond)
	require.NoError(t, c.SignalWorkflow(ctx, id, "", SourcesChangedSignal, []string{}))
	n = l.passes.Load()
	require.Eventually(t, func() bool { return l.passes.Load() > n }, 20*time.Second, 50*time.Millisecond)
	handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: id, UpdateName: SyncNowUpdate, WaitForStage: client.WorkflowUpdateStageCompleted})
	require.NoError(t, err)
	require.NoError(t, handle.Get(ctx, nil))
	l.synced.Store(false)
	require.NoError(t, c.SignalWorkflow(ctx, id, "", SourcesChangedSignal, []string{}))
	require.NoError(t, run.Get(ctx, nil))
	writeHistory(t, c, id, run.GetRunID(), "lifecycle.json")

	// A loop still waiting, as a new pod would find one mid-interval.
	l.synced.Store(true)
	id = WorkflowID(queue, "waiting")
	run, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: queue}, WorkflowName, Input{Corpus: "waiting", PendingSourceIDs: []string{"carried"}})
	require.NoError(t, err)
	n = l.passes.Load()
	require.Eventually(t, func() bool { return l.passes.Load() > n }, 20*time.Second, 50*time.Millisecond)
	time.Sleep(time.Second)
	writeHistory(t, c, id, run.GetRunID(), "waiting.json")
	require.NoError(t, c.TerminateWorkflow(ctx, id, run.GetRunID(), "captured"))
}

func writeHistory(t *testing.T, c client.Client, id, runID, name string) {
	t.Helper()
	iter := c.GetWorkflowHistory(context.Background(), id, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{}
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		history.Events = append(history.Events, event)
	}
	raw, err := protojson.MarshalOptions{Indent: " "}.Marshal(history)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join("testdata", "histories"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join("testdata", "histories", name), append(raw, '\n'), 0o644))
}
