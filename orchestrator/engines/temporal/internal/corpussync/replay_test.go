package corpussync

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Replays the committed histories against the workflow as it is now. A
// failure means a change altered the commands an existing loop issues: every
// Corpus's loop is long-lived, and a new pod replays it, so the change must
// keep the old path behind workflow.GetVersion. Fix the change; don't
// re-capture the histories (devserver_test.go).
func TestCorpusSyncReplaysCommittedHistories(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "histories", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, files, "no committed histories to replay")
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(CorpusSyncWorkflow, workflow.RegisterOptions{Name: WorkflowName})
			require.NoError(t, replayer.ReplayWorkflowHistoryFromJSONFile(nil, file))
		})
	}
}
