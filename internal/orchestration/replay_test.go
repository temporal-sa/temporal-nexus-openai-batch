package orchestration

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// The fixture is a complete, real pre-V2 fake-provider execution. Worker/host
// identities, execution UUIDs, logical run IDs and fake provider IDs have been
// replaced consistently, including inside JSON payloads. Event timestamps and
// the recorded command sequence are unchanged. Replay is entirely offline.
func TestLegacyBatchWorkflowHistoryReplaysAlongsideV2(t *testing.T) {
	file, err := os.Open("testdata/legacy_batch.json")
	require.NoError(t, err)
	defer file.Close()
	history, err := client.HistoryFromJSON(file, client.HistoryJSONOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, history.Events)
	start := history.Events[0].GetWorkflowExecutionStartedEventAttributes()
	require.NotNil(t, start)
	require.Equal(t, "BatchWorkflow", start.WorkflowType.Name)
	require.Equal(t, enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, history.Events[len(history.Events)-1].EventType)
	var ref model.BatchRef
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(start.Input, &ref))
	require.Equal(t, "legacy-replay", ref.RunID)
	require.Equal(t, "legacy-replay-00000001", ref.BatchID)
	require.Equal(t, "fake", ref.Config.Mode)

	// Register the same two distinct type names used by the service. Registering
	// V2 must not replace the implementation needed by historical executions.
	require.Equal(t, "BatchWorkflowV2", BatchWorkflowType)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(BatchWorkflowLegacy, workflow.RegisterOptions{Name: "BatchWorkflow"})
	replayer.RegisterWorkflowWithOptions(BatchWorkflowV2, workflow.RegisterOptions{Name: BatchWorkflowType})
	require.NoError(t, replayer.ReplayWorkflowHistory(nil, history))
}
