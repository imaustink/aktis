package workflows_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/agentrun"
	"github.com/aktisai/orchestrator/engines/temporal/internal/attachments"
	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/activities"
	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/workflows"
)

var cookCaller = activities.Caller{Subject: "user:1", Roles: []string{"cook"}}

// A skill turn's images reach the planner as Refs, and the request text says
// they exist (for retrieval, routing and the transcript, which cannot see them).
func TestImagesReachTheSkillPlanner(t *testing.T) {
	le := newLoopEnv(t)
	le.skills = []catalog.SkillDescriptor{recipesSkillTools().Skill}
	le.selected = "recipes"
	le.skillTools = recipesSkillTools()
	le.plans = []activities.PlannedAction{{Action: activities.ActionRespond, Response: "Got it."}}

	photo := attachments.Ref{Key: "attachments/s/1.png", MediaType: "image/png"}
	var result workflows.TurnResult
	le.sendTurnInput(t, "turn-1", workflows.TurnInput{
		Message: "Extract this recipe: https://example.com/r", Caller: cookCaller, Images: []attachments.Ref{photo},
	}, &result, time.Millisecond)

	le.env.ExecuteWorkflow(workflows.ConversationWorkflowName, (*workflows.ConversationState)(nil))
	require.True(t, le.env.IsWorkflowCompleted())
	require.NoError(t, le.env.GetWorkflowError())

	require.Equal(t, "Got it.", result.Reply)
	require.NotEmpty(t, le.planInputs)
	require.Equal(t, []attachments.Ref{photo}, le.planInputs[0].Images)
	require.True(t, strings.HasSuffix(le.planInputs[0].Request, attachments.Note(1)), le.planInputs[0].Request)
}

// A follow-up turn still shows the model an earlier turn's stored image.
func TestEarlierImagesStayInContext(t *testing.T) {
	le := newLoopEnv(t)
	le.needsCapability = false // bare path: CompleteTurn

	first := attachments.Ref{Key: "attachments/s/1.png", MediaType: "image/png"}
	second := attachments.Ref{URL: "https://example.com/2.jpg"}
	var r1, r2, r3 workflows.TurnResult
	le.sendTurnInput(t, "turn-1", workflows.TurnInput{Message: "what is this?", Caller: cookCaller, Images: []attachments.Ref{first}}, &r1, time.Millisecond)
	le.sendTurnInput(t, "turn-2", workflows.TurnInput{Message: "and this?", Caller: cookCaller, Images: []attachments.Ref{second}}, &r2, time.Second)
	le.sendTurnInput(t, "turn-3", workflows.TurnInput{Message: "compare them", Caller: cookCaller}, &r3, 2*time.Second)

	le.env.ExecuteWorkflow(workflows.ConversationWorkflowName, (*workflows.ConversationState)(nil))
	require.True(t, le.env.IsWorkflowCompleted())
	require.NoError(t, le.env.GetWorkflowError())

	require.Len(t, le.completeTurnInputs, 3)
	require.Equal(t, []attachments.Ref{first}, le.completeTurnInputs[0].Images)
	require.Empty(t, le.completeTurnInputs[0].ContextImages)
	// This turn's image is resolved strictly; the earlier one best-effort.
	require.Equal(t, []attachments.Ref{second}, le.completeTurnInputs[1].Images)
	require.Equal(t, []attachments.Ref{first}, le.completeTurnInputs[1].ContextImages)
	// An earlier EXTERNAL image is not re-sent: the provider rejects a whole
	// request over one unfetchable URL, and the engine cannot check it.
	require.Empty(t, le.completeTurnInputs[2].Images)
	require.Equal(t, []attachments.Ref{first}, le.completeTurnInputs[2].ContextImages)
	// An image-only follow-up carries no note; only image-bearing messages do.
	require.Equal(t, "compare them", le.completeTurnInputs[2].Messages[0].Content)
}

// An agent pod gets its goal as text, so the images reach it as presigned
// download links appended to the goal.
func TestImagesReachADelegatedAgentAsLinks(t *testing.T) {
	le := newLoopEnv(t)
	agent := bridgedAgent()
	le.resolvedAgent = &agent

	var presigned [][]attachments.Ref
	le.env.RegisterActivityWithOptions(func(_ context.Context, refs []attachments.Ref) ([]string, error) {
		presigned = append(presigned, refs)
		urls := make([]string, len(refs))
		for i, r := range refs {
			urls[i] = "https://signed.example/" + r.Key
		}
		return urls, nil
	}, registerOpts(activities.PresignAttachmentsActivityName))

	photo := attachments.Ref{Key: "attachments/s/1.png", MediaType: "image/png"}
	var result workflows.TurnResult
	le.sendTurnInput(t, "turn-1", workflows.TurnInput{
		Message: "Fix the layout bug in this screenshot", Caller: activities.Caller{Subject: "svc:gw", Roles: []string{"agent"}},
		ForcedAgentID: agent.ID, Images: []attachments.Ref{photo},
	}, &result, time.Millisecond)
	le.deliverUp(agentrun.UpMessage{Seq: 1, Type: agentrun.UpReady}, time.Second)
	le.deliverUp(agentrun.UpMessage{Seq: 2, Type: agentrun.UpReply, Final: true, Message: "Fixed."}, 2*time.Second)

	le.env.ExecuteWorkflow(workflows.ConversationWorkflowName, (*workflows.ConversationState)(nil))
	require.True(t, le.env.IsWorkflowCompleted())
	require.NoError(t, le.env.GetWorkflowError())

	require.Equal(t, "Fixed.", result.Reply)
	require.Equal(t, [][]attachments.Ref{{photo}}, presigned)
	require.Len(t, le.agentRunLaunches, 1)
	require.Contains(t, le.agentRunLaunches[0].Goal, "https://signed.example/attachments/s/1.png")
	require.Contains(t, le.agentRunLaunches[0].Goal, "<attached_images>")
}
