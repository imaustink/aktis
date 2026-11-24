package workflows

import (
	"encoding/json"
	"errors"
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/aktisai/orchestrator/engines/temporal/internal/attachments"
	"github.com/aktisai/orchestrator/engines/temporal/internal/authz"
	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
	"github.com/aktisai/orchestrator/engines/temporal/internal/continuation"
	"github.com/aktisai/orchestrator/engines/temporal/internal/identitylink"
	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/activities"
)

// agentActsOnGitHub reports whether an agent's runs work in a GitHub
// repository, and so must pass the read gate: declaring the github provider
// is what makes a run read as its caller (ADR 0041).
func agentActsOnGitHub(agent catalog.AgentDescriptor) bool {
	for _, p := range agent.IdentityProviders {
		if p == identitylink.ProviderGitHub {
			return true
		}
	}
	return false
}

// withImageLinks appends download links for refs to an agent's text. An agent
// pod receives its goal and follow-up prompts as text only, so this is how a
// user's image reaches it. A presign failure fails the turn: an agent told
// about images it cannot fetch would act as if it had seen them.
func withImageLinks(ctx workflow.Context, actx workflow.Context, text string, refs []attachments.Ref) (string, error) {
	if len(refs) == 0 {
		return text, nil
	}
	var urls []string
	if err := workflow.ExecuteActivity(actx, activities.PresignAttachmentsActivityName, refs).Get(ctx, &urls); err != nil {
		return "", fmt.Errorf("prepare image attachments for the agent: %w", err)
	}
	return text + attachments.DownloadBlock(urls), nil
}

// delegateToAgent starts a fresh agent episode as a child workflow and
// relays its first non-progress up-signal into the turn: a question becomes
// the reply with the episode left active for the next turn; a final message
// closes the episode and banks the agent's continuation token.
func delegateToAgent(ctx workflow.Context, actx workflow.Context, state *ConversationState, in TurnInput, agent catalog.AgentDescriptor, meta *TurnMeta, note func(string), pending *authz.PendingLink) (string, TurnMeta, error) {
	meta.Path = "agent"
	meta.AgentID = agent.ID

	// Which repository a GitHub-acting agent will work in, for a chat caller
	// whose request did not arrive with one. A webhook turn's repository comes
	// from its event and nowhere else: its sender was verified on THAT
	// repository, so a different one read out of the prompt would be
	// unverified.
	if agentActsOnGitHub(agent) && in.TargetRepository == "" && in.Caller.PerUser {
		var repo string
		if err := workflow.ExecuteActivity(actx, activities.ExtractTargetRepositoryActivityName, activities.ExtractTargetRepositoryInput{
			Request: in.Message,
		}).Get(ctx, &repo); err != nil {
			return "", *meta, fmt.Errorf("extract target repository for %s: %w", agent.ID, err)
		}
		in.TargetRepository = repo
	}

	// Authorization pre-flight, before anything is launched (upstream ADR
	// 0030). Plain control flow: no model call reaches this decision, and
	// nothing downstream can skip it.
	verdict, launch, err := authorizeAgent(ctx, actx, in, agent, pending)
	if err != nil {
		return "", *meta, fmt.Errorf("authorize %s: %w", agent.ID, err)
	}
	switch verdict.Kind {
	case authz.KindAuthorized:
		// Adopt the principal the credentials were actually keyed by. The
		// pre-flight may have UPGRADED it this turn; without adopting it,
		// anything that later re-derives the key would invalidate a record
		// that was never written and leave the caller re-reading a dead
		// credential forever.
		if verdict.Principal != "" {
			in.Caller.Principal = verdict.Principal
		}
		state.PendingIdentityLink = nil
	case authz.KindLinkRequired:
		meta.Path = "link-required"
		note("Waiting for an account link")
		if verdict.Pending != nil {
			anchor := *verdict.Pending
			// Capture the goal, not the message that eventually notices the
			// link landed. Without this the resume re-delegates "ok, linked
			// it" and the user's actual request is lost.
			anchor.Request = in.Message
			anchor.ReadOnly = in.ReadOnly
			anchor.IdentityOverride = in.IdentityOverride
			anchor.Outcome = launchOutcome(in, agent.ID, pending)
			anchor.RequestedBy = &authz.LinkOwner{
				Subject:     in.Caller.Subject,
				PerUser:     in.Caller.PerUser,
				SenderLogin: in.SenderLogin,
			}
			state.PendingIdentityLink = &anchor
		}
		return verdict.Message, *meta, nil
	case authz.KindRefused:
		// The caller can't read the repository, or named none. Nothing to
		// resume: no link they could complete would change the answer.
		meta.Path = "refused"
		state.PendingIdentityLink = nil
		return verdict.Message, *meta, nil
	default:
		meta.Path = "misconfigured"
		state.PendingIdentityLink = nil
		return "I can't run that agent right now: " + verdict.Error, *meta, nil
	}

	note("Delegating to agent " + agent.ID + "…")

	goal, err := withImageLinks(ctx, actx, in.Message, append(in.Images[:len(in.Images):len(in.Images)], in.ContextImages...))
	if err != nil {
		return "", *meta, err
	}
	if token := state.AgentContinuations[agent.ID]; token != "" {
		goal = continuation.Prepend(token, goal)
	}

	childID, err := newChildAgentID(ctx, agent.ID)
	if err != nil {
		return "", *meta, err
	}
	cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: childID})
	child := workflow.ExecuteChildWorkflow(cctx, agentWorkflowNameFor(agent), AgentWorkflowInput{
		Agent:            agent,
		Goal:             goal,
		Caller:           in.Caller,
		ParentWorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		Depth:            1,
		// A reference, not credentials: the child attaches it to the Jobs it
		// launches, and the kubelet is the only thing that reads a value.
		Credentials: credentials{SecretName: verdict.SecretName, EnvVars: verdict.EnvVarNames},
		ReadOnly:    in.ReadOnly,
		Params:      in.Params,
		// Only the agent the trigger forced reports the trigger's outcome;
		// a planner-chosen delegation in the same turn keeps the default.
		Outcome: launchOutcome(in, agent.ID, pending),
		Launch:  launch,
	})
	// Wait for the start (not completion): the update handler returns while
	// the child keeps running under this conversation. ParentClosePolicy
	// (default TERMINATE) reaps abandoned episodes when the conversation
	// completes.
	if err := child.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		return "", *meta, fmt.Errorf("start agent %s: %w", agent.ID, err)
	}
	state.ActiveAgentID = agent.ID
	state.ActiveAgentWorkflowID = childID
	state.ActiveAgentReadOnly = in.ReadOnly

	gen := state.beginAgentListen(ctx)
	reply, outcome, preempted, failed := handleAgentUp(ctx, state, agent.ID, childID, gen, note)
	meta.Outcome = outcome
	if err := forcedAgentFailure(in, agent.ID, failed, reply); err != nil {
		return "", *meta, err
	}
	if preempted {
		// Vanishingly unlikely for the call that JUST started this episode
		// (nothing else could have raced it in), but handled the same way
		// as agentloop.go's re-attach call for the same reason: whichever
		// call actually owns the current generation is responsible for
		// finishing this exchange, never two at once.
		reply = "A newer message is already being processed for this conversation."
	}
	return reply, *meta, nil
}

// handleAgentUp pumps the active child's up-signals until something ends
// the turn: progress lines feed the narration; a question ends the turn
// with the episode still active; final/failed/timeout close the episode.
//
// myGeneration is this call's stake in ConversationState.AgentListenGeneration
// (from beginAgentListen) — see its doc comment. preempted=true means a
// LATER call took over this episode's up-signal channel before (or while)
// this one was still waiting; the caller must not act on the empty message
// (no clearActive, no reply content) since the newer call owns finishing
// this exchange.
//
// outcome is the final reply's runner outcome (docs/adr/0052 §2), set only
// when the episode concluded with a Final message carrying one.
func handleAgentUp(ctx workflow.Context, state *ConversationState, agentID, childID string, myGeneration int, note func(string)) (message string, outcome json.RawMessage, preempted, failed bool) {
	upCh := workflow.GetSignalChannel(ctx, AgentUpSignalPrefix+childID)
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	defer cancelTimer()
	timer := workflow.NewTimer(timerCtx, agentEpisodeTimeout)

	clearActive := func() {
		state.ActiveAgentID, state.ActiveAgentWorkflowID = "", ""
	}

	for {
		// Checked every pass, not just on a preempt-channel wake: a
		// generation bump that raced ahead of this loop even starting (or
		// ahead of the preempt channel delivering) must still be caught here
		// before this call touches upCh again.
		if state.AgentListenGeneration != myGeneration {
			return "", nil, true, false
		}

		var (
			u          AgentUp
			received   bool
			timedOut   bool
			superseded bool
		)
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(upCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &u)
			received = true
		})
		selector.AddFuture(timer, func(workflow.Future) { timedOut = true })
		selector.AddReceive(state.agentListenPreempt, func(c workflow.ReceiveChannel, _ bool) {
			var gen int
			c.Receive(ctx, &gen)
			superseded = true
		})
		selector.Select(ctx)

		if superseded {
			// Might be a notification meant for an even earlier call than
			// this one; the generation check at the top of the loop is the
			// actual authority, so just loop back to it rather than trusting
			// this wake-up alone.
			continue
		}
		if timedOut {
			clearActive()
			_ = workflow.RequestCancelExternalWorkflow(ctx, childID, "").Get(ctx, nil)
			return fmt.Sprintf("Agent %s didn't respond within %s; I've cancelled it.", agentID, agentEpisodeTimeout), nil, false, true
		}
		if !received {
			continue
		}
		switch {
		case u.Progress:
			note(u.Message)
		case u.Failed:
			clearActive()
			return fmt.Sprintf("Agent %s failed (%s): %s", agentID, u.Code, u.Message), nil, false, true
		case u.Final:
			clearActive()
			if u.Result != "" {
				if state.AgentContinuations == nil {
					state.AgentContinuations = map[string]string{}
				}
				state.AgentContinuations[agentID] = u.Result
			}
			return u.Message, u.Outcome, false, false
		default: // a question — the episode stays active across turns
			note("Agent " + agentID + " needs input")
			return u.Message, nil, false, false
		}
	}
}

// forcedAgentFailure fails the turn when the agent a trigger forced failed
// (or never answered), as runForcedTool does for a forced tool: whoever reads
// the run's outcome -- the run-completion event the factory chains on,
// reply.http, a GitHub result comment -- must see failed, not a reply that
// merely describes the failure. Chat and planner delegation keep answering
// with the failure as text.
func forcedAgentFailure(in TurnInput, agentID string, failed bool, reply string) error {
	if !failed || in.ForcedAgentID == "" || in.ForcedAgentID != agentID {
		return nil
	}
	return errors.New(reply)
}

// launchOutcome is the trigger outcome (docs/adr/0055) an agent launch
// carries: a resumed launch keeps the one its anchor parked with; otherwise
// the turn's, when agentID is the agent its trigger forced, else "".
func launchOutcome(in TurnInput, agentID string, pending *authz.PendingLink) string {
	if pending != nil {
		return pending.Outcome
	}
	if in.ForcedAgentID != "" && in.ForcedAgentID == agentID {
		return in.Outcome
	}
	return ""
}
