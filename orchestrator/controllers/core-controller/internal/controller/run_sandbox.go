/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	toolv1alpha1 "github.com/aktisai/orchestrator/controllers/core-controller/api/v1alpha1"
	"github.com/aktisai/orchestrator/controllers/core-controller/internal/sandboxapi"
)

// ExecutionBackend selects which workload kind a run is executed as.
type ExecutionBackend string

const (
	// BackendJob is the original one-shot batch/v1 Job (ADR 0010). Default.
	BackendJob ExecutionBackend = "job"
	// BackendSandbox executes the run as a kubernetes-sigs/agent-sandbox
	// Sandbox: the same hardened pod spec, but as a suspendable, stateful
	// object with a stable identity instead of a batch Job.
	BackendSandbox ExecutionBackend = "sandbox"
)

// ExecutionBackendAnnotation lets a single catalog entry (Tool, Agent) or run
// (ToolRun, AgentRun) opt into a backend regardless of the controller-wide
// default, so a cluster can migrate one workload at a time rather than
// flipping everything at once. A value on the run wins over one on its Tool or
// Agent, which wins over AGENT_EXECUTION_BACKEND.
const ExecutionBackendAnnotation = "core.aktis.ai/execution-backend"

// defaultExecutionBackend resolves the controller-wide default from
// AGENT_EXECUTION_BACKEND. Anything other than "sandbox" -- including unset
// and any typo -- means Job, so a misconfigured value can never silently move
// production onto the experimental path.
func defaultExecutionBackend() ExecutionBackend {
	if os.Getenv("AGENT_EXECUTION_BACKEND") == string(BackendSandbox) {
		return BackendSandbox
	}
	return BackendJob
}

// resolveExecutionBackend picks the backend for one run, most specific first.
func resolveExecutionBackend(runAnnotations, catalogAnnotations map[string]string) ExecutionBackend {
	for _, m := range []map[string]string{runAnnotations, catalogAnnotations} {
		switch ExecutionBackend(m[ExecutionBackendAnnotation]) {
		case BackendSandbox:
			return BackendSandbox
		case BackendJob:
			return BackendJob
		}
	}
	return defaultExecutionBackend()
}

// buildRunSandbox builds a Sandbox carrying the byte-identical hardened pod
// spec buildRunJob puts in its Job template -- both call buildRunPodSpec, so
// the security contract, callback wiring and secretEnv resolution cannot
// diverge between backends.
//
// Three Job behaviors are translated rather than copied:
//
//   - activeDeadlineSeconds (a duration from start) becomes spec.shutdownTime
//     (an absolute instant), computed here at build time. shutdownPolicy is
//     Retain, not Delete, because a timed-out run must stay readable long
//     enough for the reconciler to record a terminal phase -- Delete would
//     race the status sync and surface as JobMissing.
//   - backoffLimit 0 becomes restartPolicy Never in the pod spec, which
//     buildRunPodSpec already sets: a Sandbox pod that exits is terminal, and
//     the Finished condition reports which way it went.
//   - ttlSecondsAfterFinished has no Sandbox equivalent. A finished Sandbox is
//     retained until its owner is deleted; ownerReferences from the run
//     handle reclamation, same as for a Job. (An AgentRun is itself reclaimed
//     after its retention window, which takes the Sandbox with it.)
func buildRunSandbox(p runJobParams) (*sandboxapi.Sandbox, error) {
	podSpec, err := buildRunPodSpec(p)
	if err != nil {
		return nil, err
	}

	timeout := defaultTimeoutSeconds
	if p.timeoutSeconds > 0 {
		timeout = int64(p.timeoutSeconds)
	}
	shutdownAt := metav1.NewTime(time.Now().Add(time.Duration(timeout) * time.Second))
	retain := sandboxapi.ShutdownPolicyRetain

	return &sandboxapi.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:        p.jobName,
			Namespace:   p.namespace,
			Labels:      p.labels,
			Annotations: p.annotations,
		},
		Spec: sandboxapi.SandboxSpec{
			PodTemplate: sandboxapi.PodTemplate{
				Spec: podSpec,
				ObjectMeta: sandboxapi.PodMetadata{
					Labels:      p.labels,
					Annotations: p.annotations,
				},
			},
			// A one-shot run needs no inbound addressing. Explicitly false
			// rather than unset: upstream treats unset as "preserve any
			// pre-existing Service" for backward compatibility.
			Service:        ptr.To(false),
			ShutdownTime:   &shutdownAt,
			ShutdownPolicy: &retain,
			OperatingMode:  sandboxapi.OperatingModeRunning,
		},
	}, nil
}

// sandboxPhase maps an observed Sandbox onto the same run-phase enum jobPhase
// produces, preserving ADR 0010's rule that the workload's own status -- not
// the callback payload -- decides a run's terminal phase.
//
// Precedence matters, and the first rule is the subtle one.
//
// A suspension is checked from spec.operatingMode BEFORE any condition,
// because suspending terminates the backing pod, and for a moment that pod is
// a failed pod: upstream publishes a transient Finished=True/PodFailed while
// the termination settles, then drops the condition once the Sandbox is fully
// suspended. Reading conditions first therefore latches a suspended run to
// Failed -- terminal, and never reconciled again, so the eventual resume finds
// a run nothing is watching. Observed directly in the Act 4 demo before this
// ordering was introduced. spec.operatingMode is desired state, set before the
// teardown begins and cleared on resume, so it is the only signal that does
// not race the pod's death.
//
// After that: Finished is the terminal signal (PodSucceeded/PodFailed).
// Expiry comes next, because an expired Sandbox has had its pod torn down and
// will never report Finished -- that is the timeout path, equivalent to a Job
// tripping activeDeadlineSeconds. A suspended run reports Running rather than
// a phase of its own: it has not finished, and ToolRunPhase has no Suspended
// member that could be added without breaking every consumer of the CRD.
func sandboxPhase(sb *sandboxapi.Sandbox, currentMessage string) (toolv1alpha1.ToolRunPhase, string) {
	if sb.Spec.OperatingMode == sandboxapi.OperatingModeSuspended {
		return toolv1alpha1.ToolRunPhaseRunning, "Sandbox suspended; pod terminated, volumes retained"
	}

	if c := meta.FindStatusCondition(sb.Status.Conditions, sandboxapi.ConditionFinished); c != nil && c.Status == metav1.ConditionTrue {
		switch c.Reason {
		case sandboxapi.ReasonPodSucceeded:
			return toolv1alpha1.ToolRunPhaseSucceeded, "Sandbox pod completed successfully"
		case sandboxapi.ReasonPodFailed:
			return toolv1alpha1.ToolRunPhaseFailed, genericSandboxFailedMessage
		}
	}

	ready := meta.FindStatusCondition(sb.Status.Conditions, sandboxapi.ConditionReady)
	if ready != nil && ready.Status == metav1.ConditionFalse {
		switch ready.Reason {
		case sandboxapi.ReasonSandboxExpired:
			return toolv1alpha1.ToolRunPhaseFailed, "Sandbox expired before completing (shutdownTime reached)"
		case sandboxapi.ReasonSuspended:
			return toolv1alpha1.ToolRunPhaseRunning, "Sandbox suspended; pod terminated, volumes retained"
		}
	}

	if ready != nil && ready.Status == metav1.ConditionTrue {
		return toolv1alpha1.ToolRunPhaseRunning, currentMessage
	}

	// No Ready condition yet, or Ready=False for a provisioning reason
	// (DependenciesNotReady) -- the Sandbox exists but its pod is not up.
	return toolv1alpha1.ToolRunPhasePending, currentMessage
}

// sandboxStartTime / sandboxCompletionTime recover the timestamps ToolRunStatus
// records from Job.status. A Sandbox has no startTime/completionTime fields,
// so the condition transitions stand in: Ready first going True is the pod
// running, and Finished going True is the pod reaching a terminal phase.
func sandboxStartTime(sb *sandboxapi.Sandbox) *metav1.Time {
	if c := meta.FindStatusCondition(sb.Status.Conditions, sandboxapi.ConditionReady); c != nil && !c.LastTransitionTime.IsZero() {
		return &c.LastTransitionTime
	}
	return nil
}

func sandboxCompletionTime(sb *sandboxapi.Sandbox) *metav1.Time {
	if c := meta.FindStatusCondition(sb.Status.Conditions, sandboxapi.ConditionFinished); c != nil && c.Status == metav1.ConditionTrue && !c.LastTransitionTime.IsZero() {
		return &c.LastTransitionTime
	}
	return nil
}

const genericSandboxFailedMessage = "Sandbox pod failed (see Sandbox/Pod events for detail)"

// runLifecycle is the lifecycle slice of a run's status that a workload is
// mirrored onto. ToolRunStatus and AgentRunStatus carry the same four fields,
// so both reconcilers sync a Sandbox through one function (observeSandbox)
// instead of each re-deriving the condition rules on its own.
type runLifecycle struct {
	phase          toolv1alpha1.ToolRunPhase
	message        string
	startTime      *metav1.Time
	completionTime *metav1.Time
}

// observeSandbox folds an observed Sandbox into a run's lifecycle fields and
// reports whether the phase or message moved, i.e. whether there is anything
// to write.
//
// podReader and podLabels locate the run's pod on the transition to Failed, so
// a Sandbox-backed run records the same concrete cause a Job-backed one does
// (`container "run" was OOMKilled ...`) rather than only "pod failed". The pod
// carries podLabels because buildRunSandbox stamps the run's labels onto the
// pod template; it is not looked up by a Job-style "job-name" label, which a
// Sandbox pod never has. A nil reader skips the lookup.
func observeSandbox(ctx context.Context, podReader client.Reader, sb *sandboxapi.Sandbox, podLabels map[string]string, cur runLifecycle) (runLifecycle, bool) {
	next := cur
	next.phase, next.message = sandboxPhase(sb, cur.message)

	// StartTime is set once and never revised. Unlike Job.status.startTime,
	// the Ready condition's LastTransitionTime moves every time readiness
	// flips -- including to False when the pod finishes or is suspended -- so
	// reassigning it on each sync would walk the recorded start forward.
	if next.startTime == nil {
		next.startTime = sandboxStartTime(sb)
	}
	if t := sandboxCompletionTime(sb); t != nil {
		next.completionTime = t
	}

	terminal := next.phase == toolv1alpha1.ToolRunPhaseSucceeded || next.phase == toolv1alpha1.ToolRunPhaseFailed
	if terminal && next.completionTime == nil {
		// The expiry path: the pod was torn down at shutdownTime and Finished
		// is never published, so there is no transition to read a completion
		// time from. Without one an AgentRun's retention window would be
		// measured from its creation, and a run that timed out after longer
		// than the window would be reclaimed the moment it failed -- before
		// anyone could read why.
		next.completionTime = ptr.To(metav1.Now())
	}

	if next.phase == toolv1alpha1.ToolRunPhaseFailed && cur.phase != toolv1alpha1.ToolRunPhaseFailed && next.message == genericSandboxFailedMessage {
		if detail := sandboxPodFailure(ctx, podReader, sb.Namespace, podLabels); detail != "" {
			next.message = "Sandbox pod failed: " + detail
		}
	}

	return next, next.phase != cur.phase || next.message != cur.message
}

// sandboxPodFailure is the concrete reason a Sandbox's pod failed, or "" when
// none can be read. Unlike failedRunMessage it never holds the run back on a
// List error: a failed Sandbox pod is retained with its Sandbox rather than
// reaped on a TTL, so the detail is still there to inspect by hand, and a run
// that stays non-terminal is the worse outcome.
func sandboxPodFailure(ctx context.Context, podReader client.Reader, namespace string, podLabels map[string]string) string {
	if podReader == nil || len(podLabels) == 0 {
		return ""
	}
	var list corev1.PodList
	if err := podReader.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabels(podLabels)); err != nil {
		logf.FromContext(ctx).Error(err, "Could not list Pods of failed Sandbox")
		return ""
	}
	return strings.Join(describePodFailures(list.Items), "; ")
}

// sandboxAPIAvailable reports whether agents.x-k8s.io/v1beta1 Sandbox is served
// by this cluster, via the manager's RESTMapper.
func sandboxAPIAvailable(mgr ctrl.Manager) bool {
	_, err := mgr.GetRESTMapper().RESTMapping(
		schema.GroupKind{Group: sandboxapi.GroupVersion.Group, Kind: "Sandbox"},
		sandboxapi.GroupVersion.Version,
	)
	return err == nil
}
