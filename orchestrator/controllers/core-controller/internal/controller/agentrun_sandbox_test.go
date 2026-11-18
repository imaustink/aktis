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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	toolv1alpha1 "github.com/aktisai/orchestrator/controllers/core-controller/api/v1alpha1"
	"github.com/aktisai/orchestrator/controllers/core-controller/internal/sandboxapi"
)

const sandboxTestNS = "aktis"

func sandboxTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, toolv1alpha1.AddToScheme, sandboxapi.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// agentFixture exercises what an Agent adds over a Tool: an init container and
// a file-based skill mount, both of which have to survive the backend switch.
func agentFixture(annotations map[string]string) *toolv1alpha1.Agent {
	return &toolv1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "swe", Namespace: sandboxTestNS, Annotations: annotations},
		Spec: toolv1alpha1.AgentSpec{
			Image:              "ghcr.io/example/swe-agent:latest",
			ServiceAccountName: "swe-agent",
			SecretEnv: []toolv1alpha1.SecretEnvVar{{
				Name:      "ANTHROPIC_API_KEY",
				SecretRef: toolv1alpha1.SecretKeySelector{Name: "llm", Key: "key"},
			}},
			InitContainers: []toolv1alpha1.InitContainer{{
				Name: "seed-credentials", Image: "busybox:1.36", Args: []string{"sh", "-c", "true"},
			}},
			Skills: []toolv1alpha1.AgentSkill{{Name: "review", ConfigMapName: "skill-review"}},
		},
	}
}

func agentRunFixture(name string, annotations map[string]string) *toolv1alpha1.AgentRun {
	return &toolv1alpha1.AgentRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: sandboxTestNS, Annotations: annotations},
		Spec: toolv1alpha1.AgentRunSpec{
			AgentRef:       "swe",
			Goal:           "fix the flaky test",
			TimeoutSeconds: 600,
			Callback:       toolv1alpha1.ToolRunCallback{NatsSubject: "agent.run-1.result", NatsUrl: "nats://nats:4222"},
		},
	}
}

func reconcileAgentRun(t *testing.T, r *AgentRunReconciler, name string) *toolv1alpha1.AgentRun {
	t.Helper()
	key := types.NamespacedName{Namespace: sandboxTestNS, Name: name}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var run toolv1alpha1.AgentRun
	if err := r.Get(context.Background(), key, &run); err != nil {
		t.Fatalf("get AgentRun: %v", err)
	}
	return &run
}

func sandboxAnnotated() map[string]string {
	return map[string]string{ExecutionBackendAnnotation: string(BackendSandbox)}
}

// An AgentRun on the Sandbox backend launches a Sandbox and no Job, records
// the backend it used, and carries the pod the Job backend would have run.
func TestAgentRunLaunchesSandboxWithTheJobBackendsPod(t *testing.T) {
	scheme := sandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&toolv1alpha1.AgentRun{}).
		WithObjects(agentFixture(nil),
			agentRunFixture("on-sandbox", sandboxAnnotated()),
			agentRunFixture("on-job", nil)).
		Build()
	r := &AgentRunReconciler{Client: c, Scheme: scheme}

	sbRun := reconcileAgentRun(t, r, "on-sandbox")
	if sbRun.Status.ExecutionBackend != string(BackendSandbox) || sbRun.Status.JobName != "on-sandbox" || sbRun.Status.Phase != toolv1alpha1.ToolRunPhasePending {
		t.Fatalf("sandbox run status = %+v, want Pending on backend sandbox", sbRun.Status)
	}
	jobRun := reconcileAgentRun(t, r, "on-job")
	if jobRun.Status.ExecutionBackend != string(BackendJob) {
		t.Fatalf("job run executionBackend = %q, want job", jobRun.Status.ExecutionBackend)
	}

	ctx := context.Background()
	var sb sandboxapi.Sandbox
	if err := c.Get(ctx, types.NamespacedName{Namespace: sandboxTestNS, Name: "on-sandbox"}, &sb); err != nil {
		t.Fatalf("the Sandbox was not created: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: sandboxTestNS, Name: "on-sandbox"}, &batchv1.Job{}); err == nil {
		t.Fatal("a Job was created for a run on the Sandbox backend")
	}
	if owner := metav1.GetControllerOf(&sb); owner == nil || owner.Kind != "AgentRun" || owner.Name != "on-sandbox" {
		t.Errorf("Sandbox controller owner = %+v, want the AgentRun (it is what reclaims the Sandbox)", owner)
	}

	var job batchv1.Job
	if err := c.Get(ctx, types.NamespacedName{Namespace: sandboxTestNS, Name: "on-job"}, &job); err != nil {
		t.Fatalf("the Job was not created: %v", err)
	}
	// Each run's name appears in its own AGENT_RUN_ID env; with that one
	// difference normalised away the two pods must be identical.
	normalise := func(spec corev1.PodSpec) corev1.PodSpec {
		spec = *spec.DeepCopy()
		for i, e := range spec.Containers[0].Env {
			if e.Name == "AGENT_RUN_ID" {
				spec.Containers[0].Env[i].Value = "<run>"
			}
		}
		return spec
	}
	if diff := cmp.Diff(normalise(job.Spec.Template.Spec), normalise(sb.Spec.PodTemplate.Spec)); diff != "" {
		t.Errorf("AgentRun Job and Sandbox pod specs differ (-job +sandbox):\n%s", diff)
	}

	// Not vacuous: the agent-only parts are really in the Sandbox's pod.
	spec := sb.Spec.PodTemplate.Spec
	if len(spec.InitContainers) != 1 {
		t.Errorf("init containers = %d, want the Agent's 1", len(spec.InitContainers))
	}
	var skillMounted bool
	for _, m := range spec.Containers[0].VolumeMounts {
		if m.MountPath == skillsMountDir+"/review" && m.ReadOnly {
			skillMounted = true
		}
	}
	if !skillMounted {
		t.Error("the Agent's skill is not mounted read-only in the Sandbox pod")
	}
	if sb.Spec.PodTemplate.ObjectMeta.Labels["core.aktis.ai/agentrun"] != "on-sandbox" {
		t.Error("the Sandbox pod template lacks the agentrun label the failure lookup selects on")
	}
}

// An Agent can opt every one of its runs in, and a run can opt back out.
func TestAgentAnnotationSelectsBackendAndRunOverridesIt(t *testing.T) {
	scheme := sandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&toolv1alpha1.AgentRun{}).
		WithObjects(agentFixture(sandboxAnnotated()),
			agentRunFixture("inherits", nil),
			agentRunFixture("opts-out", map[string]string{ExecutionBackendAnnotation: string(BackendJob)})).
		Build()
	r := &AgentRunReconciler{Client: c, Scheme: scheme}

	if got := reconcileAgentRun(t, r, "inherits").Status.ExecutionBackend; got != string(BackendSandbox) {
		t.Errorf("run under a sandbox-annotated Agent ran on %q, want sandbox", got)
	}
	if got := reconcileAgentRun(t, r, "opts-out").Status.ExecutionBackend; got != string(BackendJob) {
		t.Errorf("run annotated job under a sandbox-annotated Agent ran on %q, want job", got)
	}
}

// The Sandbox's conditions, not the Job path, drive a Sandbox-backed run to
// its terminal phase -- and a suspension along the way does not end it.
func TestAgentRunFollowsItsSandbox(t *testing.T) {
	scheme := sandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&toolv1alpha1.AgentRun{}).
		WithObjects(agentFixture(nil), agentRunFixture("run", sandboxAnnotated())).
		Build()
	r := &AgentRunReconciler{Client: c, Scheme: scheme}
	ctx := context.Background()
	key := types.NamespacedName{Namespace: sandboxTestNS, Name: "run"}

	reconcileAgentRun(t, r, "run")

	setSandbox := func(mode sandboxapi.SandboxOperatingMode, conds ...metav1.Condition) {
		t.Helper()
		var sb sandboxapi.Sandbox
		if err := c.Get(ctx, key, &sb); err != nil {
			t.Fatal(err)
		}
		sb.Spec.OperatingMode = mode
		sb.Status.Conditions = conds
		if err := c.Update(ctx, &sb); err != nil {
			t.Fatal(err)
		}
	}

	setSandbox(sandboxapi.OperatingModeRunning, cond("Ready", metav1.ConditionTrue, "DependenciesReady"))
	run := reconcileAgentRun(t, r, "run")
	if run.Status.Phase != toolv1alpha1.ToolRunPhaseRunning || run.Status.StartTime == nil {
		t.Fatalf("after Ready: phase %q, startTime %v; want Running with a start time", run.Status.Phase, run.Status.StartTime)
	}

	setSandbox(sandboxapi.OperatingModeSuspended,
		cond("Ready", metav1.ConditionFalse, "PodFailed"), cond("Finished", metav1.ConditionTrue, "PodFailed"))
	if run = reconcileAgentRun(t, r, "run"); run.Status.Phase != toolv1alpha1.ToolRunPhaseRunning {
		t.Fatalf("mid-suspension phase = %q, want Running: a parked agent is not a failed one", run.Status.Phase)
	}

	setSandbox(sandboxapi.OperatingModeRunning, cond("Finished", metav1.ConditionTrue, "PodSucceeded"))
	run = reconcileAgentRun(t, r, "run")
	if run.Status.Phase != toolv1alpha1.ToolRunPhaseSucceeded || run.Status.CompletionTime == nil {
		t.Fatalf("after Finished: phase %q, completionTime %v; want Succeeded with a completion time", run.Status.Phase, run.Status.CompletionTime)
	}
}

// A failed Sandbox-backed agent says why, from its pod -- an OOM-killed SWE
// agent must not read as a bare "pod failed" just because a Sandbox ran it.
func TestFailedSandboxAgentRunRecordsThePodsReason(t *testing.T) {
	scheme := sandboxTestScheme(t)
	pod := oomPod("run")
	pod.Namespace = sandboxTestNS
	pod.Labels = map[string]string{"core.aktis.ai/agentrun": "run"}
	other := oomPod("someone-else")
	other.Namespace = sandboxTestNS
	other.Labels = map[string]string{"core.aktis.ai/agentrun": "another-run"}
	other.Status.ContainerStatuses[0].State.Terminated.Reason = "Error"

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&toolv1alpha1.AgentRun{}).
		WithObjects(agentFixture(nil), agentRunFixture("run", sandboxAnnotated()), pod, other).
		Build()
	r := &AgentRunReconciler{Client: c, Scheme: scheme, PodReader: c}
	ctx := context.Background()
	key := types.NamespacedName{Namespace: sandboxTestNS, Name: "run"}

	reconcileAgentRun(t, r, "run")
	var sb sandboxapi.Sandbox
	if err := c.Get(ctx, key, &sb); err != nil {
		t.Fatal(err)
	}
	sb.Status.Conditions = []metav1.Condition{cond("Finished", metav1.ConditionTrue, "PodFailed")}
	if err := c.Update(ctx, &sb); err != nil {
		t.Fatal(err)
	}

	run := reconcileAgentRun(t, r, "run")
	if run.Status.Phase != toolv1alpha1.ToolRunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", run.Status.Phase)
	}
	if !strings.Contains(run.Status.Message, "OOMKilled") || strings.Contains(run.Status.Message, "Error") {
		t.Errorf("message = %q, want this run's own pod's OOMKilled reason and no other run's", run.Status.Message)
	}
}

// An expired Sandbox never publishes Finished, so the completion time has to
// be stamped at the transition or retention is measured from creation.
func TestExpiredSandboxAgentRunGetsACompletionTime(t *testing.T) {
	scheme := sandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&toolv1alpha1.AgentRun{}).
		WithObjects(agentFixture(nil), agentRunFixture("run", sandboxAnnotated())).
		Build()
	r := &AgentRunReconciler{Client: c, Scheme: scheme}
	ctx := context.Background()
	key := types.NamespacedName{Namespace: sandboxTestNS, Name: "run"}

	reconcileAgentRun(t, r, "run")
	var sb sandboxapi.Sandbox
	if err := c.Get(ctx, key, &sb); err != nil {
		t.Fatal(err)
	}
	sb.Status.Conditions = []metav1.Condition{cond("Ready", metav1.ConditionFalse, "SandboxExpired")}
	if err := c.Update(ctx, &sb); err != nil {
		t.Fatal(err)
	}

	run := reconcileAgentRun(t, r, "run")
	if run.Status.Phase != toolv1alpha1.ToolRunPhaseFailed || run.Status.CompletionTime == nil {
		t.Fatalf("expired run: phase %q, completionTime %v; want Failed with a completion time", run.Status.Phase, run.Status.CompletionTime)
	}
}

// On a cluster without agent-sandbox the run fails with the reason instead of
// retrying a create that can never succeed.
func TestAgentRunFailsCleanlyWithoutTheSandboxCRD(t *testing.T) {
	scheme := sandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&toolv1alpha1.AgentRun{}).
		WithObjects(agentFixture(nil), agentRunFixture("run", sandboxAnnotated())).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*sandboxapi.Sandbox); ok {
					return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: sandboxapi.GroupVersion.Group, Kind: "Sandbox"}}
				}
				return cl.Create(ctx, obj, opts...)
			},
		}).
		Build()
	r := &AgentRunReconciler{Client: c, Scheme: scheme}

	run := reconcileAgentRun(t, r, "run")
	if run.Status.Phase != toolv1alpha1.ToolRunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", run.Status.Phase)
	}
	ready := meta.FindStatusCondition(run.Status.Conditions, "Ready")
	if ready == nil || ready.Reason != "ExecutionBackendUnavailable" {
		t.Errorf("Ready condition = %+v, want reason ExecutionBackendUnavailable", ready)
	}
}
