package agentrun_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/aktisai/orchestrator/engines/temporal/internal/agentrun"
	"github.com/aktisai/orchestrator/engines/temporal/internal/toolrun"
)

func agentRunWithJob(phase, jobName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": agentrun.GVR.Group + "/" + agentrun.GVR.Version,
		"kind":       "AgentRun",
		"metadata":   map[string]any{"name": "run-1", "namespace": "ns"},
		"status":     map[string]any{"phase": phase, "jobName": jobName},
	}}
}

func runPod(name, jobName, phase string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]any{"name": name, "namespace": "ns", "labels": map[string]any{"job-name": jobName}},
		"status":     map[string]any{"phase": phase},
	}}
}

func statusClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			agentrun.GVR:                      "AgentRunList",
			{Version: "v1", Resource: "pods"}: "PodList",
		}, objs...)
}

// A Job reads Running as soon as its pod exists, so whether the agent has
// actually started comes from the pod: Pending covers scheduling, image pulls
// and init containers.
func TestGetStatusReportsAPodStillStarting(t *testing.T) {
	for _, tc := range []struct {
		name     string
		phase    string
		pods     []*unstructured.Unstructured
		starting bool
	}{
		{"pod pulling its image", "Running", []*unstructured.Unstructured{runPod("p1", "job-1", "Pending")}, true},
		{"no pod yet", "Pending", nil, true},
		{"pod started", "Running", []*unstructured.Unstructured{runPod("p1", "job-1", "Running")}, false},
		{"a retry pending after a pod that ran", "Running", []*unstructured.Unstructured{runPod("p1", "job-1", "Failed"), runPod("p2", "job-1", "Pending")}, false},
		{"another job's running pod", "Running", []*unstructured.Unstructured{runPod("p1", "job-1", "Pending"), runPod("p9", "job-9", "Running")}, true},
		{"finished run", "Succeeded", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := []runtime.Object{agentRunWithJob(tc.phase, "job-1")}
			for _, p := range tc.pods {
				objs = append(objs, p)
			}
			launcher := agentrun.NewK8sLauncher(statusClient(objs...), "ns", toolrun.SecretRef{Name: "cb", Key: "k"})
			status, err := launcher.GetStatus(context.Background(), "run-1")
			require.NoError(t, err)
			require.Equal(t, tc.phase, status.Phase)
			require.Equal(t, tc.starting, status.Starting)
		})
	}
}

func TestStopDeletesTheRunAndToleratesAGoneOne(t *testing.T) {
	client := statusClient(agentRunWithJob("Running", "job-1"))
	launcher := agentrun.NewK8sLauncher(client, "ns", toolrun.SecretRef{Name: "cb", Key: "k"})

	require.NoError(t, launcher.Stop(context.Background(), "run-1"))
	_, err := client.Resource(agentrun.GVR).Namespace("ns").Get(context.Background(), "run-1", metav1.GetOptions{})
	require.Error(t, err)
	require.NoError(t, launcher.Stop(context.Background(), "run-1"), "already gone is stopped")
}
