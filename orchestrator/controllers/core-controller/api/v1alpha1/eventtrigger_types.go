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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EventTriggerMatch selects events by a CEL predicate over the envelope
// (docs/event-triggers.md §3).
type EventTriggerMatch struct {
	// cel must evaluate to bool. Absent matches every event from sourceRef.
	// +optional
	CEL string `json:"cel,omitempty"`
}

// OrchestratorTarget runs an ordinary conversational turn (skill retrieval)
// on the rendered prompt. It has no fields; its presence selects it.
type OrchestratorTarget struct{}

// NoneTarget runs nothing: the trigger only executes its reply side effects
// (labels, assignees, the ack). It has no fields; its presence selects it. It
// is how a handoff such as "assign the PR to a human" is expressed
// (docs/adr/0052 §3).
type NoneTarget struct{}

// EventTriggerTarget is exactly one of orchestrator/skillRef/agentRef/toolRef/none.
// +kubebuilder:validation:XValidation:rule="(has(self.orchestrator)?1:0)+(has(self.skillRef)?1:0)+(has(self.agentRef)?1:0)+(has(self.toolRef)?1:0)+(has(self.none)?1:0)==1",message="exactly one of orchestrator, skillRef, agentRef, toolRef or none must be set"
// +kubebuilder:validation:XValidation:rule="has(self.toolRef) || has(self.none) || (has(self.promptTemplate) && size(self.promptTemplate) > 0)",message="promptTemplate is required unless toolRef or none is set"
// +kubebuilder:validation:XValidation:rule="!has(self.none) || !has(self.promptTemplate) || size(self.promptTemplate) == 0",message="promptTemplate cannot be set with none: nothing runs to receive it"
// +kubebuilder:validation:XValidation:rule="!has(self.none) || !has(self.readOnly) || !self.readOnly",message="readOnly cannot be set with none: nothing runs to restrict"
// +kubebuilder:validation:XValidation:rule="!has(self.none) || !has(self.identity)",message="identity cannot be set with none: nothing runs to authorize"
// +kubebuilder:validation:XValidation:rule="!has(self.identity) || !has(self.toolRef)",message="identity applies to agent runs; it cannot be set with toolRef"
type EventTriggerTarget struct {
	// orchestrator runs a conversational turn on the rendered prompt.
	// +optional
	Orchestrator *OrchestratorTarget `json:"orchestrator,omitempty"`

	// skillRef forces a Skill CR (same namespace).
	// +optional
	SkillRef string `json:"skillRef,omitempty"`

	// agentRef dispatches an Agent CR (same namespace) as a subagent.
	// +optional
	AgentRef string `json:"agentRef,omitempty"`

	// toolRef runs a Tool CR (same namespace) once, with params as its
	// arguments, skipping the planner.
	// +optional
	ToolRef string `json:"toolRef,omitempty"`

	// none runs nothing: the trigger is accepted and completes at once with an
	// empty result, executing only its reply side effects. No AgentRun, no
	// tool, no identity check.
	// +optional
	None *NoneTarget `json:"none,omitempty"`

	// promptTemplate is the request, with {{param}} placeholders.
	// +optional
	PromptTemplate string `json:"promptTemplate,omitempty"`

	// readOnly makes every AgentRun/ToolRun this turn launches read-only and
	// suppresses write credentials. It can only restrict.
	// +optional
	ReadOnly bool `json:"readOnly,omitempty"`

	// identity overrides the identity mode of every agent this turn
	// authorizes (docs/adr/0049): agent, delegated or hybrid, in place of the
	// Agent's defaultIdentity. Unlike readOnly it can widen what a run
	// reaches, so it travels only inside the signed dispatch assertion and
	// must be in the Agent's allowedIdentities. Temporal engine only.
	// +optional
	Identity IdentityMode `json:"identity,omitempty"`
}

// GitHubReply posts to an issue or pull request. Every string is a
// {{param}} template.
type GitHubReply struct {
	// repository is "owner/name".
	// +required
	// +kubebuilder:validation:MinLength=1
	Repository string `json:"repository"`

	// issueNumber is the issue or pull request number.
	// +required
	// +kubebuilder:validation:MinLength=1
	IssueNumber string `json:"issueNumber"`

	// ack is a comment posted before the run starts.
	// +optional
	Ack string `json:"ack,omitempty"`

	// sessionPage appends a session-page link to the ack comment.
	// +optional
	SessionPage bool `json:"sessionPage,omitempty"`

	// result posts the turn's result as a comment.
	// +optional
	Result bool `json:"result,omitempty"`

	// removeLabel is removed when the run ends, on every outcome.
	// +optional
	RemoveLabel string `json:"removeLabel,omitempty"`

	// addLabels are added when the run is accepted, alongside the ack. Each
	// entry is a {{param}} template; one that renders empty is dropped. Labels
	// the bot adds never re-enter routing (bot-authored events are dropped),
	// so they are safe as status markers (docs/adr/0052 §3).
	// +optional
	// +kubebuilder:validation:MaxItems=10
	AddLabels []string `json:"addLabels,omitempty"`

	// assignees are logins assigned to the issue or pull request when the run
	// is accepted. Each entry is a {{param}} template; one that renders empty
	// is dropped. GitHub caps an issue at 10 assignees.
	// +optional
	// +kubebuilder:validation:MaxItems=10
	Assignees []string `json:"assignees,omitempty"`

	// onIdentityError posts the printable message of an identity error (the
	// sender must link an account first, docs/adr/0049 §6) as a comment.
	// GitHub never reads a webhook's response body, so without this nobody
	// would see why nothing ran. The webhook is answered 403 either way.
	// +optional
	OnIdentityError bool `json:"onIdentityError,omitempty"`
}

// HTTPReply POSTs the terminal outcome to a fixed URL.
type HTTPReply struct {
	// url is a literal, never templated: a reply target must come from
	// operator config, not from event content (SSRF).
	// +required
	// +kubebuilder:validation:Pattern=`^https?://`
	URL string `json:"url"`

	// secretRef signs the body (x-event-signature: sha256=<hex hmac>).
	// +optional
	SecretRef *SecretKeySelector `json:"secretRef,omitempty"`
}

// EventTriggerReply declares side effects executed by integration-gateway.
type EventTriggerReply struct {
	// +optional
	GitHub *GitHubReply `json:"github,omitempty"`

	// +optional
	HTTP *HTTPReply `json:"http,omitempty"`
}

// EventTriggerOnComplete publishes a run's completion as an event
// (docs/adr/0052 §1).
type EventTriggerOnComplete struct {
	// emitTo names a run-kind EventSource (spec.run) in the same namespace.
	// When a run this trigger started finishes, integration-gateway relays a
	// kind "run" event on it carrying the run's status, params, outcome and
	// chain counters, so other triggers can chain off it. The trigger is not
	// Ready until the source exists and is a run source.
	// +required
	// +kubebuilder:validation:MinLength=1
	EmitTo string `json:"emitTo"`

	// waitUntilClosed holds the completion event back until the GitHub issue
	// or pull request named by two of this trigger's params is closed
	// (merged or not), docs/adr/0055 §2. The gateway records the held event
	// durably and re-checks it on a sweep; it is dropped instead if the issue
	// the run replied to is closed first.
	// +optional
	WaitUntilClosed *OnCompleteWaitUntilClosed `json:"waitUntilClosed,omitempty"`

	// serialize emits completion events one at a time per group and key: an
	// event is held while the run started by the previous event with the same
	// group and key is still going (docs/adr/0055 §3).
	// +optional
	Serialize *OnCompleteSerialize `json:"serialize,omitempty"`
}

// OnCompleteWaitUntilClosed names the params holding the issue/PR to wait on.
type OnCompleteWaitUntilClosed struct {
	// repositoryParam names the spec.params key holding "owner/name".
	// +required
	// +kubebuilder:validation:MinLength=1
	RepositoryParam string `json:"repositoryParam"`

	// numberParam names the spec.params key holding the issue/PR number.
	// +required
	// +kubebuilder:validation:MinLength=1
	NumberParam string `json:"numberParam"`
}

// OnCompleteSerialize names a queue: one in-flight run per group and key.
type OnCompleteSerialize struct {
	// group is the queue's name, shared by every trigger that serializes
	// into it.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Group string `json:"group"`

	// keyParam names the spec.params key whose value partitions the group
	// (e.g. the repository, so each repository has its own queue).
	// +required
	// +kubebuilder:validation:MinLength=1
	KeyParam string `json:"keyParam"`
}

// EventTriggerSpec defines what a matching event dispatches.
type EventTriggerSpec struct {
	// sourceRef restricts the trigger to one EventSource.
	// +optional
	SourceRef string `json:"sourceRef,omitempty"`

	// priority breaks ties when several triggers match; higher wins, then name.
	// +optional
	Priority int32 `json:"priority,omitempty"`

	// match selects events.
	// +optional
	Match EventTriggerMatch `json:"match,omitempty"`

	// params maps a name to a CEL expression evaluated against the envelope.
	// +optional
	Params map[string]string `json:"params,omitempty"`

	// sessionKey is a {{param}} template for the turn's session id.
	// +optional
	SessionKey string `json:"sessionKey,omitempty"`

	// target is what the event dispatches.
	// +required
	Target EventTriggerTarget `json:"target"`

	// reply declares side effects around the run.
	// +optional
	Reply *EventTriggerReply `json:"reply,omitempty"`

	// onComplete publishes the run's completion as an event. Absent, the
	// trigger behaves as it always has: nothing is emitted.
	// +optional
	OnComplete *EventTriggerOnComplete `json:"onComplete,omitempty"`
}

// EventTriggerStatus defines the observed state of EventTrigger.
type EventTriggerStatus struct {
	// conditions; "Ready" reports that the CEL compiles and refs resolve.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=".spec.sourceRef"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// EventTrigger maps events matching a CEL predicate to an orchestrator turn,
// Skill, Agent or Tool with CEL-derived params (docs/adr/0048), or to reply
// side effects alone (target.none, docs/adr/0052).
type EventTrigger struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of EventTrigger
	// +required
	Spec EventTriggerSpec `json:"spec"`

	// status defines the observed state of EventTrigger
	// +optional
	Status EventTriggerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// EventTriggerList contains a list of EventTrigger
type EventTriggerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []EventTrigger `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EventTrigger{}, &EventTriggerList{})
}
