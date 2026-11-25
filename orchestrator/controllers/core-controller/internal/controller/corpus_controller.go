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
	"fmt"
	"slices"

	batchv1 "k8s.io/api/batch/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1alpha1 "github.com/aktisai/orchestrator/controllers/core-controller/api/v1alpha1"
)

const corpusConditionReady = "Ready"

// reasonScopeMismatch is the Degraded reason for a scope whose shape does not
// match its Connection's provider — the check CEL can no longer make, because
// the provider now lives on another resource.
const reasonScopeMismatch = "ScopeMismatch"

// CorpusCollectionName is the vector-store collection a Corpus owns.
//
// Storage is per-Corpus rather than per-KnowledgeBase (ADR 0039 §1), so a
// Corpus shared by several knowledge bases is embedded once and recomposing a
// knowledge base costs no re-indexing.
//
// Namespace and name both appear because collections are global in the vector
// store while Corpus names are only unique per namespace. Without the
// namespace, two same-named Corpora in different namespaces would silently
// share one collection — a cross-tenant leak of exactly the kind ADR 0039 §1
// argues the per-collection split exists to prevent.
func CorpusCollectionName(namespace, name string) string {
	return fmt.Sprintf("corpus_%s_%s", namespace, name)
}

// CorpusReconciler reconciles a Corpus object
type CorpusReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core.aktis.ai,resources=corpora,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.aktis.ai,resources=corpora/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.aktis.ai,resources=corpora/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.aktis.ai,resources=connections,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;delete

// Reconcile assigns a Corpus its collection and resolves what it inherits from
// its Connection.
//
// The resolution is the reason this controller does more than its predecessor.
// A Corpus names a Connection; the provider and the identity providers live
// there (ADR 0043). Publishing them into status means both engines keep
// watching ONE kind to build their catalogs, instead of each growing a join
// across two resources — the split stays here rather than spreading.
//
// Two checks moved here from admission, because CEL cannot read another
// resource and the provider is no longer on this object:
//
//   - the scope's shape must match the provider (a confluence Corpus wants a
//     space, a slack one a channel)
//   - the scope must be inside the Connection's allowedScopes, when it sets one
//
// Both fail the Corpus as Degraded rather than deleting or mutating it, and
// the broker refuses to bind a Corpus it cannot make sense of anyway, so a
// mis-shaped one is inert in both places rather than half-served in one.
//
// Credential presence is still deliberately NOT validated: this controller
// holds no RBAC on Secrets, and confirming one exists would mean granting read
// over Secrets to buy a nicety. A missing credential surfaces as a failed sync.
func (r *CorpusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var corpus corev1alpha1.Corpus
	if err := r.Get(ctx, req.NamespacedName, &corpus); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	collection := CorpusCollectionName(corpus.Namespace, corpus.Name)
	if corpus.Status.Collection != collection {
		log.Info("Assigned Corpus a vector-store collection",
			"corpus", corpus.Name, "collection", collection)
	}
	corpus.Status.Collection = collection
	corpus.Status.ObservedGeneration = corpus.Generation

	var connection corev1alpha1.Connection
	err := r.Get(ctx, types.NamespacedName{Namespace: corpus.Namespace, Name: corpus.Spec.ConnectionRef}, &connection)
	if apierrors.IsNotFound(err) {
		// Not an error to retry into: the Connection may simply not exist yet,
		// and a watch brings us back the moment it does.
		return r.degrade(ctx, &corpus, "ConnectionNotFound",
			fmt.Sprintf("connectionRef %q does not resolve", corpus.Spec.ConnectionRef))
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if reason, message := checkScope(&corpus, &connection); reason != "" {
		return r.degrade(ctx, &corpus, reason, message)
	}

	// Inherited from the Connection so consumers need not join (ADR 0043 §1).
	corpus.Status.Provider = connection.Spec.Provider
	corpus.Status.IdentityProviders = connection.Spec.IdentityProviders

	// The periodic reconcile is the broker's per-Corpus Temporal workflow
	// (docs/adr/0058); a sync CronJob an earlier release created is removed.
	if err := r.removeLegacySyncCronJob(ctx, &corpus); err != nil {
		return ctrl.Result{}, err
	}

	meta.SetStatusCondition(&corpus.Status.Conditions, metav1.Condition{
		Type:   corpusConditionReady,
		Status: metav1.ConditionTrue,
		Reason: "Accepted",
		Message: fmt.Sprintf("scoped %s corpus over connection %s; indexes into %s",
			connection.Spec.Provider, connection.Name, collection),
		ObservedGeneration: corpus.Generation,
	})

	if err := r.Status().Update(ctx, &corpus); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// degrade records why this Corpus cannot be served and stops.
//
// The provider and identityProviders are CLEARED rather than left at their last
// values: they are a claim about what this Corpus resolves to, and continuing
// to publish a stale claim would let a consumer act on a Connection this Corpus
// no longer validly references.
func (r *CorpusReconciler) degrade(
	ctx context.Context,
	corpus *corev1alpha1.Corpus,
	reason, message string,
) (ctrl.Result, error) {
	corpus.Status.Provider = ""
	corpus.Status.IdentityProviders = nil
	meta.SetStatusCondition(&corpus.Status.Conditions, metav1.Condition{
		Type:               corpusConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: corpus.Generation,
	})
	return ctrl.Result{}, r.Status().Update(ctx, corpus)
}

// checkScope enforces what CEL no longer can: that the scope matches the
// provider, and that it is inside whatever the Connection permits.
func checkScope(corpus *corev1alpha1.Corpus, connection *corev1alpha1.Connection) (reason, message string) {
	scope := corpus.Spec.Scope

	// A wildcard Corpus reaches every unit the Connection permits, so there is no
	// single unit to match against the cap — only the provider must be one a
	// driver implements. The allowlist (when set) bounds the wildcard at
	// enumeration time in the broker, not here.
	if scope.All {
		switch connection.Spec.Provider {
		case "gdrive":
			// gdrive has no cheap "every folder" enumeration, so a wildcard gdrive
			// Corpus must be capped by allowedScopes.folderIDs (the broker refuses an
			// uncapped one). Enforce it here rather than deferring to a failed sync.
			if connection.Spec.AllowedScopes == nil || len(connection.Spec.AllowedScopes.FolderIDs) == 0 {
				return reasonScopeMismatch, "a wildcard gdrive Corpus requires the Connection to cap it with allowedScopes.folderIDs"
			}
			return "", ""
		case "confluence", "slack", "jazzhr":
			return "", ""
		default:
			return "UnknownProvider", fmt.Sprintf("connection %s has provider %q, which no driver implements",
				connection.Name, connection.Spec.Provider)
		}
	}

	unit, allowed := "", []string(nil)
	switch connection.Spec.Provider {
	case "confluence":
		unit = scope.Space
		if connection.Spec.AllowedScopes != nil {
			allowed = connection.Spec.AllowedScopes.Spaces
		}
		if unit == "" {
			return reasonScopeMismatch, "a confluence Corpus must set scope.space or scope.all"
		}
	case "slack":
		unit = scope.Channel
		if connection.Spec.AllowedScopes != nil {
			allowed = connection.Spec.AllowedScopes.Channels
		}
		if unit == "" {
			return reasonScopeMismatch, "a slack Corpus must set scope.channel or scope.all"
		}
	case "gdrive":
		unit = scope.FolderID
		if connection.Spec.AllowedScopes != nil {
			allowed = connection.Spec.AllowedScopes.FolderIDs
		}
		if unit == "" {
			return reasonScopeMismatch, "a gdrive Corpus must set scope.folderID or scope.all"
		}
	case "jazzhr":
		unit = scope.JobID
		if connection.Spec.AllowedScopes != nil {
			allowed = connection.Spec.AllowedScopes.JobIDs
		}
		if unit == "" {
			return reasonScopeMismatch, "a jazzhr Corpus must set scope.jobID or scope.all"
		}
	default:
		return "UnknownProvider", fmt.Sprintf("connection %s has provider %q, which no driver implements",
			connection.Name, connection.Spec.Provider)
	}

	// An empty allowlist means the Connection set no cap for this provider, not
	// that it permits nothing — the cap is opt-in (ADR 0043 §3).
	if len(allowed) > 0 && !slices.Contains(allowed, unit) {
		return "OutsideAllowedScopes", fmt.Sprintf(
			"connection %s does not permit %q; add it to spec.allowedScopes to allow it",
			connection.Name, unit)
	}
	return "", ""
}

// LegacySyncCronJobName is the CronJob a Corpus owned before its sync moved
// onto Temporal (docs/adr/0058).
func LegacySyncCronJobName(corpus string) string { return "corpus-sync-" + corpus }

// removeLegacySyncCronJob deletes the sync CronJob an earlier release created
// for this Corpus.
//
// That CronJob POSTed the broker's /corpora/:name/sync on a schedule, while
// the broker ran its own timer for the same pass -- two schedulers for one
// job. The broker's per-Corpus Temporal workflow is now the only one: it
// re-reads reconcileInterval itself, and its passes never overlap across
// replicas, which ForbidConcurrent could only promise for the kicks. Left in
// place, the old CronJob would keep asking for a full pass on top of the
// workflow's own, each one a full walk of a client's source.
//
// Only a CronJob a Corpus of this name controls is deleted: the name alone
// could be anyone's.
func (r *CorpusReconciler) removeLegacySyncCronJob(ctx context.Context, corpus *corev1alpha1.Corpus) error {
	var existing batchv1.CronJob
	err := r.Get(ctx, types.NamespacedName{Namespace: corpus.Namespace, Name: LegacySyncCronJobName(corpus.Name)}, &existing)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !controlledByCorpus(&existing, corpus) {
		return nil
	}
	logf.FromContext(ctx).Info("Removing the legacy sync CronJob; the broker's workflow syncs this Corpus now",
		"corpus", corpus.Name)
	return client.IgnoreNotFound(r.Delete(ctx, &existing))
}

// controlledByCorpus is whether a Corpus of this name controls the object. By
// name, not UID: the CronJob may still name an old-group Corpus from the aktis
// API-group move (ADR 0047), which recreated every Corpus.
func controlledByCorpus(obj metav1.Object, corpus *corev1alpha1.Corpus) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == "Corpus" && ref.Name == corpus.Name && ref.Controller != nil && *ref.Controller {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *CorpusReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1alpha1.Corpus{}).
		// A Corpus inherits from its Connection, so a Connection edit — a
		// provider change, a tightened allowlist — has to re-resolve every
		// Corpus over it. Without this they would keep publishing what the
		// Connection used to say.
		Watches(
			&corev1alpha1.Connection{},
			handler.EnqueueRequestsFromMapFunc(r.corporaForConnection),
			builder.WithPredicates(),
		).
		Named("corpus").
		Complete(r)
}

func (r *CorpusReconciler) corporaForConnection(ctx context.Context, obj client.Object) []reconcile.Request {
	var corpora corev1alpha1.CorpusList
	if err := r.List(ctx, &corpora, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, corpus := range corpora.Items {
		if corpus.Spec.ConnectionRef != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: corpus.Namespace, Name: corpus.Name},
		})
	}
	return requests
}
