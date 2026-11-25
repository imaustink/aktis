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
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1alpha1 "github.com/aktisai/orchestrator/controllers/core-controller/api/v1alpha1"
)

const providerSlack = "slack"

// corpusFor is a minimal valid Corpus over the named Connection.
func corpusFor(connectionRef, name string) *corev1alpha1.Corpus {
	return &corev1alpha1.Corpus{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1alpha1.CorpusSpec{
			ConnectionRef: connectionRef,
			Description:   "The GLOBEX client's Confluence space.",
			DisplayName:   "GLOBEX Confluence",
			AllowedRoles:  []string{"reader"},
			Scope:         corev1alpha1.CorpusScope{Space: "GLOBEX"},
		},
	}
}

func jazzhrConnection(name string) *corev1alpha1.Connection {
	return &corev1alpha1.Connection{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1alpha1.ConnectionSpec{
			Provider:    "jazzhr",
			DisplayName: "Bitovi JazzHR",
		},
	}
}

func jazzhrCorpusFor(connectionRef, name string, scope corev1alpha1.CorpusScope) *corev1alpha1.Corpus {
	return &corev1alpha1.Corpus{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1alpha1.CorpusSpec{
			ConnectionRef: connectionRef,
			Description:   "Candidates for a JazzHR job.",
			DisplayName:   "Senior Platform Engineer",
			AllowedRoles:  []string{"recruiter"},
			Scope:         scope,
		},
	}
}

var _ = Describe("Corpus Controller", func() {
	ctx := context.Background()

	reconcileCorpus := func(name string) {
		r := &CorpusReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: "default"},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	stored := func(name string) corev1alpha1.Corpus {
		var out corev1alpha1.Corpus
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &out)).To(Succeed())
		return out
	}

	// Scope is the security boundary (ADR 0038). What CEL can still enforce
	// without the provider — which now lives on the Connection — is enforced at
	// admission; the rest moved to the controller (ADR 0043).
	Context("scope shape", func() {
		It("rejects a corpus scoped to two things at once", func() {
			corpus := corpusFor("anything", "scope-two-things")
			corpus.Spec.Scope.Channel = "C6RQKL5BK"
			Expect(k8sClient.Create(ctx, corpus)).To(
				MatchError(ContainSubstring("exactly one")),
				"a widened scope is the failure this validation exists to catch")
		})

		It("rejects a corpus with no scope at all", func() {
			corpus := corpusFor("anything", "scope-none")
			corpus.Spec.Scope = corev1alpha1.CorpusScope{}
			Expect(k8sClient.Create(ctx, corpus)).To(HaveOccurred())
		})

		It("requires a connectionRef", func() {
			corpus := corpusFor("", "no-connection-ref")
			Expect(k8sClient.Create(ctx, corpus)).To(HaveOccurred())
		})
	})

	Context("resolution from its Connection", func() {
		It("inherits provider and identityProviders into status", func() {
			// Published into status so both engines keep watching ONE kind,
			// rather than each growing a join across two resources.
			conn := confluenceConnection("inherit-src")
			conn.Spec.IdentityProviders = []string{"atlassian"}
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, conn) }()

			corpus := corpusFor("inherit-src", "inherits")
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("inherits")

			got := stored("inherits")
			Expect(got.Status.Provider).To(Equal("confluence"))
			Expect(got.Status.IdentityProviders).To(Equal([]string{"atlassian"}))
			Expect(got.Status.Collection).To(Equal(CorpusCollectionName("default", "inherits")))
		})

		It("degrades when its connectionRef does not resolve", func() {
			corpus := corpusFor("no-such-connection", "dangling")
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("dangling")

			// Not an error to retry into: a watch brings us back the moment the
			// Connection appears.
			Expect(stored("dangling").Status.Conditions).To(ContainElement(
				HaveField("Reason", Equal("ConnectionNotFound"))))
		})

		It("clears an inherited provider when it stops resolving", func() {
			// A stale claim would let a consumer act on a Connection this
			// Corpus no longer validly references.
			conn := confluenceConnection("goes-away")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			corpus := corpusFor("goes-away", "was-resolved")
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("was-resolved")
			Expect(stored("was-resolved").Status.Provider).To(Equal("confluence"))

			Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
			reconcileCorpus("was-resolved")
			Expect(stored("was-resolved").Status.Provider).To(BeEmpty())
		})
	})

	// The provider-specific shape check CEL can no longer do, because the
	// provider is on another resource.
	Context("scope must match the provider", func() {
		It("degrades a confluence corpus scoped to a channel", func() {
			conn := confluenceConnection("mismatch-src")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, conn) }()

			corpus := corpusFor("mismatch-src", "mismatched")
			corpus.Spec.Scope = corev1alpha1.CorpusScope{Channel: "C6RQKL5BK"}
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("mismatched")

			Expect(stored("mismatched").Status.Conditions).To(ContainElement(
				HaveField("Reason", Equal("ScopeMismatch"))))
		})

		It("accepts a jazzhr corpus scoped to a job", func() {
			conn := jazzhrConnection("jazz-src")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, conn) }()

			corpus := jazzhrCorpusFor("jazz-src", "senior-eng", corev1alpha1.CorpusScope{JobID: "abc123"})
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("senior-eng")
			Expect(stored("senior-eng").Status.Provider).To(Equal("jazzhr"))
		})

	})

	// A wildcard Corpus reaches every unit the Connection permits, so it is not
	// matched against a single allowed unit — only the provider must be known.
	Context("the wildcard scope (scope.all)", func() {
		It("permits a wildcard jazzhr corpus even when the Connection caps jobs", func() {
			conn := jazzhrConnection("jazz-wild")
			conn.Spec.AllowedScopes = &corev1alpha1.ConnectionAllowedScopes{JobIDs: []string{"abc123"}}
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, conn) }()

			corpus := jazzhrCorpusFor("jazz-wild", "all-jobs", corev1alpha1.CorpusScope{All: true})
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("all-jobs")
			Expect(stored("all-jobs").Status.Provider).To(Equal("jazzhr"))
		})

		It("still rejects a NAMED job outside the cap", func() {
			conn := jazzhrConnection("jazz-capped")
			conn.Spec.AllowedScopes = &corev1alpha1.ConnectionAllowedScopes{JobIDs: []string{"permitted"}}
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, conn) }()

			corpus := jazzhrCorpusFor("jazz-capped", "outside", corev1alpha1.CorpusScope{JobID: "forbidden"})
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("outside")
			Expect(stored("outside").Status.Conditions).To(ContainElement(
				HaveField("Reason", Equal("OutsideAllowedScopes"))))
		})

		It("degrades an UNCAPPED wildcard gdrive corpus — Drive cannot be enumerated", func() {
			// gdrive is the one provider whose wildcard requires a cap; the broker
			// refuses an uncapped one, so admission must too rather than deferring
			// to a failed sync.
			conn := &corev1alpha1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: "gdrive-wild-uncapped", Namespace: "default"},
				Spec:       corev1alpha1.ConnectionSpec{Provider: "gdrive", DisplayName: "Fake Drive"},
			}
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, conn) }()

			corpus := jazzhrCorpusFor("gdrive-wild-uncapped", "drive-all", corev1alpha1.CorpusScope{All: true})
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("drive-all")
			Expect(stored("drive-all").Status.Conditions).To(ContainElement(
				HaveField("Reason", Equal("ScopeMismatch"))))
		})

		It("admits a wildcard gdrive corpus when the Connection caps its folders", func() {
			conn := &corev1alpha1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: "gdrive-wild-capped", Namespace: "default"},
				Spec: corev1alpha1.ConnectionSpec{
					Provider:      "gdrive",
					AllowedScopes: &corev1alpha1.ConnectionAllowedScopes{FolderIDs: []string{"FROOT"}},
				},
			}
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, conn) }()

			corpus := jazzhrCorpusFor("gdrive-wild-capped", "drive-capped", corev1alpha1.CorpusScope{All: true})
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("drive-capped")
			Expect(stored("drive-capped").Status.Provider).To(Equal("gdrive"))
		})
	})

	// The credential holder decides what the credential may pull (ADR 0043 §3).
	Context("the Connection's allowedScopes cap", func() {
		It("refuses a subset the Connection does not permit", func() {
			conn := confluenceConnection("capped")
			conn.Spec.AllowedScopes = &corev1alpha1.ConnectionAllowedScopes{Spaces: []string{"PERMITTED"}}
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, conn) }()

			corpus := corpusFor("capped", "outside-cap")
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed()) // scope.space is "GLOBEX"
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("outside-cap")

			Expect(stored("outside-cap").Status.Conditions).To(ContainElement(
				HaveField("Reason", Equal("OutsideAllowedScopes"))))
		})

		It("permits a subset inside the cap", func() {
			conn := confluenceConnection("capped-ok")
			conn.Spec.AllowedScopes = &corev1alpha1.ConnectionAllowedScopes{Spaces: []string{"GLOBEX"}}
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, conn) }()

			corpus := corpusFor("capped-ok", "inside-cap")
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("inside-cap")
			Expect(stored("inside-cap").Status.Provider).To(Equal("confluence"))
		})

		It("permits anything when the Connection sets no cap", func() {
			// An absent allowlist means no cap, not an empty one: the cap is
			// opt-in, and reading it the other way would break every Connection
			// that never set one.
			conn := confluenceConnection("uncapped")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, conn) }()

			corpus := corpusFor("uncapped", "no-cap-anything")
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, corpus) }()

			reconcileCorpus("no-cap-anything")
			Expect(stored("no-cap-anything").Status.Provider).To(Equal("confluence"))
		})
	})

	Context("sync validation", func() {
		It("rejects webhook mode without a reconcile interval", func() {
			corpus := corpusFor("anything", "webhook-no-interval")
			corpus.Spec.Sync = &corev1alpha1.CorpusSync{Mode: corev1alpha1.CorpusSyncWebhook}
			Expect(k8sClient.Create(ctx, corpus)).To(
				MatchError(ContainSubstring("reconcileInterval is required")))
		})

		It("accepts mode none without one, since it indexes nothing", func() {
			corpus := corpusFor("anything", "sync-none")
			corpus.Spec.Sync = &corev1alpha1.CorpusSync{Mode: corev1alpha1.CorpusSyncNone}
			Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
			Expect(k8sClient.Delete(ctx, corpus)).To(Succeed())
		})
	})
})

// The periodic reconcile used to be a CronJob per Corpus kicking the broker,
// on top of the broker's own timer. It is now the broker's per-Corpus
// Temporal workflow (docs/adr/0058) alone, so the controller creates no
// CronJob and removes the one an earlier release left behind.
var _ = Describe("Corpus sync CronJob (legacy)", func() {
	ctx := context.Background()

	reconcileCorpus := func(name string) {
		r := &CorpusReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: "default"},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	cronJob := func(corpus string) (*batchv1.CronJob, error) {
		var cj batchv1.CronJob
		err := k8sClient.Get(ctx,
			types.NamespacedName{Name: LegacySyncCronJobName(corpus), Namespace: "default"}, &cj)
		return &cj, err
	}

	syncing := func(connectionRef, name string, interval time.Duration) *corev1alpha1.Corpus {
		corpus := corpusFor(connectionRef, name)
		corpus.Spec.Sync = &corev1alpha1.CorpusSync{
			Mode:              corev1alpha1.CorpusSyncPoll,
			ReconcileInterval: &metav1.Duration{Duration: interval},
		}
		return corpus
	}

	legacyCronJob := func(corpus string, owner metav1.OwnerReference) *batchv1.CronJob {
		return &batchv1.CronJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:            LegacySyncCronJobName(corpus),
				Namespace:       "default",
				Labels:          map[string]string{"core.aktis.ai/corpus": corpus},
				OwnerReferences: []metav1.OwnerReference{owner},
			},
			Spec: batchv1.CronJobSpec{
				Schedule: "0 * * * *",
				JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: "kick", Image: "curlimages/curl:8.11.1"}},
					},
				}}},
			},
		}
	}

	It("creates no CronJob for a syncing Corpus", func() {
		conn := confluenceConnection("cron-src")
		Expect(k8sClient.Create(ctx, conn)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, conn) }()

		corpus := syncing("cron-src", "cron-none-made", 6*time.Hour)
		Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, corpus) }()

		reconcileCorpus("cron-none-made")

		_, err := cronJob("cron-none-made")
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("removes the sync CronJob an earlier release created", func() {
		// Left in place it would keep kicking a full pass on top of the
		// broker's workflow -- the two-schedulers problem this replaces.
		conn := confluenceConnection("cron-legacy-src")
		Expect(k8sClient.Create(ctx, conn)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, conn) }()

		corpus := syncing("cron-legacy-src", "cron-legacy", time.Hour)
		Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, corpus) }()

		isController := true
		stale := legacyCronJob("cron-legacy", metav1.OwnerReference{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "Corpus",
			Name:       corpus.Name,
			UID:        corpus.UID,
			Controller: &isController,
		})
		Expect(k8sClient.Create(ctx, stale)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, stale) }()

		reconcileCorpus("cron-legacy")

		_, err := cronJob("cron-legacy")
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("leaves a same-named CronJob it does not control alone", func() {
		conn := confluenceConnection("cron-foreign-src")
		Expect(k8sClient.Create(ctx, conn)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, conn) }()

		corpus := syncing("cron-foreign-src", "cron-foreign", time.Hour)
		Expect(k8sClient.Create(ctx, corpus)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, corpus) }()

		isController := true
		foreign := legacyCronJob("cron-foreign", metav1.OwnerReference{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       "someone-else",
			UID:        types.UID("00000000-0000-0000-0000-00000000beef"),
			Controller: &isController,
		})
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, foreign) }()

		reconcileCorpus("cron-foreign")

		_, err := cronJob("cron-foreign")
		Expect(err).NotTo(HaveOccurred())
	})
})
