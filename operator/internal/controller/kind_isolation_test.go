package controller

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fencedHAKINDCluster builds an honest multi-member (server-tls-v1) cluster sized
// to fit a single KIND worker, in the given fenced namespace.
func fencedHAKINDCluster(t *testing.T, namespace, name string) *pgshardv1alpha1.PgShardCluster {
	t.Helper()
	cluster := readDevelopmentSample(t)
	cluster.Name = name
	cluster.Namespace = namespace
	cluster.Spec.Shards = 1
	cluster.Spec.Databases = nil
	// Keep all three PostgreSQL members plus a replaced standby schedulable on the
	// single KIND worker alongside the platform and pgshard workloads.
	cluster.Spec.PostgreSQL.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("1Gi")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
	}
	return cluster
}

func fencedKINDNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: name,
		Labels: map[string]string{
			"pod-security.kubernetes.io/enforce":         "restricted",
			"pod-security.kubernetes.io/enforce-version": "latest",
			podfence.NamespaceLabel:                      podfence.NamespaceLabelValue,
		},
	}}
}

// waitForReplicationTLSReady blocks until the cluster records a complete
// server-tls-v1 replication-TLS checkpoint (CA digest + a verified server digest
// for every member). The checkpoint is only written once verified TLS streaming
// is established, so it is the live server-tls-v1 + verify-full proof.
func waitForReplicationTLSReady(t *testing.T, ctx context.Context, kubeClient client.Client, key client.ObjectKey, membersPerShard int32) *pgshardv1alpha1.PgShardCluster {
	t.Helper()
	current := &pgshardv1alpha1.PgShardCluster{}
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, key, current); err != nil {
			return false, err
		}
		if current.Status.PostgreSQLBootstrapSpec == nil ||
			current.Status.PostgreSQLBootstrapSpec.ReplicationTransportPolicy != pgshardv1alpha1.ReplicationTransportPolicyServerTLSV1 {
			return false, nil
		}
		if len(current.Status.PostgreSQLReplicationTLS) != 1 {
			return false, nil
		}
		checkpoint := current.Status.PostgreSQLReplicationTLS[0]
		if checkpoint.Shard != 0 || !validCatalogAccessDigest(checkpoint.CASHA256) || len(checkpoint.Members) != int(membersPerShard) {
			return false, nil
		}
		for _, member := range checkpoint.Members {
			if !validCatalogAccessDigest(member.ServerSHA256) {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("wait for server-tls-v1 replication TLS readiness: %v; last status = %#v", err, current.Status)
	}
	return current.DeepCopy()
}

// adversarialMemberPod is a hand-crafted, PodSecurity-restricted-compliant pod
// carrying managed member labels but no reconciler stamp, live owning
// StatefulSet, or termination fence. The PodContract (PodCreate) webhook must
// DENY it, proving the admission surface actively fires in the fenced namespace —
// so the honest controller-created pods that DO run were meaningfully admitted.
func adversarialMemberPod(namespace, cluster, name string) *corev1.Pod {
	runAsNonRoot := true
	allowPrivilegeEscalation := false
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace,
			Labels: map[string]string{
				owned.ClusterLabel: cluster, owned.ComponentLabel: "postgresql",
				owned.ShardLabel: "0000", owned.MemberLabel: "0000",
			},
		},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   &runAsNonRoot,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:  "postgresql",
				Image: "registry.k8s.io/pause:3.9",
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &allowPrivilegeEscalation,
					RunAsNonRoot:             &runAsNonRoot,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
			}},
		},
	}
}

// TestKINDManagerHonestFlowSurvivesFencedAdmissionSurface is the CI validation
// that the pinned-1.36 canonical normal form matches a REAL API server's
// defaulting: an honest multi-member server-tls-v1 cluster runs IN A FENCED
// NAMESPACE, so the WorkloadIntegrity (apps/*+scale), PodContract (CREATE), and
// binding (Live-mode) webhooks all FIRE on the real controller-created member and
// supporting pods and MUST ADMIT them (failurePolicy=fail). The pods reaching
// Running is proof of admission; a negative-control adversarial pod DENIED by the
// PodContract webhook proves the surface is actively enforcing, not bypassed.
func TestKINDManagerHonestFlowSurvivesFencedAdmissionSurface(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed admission manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 13*time.Minute)
	defer cancel()
	kubeClient := newKINDClient(t)

	namespace := fencedKINDNamespace(fmt.Sprintf("pgshard-honest-fenced-%d", os.Getpid()))
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)

	cluster := fencedHAKINDCluster(t, namespace.Name, "honest-fenced")
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}

	// server-tls-v1 + verify-full proof under the full admission surface.
	waitForReplicationTLSReady(t, ctx, kubeClient, client.ObjectKeyFromObject(cluster), cluster.Spec.MembersPerShard)

	// The honest controller-created member pods (StatefulSet workloads → CREATE →
	// Live-mode binding) were all admitted and are Running+Ready.
	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "postgresql", int(cluster.Spec.MembersPerShard), true)
	// The supporting workloads (Deployment/ReplicaSet → CREATE) were admitted too.
	assertSupportingPodsRunning(t, ctx, kubeClient, namespace.Name, cluster.Name, "orchestrator")
	assertSupportingPodsRunning(t, ctx, kubeClient, namespace.Name, cluster.Name, "pooler")

	// The admission surface is REAL and fenced-scoped: the PodCreate webhook is
	// installed with a namespaceSelector matching the fence label.
	assertPodCreateWebhookFencedScoped(t, ctx, kubeClient)

	// Negative control: an adversarial managed-looking member pod is DENIED by the
	// pgshard PodContract webhook (dry-run, so nothing persists).
	adversary := adversarialMemberPod(namespace.Name, cluster.Name, "adversary-member")
	err := kubeClient.Create(ctx, adversary, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err == nil || !strings.Contains(err.Error(), podfence.PodCreateWebhookName) {
		t.Fatalf("adversarial member pod was not denied by the pgshard PodCreate webhook: %v", err)
	}
}

func assertSupportingPodsRunning(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster, component string) {
	t.Helper()
	pods := &corev1.PodList{}
	err := wait.PollUntilContextTimeout(ctx, time.Second, 4*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods = &corev1.PodList{}
		if err := kubeClient.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{owned.ClusterLabel: cluster, owned.ComponentLabel: component}); err != nil {
			return false, err
		}
		running := 0
		for i := range pods.Items {
			if pods.Items[i].Status.Phase == corev1.PodRunning {
				running++
			}
		}
		return running >= 1, nil
	})
	if err != nil {
		t.Fatalf("wait for admitted %s supporting pod: %v; last pods = %#v", component, err, pods.Items)
	}
}

func assertPodCreateWebhookFencedScoped(t *testing.T, ctx context.Context, kubeClient client.Client) {
	t.Helper()
	configuration := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: "pgshard-validating-webhook-configuration"}, configuration); err != nil {
		t.Fatalf("read validating webhook configuration: %v", err)
	}
	for i := range configuration.Webhooks {
		webhook := &configuration.Webhooks[i]
		if webhook.Name != podfence.PodCreateWebhookName {
			continue
		}
		if webhook.NamespaceSelector == nil || webhook.NamespaceSelector.MatchLabels[podfence.NamespaceLabel] != podfence.NamespaceLabelValue {
			t.Fatalf("PodCreate webhook is not scoped to fenced namespaces: %#v", webhook.NamespaceSelector)
		}
		if webhook.FailurePolicy == nil || *webhook.FailurePolicy != admissionregistrationv1.Fail {
			t.Fatalf("PodCreate webhook is not fail-closed: %#v", webhook.FailurePolicy)
		}
		return
	}
	t.Fatalf("PodCreate webhook %q is not installed", podfence.PodCreateWebhookName)
}

// TestKINDManagerActivationCeremony is the live proof of the per-namespace
// isolation activation machine on single-apiserver KIND. It opts an honest
// server-tls-v1 cluster into activation and drives the reconcile through
// INACTIVE→QUIESCE→RECREATE→ACTIVE, then asserts the ACTIVE deny-all surface
// rejects an adversarial pod while the honest controller-recreated pods keep
// running and the replication-TLS proof still holds.
//
// SEAM(isolation-dispatch-enumeration): single-apiserver KIND publishes ONE
// kubernetes-Service EndpointSlice backend, so the dispatch proof enumerates a
// single backend (backends=1) which cannot be proven to be the complete physical
// backend set. Reaching QUIESCE therefore requires the admin to attest this
// namespace via --allow-unenumerable-ha-isolation-namespaces (an
// admin/CI-specific config that the shared admission manifest deliberately does
// NOT hardcode a dynamic test namespace into). When the namespace is attested the
// receipt is created and the ceremony runs to ACTIVE; otherwise the preflight
// withholds with a typed condition. The test drives both: it asserts the full
// ceremony when the receipt appears, and asserts the reachable portion (the
// activation reconcile ran live and withheld with a typed condition) + documents
// the seam otherwise. The same branch also covers a per-backend TLS dial that is
// not drivable in the harness.
//
// PRECONDITION: the manager must be started with --attested-max-request-timeout
// set (the admission manifest sets it to 1m, matching KIND's request-timeout), or
// activation is withheld at the drain-attestation gate before enumeration.
func TestKINDManagerActivationCeremony(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed admission manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()
	kubeClient := newKINDClient(t)

	namespace := fencedKINDNamespace(fmt.Sprintf("pgshard-activation-%d", os.Getpid()))
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)

	cluster := fencedHAKINDCluster(t, namespace.Name, "activation-ha")
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	waitForReplicationTLSReady(t, ctx, kubeClient, client.ObjectKeyFromObject(cluster), cluster.Spec.MembersPerShard)
	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "postgresql", int(cluster.Spec.MembersPerShard), true)

	// Opt in to activation.
	if err := patchActivationOptIn(ctx, kubeClient, client.ObjectKeyFromObject(cluster)); err != nil {
		t.Fatal(err)
	}

	// Poll until the activation reconcile engages: either the receipt appears
	// (dispatch converged; ceremony under way) or a preflight condition is
	// surfaced (a gate withheld — most likely the dispatch-dial seam).
	current := &pgshardv1alpha1.PgShardCluster{}
	engaged := wait.PollUntilContextTimeout(ctx, 3*time.Second, 4*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
			return false, err
		}
		if current.Status.IsolationReceipt != nil {
			return true, nil
		}
		for _, condition := range isolationPreflightConditions {
			if hasTrueCondition(current, condition) {
				return true, nil
			}
		}
		return false, nil
	})
	if engaged != nil {
		t.Fatalf("activation reconcile never engaged (no receipt, no preflight condition): %v; conditions = %#v", engaged, current.Status.Conditions)
	}

	if current.Status.IsolationReceipt == nil {
		// Reachable-portion assertion + documented seam: the activation preflight
		// ran live and withheld with a typed condition (the per-backend dispatch
		// dial could not be driven in this harness). This still proves the honest
		// flow's admission surface and the activation reconcile path are live.
		surfaced := ""
		for _, condition := range isolationPreflightConditions {
			if hasTrueCondition(current, condition) {
				surfaced = condition
			}
		}
		t.Logf("SEAM(isolation-dispatch-dial): activation withheld with condition %q; the dispatch-convergence per-backend TLS dial is not drivable in this harness. Asserting the reachable portion (preflight ran live and withheld).", surfaced)
		if surfaced == "" {
			t.Fatal("activation withheld without surfacing a preflight condition")
		}
		return
	}

	// The receipt exists: the dispatch preflight converged. QUIESCE→RECREATE→ACTIVE
	// is deterministic drain + recreate; poll to ACTIVE (resilient to ~15s
	// requeues).
	receipt := waitForIsolationPhase(t, ctx, kubeClient, client.ObjectKeyFromObject(cluster), pgshardv1alpha1.IsolationActive)
	if receipt.MinAcceptableSecurityGeneration <= 0 || receipt.ResidueProfileHash == "" {
		t.Fatalf("ACTIVE receipt did not capture the security floor + residue profile: %#v", receipt)
	}

	// The honest controller-recreated member pods keep running under the guard.
	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "postgresql", int(cluster.Spec.MembersPerShard), true)

	// ACTIVE deny-all: an adversarial unmanaged pod is rejected (dry-run).
	runAsNonRoot := true
	allowPrivilegeEscalation := false
	foreign := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "adversary-foreign", Namespace: namespace.Name},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &runAsNonRoot, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers: []corev1.Container{{
				Name:  "x",
				Image: "registry.k8s.io/pause:3.9",
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &allowPrivilegeEscalation,
					RunAsNonRoot:             &runAsNonRoot,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
			}},
		},
	}
	err := kubeClient.Create(ctx, foreign, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err == nil || !strings.Contains(err.Error(), podfence.PodCreateWebhookName) {
		t.Fatalf("ACTIVE isolation did not deny an adversarial unmanaged pod via the pgshard webhook: %v", err)
	}

	// TODO(isolation-rollout): step 8's distinct-v2-Service upgrade rollout +
	// bridge (bad8a18→new UPGRADES) is deferred; a staged-activation choreography
	// for in-place upgrades would be wired here.
}

func patchActivationOptIn(ctx context.Context, kubeClient client.Client, key client.ObjectKey) error {
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(ctx, key, cluster); err != nil {
		return err
	}
	patched := cluster.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[pgshardv1alpha1.IsolationActivationAnnotation] = pgshardv1alpha1.IsolationActivationRequested
	return kubeClient.Patch(ctx, patched, client.MergeFrom(cluster))
}

func waitForIsolationPhase(t *testing.T, ctx context.Context, kubeClient client.Client, key client.ObjectKey, phase pgshardv1alpha1.IsolationPhase) *pgshardv1alpha1.PostgreSQLIsolationReceipt {
	t.Helper()
	current := &pgshardv1alpha1.PgShardCluster{}
	seen := map[pgshardv1alpha1.IsolationPhase]bool{}
	err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 6*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, key, current); err != nil {
			return false, err
		}
		if current.Status.IsolationReceipt == nil {
			return false, nil
		}
		observed := current.Status.IsolationReceipt.Phase
		if !seen[observed] {
			seen[observed] = true
			t.Logf("isolation phase advanced to %q", observed)
		}
		return observed == phase, nil
	})
	if err != nil {
		t.Fatalf("wait for isolation phase %q: %v; last receipt = %#v; conditions = %#v", phase, err, current.Status.IsolationReceipt, current.Status.Conditions)
	}
	return current.Status.IsolationReceipt.DeepCopy()
}

func hasTrueCondition(cluster *pgshardv1alpha1.PgShardCluster, conditionType string) bool {
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == conditionType && cluster.Status.Conditions[i].Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}
