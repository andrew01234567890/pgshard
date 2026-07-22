package controller

import (
	"context"
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/version"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type fakeServerVersion struct {
	info *version.Info
	err  error
}

func (f fakeServerVersion) ServerVersion() (*version.Info, error) { return f.info, f.err }

type fakeMinorGate struct {
	ok       bool
	observed string
	err      error
}

func (f fakeMinorGate) SupportedMinor(ctx context.Context) (bool, string, error) {
	return f.ok, f.observed, f.err
}

type fakeIdentityProber struct {
	matched bool
	detail  string
	err     error
}

func (f fakeIdentityProber) Probe(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, string, error) {
	return f.matched, f.detail, f.err
}

func TestParseMinor(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]int{"36": 36, "36+": 36, "36.2": 36, "29": 29} {
		if got, ok := parseMinor(raw); !ok || got != want {
			t.Fatalf("parseMinor(%q) = %d,%v want %d", raw, got, ok, want)
		}
	}
	if _, ok := parseMinor("v"); ok {
		t.Fatal("parseMinor accepted a non-numeric minor")
	}
}

func TestServerVersionGateRange(t *testing.T) {
	t.Parallel()
	gate := NewServerVersionGate(fakeServerVersion{info: &version.Info{Major: "1", Minor: "36", GitVersion: "v1.36.2"}})
	if ok, _, err := gate.SupportedMinor(context.Background()); err != nil || !ok {
		t.Fatalf("supported minor rejected: ok=%v err=%v", ok, err)
	}
	tooOld := NewServerVersionGate(fakeServerVersion{info: &version.Info{Major: "1", Minor: "29", GitVersion: "v1.29.0"}})
	if ok, observed, err := tooOld.SupportedMinor(context.Background()); err != nil || ok || observed != "v1.29.0" {
		t.Fatalf("out-of-range minor accepted: ok=%v observed=%q err=%v", ok, observed, err)
	}
}

func TestAggregateDispatchProof(t *testing.T) {
	t.Parallel()
	allSentinel := []backendProbe{
		{sliceName: "kubernetes", sliceRV: "1", address: "10.0.0.1", port: 443, sentinelObserved: true, outcome: "sentinel"},
		{sliceName: "kubernetes", sliceRV: "1", address: "10.0.0.2", port: 443, sentinelObserved: true, outcome: "sentinel"},
	}
	if proof := aggregateDispatchProof("rv7", allSentinel); !proof.converged {
		t.Fatalf("all-backends-sentinel not converged: %#v", proof)
	}
	oneAllowed := []backendProbe{
		{sliceName: "kubernetes", sliceRV: "1", address: "10.0.0.1", port: 443, sentinelObserved: true, outcome: "sentinel"},
		{sliceName: "kubernetes", sliceRV: "1", address: "10.0.0.2", port: 443, sentinelObserved: false, outcome: "admitted"},
	}
	if proof := aggregateDispatchProof("rv7", oneAllowed); proof.converged {
		t.Fatalf("one-backend-allow reported converged: %#v", proof)
	}
	if proof := aggregateDispatchProof("rv7", nil); proof.converged || proof.reason != dispatchUnconvergedReasonUnsupportedHA {
		t.Fatalf("an empty backend set was not treated as unsupported HA: %#v", proof)
	}
}

func TestDispatchTupleHashBinding(t *testing.T) {
	t.Parallel()
	base := []backendProbe{{sliceName: "kubernetes", sliceRV: "1", address: "10.0.0.1", port: 443}}
	stable := dispatchTupleHash("rv7", base)
	if stable != dispatchTupleHash("rv7", base) {
		t.Fatal("tuple hash is not deterministic")
	}
	// A new backend, a changed EndpointSlice RV, or a changed webhook-config RV all
	// change the tuple.
	if dispatchTupleHash("rv7", append(base, backendProbe{sliceName: "kubernetes", sliceRV: "1", address: "10.0.0.2", port: 443})) == stable {
		t.Fatal("a backend-set change did not change the tuple")
	}
	if dispatchTupleHash("rv8", base) == stable {
		t.Fatal("a webhook-config RV change did not change the tuple")
	}
	if dispatchTupleHash("rv7", []backendProbe{{sliceName: "kubernetes", sliceRV: "2", address: "10.0.0.1", port: 443}}) == stable {
		t.Fatal("an EndpointSlice RV change did not change the tuple")
	}
}

func TestIdentitiesMatch(t *testing.T) {
	t.Parallel()
	configured := controllerIdentitySet{
		statefulSet: "system:serviceaccount:kube-system:statefulset-controller",
		replicaSet:  "system:serviceaccount:kube-system:replicaset-controller",
		deployment:  "system:serviceaccount:kube-system:deployment-controller",
		hpa:         "system:node:hpa",
	}
	if matched, detail := identitiesMatch(configured, configured); !matched {
		t.Fatalf("matching identities rejected: %s", detail)
	}
	wrong := configured
	wrong.deployment = "system:serviceaccount:kube-system:impostor"
	if matched, detail := identitiesMatch(wrong, configured); matched || detail == "" {
		t.Fatalf("deployment-controller mismatch accepted: %v %q", matched, detail)
	}
	blank := configured
	blank.hpa = ""
	if matched, _ := identitiesMatch(blank, configured); matched {
		t.Fatal("an unobserved controller was accepted")
	}
	badConfigured := configured
	badConfigured.statefulSet = "mallory"
	if matched, _ := identitiesMatch(badConfigured, badConfigured); matched {
		t.Fatal("a non-principal configured identity was accepted")
	}
}

func preflightReconciler() *PgShardClusterReconciler {
	return &PgShardClusterReconciler{
		MinorGate:      fakeMinorGate{ok: true, observed: "v1.36.2"},
		IdentityProber: fakeIdentityProber{matched: true},
		DispatchProber: fakeDispatchProber{proof: dispatchProof{converged: true, tupleHash: "tuple-abc"}},
	}
}

func TestPreflightConvergedComposition(t *testing.T) {
	t.Parallel()
	cluster := genCluster("preflight", "preflight-uid")

	if proof, ok := preflightReconciler().preflightConverged(context.Background(), cluster); !ok || proof.tupleHash != "tuple-abc" {
		t.Fatalf("all-gates-pass preflight withheld: ok=%v proof=%#v", ok, proof)
	}

	for name, mutate := range map[string]func(*PgShardClusterReconciler){
		"minor unsupported": func(r *PgShardClusterReconciler) {
			r.MinorGate = fakeMinorGate{ok: false, observed: "v1.29.0"}
		},
		"identity mismatch": func(r *PgShardClusterReconciler) {
			r.IdentityProber = fakeIdentityProber{matched: false, detail: "deployment-controller observed mallory"}
		},
		"dispatch unconverged": func(r *PgShardClusterReconciler) {
			r.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: false, reason: "backend allowed"}}
		},
		"ha unsupported": func(r *PgShardClusterReconciler) {
			r.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: false, reason: dispatchUnconvergedReasonUnsupportedHA}}
		},
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			blocked := genCluster("preflight", "preflight-uid")
			reconciler := preflightReconciler()
			mutate(reconciler)
			if _, ok := reconciler.preflightConverged(context.Background(), blocked); ok {
				t.Fatalf("%s did not withhold activation", name)
			}
			wantCondition := map[string]string{
				"minor unsupported":    isolationMinorUnsupportedCondition,
				"identity mismatch":    isolationControllerIdentityMismatchCond,
				"dispatch unconverged": isolationDispatchNotConvergedCondition,
				"ha unsupported":       isolationHAUnsupportedCondition,
			}[name]
			if condition := meta.FindStatusCondition(blocked.Status.Conditions, wantCondition); condition == nil {
				t.Fatalf("%s did not surface %q: %#v", name, wantCondition, blocked.Status.Conditions)
			}
		})
	}
}

func TestRevalidateDispatchTupleInvalidation(t *testing.T) {
	t.Parallel()
	cluster := genCluster("tuplecase", "tuplecase-uid")
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActivatingQuiesce, DispatchTupleHash: "tuple-old",
	}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	// The backend set changed mid-activation: the prober now reports a new tuple.
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, tupleHash: "tuple-new"}}

	valid, err := reconciler.revalidateDispatchTuple(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if valid {
		t.Fatal("a changed dispatch tuple was treated as still valid")
	}
	// The receipt is HELD in a durable-deny phase (QUIESCE), never reset to INACTIVE:
	// enforcement must not drop to fail-open while the backend set is in flux. Because
	// the new tuple is itself converged, it is re-sealed under it while quiesced.
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt == nil {
		t.Fatal("the receipt was dropped (fail-open) after tuple invalidation")
	}
	if receipt.Phase != pgshardv1alpha1.IsolationActivatingQuiesce {
		t.Fatalf("receipt was not held quiesced after tuple invalidation: %#v", receipt)
	}
	if receipt.DispatchTupleHash != "tuple-new" {
		t.Fatalf("receipt was not re-sealed under the new converged tuple: %#v", receipt)
	}
	if receipt.SealedParents != nil {
		t.Fatalf("sealed parents were not cleared for re-enumeration: %#v", receipt)
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(reloaded.Status.Conditions, isolationDispatchNotConvergedCondition) == nil {
		t.Fatalf("tuple invalidation did not surface the dispatch condition: %#v", reloaded.Status.Conditions)
	}
}

// tlsReadyActivationCluster is a multi-member cluster that satisfies the
// activation prerequisites: opted in, server-tls-v1 transport, complete TLS
// checkpoints.
func tlsReadyActivationCluster(name string, uid types.UID) *pgshardv1alpha1.PgShardCluster {
	cluster := genCluster(name, uid)
	cluster.Spec.MembersPerShard = 3
	cluster.Annotations = map[string]string{pgshardv1alpha1.IsolationActivationAnnotation: pgshardv1alpha1.IsolationActivationRequested}
	cluster.Status.PostgreSQLBootstrapSpec = &pgshardv1alpha1.PostgreSQLBootstrapSpecStatus{ReplicationTransportPolicy: pgshardv1alpha1.ReplicationTransportPolicyServerTLSV1}
	cluster.Status.PostgreSQLReplicationTLS = []pgshardv1alpha1.PostgreSQLReplicationTLSStatus{{
		Shard: 0, CASecretName: name + "-replication-ca", CASHA256: strings.Repeat("a", 64),
		Members: []pgshardv1alpha1.PostgreSQLReplicationTLSMemberStatus{{Member: 0, ServerSHA256: strings.Repeat("b", 64)}},
	}}
	return cluster
}

func TestReconcileIsolationActivationOptInEntersQuiesce(t *testing.T) {
	t.Parallel()
	cluster := tlsReadyActivationCluster("optincase", "optincase-uid")
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	reconciler.MinorGate = fakeMinorGate{ok: true, observed: "v1.36.2"}
	reconciler.IdentityProber = fakeIdentityProber{matched: true}
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, tupleHash: "tuple-abc"}}

	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt == nil || receipt.Phase != pgshardv1alpha1.IsolationActivatingQuiesce {
		t.Fatalf("opted-in cluster did not enter quiesce: %#v", receipt)
	}
	if receipt.NamespaceUID != "ns-uid" || receipt.DispatchTupleHash != "tuple-abc" {
		t.Fatalf("quiesce receipt not bound to namespace/tuple: %#v", receipt)
	}
}

func TestReconcileIsolationActivationOptOutStaysInactive(t *testing.T) {
	t.Parallel()
	cluster := genCluster("optoutcase", "optoutcase-uid")
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	// Even with a converged preflight, a cluster that has not opted in never
	// activates (the eligibility gate short-circuits before the probers run).
	reconciler.MinorGate = fakeMinorGate{ok: true}
	reconciler.IdentityProber = fakeIdentityProber{matched: true}
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, tupleHash: "tuple-abc"}}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if reloadReceipt(t, kubeClient, cluster) != nil {
		t.Fatal("a non-opted-in cluster activated")
	}
}

func TestActivationWithheldWithoutTLSPrerequisite(t *testing.T) {
	t.Parallel()
	// Opted in but legacy cleartext (no server-tls-v1 / no checkpoints).
	cluster := genCluster("cleartextcase", "cleartextcase-uid")
	cluster.Spec.MembersPerShard = 3
	cluster.Annotations = map[string]string{pgshardv1alpha1.IsolationActivationAnnotation: pgshardv1alpha1.IsolationActivationRequested}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	reconciler.MinorGate = fakeMinorGate{ok: true}
	reconciler.IdentityProber = fakeIdentityProber{matched: true}
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, tupleHash: "t"}}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if reloadReceipt(t, kubeClient, cluster) != nil {
		t.Fatal("a legacy cleartext cluster received an isolation receipt")
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(reloaded.Status.Conditions, isolationTLSPrerequisiteCondition) == nil {
		t.Fatalf("TLS prerequisite condition not surfaced: %#v", reloaded.Status.Conditions)
	}
}

func TestActivationWithheldWithMultipleClusters(t *testing.T) {
	t.Parallel()
	clusterA := tlsReadyActivationCluster("clustera", "clustera-uid")
	clusterB := tlsReadyActivationCluster("clusterb", "clusterb-uid")
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), clusterA, clusterB)
	reconciler.MinorGate = fakeMinorGate{ok: true}
	reconciler.IdentityProber = fakeIdentityProber{matched: true}
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, tupleHash: "t"}}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), clusterA); err != nil {
		t.Fatal(err)
	}
	if reloadReceipt(t, kubeClient, clusterA) != nil {
		t.Fatal("activation proceeded with multiple clusters in the namespace")
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(clusterA), reloaded); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(reloaded.Status.Conditions, isolationMultipleClustersCondition) == nil {
		t.Fatalf("multiple-clusters condition not surfaced: %#v", reloaded.Status.Conditions)
	}
}

func TestActivationWithheldWithLimitRange(t *testing.T) {
	t.Parallel()
	cluster := tlsReadyActivationCluster("limitcase", "limitcase-uid")
	limitRange := &corev1.LimitRange{ObjectMeta: metav1.ObjectMeta{Name: "defaults", Namespace: genTestNamespace}}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster, limitRange)
	reconciler.MinorGate = fakeMinorGate{ok: true}
	reconciler.IdentityProber = fakeIdentityProber{matched: true}
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, tupleHash: "t"}}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if reloadReceipt(t, kubeClient, cluster) != nil {
		t.Fatal("activation proceeded while a LimitRange was present")
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(reloaded.Status.Conditions, isolationLimitRangePresentCondition) == nil {
		t.Fatalf("limit-range condition not surfaced: %#v", reloaded.Status.Conditions)
	}
}

func TestActivationWithheldWhileSupportingRolling(t *testing.T) {
	t.Parallel()
	cluster := tlsReadyActivationCluster("rollingcase", "rollingcase-uid")
	// A supporting class is mid-roll: its prior generation is still populated, so
	// the admissible set is in flux and activation must be withheld.
	cluster.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", DeploymentUID: "deploy-uid",
		CurrentReplicaSetUID: "rs-b", CurrentContractHash: genHashB,
		PriorReplicaSetUID: "rs-a", PriorContractHash: genHashA,
	}}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	reconciler.MinorGate = fakeMinorGate{ok: true}
	reconciler.IdentityProber = fakeIdentityProber{matched: true}
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, tupleHash: "t"}}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if reloadReceipt(t, kubeClient, cluster) != nil {
		t.Fatal("activation proceeded while a supporting-generation roll was in progress")
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(reloaded.Status.Conditions, isolationSupportingRollingCondition) == nil {
		t.Fatalf("supporting-rolling condition not surfaced: %#v", reloaded.Status.Conditions)
	}
}
