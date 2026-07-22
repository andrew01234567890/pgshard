package controller

import (
	"context"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
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
	if reloadReceipt(t, kubeClient, cluster) != nil {
		t.Fatal("the receipt was not reset after tuple invalidation")
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(reloaded.Status.Conditions, isolationDispatchNotConvergedCondition) == nil {
		t.Fatalf("tuple invalidation did not surface the dispatch condition: %#v", reloaded.Status.Conditions)
	}
}

func TestReconcileIsolationActivationOptInEntersQuiesce(t *testing.T) {
	t.Parallel()
	cluster := genCluster("optincase", "optincase-uid")
	cluster.Annotations = map[string]string{pgshardv1alpha1.IsolationActivationAnnotation: pgshardv1alpha1.IsolationActivationRequested}
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
