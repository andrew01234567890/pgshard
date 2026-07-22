package controller

import (
	"context"
	"errors"
	"fmt"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/api/meta"
)

const (
	isolationDispatchNotConvergedCondition  = "IsolationDispatchNotConverged"
	isolationMinorUnsupportedCondition      = "IsolationMinorUnsupported"
	isolationControllerIdentityMismatchCond = "IsolationControllerIdentityMismatch"
	isolationHAUnsupportedCondition         = "IsolationHAUnsupported"
	isolationTLSPrerequisiteCondition       = "IsolationTLSPrerequisiteMissing"
	isolationMultipleClustersCondition      = "IsolationMultipleClusters"
	isolationLimitRangePresentCondition     = "IsolationLimitRangePresent"
	isolationSupportingRollingCondition     = "IsolationSupportingRolling"
	isolationDrainUnattestedCondition       = "IsolationDrainBoundUnattested"
	isolationSealedParentDriftCondition     = "IsolationSealedParentDrift"
	dispatchUnconvergedReasonUnsupportedHA  = "ha-unsupported"
)

var isolationPreflightConditions = []string{
	isolationDispatchNotConvergedCondition,
	isolationMinorUnsupportedCondition,
	isolationControllerIdentityMismatchCond,
	isolationHAUnsupportedCondition,
	isolationTLSPrerequisiteCondition,
	isolationMultipleClustersCondition,
	isolationLimitRangePresentCondition,
	isolationSupportingRollingCondition,
	isolationDrainUnattestedCondition,
	isolationSealedParentDriftCondition,
}

// dispatchProof is the result of a dispatch-convergence probe. converged is true
// only when EVERY live API-server backend returned the exact sentinel denial.
// tupleHash binds the proof to {webhook-config resourceVersion, backend
// EndpointSlice addresses + their resourceVersions}; any change invalidates it.
// backends is the number of enumerated backend addresses: at most one means the
// EndpointSlices do not prove physical-backend enumeration (a lone address may
// be an opaque VIP), which requires the explicit durable single-server
// acknowledgement annotation.
type dispatchProof struct {
	tupleHash string
	converged bool
	backends  int
	reason    string
}

// dispatchProofAccepted layers the enumeration-trust gate onto a converged
// proof: a proof from at most one enumerated backend is accepted only when the
// cluster ADMINISTRATOR has attested the namespace at operator install via the
// --allow-unenumerable-ha-isolation-namespaces flag (UnenumerableHAAckNamespaces).
// The attestation is deliberately NOT sourced from a namespaced PgShardCluster
// annotation, which any principal with cluster update could set to force
// activation over an unsound dispatch proof; the manager flag is admin-controlled
// at install time. It returns ok, or the ha-unsupported detail to surface.
func (r *PgShardClusterReconciler) dispatchProofAccepted(cluster *pgshardv1alpha1.PgShardCluster, proof dispatchProof) (bool, string) {
	if !proof.converged {
		return false, ""
	}
	if proof.backends > 1 {
		return true, ""
	}
	if r.UnenumerableHAAckNamespaces[cluster.Namespace] {
		return true, ""
	}
	return false, fmt.Sprintf("the kubernetes Service EndpointSlices enumerate %d API-server backend(s), which cannot prove physical-backend enumeration (an opaque VIP may hide unproven backends); the cluster administrator must attest namespace %q via the operator's --allow-unenumerable-ha-isolation-namespaces install flag to activate over a single published endpoint", proof.backends, cluster.Namespace)
}

// dispatchProber proves that every live API-server backend dispatches Pod CREATE
// to the pgshard webhook via a per-backend dryRun sentinel probe. The probe runs
// in the activating cluster's FENCED namespace (which the PodCreate selector
// covers), not the operator namespace.
type dispatchProber interface {
	Prove(ctx context.Context, namespace string) (dispatchProof, error)
}

// minorGate reports whether the live API server is within the operator's
// supported Kubernetes minor range.
type minorGate interface {
	SupportedMinor(ctx context.Context) (ok bool, observed string, err error)
}

// controllerIdentityProber creates disposable probe workloads and reports whether
// the controller identities the webhook observes match the configured allowlist.
type controllerIdentityProber interface {
	Probe(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (matched bool, detail string, err error)
}

// preflightConverged composes the whole activation preflight: the build-time
// bridge ceiling AND the supported-minor gate AND the controller-identity probe
// AND dispatch convergence. Every gate is fail-closed and surfaces its own typed
// condition. It returns the dispatch proof (whose tuple binds the receipt) and
// whether activation may proceed.
func (r *PgShardClusterReconciler) preflightConverged(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (dispatchProof, bool) {
	if !isolationBuildAllowsActivation {
		// A bridge build can never activate; this is a deliberate build choice,
		// not a runtime failure, so no failure condition is surfaced.
		return dispatchProof{}, false
	}

	if r.MinorGate == nil {
		r.setIsolationPreflightCondition(cluster, isolationMinorUnsupportedCondition, "Unavailable", "the supported-minor gate is not wired")
		return dispatchProof{}, false
	}
	supported, observed, err := r.MinorGate.SupportedMinor(ctx)
	if err != nil {
		r.setIsolationPreflightCondition(cluster, isolationMinorUnsupportedCondition, "ProbeFailed", fmt.Sprintf("cannot read the API server version: %v", err))
		return dispatchProof{}, false
	}
	if !supported {
		r.setIsolationPreflightCondition(cluster, isolationMinorUnsupportedCondition, "OutOfRange", fmt.Sprintf("API server version %q is outside the supported Kubernetes minor range", observed))
		return dispatchProof{}, false
	}

	if r.IdentityProber == nil {
		r.setIsolationPreflightCondition(cluster, isolationControllerIdentityMismatchCond, "Unavailable", "the controller-identity probe is not wired")
		return dispatchProof{}, false
	}
	matched, detail, err := r.IdentityProber.Probe(ctx, cluster)
	if err != nil {
		r.setIsolationPreflightCondition(cluster, isolationControllerIdentityMismatchCond, "ProbeFailed", fmt.Sprintf("controller-identity probe failed: %v", err))
		return dispatchProof{}, false
	}
	if !matched {
		r.setIsolationPreflightCondition(cluster, isolationControllerIdentityMismatchCond, "Mismatch", detail)
		return dispatchProof{}, false
	}

	if r.DispatchProber == nil {
		r.setIsolationPreflightCondition(cluster, isolationDispatchNotConvergedCondition, "Unavailable", "the dispatch-convergence prober is not wired")
		return dispatchProof{}, false
	}
	proof, err := r.DispatchProber.Prove(ctx, cluster.Namespace)
	if err != nil {
		r.setIsolationPreflightCondition(cluster, isolationDispatchNotConvergedCondition, "ProbeFailed", fmt.Sprintf("dispatch-convergence probe failed: %v", err))
		return proof, false
	}
	if !proof.converged {
		condition := isolationDispatchNotConvergedCondition
		reason := "NotConverged"
		if proof.reason == dispatchUnconvergedReasonUnsupportedHA {
			condition = isolationHAUnsupportedCondition
			reason = "UnsupportedHA"
		}
		r.setIsolationPreflightCondition(cluster, condition, reason, proof.reasonMessage())
		return proof, false
	}
	if ok, detail := r.dispatchProofAccepted(cluster, proof); !ok {
		r.setIsolationPreflightCondition(cluster, isolationHAUnsupportedCondition, "EnumerationUnproven", detail)
		return proof, false
	}
	clearIsolationPreflightConditions(cluster)
	return proof, true
}

// revalidateDispatchTuple re-proves dispatch convergence during an in-progress
// activation (QUIESCE/RECREATE/ACTIVE). It fails CLOSED for BOTH an invalidated
// tuple AND a proof read/confirmation ERROR: it FIRST durably drops the receipt
// out of ACTIVE/RECREATE to ACTIVATING_QUIESCE (which denies every create) and
// clears the seals, THEN returns any probe error — so a transient EndpointSlice/
// VWC read or dispatch-confirmation failure can never leave an ACTIVE receipt
// intact. It never resets to INACTIVE (fail-open). On a clean re-enumeration it
// re-seals under the fresh tuple during the quiesce pass. It returns whether the
// in-progress proof is still valid.
func (r *PgShardClusterReconciler) revalidateDispatchTuple(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	receipt := cluster.Status.IsolationReceipt
	var proof dispatchProof
	var proveErr error
	if r.DispatchProber != nil {
		proof, proveErr = r.DispatchProber.Prove(ctx, cluster.Namespace)
	}
	accepted := false
	ackDetail := ""
	if proveErr == nil {
		accepted, ackDetail = r.dispatchProofAccepted(cluster, proof)
		if r.DispatchProber != nil && accepted && proof.tupleHash == receipt.DispatchTupleHash {
			return true, nil
		}
	}
	// Fail CLOSED. Drop out of ACTIVE/RECREATE to the durable deny phase (QUIESCE)
	// and clear the seals BEFORE returning — on a proof error just as on an
	// invalidated tuple. Never drop to INACTIVE (fail-open).
	receipt.Phase = pgshardv1alpha1.IsolationActivatingQuiesce
	receipt.SealedParents = nil
	receipt.RecreatePendingUIDs = nil
	receipt.ActivatedAt = metav1.Now()
	condition := isolationDispatchNotConvergedCondition
	reason := "TupleInvalidated"
	message := "the dispatch-convergence proof was invalidated (API-server backend set or webhook config changed during activation); the namespace is held quiesced while re-proving"
	switch {
	case proveErr != nil:
		reason = "ProbeFailed"
		message = fmt.Sprintf("re-proving dispatch convergence failed; the namespace is held quiesced (fail-closed) rather than left active: %v", proveErr)
	case accepted:
		// The new tuple is itself converged and accepted: re-seal under it while
		// staying quiesced.
		receipt.DispatchTupleHash = proof.tupleHash
	case proof.converged:
		condition = isolationHAUnsupportedCondition
		reason = "EnumerationUnproven"
		message = ackDetail
	case proof.reason == dispatchUnconvergedReasonUnsupportedHA:
		condition = isolationHAUnsupportedCondition
		reason = "UnsupportedHA"
		message = proof.reasonMessage()
	}
	r.setIsolationPreflightCondition(cluster, condition, reason, message)
	if updateErr := r.Status().Update(ctx, cluster); updateErr != nil {
		return false, errors.Join(fmt.Errorf("hold isolation quiesced after dispatch re-proof: %w", updateErr), proveErr)
	}
	return false, proveErr
}

func (proof dispatchProof) reasonMessage() string {
	if proof.reason == "" {
		return "at least one API-server backend did not dispatch the sentinel Pod create to the pgshard webhook"
	}
	return proof.reason
}

func (r *PgShardClusterReconciler) setIsolationPreflightCondition(cluster *pgshardv1alpha1.PgShardCluster, conditionType, reason, message string) {
	for _, other := range isolationPreflightConditions {
		if other != conditionType {
			meta.RemoveStatusCondition(&cluster.Status.Conditions, other)
		}
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: cluster.Generation,
		Reason:             reason,
		Message:            message,
	})
}

func clearIsolationPreflightConditions(cluster *pgshardv1alpha1.PgShardCluster) {
	for _, conditionType := range isolationPreflightConditions {
		meta.RemoveStatusCondition(&cluster.Status.Conditions, conditionType)
	}
}
