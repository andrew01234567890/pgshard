package controller

import (
	"context"
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
// proof: a proof from at most one enumerated backend is accepted only with the
// cluster's explicit durable single-server/provider acknowledgement. It returns
// ok, or the ha-unsupported detail to surface.
func dispatchProofAccepted(cluster *pgshardv1alpha1.PgShardCluster, proof dispatchProof) (bool, string) {
	if !proof.converged {
		return false, ""
	}
	if proof.backends > 1 {
		return true, ""
	}
	if cluster.Annotations[pgshardv1alpha1.IsolationDispatchTopologyAckAnnotation] == pgshardv1alpha1.IsolationDispatchTopologyAckSingleServer {
		return true, ""
	}
	return false, fmt.Sprintf("the kubernetes Service EndpointSlices enumerate %d API-server backend(s), which cannot prove physical-backend enumeration (an opaque VIP may hide unproven backends); set the %s=%s annotation to attest the published endpoint is the complete backend set", proof.backends, pgshardv1alpha1.IsolationDispatchTopologyAckAnnotation, pgshardv1alpha1.IsolationDispatchTopologyAckSingleServer)
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
	if ok, detail := dispatchProofAccepted(cluster, proof); !ok {
		r.setIsolationPreflightCondition(cluster, isolationHAUnsupportedCondition, "EnumerationUnproven", detail)
		return proof, false
	}
	clearIsolationPreflightConditions(cluster)
	return proof, true
}

// revalidateDispatchTuple re-proves dispatch convergence during an in-progress
// activation. If the backend tuple changed or convergence was lost it fails
// CLOSED: the receipt is held in ACTIVATING_QUIESCE (which denies every create),
// never reset to INACTIVE, so enforcement is never dropped to fail-open while
// membership is in flux. It re-enumerates by clearing the sealed state so the
// quiesce phase re-seals under the fresh tuple. It returns whether the in-progress
// proof is still valid.
func (r *PgShardClusterReconciler) revalidateDispatchTuple(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	receipt := cluster.Status.IsolationReceipt
	var proof dispatchProof
	if r.DispatchProber != nil {
		var err error
		proof, err = r.DispatchProber.Prove(ctx, cluster.Namespace)
		if err != nil {
			return false, fmt.Errorf("re-prove dispatch convergence: %w", err)
		}
	}
	accepted, ackDetail := dispatchProofAccepted(cluster, proof)
	if r.DispatchProber != nil && accepted && proof.tupleHash == receipt.DispatchTupleHash {
		return true, nil
	}
	// Invalidated. Hold a durable deny phase (QUIESCE) and re-enumerate; never drop
	// to INACTIVE (fail-open) mid-activation.
	receipt.Phase = pgshardv1alpha1.IsolationActivatingQuiesce
	receipt.SealedParents = nil
	receipt.RecreatePendingUIDs = nil
	receipt.ActivatedAt = metav1.Now()
	condition := isolationDispatchNotConvergedCondition
	reason := "TupleInvalidated"
	message := "the dispatch-convergence proof was invalidated (API-server backend set or webhook config changed during activation); the namespace is held quiesced while re-proving"
	if accepted {
		// The new tuple is itself converged and accepted: re-seal under it while
		// staying quiesced.
		receipt.DispatchTupleHash = proof.tupleHash
	} else if proof.converged {
		condition = isolationHAUnsupportedCondition
		reason = "EnumerationUnproven"
		message = ackDetail
	} else if proof.reason == dispatchUnconvergedReasonUnsupportedHA {
		condition = isolationHAUnsupportedCondition
		reason = "UnsupportedHA"
		message = proof.reasonMessage()
	}
	r.setIsolationPreflightCondition(cluster, condition, reason, message)
	if err := r.Status().Update(ctx, cluster); err != nil {
		return false, fmt.Errorf("hold isolation quiesced after dispatch tuple invalidation: %w", err)
	}
	return false, nil
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
