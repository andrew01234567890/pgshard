package operator

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// AnnotationInternalTLSPhase records on a member, router or controller pod
// the step of a move to mutual TLS it was rendered in. It is what the move
// waits on: a pod rendered before a step still dials, or still refuses,
// the way that step changed.
const AnnotationInternalTLSPhase = "pgshard.io/internal-tls-phase"

// internalTLSPhase is the step of a move to mutual TLS the cluster is
// rendered in, or empty outside one.
func internalTLSPhase(c *pgshardv1alpha1.PgShardCluster) string {
	if c.Status.InternalTLS == nil || c.Status.InternalTLS.Move == nil {
		return ""
	}
	return c.Status.InternalTLS.Move.Phase
}

func tlsMode(mode string) bool {
	return mode == "issued" || strings.HasPrefix(mode, "secret:")
}

// reconcileInternalTLSMove advances a move from plaintext to mutual TLS by
// one step when every pod of the step before has gone, records the result
// in status before anything is rendered from it, and leaves c rendering
// what the move requires.
//
// A spec set back to insecure while callers still dial plaintext ends the
// move: nothing depends on TLS yet. Any other change to spec.internalTLS
// during a move waits for it to complete, and the move's target is rendered
// meanwhile -- once callers dial TLS the listeners cannot stop serving it,
// and a pod's recorded step says nothing about which material it holds.
func (r *ClusterReconciler) reconcileInternalTLSMove(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) error {
	base := c.DeepCopy()
	specMode := internalTLSMode(c)
	st := c.Status.InternalTLS
	if st == nil {
		st = &pgshardv1alpha1.InternalTLSStatus{Mode: specMode}
		c.Status.InternalTLS = st
	}
	waiting := ""
	switch move := st.Move; {
	case move == nil && st.Mode == "insecure" && tlsMode(specMode):
		st.Move = &pgshardv1alpha1.InternalTLSMove{Phase: pgshardv1alpha1.InternalTLSAccepting,
			Target: *c.Spec.InternalTLS.DeepCopy(), StartedAt: metav1.NewTime(r.now())}
		waiting = "every member, router and controller pod to serve TLS alongside plaintext"
	case move == nil:
		st.Mode = specMode
	case move.Phase == pgshardv1alpha1.InternalTLSAccepting && !tlsMode(specMode):
		st.Move, st.Mode = nil, specMode
	case move.Phase == pgshardv1alpha1.InternalTLSAccepting:
		left, err := r.podsBehindTheMove(ctx, c, base)
		if err != nil {
			return err
		}
		if len(left) == 0 {
			move.Phase = pgshardv1alpha1.InternalTLSDialing
			waiting = "every router pod to dial TLS"
		} else {
			waiting = "pods still refusing TLS or not serving it: " + strings.Join(left, ", ")
		}
	case move.Phase == pgshardv1alpha1.InternalTLSDialing:
		left, err := r.podsBehindTheMove(ctx, c, base)
		if err != nil {
			return err
		}
		if len(left) == 0 {
			st.Mode = internalTLSModeOf(c.Namespace, move.Target)
			st.Move = nil
		} else {
			waiting = "router pods still dialling plaintext: " + strings.Join(left, ", ")
		}
	}
	if st.Move != nil {
		msg := fmt.Sprintf("moving to mutual TLS, %s: waiting for %s", st.Move.Phase, waiting)
		if !equality.Semantic.DeepEqual(c.Spec.InternalTLS, st.Move.Target) {
			msg += "; spec.internalTLS changed during the move and is applied once it completes"
		}
		meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{Type: pgshardv1alpha1.ConditionInternalTLSMoving,
			Status: metav1.ConditionTrue, Reason: st.Move.Phase, ObservedGeneration: c.Generation, Message: msg})
	} else {
		meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{Type: pgshardv1alpha1.ConditionInternalTLSMoving,
			Status: metav1.ConditionFalse, Reason: "Settled", ObservedGeneration: c.Generation,
			Message: "internal transport: " + st.Mode})
	}
	if !equality.Semantic.DeepEqual(base.Status.InternalTLS, c.Status.InternalTLS) ||
		!equality.Semantic.DeepEqual(meta.FindStatusCondition(base.Status.Conditions, pgshardv1alpha1.ConditionInternalTLSMoving), meta.FindStatusCondition(c.Status.Conditions, pgshardv1alpha1.ConditionInternalTLSMoving)) {
		// Before anything is rendered from it: a pass that rendered a step
		// and died before recording it would render the previous step
		// again on restart, and roll every member a second time.
		if err := r.Status().Patch(ctx, c, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("internal TLS move: %w", err)
		}
	}
	if st.Move != nil {
		c.Spec.InternalTLS = *st.Move.Target.DeepCopy()
	}
	return nil
}

func internalTLSModeOf(namespace string, spec pgshardv1alpha1.InternalTLSSpec) string {
	return internalTLSMode(&pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Namespace: namespace}, Spec: pgshardv1alpha1.PgShardClusterSpec{InternalTLS: spec}})
}

// podsBehindTheMove names the pods not yet rendered in the step the move is
// in (base, the status as the pass found it). Terminating pods count: a
// router being drained keeps dialling, and a member being replaced keeps
// serving, until it is gone. In Accepting that is every member, router and
// controller pod; in Dialing only routers, since nothing else changes.
func (r *ClusterReconciler) podsBehindTheMove(ctx context.Context, c, base *pgshardv1alpha1.PgShardCluster) ([]string, error) {
	phase := internalTLSPhase(base)
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(c.Namespace), client.MatchingLabels{LabelCluster: c.Name}); err != nil {
		return nil, err
	}
	var left []string
	for _, p := range pods.Items {
		component := p.Labels[LabelComponent]
		member := p.Labels[LabelGroup] != ""
		switch {
		case phase == pgshardv1alpha1.InternalTLSDialing && component != routerComponent:
			continue
		case !member && component != routerComponent && component != controllerComponent:
			continue
		}
		got := p.Annotations[AnnotationInternalTLSPhase]
		if got == phase || (phase == pgshardv1alpha1.InternalTLSAccepting && got == pgshardv1alpha1.InternalTLSDialing) {
			continue
		}
		left = append(left, p.Name)
	}
	sort.Strings(left)
	return left, nil
}
