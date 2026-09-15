package operator

import (
	"context"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func moveReconciler(t *testing.T, objs ...client.Object) *ClusterReconciler {
	t.Helper()
	scheme, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(&pgshardv1alpha1.PgShardCluster{}).Build()
	return &ClusterReconciler{Client: cl, Now: func() time.Time { return time.Unix(1_800_000_000, 0) }}
}

// movePod is a pod of the cluster rendered in phase ("" for one rendered
// outside a move).
func movePod(c *pgshardv1alpha1.PgShardCluster, name, phase string, labels map[string]string) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: map[string]string{LabelCluster: c.Name}}}
	for k, v := range labels {
		p.Labels[k] = v
	}
	if phase != "" {
		p.Annotations = map[string]string{AnnotationInternalTLSPhase: phase}
	}
	return p
}

func moveMemberPod(c *pgshardv1alpha1.PgShardCluster, name, phase string) *corev1.Pod {
	return movePod(c, name, phase, map[string]string{LabelGroup: "shard-0"})
}

func moveRouterPod(c *pgshardv1alpha1.PgShardCluster, name, phase string) *corev1.Pod {
	return movePod(c, name, phase, map[string]string{LabelComponent: routerComponent})
}

func moveControllerPod(c *pgshardv1alpha1.PgShardCluster, name, phase string) *corev1.Pod {
	return movePod(c, name, phase, map[string]string{LabelComponent: controllerComponent})
}

// pass runs the move step on the stored cluster and reports what it
// recorded and what it left the pass rendering.
func pass(t *testing.T, r *ClusterReconciler, c *pgshardv1alpha1.PgShardCluster) (stored, rendered *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	ctx := context.Background()
	rendered = &pgshardv1alpha1.PgShardCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), rendered); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileInternalTLSMove(ctx, rendered); err != nil {
		t.Fatal(err)
	}
	stored = &pgshardv1alpha1.PgShardCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), stored); err != nil {
		t.Fatal(err)
	}
	return stored, rendered
}

func insecureCluster(name string) *pgshardv1alpha1.PgShardCluster {
	c := newCluster(name)
	c.Status.InternalTLS = &pgshardv1alpha1.InternalTLSStatus{Mode: "insecure"}
	return c
}

// TestAClusterThatIsNotMovingRecordsItsTransport (PGS-236): a cluster
// created with TLS, or first seen by an operator that tracks the move, is
// taken to run what its spec says; only one recorded as plaintext moves.
func TestAClusterThatIsNotMovingRecordsItsTransport(t *testing.T) {
	for _, c := range []*pgshardv1alpha1.PgShardCluster{issuingCluster("born-tls"), newCluster("born-insecure")} {
		r := moveReconciler(t, c)
		stored, _ := pass(t, r, c)
		if st := stored.Status.InternalTLS; st == nil || st.Move != nil || st.Mode != internalTLSMode(c) {
			t.Errorf("%s: recorded %+v, want mode %s and no move", c.Name, st, internalTLSMode(c))
		}
	}
}

// TestAMoveToTLSWaitsForEveryPodOfTheStepBefore (PGS-236): the move steps
// from Accepting to Dialing only once every member, router and controller
// pod serves TLS alongside plaintext -- a terminating one included, since it
// keeps serving until it is gone -- and completes only once every router
// dials TLS.
func TestAMoveToTLSWaitsForEveryPodOfTheStepBefore(t *testing.T) {
	c := insecureCluster("moving")
	c.Spec.InternalTLS = pgshardv1alpha1.InternalTLSSpec{Issue: true}
	oldRouter := moveRouterPod(c, "moving-router-old", "")
	oldRouter.DeletionTimestamp = &metav1.Time{Time: time.Unix(1_799_999_000, 0)}
	oldRouter.Finalizers = []string{"test/keep"}
	objs := []client.Object{c, moveMemberPod(c, "moving-shard-0-0", ""), moveRouterPod(c, "moving-router-a", ""), oldRouter, moveControllerPod(c, "moving-controller-x", "")}
	r := moveReconciler(t, objs...)
	ctx := context.Background()
	phaseOf := internalTLSPhase
	annotate := func(name, phase string) {
		t.Helper()
		var p corev1.Pod
		if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: name}, &p); err != nil {
			t.Fatal(err)
		}
		if p.Annotations == nil {
			p.Annotations = map[string]string{}
		}
		p.Annotations[AnnotationInternalTLSPhase] = phase
		if err := r.Update(ctx, &p); err != nil {
			t.Fatal(err)
		}
	}

	stored, rendered := pass(t, r, c)
	if phaseOf(stored) != pgshardv1alpha1.InternalTLSAccepting || phaseOf(rendered) != pgshardv1alpha1.InternalTLSAccepting {
		t.Fatalf("an insecure cluster given issue: true recorded %q, rendered %q; want Accepting", phaseOf(stored), phaseOf(rendered))
	}
	if stored.Status.InternalTLS.Move.Target != (pgshardv1alpha1.InternalTLSSpec{Issue: true}) {
		t.Fatalf("the move's target is %+v", stored.Status.InternalTLS.Move.Target)
	}

	for _, name := range []string{"moving-shard-0-0", "moving-router-a", "moving-controller-x"} {
		annotate(name, pgshardv1alpha1.InternalTLSAccepting)
	}
	stored, _ = pass(t, r, c)
	cond := meta.FindStatusCondition(stored.Status.Conditions, pgshardv1alpha1.ConditionInternalTLSMoving)
	if phaseOf(stored) != pgshardv1alpha1.InternalTLSAccepting || cond == nil || !strings.Contains(cond.Message, "moving-router-old") {
		t.Fatalf("with a terminating router still rendered before the move: phase %q, condition %+v; want Accepting, naming it", phaseOf(stored), cond)
	}

	annotate("moving-router-old", pgshardv1alpha1.InternalTLSAccepting)
	if stored, _ = pass(t, r, c); phaseOf(stored) != pgshardv1alpha1.InternalTLSDialing {
		t.Fatalf("every pod serves TLS alongside plaintext, yet the move is %q", phaseOf(stored))
	}

	if stored, _ = pass(t, r, c); phaseOf(stored) != pgshardv1alpha1.InternalTLSDialing {
		t.Fatalf("no router dials TLS yet, but the move is %q", phaseOf(stored))
	}
	for _, name := range []string{"moving-router-a", "moving-router-old"} {
		annotate(name, pgshardv1alpha1.InternalTLSDialing)
	}
	stored, rendered = pass(t, r, c)
	if st := stored.Status.InternalTLS; st.Move != nil || st.Mode != "issued" || phaseOf(rendered) != "" {
		t.Fatalf("every router dials TLS: recorded %+v, rendered phase %q; want the move complete in mode issued", st, phaseOf(rendered))
	}
	if cond := meta.FindStatusCondition(stored.Status.Conditions, pgshardv1alpha1.ConditionInternalTLSMoving); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("the moving condition after the move: %+v", cond)
	}
}

// TestASpecChangeDuringAMove (PGS-236): set back to insecure while callers
// still dial plaintext, the move ends; once they dial TLS, the move's
// target is rendered whatever the spec says until it completes.
func TestASpecChangeDuringAMove(t *testing.T) {
	accepting := insecureCluster("back-early")
	accepting.Status.InternalTLS.Move = &pgshardv1alpha1.InternalTLSMove{Phase: pgshardv1alpha1.InternalTLSAccepting, Target: pgshardv1alpha1.InternalTLSSpec{Issue: true}}
	r := moveReconciler(t, accepting)
	if stored, rendered := pass(t, r, accepting); stored.Status.InternalTLS.Move != nil || !rendered.Spec.InternalTLS.Insecure {
		t.Errorf("insecure again while Accepting: move %+v, rendering %+v; want the move ended and insecure rendered", stored.Status.InternalTLS.Move, rendered.Spec.InternalTLS)
	}

	dialing := insecureCluster("back-late")
	dialing.Status.InternalTLS.Move = &pgshardv1alpha1.InternalTLSMove{Phase: pgshardv1alpha1.InternalTLSDialing, Target: pgshardv1alpha1.InternalTLSSpec{Issue: true}}
	r = moveReconciler(t, dialing, moveRouterPod(dialing, "back-late-router", pgshardv1alpha1.InternalTLSAccepting))
	stored, rendered := pass(t, r, dialing)
	if internalTLSPhase(stored) != pgshardv1alpha1.InternalTLSDialing || !rendered.Spec.InternalTLS.Issue || rendered.Spec.InternalTLS.Insecure {
		t.Fatalf("insecure again while Dialing: phase %q, rendering %+v; want the move held and its target rendered", internalTLSPhase(stored), rendered.Spec.InternalTLS)
	}
	if cond := meta.FindStatusCondition(stored.Status.Conditions, pgshardv1alpha1.ConditionInternalTLSMoving); cond == nil || !strings.Contains(cond.Message, "changed during the move") {
		t.Fatalf("the condition does not say the spec change waits: %+v", cond)
	}
	if !dialing.Spec.InternalTLS.Insecure {
		t.Fatal("the test changed the stored spec")
	}
	var again pgshardv1alpha1.PgShardCluster
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(dialing), &again); err != nil || !again.Spec.InternalTLS.Insecure {
		t.Fatalf("rendering the target wrote it into the stored spec: %+v %v", again.Spec.InternalTLS, err)
	}
}

// TestEachStepOfAMoveRendersWhatItsCallersNeed (PGS-236): Accepting makes
// every listener serve plaintext alongside TLS and keeps the routers
// dialling plaintext; Dialing only switches the routers' dials, so members
// do not roll again; a finished move drops plaintext everywhere.
func TestEachStepOfAMoveRendersWhatItsCallersNeed(t *testing.T) {
	render := func(phase string) (MemberTemplate, []string, []string, []string) {
		c := issuingCluster("steps")
		if phase != "" {
			c.Status.InternalTLS = &pgshardv1alpha1.InternalTLSStatus{Mode: "insecure", Move: &pgshardv1alpha1.InternalTLSMove{Phase: phase, Target: c.Spec.InternalTLS}}
		}
		g := Groups(c)[1]
		tpl := Template(c, g, nil, nil)
		pod := Renderer{}.Pod(c, g, 0, "primary", "pvc", tpl)
		var pooler []string
		for _, ct := range pod.Spec.Containers {
			if ct.Name == poolerContainer {
				pooler = ct.Args
			}
		}
		if got := pod.Annotations[AnnotationInternalTLSPhase]; got != phase {
			t.Errorf("%q: member pod records phase %q", phase, got)
		}
		router := Renderer{}.RouterDeployment(c)
		if got := router.Spec.Template.Annotations[AnnotationInternalTLSPhase]; got != phase {
			t.Errorf("%q: router pods record phase %q", phase, got)
		}
		controller := Renderer{}.ControllerDeployment(c)
		if got := controller.Spec.Template.Annotations[AnnotationInternalTLSPhase]; got != phase {
			t.Errorf("%q: controller pods record phase %q", phase, got)
		}
		if got := agentGRPCTLS(c).AcceptPlaintext; got != (phase != "") {
			t.Errorf("%q: agent accepts plaintext = %v", phase, got)
		}
		return tpl, pooler, router.Spec.Template.Spec.Containers[0].Args, controller.Spec.Template.Spec.Containers[0].Args
	}
	has := slices.Contains[[]string]

	accTpl, accPooler, accRouter, accController := render(pgshardv1alpha1.InternalTLSAccepting)
	if !has(accPooler, "--tls-accept-plaintext") || !has(accController, "--tls-accept-plaintext") {
		t.Errorf("Accepting: pooler %v, controller %v; both must also serve plaintext", accPooler, accController)
	}
	if !has(accRouter, "--tls-accept-plaintext") || !has(accRouter, "--tls-dial-plaintext") {
		t.Errorf("Accepting: router %v must serve both and still dial plaintext", accRouter)
	}

	dialTpl, dialPooler, dialRouter, dialController := render(pgshardv1alpha1.InternalTLSDialing)
	if !has(dialRouter, "--tls-accept-plaintext") || has(dialRouter, "--tls-dial-plaintext") {
		t.Errorf("Dialing: router %v must dial TLS and still serve both", dialRouter)
	}
	if dialTpl.Hash() != accTpl.Hash() || !slices.Equal(dialPooler, accPooler) || !slices.Equal(dialController, accController) {
		t.Error("Dialing renders members or the controller differently from Accepting, so it rolls them again")
	}

	doneTpl, donePooler, doneRouter, doneController := render("")
	for what, args := range map[string][]string{"pooler": donePooler, "router": doneRouter, "controller": doneController} {
		if has(args, "--tls-accept-plaintext") || has(args, "--tls-dial-plaintext") {
			t.Errorf("after the move the %s still takes plaintext: %v", what, args)
		}
	}
	if doneTpl.Hash() == accTpl.Hash() {
		t.Error("finishing the move does not roll the members that still serve plaintext")
	}
}

// TestTheOperatorTakesBarriersInPlaintextUntilTheControllerServesTLS
// (PGS-236): in Accepting the controller may still be the pod rendered
// before the move, which serves plaintext only, so the operator's scheduled
// barriers dial it in plaintext; from Dialing on every controller pod serves
// TLS and the operator dials it with the cluster's credentials.
func TestTheOperatorTakesBarriersInPlaintextUntilTheControllerServesTLS(t *testing.T) {
	ctx := context.Background()
	c := issuingCluster("barrier-move")
	r := pkiReconciler(t, time.Now(), c)
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	pgshardv1.RegisterControllerServer(g, pgshardv1.UnimplementedControllerServer{})
	go func() { _ = g.Serve(l) }()
	t.Cleanup(g.Stop)

	barriers := GRPCBarrierClient{Issued: &IssuedCredentials{Client: r.Client}}
	take := func(phase string) error {
		c.Status.InternalTLS = &pgshardv1alpha1.InternalTLSStatus{Mode: "insecure", Move: &pgshardv1alpha1.InternalTLSMove{Phase: phase, Target: c.Spec.InternalTLS}}
		err := barriers.CreateBarrier(ctx, c, l.Addr().String(), "b")
		if status.Code(err) == codes.Unimplemented {
			return nil
		}
		return err
	}
	if err := take(pgshardv1alpha1.InternalTLSAccepting); err != nil {
		t.Fatalf("Accepting: the operator could not reach a controller still serving plaintext: %v", err)
	}
	if err := take(pgshardv1alpha1.InternalTLSDialing); err == nil {
		t.Fatal("Dialing: the operator still dialled the controller in plaintext")
	}
}
