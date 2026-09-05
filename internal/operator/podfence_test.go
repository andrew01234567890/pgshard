package operator

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// gracePeriods records the grace period every Delete carried, so a force
// delete is visible as the zero the API server reads it as.
type gracePeriods struct {
	mu   sync.Mutex
	seen []int64
}

func (g *gracePeriods) add(opts []client.DeleteOption) {
	var o client.DeleteOptions
	o.ApplyOptions(opts)
	g.mu.Lock()
	defer g.mu.Unlock()
	if o.GracePeriodSeconds == nil {
		g.seen = append(g.seen, -1)
		return
	}
	g.seen = append(g.seen, *o.GracePeriodSeconds)
}

func (g *gracePeriods) list() []int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]int64(nil), g.seen...)
}

// fencedPod is the old primary, held by a finalizer so the fake client leaves
// it Terminating on delete the way the API server leaves a Pod whose kubelet
// has not confirmed the containers are gone.
func fencedPod(t *testing.T, finalizer bool) (*corev1.Pod, client.Client, *gracePeriods) {
	t.Helper()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "demo-shard-0-a", UID: types.UID("u1")}}
	if finalizer {
		pod.Finalizers = []string{"pgshard.io/test-hold"}
	}
	scheme, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	grace := &gracePeriods{}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				grace.add(opts)
				return c.Delete(ctx, obj, opts...)
			},
		}).Build()
	return pod, cl, grace
}

// TestFencingTheOldPrimaryWaitsForItsContainersToStop: the delete used to
// carry grace zero, which is a force delete -- the API server drops the
// object without the kubelet confirming anything, so the wait that follows
// returned on its first Get while PostgreSQL was still running.
func TestFencingTheOldPrimaryWaitsForItsContainersToStop(t *testing.T) {
	pod, cl, grace := fencedPod(t, true)
	r := &ClusterReconciler{Client: cl, PollInterval: time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- r.fencePod(context.Background(), &memberInfo{pod: pod}) }()

	select {
	case err := <-done:
		t.Fatalf("fencePod returned (%v) while the Pod was still terminating", err)
	case <-time.After(50 * time.Millisecond):
	}
	if g := grace.list(); len(g) != 1 || g[0] <= 0 {
		t.Fatalf("delete grace periods %v, want one non-zero: grace zero is a force delete and confirms nothing", g)
	}

	// The kubelet reports the containers gone.
	var live corev1.Pod
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(pod), &live); err != nil {
		t.Fatal(err)
	}
	live.Finalizers = nil
	if err := cl.Update(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("fencePod: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fencePod did not return once the Pod was gone")
	}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(pod), &live); !apierrors.IsNotFound(err) {
		t.Fatalf("pod still present after the fence: %v", err)
	}
}

// TestANodeThatNeverConfirmsDoesNotBlockThePromotion: a Pod on a down or
// partitioned node stays Terminating until its node object goes, so waiting
// for confirmation for good would make every node failure an outage. The
// delete is escalated instead and the promotion proceeds on the Lease and
// epoch fences.
func TestANodeThatNeverConfirmsDoesNotBlockThePromotion(t *testing.T) {
	pod, cl, grace := fencedPod(t, true)
	now := time.Now()
	r := &ClusterReconciler{Client: cl, PollInterval: time.Millisecond, PodFenceTimeout: 30 * time.Second,
		Now: func() time.Time { now = now.Add(11 * time.Second); return now }}
	if err := r.fencePod(context.Background(), &memberInfo{pod: pod}); err != nil {
		t.Fatalf("fencePod refused to give up on a node that never confirms: %v", err)
	}
	g := grace.list()
	if len(g) != 2 || g[0] <= 0 || g[1] != 0 {
		t.Fatalf("delete grace periods %v, want a graceful delete then a force delete", g)
	}
}
