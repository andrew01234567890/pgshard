package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

// Holder is the identity this agent takes the lease under.
func (l *Lease) Holder() string {
	if l == nil {
		return ""
	}
	return l.holder
}

// ErrLeaseHeld is returned when another holder owns an unexpired lease.
var ErrLeaseHeld = errors.New("lease held by another identity")

// LabelCluster names the cluster an object belongs to. Every object pgshard
// creates carries it, and a namespace holding several clusters is sorted by
// it. The agent cannot import the operator, which owns the key, so it is
// repeated here and a test keeps the two equal.
const LabelCluster = "pgshard.io/cluster"

// Lease guards the shard primary with a coordination.k8s.io Lease.
type Lease struct {
	client coordclient.LeaseInterface
	name   string
	// cluster labels the Lease. Everything else pgshard creates carries
	// pgshard.io/cluster, and a namespace holding several clusters is
	// sorted by it; without it a Lease can only be attributed by matching
	// names, and a cluster called "a" prefixes "ab"'s objects.
	cluster  string
	holder   string
	duration time.Duration
	renew    time.Duration
	retry    time.Duration
	log      *slog.Logger
	now      func() time.Time

	mu       sync.Mutex
	acquired time.Time
}

// NewLease builds a Lease client from the in-cluster config; it returns
// (nil, nil) when the agent is not running in a pod so callers can disable
// leasing explicitly.
func NewLease(cfg *Config, log *slog.Logger) (*Lease, error) {
	rc, err := rest.InClusterConfig()
	if errors.Is(err, rest.ErrNotInCluster) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, err
	}
	return NewLeaseWithClient(cs.CoordinationV1().Leases(cfg.Lease.Namespace), cfg, log), nil
}

// NewLeaseWithClient builds a Lease over an explicit client (tests use fakes).
func NewLeaseWithClient(client coordclient.LeaseInterface, cfg *Config, log *slog.Logger) *Lease {
	return &Lease{
		client:   client,
		name:     cfg.LeaseName(),
		cluster:  cfg.Cluster,
		holder:   cfg.PodName,
		duration: time.Duration(cfg.Lease.Duration),
		renew:    time.Duration(cfg.Lease.Renew),
		retry:    time.Duration(cfg.Lease.Retry),
		log:      log,
		now:      time.Now,
	}
}

// Acquire takes the lease, creating it if absent or claiming it when expired
// or already ours. Updates are conditional on resourceVersion so two agents
// cannot both succeed.
func (l *Lease) Acquire(ctx context.Context) error {
	// A conflicting update is retried once from a fresh read: our own hold
	// loop or the operator's fence write may have bumped the resourceVersion
	// under us while we still own the lease. Only a lease whose holder is
	// genuinely someone else is reported as ErrLeaseHeld.
	for attempt := 0; ; attempt++ {
		err := l.acquireOnce(ctx)
		if err == nil {
			l.markAcquired()
		}
		if err == nil || !errors.Is(err, errLeaseConflict) || attempt >= 1 {
			if errors.Is(err, errLeaseConflict) {
				return fmt.Errorf("lease update conflicted twice: %w", err)
			}
			return err
		}
	}
}

// errLeaseConflict marks an optimistic-concurrency failure whose holder was
// still ours (or unknown); it is transient, unlike ErrLeaseHeld.
var errLeaseConflict = errors.New("lease update conflict")

func (l *Lease) acquireOnce(ctx context.Context) error {
	cur, err := l.client.Get(ctx, l.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = l.client.Create(ctx, l.spec(nil), metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			return errLeaseConflict
		}
		return err
	}
	if err != nil {
		return err
	}
	if !l.canTake(cur) {
		return fmt.Errorf("%w: %s", ErrLeaseHeld, ptr.Deref(cur.Spec.HolderIdentity, ""))
	}
	_, err = l.client.Update(ctx, l.spec(cur), metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return errLeaseConflict
	}
	return err
}

func (l *Lease) canTake(cur *coordinationv1.Lease) bool {
	holder := ptr.Deref(cur.Spec.HolderIdentity, "")
	if holder == "" || holder == l.holder {
		return true
	}
	if cur.Spec.RenewTime == nil {
		return true
	}
	dur := time.Duration(ptr.Deref(cur.Spec.LeaseDurationSeconds, int32(l.duration.Seconds()))) * time.Second
	return l.now().After(cur.Spec.RenewTime.Add(dur))
}

func (l *Lease) spec(base *coordinationv1.Lease) *coordinationv1.Lease {
	out := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: l.name}}
	if base != nil {
		out = base.DeepCopy()
	}
	if l.cluster != "" {
		if out.Labels == nil {
			out.Labels = map[string]string{}
		}
		out.Labels[LabelCluster] = l.cluster
	}
	now := metav1.NewMicroTime(l.now())
	if base == nil || ptr.Deref(base.Spec.HolderIdentity, "") != l.holder {
		out.Spec.AcquireTime = &now
		out.Spec.LeaseTransitions = ptr.To(ptr.Deref(out.Spec.LeaseTransitions, 0) + 1)
	}
	out.Spec.HolderIdentity = ptr.To(l.holder)
	out.Spec.LeaseDurationSeconds = ptr.To(int32(l.duration.Seconds()))
	out.Spec.RenewTime = &now
	return out
}

func (l *Lease) markAcquired() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acquired = l.now()
}

// Stale reports whether the lease has not been renewed within its duration.
// The self-fence on losing a lease runs inside Hold's goroutine, so a process
// that is frozen or wedged never reaches it and keeps its PostgreSQL child
// writable. Reporting staleness here lets the liveness probe fail instead,
// which puts the deadline in the kubelet's hands rather than the agent's.
func (l *Lease) Stale() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.acquired.IsZero() {
		return false
	}
	return l.now().Sub(l.acquired) > l.duration
}

// Hold renews the lease every renew interval until ctx ends. It returns a
// non-nil error when the lease is lost (taken by another holder, or not
// renewed within the lease duration); the caller must then fence.
func (l *Lease) Hold(ctx context.Context) error {
	deadline := l.now().Add(l.duration)
	t := time.NewTicker(l.renew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if err := l.Acquire(ctx); err != nil {
			if errors.Is(err, ErrLeaseHeld) {
				return err
			}
			l.log.Warn("lease renew failed", "err", err)
			if l.now().After(deadline) {
				return fmt.Errorf("lease not renewed within %s: %w", l.duration, err)
			}
			t.Reset(l.retry)
			continue
		}
		deadline = l.now().Add(l.duration)
		t.Reset(l.renew)
	}
}

// Release clears the holder so a successor can take the lease immediately.
func (l *Lease) Release(ctx context.Context) error {
	cur, err := l.client.Get(ctx, l.name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if ptr.Deref(cur.Spec.HolderIdentity, "") != l.holder {
		return nil
	}
	cur.Spec.HolderIdentity = ptr.To("")
	_, err = l.client.Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

// Reachable reports whether the API server answers a Lease read.
func (l *Lease) Reachable(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := l.client.Get(ctx, l.name, metav1.GetOptions{})
	return err == nil || apierrors.IsNotFound(err)
}
