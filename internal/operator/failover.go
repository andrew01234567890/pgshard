package operator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

const (
	// DefaultFailoverDelay is how long the primary must be unhealthy (pod
	// not Ready and Agent.Status failing, or pod missing) before failover.
	DefaultFailoverDelay = 10 * time.Second
	// standbyQuiesceTimeout bounds the wait for every standby to stop
	// streaming from the old primary.
	standbyQuiesceTimeout = 30 * time.Second
	standbyQuiescePoll    = time.Second
	// FenceHolder is the Lease holder identity the operator writes while a
	// failover is in flight; a live old primary that sees it self-fences.
	FenceHolder = "pgshard-operator"
	// fenceLeaseSeconds must cover the wait for standbys plus promotion.
	fenceLeaseSeconds = int32(60)
)

var (
	errNoCandidate = errors.New("no eligible failover candidate")
	// errAsyncFailover refuses automatic failover under asynchronous
	// durability, where no standby was required to acknowledge commits, so
	// promoting a reachable standby could silently lose acknowledged writes.
	errAsyncFailover    = fmt.Errorf("%w: minSyncStandbys=0 (asynchronous durability) has no standby guaranteed to hold acknowledged commits; promotion is refused to avoid data loss", errNoCandidate)
	errPrimaryStillLive = errors.New("old primary still reports itself primary")
	// ErrLeaseHeldByOther is returned when the group Lease is renewed by an
	// identity that is neither the old primary nor the operator's fence.
	ErrLeaseHeldByOther = errors.New("lease held by another live holder")
)

// memberView is what candidate selection knows about one member.
type memberView struct {
	Name string
	// Listed is true when the member appears in synchronous_standby_names,
	// i.e. it may hold the only copy of an acknowledged commit. The operator
	// lists every non-primary member (healthy first), so a lagging or
	// not-yet-Ready standby is still Listed and must not be skipped.
	Listed     bool
	Reachable  bool
	InRecovery bool
	Streaming  bool
	FlushLSN   uint64
	// Why carries the probe's error when Reachable is false. A member the
	// probe could not reach and one that is genuinely gone both arrive here
	// as Reachable=false, and the refusal they produce names neither; the
	// views are logged, so the reason travels with them.
	Why string
}

// errQuorum is returned when unreachable listed standbys could hold an
// acknowledged commit that no reachable standby has: promoting anyway could
// lose it, so automatic failover refuses.
var errQuorum = fmt.Errorf("%w: unreachable synchronous standbys may hold acknowledged commits", errNoCandidate)

// chooseCandidate picks the reachable in-recovery member with the highest
// flushed LSN, excluding exclude. Any standby in the synchronous list may have
// acknowledged a commit, so all reachable standbys are eligible; if a listed
// standby is unreachable and the reachable ones do not outnumber the acks
// (reachable + numSync <= listed), no candidate is admissible. preferred wins
// when it holds the maximum LSN. Ties break by name.
func chooseCandidate(members []memberView, exclude, preferred string, numSync int) (string, error) {
	if numSync < 1 {
		numSync = 1
	}
	var eligible []memberView
	listed, reachableListed := 0, 0
	for _, m := range members {
		if m.Name == exclude {
			continue
		}
		if m.Listed {
			listed++
			if m.Reachable {
				reachableListed++
			}
		}
		if !m.Reachable || !m.InRecovery {
			continue
		}
		eligible = append(eligible, m)
	}
	if len(eligible) == 0 {
		return "", errNoCandidate
	}
	if reachableListed < listed && reachableListed+numSync <= listed {
		return "", errQuorum
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].FlushLSN != eligible[j].FlushLSN {
			return eligible[i].FlushLSN > eligible[j].FlushLSN
		}
		return eligible[i].Name < eligible[j].Name
	})
	best := eligible[0]
	for _, m := range eligible {
		if m.Name == preferred && m.FlushLSN == best.FlushLSN {
			return m.Name, nil
		}
	}
	return best.Name, nil
}

// minSyncStandbys is the number of synchronous acknowledgements the cluster
// requires (ANY n); at least one.
func minSyncStandbys(c *pgshardv1alpha1.PgShardCluster) int {
	if c.Spec.Durability.MinSyncStandbys > 0 {
		return c.Spec.Durability.MinSyncStandbys
	}
	return 1
}

// refuseAsyncFailover refuses any promotion (automatic failover or
// operator-initiated switchover) under asynchronous durability
// (minSyncStandbys=0). No standby was required to acknowledge commits, so even
// the highest-flushed reachable standby may lag the old primary's acknowledged
// writes; promoting it would lose them. The CRD forbids minSyncStandbys=0
// today, so this is defence in depth.
func refuseAsyncFailover(c *pgshardv1alpha1.PgShardCluster) error {
	if c.Spec.Durability.MinSyncStandbys == 0 {
		return errAsyncFailover
	}
	return nil
}

// nextEpoch is the epoch a promotion must carry: above the group's epoch and
// above whatever the candidate already accepted.
func nextEpoch(groupEpoch int64, agentEpoch uint64) int64 {
	if e := int64(agentEpoch) + 1; e > groupEpoch+1 {
		return e
	}
	return groupEpoch + 1
}

// promotionEpoch is the epoch to (re)promote the designated primary with
// when it still reports itself a standby: the group epoch unless the agent
// already accepted it.
func promotionEpoch(groupEpoch int64, agentEpoch uint64) int64 {
	if int64(agentEpoch) >= groupEpoch {
		return int64(agentEpoch) + 1
	}
	return groupEpoch
}

// primaryHealthy is the readiness signal for the failover timer: the pod
// exists and either the kubelet reports it Ready or the agent still answers
// Status as a running primary.
func primaryHealthy(pod *corev1.Pod, ready bool, st AgentStatus, stErr error) bool {
	if pod == nil {
		return false
	}
	return ready || (stErr == nil && st.Running && st.Primary)
}

// leaseFenceable reports whether the operator may write the group Lease
// over its current holder: nobody, an expired holder, the operator itself,
// or one of allowed (the old primary it is failing away from, the member it
// is handing the Lease to).
func leaseFenceable(l *coordinationv1.Lease, now time.Time, allowed ...string) bool {
	holder := ptr.Deref(l.Spec.HolderIdentity, "")
	if holder == "" || holder == FenceHolder {
		return true
	}
	for _, a := range allowed {
		if a != "" && holder == a {
			return true
		}
	}
	if l.Spec.RenewTime == nil {
		return true
	}
	dur := time.Duration(ptr.Deref(l.Spec.LeaseDurationSeconds, 15)) * time.Second
	return now.After(l.Spec.RenewTime.Add(dur))
}

func (r *ClusterReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *ClusterReconciler) failoverDelay() time.Duration {
	if r.FailoverDelay > 0 {
		return r.FailoverDelay
	}
	return DefaultFailoverDelay
}

// repromoteInterval is the least time between re-promotions of a primary
// whose post-promotion setup keeps failing.
const repromoteInterval = 30 * time.Second

// repromoteDue reports whether a re-promotion of the group's pending primary
// may be issued now, recording the attempt when it may.
func (r *ClusterReconciler) repromoteDue(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastRepromote == nil {
		r.lastRepromote = map[string]time.Time{}
	}
	if last, ok := r.lastRepromote[key]; ok && r.now().Sub(last) < repromoteInterval {
		return false
	}
	r.lastRepromote[key] = r.now()
	return true
}

// unhealthyFor records that the group's primary was unhealthy at now and
// returns how long it has been continuously so.
func (r *ClusterReconciler) unhealthyFor(key string, unhealthy bool) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unhealthySince == nil {
		r.unhealthySince = map[string]time.Time{}
	}
	if !unhealthy {
		delete(r.unhealthySince, key)
		return 0
	}
	since, ok := r.unhealthySince[key]
	if !ok {
		since = r.now()
		r.unhealthySince[key] = since
	}
	return r.now().Sub(since)
}

// fenceLease takes the group Lease for the operator (or hands it to holder)
// and publishes the epoch and primary as annotations. It refuses when the
// Lease is renewed by anyone but the old primary or the operator.
func (r *ClusterReconciler) fenceLease(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, oldPrimary, holder string, epoch int64, primary string) error {
	key := types.NamespacedName{Namespace: c.Namespace, Name: g.LeaseName()}
	var lease coordinationv1.Lease
	err := r.Get(ctx, key, &lease)
	create := apierrors.IsNotFound(err)
	if err != nil && !create {
		return err
	}
	if !create && !leaseFenceable(&lease, r.now(), oldPrimary, holder) {
		return fmt.Errorf("%w: %s", ErrLeaseHeldByOther, ptr.Deref(lease.Spec.HolderIdentity, ""))
	}
	lease.Name = key.Name
	lease.Namespace = key.Namespace
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[AnnotationPrimaryEpoch] = fmt.Sprint(epoch)
	lease.Annotations[AnnotationPrimary] = primary
	now := metav1.NewMicroTime(r.now())
	if ptr.Deref(lease.Spec.HolderIdentity, "") != holder {
		lease.Spec.AcquireTime = &now
		lease.Spec.LeaseTransitions = ptr.To(ptr.Deref(lease.Spec.LeaseTransitions, 0) + 1)
	}
	lease.Spec.HolderIdentity = ptr.To(holder)
	lease.Spec.LeaseDurationSeconds = ptr.To(fenceLeaseSeconds)
	lease.Spec.RenewTime = &now
	if create {
		return r.Create(ctx, &lease)
	}
	return r.Update(ctx, &lease)
}

// releaseLease clears a fence the operator holds so a primary can start.
func (r *ClusterReconciler) releaseLease(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) error {
	var lease coordinationv1.Lease
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: g.LeaseName()}, &lease); err != nil {
		return client.IgnoreNotFound(err)
	}
	if ptr.Deref(lease.Spec.HolderIdentity, "") != FenceHolder {
		return nil
	}
	lease.Spec.HolderIdentity = ptr.To("")
	return r.Update(ctx, &lease)
}

// publishFence writes the new epoch and primary everywhere a reader may look
// before anything is promoted: the catalog shard_status row (shard groups),
// the PgShardGroup status and the Lease annotations (all groups).
func (r *ClusterReconciler) publishFence(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, oldPrimary, primary string, epoch int64, password string) error {
	if g.Kind == "shard" {
		catalog := Groups(c)[0]
		if err := r.Prober.PublishShardStatus(ctx, DSN(catalog.ServiceRW(), c.Namespace, password), []ShardStatus{{Group: g, Epoch: epoch, Endpoint: r.memberEndpoint(c, g, primary)}}); err != nil {
			return fmt.Errorf("publish shard_status: %w", err)
		}
	}
	var pg pgshardv1alpha1.PgShardGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: g.Prefix()}, &pg); err != nil {
		return err
	}
	base := pg.DeepCopy()
	pg.Status.Primary = primary
	pg.Status.Epoch = epoch
	if err := r.Status().Patch(ctx, &pg, client.MergeFrom(base)); err != nil {
		return err
	}
	return r.fenceLease(ctx, c, g, oldPrimary, primary, epoch, primary)
}

// memberEndpoint is what shard_status.primary_endpoint carries: the pooler
// sidecar of the primary, which is what the router dials.
func (r *ClusterReconciler) memberEndpoint(c *pgshardv1alpha1.PgShardCluster, g Group, member string) string {
	return fmt.Sprintf("%s:%d", g.MemberHost(member, c.Namespace), poolerGRPCPort)
}

func agentAddr(ip string) string { return fmt.Sprintf("%s:%d", ip, agentGRPCPort) }

// fencePod deletes the old primary's Pod and waits for it to be gone, so a
// primary that is alive but unreachable cannot keep writing while a successor
// is promoted. A Pod that is already absent needs nothing. This is not
// positive fencing under every failure: a node that is up but partitioned from
// the API server will not act on the delete, which is why the agent also has a
// watchdog whose deadline the kubelet enforces.
func (r *ClusterReconciler) fencePod(ctx context.Context, m *memberInfo) error {
	if m == nil || m.pod == nil {
		return nil
	}
	if err := r.Delete(ctx, m.pod, client.GracePeriodSeconds(0)); client.IgnoreNotFound(err) != nil {
		return err
	}
	deadline := r.now().Add(r.podFenceTimeout())
	for {
		var pod corev1.Pod
		err := r.Get(ctx, client.ObjectKeyFromObject(m.pod), &pod)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if pod.UID != m.pod.UID {
			// Replaced already, so the one that was primary is gone.
			return nil
		}
		if r.now().After(deadline) {
			return fmt.Errorf("pod %s still present after %s", m.pod.Name, r.podFenceTimeout())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.pollInterval()):
		}
	}
}

// DefaultPodFenceTimeout bounds the wait for the old primary's Pod to go.
const DefaultPodFenceTimeout = 60 * time.Second

func (r *ClusterReconciler) podFenceTimeout() time.Duration {
	if r.PodFenceTimeout > 0 {
		return r.PodFenceTimeout
	}
	return DefaultPodFenceTimeout
}

func (r *ClusterReconciler) patchRole(ctx context.Context, pod *corev1.Pod, role string) error {
	if pod == nil || pod.Labels[LabelRole] == role {
		return nil
	}
	base := pod.DeepCopy()
	pod.Labels[LabelRole] = role
	return r.Patch(ctx, pod, client.MergeFrom(base))
}

// failover moves the group's primary away from oldPrimary. planned is set
// for a switchover, where oldPrimary was stopped deliberately and preferred
// names the member to promote.
func (r *ClusterReconciler) failover(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, state groupState, members map[string]*memberInfo, password, preferred string) (groupState, error) {
	log := logf.FromContext(ctx).WithValues("group", g.Name(), "oldPrimary", state.primary)
	old := state.primary
	if err := refuseAsyncFailover(c); err != nil {
		return state, err
	}
	// With no standby ever observed streaming there is nothing that can hold
	// an acknowledged commit; refuse rather than promote an empty clone.
	if len(state.syncSet) == 0 {
		return state, errNoCandidate
	}
	if m := members[old]; m != nil {
		if err := r.patchRole(ctx, m.pod, RoleUnhealthy); err != nil {
			return state, err
		}
	}
	if err := r.fenceLease(ctx, c, g, old, FenceHolder, state.epoch, ""); err != nil {
		return state, err
	}
	log.Info("failover started: lease fenced, waiting for the old primary to stop and standbys to disconnect")
	if r.Metrics != nil {
		r.Metrics.Failovers.Inc()
	}

	views, err := r.quiesce(ctx, g, old, members, password)
	if err != nil {
		return state, err
	}
	for i := range views {
		// Every non-primary member is in synchronous_standby_names.
		views[i].Listed = views[i].Name != old
	}
	candidate, err := chooseCandidate(views, old, preferred, minSyncStandbys(c))
	if err != nil {
		log.Info("no failover candidate; releasing the fence", "views", fmt.Sprintf("%+v", views))
		return state, errors.Join(err, r.releaseLease(ctx, c, g))
	}
	var candEpoch uint64
	if m := members[candidate]; m != nil && m.ip != "" {
		if st, err := r.Agents.Status(ctx, agentAddr(m.ip)); err == nil {
			candEpoch = st.Epoch
		}
	}
	// quiesce treats an agent it cannot reach as gone, because an unreachable
	// agent is the common case in a failover and waiting for one that will
	// never answer would make every partition an outage. That leaves the old
	// primary possibly alive and writable. Remove its Pod before publishing a
	// new epoch, so a primary the operator cannot talk to is still stopped by
	// the kubelet rather than assumed stopped.
	if err := r.fencePod(ctx, members[old]); err != nil {
		log.Info("cannot fence the old primary; not promoting", "old", old, "err", err)
		return state, errors.Join(fmt.Errorf("fencing %s: %w", old, err), r.releaseLease(ctx, c, g))
	}
	epoch := nextEpoch(state.epoch, candEpoch)
	if err := r.publishFence(ctx, c, g, old, candidate, epoch, password); err != nil {
		return state, err
	}
	state.primary, state.epoch = candidate, epoch
	log.Info("fence published; promoting", "candidate", candidate, "epoch", epoch)
	if err := r.Agents.Promote(ctx, agentAddr(members[candidate].ip), uint64(epoch), candidate); err != nil {
		return state, fmt.Errorf("promote %s: %w", candidate, err)
	}
	// A promotion mid-barrier hands service to a member that never received
	// the pause, and the agent rewrote postgresql.auto.conf on its way up.
	// Reapply it from the fence before the new primary is labelled to serve,
	// rather than leaving the gap for the next reconcile to close.
	r.pauseIfFenced(ctx, c, g, HostDSN(members[candidate].ip, password), password)
	if err := r.patchRole(ctx, members[candidate].pod, RolePrimary); err != nil {
		return state, err
	}
	// Standbys stream through named slots; the new primary only inherits the
	// slots that slot sync copied while it was a standby. Create the rest now
	// so sync-rep commits against it cannot wait on a standby that can never
	// reconnect.
	var slots []string
	for _, name := range g.MemberNames() {
		if name != candidate {
			slots = append(slots, SlotName(name))
		}
	}
	if err := r.Prober.EnsureSlots(ctx, HostDSN(members[candidate].ip, password), slots, SlotName(candidate)); err != nil {
		log.Error(err, "ensure slots on the new primary; retried next reconcile")
	}
	r.unhealthyFor(g.Prefix(), false)
	log.Info("failover complete", "primary", candidate, "epoch", epoch)
	return state, nil
}

// pauseIfFenced makes a primary refuse writes when the catalog write fence
// is raised, so a group that changes primary during a barrier comes back
// holding still rather than serving writes the barrier believes are stopped.
func (r *ClusterReconciler) pauseIfFenced(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, dsn, password string) {
	log := logf.FromContext(ctx)
	if g.Kind != "shard" {
		return
	}
	// A promotion is never held up by this. Failing over is what keeps the
	// shard serving at all, and a barrier certifies that every shard is
	// still refusing writes before it records anything, so a primary that
	// comes up unpaused fails the barrier rather than corrupting its point.
	fenced, err := r.Prober.WriteFenced(ctx, DSN(Groups(c)[0].ServiceRW(), c.Namespace, password))
	if err != nil {
		log.Info("could not read the write fence while promoting; continuing", "group", g.Name(), "err", err)
		return
	}
	if !fenced {
		return
	}
	if err := r.Prober.PauseWrites(ctx, dsn); err != nil {
		log.Error(err, "promoted under a raised write fence but could not pause the new primary", "group", g.Name())
		return
	}
	log.Info("promoted under a raised write fence; the new primary refuses writes", "group", g.Name())
}

// quiesce waits until the old primary no longer answers as a running primary
// and every other reachable member has stopped streaming, then returns the
// members' views. After standbyQuiesceTimeout it proceeds with whatever is
// reachable, unless the old primary is still live.
func (r *ClusterReconciler) quiesce(ctx context.Context, g Group, old string, members map[string]*memberInfo, password string) ([]memberView, error) {
	log := logf.FromContext(ctx).WithValues("group", g.Name(), "oldPrimary", old)
	deadline := r.now().Add(r.quiesceTimeout())
	for {
		oldGone := true
		if m := members[old]; m != nil && m.pod != nil && m.ip != "" {
			if st, err := r.Agents.Status(ctx, agentAddr(m.ip)); err == nil && st.Running && st.Primary {
				oldGone = false
			}
		}
		views := make([]memberView, 0, len(members))
		allStopped := true
		for _, name := range g.MemberNames() {
			if name == old {
				continue
			}
			v := memberView{Name: name}
			if m := members[name]; m != nil && m.pod != nil && m.ip != "" {
				st, err := r.Prober.ProbeStandby(ctx, HostDSN(m.ip, password))
				if err != nil {
					// quiesce polls, so logging each failure would bury the
					// run. The views are already logged every iteration.
					v.Why = err.Error()
				} else {
					v.Reachable, v.InRecovery, v.Streaming, v.FlushLSN = true, st.InRecovery, st.Streaming, st.FlushLSN
				}
			}
			if !v.Reachable || v.Streaming {
				allStopped = false
			}
			views = append(views, v)
		}
		if oldGone && allStopped {
			return views, nil
		}
		log.V(1).Info("quiesce: waiting", "oldGone", oldGone, "views", fmt.Sprintf("%+v", views))
		if r.now().After(deadline) {
			if !oldGone {
				return nil, errPrimaryStillLive
			}
			return views, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(r.pollInterval()):
		}
	}
}

func (r *ClusterReconciler) quiesceTimeout() time.Duration {
	if r.QuiesceTimeout > 0 {
		return r.QuiesceTimeout
	}
	return standbyQuiesceTimeout
}

func (r *ClusterReconciler) pollInterval() time.Duration {
	if r.PollInterval > 0 {
		return r.PollInterval
	}
	return standbyQuiescePoll
}

// converge repairs a group whose designated primary is not acting as one:
// a standby that must be (re)promoted, or a former primary that must be
// demoted. It returns the possibly bumped state.
// admissiblePrimary reports why target must not be promoted, or "" when it
// may be. Promotion is safe only for the member that holds every
// acknowledged commit, which is what chooseCandidate encodes; the failover
// path has always asked it, and this is the same question asked on the
// healthy path, so there is one rule for who may become primary.
//
// The synchronous set is what state.syncSet last observed streaming, so a
// member that has never streamed cannot hold an acknowledgement and does
// not make the group unpromotable while it is still being created.
func (r *ClusterReconciler) admissiblePrimary(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, state groupState, members map[string]*memberInfo, password, target string) string {
	views := make([]memberView, 0, len(members))
	for _, name := range g.MemberNames() {
		v := memberView{Name: name, Listed: state.syncSet[name]}
		if m := members[name]; m != nil && m.pod != nil && m.ip != "" {
			st, err := r.Prober.ProbeStandby(ctx, HostDSN(m.ip, password))
			if err != nil {
				// An unreachable synchronous standby is the one condition
				// that refuses promotion outright, and discarding the
				// error left no way to tell a member that is genuinely
				// gone from one the probe itself could not reach.
				v.Why = err.Error()
				logf.FromContext(ctx).Info("member did not answer the promotion probe",
					"group", g.Name(), "member", name, "listed", v.Listed, "err", err.Error())
			} else {
				v.Reachable, v.InRecovery, v.Streaming, v.FlushLSN = true, st.InRecovery, st.Streaming, st.FlushLSN
			}
		}
		views = append(views, v)
	}
	for _, v := range views {
		// A live primary elsewhere is the failover path's business, not
		// this one: promoting beside it is how two primaries happen, and
		// the commits it has taken are not on the member being promoted.
		if v.Reachable && !v.InRecovery && v.Name != target {
			return fmt.Sprintf("designated primary %s not promoted: %s is running as a primary", target, v.Name)
		}
	}
	candidate, err := chooseCandidate(views, "", target, minSyncStandbys(c))
	switch {
	case err != nil:
		return fmt.Sprintf("designated primary %s not promoted: %v", target, err)
	case candidate != target:
		return fmt.Sprintf("designated primary %s not promoted: %s holds a higher flushed LSN", target, candidate)
	}
	return ""
}

func (r *ClusterReconciler) converge(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, state groupState, members map[string]*memberInfo, password string) (groupState, string, error) {
	log := logf.FromContext(ctx).WithValues("group", g.Name())
	refused := ""
	for _, name := range g.MemberNames() {
		m := members[name]
		if m == nil || m.pod == nil || m.ip == "" {
			continue
		}
		st, err := r.Agents.Status(ctx, agentAddr(m.ip))
		if err != nil || !st.Running {
			continue
		}
		switch {
		case name == state.primary && (!st.Primary || st.PromotionPending):
			// A designated primary that is still a standby must be promoted;
			// one that promoted but whose post-promotion setup failed reports
			// PromotionPending and is re-promoted (the agent's Promote is
			// idempotent and only re-runs the setup) so it never stays
			// half-configured. Each re-promote bumps the epoch and rewrites
			// the fence, so a persistent setup failure is retried no faster
			// than repromoteInterval instead of on every reconcile.
			if st.Primary && st.PromotionPending && !r.repromoteDue(g.Prefix()) {
				log.Info("designated primary still finishing its promotion; waiting before re-promoting", "member", name)
				continue
			}
			// A member that is already primary is finishing a promotion the
			// operator decided on; only a standby is a new promotion, and
			// only that needs the candidate gate.
			if !st.Primary {
				if why := r.admissiblePrimary(ctx, c, g, state, members, password, name); why != "" {
					log.Info("refusing to promote the designated primary", "member", name, "reason", why)
					refused = why
					continue
				}
			}
			epoch := promotionEpoch(state.epoch, st.Epoch)
			if epoch != state.epoch {
				if err := r.publishFence(ctx, c, g, "", name, epoch, password); err != nil {
					return state, refused, err
				}
				state.epoch = epoch
			}
			log.Info("designated primary needs (re)promotion", "member", name, "epoch", epoch, "standby", !st.Primary, "promotionPending", st.PromotionPending)
			if err := r.Agents.Promote(ctx, agentAddr(m.ip), uint64(epoch), name); err != nil {
				return state, refused, fmt.Errorf("promote %s: %w", name, err)
			}
			if err := r.patchRole(ctx, m.pod, RolePrimary); err != nil {
				return state, refused, err
			}
		case name == state.primary && m.pod.Labels[LabelRole] != RolePrimary:
			// Promoted, healthy, and wearing the wrong label: the failover
			// patches the label after the promotion, so an operator that
			// exits in between leaves a shard with a writable primary that
			// the -rw Service selects nothing for. Nothing else repairs it,
			// because every other case here asks the agent what it is, and
			// the agent is right -- it is the label that is stale.
			log.Info("designated primary is not labelled primary; repairing", "member", name, "epoch", state.epoch)
			if err := r.patchRole(ctx, m.pod, RolePrimary); err != nil {
				return state, refused, err
			}
		case name != state.primary && st.Primary:
			log.Info("member reports itself primary but is not designated; demoting", "member", name, "epoch", state.epoch)
			if err := r.patchRole(ctx, m.pod, RoleUnhealthy); err != nil {
				return state, refused, err
			}
			if err := r.Agents.Demote(ctx, agentAddr(m.ip), uint64(state.epoch)); err != nil {
				log.Info("demote failed; will retry", "member", name, "err", err.Error())
			}
		case name != state.primary && m.pod.Labels[LabelRole] != RoleReplica:
			if err := r.patchRole(ctx, m.pod, RoleReplica); err != nil {
				return state, refused, err
			}
		}
	}
	return state, refused, nil
}
