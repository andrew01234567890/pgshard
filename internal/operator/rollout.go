package operator

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// DefaultRolloutTimeout is how long one rolling step (a member restart or
// rebuild) may take before the cluster reports Degraded and the rollout holds.
const DefaultRolloutTimeout = 15 * time.Minute

// AnnotationSettingsHash on a member pod is the settings hash the member
// runs with; it changes on a live reload without a pod restart.
const AnnotationSettingsHash = "pgshard.io/settings-hash"

// storageChange classifies the difference between a member's PVC and the
// spec: nothing, an online expansion, or a rebuild onto a new claim.
type storageChange int

const (
	storageUnchanged storageChange = iota
	storageExpand
	storageRebuild
	// storageShrink is a size decrease: PVCs cannot shrink, so it is
	// reported and otherwise ignored.
	storageShrink
)

func (s storageChange) String() string {
	return [...]string{"unchanged", "expand", "rebuild", "shrink"}[s]
}

// classifyStorage compares the live claim with the desired storage. A nil
// desired class means "no opinion" (admission fills the default class in).
func classifyStorage(pvc *corev1.PersistentVolumeClaim, want pgshardv1alpha1.StorageSpec, expandable bool) storageChange {
	if want.StorageClassName != nil && !equalClass(pvc.Spec.StorageClassName, want.StorageClassName) {
		return storageRebuild
	}
	have := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	switch have.Cmp(want.Size) {
	case 0:
		return storageUnchanged
	case 1:
		return storageShrink
	}
	if expandable {
		return storageExpand
	}
	return storageRebuild
}

func equalClass(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// nextPVCName returns the successor claim name: member-v2 after member,
// member-v3 after member-v2.
func nextPVCName(member, current string) string {
	n := 1
	if rest, ok := strings.CutPrefix(current, member+"-v"); ok {
		if v, err := strconv.Atoi(rest); err == nil {
			n = v
		}
	}
	return fmt.Sprintf("%s-v%d", member, n+1)
}

// needsRestart reports whether the settings change from before to after
// needs a postmaster restart, given the live pg_settings contexts.
func needsRestart(changed []string, live map[string]SettingState) bool {
	for _, name := range changed {
		st, ok := live[name]
		if !ok || st.Context == "postmaster" || st.Context == "internal" {
			return true
		}
	}
	return false
}

// rolloutOrder returns the stale members standbys first (by name) and the
// primary last.
func rolloutOrder(stale []string, primary string) []string {
	out := make([]string, 0, len(stale))
	hasPrimary := false
	for _, m := range stale {
		if m == primary {
			hasPrimary = true
			continue
		}
		out = append(out, m)
	}
	sort.Strings(out)
	if hasPrimary {
		out = append(out, primary)
	}
	return out
}

// memberStaleness is what one member's pod carries versus the desired template.
type memberStaleness struct {
	restart bool
	reload  bool
}

// classifyPod compares a pod's stamps with the template. settingsRestart is
// the group's sticky flag: a postmaster-context setting changed since the
// pods last restarted, so a settings difference means a restart.
func classifyPod(pod *corev1.Pod, tpl MemberTemplate, settingsRestart bool) memberStaleness {
	if pod == nil {
		return memberStaleness{}
	}
	tplStale := pod.Annotations[AnnotationTemplateHash] != tpl.Hash()
	setStale := pod.Annotations[AnnotationSettingsHash] != tpl.SettingsHash()
	if tplStale || (setStale && settingsRestart) {
		return memberStaleness{restart: true}
	}
	return memberStaleness{reload: setStale}
}

func (r *ClusterReconciler) rolloutTimeout() time.Duration {
	if r.RolloutTimeout > 0 {
		return r.RolloutTimeout
	}
	return DefaultRolloutTimeout
}

// classifySettingsChange runs before the ConfigMap is rewritten: it diffs
// the settings the ConfigMap was rendered with against the desired ones and
// asks the primary which of them need a restart. It returns whether the
// group must restart for this change and whether the answer is known (a
// primary was reachable, or nothing changed).
func (r *ClusterReconciler) classifySettingsChange(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, tpl MemberTemplate, primaryIP, password string) (restart bool, known bool, err error) {
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: g.ConfigMapName()}, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return false, true, nil
		}
		return false, false, err
	}
	changed := changedSettings(ConfigMapSettings(&cm), tpl.Settings)
	if len(changed) == 0 {
		return false, true, nil
	}
	if primaryIP == "" {
		return false, false, nil
	}
	live, err := r.Prober.Settings(ctx, HostDSN(primaryIP, password), changed)
	if err != nil {
		logf.FromContext(ctx).Info("cannot classify settings change; holding the ConfigMap", "group", g.Name(), "err", err.Error())
		return false, false, nil
	}
	sort.Strings(changed)
	logf.FromContext(ctx).Info("settings changed", "group", g.Name(), "names", changed, "restart", needsRestart(changed, live))
	return needsRestart(changed, live), true, nil
}

// rollout advances the group's rolling operation by at most one step per
// pass. It only runs on a healthy, fully streaming group; a step in flight
// is waited on and reported, and held with Degraded after the timeout.
func (r *ClusterReconciler) rollout(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, obs *groupObservation, members map[string]*memberInfo, password string) error {
	log := logf.FromContext(ctx).WithValues("group", g.Name())
	pg, err := r.getGroup(ctx, c, g)
	if err != nil {
		return err
	}
	tpl := obs.template
	var restartList, reloadList []string
	for _, name := range g.MemberNames() {
		m := members[name]
		if m == nil || m.pod == nil {
			continue
		}
		s := classifyPod(m.pod, tpl, pg.Status.SettingsRestartPending)
		if s.restart {
			restartList = append(restartList, name)
		}
		if s.reload {
			reloadList = append(reloadList, name)
		}
	}
	if r.Metrics != nil {
		r.Metrics.RollingUpdates.WithLabelValues(c.Namespace + "/" + c.Name).Set(float64(len(restartList)))
	}
	storage, err := r.classifyMemberStorage(ctx, c, g, obs.state)
	if err != nil {
		return err
	}
	var rebuildList, expandList []string
	for _, name := range g.MemberNames() {
		switch storage[name] {
		case storageRebuild:
			rebuildList = append(rebuildList, name)
		case storageExpand:
			expandList = append(expandList, name)
		}
	}

	settled := obs.ready() && obs.podsReady == g.Replicas && obs.syncApplied && obs.streamingCount() == g.Replicas-1
	inFlight := pg.Status.Rollout
	if inFlight != nil && !settled {
		r.reportStep(obs, inFlight)
		return nil
	}
	if !settled {
		obs.rollout = nil
		return nil
	}

	// A step in flight has completed once the group is settled again; a
	// rebuild also retires the old claim now that the new member streams.
	if inFlight != nil {
		if inFlight.Phase == pgshardv1alpha1.RolloutPhaseRebuilding {
			if err := r.deleteRetiredPVCs(ctx, c, g, inFlight.Member, obs.state.pvcs[inFlight.Member]); err != nil {
				return err
			}
		}
		if err := r.setGroupRollout(ctx, c, g, nil); err != nil {
			return err
		}
		log.Info("rollout step complete", "phase", inFlight.Phase, "member", inFlight.Member)
	}

	// A member the spec has stopped describing is retired before anything
	// else: while it is there the group has more members than it should,
	// and every other step reasons about the ones it does.
	extras, err := r.extraMembers(ctx, c, g)
	if err != nil {
		return err
	}
	if len(extras) > 0 {
		return r.stepRetire(ctx, c, g, obs, members, password, extras)
	}

	if len(restartList) == 0 && len(rebuildList) == 0 && len(reloadList) == 0 && len(expandList) == 0 {
		if pg.Status.SettingsRestartPending {
			if err := r.patchGroupStatus(ctx, c, g, func(pg *pgshardv1alpha1.PgShardGroup) { pg.Status.SettingsRestartPending = false }); err != nil {
				return err
			}
		}
		obs.rollout = nil
		return nil
	}

	for _, name := range expandList {
		if err := r.expandPVC(ctx, c, g, name, obs.state.pvcs[name]); err != nil {
			return err
		}
	}

	// Rebuilds and restarts change one member at a time; a member that
	// needs both gets a single rebuild (the new pod carries the new template).
	restarts := map[string]bool{}
	for _, m := range restartList {
		restarts[m] = true
	}
	primary := obs.state.primary
	if len(rebuildList) > 0 || len(restartList) > 0 {
		if obs.streamingCount()-1 < c.Spec.Durability.MinSyncStandbys {
			obs.rollout = &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseHeld, Reason: "restarting a standby would drop the sync set below minSyncStandbys"}
			return nil
		}
		order := rolloutOrder(append(append([]string{}, rebuildList...), restartList...), primary)
		next := order[0]
		if next == primary {
			return r.stepAwayFromPrimary(ctx, c, g, obs, members, password)
		}
		if storage[next] == storageRebuild {
			return r.stepRebuild(ctx, c, g, obs, members[next])
		}
		return r.stepRestart(ctx, c, g, obs, members[next])
	}

	return r.stepReload(ctx, g, obs, members, reloadList)
}

// stepRetire removes one member the lowered replica count no longer
// describes, and only one: the group is down a member while it happens, and
// taking two out at once is what leaves a group with no promotable standby.
//
// It runs from the same settled gate as every other step, so the previous
// retirement has to have finished before the next begins.
func (r *ClusterReconciler) stepRetire(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, obs *groupObservation,
	members map[string]*memberInfo, password string, extras []string) error {
	next := extras[0]
	// Whether this member is the primary is asked of the pod, not of the
	// designation. loadState re-designates a primary that is outside the
	// member set (the guard PGS-466 added), so by the time a retirement
	// runs, state.primary already names someone else -- while PostgreSQL
	// on the retired member may still be the primary. Deleting it then
	// would be a failover the operator did to itself, and to a member it
	// was in the middle of removing.
	//
	// Held rather than switched: the switchover path moves the designated
	// primary, which is not this one. What has to happen first is that the
	// role moves off the member being retired, and holding says so instead
	// of guessing at it.
	if primaryPod, err := r.memberIsPrimary(ctx, c, next); err != nil {
		return err
	} else if primaryPod {
		obs.rollout = &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseHeld, Member: next,
			Reason: next + " is still the primary; retiring it would fail the group over to a member it is removing"}
		return r.setGroupRollout(ctx, c, g, obs.rollout)
	}
	if next == obs.state.primary {
		// A planned switchover, not a failover: the primary is healthy and
		// the point is to leave it that way. Retirement continues on a
		// later pass, once the group has a primary it is keeping.
		return r.stepAwayFromPrimary(ctx, c, g, obs, members, password)
	}
	// synchronous_standby_names is rewritten from MemberNames() every pass,
	// so a lowered count has already taken this member out of it. Confirm
	// that reached the primary before the pod goes: deleting a member the
	// primary is still waiting on would stall every commit until the wait
	// timed out.
	if !obs.syncApplied {
		obs.rollout = &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseHeld, Member: next,
			Reason: "waiting for synchronous_standby_names to drop " + next + " before retiring it"}
		return nil
	}
	// The slot goes before the pod, and the pod does not go until it has.
	// A slot left behind pins WAL on the primary until the disk fills --
	// the failure the old refusal existed to prevent -- and once the pod is
	// gone nothing lists this member any more (its claim is kept on
	// purpose and is not work), so there is no later pass to try again on.
	//
	// DropSlot, not EnsureSlots' drop: that one drops only an INACTIVE
	// slot, which is right for the slot a new primary holds for itself and
	// wrong here, because a member being retired streams from its slot
	// until the moment its pod goes. It did nothing and said nothing.
	if primary := members[obs.state.primary]; primary != nil && primary.ip != "" {
		if err := r.Prober.DropSlot(ctx, HostDSN(primary.ip, password), SlotName(next)); err != nil {
			obs.rollout = &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseHeld, Member: next,
				Reason: "cannot drop the replication slot of " + next + " on the primary: " + err.Error()}
			return r.setGroupRollout(ctx, c, g, obs.rollout)
		}
	}
	step := &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseRetiring, Member: next,
		Reason: "replica count lowered to " + strconv.Itoa(g.Replicas), Since: r.metaNow()}
	if err := r.setGroupRollout(ctx, c, g, step); err != nil {
		return err
	}
	if err := r.retireMember(ctx, c, g, next); err != nil {
		return err
	}
	obs.rollout = step
	return nil
}

// stepRestart deletes one stale standby pod; it comes back with the new
// template on the same claim.
func (r *ClusterReconciler) stepRestart(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, obs *groupObservation, m *memberInfo) error {
	step := &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseRestarting, Member: m.name, Reason: "template changed", Since: r.metaNow()}
	if err := r.setGroupRollout(ctx, c, g, step); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("rolling restart: deleting standby pod", "group", g.Name(), "member", m.name)
	if err := r.Delete(ctx, m.pod); client.IgnoreNotFound(err) != nil {
		return err
	}
	obs.rollout = step
	return nil
}

// stepRebuild moves one standby onto a fresh claim: the successor PVC is
// created and recorded, the pod deleted; the recreated pod clones from the
// primary. The old claim stays until the member streams again.
func (r *ClusterReconciler) stepRebuild(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, obs *groupObservation, m *memberInfo) error {
	current := obs.state.pvcs[m.name]
	next := nextPVCName(m.name, current)
	if err := r.ensureOwned(ctx, c, r.Renderer.PVC(c, g, ordinalOf(g, m.name), next), nil); err != nil {
		return err
	}
	step := &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseRebuilding, Member: m.name, Reason: "storage changed: " + current + " -> " + next, Since: r.metaNow()}
	obs.state.pvcs[m.name] = next
	for i := range obs.members {
		if obs.members[i].Name == m.name {
			obs.members[i].PVC = next
		}
	}
	if err := r.patchGroupStatus(ctx, c, g, func(pg *pgshardv1alpha1.PgShardGroup) {
		pg.Status.Rollout = step
		setMemberPVC(pg, m.name, next)
	}); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("storage rebuild: deleting standby pod", "group", g.Name(), "member", m.name, "pvc", next)
	if err := r.Delete(ctx, m.pod); client.IgnoreNotFound(err) != nil {
		return err
	}
	obs.rollout = step
	return nil
}

// stepAwayFromPrimary requests a switchover to the freshest standby so the
// primary can be restarted or rebuilt as a standby on a later pass. Only
// one switchover is in flight per cluster.
func (r *ClusterReconciler) stepAwayFromPrimary(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, obs *groupObservation, members map[string]*memberInfo, password string) error {
	if cur := c.Annotations[AnnotationSwitchover]; cur != "" {
		step := &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseSwitchover, Member: obs.state.primary, Reason: "waiting for the switchover to " + cur}
		obs.rollout = step
		return nil
	}
	target, why := r.freshestStandby(ctx, g, obs.state, members, password)
	if target == "" {
		obs.rollout = &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseHeld, Member: obs.state.primary, Reason: why}
		return nil
	}
	step := &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseSwitchover, Member: obs.state.primary, Reason: "switching over to " + target + " before restarting the primary", Since: r.metaNow()}
	if err := r.setGroupRollout(ctx, c, g, step); err != nil {
		return err
	}
	base := c.DeepCopy()
	if c.Annotations == nil {
		c.Annotations = map[string]string{}
	}
	c.Annotations[AnnotationSwitchover] = target
	if err := r.Patch(ctx, c, client.MergeFrom(base)); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("rollout: switchover requested", "group", g.Name(), "from", obs.state.primary, "to", target)
	obs.rollout = step
	return nil
}

// freshestStandby picks the reachable standby with the highest flushed LSN
// (planned switchover target); unlike failover it only considers standbys that
// were streaming, since the primary is alive and no acknowledgement can be
// lost by the choice.
// It returns the target and, when there is none, why -- an empty sync set
// and a sync set whose standbys did not answer are different problems and
// a held rollout should not report them with the same sentence.
func (r *ClusterReconciler) freshestStandby(ctx context.Context, g Group, state groupState, members map[string]*memberInfo, password string) (string, string) {
	var views []memberView
	for _, name := range g.MemberNames() {
		if name == state.primary || !state.syncSet[name] {
			continue
		}
		v := memberView{Name: name, Listed: true}
		if m := members[name]; m != nil && m.ip != "" {
			st, err := r.Prober.ProbeStandby(ctx, HostDSN(m.ip, password))
			if err != nil {
				v.Why = err.Error()
			} else {
				v.Reachable, v.InRecovery, v.FlushLSN = true, st.InRecovery, st.FlushLSN
				v.ReadySlots = r.readySlots(ctx, g, name, m.ip)
			}
		}
		views = append(views, v)
	}
	best, err := chooseCandidate(views, state.primary, "", 1)
	if err != nil {
		// A rollout held because the sync set is empty and one held
		// because its standbys did not answer look identical from the
		// outside, and an operator reading the second while looking at a
		// healthy sync set has been told something that is not true.
		logf.FromContext(ctx).Info("no switchover target", "group", g.Name(),
			"why", err.Error(), "views", fmt.Sprintf("%+v", views))
		if len(views) == 0 {
			return "", "no sync-set standby to switch over to"
		}
		if unreachable := unreachableStandbys(views); len(unreachable) > 0 {
			return "", "no sync-set standby answered its probe: " + strings.Join(unreachable, ", ")
		}
		return "", "no sync-set standby eligible to switch over to"
	}
	return best, ""
}

// unreachableStandbys names the sync-set standbys whose probe did not
// answer, so a held rollout can say which kind of nothing it found.
func unreachableStandbys(views []memberView) []string {
	var out []string
	for _, v := range views {
		if !v.Reachable {
			out = append(out, v.Name)
		}
	}
	return out
}

// stepReload asks every stale agent to reread its config and stamps the pod
// once the agent reports the desired settings hash. It repeats until all
// members are stamped; the ConfigMap volume may lag the API by a minute.
func (r *ClusterReconciler) stepReload(ctx context.Context, g Group, obs *groupObservation, members map[string]*memberInfo, names []string) error {
	log := logf.FromContext(ctx).WithValues("group", g.Name())
	want := obs.template.SettingsHash()
	step := &pgshardv1alpha1.GroupRollout{Phase: pgshardv1alpha1.RolloutPhaseReloading, Member: strings.Join(names, ","), Reason: "settings changed; reloading without restart"}
	remaining := 0
	for _, name := range names {
		m := members[name]
		if m == nil || m.ip == "" {
			remaining++
			continue
		}
		got, err := r.Agents.Reload(ctx, agentAddr(m.ip))
		if err != nil {
			log.Info("reload failed; will retry", "member", name, "err", err.Error())
			remaining++
			continue
		}
		if got != want {
			log.Info("agent has not seen the new settings yet", "member", name, "have", got, "want", want)
			remaining++
			continue
		}
		base := m.pod.DeepCopy()
		if m.pod.Annotations == nil {
			m.pod.Annotations = map[string]string{}
		}
		m.pod.Annotations[AnnotationSettingsHash] = want
		if err := r.Patch(ctx, m.pod, client.MergeFrom(base)); err != nil {
			return err
		}
		log.Info("settings reloaded", "member", name)
	}
	if remaining == 0 {
		obs.rollout = nil
		return nil
	}
	obs.rollout = step
	return nil
}

// reportStep publishes an in-flight or held step and marks the group held
// once it has taken longer than the rollout timeout.
func (r *ClusterReconciler) reportStep(obs *groupObservation, step *pgshardv1alpha1.GroupRollout) {
	held := *step
	if step.Since != nil && r.now().Sub(step.Since.Time) > r.rolloutTimeout() {
		held.Phase = pgshardv1alpha1.RolloutPhaseHeld
		held.Reason = fmt.Sprintf("%s of %s has not completed within %s; holding (%s)", step.Phase, step.Member, r.rolloutTimeout(), step.Reason)
	}
	obs.rollout = &held
}

func (r *ClusterReconciler) metaNow() *metav1.Time {
	t := metav1.NewTime(r.now())
	return &t
}

func (r *ClusterReconciler) getGroup(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) (*pgshardv1alpha1.PgShardGroup, error) {
	var pg pgshardv1alpha1.PgShardGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: g.Prefix()}, &pg); err != nil {
		return nil, err
	}
	return &pg, nil
}

// patchGroupStatus applies mutate to the group's status in one patch.
func (r *ClusterReconciler) patchGroupStatus(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, mutate func(*pgshardv1alpha1.PgShardGroup)) error {
	pg, err := r.getGroup(ctx, c, g)
	if err != nil {
		return err
	}
	base := pg.DeepCopy()
	mutate(pg)
	return r.Status().Patch(ctx, pg, client.MergeFrom(base))
}

// setGroupRollout persists the step in flight (nil when idle).
func (r *ClusterReconciler) setGroupRollout(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, step *pgshardv1alpha1.GroupRollout) error {
	return r.patchGroupStatus(ctx, c, g, func(pg *pgshardv1alpha1.PgShardGroup) { pg.Status.Rollout = step })
}

func setMemberPVC(pg *pgshardv1alpha1.PgShardGroup, member, pvc string) {
	for i := range pg.Status.Members {
		if pg.Status.Members[i].Name == member {
			pg.Status.Members[i].PVC = pvc
			return
		}
	}
	pg.Status.Members = append(pg.Status.Members, pgshardv1alpha1.MemberStatus{Name: member, PVC: pvc})
}

// classifyMemberStorage compares every member's current claim with the spec.
func (r *ClusterReconciler) classifyMemberStorage(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, state groupState) (map[string]storageChange, error) {
	out := map[string]storageChange{}
	classes := map[string]bool{}
	for _, name := range g.MemberNames() {
		var pvc corev1.PersistentVolumeClaim
		if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: state.pvcs[name]}, &pvc); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		expandable := false
		if sc := pvc.Spec.StorageClassName; sc != nil && *sc != "" {
			if v, ok := classes[*sc]; ok {
				expandable = v
			} else {
				expandable = r.classExpandable(ctx, *sc)
				classes[*sc] = expandable
			}
		}
		out[name] = classifyStorage(&pvc, g.Storage, expandable)
	}
	return out, nil
}

func (r *ClusterReconciler) classExpandable(ctx context.Context, name string) bool {
	var sc storagev1.StorageClass
	if err := r.Get(ctx, types.NamespacedName{Name: name}, &sc); err != nil {
		return false
	}
	return sc.AllowVolumeExpansion != nil && *sc.AllowVolumeExpansion
}

// expandPVC raises the claim's request in place; the volume grows online.
func (r *ClusterReconciler) expandPVC(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, member, pvcName string) error {
	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: pvcName}, &pvc); err != nil {
		return client.IgnoreNotFound(err)
	}
	base := pvc.DeepCopy()
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = g.Storage.Size
	logf.FromContext(ctx).Info("expanding claim online", "group", g.Name(), "member", member, "pvc", pvcName, "size", g.Storage.Size.String())
	return r.Patch(ctx, &pvc, client.MergeFrom(base))
}

// deleteRetiredPVCs removes every claim of member other than keep. It is
// only called once the member's pod on keep is Ready and streaming.
func (r *ClusterReconciler) deleteRetiredPVCs(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, member, keep string) error {
	var list corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &list, client.InNamespace(c.Namespace), client.MatchingLabels{LabelCluster: c.Name, LabelGroup: g.Name(), LabelMember: member}); err != nil {
		return err
	}
	if keep != member {
		var first corev1.PersistentVolumeClaim
		if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: member}, &first); err == nil {
			list.Items = append(list.Items, first)
		} else if !apierrors.IsNotFound(err) {
			return err
		}
	}
	seen := map[string]bool{}
	for i := range list.Items {
		pvc := &list.Items[i]
		if pvc.Name == keep || seen[pvc.Name] {
			continue
		}
		seen[pvc.Name] = true
		logf.FromContext(ctx).Info("deleting retired claim", "group", g.Name(), "member", member, "pvc", pvc.Name)
		if err := r.Delete(ctx, pvc); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}
