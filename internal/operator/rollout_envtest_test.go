package operator

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func podUID(t *testing.T, name string) string {
	t.Helper()
	var pod corev1.Pod
	get(t, name, &pod)
	return string(pod.UID)
}

func settingsStamp(t *testing.T, name string) string {
	t.Helper()
	var pod corev1.Pod
	get(t, name, &pod)
	return pod.Annotations[AnnotationSettingsHash]
}

func podExists(t *testing.T, name string) bool {
	t.Helper()
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, &corev1.Pod{})
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

func patchSpec(t *testing.T, c *pgshardv1alpha1.PgShardCluster, mutate func(*pgshardv1alpha1.PgShardCluster)) {
	t.Helper()
	get(t, c.Name, c)
	base := c.DeepCopy()
	mutate(c)
	if err := k8sClient.Patch(context.Background(), c, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
}

// memberBack simulates the kubelet bringing a recreated member up as a
// streaming standby.
func memberBack(t *testing.T, fp *fakeProber, fa *fakeAgents, name, ip string) {
	t.Helper()
	markPodRunning(t, name, ip, true)
	fa.set(ip, AgentStatus{Running: true}, nil)
	fp.mu.Lock()
	fp.streaming[name] = true
	fp.mu.Unlock()
}

func TestSighupSettingChangeReloadsWithoutRestart(t *testing.T) {
	r, _, fa, c := healthyCluster(t, "rl")
	uids := map[string]string{}
	for _, g := range Groups(c) {
		for _, m := range g.MemberNames() {
			uids[m] = podUID(t, m)
		}
	}
	patchSpec(t, c, func(c *pgshardv1alpha1.PgShardCluster) {
		c.Spec.PostgreSQL.Parameters["log_min_duration_statement"] = "250ms"
	})
	want := Template(c, nil, nil).SettingsHash()

	// The agents have not seen the new volume yet: nothing is stamped.
	reconcile(t, r, c)
	if got := settingsStamp(t, "rl-shard-0-1"); got == want {
		t.Fatal("pod must not be stamped before the agent reports the new hash")
	}
	if len(fa.reloads) != 6 {
		t.Fatalf("every member is asked to reload: %v", fa.reloads)
	}
	if got := condition(t, "rl", pgshardv1alpha1.ConditionRolloutInProgress); got.Status != metav1.ConditionTrue {
		t.Fatalf("RolloutInProgress: %+v", got)
	}
	var cm corev1.ConfigMap
	get(t, "rl-shard-0-config", &cm)
	if ConfigMapSettings(&cm)["log_min_duration_statement"] != "250ms" {
		t.Fatalf("ConfigMap must carry the new settings: %v", cm.Data[settingsKey])
	}

	for gi, g := range Groups(c) {
		for i := range g.MemberNames() {
			fa.reloadHash[agentAddr(podIP(gi, i))] = want
		}
	}
	reconcile(t, r, c)
	for _, g := range Groups(c) {
		for _, m := range g.MemberNames() {
			if podUID(t, m) != uids[m] {
				t.Fatalf("%s must not be restarted for a sighup setting", m)
			}
			if got := settingsStamp(t, m); got != want {
				t.Fatalf("%s settings stamp %q want %q", m, got, want)
			}
		}
	}
	if got := condition(t, "rl", pgshardv1alpha1.ConditionRolloutInProgress); got.Status != metav1.ConditionFalse {
		t.Fatalf("RolloutInProgress after reload: %+v", got)
	}
	if st := groupStatus(t, "rl-shard-0"); st.SettingsRestartPending || st.Rollout != nil {
		t.Fatalf("group status after reload: %+v", st)
	}
	get(t, "rl", c)
	if c.Status.Rollout.Phase != pgshardv1alpha1.RolloutPhaseIdle {
		t.Fatalf("status.rollout %+v", c.Status.Rollout)
	}
}

func TestPostmasterSettingChangeRollsStandbysThenSwitchesOver(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "rr")
	fp.settings = map[string]SettingState{"max_connections": {Context: "postmaster"}}
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 900}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 950}
	fp.standbys[podIP(0, 1)] = StandbyState{InRecovery: true, FlushLSN: 100}
	fp.standbys[podIP(0, 2)] = StandbyState{InRecovery: true, FlushLSN: 100}
	uid := map[string]string{}
	for _, m := range []string{"rr-shard-0-0", "rr-shard-0-1", "rr-shard-0-2", "rr-catalog-0", "rr-catalog-1", "rr-catalog-2"} {
		uid[m] = podUID(t, m)
	}
	patchSpec(t, c, func(c *pgshardv1alpha1.PgShardCluster) {
		c.Spec.PostgreSQL.Parameters["max_connections"] = "200"
	})

	// Pass 1: classification marks the restart, the first standby of every
	// group goes; nothing else moves.
	reconcile(t, r, c)
	if st := groupStatus(t, "rr-shard-0"); !st.SettingsRestartPending || st.Rollout == nil || st.Rollout.Phase != pgshardv1alpha1.RolloutPhaseRestarting || st.Rollout.Member != "rr-shard-0-1" {
		t.Fatalf("shard group after pass 1: %+v", st)
	}
	if st := groupStatus(t, "rr-catalog"); st.Rollout == nil || st.Rollout.Member != "rr-catalog-1" {
		t.Fatalf("groups roll in parallel; catalog after pass 1: %+v", st)
	}
	if podExists(t, "rr-shard-0-1") || !podExists(t, "rr-shard-0-2") || podUID(t, "rr-shard-0-0") != uid["rr-shard-0-0"] {
		t.Fatal("only the first standby may be deleted in pass 1")
	}
	if len(fa.reloads) != 0 {
		t.Fatalf("a postmaster change must not be reloaded: %v", fa.reloads)
	}

	// Pass 2: the pod is recreated (not Ready yet); the rollout waits.
	reconcile(t, r, c)
	if !podExists(t, "rr-shard-0-1") || podExists(t, "rr-shard-0-2") && podUID(t, "rr-shard-0-2") != uid["rr-shard-0-2"] {
		t.Fatal("pass 2 recreates -1 and leaves -2 alone")
	}
	if got := settingsStamp(t, "rr-shard-0-1"); got != Template(c, nil, nil).SettingsHash() {
		t.Fatalf("recreated pod must carry the new settings stamp, got %q", got)
	}
	if got := condition(t, "rr", pgshardv1alpha1.ConditionRolloutInProgress); got.Status != metav1.ConditionTrue {
		t.Fatalf("RolloutInProgress: %+v", got)
	}
	reconcile(t, r, c)
	if podUID(t, "rr-shard-0-2") != uid["rr-shard-0-2"] {
		t.Fatal("-2 must wait until -1 is back")
	}

	// -1 returns: -2 goes.
	memberBack(t, fp, fa, "rr-shard-0-1", podIP(1, 1))
	memberBack(t, fp, fa, "rr-catalog-1", podIP(0, 1))
	reconcile(t, r, c)
	if podExists(t, "rr-shard-0-2") {
		t.Fatal("-2 must be deleted once -1 streams again")
	}
	if podUID(t, "rr-shard-0-0") != uid["rr-shard-0-0"] {
		t.Fatal("the primary is last")
	}
	reconcile(t, r, c)
	memberBack(t, fp, fa, "rr-shard-0-2", podIP(1, 2))
	memberBack(t, fp, fa, "rr-catalog-2", podIP(0, 2))

	// Both standbys carry the new template: a switchover to the freshest
	// standby is requested for the primary.
	reconcile(t, r, c)
	get(t, "rr", c)
	target := c.Annotations[AnnotationSwitchover]
	if target != "rr-shard-0-2" && target != "rr-catalog-1" && target != "rr-catalog-2" {
		t.Fatalf("switchover must target a standby, got %q", target)
	}
	if podUID(t, "rr-shard-0-0") != uid["rr-shard-0-0"] || podUID(t, "rr-catalog-0") != uid["rr-catalog-0"] {
		t.Fatal("primaries are not deleted directly")
	}
	// The switchover runs; the old primary is stopped, the target promoted,
	// the old primary recreated with the new template as a standby.
	oldPrimaryGroup := "rr-shard-0"
	oldPrimary := "rr-shard-0-0"
	if target != "rr-shard-0-2" {
		oldPrimaryGroup, oldPrimary = "rr-catalog", "rr-catalog-0"
	}
	fa.set(podIP(0, 0), AgentStatus{}, errors.New("stopped"))
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("stopped"))
	reconcile(t, r, c)
	if st := groupStatus(t, oldPrimaryGroup); st.Primary != target || st.Epoch != 1 {
		t.Fatalf("switchover outcome: %+v", st)
	}
	if len(fa.promotes) != 1 {
		t.Fatalf("exactly one promotion for the group, got %v", fa.promotes)
	}
	reconcile(t, r, c)
	if podUID(t, oldPrimary) == uid[oldPrimary] {
		t.Fatal("old primary must come back as a new pod")
	}
	if got := settingsStamp(t, oldPrimary); got != Template(c, nil, nil).SettingsHash() {
		t.Fatalf("old primary must carry the new stamp, got %q", got)
	}
}

func TestRolloutHoldsWithDegradedWhenMemberDoesNotReturn(t *testing.T) {
	r, fp, _, c := healthyCluster(t, "hold")
	now := time.Now()
	r.Now = func() time.Time { return now }
	r.RolloutTimeout = time.Minute
	fp.settings = map[string]SettingState{"max_connections": {Context: "postmaster"}}
	patchSpec(t, c, func(c *pgshardv1alpha1.PgShardCluster) {
		c.Spec.PostgreSQL.Parameters["max_connections"] = "300"
	})
	reconcile(t, r, c)
	reconcile(t, r, c)
	if got := condition(t, "hold", pgshardv1alpha1.ConditionDegraded); got.Status != metav1.ConditionFalse {
		t.Fatalf("not degraded within the timeout: %+v", got)
	}
	now = now.Add(2 * time.Minute)
	reconcile(t, r, c)
	if got := condition(t, "hold", pgshardv1alpha1.ConditionDegraded); got.Status != metav1.ConditionTrue || got.Reason != "RolloutHeld" {
		t.Fatalf("Degraded after the timeout: %+v", got)
	}
	get(t, "hold", c)
	if c.Status.Rollout.Phase != pgshardv1alpha1.RolloutPhaseHeld {
		t.Fatalf("status.rollout %+v", c.Status.Rollout)
	}
	if !podExists(t, "hold-shard-0-2") || podUID(t, "hold-shard-0-2") == "" {
		t.Fatal("no further member may be touched while held")
	}
	if !podExists(t, "hold-shard-0-1") {
		t.Fatal("the missing member is recreated, never abandoned")
	}
	if got := condition(t, "hold", pgshardv1alpha1.ConditionRolloutInProgress); got.Status != metav1.ConditionTrue {
		t.Fatalf("still in progress: %+v", got)
	}
}

func TestRestartAnnotationRollsEveryMemberAndRecordsToken(t *testing.T) {
	r, _, _, c := healthyCluster(t, "tok")
	patchSpec(t, c, func(c *pgshardv1alpha1.PgShardCluster) {
		c.Annotations = map[string]string{AnnotationRestart: "2026-08-18"}
	})
	reconcile(t, r, c)
	if podExists(t, "tok-shard-0-1") || podExists(t, "tok-catalog-1") {
		t.Fatal("first standbys of both groups must be restarted")
	}
	get(t, "tok", c)
	if c.Status.Rollout.LastRestartToken == "2026-08-18" {
		t.Fatal("token is recorded only once the restart completed")
	}
	if c.Status.Rollout.Phase != pgshardv1alpha1.RolloutPhaseRestarting {
		t.Fatalf("status.rollout %+v", c.Status.Rollout)
	}
}

func TestStorageClassChangeRebuildsMembersOntoNewClaims(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "sc")
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 900}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 900}
	patchSpec(t, c, func(c *pgshardv1alpha1.PgShardCluster) {
		c.Spec.Storage.StorageClassName = ptr.To("fast")
	})
	reconcile(t, r, c)
	st := groupStatus(t, "sc-shard-0")
	if st.Rollout == nil || st.Rollout.Phase != pgshardv1alpha1.RolloutPhaseRebuilding || st.Rollout.Member != "sc-shard-0-1" {
		t.Fatalf("shard group after pass 1: %+v", st)
	}
	var next corev1.PersistentVolumeClaim
	get(t, "sc-shard-0-1-v2", &next)
	if next.Spec.StorageClassName == nil || *next.Spec.StorageClassName != "fast" || next.Labels[LabelMember] != "sc-shard-0-1" {
		t.Fatalf("successor claim: %+v", next.Spec)
	}
	ownedBy(t, &next, c)
	if podExists(t, "sc-shard-0-1") {
		t.Fatal("member pod must be deleted so it comes back on the new claim")
	}
	if cat := groupStatus(t, "sc-catalog"); cat.Rollout != nil {
		t.Fatalf("catalog storage is unchanged: %+v", cat.Rollout)
	}
	retiredClaimAlive(t, "sc-shard-0-1")

	reconcile(t, r, c)
	var pod corev1.Pod
	get(t, "sc-shard-0-1", &pod)
	if got := pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "sc-shard-0-1-v2" {
		t.Fatalf("recreated pod must mount the successor, got %q", got)
	}
	found := false
	for _, m := range groupStatus(t, "sc-shard-0").Members {
		if m.Name == "sc-shard-0-1" && m.PVC == "sc-shard-0-1-v2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("group status must track the claim: %+v", groupStatus(t, "sc-shard-0").Members)
	}
	retiredClaimAlive(t, "sc-shard-0-1")

	memberBack(t, fp, fa, "sc-shard-0-1", podIP(1, 1))
	reconcile(t, r, c)
	var old corev1.PersistentVolumeClaim
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "sc-shard-0-1"}, &old)
	if err == nil && old.DeletionTimestamp == nil {
		t.Fatal("old claim must be deleted once the member streams from the new one")
	} else if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	if podExists(t, "sc-shard-0-2") {
		t.Fatal("next member is rebuilt only after the previous one settled")
	}
	get(t, "sc-shard-0-2-v2", &corev1.PersistentVolumeClaim{})
}

func TestStorageGrowthExpandsInPlaceWhenTheClassAllowsIt(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "grow"}, Provisioner: "example.invalid/none", AllowVolumeExpansion: ptr.To(true)}
	if err := k8sClient.Create(ctx, sc); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), sc) })
	r, _, _, c := setupWithAgents(t, "grow")
	patchSpec(t, c, func(c *pgshardv1alpha1.PgShardCluster) {
		c.Spec.Storage.StorageClassName = ptr.To("grow")
	})
	reconcile(t, r, c)
	for _, m := range Groups(c)[1].MemberNames() {
		var pvc corev1.PersistentVolumeClaim
		get(t, m, &pvc)
		pvc.Status.Phase = corev1.ClaimBound
		if err := k8sClient.Status().Update(ctx, &pvc); err != nil {
			t.Fatal(err)
		}
	}
	r2, fp, fa, _ := setupHealthy(t, r, c)
	_ = fp
	_ = fa
	patchSpec(t, c, func(c *pgshardv1alpha1.PgShardCluster) {
		c.Spec.Storage.Size = resource.MustParse("3Gi")
	})
	uid := podUID(t, "grow-shard-0-1")
	reconcile(t, r2, c)
	var pvc corev1.PersistentVolumeClaim
	get(t, "grow-shard-0-1", &pvc)
	if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(resource.MustParse("3Gi")) != 0 {
		t.Fatalf("claim must be expanded in place, got %s", got.String())
	}
	if podUID(t, "grow-shard-0-1") != uid {
		t.Fatal("expansion must not restart the member")
	}
	if st := groupStatus(t, "grow-shard-0"); st.Rollout != nil {
		t.Fatalf("expansion is not a rolling step: %+v", st.Rollout)
	}
}

// setupHealthy drives an already created cluster to Ready the way
// healthyCluster does for a fresh one.
func setupHealthy(t *testing.T, r *ClusterReconciler, c *pgshardv1alpha1.PgShardCluster) (*ClusterReconciler, *fakeProber, *fakeAgents, *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	fp := r.Prober.(*fakeProber)
	fa := r.Agents.(*fakeAgents)
	markPodsRunning(t, c)
	fp.err = nil
	fp.streaming = map[string]bool{}
	for gi, g := range Groups(c) {
		for i := 0; i < g.Replicas; i++ {
			fa.set(podIP(gi, i), AgentStatus{Running: true, Primary: i == 0}, nil)
			if i > 0 {
				fp.streaming[g.MemberName(i)] = true
			}
		}
	}
	reconcile(t, r, c)
	if cond := condition(t, c.Name, pgshardv1alpha1.ConditionReady); cond.Status != metav1.ConditionTrue {
		t.Fatalf("cluster must be Ready before the scenario: %+v", cond)
	}
	return r, fp, fa, c
}

// retiredClaimAlive asserts the old claim is neither gone nor marked for
// deletion: envtest keeps deleted PVCs around with a DeletionTimestamp, so
// a plain Get would not notice an early delete.
func retiredClaimAlive(t *testing.T, name string) {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	get(t, name, &pvc)
	if pvc.DeletionTimestamp != nil {
		t.Fatalf("retired claim %s deleted before the successor settled", name)
	}
}
