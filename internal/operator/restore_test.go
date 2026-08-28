package operator

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent/backup"
)

func restoreClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&pgshardv1alpha1.PgShardRestore{}, &pgshardv1alpha1.PgShardCluster{}, &pgshardv1alpha1.PgShardGroup{}).Build()
}

func completedBackup(name, cluster string) *pgshardv1alpha1.PgShardBackup {
	return &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: cluster, Type: "full"},
		Status: pgshardv1alpha1.PgShardBackupStatus{Phase: pgshardv1alpha1.BackupPhaseCompleted, BackupID: "20260819-100000F", Groups: []pgshardv1alpha1.GroupBackupStatus{
			{Group: "catalog", Stanza: cluster + "-catalog-pg18", BackupID: "20260819-100000F"},
			{Group: "shard-0", Stanza: cluster + "-shard-0-pg18", BackupID: "20260819-100003F"},
		}}}
}

func superuserSecret(cluster string) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretName(cluster), Namespace: "default"}, Type: corev1.SecretTypeBasicAuth,
		Data: map[string][]byte{"username": []byte(superuserName), secretKey: []byte("source-pw")}}
}

func newRestore(name string, spec pgshardv1alpha1.PgShardRestoreSpec) *pgshardv1alpha1.PgShardRestore {
	return &pgshardv1alpha1.PgShardRestore{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Spec: spec}
}

func reconcileRestore(t *testing.T, r *RestoreReconciler, name string) (ctrl.Result, *pgshardv1alpha1.PgShardRestore) {
	t.Helper()
	key := client.ObjectKey{Namespace: "default", Name: name}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatal(err)
	}
	var got pgshardv1alpha1.PgShardRestore
	if err := r.Get(context.Background(), key, &got); err != nil {
		t.Fatal(err)
	}
	return res, &got
}

func TestRestoreTargetOptions(t *testing.T) {
	name, xid, lsn := "rp", "42", "0/3000000"
	imm := true
	tli := int64(3)
	at := metav1.NewTime(time.Date(2026, 8, 19, 10, 15, 0, 0, time.FixedZone("x", 3600)))
	cases := []struct {
		spec pgshardv1alpha1.PgShardRestoreSpec
		want string
		err  bool
	}{
		{pgshardv1alpha1.PgShardRestoreSpec{}, "type=default", false},
		{pgshardv1alpha1.PgShardRestoreSpec{BackupID: "b"}, "type=default set=b", false},
		{pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{Time: &at}, TargetTLI: &tli, Exclusive: true}, "type=time target=2026-08-19 09:15:00+00 timeline=3 exclusive", false},
		{pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{LSN: &lsn}}, "type=lsn target=0/3000000", false},
		{pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{Name: &name}, BackupID: "b"}, "type=name set=b target=rp", false},
		{pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{XID: &xid}, BackupID: "b"}, "type=xid set=b target=42", false},
		{pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{Immediate: &imm}, BackupID: "b"}, "type=immediate set=b", false},
		{pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{Name: &name}}, "", true},
		{pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{XID: &xid}}, "", true},
		{pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{Immediate: &imm}}, "", true},
		{pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{Name: &name, LSN: &lsn}, BackupID: "b"}, "", true},
		{pgshardv1alpha1.PgShardRestoreSpec{Exclusive: true}, "", true},
	}
	for i, c := range cases {
		o, err := restoreTargetOptions(&c.spec)
		if (err != nil) != c.err {
			t.Errorf("case %d: err=%v", i, err)
			continue
		}
		if !c.err && o.String() != c.want {
			t.Errorf("case %d: got %q want %q", i, o.String(), c.want)
		}
	}
}

func TestRestoreSourceRoundTripAndAgentConfig(t *testing.T) {
	c := boundCluster("new")
	one := 1
	c.Spec.Shards = &one
	src := RestoreSource{SourceCluster: "old", Major: 18, Restore: "r1", BackupIDs: map[string]string{"catalog": "A", "shard-0": "B"}, Type: backup.TargetName, Target: "rp", TargetTLI: 2, Exclusive: true}
	c.Annotations = map[string]string{AnnotationRestoreSource: src.Encode()}
	got, ok := RestoreSourceOf(c)
	if !ok || got.Stanza(Groups(c)[1]) != "old-shard-0-pg18" || got.Options(Groups(c)[1]).BackupID != "B" || got.Options(Groups(c)[0]).String() != "type=name stanza=old-catalog-pg18 set=A target=rp timeline=2 exclusive" {
		t.Fatalf("decoded %+v ok=%v", got, ok)
	}
	if _, ok := RestoreSourceOf(boundCluster("plain")); ok {
		t.Fatal("cluster without annotation reported a source")
	}
	g := Groups(c)[1]
	cfg := agentConfig(c, g, g.MemberName(0), g.MemberName(0), Template(c, Group{}, nil, newPolicy()), false, true)
	if cfg.Restore == nil || cfg.Restore.Stanza != "old-shard-0-pg18" || cfg.Restore.BackupID != "B" || cfg.Restore.Type != backup.TargetName || !cfg.RecloneFromRepo {
		t.Fatalf("agent config restore = %+v reclone=%v", cfg.Restore, cfg.RecloneFromRepo)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if plain := agentConfig(c, g, g.MemberName(0), g.MemberName(0), Template(c, Group{}, nil, nil), false, true); plain.Restore != nil || plain.RecloneFromRepo {
		t.Fatal("restore and reclone settings need a policy")
	}
	if noRepo := agentConfig(c, g, g.MemberName(0), g.MemberName(0), Template(c, Group{}, nil, newPolicy()), false, false); noRepo.RecloneFromRepo {
		t.Fatal("recloneFromRepo set without a completed backup")
	}
	cm := Renderer{}.ConfigMap(c, g, g.MemberName(0), nil, newPolicy(), true)
	if !strings.Contains(cm.Data[agentConfigKey(g.MemberName(1))], `"recloneFromRepo": true`) || !strings.Contains(cm.Data[agentConfigKey(g.MemberName(1))], `"stanza": "old-shard-0-pg18"`) {
		t.Fatalf("configmap lacks restore settings:\n%s", cm.Data[agentConfigKey(g.MemberName(1))])
	}
}

func TestRestoreReconcilerCreatesClusterAndFollowsRecovery(t *testing.T) {
	source := boundCluster("old")
	one := 1
	source.Spec.Shards = &one
	name := "before-purge"
	rs := newRestore("r1", pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "new", BackupID: "b1", Target: pgshardv1alpha1.RestoreTarget{Name: &name}})
	cl := restoreClient(t, source, newPolicy(), completedBackup("b1", "old"), rs, superuserSecret("old"))
	agents := newFakeAgents(nil)
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	r := &RestoreReconciler{Client: cl, Agents: agents, Now: func() time.Time { return now }}

	res, got := reconcileRestore(t, r, "r1")
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring || res.RequeueAfter != restorePollInterval || got.Status.StartedAt == nil {
		t.Fatalf("after create: %+v res=%v", got.Status, res)
	}
	if len(got.Status.Groups) != 2 || got.Status.Groups[0].SourceStanza != "old-catalog-pg18" || got.Status.Groups[0].BackupID != "20260819-100000F" || got.Status.Groups[1].BackupID != "20260819-100003F" || got.Status.Groups[1].ReachedTarget {
		t.Fatalf("groups: %+v", got.Status.Groups)
	}
	var created pgshardv1alpha1.PgShardCluster
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "new"}, &created); err != nil {
		t.Fatal(err)
	}
	src, ok := RestoreSourceOf(&created)
	if !ok || src.SourceCluster != "old" || src.Restore != "r1" || src.Type != backup.TargetName || src.Target != name || src.BackupIDs["shard-0"] != "20260819-100003F" {
		t.Fatalf("restore source: %+v ok=%v", src, ok)
	}
	if created.Labels[LabelRestoredFrom] != "r1" || created.Spec.Backup.PolicyRef != "nightly" || created.Spec.PostgreSQL.Major != 18 || created.Spec.Catalog.Replicas != 3 {
		t.Fatalf("created spec: %+v labels=%v", created.Spec, created.Labels)
	}
	var sec corev1.Secret
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: SecretName("new")}, &sec); err != nil || string(sec.Data[secretKey]) != "source-pw" {
		t.Fatalf("superuser secret not copied: %v %q", err, sec.Data)
	}

	_, got = reconcileRestore(t, r, "r1")
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring || got.Status.Groups[0].Message != "group not created yet" {
		t.Fatalf("no groups yet: %+v", got.Status)
	}
	for _, g := range Groups(&created) {
		pg := &pgshardv1alpha1.PgShardGroup{ObjectMeta: metav1.ObjectMeta{Name: g.Prefix(), Namespace: "default"}}
		if err := cl.Create(context.Background(), pg); err != nil {
			t.Fatal(err)
		}
		pg.Status.Primary = g.MemberName(0)
		if err := cl.Status().Update(context.Background(), pg); err != nil {
			t.Fatal(err)
		}
	}
	_, got = reconcileRestore(t, r, "r1")
	if got.Status.Groups[0].Message != "primary new-catalog-0 has no pod yet" {
		t.Fatalf("no pods yet: %+v", got.Status.Groups)
	}
	if err := cl.Create(context.Background(), readyPod("new-catalog-0", "10.1.0.1")); err != nil {
		t.Fatal(err)
	}
	pod := readyPod("new-shard-0-0", "10.1.0.2")
	pod.Status.Conditions = nil
	if err := cl.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	agents.set("10.1.0.1", AgentStatus{Running: true, Primary: false, Timeline: 1}, nil)
	_, got = reconcileRestore(t, r, "r1")
	if got.Status.Groups[0].ReachedTarget || !strings.Contains(got.Status.Groups[0].Message, "still in recovery") || !strings.Contains(got.Status.Groups[1].Message, "not ready yet") {
		t.Fatalf("recovering: %+v", got.Status.Groups)
	}
	agents.set("10.1.0.1", AgentStatus{Running: true, Primary: true, Timeline: 2}, nil)
	agents.set("10.1.0.2", AgentStatus{Running: true, Primary: true, Timeline: 5}, nil)
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := cl.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	_, got = reconcileRestore(t, r, "r1")
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring || !got.Status.Groups[0].ReachedTarget || got.Status.Groups[0].Timeline != 2 || !got.Status.Groups[1].ReachedTarget || got.Status.Groups[1].Timeline != 5 {
		t.Fatalf("all recovered but cluster not ready: %+v", got.Status)
	}
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "new"}, &created); err != nil {
		t.Fatal(err)
	}
	meta.SetStatusCondition(&created.Status.Conditions, metav1.Condition{Type: pgshardv1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: "Ready"})
	if err := cl.Status().Update(context.Background(), &created); err != nil {
		t.Fatal(err)
	}
	if cfg := agentConfig(&created, Groups(&created)[1], "new-shard-0-0", "new-shard-0-0", Template(&created, Groups(&created)[1], nil, newPolicy()), false, true); cfg.Restore == nil {
		t.Fatal("restoring cluster renders no restore config")
	}
	res, got = reconcileRestore(t, r, "r1")
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseRecovered || got.Status.CompletedAt == nil || res.RequeueAfter != 0 || got.Status.Error != "" {
		t.Fatalf("recovered: %+v res=%v", got.Status, res)
	}
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "new"}, &created); err != nil {
		t.Fatal(err)
	}
	if _, ok := created.Annotations[AnnotationRestoreSource]; ok || created.Labels[LabelRestoredFrom] != "r1" {
		t.Fatalf("recovered cluster keeps restore source: annotations=%v labels=%v", created.Annotations, created.Labels)
	}
	if cfg := agentConfig(&created, Groups(&created)[1], "new-shard-0-0", "new-shard-0-0", Template(&created, Groups(&created)[1], nil, newPolicy()), false, true); cfg.Restore != nil || !cfg.RecloneFromRepo {
		t.Fatalf("recovered cluster still renders restore config: %+v", cfg.Restore)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, "Progressing"); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "Recovered" {
		t.Fatalf("condition: %+v", cond)
	}
	if reqs := clusterToRestore(context.Background(), &created); len(reqs) != 1 || reqs[0].Name != "r1" {
		t.Fatalf("clusterToRestore = %v", reqs)
	}
	if reqs := clusterToRestore(context.Background(), source); len(reqs) != 0 {
		t.Fatalf("source cluster maps to %v", reqs)
	}
}

func TestRestoreReconcilerFailures(t *testing.T) {
	source := boundCluster("old")
	one := 1
	source.Spec.Shards = &one
	name := "rp"
	running := completedBackup("b-running", "old")
	running.Status.Phase = pgshardv1alpha1.BackupPhaseRunning
	other := completedBackup("b-other", "other")
	foreign := boundCluster("taken")
	crashPod := readyPod("crash-catalog-0", "10.2.0.1")
	crashPod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "postgres", RestartCount: 3, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}
	crashCluster := boundCluster("crash")
	crashCluster.Spec.Shards = &one
	crashCluster.Labels = map[string]string{LabelRestoredFrom: "crash"}
	crashCluster.Annotations = map[string]string{AnnotationRestoreSource: RestoreSource{SourceCluster: "old", Major: 18, Restore: "crash"}.Encode()}
	two := 2
	moreShards := source.Spec.DeepCopy()
	moreShards.Shards = &two
	unbound := source.Spec.DeepCopy()
	unbound.Backup.PolicyRef = ""
	unboundSource := newCluster("unbound")
	unboundSource.Spec.Shards = &one

	cases := map[string]struct {
		spec pgshardv1alpha1.PgShardRestoreSpec
		want string
	}{
		"same name":         {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "old"}, "must be set and differ"},
		"name without set":  {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "n1", Target: pgshardv1alpha1.RestoreTarget{Name: &name}}, "requires a backup id"},
		"missing source":    {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "gone", NewClusterName: "n2"}, `source cluster "gone" not found`},
		"backup running":    {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "n3", BackupID: "b-running", Target: pgshardv1alpha1.RestoreTarget{Name: &name}}, "is Running, not Completed"},
		"foreign backup":    {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "n4", BackupID: "b-other"}, "belongs to cluster other"},
		"cluster taken":     {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "taken"}, "already exists and was not created by this restore"},
		"crash":             {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "crash"}, "crash looping"},
		"shape change":      {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "n5", ClusterSpec: moreShards}, "must keep the source's 2 groups"},
		"no policy":         {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "unbound", NewClusterName: "n6"}, "needs spec.backup.policyRef"},
		"unbound override":  {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "n7", ClusterSpec: unbound}, ""},
		"raw label default": {pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "n8", BackupID: "20260819-100000F"}, ""},
	}
	for tname, c := range cases {
		t.Run(tname, func(t *testing.T) {
			rs := newRestore(tname, c.spec)
			cl := restoreClient(t, source, unboundSource, newPolicy(), running, other, foreign, crashCluster, crashPod, rs, superuserSecret("old"), superuserSecret("unbound"),
				&pgshardv1alpha1.PgShardGroup{ObjectMeta: metav1.ObjectMeta{Name: "crash-catalog", Namespace: "default"}, Status: pgshardv1alpha1.PgShardGroupStatus{Primary: "crash-catalog-0"}})
			r := &RestoreReconciler{Client: cl, Agents: newFakeAgents(nil)}
			_, got := reconcileRestore(t, r, tname)
			if c.want == "" {
				if got.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring {
					t.Fatalf("phase=%s err=%s", got.Status.Phase, got.Status.Error)
				}
				var created pgshardv1alpha1.PgShardCluster
				if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: c.spec.NewClusterName}, &created); err != nil {
					t.Fatal(err)
				}
				if created.Spec.Backup.PolicyRef != "nightly" {
					t.Fatalf("policyRef=%q", created.Spec.Backup.PolicyRef)
				}
				if src, _ := RestoreSourceOf(&created); c.spec.BackupID != "" && (src.BackupIDs["catalog"] != c.spec.BackupID || src.BackupIDs["shard-0"] != c.spec.BackupID) {
					t.Fatalf("raw label not applied to every group: %+v", src.BackupIDs)
				}
				return
			}
			if got.Status.Phase != pgshardv1alpha1.RestorePhaseFailed || !strings.Contains(got.Status.Error, c.want) {
				t.Fatalf("phase=%s error=%q want %q", got.Status.Phase, got.Status.Error, c.want)
			}
			if got.Status.CompletedAt == nil {
				t.Fatal("completedAt unset")
			}
			// Terminal phases are left alone.
			res, again := reconcileRestore(t, r, tname)
			if res.RequeueAfter != 0 || again.Status.Error != got.Status.Error {
				t.Fatalf("terminal restore reconciled again: %+v", again.Status)
			}
		})
	}
}

func TestRestoreReconcilerTimesOut(t *testing.T) {
	source := boundCluster("old")
	one := 1
	source.Spec.Shards = &one
	rs := newRestore("slow", pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "slow-new"})
	cl := restoreClient(t, source, newPolicy(), rs, superuserSecret("old"))
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	r := &RestoreReconciler{Client: cl, Agents: newFakeAgents(nil), Now: func() time.Time { return now }}
	if _, got := reconcileRestore(t, r, "slow"); got.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring {
		t.Fatalf("phase=%s", got.Status.Phase)
	}
	now = now.Add(restoreTimeout - time.Minute)
	if _, got := reconcileRestore(t, r, "slow"); got.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring {
		t.Fatalf("phase=%s before the deadline", got.Status.Phase)
	}
	now = now.Add(2 * time.Minute)
	if _, got := reconcileRestore(t, r, "slow"); got.Status.Phase != pgshardv1alpha1.RestorePhaseFailed || !strings.Contains(got.Status.Error, "did not recover within") {
		t.Fatalf("phase=%s err=%s", got.Status.Phase, got.Status.Error)
	}
}

func TestCrashLoopReason(t *testing.T) {
	pod := readyPod("p", "10.0.0.9")
	waiting := corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "postgres", RestartCount: 1, State: waiting}}
	if crashLoopReason(pod) != "" {
		t.Fatal("a single restart is not a crash loop")
	}
	pod.Status.ContainerStatuses[0].RestartCount = 2
	if !strings.Contains(crashLoopReason(pod), "restarted 2 times") {
		t.Fatal(crashLoopReason(pod))
	}
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	if crashLoopReason(pod) != "" {
		t.Fatal("a running container is not a crash loop")
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "pooler", RestartCount: 9, State: waiting}}
	if crashLoopReason(pod) != "" {
		t.Fatal("only the postgres container counts")
	}
}

// fakeCertifier answers the barrier certification question without a catalog.
type fakeCertifier struct {
	certified bool
	err       error
	asked     string
	password  string
}

func (f *fakeCertifier) CertifiedBarrier(_ context.Context, _, password, name string) (bool, error) {
	f.asked, f.password = name, password
	return f.certified, f.err
}

// TestRestoreRefusesAnUncertifiedBarrier: a barrier attempt that created the
// physical restore point on every group and then failed certification leaves
// a name that restores cleanly on every group with no error, landing the
// cluster on a point explicitly recorded as not two-phase-consistent. The
// restore checked only that the CRD field was set, while isBarrierRestore's
// own comment claimed it "targets a certified barrier".
func TestRestoreRefusesAnUncertifiedBarrier(t *testing.T) {
	source := boundCluster("old")
	one := 1
	source.Spec.Shards = &one
	barrier := "nightly-2026"
	rs := newRestore("r1", pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "new",
		BackupID: "b1", Target: pgshardv1alpha1.RestoreTarget{Barrier: &barrier}})
	cl := restoreClient(t, source, newPolicy(), completedBackup("b1", "old"), rs, superuserSecret("old"))
	cert := &fakeCertifier{certified: false}
	r := &RestoreReconciler{Client: cl, Agents: newFakeAgents(nil), Barriers: cert,
		Now: func() time.Time { return time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC) }}

	_, got := reconcileRestore(t, r, "r1")
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseFailed {
		t.Fatalf("phase %s: an uncertified barrier was accepted", got.Status.Phase)
	}
	if !strings.Contains(got.Status.Error, "not certified") {
		t.Fatalf("message %q does not say the barrier was uncertified", got.Status.Error)
	}
	// The operator holds no PGPASSWORD for an arbitrary cluster, so the
	// check has to authenticate from that cluster's own secret. Without
	// this it reached the catalog and was refused, and every barrier
	// restore failed as "cannot confirm".
	if cert.password != "source-pw" {
		t.Fatalf("certifier password = %q, want the source cluster's superuser secret", cert.password)
	}
	// The catalog row is keyed by the barrier name. Asking for the WAL
	// restore point's name instead finds nothing, and "no row" reads as
	// uncertified -- so the gate refuses every barrier, including the good
	// ones it exists to admit.
	if cert.asked != barrier {
		t.Fatalf("asked about %q, want the barrier name %q as pgshard.restore_points keys it", cert.asked, barrier)
	}
	// The cluster must not exist: the point of the check is to refuse before
	// any group is created.
	var created pgshardv1alpha1.PgShardCluster
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "new"}, &created); err == nil {
		t.Fatal("the restore created a cluster despite refusing the barrier")
	}
}

func TestRestoreAcceptsACertifiedBarrier(t *testing.T) {
	source := boundCluster("old")
	one := 1
	source.Spec.Shards = &one
	barrier := "nightly-2026"
	rs := newRestore("r1", pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "new",
		BackupID: "b1", Target: pgshardv1alpha1.RestoreTarget{Barrier: &barrier}})
	cl := restoreClient(t, source, newPolicy(), completedBackup("b1", "old"), rs, superuserSecret("old"))
	r := &RestoreReconciler{Client: cl, Agents: newFakeAgents(nil), Barriers: &fakeCertifier{certified: true},
		Now: func() time.Time { return time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC) }}

	_, got := reconcileRestore(t, r, "r1")
	if got.Status.Phase == pgshardv1alpha1.RestorePhaseFailed {
		t.Fatalf("a certified barrier was refused: %s", got.Status.Error)
	}
}

// TestRestoreKeepsTheSourcesGroupNames: a source that has resharded serves
// generation N, and Group.Name() carries it -- shard-0-g3, catalog-g2. The
// new cluster is created with no status, so its own groups are generation
// one. Deriving the stanza and the backup label from the new names looked
// for a repository that does not exist and a label recorded under another
// name, so no cluster that had ever resharded could be restored.
func TestRestoreKeepsTheSourcesGroupNames(t *testing.T) {
	source := boundCluster("old")
	one := 1
	source.Spec.Shards = &one
	source.Status.EffectiveShards = 1
	source.Status.ServingGeneration = 3
	source.Status.ServingPGMajor = 18
	source.Status.CatalogGeneration = 2

	b := completedBackup("b1", "old")
	b.Status.Groups = []pgshardv1alpha1.GroupBackupStatus{
		{Group: "catalog-g2", Stanza: "old-catalog-g2-pg18", BackupID: "20260819-100000F"},
		{Group: "shard-0-g3", Stanza: "old-shard-0-g3-pg18", BackupID: "20260819-100003F"},
	}
	name := "before-purge"
	rs := newRestore("r1", pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "new", BackupID: "b1",
		Target: pgshardv1alpha1.RestoreTarget{Name: &name}})
	cl := restoreClient(t, source, newPolicy(), b, rs, superuserSecret("old"))
	r := &RestoreReconciler{Client: cl, Agents: newFakeAgents(nil),
		Now: func() time.Time { return time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC) }}

	_, got := reconcileRestore(t, r, "r1")
	if got.Status.Phase == pgshardv1alpha1.RestorePhaseFailed {
		t.Fatalf("restore failed: %s", got.Status.Error)
	}
	want := map[string]struct{ stanza, id string }{
		"catalog": {"old-catalog-g2-pg18", "20260819-100000F"},
		"shard-0": {"old-shard-0-g3-pg18", "20260819-100003F"},
	}
	for _, g := range got.Status.Groups {
		w, ok := want[g.Group]
		if !ok {
			t.Fatalf("unexpected group %q", g.Group)
		}
		if g.SourceStanza != w.stanza {
			t.Errorf("%s stanza = %q, want the source's own %q", g.Group, g.SourceStanza, w.stanza)
		}
		if g.BackupID != w.id {
			t.Errorf("%s backup id = %q, want %q: labels are recorded under the source group names", g.Group, g.BackupID, w.id)
		}
	}

	// And the agent restores from the same place, since that is what
	// actually reaches pgBackRest.
	var created pgshardv1alpha1.PgShardCluster
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "new"}, &created); err != nil {
		t.Fatal(err)
	}
	src, ok := RestoreSourceOf(&created)
	if !ok {
		t.Fatal("the new cluster carries no restore source")
	}
	for _, g := range Groups(&created) {
		if got, w := src.Options(g).Stanza, want[g.Name()].stanza; got != w {
			t.Errorf("%s restore stanza = %q, want %q", g.Name(), got, w)
		}
	}
}

// TestRestoreWillNotAdoptAForeignSuperuserSecret: the copy ignored
// AlreadyExists, so a secret left by a deleted cluster -- or belonging to a
// live one -- was adopted by name alone. The restored catalog holds the
// source's password, so a restored cluster given someone else's credential
// locks its own agents and routers out of itself, and nothing says so.
func TestRestoreWillNotAdoptAForeignSuperuserSecret(t *testing.T) {
	newSource := func() *pgshardv1alpha1.PgShardCluster {
		c := boundCluster("old")
		one := 1
		c.Spec.Shards = &one
		c.UID = "source-uid"
		return c
	}
	restore := func() *pgshardv1alpha1.PgShardRestore {
		rs := newRestore("r1", pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "new", BackupID: "b1"})
		rs.UID = "restore-uid"
		return rs
	}
	stranger := func(pw string, ann map[string]string) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: SecretName("new"), Namespace: "default", Annotations: ann},
			Type:       corev1.SecretTypeBasicAuth,
			Data:       map[string][]byte{"username": []byte(superuserName), secretKey: []byte(pw)},
		}
	}
	for _, c := range []struct {
		name   string
		secret *corev1.Secret
		want   string
	}{
		{"unstamped leftover", stranger("someone-elses-pw", nil), "was not created by this restore"},
		{"another restore", stranger("source-pw", map[string]string{AnnotationRestoreUID: "other-restore"}), "was not created by this restore"},
		{"another source", stranger("source-pw", map[string]string{AnnotationRestoreUID: "restore-uid", AnnotationRestoreSourceUID: "other-source"}), "another source cluster"},
		{"same restore, wrong password", stranger("drifted", map[string]string{AnnotationRestoreUID: "restore-uid", AnnotationRestoreSourceUID: "source-uid"}), "different password"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cl := restoreClient(t, newSource(), newPolicy(), completedBackup("b1", "old"), restore(), superuserSecret("old"), c.secret)
			r := &RestoreReconciler{Client: cl, Agents: newFakeAgents(nil), Now: time.Now}
			_, got := reconcileRestore(t, r, "r1")
			if got.Status.Phase != pgshardv1alpha1.RestorePhaseFailed || !strings.Contains(got.Status.Error, c.want) {
				t.Fatalf("phase %s message %q, want a failure mentioning %q", got.Status.Phase, got.Status.Error, c.want)
			}
			var cluster pgshardv1alpha1.PgShardCluster
			if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "new"}, &cluster); err == nil {
				t.Fatal("the cluster must not be created against a credential that is not the source's")
			}
			var after corev1.Secret
			if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: SecretName("new")}, &after); err != nil {
				t.Fatalf("the foreign secret must be left alone: %v", err)
			}
			if string(after.Data[secretKey]) != string(c.secret.Data[secretKey]) {
				t.Fatalf("the foreign secret was rewritten: %q", after.Data[secretKey])
			}
		})
	}

	t.Run("this restore's own earlier copy is reused", func(t *testing.T) {
		mine := stranger("source-pw", map[string]string{AnnotationRestoreUID: "restore-uid", AnnotationRestoreSourceUID: "source-uid"})
		cl := restoreClient(t, newSource(), newPolicy(), completedBackup("b1", "old"), restore(), superuserSecret("old"), mine)
		r := &RestoreReconciler{Client: cl, Agents: newFakeAgents(nil), Now: time.Now}
		_, got := reconcileRestore(t, r, "r1")
		if got.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring {
			t.Fatalf("a retry must reuse its own copy: %s %q", got.Status.Phase, got.Status.Error)
		}
	})
}

// TestRestoredClusterOwnsItsCopiedSecret: the copy is made before the
// cluster exists, so it cannot be owned at that moment and nothing owned it
// afterwards either. Deleting the restored cluster then left its credential
// behind, for the next cluster of that name to inherit.
func TestRestoredClusterOwnsItsCopiedSecret(t *testing.T) {
	c := boundCluster("new")
	c.UID = "cluster-uid"
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretName("new"), Namespace: "default"},
		Type: corev1.SecretTypeBasicAuth, Data: map[string][]byte{"username": []byte(superuserName), secretKey: []byte("source-pw")}}
	cl := restoreClient(t, c, sec)
	r := &ClusterReconciler{Client: cl}
	pw, err := r.ensureSecret(context.Background(), c)
	if err != nil || pw != "source-pw" {
		t.Fatalf("ensureSecret: %q %v", pw, err)
	}
	var got corev1.Secret
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: SecretName("new")}, &got); err != nil {
		t.Fatal(err)
	}
	owner := metav1.GetControllerOf(&got)
	if owner == nil || owner.Name != "new" || owner.Kind != "PgShardCluster" {
		t.Fatalf("the restored cluster must own its credential: %+v", owner)
	}

	// A secret another controller owns is not taken: adopting one is how a
	// deletion elsewhere removes a live cluster's credential.
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretName("other"), Namespace: "default"},
		Type: corev1.SecretTypeBasicAuth, Data: map[string][]byte{secretKey: []byte("pw")}}
	elsewhere := boundCluster("elsewhere")
	elsewhere.UID = "elsewhere-uid"
	if err := controllerutil.SetControllerReference(elsewhere, foreign, cl.Scheme()); err != nil {
		t.Fatal(err)
	}
	other := boundCluster("other")
	other.UID = "other-uid"
	cl2 := restoreClient(t, other, foreign)
	r2 := &ClusterReconciler{Client: cl2}
	if _, err := r2.ensureSecret(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if err := cl2.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: SecretName("other")}, &got); err != nil {
		t.Fatal(err)
	}
	if owner := metav1.GetControllerOf(&got); owner == nil || owner.Name != "elsewhere" {
		t.Fatalf("an owned secret must keep its owner: %+v", owner)
	}
}
