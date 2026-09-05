package operator

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent"
	"github.com/andrew01234567890/pgshard/internal/agent/backup"
)

func newPolicy() *pgshardv1alpha1.PgShardBackupPolicy {
	no := false
	return &pgshardv1alpha1.PgShardBackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default", UID: "pol-uid"},
		Spec: pgshardv1alpha1.PgShardBackupPolicySpec{
			ObjectStore: pgshardv1alpha1.ObjectStoreSpec{
				Type: "s3", Bucket: "pgshard", Endpoint: "http://minio.objectstores.svc:9000", Region: "us-east-1", Prefix: "demo", URIStyle: "path", VerifyTLS: &no,
				Credentials: pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "minio-creds"}},
				Encryption:  pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "repo-key"}},
			},
			Retention: pgshardv1alpha1.BackupRetention{Full: 3, Differential: 2},
			Schedules: pgshardv1alpha1.BackupSchedules{Full: "0 2 * * *", Incremental: "*/15 * * * *"},
			LogLevel:  "detail",
		},
	}
}

// boundCluster is a cluster referencing the nightly policy.
func boundCluster(name string) *pgshardv1alpha1.PgShardCluster {
	c := newCluster(name)
	c.Spec.Backup.PolicyRef = "nightly"
	return c
}

func TestBackupSettingsPerStore(t *testing.T) {
	c := newCluster("demo")
	g := Groups(c)[1]
	pol := newPolicy()
	s := BackupSettings(c, g, &pol.Spec)
	if s.Stanza != "demo-shard-0-pg18" || s.Repo.Type != "s3" || s.Repo.Bucket != "pgshard" || s.Repo.Path != "/demo" || s.Repo.URIStyle != "path" ||
		s.Repo.CredentialsDir != backupCredentialsMountPath || s.CipherPassFile != backupEncryptionMountPath+"/passphrase" || s.RetentionFull != 3 || s.RetentionDiff != 2 || s.LogLevel != "detail" || s.Repo.VerifyTLS == nil || *s.Repo.VerifyTLS {
		t.Errorf("s3 settings: %+v", s)
	}
	if err := s.WithDefaults().Validate(); err != nil {
		t.Error(err)
	}
	az := pol.Spec.DeepCopy()
	az.ObjectStore = pgshardv1alpha1.ObjectStoreSpec{Type: "azure", Container: "blobs", Endpoint: "http://azurite:10000", CredentialType: "sas",
		Credentials: pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "az"}}}
	if s := BackupSettings(c, g, az); s.Repo.Bucket != "blobs" || s.Repo.KeyType != "sas" || s.CipherPassFile != "" || s.Repo.Path != "" {
		t.Errorf("azure settings: %+v", s)
	}
	gcs := pol.Spec.DeepCopy()
	gcs.ObjectStore = pgshardv1alpha1.ObjectStoreSpec{Type: "gcs", Bucket: "b", CredentialType: "auto", Prefix: "/x"}
	if s := BackupSettings(c, g, gcs); s.Repo.Type != "gcs" || s.Repo.KeyType != "auto" || s.Repo.CredentialsDir != "" || s.Repo.Path != "/x" {
		t.Errorf("gcs settings: %+v", s)
	}
	sftp := pol.Spec.DeepCopy()
	sftp.ObjectStore = pgshardv1alpha1.ObjectStoreSpec{Type: "sftp", SFTP: &pgshardv1alpha1.SFTPStoreSpec{Host: "h", User: "u", Port: 2222, HostKeyCheck: "none"},
		Credentials: pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "key"}}}
	if s := BackupSettings(c, g, sftp); s.Repo.Host != "h" || s.Repo.HostUser != "u" || s.Repo.HostPort != 2222 || s.Repo.HostKeyCheck != "none" {
		t.Errorf("sftp settings: %+v", s)
	}
	if s := BackupSettings(c, Groups(c)[0], &pol.Spec); s.Stanza != "demo-catalog-pg18" {
		t.Errorf("catalog stanza %q", s.Stanza)
	}
}

func TestTemplateAndPodCarryBackupPolicy(t *testing.T) {
	c := newCluster("demo")
	g := Groups(c)[1]
	pol := newPolicy()
	plain := Template(c, Group{}, nil, nil)
	withPol := Template(c, Group{}, nil, pol)
	if plain.Hash() == withPol.Hash() {
		t.Fatal("attaching a policy must change the pod hash (archive_mode needs a restart)")
	}
	if plain.SettingsHash() != withPol.SettingsHash() {
		t.Fatal("policy must not change the GUC settings hash")
	}
	pol2 := pol.DeepCopy()
	pol2.Spec.Retention.Full = 9
	if Template(c, Group{}, nil, pol2).Hash() == withPol.Hash() {
		t.Fatal("policy change must change the pod hash")
	}
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, "pvc", withPol)
	mounts := map[string]string{}
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		mounts[m.Name] = m.MountPath
	}
	if mounts[backupCredentialsVolume] != backupCredentialsMountPath || mounts[backupEncryptionVolume] != backupEncryptionMountPath {
		t.Errorf("mounts %v", mounts)
	}
	vols := map[string]string{}
	for _, v := range pod.Spec.Volumes {
		if v.Secret != nil {
			vols[v.Name] = v.Secret.SecretName
		}
	}
	if vols[backupCredentialsVolume] != "minio-creds" || vols[backupEncryptionVolume] != "repo-key" {
		t.Errorf("volumes %v", vols)
	}
	// Named, not counted: a pod gains volumes for reasons that have nothing
	// to do with backups, and a count says "something changed" where the
	// test means "no backup secret is here".
	bare := Renderer{}.Pod(c, g, 0, RolePrimary, "pvc", plain)
	for _, v := range bare.Spec.Volumes {
		if v.Name == backupCredentialsVolume || v.Name == backupEncryptionVolume {
			t.Errorf("pod without policy mounts %s", v.Name)
		}
	}
	cm := Renderer{}.ConfigMap(c, g, g.MemberName(0), nil, pol, false)
	var cfg agent.Config
	if err := json.Unmarshal([]byte(cm.Data[agentConfigKey(g.MemberName(1))]), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Backup == nil || cfg.Backup.Stanza != "demo-shard-0-pg18" || cfg.Backup.Repo.Endpoint != "http://minio.objectstores.svc:9000" || cfg.Backup.Repo.CredentialsDir != backupCredentialsMountPath {
		t.Errorf("agent backup config: %+v", cfg.Backup)
	}
	if got, err := backup.Render(cfg.Backup.WithDefaults(), "/pgdata", 5432); err == nil || !strings.Contains(err.Error(), backupEncryptionMountPath+"/passphrase") {
		t.Errorf("rendering must look for the mounted credentials: %q %v", got, err)
	}
	plainCM := Renderer{}.ConfigMap(c, g, g.MemberName(0), nil, nil, false)
	if strings.Contains(plainCM.Data[agentConfigKey(g.MemberName(1))], `"backup"`) {
		t.Error("no policy must render no backup section")
	}
}

func TestBackupHealth(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	at := func(t time.Time) *metav1.Time { m := metav1.NewTime(t); return &m }
	daily := pgshardv1alpha1.BackupSchedules{Full: "0 2 * * *"}
	both := pgshardv1alpha1.BackupSchedules{Full: "0 2 * * *", Incremental: "*/15 * * * *"}
	cases := []struct {
		name   string
		sched  pgshardv1alpha1.BackupSchedules
		last   map[string]*metav1.Time
		status metav1.ConditionStatus
		reason string
	}{
		{"no schedule no backups", pgshardv1alpha1.BackupSchedules{}, nil, metav1.ConditionFalse, "NoBackups"},
		{"no schedule with backup", pgshardv1alpha1.BackupSchedules{}, map[string]*metav1.Time{"incremental": at(now.Add(-time.Hour))}, metav1.ConditionTrue, "Unscheduled"},
		{"scheduled never ran", daily, nil, metav1.ConditionFalse, "Overdue"},
		{"full within period", daily, map[string]*metav1.Time{"full": at(now.Add(-11 * time.Hour))}, metav1.ConditionTrue, "Current"},
		{"full missed once still in grace", daily, map[string]*metav1.Time{"full": at(now.Add(-30 * time.Hour))}, metav1.ConditionTrue, "Current"},
		{"full missed a whole period", daily, map[string]*metav1.Time{"full": at(now.Add(-50 * time.Hour))}, metav1.ConditionFalse, "Overdue"},
		{"incr covered by fresh full", both, map[string]*metav1.Time{"full": at(now.Add(-10 * time.Minute))}, metav1.ConditionTrue, "Current"},
		{"incr overdue while full fine", both, map[string]*metav1.Time{"full": at(now.Add(-5 * time.Hour)), "incremental": at(now.Add(-40 * time.Minute))}, metav1.ConditionFalse, "Overdue"},
		{"incr fresh but full overdue", both, map[string]*metav1.Time{"full": at(now.Add(-72 * time.Hour)), "incremental": at(now.Add(-5 * time.Minute))}, metav1.ConditionFalse, "Overdue"},
		{"invalid expression", pgshardv1alpha1.BackupSchedules{Full: "nope"}, map[string]*metav1.Time{"full": at(now)}, metav1.ConditionFalse, "Overdue"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := BackupHealth(now, tc.sched, tc.last)
			if got.Type != pgshardv1alpha1.ConditionBackupHealthy || got.Status != tc.status || got.Reason != tc.reason {
				t.Errorf("got %s/%s (%s) want %s/%s", got.Status, got.Reason, got.Message, tc.status, tc.reason)
			}
		})
	}
}

func TestLastSuccessfulAndNames(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 8, 19, 1, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Hour))
	bs := []pgshardv1alpha1.PgShardBackup{
		{Spec: pgshardv1alpha1.PgShardBackupSpec{Type: ""}, Status: pgshardv1alpha1.PgShardBackupStatus{Phase: "Completed", CompletedAt: &t0}},
		{Spec: pgshardv1alpha1.PgShardBackupSpec{Type: "full"}, Status: pgshardv1alpha1.PgShardBackupStatus{Phase: "Completed", CompletedAt: &t1}},
		{Spec: pgshardv1alpha1.PgShardBackupSpec{Type: "incremental"}, Status: pgshardv1alpha1.PgShardBackupStatus{Phase: "Failed", CompletedAt: &t1}},
		{Spec: pgshardv1alpha1.PgShardBackupSpec{Type: "incremental"}, Status: pgshardv1alpha1.PgShardBackupStatus{Phase: "Completed"}},
	}
	last := lastSuccessful(bs)
	if !last["full"].Equal(&t1) || last["incremental"] != nil || last["differential"] != nil {
		t.Errorf("last %v", last)
	}
	if got := ScheduledBackupName("nightly", "demo", "incremental", t1.Time); got != "nightly-demo-incr-20260819-0200" {
		t.Errorf("name %q", got)
	}
	if _, err := ParseSchedule("*/15 * * * *"); err != nil {
		t.Error(err)
	}
	if _, err := ParseSchedule("@daily"); err != nil {
		t.Error(err)
	}
	if _, err := ParseSchedule("0 0 0 * * *"); err == nil {
		t.Error("six-field expressions must be rejected")
	}
	if formatLSN(0x4000050) != "0/4000050" || formatLSN(1<<32|0x28) != "1/28" {
		t.Error("lsn format")
	}
}

func fakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&pgshardv1alpha1.PgShardBackup{}, &pgshardv1alpha1.PgShardBackupPolicy{}, &pgshardv1alpha1.PgShardGroup{}).Build()
}

func TestSchedulerFireCreatesOwnedBackupAndSkipsWhileRunning(t *testing.T) {
	pol := newPolicy()
	other := boundCluster("other")
	other.Spec.Backup.PolicyRef = "weekly"
	cl := fakeClient(t, pol, boundCluster("demo"), other)
	s := NewBackupScheduler(cl)
	tick := time.Date(2026, 8, 19, 2, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return tick }
	key := client.ObjectKeyFromObject(pol)
	if err := s.Fire(context.Background(), key, "full"); err != nil {
		t.Fatal(err)
	}
	var b pgshardv1alpha1.PgShardBackup
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "nightly-demo-full-20260819-0200"}, &b); err != nil {
		t.Fatal(err)
	}
	if b.Spec.ClusterName != "demo" || b.Spec.Type != "full" || b.Labels[LabelBackupPolicy] != "nightly" || len(b.OwnerReferences) != 1 || b.OwnerReferences[0].Name != "nightly" {
		t.Errorf("backup %+v", b)
	}
	if err := s.Fire(context.Background(), key, "full"); err != nil {
		t.Fatal("re-firing the same tick must be idempotent:", err)
	}
	tick = tick.Add(15 * time.Minute)
	if err := s.Fire(context.Background(), key, "incremental"); err != nil {
		t.Fatal(err)
	}
	var list pgshardv1alpha1.PgShardBackupList
	_ = cl.List(context.Background(), &list)
	if len(list.Items) != 1 {
		t.Fatalf("a tick must be skipped while the previous backup has not finished: %d backups", len(list.Items))
	}
	b.Status.Phase = pgshardv1alpha1.BackupPhaseCompleted
	if err := cl.Status().Update(context.Background(), &b); err != nil {
		t.Fatal(err)
	}
	if err := s.Fire(context.Background(), key, "incremental"); err != nil {
		t.Fatal(err)
	}
	_ = cl.List(context.Background(), &list)
	if len(list.Items) != 2 {
		t.Fatalf("expected the incremental to be created: %d", len(list.Items))
	}
	for _, it := range list.Items {
		if it.Spec.ClusterName != "demo" {
			t.Errorf("backup for a cluster bound to another policy: %s", it.Name)
		}
	}
	if err := s.Fire(context.Background(), types.NamespacedName{Namespace: "default", Name: "gone"}, "full"); err != nil {
		t.Fatal("missing policy must be ignored:", err)
	}
}

func TestSchedulerSetKeepsUnchangedEntriesAndRejectsBadCron(t *testing.T) {
	s := NewBackupScheduler(fakeClient(t))
	key := types.NamespacedName{Namespace: "default", Name: "p"}
	if err := s.Set(key, pgshardv1alpha1.BackupSchedules{Full: "0 2 * * *", Incremental: "*/15 * * * *"}); err != nil {
		t.Fatal(err)
	}
	if s.Entries(key) != 2 {
		t.Fatalf("entries %d", s.Entries(key))
	}
	ids := append([]cron.EntryID(nil), s.entries[key]...)
	if err := s.Set(key, pgshardv1alpha1.BackupSchedules{Full: "0 2 * * *", Incremental: "*/15 * * * *"}); err != nil {
		t.Fatal(err)
	}
	for i, id := range s.entries[key] {
		if id != ids[i] {
			t.Fatal("unchanged schedules must keep their cron entries")
		}
	}
	if err := s.Set(key, pgshardv1alpha1.BackupSchedules{Full: "bad"}); err == nil || s.Entries(key) != 0 {
		t.Fatalf("bad cron must fail and arm nothing: %v %d", err, s.Entries(key))
	}
	if err := s.Set(key, pgshardv1alpha1.BackupSchedules{Differential: "@hourly"}); err != nil || s.Entries(key) != 1 {
		t.Fatalf("set %v %d", err, s.Entries(key))
	}
	s.Remove(key)
	if s.Entries(key) != 0 {
		t.Fatal("remove")
	}
}

type fakeBackupAgents struct {
	mu      sync.Mutex
	calls   []string
	fail    map[string]error
	results map[string]BackupResult
	block   chan struct{}
}

func (f *fakeBackupAgents) Backup(ctx context.Context, addr string, t string) (BackupResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "backup "+addr+" "+t)
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return BackupResult{}, ctx.Err()
		}
	}
	if err := f.fail[addr]; err != nil {
		return BackupResult{Log: []string{"ERROR: boom"}}, err
	}
	res := f.results[addr]
	if res.Label == "" {
		res = BackupResult{Label: "20260819-020000F", Type: t, StartLSN: 0x2000028, StopLSN: 0x2000120, ArchiveStart: "000000010000000000000002", ArchiveStop: "000000010000000000000002", SizeBytes: 100, RepoBytes: 10}
	}
	return res, nil
}

func (f *fakeBackupAgents) Expire(_ context.Context, addr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "expire "+addr)
	return f.fail["expire "+addr]
}

func (f *fakeBackupAgents) Info(context.Context, string) (RepoInfo, error) { return RepoInfo{}, nil }

func (f *fakeBackupAgents) Verify(_ context.Context, addr string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "verify "+addr)
	return []string{"verified " + addr}, f.fail["verify "+addr]
}

func (f *fakeBackupAgents) journal() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func readyPod(name, ip string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
}

func backupFixture(t *testing.T, agents *fakeBackupAgents) (*BackupReconciler, client.Client, *pgshardv1alpha1.PgShardBackup) {
	t.Helper()
	c := boundCluster("demo")
	one := 1
	c.Spec.Shards = &one
	pol := newPolicy()
	b := &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: "default", UID: "b1-uid"},
		Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "demo", Type: "incremental"}}
	objs := []client.Object{c, pol, b,
		&pgshardv1alpha1.PgShardGroup{ObjectMeta: metav1.ObjectMeta{Name: "demo-catalog", Namespace: "default"}, Status: pgshardv1alpha1.PgShardGroupStatus{Primary: "demo-catalog-0"}},
		&pgshardv1alpha1.PgShardGroup{ObjectMeta: metav1.ObjectMeta{Name: "demo-shard-0", Namespace: "default"}, Status: pgshardv1alpha1.PgShardGroupStatus{Primary: "demo-shard-0-2"}},
		readyPod("demo-catalog-0", "10.0.0.1"), readyPod("demo-shard-0-2", "10.0.0.2"),
	}
	cl := fakeClient(t, objs...)
	now := time.Date(2026, 8, 19, 3, 0, 0, 0, time.UTC)
	r := &BackupReconciler{Client: cl, Agents: agents, Now: func() time.Time { return now }}
	return r, cl, b
}

func reconcileBackup(t *testing.T, r *BackupReconciler, b *pgshardv1alpha1.PgShardBackup) (ctrl.Result, *pgshardv1alpha1.PgShardBackup) {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(b)})
	if err != nil {
		t.Fatal(err)
	}
	var got pgshardv1alpha1.PgShardBackup
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(b), &got); err != nil {
		t.Fatal(err)
	}
	return res, &got
}

func waitRun(t *testing.T, r *BackupReconciler, b *pgshardv1alpha1.PgShardBackup) {
	t.Helper()
	run := r.run(b.UID)
	if run == nil {
		t.Fatal("no run in flight")
	}
	select {
	case <-run.done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not finish")
	}
}

func TestBackupReconcilerRunsEveryGroupPrimaryInOrder(t *testing.T) {
	agents := &fakeBackupAgents{results: map[string]BackupResult{
		"10.0.0.2:9090": {Label: "20260819-020000F_20260819-030000I", Type: "incr", StartLSN: 0x3000028, StopLSN: 0x4000050, ArchiveStart: "000000010000000000000003", ArchiveStop: "000000010000000000000004", SizeBytes: 200, RepoBytes: 20},
	}}
	r, _, b := backupFixture(t, agents)
	res, got := reconcileBackup(t, r, b)
	if got.Status.Phase != pgshardv1alpha1.BackupPhaseRunning || res.RequeueAfter != backupPollInterval || got.Status.StartedAt == nil || len(got.Status.Groups) != 2 || got.Status.Groups[1].Stanza != "demo-shard-0-pg18" {
		t.Fatalf("after start: %+v", got.Status)
	}
	waitRun(t, r, b)
	_, got = reconcileBackup(t, r, b)
	if got.Status.Phase != pgshardv1alpha1.BackupPhaseCompleted || got.Status.CompletedAt == nil || got.Status.BackupID != "20260819-020000F" || got.Status.Error != "" {
		t.Fatalf("after finish: %+v", got.Status)
	}
	// Every backup first, then every expire. Expiring a group the moment
	// its own backup lands can retire the set the last complete cluster
	// backup depends on while a later group is still running; if that one
	// then fails, nothing restorable cluster-wide is left.
	if want := []string{"backup 10.0.0.1:9090 incr", "backup 10.0.0.2:9090 incr", "expire 10.0.0.1:9090", "expire 10.0.0.2:9090"}; strings.Join(agents.journal(), ",") != strings.Join(want, ",") {
		t.Fatalf("calls %v", agents.journal())
	}
	shard := got.Status.Groups[1]
	if shard.Group != "shard-0" || shard.BackupID != "20260819-020000F_20260819-030000I" || shard.StartLSN != "0/3000028" || shard.StopLSN != "0/4000050" || shard.WALStop != "000000010000000000000004" || shard.SizeBytes != 200 || shard.RepoSizeBytes != 20 || shard.Duration == "" || shard.Error != "" {
		t.Errorf("shard status %+v", shard)
	}
	if c := meta.FindStatusCondition(got.Status.Conditions, ConditionRetentionApplied); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("retention condition %+v", c)
	}
	if c := meta.FindStatusCondition(got.Status.Conditions, "Progressing"); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "Completed" {
		t.Errorf("progressing %+v", c)
	}
	if r.run(b.UID) != nil {
		t.Error("finished run must be forgotten")
	}
	if res, _ := reconcileBackup(t, r, b); res.RequeueAfter != 0 || len(agents.journal()) != 4 {
		t.Error("terminal backup must not run again")
	}
}

// A group's failure fails the RUN and not the groups after it. Each group
// has its own stanza and its own repository, so one failing says nothing
// about the next -- and abandoning the run left every later group with
// whatever backup it had from the run before, which is the recovery-point
// window a backup is taken to close.
func TestBackupReconcilerBacksUpEveryGroupEvenAfterOneFails(t *testing.T) {
	agents := &fakeBackupAgents{fail: map[string]error{"10.0.0.1:9090": errors.New("exit status 41")}}
	r, _, b := backupFixture(t, agents)
	reconcileBackup(t, r, b)
	waitRun(t, r, b)
	_, got := reconcileBackup(t, r, b)
	if got.Status.Phase != pgshardv1alpha1.BackupPhaseFailed || !strings.Contains(got.Status.Error, "group catalog: exit status 41") || got.Status.BackupID != "" {
		t.Fatalf("status %+v", got.Status)
	}
	if len(got.Status.Groups) != 2 || got.Status.Groups[1].Group != "shard-0" || got.Status.Groups[1].Error != "" || got.Status.Groups[1].BackupID == "" {
		t.Fatalf("the shard must be backed up although the catalog failed: %+v", got.Status.Groups)
	}
	if want := []string{"backup 10.0.0.1:9090 incr", "backup 10.0.0.2:9090 incr"}; strings.Join(agents.journal(), ",") != strings.Join(want, ",") {
		t.Fatalf("calls %v", agents.journal())
	}
	// Retention is the one thing that still waits for the whole run:
	// expiring on the group that succeeded can retire the set the group
	// that failed still depends on.
	if c := meta.FindStatusCondition(got.Status.Conditions, ConditionRetentionApplied); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "RunIncomplete" {
		t.Errorf("retention condition %+v", c)
	}
}

// Both groups failing is still one run, and the status names both. Naming
// only the last hides from whoever has to decide what is restorable.
func TestBackupReconcilerNamesEveryFailedGroup(t *testing.T) {
	agents := &fakeBackupAgents{fail: map[string]error{
		"10.0.0.1:9090": errors.New("exit status 41"),
		"10.0.0.2:9090": errors.New("exit status 42"),
	}}
	r, _, b := backupFixture(t, agents)
	reconcileBackup(t, r, b)
	waitRun(t, r, b)
	_, got := reconcileBackup(t, r, b)
	if got.Status.Phase != pgshardv1alpha1.BackupPhaseFailed ||
		!strings.Contains(got.Status.Error, "group catalog: exit status 41") ||
		!strings.Contains(got.Status.Error, "group shard-0: exit status 42") {
		t.Fatalf("status %+v", got.Status)
	}
}

func TestBackupReconcilerRetentionFailureIsReportedNotFatal(t *testing.T) {
	agents := &fakeBackupAgents{fail: map[string]error{"expire 10.0.0.2:9090": errors.New("lock held")}}
	r, _, b := backupFixture(t, agents)
	reconcileBackup(t, r, b)
	waitRun(t, r, b)
	_, got := reconcileBackup(t, r, b)
	if got.Status.Phase != pgshardv1alpha1.BackupPhaseCompleted {
		t.Fatalf("status %+v", got.Status)
	}
	if c := meta.FindStatusCondition(got.Status.Conditions, ConditionRetentionApplied); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "ExpireFailed" || !strings.Contains(c.Message, "shard-0: lock held") {
		t.Errorf("retention condition %+v", c)
	}
}

func TestBackupReconcilerWaitsForPrimariesAndFailsWithoutPolicyOrCluster(t *testing.T) {
	agents := &fakeBackupAgents{}
	r, cl, b := backupFixture(t, agents)
	var pod corev1.Pod
	_ = cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-shard-0-2"}, &pod)
	_ = cl.Delete(context.Background(), &pod)
	res, got := reconcileBackup(t, r, b)
	if got.Status.Phase != pgshardv1alpha1.BackupPhasePending || res.RequeueAfter != backupPollInterval || len(agents.journal()) != 0 {
		t.Fatalf("pending: %+v %v", got.Status, res)
	}
	if c := meta.FindStatusCondition(got.Status.Conditions, "Progressing"); c == nil || !strings.Contains(c.Message, "demo-shard-0-2") {
		t.Errorf("pending message %+v", c)
	}
	if err := cl.Create(context.Background(), readyPod("demo-shard-0-2", "10.0.0.2")); err != nil {
		t.Fatal(err)
	}
	_, got = reconcileBackup(t, r, b)
	if got.Status.Phase != pgshardv1alpha1.BackupPhaseRunning {
		t.Fatalf("running: %+v", got.Status)
	}
	waitRun(t, r, b)

	orphan := &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "default", UID: "o"}, Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "nope"}}
	if err := cl.Create(context.Background(), orphan); err != nil {
		t.Fatal(err)
	}
	if _, got := reconcileBackup(t, r, orphan); got.Status.Phase != pgshardv1alpha1.BackupPhaseFailed || !strings.Contains(got.Status.Error, `cluster "nope" not found`) {
		t.Fatalf("orphan %+v", got.Status)
	}
	var pol pgshardv1alpha1.PgShardBackupPolicy
	_ = cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "nightly"}, &pol)
	_ = cl.Delete(context.Background(), &pol)
	b2 := &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Name: "b2", Namespace: "default", UID: "b2"}, Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "demo", Type: "bogus"}}
	if err := cl.Create(context.Background(), b2); err != nil {
		t.Fatal(err)
	}
	if _, got := reconcileBackup(t, r, b2); got.Status.Phase != pgshardv1alpha1.BackupPhaseFailed || !strings.Contains(got.Status.Error, "unknown backup type") {
		t.Fatalf("bad type %+v", got.Status)
	}
	b3 := &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Name: "b3", Namespace: "default", UID: "b3"}, Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "demo"}}
	if err := cl.Create(context.Background(), b3); err != nil {
		t.Fatal(err)
	}
	if _, got := reconcileBackup(t, r, b3); got.Status.Phase != pgshardv1alpha1.BackupPhaseFailed || !strings.Contains(got.Status.Error, "backup policy not found") {
		t.Fatalf("missing policy %+v", got.Status)
	}
	var demo pgshardv1alpha1.PgShardCluster
	_ = cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo"}, &demo)
	demo.Spec.Backup.PolicyRef = ""
	if err := cl.Update(context.Background(), &demo); err != nil {
		t.Fatal(err)
	}
	b4 := &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Name: "b4", Namespace: "default", UID: "b4"}, Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "demo"}}
	if err := cl.Create(context.Background(), b4); err != nil {
		t.Fatal(err)
	}
	if _, got := reconcileBackup(t, r, b4); got.Status.Phase != pgshardv1alpha1.BackupPhaseFailed || !strings.Contains(got.Status.Error, "no spec.backup.policyRef") {
		t.Fatalf("unbound cluster %+v", got.Status)
	}
}

func TestBackupReconcilerSerialisesBackupsPerCluster(t *testing.T) {
	agents := &fakeBackupAgents{}
	r, cl, b := backupFixture(t, agents)
	running := &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Name: "sched", Namespace: "default", UID: "sched"}, Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "demo"}}
	if err := cl.Create(context.Background(), running); err != nil {
		t.Fatal(err)
	}
	running.Status.Phase = pgshardv1alpha1.BackupPhaseRunning
	if err := cl.Status().Update(context.Background(), running); err != nil {
		t.Fatal(err)
	}
	res, got := reconcileBackup(t, r, b)
	if got.Status.Phase != pgshardv1alpha1.BackupPhasePending || res.RequeueAfter != backupPollInterval || len(agents.journal()) != 0 {
		t.Fatalf("must wait for the running backup: %+v", got.Status)
	}
	if c := meta.FindStatusCondition(got.Status.Conditions, "Progressing"); c == nil || !strings.Contains(c.Message, "sched of cluster demo is still running") {
		t.Errorf("pending message %+v", c)
	}
	running.Status.Phase = pgshardv1alpha1.BackupPhaseCompleted
	if err := cl.Status().Update(context.Background(), running); err != nil {
		t.Fatal(err)
	}
	if _, got := reconcileBackup(t, r, b); got.Status.Phase != pgshardv1alpha1.BackupPhaseRunning {
		t.Fatalf("must start once the other finished: %+v", got.Status)
	}
	waitRun(t, r, b)
}

func TestBackupReconcilerFailsRunningBackupAfterRestart(t *testing.T) {
	agents := &fakeBackupAgents{}
	r, cl, b := backupFixture(t, agents)
	b.Status.Phase = pgshardv1alpha1.BackupPhaseRunning
	if err := cl.Status().Update(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if _, got := reconcileBackup(t, r, b); got.Status.Phase != pgshardv1alpha1.BackupPhaseFailed || !strings.Contains(got.Status.Error, "operator restarted") || len(agents.journal()) != 0 {
		t.Fatalf("status %+v", got.Status)
	}
}

func TestBackupPolicyReconcilerStatusAndSchedules(t *testing.T) {
	c := boundCluster("demo")
	pol := newPolicy()
	done := metav1.NewTime(time.Date(2026, 8, 19, 2, 30, 0, 0, time.UTC))
	full := &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Name: "f", Namespace: "default"}, Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "demo", Type: "full"},
		Status: pgshardv1alpha1.PgShardBackupStatus{Phase: "Completed", CompletedAt: &done}}
	other := &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Name: "o", Namespace: "default"}, Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "other", Type: "incremental"},
		Status: pgshardv1alpha1.PgShardBackupStatus{Phase: "Completed", CompletedAt: &done}}
	cl := fakeClient(t, c, pol, full, other)
	sched := NewBackupScheduler(cl)
	now := done.Add(10 * time.Minute)
	r := &BackupPolicyReconciler{Client: cl, Scheduler: sched, Now: func() time.Time { return now }}
	key := client.ObjectKeyFromObject(pol)
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter != policyRequeue {
		t.Errorf("requeue %v", res)
	}
	var got pgshardv1alpha1.PgShardBackupPolicy
	_ = cl.Get(context.Background(), key, &got)
	if len(got.Status.Clusters) != 1 || got.Status.Clusters[0].Name != "demo" || got.Status.Clusters[0].LastFullTime == nil || !got.Status.Clusters[0].LastFullTime.Equal(&done) || got.Status.Clusters[0].LastIncrementalTime != nil || !got.Status.Clusters[0].Healthy {
		t.Errorf("clusters %+v", got.Status.Clusters)
	}
	if cnd := meta.FindStatusCondition(got.Status.Conditions, ConditionPolicyValid); cnd == nil || cnd.Status != metav1.ConditionTrue {
		t.Errorf("valid %+v", cnd)
	}
	if cnd := meta.FindStatusCondition(got.Status.Conditions, pgshardv1alpha1.ConditionBackupHealthy); cnd == nil || cnd.Status != metav1.ConditionTrue {
		t.Errorf("healthy %+v", cnd)
	}
	if sched.Entries(key) != 2 {
		t.Errorf("entries %d", sched.Entries(key))
	}
	now = done.Add(2 * time.Hour)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	_ = cl.Get(context.Background(), key, &got)
	if cnd := meta.FindStatusCondition(got.Status.Conditions, pgshardv1alpha1.ConditionBackupHealthy); cnd == nil || cnd.Status != metav1.ConditionFalse || !strings.Contains(cnd.Message, "demo: incremental backup overdue") {
		t.Errorf("overdue %+v", cnd)
	}
	if got.Status.Clusters[0].Healthy || !strings.Contains(got.Status.Clusters[0].Message, "overdue") {
		t.Errorf("cluster status must be unhealthy: %+v", got.Status.Clusters[0])
	}

	got.Spec.ObjectStore.Bucket = ""
	if err := cl.Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	_ = cl.Get(context.Background(), key, &got)
	if cnd := meta.FindStatusCondition(got.Status.Conditions, ConditionPolicyValid); cnd == nil || cnd.Status != metav1.ConditionFalse || !strings.Contains(cnd.Message, "s3 repo needs bucket") {
		t.Errorf("invalid %+v", cnd)
	}
	if sched.Entries(key) != 0 {
		t.Error("invalid policy must disarm its schedules")
	}
	got.Spec.ObjectStore.Bucket = "pgshard"
	got.Spec.Schedules.Full = "61 * * * *"
	if err := cl.Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	_ = cl.Get(context.Background(), key, &got)
	if cnd := meta.FindStatusCondition(got.Status.Conditions, ConditionPolicyValid); cnd == nil || cnd.Status != metav1.ConditionFalse || !strings.Contains(cnd.Message, "schedules.full") {
		t.Errorf("bad cron %+v", cnd)
	}
	if err := cl.Delete(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyBindingAndWatches(t *testing.T) {
	pol := newPolicy()
	bound := boundCluster("demo")
	unbound := newCluster("plain")
	dangling := newCluster("dangling")
	dangling.Spec.Backup.PolicyRef = "gone"
	cl := fakeClient(t, pol, bound, unbound, dangling)
	ctx := context.Background()
	if p, err := findBackupPolicy(ctx, cl, bound); err != nil || p == nil || p.Name != "nightly" {
		t.Errorf("bound: %v %v", p, err)
	}
	if p, err := findBackupPolicy(ctx, cl, unbound); err != nil || p != nil {
		t.Errorf("unbound: %v %v", p, err)
	}
	if _, err := findBackupPolicy(ctx, cl, dangling); !errors.Is(err, ErrBackupPolicyMissing) {
		t.Errorf("dangling: %v", err)
	}
	r := &ClusterReconciler{Client: cl}
	if reqs := r.policyToClusters(ctx, pol); len(reqs) != 1 || reqs[0].Name != "demo" {
		t.Errorf("policy map %v", reqs)
	}
	if reqs := clusterToPolicy(ctx, bound); len(reqs) != 1 || reqs[0].Name != "nightly" {
		t.Errorf("cluster map %v", reqs)
	}
	if reqs := clusterToPolicy(ctx, unbound); len(reqs) != 0 {
		t.Errorf("unbound cluster map %v", reqs)
	}
	b := &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "b"}, Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "demo"}}
	if reqs := backupToCluster(ctx, b); len(reqs) != 1 || reqs[0].Name != "demo" {
		t.Errorf("backup map %v", reqs)
	}
	pr := &BackupPolicyReconciler{Client: cl}
	if reqs := pr.backupToPolicy(ctx, b); len(reqs) != 1 || reqs[0].Name != "nightly" {
		t.Errorf("backup to policy map %v", reqs)
	}
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	cr := &ClusterReconciler{Client: cl, Now: func() time.Time { return now }}
	if _, _, cond, err := cr.backupState(ctx, unbound); err != nil || cond.Status != metav1.ConditionFalse || cond.Reason != "NoPolicy" {
		t.Errorf("unbound state: %+v %v", cond, err)
	}
	if _, _, cond, err := cr.backupState(ctx, dangling); err != nil || cond.Status != metav1.ConditionFalse || cond.Reason != "PolicyMissing" {
		t.Errorf("dangling state: %+v %v", cond, err)
	}
	if p, _, cond, err := cr.backupState(ctx, bound); err != nil || p == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "Overdue" || !strings.Contains(cond.Message, "policy nightly (s3)") {
		t.Errorf("bound state without backups: %+v %v", cond, err)
	}
	done := metav1.NewTime(now.Add(-5 * time.Minute))
	full := &pgshardv1alpha1.PgShardBackup{ObjectMeta: metav1.ObjectMeta{Name: "f", Namespace: "default"}, Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "demo", Type: "full"},
		Status: pgshardv1alpha1.PgShardBackupStatus{Phase: "Completed", CompletedAt: &done}}
	if err := cl.Create(ctx, full); err != nil {
		t.Fatal(err)
	}
	if _, _, cond, err := cr.backupState(ctx, bound); err != nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Current" {
		t.Errorf("bound state with fresh full: %+v %v", cond, err)
	}
	pol.Finalizers = []string{"pgshard.io/test"}
	if err := cl.Update(ctx, pol); err != nil {
		t.Fatal(err)
	}
	if err := cl.Delete(ctx, pol); err != nil {
		t.Fatal(err)
	}
	if _, _, cond, err := cr.backupState(ctx, bound); err != nil || cond.Reason != "PolicyMissing" || !strings.Contains(cond.Message, "being deleted") {
		t.Errorf("policy being deleted: %+v %v", cond, err)
	}
	if reqs := r.policyToClusters(ctx, pol); len(reqs) != 1 {
		t.Errorf("a deleting policy still maps to its clusters so they drop the config: %v", reqs)
	}
}

func TestBackupPolicyStatusWithoutClusters(t *testing.T) {
	pol := newPolicy()
	cl := fakeClient(t, pol)
	r := &BackupPolicyReconciler{Client: cl, Scheduler: NewBackupScheduler(cl)}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pol)}); err != nil {
		t.Fatal(err)
	}
	var got pgshardv1alpha1.PgShardBackupPolicy
	_ = cl.Get(context.Background(), client.ObjectKeyFromObject(pol), &got)
	if cnd := meta.FindStatusCondition(got.Status.Conditions, pgshardv1alpha1.ConditionBackupHealthy); cnd == nil || cnd.Status != metav1.ConditionFalse || cnd.Reason != "NoClusters" {
		t.Errorf("healthy %+v", cnd)
	}
	if cnd := meta.FindStatusCondition(got.Status.Conditions, ConditionPolicyValid); cnd == nil || cnd.Status != metav1.ConditionTrue {
		t.Errorf("valid %+v", cnd)
	}
}

// TestBackupRecordsTheSpecItStartedWith: the reconciler joined a running
// backup by UID alone and read the cluster and type off the current spec
// at every step, so provenance came from a value that could change under
// the run. The accepted spec is recorded in status before any physical
// work, and everything downstream reads it from there.
func TestBackupRecordsTheSpecItStartedWith(t *testing.T) {
	agents := &fakeBackupAgents{}
	r, _, b := backupFixture(t, agents)
	_, got := reconcileBackup(t, r, b)
	if got.Status.ClusterName != b.Spec.ClusterName || got.Status.Type != b.Spec.Type {
		t.Fatalf("the accepted spec must be recorded before the run: %+v", got.Status)
	}
	if got.Status.Policy == "" || got.Status.PolicyUID == "" {
		t.Fatalf("the resolved policy must be recorded: %+v", got.Status)
	}
	// The API refuses a spec edit, but a cluster's backups are attributed
	// from the recorded value regardless, so nothing downstream depends on
	// the spec having stayed put.
	list, err := backupsOfCluster(context.Background(), r.Client, b.Namespace, got.Status.ClusterName)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != b.Name {
		t.Fatalf("a started backup must be attributed to the cluster it started against: %+v", list)
	}
	if list, err = backupsOfCluster(context.Background(), r.Client, b.Namespace, "somewhere-else"); err != nil || len(list) != 0 {
		t.Fatalf("attributed to the wrong cluster: %+v %v", list, err)
	}
}

// TestAnInvalidPolicyEditDoesNotReachTheMembers: the policy reconciler
// marks a bad spec Valid=False and the cluster reconciler read spec.
// anyway, so a rejected desired state became executable member
// configuration: it changed the template and the pod hash, rolled the
// members, and could leave a replacement agent refusing to start on
// settings nothing had approved.
func TestAnInvalidPolicyEditDoesNotReachTheMembers(t *testing.T) {
	ctx := context.Background()
	good := pgshardv1alpha1.PgShardBackupPolicySpec{
		ObjectStore: pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "backups", Region: "eu-west-1"},
		Schedules:   pgshardv1alpha1.BackupSchedules{Full: "0 2 * * *"},
	}
	pol := &pgshardv1alpha1.PgShardBackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default", Generation: 2},
		// Generation 2's spec never validated; generation 1's did.
		Spec: pgshardv1alpha1.PgShardBackupPolicySpec{ObjectStore: pgshardv1alpha1.ObjectStoreSpec{Type: "s3"}},
		Status: pgshardv1alpha1.PgShardBackupPolicyStatus{
			Accepted: good.DeepCopy(), AcceptedGeneration: 1,
		},
	}
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
	c.Spec.Backup.PolicyRef = "nightly"
	cl := restoreClient(t, pol, c)
	cr := &ClusterReconciler{Client: cl, Now: time.Now}

	got, _, cond, err := cr.backupState(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("the members must keep the configuration that validated, not lose it")
	}
	if got.Spec.ObjectStore.Bucket != "backups" || got.Spec.ObjectStore.Region != "eu-west-1" {
		t.Fatalf("members were given the rejected spec: %+v", got.Spec.ObjectStore)
	}
	if !reflect.DeepEqual(Template(c, Groups(c)[0], nil, got).Backup, &good) {
		t.Fatal("the member template must carry the accepted spec")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != "PolicyInvalid" {
		t.Fatalf("the cluster must say the policy is not being followed: %+v", cond)
	}
	if !strings.Contains(cond.Message, "generation 2 did not validate") {
		t.Fatalf("condition message %q", cond.Message)
	}

	// Once the edit validates, it is what the members get.
	validated := pol.DeepCopy()
	validated.Status.Accepted = pol.Spec.DeepCopy()
	validated.Status.AcceptedGeneration = 2
	cr = &ClusterReconciler{Client: restoreClient(t, validated, c.DeepCopy()), Now: time.Now}
	got, _, cond, err = cr.backupState(ctx, c)
	if err != nil || got == nil {
		t.Fatalf("after validation: %v", err)
	}
	if got.Spec.ObjectStore.Bucket != "" || cond.Reason == "PolicyInvalid" {
		t.Fatalf("an accepted edit must be followed: %+v %+v", got.Spec.ObjectStore, cond)
	}
}

// TestRepositoryEncryptionCondition: whether a backup can be read by
// whoever holds the bucket is worth saying on the policy, not leaving to be
// inferred from a spec field that may not be set at all.
func TestRepositoryEncryptionCondition(t *testing.T) {
	pol := func(mutate func(*pgshardv1alpha1.ObjectStoreSpec)) *pgshardv1alpha1.PgShardBackupPolicy {
		p := &pgshardv1alpha1.PgShardBackupPolicy{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default", Generation: 2}}
		p.Spec.ObjectStore = pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b"}
		mutate(&p.Spec.ObjectStore)
		return p
	}
	encrypted := repositoryEncryption(pol(func(st *pgshardv1alpha1.ObjectStoreSpec) {
		st.Encryption = pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "k"}}
	}))
	if encrypted.Status != metav1.ConditionTrue || encrypted.Reason != "Encrypted" {
		t.Errorf("encrypted store: %+v", encrypted)
	}
	plain := repositoryEncryption(pol(func(st *pgshardv1alpha1.ObjectStoreSpec) { st.InsecureUnencrypted = true }))
	if plain.Status != metav1.ConditionFalse || plain.Reason != "Unencrypted" {
		t.Errorf("plain store: %+v", plain)
	}
	local := repositoryEncryption(pol(func(st *pgshardv1alpha1.ObjectStoreSpec) { st.Type = "posix" }))
	if local.Status != metav1.ConditionFalse || local.Reason != "LocalRepository" {
		t.Errorf("posix store: %+v", local)
	}
	if encrypted.ObservedGeneration != 2 {
		t.Errorf("observedGeneration %d", encrypted.ObservedGeneration)
	}

	// What the members archive with is the accepted spec: an edit that does
	// not validate never reaches them, so a policy whose new spec adds
	// encryption while its accepted one has none is still writing in the
	// clear, and must say so.
	pending := pol(func(st *pgshardv1alpha1.ObjectStoreSpec) {
		st.Encryption = pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "k"}}
	})
	accepted := pending.Spec.DeepCopy()
	accepted.ObjectStore.Encryption = pgshardv1alpha1.SecretRefSpec{}
	pending.Status.Accepted = accepted
	got := repositoryEncryption(pending)
	if got.Status != metav1.ConditionFalse || got.Reason != "Unencrypted" {
		t.Errorf("a policy whose accepted spec has no encryption: %+v", got)
	}
	// And the advice must not be "turn it on", which breaks the repository.
	if strings.Contains(got.Message, "set objectStore.encryption.secretRef") {
		t.Errorf("the message tells an operator to do the thing that breaks an existing repository: %q", got.Message)
	}
}

// TestDeletingABackupStopsIt: a PgShardBackup that is deleted while its
// backup is running used to leave the worker going. It was bound only to
// the manager context and a twelve-hour timeout, it had nothing left to
// report to, and because the deleted record was no longer listed as Running
// a replacement could start against the same stanza beside it.
func TestDeletingABackupStopsIt(t *testing.T) {
	agents := &fakeBackupAgents{block: make(chan struct{})}
	r, cl, b := backupFixture(t, agents)
	if _, got := reconcileBackup(t, r, b); got.Status.Phase != pgshardv1alpha1.BackupPhaseRunning {
		t.Fatalf("phase %v", got.Status.Phase)
	}
	run := r.run(b.UID)
	if run == nil {
		t.Fatal("no run in flight")
	}

	// The record goes while the backup is still blocked in the agent.
	if err := cl.Delete(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(b)})
		if err != nil {
			t.Fatal(err)
		}
		if res.RequeueAfter == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the reconciler never finished stopping the run")
		}
		time.Sleep(5 * time.Millisecond)
	}

	select {
	case <-run.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker kept running after its record was deleted")
	}
	if r.run(b.UID) != nil {
		t.Error("the stopped run must not be left in the registry")
	}
}
