package admin

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/dockertest"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/operator"
)

func TestWatchesNotifyOnGroupUpdate(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		dockertest.EnvtestMissing(t, "the admin watch envtest")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	scheme, err := operator.NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		t.Fatal(err)
	}
	n := NewNotifier()
	if err := RegisterWatches(mgr, n, ""); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache sync")
	}
	events, unsub := n.Subscribe()
	defer unsub()

	drain := func() {
		for {
			select {
			case <-events:
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}
	drain()
	pg := &pgshardv1alpha1.PgShardGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-shard-0", Namespace: "default", Labels: map[string]string{operator.LabelCluster: "demo"}},
		Spec:       pgshardv1alpha1.PgShardGroupSpec{ClusterRef: "demo", Kind: "shard"},
	}
	if err := mgr.GetClient().Create(ctx, pg); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events:
	case <-time.After(10 * time.Second):
		t.Fatal("no notification after PgShardGroup create")
	}
	drain()
	pg.Status.Primary = "demo-shard-0-0"
	if err := mgr.GetClient().Status().Update(ctx, pg); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events:
	case <-time.After(10 * time.Second):
		t.Fatal("no notification after PgShardGroup status update")
	}
}

func TestRunServesAndShutsDown(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		dockertest.EnvtestMissing(t, "the admin watch envtest")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	user, err := env.ControlPlane.AddUser(envtest.User{Name: "admin-ui", Groups: []string{"system:masters"}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	kc, err := user.KubeConfig()
	if err != nil {
		t.Fatal(err)
	}
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(kubeconfig, kc, 0o600); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, Options{Listen: addr, Kubeconfig: kubeconfig, Namespace: "default"}) }()
	var resp *http.Response
	for deadline := time.Now().Add(20 * time.Second); ; {
		select {
		case runErr := <-done:
			t.Fatalf("Run exited early: %v", runErr)
		default:
		}
		resp, err = http.Get("http://" + addr + "/api/v1/clusters")
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != "[]" {
		t.Fatalf("clusters: %d %s", resp.StatusCode, body)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
	if _, err := http.Get("http://" + addr + "/healthz"); err == nil {
		t.Fatal("listener still open after shutdown")
	}
}

// TestAScopedAdminIsNotToldAboutAnotherCluster proves the wiring, not just
// the mapping: a real informer, a real object, and the notifier an admin's
// /events stream reads. The tick names no cluster, so the only way to see
// whether it should have fired is to change one cluster and watch the
// other's admin.
//
// Both halves matter equally. Never notifying would pass "no event for B"
// and leave every scoped admin's page frozen, which is why the same test
// asserts A's own churn still arrives -- for its groups, pods, backups,
// restores and reshards, each one rather than a representative.
func TestAScopedAdminIsNotToldAboutAnotherCluster(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		dockertest.EnvtestMissing(t, "the admin watch envtest")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	scheme, err := operator.NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		t.Fatal(err)
	}
	n := NewNotifier()
	if err := RegisterWatches(mgr, n, "a"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache sync")
	}
	events, unsub := n.Subscribe()
	defer unsub()
	cl := mgr.GetClient()

	drain := func() {
		for {
			select {
			case <-events:
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}
	// A notification is asynchronous, so proving one did NOT happen means
	// waiting long enough that it would have. Everything here notifies in
	// well under a second when it notifies at all.
	quiet := func(t *testing.T, what string) {
		t.Helper()
		select {
		case <-events:
			t.Fatalf("a change to %s reached an admin scoped to cluster a", what)
		case <-time.After(3 * time.Second):
		}
	}
	notified := func(t *testing.T, what string) {
		t.Helper()
		select {
		case <-events:
		case <-time.After(10 * time.Second):
			t.Fatalf("no notification after %s", what)
		}
	}
	labels := func(cluster string) map[string]string {
		return map[string]string{operator.LabelCluster: cluster}
	}

	// Another cluster's objects, one kind at a time.
	drain()
	for _, obj := range []client.Object{
		&pgshardv1alpha1.PgShardGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "b-shard-0", Namespace: "default", Labels: labels("b")},
			Spec:       pgshardv1alpha1.PgShardGroupSpec{ClusterRef: "b", Kind: "shard"}},
		&pgshardv1alpha1.PgShardBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "b-backup", Namespace: "default"},
			Spec:       pgshardv1alpha1.PgShardBackupSpec{ClusterName: "b", Type: "full"}},
		&pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: "b-reshard", Namespace: "default"},
			Spec: pgshardv1alpha1.PgShardReshardSpec{ClusterName: "b", FromGeneration: 1, TargetGeneration: 2,
				TargetShardSet: "g2", TargetShards: 2,
				TargetRanges: []pgshardv1alpha1.ReshardRange{
					{ShardID: 0, RangeStart: -9223372036854775808, RangeEnd: 0},
					{ShardID: 1, RangeStart: 0, RangeEnd: 9223372036854775807},
				}}},
	} {
		if err := cl.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
		quiet(t, obj.GetName())
	}

	// This cluster's own, every kind it watches.
	for _, obj := range []client.Object{
		&pgshardv1alpha1.PgShardGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "a-shard-0", Namespace: "default", Labels: labels("a")},
			Spec:       pgshardv1alpha1.PgShardGroupSpec{ClusterRef: "a", Kind: "shard"}},
		&pgshardv1alpha1.PgShardBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "a-backup", Namespace: "default"},
			Spec:       pgshardv1alpha1.PgShardBackupSpec{ClusterName: "a", Type: "full"}},
		&pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: "a-reshard", Namespace: "default"},
			Spec: pgshardv1alpha1.PgShardReshardSpec{ClusterName: "a", FromGeneration: 1, TargetGeneration: 2,
				TargetShardSet: "g2", TargetShards: 2,
				TargetRanges: []pgshardv1alpha1.ReshardRange{
					{ShardID: 0, RangeStart: -9223372036854775808, RangeEnd: 0},
					{ShardID: 1, RangeStart: 0, RangeEnd: 9223372036854775807},
				}}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "a-shard-0-0", Namespace: "default", Labels: labels("a")},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres"}}}},
	} {
		drain()
		if err := cl.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
		notified(t, obj.GetName())
	}

	// A restore that builds cluster a names b as its source, so placing it
	// by source alone would hide the thing a's admin most wants to watch.
	drain()
	restore := &pgshardv1alpha1.PgShardRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "b-into-a", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "b", NewClusterName: "a"},
	}
	if err := cl.Create(ctx, restore); err != nil {
		t.Fatalf("create restore: %v", err)
	}
	notified(t, "a restore creating cluster a")
}
