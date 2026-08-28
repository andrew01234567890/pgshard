package metrics

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func scrape(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	Handler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d", rec.Code)
	}
	return rec.Body.String()
}

func TestRegistryCarriesBuildInfo(t *testing.T) {
	body := scrape(t, NewRegistry("router"))
	if !strings.Contains(body, `pgshard_build_info{process="router"`) {
		t.Fatalf("missing build info:\n%s", body)
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Fatal("missing Go runtime collector")
	}
}

func TestRouterSetServesKeySeries(t *testing.T) {
	reg := NewRegistry("router")
	m := NewRouter(reg, func() float64 { return 3 }, func() float64 { return 12 })
	m.Connections.Inc()
	m.Queries.WithLabelValues("Single", "simple").Inc()
	m.Refusals.WithLabelValues("0A000").Add(2)
	m.TwoPCCommits.Inc()
	m.ScatterFanout.Observe(4)
	m.ShardLatency.WithLabelValues("default/0").Observe(0.01)
	m.CacheHit()
	m.CacheMiss()
	body := scrape(t, reg)
	for _, want := range []string{
		"pgshard_router_active_sessions 3",
		"pgshard_router_snapshot_age_seconds 12",
		"pgshard_router_connections_total 1",
		`pgshard_router_queries_total{kind="Single",opcode="simple"} 1`,
		`pgshard_router_refusals_total{sqlstate="0A000"} 2`,
		"pgshard_router_twopc_commits_total 1",
		"pgshard_router_plan_cache_hits_total 1",
		"pgshard_router_plan_cache_misses_total 1",
		"pgshard_router_scatter_fanout_count 1",
		`pgshard_router_shard_latency_seconds_count{shard="default/0"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestPoolerSetServesKeySeries(t *testing.T) {
	reg := NewRegistry("pooler")
	m := NewPooler(reg, func() float64 { return 5 }, func() float64 { return 2 }, func() float64 { return 7 })
	m.BackendDials.WithLabelValues("ok").Inc()
	m.PoolWaits.Inc()
	m.PreparedHits.Inc()
	m.StreamLagBytes.Set(42)
	body := scrape(t, reg)
	for _, want := range []string{
		"pgshard_pooler_backends_live 5",
		"pgshard_pooler_backends_idle 2",
		"pgshard_pooler_snapshot_age_seconds 7",
		`pgshard_pooler_backend_dials_total{result="ok"} 1`,
		"pgshard_pooler_pool_waits_total 1",
		"pgshard_pooler_prepared_cache_hits_total 1",
		"pgshard_pooler_stream_lag_bytes 42",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestAgentSetServesKeySeries(t *testing.T) {
	reg := NewRegistry("agent")
	m := NewAgent(reg, func() float64 { return 1 }, func() float64 { return 0 })
	m.FenceEvents.Inc()
	m.BackupLastAge.Set(120)
	m.SlotWALStatus.WithLabelValues("s1", "reserved").Set(1)
	body := scrape(t, reg)
	for _, want := range []string{
		"pgshard_agent_primary 1",
		"pgshard_agent_replication_lag_bytes 0",
		"pgshard_agent_isolation_fence_events_total 1",
		"pgshard_agent_backup_last_age_seconds 120",
		`pgshard_agent_slot_wal_status{slot="s1",wal_status="reserved"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestControllerSetServesKeySeries(t *testing.T) {
	reg := NewRegistry("controller")
	m := NewController(reg)
	m.Workflows.WithLabelValues("reshard", "running").Set(1)
	m.InDoubt.Set(2)
	m.InDoubtOldestAge.Set(30)
	m.Migrations.WithLabelValues("queued").Set(4)
	m.ResolvedTxns.WithLabelValues("committed").Inc()
	body := scrape(t, reg)
	for _, want := range []string{
		`pgshard_controller_workflows{kind="reshard",state="running"} 1`,
		"pgshard_controller_in_doubt_transactions 2",
		"pgshard_controller_in_doubt_oldest_age_seconds 30",
		`pgshard_controller_ddl_migrations{state="queued"} 4`,
		`pgshard_controller_resolved_transactions_total{outcome="committed"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestOperatorSetRegisters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOperator(reg)
	m.Failovers.Inc()
	m.RollingUpdates.WithLabelValues("ns/demo").Set(2)
	body := scrape(t, reg)
	for _, want := range []string{
		"pgshard_operator_failovers_total 1",
		`pgshard_operator_rolling_update_pending{cluster="ns/demo"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestServeExposesMetricsAndShutsDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reg := NewRegistry("test")
	done := make(chan error, 1)
	addr := "127.0.0.1:0"
	// Bind explicitly so the test can find the port.
	srv := httptest.NewServer(Handler(reg))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	go func() { done <- Serve(ctx, addr, reg) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Serve returned %v", err)
	}
}

// TestServeReportsAnAcceptFailureWithoutWaitingForShutdown: Serve waited
// unconditionally on a channel that only closed after ctx was cancelled, so a
// listener or accept failure could not be returned until shutdown. The
// controller and pooler run this in a background goroutine specifically to
// log such a failure, so the metrics endpoint stayed dead while the process
// looked healthy and the error sat in a goroutine nobody read.
func TestServeReportsAnAcceptFailureWithoutWaitingForShutdown(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// ctx is never cancelled: the point is that Serve returns anyway.
	done := make(chan error, 1)
	go func() { done <- serveOn(context.Background(), l, prometheus.NewRegistry()) }()

	// Take the listener away, as a closed socket or an accept failure would.
	time.Sleep(50 * time.Millisecond)
	_ = l.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a listener that went away was reported as a clean shutdown")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after its listener failed; it is waiting for a cancellation that may never come")
	}
}

// A cancelled context must still shut down cleanly and report nil.
func TestServeReturnsNilOnCancellation(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveOn(ctx, l, prometheus.NewRegistry()) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown reported %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancellation")
	}
}
