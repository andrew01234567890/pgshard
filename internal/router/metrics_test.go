package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

func TestMetricsHandlerServesKeySeries(t *testing.T) {
	r, err := New(Config{Snapshot: func() *snapshot.Snapshot { return nil },
		Poolers: NewPoolers(nil, func() *snapshot.Snapshot { return nil }, nil)})
	if err != nil {
		t.Fatal(err)
	}
	r.metrics.Connections.Inc()
	r.metrics.TwoPCInDoubt.Inc()
	rec := httptest.NewRecorder()
	r.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"pgshard_router_active_sessions 0",
		"pgshard_router_connections_total 1",
		"pgshard_router_twopc_in_doubt_total 1",
		`pgshard_build_info{process="router"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in /metrics", want)
		}
	}
}
