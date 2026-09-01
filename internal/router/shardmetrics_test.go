package router

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

// TestAHotShardCanBeSeen: per-shard latency alone cannot tell a shard that
// is busy from one that is slow, because both show as time. Counting the
// work as well as timing it is what separates a hot shard from slow
// storage, which is the question an operator has to answer before choosing
// a reshard boundary.
func TestAHotShardCanBeSeen(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	label := "default/0"

	before := counterValue(t, h.r.metrics.ShardStatements.WithLabelValues(label))
	rowsBefore := counterValue(t, h.r.metrics.ShardRows.WithLabelValues(label))
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if got := counterValue(t, h.r.metrics.ShardStatements.WithLabelValues(label)); got <= before {
		t.Fatalf("statements %v, want more than %v", got, before)
	}
	if got := counterValue(t, h.r.metrics.ShardRows.WithLabelValues(label)); got <= rowsBefore {
		t.Fatalf("rows %v, want more than %v: a shard returning many rows is how skew shows", got, rowsBefore)
	}

	// An error is counted apart, so a shard failing fast is not mistaken
	// for one doing work.
	errBefore := counterValue(t, h.r.metrics.ShardErrors.WithLabelValues(label))
	_, _ = conn.Exec(ctx, "select boom")
	if got := counterValue(t, h.r.metrics.ShardErrors.WithLabelValues(label)); got <= errBefore {
		t.Fatalf("errors %v, want more than %v", got, errBefore)
	}
}
