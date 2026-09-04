package workload

import (
	"context"
	"fmt"

	"github.com/andrew01234567890/pgshard/test/e2e/oracle"
)

// Counted is what one stream's rows look like across every group that
// holds the ledger table: how many rows with id <= High exist, and how
// many distinct ids among them.
//
// Both numbers are needed and neither alone is enough. Distinct below the
// high-water is a LOST acknowledged commit. Rows above distinct is the
// same id in two places, which a per-owner check cannot see: uniqueness is
// enforced per shard, so a copy that landed on the right shard and a wrong
// one looks correct from the owner.
type Counted struct {
	Rows     int64
	Distinct int64
	High     int64
}

// Counter reports Counted for one stream, sweeping every group that holds
// the table rather than only the one the stream routes to now.
type Counter func(ctx context.Context, stream, high int64) (Counted, error)

// Verify asserts the ledger invariant against acknowledged high-water
// marks: every acknowledged row exists, exactly once.
//
// Rows the workload never had an answer for are deliberately not checked.
// A statement killed in flight may or may not have committed, and a test
// that demanded either answer would be asserting something the system
// never promised.
func Verify(ctx context.Context, streams []int64, acked []int64, count Counter) ([]oracle.Violation, error) {
	if len(streams) != len(acked) {
		return nil, fmt.Errorf("workload: %d streams but %d acked marks", len(streams), len(acked))
	}
	var vs []oracle.Violation
	for i, stream := range streams {
		high := acked[i]
		if high == 0 {
			vs = append(vs, oracle.Violation{
				Oracle: "acked-ledger",
				Detail: fmt.Sprintf("stream %d acknowledged nothing, so the run proves nothing about it", stream),
			})
			continue
		}
		got, err := count(ctx, stream, high)
		if err != nil {
			return nil, fmt.Errorf("stream %d: %w", stream, err)
		}
		if got.Distinct < high {
			vs = append(vs, oracle.Violation{
				Oracle: "acked-ledger",
				Detail: fmt.Sprintf("stream %d: %d of %d acknowledged rows survive; %d acknowledged commits were lost",
					stream, got.Distinct, high, high-got.Distinct),
			})
		}
		if got.Rows > got.Distinct {
			vs = append(vs, oracle.Violation{
				Oracle: "acked-ledger",
				Detail: fmt.Sprintf("stream %d: %d rows for %d distinct ids; %d rows exist in more than one place",
					stream, got.Rows, got.Distinct, got.Rows-got.Distinct),
			})
		}
	}
	return vs, nil
}
