package controller

import (
	"context"
	"fmt"
	"testing"
)

// The coalescing throughput used to be guarded by a RATIO asserted in the
// blocking test suite, which a hosted runner does not reproduce: the two
// arms are not equally affected there -- per-row is bound by round trips a
// shared runner does fine, coalescing by work it does badly -- so the ratio
// came out 1.3x on CI against 3.3x locally on the same commit, and blocked
// pull requests that could not have touched the applier (PGS-763).
//
// A ratio that fails half the time gets worked around, which is worse than
// no guard. These benchmarks are the guard that can be trusted instead:
// benchstat compares like with like on one machine across runs, so a real
// regression shows as a change in the trend rather than as a threshold
// somebody has to calibrate for the worst runner they have.
//
// They are in this package, beside what they measure, and this package
// needs Docker at TestMain -- which is why the PR benchstat gate does not
// list it (hack/perf/benchstat.sh) and the nightly perf job does.

// benchApply times one apply of rateRows operations, built by ops.
func benchApply(b *testing.B, name string, ops func(table string) []applyOp) {
	raw := connect(b, startPostgres(b))
	conn := pgxShardConn{raw}
	pass := 0
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		table := fmt.Sprintf("%s_bench_%d", name, pass)
		pass++
		mustExec(b, raw, `CREATE TABLE `+table+` (id bigint PRIMARY KEY, tenant_id bigint, note text)`)
		work := ops(table)
		b.StartTimer()
		if err := applyOps(context.Background(), targetConns{0: conn}, rateShape(table), table, work); err != nil {
			b.Fatal(err)
		}
	}
	// Operations a second is the number PGS-355 asked for, and the number a
	// human reads; ns/op is what benchstat compares.
	b.ReportMetric(float64(rateRows)*float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

// BenchmarkCatchUpApplyPerRow is catch-up as it was: one statement per row.
func BenchmarkCatchUpApplyPerRow(b *testing.B) {
	benchApply(b, "per_row", func(table string) []applyOp {
		ops := make([]applyOp, 0, rateRows)
		for i := range rateRows {
			ops = append(ops, applyOp{shard: 0, sql: rateShape(table).UpsertSQL(table, []*Tuple{rateRow(i)})[0]})
		}
		return ops
	})
}

// BenchmarkCatchUpApplyCoalesced is the same rows applied as multi-row
// statements. The gap between the two is what coalescing buys.
func BenchmarkCatchUpApplyCoalesced(b *testing.B) {
	benchApply(b, "coalesced", func(string) []applyOp {
		ops := make([]applyOp, 0, rateRows)
		for i := range rateRows {
			ops = append(ops, applyOp{shard: 0, up: rateRow(i)})
		}
		return ops
	})
}
