package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

type applyRecorder struct {
	execs []string
}

func (r *applyRecorder) Exec(_ context.Context, sql string, _ ...any) (CommandTag, error) {
	r.execs = append(r.execs, sql)
	return nil, nil
}

func (r *applyRecorder) Query(context.Context, string, ...any) (pgx.Rows, error) { panic("unused") }
func (r *applyRecorder) Close(context.Context) error                             { return nil }

func TestApplyOpsCostsARoundTripPerTargetNotPerRow(t *testing.T) {
	one, two := &applyRecorder{}, &applyRecorder{}
	targets := targetConns{1: one, 2: two}
	var ops []applyOp
	for i := range 300 {
		ops = append(ops, applyOp{shard: 1, sql: "one" + itoa(int64(i))}, applyOp{shard: 2, sql: "two" + itoa(int64(i))})
	}
	if err := applyOps(context.Background(), targets, ops); err != nil {
		t.Fatal(err)
	}
	for name, r := range map[string]*applyRecorder{"1": one, "2": two} {
		if len(r.execs) != 1 {
			t.Fatalf("target %s took %d round trips for 300 operations, want 1", name, len(r.execs))
		}
		if !strings.HasPrefix(r.execs[0], "BEGIN;") || !strings.HasSuffix(r.execs[0], "COMMIT") {
			t.Fatalf("target %s did not apply its operations in a transaction: %.40s", name, r.execs[0])
		}
	}
	if got := strings.Count(one.execs[0], "one"); got != 300 {
		t.Fatalf("target 1 carried %d of its 300 operations", got)
	}
	if strings.Contains(one.execs[0], "two") {
		t.Fatal("target 1 was sent another target's operations")
	}
}

func TestApplyOpsKeepsEachTargetsOrder(t *testing.T) {
	one := &applyRecorder{}
	ops := []applyOp{{shard: 1, sql: "insert"}, {shard: 1, sql: "delete"}}
	if err := applyOps(context.Background(), targetConns{1: one}, ops); err != nil {
		t.Fatal(err)
	}
	if want := "BEGIN;insert;delete;COMMIT"; one.execs[0] != want {
		t.Fatalf("got %q, want %q", one.execs[0], want)
	}
}

func TestApplyOpsSplitsAStatementThatWouldGrowUnbounded(t *testing.T) {
	one := &applyRecorder{}
	var ops []applyOp
	for range applyBatchOps + 1 {
		ops = append(ops, applyOp{shard: 1, sql: "x"})
	}
	if err := applyOps(context.Background(), targetConns{1: one}, ops); err != nil {
		t.Fatal(err)
	}
	if len(one.execs) != 2 {
		t.Fatalf("%d operations went out in %d statements, want 2", len(ops), len(one.execs))
	}
}
