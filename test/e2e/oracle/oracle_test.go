package oracle

import (
	"context"
	"testing"
)

func TestLedgerDetectsImbalance(t *testing.T) {
	l := &Ledger{Expected: 10, Balances: func(context.Context) (map[string]int64, error) {
		return map[string]int64{"a": 7, "b": 4}, nil
	}}
	vs, err := l.Check(context.Background())
	if err != nil || len(vs) != 1 {
		t.Fatalf("got %v, %v", vs, err)
	}
	l.Expected = 11
	if vs, _ = l.Check(context.Background()); len(vs) != 0 {
		t.Fatalf("balanced ledger reported %v", vs)
	}
}

func TestRowSetEquality(t *testing.T) {
	set := func(keys ...string) func(context.Context) (map[string]struct{}, error) {
		return func(context.Context) (map[string]struct{}, error) {
			m := map[string]struct{}{}
			for _, k := range keys {
				m[k] = struct{}{}
			}
			return m, nil
		}
	}
	r := &RowSetEquality{Left: set("1", "2"), Right: set("2", "3")}
	vs, err := r.Check(context.Background())
	if err != nil || len(vs) != 2 {
		t.Fatalf("got %v, %v", vs, err)
	}
	r.Right = set("1", "2")
	if vs, _ = r.Check(context.Background()); len(vs) != 0 {
		t.Fatalf("equal sets reported %v", vs)
	}
}
