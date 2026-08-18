// Package oracle defines correctness oracles evaluated by end-to-end and chaos tests.
package oracle

import (
	"context"
	"errors"
	"fmt"
)

// Violation describes one failed invariant.
type Violation struct {
	Oracle string
	Detail string
}

func (v Violation) Error() string { return v.Oracle + ": " + v.Detail }

// Oracle checks an invariant against the system under test.
type Oracle interface {
	Name() string
	Check(ctx context.Context) ([]Violation, error)
}

// Ledger verifies that a set of transfers between accounts preserves the total balance.
type Ledger struct {
	Expected int64
	Balances func(ctx context.Context) (map[string]int64, error)
}

// Name implements Oracle.
func (l *Ledger) Name() string { return "ledger" }

// Check implements Oracle.
func (l *Ledger) Check(ctx context.Context) ([]Violation, error) {
	if l.Balances == nil {
		return nil, errors.New("ledger: Balances not configured")
	}
	balances, err := l.Balances(ctx)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, b := range balances {
		total += b
	}
	if total != l.Expected {
		return []Violation{{Oracle: l.Name(), Detail: fmt.Sprintf("total %d != expected %d", total, l.Expected)}}, nil
	}
	return nil, nil
}

// RowSetEquality verifies that two sources report identical row sets.
type RowSetEquality struct {
	Left, Right func(ctx context.Context) (map[string]struct{}, error)
}

// Name implements Oracle.
func (r *RowSetEquality) Name() string { return "rowset-equality" }

// Check implements Oracle.
func (r *RowSetEquality) Check(ctx context.Context) ([]Violation, error) {
	if r.Left == nil || r.Right == nil {
		return nil, errors.New("rowset-equality: sources not configured")
	}
	left, err := r.Left(ctx)
	if err != nil {
		return nil, err
	}
	right, err := r.Right(ctx)
	if err != nil {
		return nil, err
	}
	var vs []Violation
	for k := range left {
		if _, ok := right[k]; !ok {
			vs = append(vs, Violation{Oracle: r.Name(), Detail: "missing on right: " + k})
		}
	}
	for k := range right {
		if _, ok := left[k]; !ok {
			vs = append(vs, Violation{Oracle: r.Name(), Detail: "missing on left: " + k})
		}
	}
	return vs, nil
}
