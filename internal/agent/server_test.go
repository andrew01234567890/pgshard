package agent

import (
	"errors"
	"fmt"
	"testing"
)

func TestPgErrMapsStaleEpochToSQLState55000(t *testing.T) {
	if pgErr(nil) != nil {
		t.Fatal("nil error must map to nil")
	}
	e := pgErr(fmt.Errorf("wrap: %w", ErrStaleEpoch))
	if e.GetSqlstate() != "55000" || e.GetMessage() != "wrap: stale epoch" {
		t.Fatalf("stale: %v", e)
	}
	if e := pgErr(errors.New("boom")); e.GetSqlstate() != "XX000" || e.GetMessage() != "boom" {
		t.Fatalf("other: %v", e)
	}
}
