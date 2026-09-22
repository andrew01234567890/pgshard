//go:build integration

package router

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PGS-977: a BEGIN with modes after other statements of a batch is applied
// to the adopted transaction by the shard itself, which answers exactly as
// one PostgreSQL does.
func TestABatchBeginWithModesBehavesAsPostgreSQL(t *testing.T) {
	s := startStack(t)
	conn := s.connect(t)
	ctx := context.Background()
	res, err := conn.PgConn().Exec(ctx, "select 1; begin read only; select current_setting('transaction_read_only')").ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	last := res[len(res)-1]
	if len(last.Rows) != 1 {
		t.Fatalf("last result %+v", last)
	}
	if ro := string(last.Rows[0][0]); ro != "on" {
		t.Fatalf("transaction_read_only = %q, want on", ro)
	}
	if st := conn.PgConn().TxStatus(); st != 'T' {
		t.Fatalf("status %c, want the adopted transaction open", st)
	}
	if _, err := conn.Exec(ctx, "rollback", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"select 1; begin isolation level serializable", "select 1; begin deferrable"} {
		_, err := conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "25001" {
			t.Fatalf("%s: err = %v, want PostgreSQL's 25001", sql, err)
		}
		if st := conn.PgConn().TxStatus(); st != 'E' {
			t.Fatalf("%s: status %c, want the adopted transaction failed", sql, st)
		}
		if _, err := conn.Exec(ctx, "rollback", pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatal(err)
		}
	}
}
