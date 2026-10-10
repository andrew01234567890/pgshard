//go:build integration

package router

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// PGS-985: over the extended protocol a COPY FROM STDIN used to hang,
// because PostgreSQL swallows the batch's Sync in copy-in mode. It is
// refused by name, quickly, and the simple-protocol COPY still loads.
func TestACopyFromStdinOverTheExtendedProtocolIsRefusedOnRealPostgreSQL(t *testing.T) {
	s := startStack(t)
	conn := s.connect(t)
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "create table ext_copy (id int)", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	qctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	start := time.Now()
	rr := conn.PgConn().ExecParams(qctx, "copy ext_copy from stdin", nil, nil, nil, nil).Read()
	if rr.Err == nil || !strings.Contains(rr.Err.Error(), "available only as a simple query") {
		t.Fatalf("err = %v after %s", rr.Err, time.Since(start))
	}
	tag, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("1\n2\n3\n"), "copy ext_copy from stdin")
	if err != nil || tag.RowsAffected() != 3 {
		t.Fatalf("simple COPY: %q %v", tag, err)
	}
}
