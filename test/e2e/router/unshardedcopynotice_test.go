//go:build integration

package router

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// PGS-983: an unsharded COPY into a table whose trigger raises a NOTICE per
// row. The backend writes the notices while the rows are still arriving;
// with nothing reading them the pooler's write, and then the router's,
// blocked for good.
func TestAnUnshardedCopyWhoseTriggerRaisesNoticesCompletes(t *testing.T) {
	s := startStack(t)
	ctx := context.Background()
	conn := s.connect(t)
	if _, err := conn.Exec(ctx, "create table noisy_load (id int primary key, pad text)", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	shard, err := pgx.Connect(ctx, strings.Replace(s.shardDSN, "/postgres?", "/"+appDatabase+"?", 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shard.Close(ctx) })
	if _, err := shard.Exec(ctx, `create function noisy_load_notice() returns trigger language plpgsql as $$
begin raise notice 'loaded % %', new.id, repeat('n', 65000); return new; end $$;
create trigger noisy_load_notice before insert on noisy_load for each row execute function noisy_load_notice()`); err != nil {
		t.Fatalf("install notice trigger: %v", err)
	}
	var in strings.Builder
	wide := strings.Repeat("w", 60000)
	for i := range 2000 {
		fmt.Fprintf(&in, "%d\t%s\n", i, wide)
	}
	done := make(chan error, 1)
	go func() {
		_, err := conn.PgConn().CopyFrom(ctx, strings.NewReader(in.String()), "copy noisy_load (id, pad) from stdin")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("COPY: %v", err)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("the unsharded COPY wedged")
	}
	var n int
	if err := conn.QueryRow(ctx, "select count(*) from noisy_load", pgx.QueryExecModeSimpleProtocol).Scan(&n); err != nil || n != 2000 {
		t.Fatalf("count %d %v", n, err)
	}
}
