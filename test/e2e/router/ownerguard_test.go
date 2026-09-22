//go:build integration

package router

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestAKeyRewritingTriggerCannotMisplaceARowThroughTheRouter is PGS-878's
// acceptance, end to end: a router in front of two shards, and the
// controller that guards them.
//
// The router routes an INSERT by the shard key in the statement. A BEFORE
// trigger that assigns the key then wrote the row, with a key belonging to
// the other shard, on the shard the router chose -- where no lookup by that
// key would ever reach it. Now the write is refused, and no row lands on
// either shard.
func TestAKeyRewritingTriggerCannotMisplaceARowThroughTheRouter(t *testing.T) {
	s := startShardedStack(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitSharded(t, conn)

	cat, err := pgx.Connect(ctx, s.catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.Close(ctx) }()
	deadline := time.Now().Add(90 * time.Second)
	for {
		var guarded bool
		if err := cat.QueryRow(ctx, `SELECT coalesce(owner_guard_generation = effective_generation, false) FROM pgshard.table_status WHERE table_name = 'orders'`).Scan(&guarded); err != nil {
			t.Fatal(err)
		}
		if guarded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the controller never guarded orders\ncontroller log:\n%s", s.controllerLog.String())
		}
		time.Sleep(500 * time.Millisecond)
	}

	home, away := int64(-1), int64(-1)
	for k := int64(1); k < 1000 && (home < 0 || away < 0); k++ {
		switch {
		case shardOf(t, k) == 0 && home < 0:
			home = k
		case shardOf(t, k) == 1 && away < 0:
			away = k
		}
	}

	// A correctly placed row is written as ever.
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", home); err != nil {
		t.Fatalf("a row with its own key, through the router: %v", err)
	}

	// The trigger a user might write, on the shard the router will choose.
	shard0, err := pgx.Connect(ctx, strings.Replace(s.shardDSN, "/postgres?", "/"+appDatabase+"?", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = shard0.Close(ctx) }()
	for _, sql := range []string{
		`CREATE FUNCTION rekey() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.tenant_id := ` + strconv.FormatInt(away, 10) + `; RETURN NEW; END $$`,
		`CREATE TRIGGER zzz_rekey BEFORE INSERT ON orders FOR EACH ROW EXECUTE FUNCTION rekey()`,
	} {
		if _, err := shard0.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	_, err = conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", home)
	if sqlstate(err) != "23514" {
		t.Fatalf("an INSERT whose BEFORE trigger rewrote the key to another shard's: %v, want 23514", err)
	}
	for shard := range 2 {
		if n := s.rowsOn(t, shard, away); n != 0 {
			t.Fatalf("shard %d holds %d row(s) of tenant %d, which the trigger wrote", shard, n, away)
		}
	}
}
