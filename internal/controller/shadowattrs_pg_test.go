package controller

import (
	"strings"
	"testing"
	"time"
)

// TestAMoveKeepsWhatLikeIncludingAllDrops: the remote shadow is built by
// hand, statement by statement, so every attribute of a table is carried
// only if something renders it. Four of them are rendered and none was
// proven to survive a move, which is the same position the exclusion
// constraint was in before it turned out not to be carried at all.
//
// Each of these is silent when it goes missing. A table comes back from a
// move with its rows intact and its fillfactor, its column comments, its
// column storage and its extended statistics quietly reset to the
// defaults, and nothing fails until somebody wonders why the planner got
// worse or why a column's documentation vanished.
func TestAMoveKeepsWhatLikeIncludingAllDrops(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	src := f.app(0)
	mustExec(t, src, `CREATE TABLE notes (
		id bigint PRIMARY KEY,
		body text NOT NULL,
		tag text NOT NULL,
		region text NOT NULL) WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.05)`)
	mustExec(t, src, `COMMENT ON COLUMN notes.body IS 'what the note says'`)
	mustExec(t, src, `ALTER TABLE notes ALTER COLUMN body SET STORAGE EXTERNAL`)
	mustExec(t, src, `ALTER TABLE notes ALTER COLUMN tag SET COMPRESSION lz4`)
	mustExec(t, src, `CREATE STATISTICS notes_tag_region ON tag, region FROM notes`)
	mustExec(t, src, `INSERT INTO notes SELECT g, 'b' || g, 't' || (g % 3), 'r' || (g % 5) FROM generate_series(1, 50) g`)

	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'notes', 'unsharded')`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference' WHERE table_name = 'notes'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("notes", 2*time.Minute, StageCompleted)

	for id := range int32(2) {
		c := f.app(id)
		opts := queryOne[string](t, c, `SELECT coalesce(array_to_string(reloptions, ','), '') FROM pg_class WHERE oid = 'public.notes'::regclass`)
		for _, want := range []string{"fillfactor=70", "autovacuum_vacuum_scale_factor=0.05"} {
			if !strings.Contains(opts, want) {
				t.Errorf("shard %d: reloptions %q lost %q", id, opts, want)
			}
		}
		if got := queryOne[string](t, c, `SELECT coalesce(col_description('public.notes'::regclass, attnum), '') FROM pg_attribute WHERE attrelid = 'public.notes'::regclass AND attname = 'body'`); got != "what the note says" {
			t.Errorf("shard %d: column comment = %q", id, got)
		}
		if got := queryOne[string](t, c, `SELECT attstorage::text FROM pg_attribute WHERE attrelid = 'public.notes'::regclass AND attname = 'body'`); got != "e" {
			t.Errorf("shard %d: body storage = %q, want e (EXTERNAL)", id, got)
		}
		if got := queryOne[string](t, c, `SELECT attcompression::text FROM pg_attribute WHERE attrelid = 'public.notes'::regclass AND attname = 'tag'`); got != "l" {
			t.Errorf("shard %d: tag compression = %q, want l (lz4)", id, got)
		}
		if n := queryOne[int64](t, c, `SELECT count(*) FROM pg_statistic_ext WHERE stxrelid = 'public.notes'::regclass`); n != 1 {
			t.Errorf("shard %d: %d extended statistics objects, want the one the table had", id, n)
		}
		if n := queryOne[int64](t, c, `SELECT count(*) FROM notes`); n != 50 {
			t.Errorf("shard %d: %d rows", id, n)
		}
	}
}
