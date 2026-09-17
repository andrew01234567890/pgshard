package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// A pending shard is never guarded by the existence check, so an object
// created out of band is a hard failure rather than a silent success. One
// refused dial per shard used to defeat that: resumed was set after every
// attempt, including one that never reached the server, so the second
// attempt took the guard, found the object somebody else had made, and
// reported applied without running anything.
func TestARefusedDialDoesNotPutAPendingShardBehindTheResumeGuard(t *testing.T) {
	f := newApplierFixture(t)
	down := map[int32]bool{0: true, 1: true, 2: true}
	f.shards.dialErr = func(shard int32) error {
		if down[shard] {
			down[shard] = false
			return errors.New("connection refused")
		}
		return nil
	}
	// The table is there, but pgshard did not put it there -- which is what
	// the shard says when the statement finally runs.
	f.shards.exists = func(_ int32, kind, name string) bool { return kind == "relation" && name == "t" }
	f.shards.exec = func(_ int32, sql string) error {
		if strings.Contains(sql, "create table") {
			return pgErr("42P07", `relation "t" already exists`)
		}
		return nil
	}
	id := f.queue(catalog.DDLMigration{Statement: "create table t (id int)", Kind: "CREATE TABLE", Scope: "all",
		Meta: catalog.MigrationMeta{Object: catalog.MigrationObject{Kind: "relation", Name: "t", Expect: "present"}}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State == catalog.MigrationComplete {
		t.Fatalf("migration %s (%s); a table created out of band was reported as this migration's work", m.State, states(m))
	}
	if s := strings.Join(f.shards.statements(0), " "); !strings.Contains(s, "create table") {
		t.Errorf("shard 0 never ran the statement: %v", s)
	}
}

// A lock timeout is the same: the statement aborted, so the next attempt is
// not a resume either.
func TestALockTimeoutDoesNotPutAPendingShardBehindTheResumeGuard(t *testing.T) {
	f := newApplierFixture(t)
	timedOut := map[int32]bool{0: true, 1: true, 2: true}
	f.shards.exec = func(shard int32, sql string) error {
		if !strings.Contains(sql, "create table") {
			return nil
		}
		if timedOut[shard] {
			timedOut[shard] = false
			return pgErr("55P03", "could not obtain lock")
		}
		return pgErr("42P07", `relation "t" already exists`)
	}
	f.shards.exists = func(_ int32, kind, name string) bool { return kind == "relation" && name == "t" }
	id := f.queue(catalog.DDLMigration{Statement: "create table t (id int)", Kind: "CREATE TABLE", Scope: "all",
		Meta: catalog.MigrationMeta{Object: catalog.MigrationObject{Kind: "relation", Name: "t", Expect: "present"}}})
	f.run(t)
	if m := f.store.get(t, id); m.State == catalog.MigrationComplete {
		t.Fatalf("migration %s (%s); a lock timeout made the retry a resume", m.State, states(m))
	}
}

// The same question across a process boundary. retrying is saved after any
// transient failure, so a shard whose dial was refused and whose controller
// then died came back with a row saying retrying -- and the next pass read
// that as "an attempt may have committed", took the guard, and reported
// applied without ever sending the statement. Whether the attempt reached
// the server is recorded on the row, because the state alone cannot say.
func TestAPersistedRetryingRowRemembersWhetherTheAttemptRan(t *testing.T) {
	for _, c := range []struct {
		name string
		ran  bool
		want bool // migration completes
	}{
		{"a dial that never reached the server", false, false},
		{"an attempt interrupted mid-statement", true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newApplierFixture(t)
			f.shards.exists = func(_ int32, kind, name string) bool { return kind == "relation" && name == "t" }
			f.shards.exec = func(_ int32, sql string) error {
				if strings.Contains(sql, "create table") {
					return pgErr("42P07", `relation "t" already exists`)
				}
				return nil
			}
			id := f.queue(catalog.DDLMigration{Statement: "create table t (id int)", Kind: "CREATE TABLE", Scope: "all",
				State: catalog.MigrationRunning,
				Meta:  catalog.MigrationMeta{Object: catalog.MigrationObject{Kind: "relation", Name: "t", Expect: "present"}},
				PerShard: map[string]catalog.ShardMigration{
					"0": {State: catalog.ShardApplied, Attempts: 1},
					"1": {State: catalog.ShardRetrying, Attempts: 1, Ran: c.ran, Error: "connection refused"},
					"2": {State: catalog.ShardApplied, Attempts: 1},
				}})
			f.run(t)
			m := f.store.get(t, id)
			if complete := m.State == catalog.MigrationComplete; complete != c.want {
				t.Fatalf("migration %s (%s), want complete=%v", m.State, states(m), c.want)
			}
		})
	}
}

// CREATE INDEX CONCURRENTLY does not run in one transaction: a lock timeout
// leaves an INVALID index behind, so the next attempt has to know to drop it
// first. Treating that timeout as "nothing happened" let a retry of the
// IF NOT EXISTS form succeed over the invalid index and report the shard
// applied with an index that answers no query.
func TestAConcurrentStatementAlwaysCountsAsHavingRun(t *testing.T) {
	m := &catalog.DDLMigration{Kind: "CREATE INDEX", Strategy: "concurrent"}
	plain := &catalog.DDLMigration{Kind: "CREATE INDEX"}
	for _, code := range []string{"55P03", "40P01", "40001"} {
		if !mayHaveRun(m, pgErr(code, "interrupted")) {
			t.Errorf("%s on a concurrent index build counts as not run, so the invalid index it leaves is never dropped", code)
		}
		if mayHaveRun(plain, pgErr(code, "interrupted")) {
			t.Errorf("%s inside one transaction counts as having run", code)
		}
	}
	// A refused dial never reached the server whatever the strategy.
	if mayHaveRun(m, &dialError{errors.New("connection refused")}) {
		t.Error("a refused dial counts as having run")
	}
}

// And the row has to be written, not only read: a shard that gives up
// carries what its attempts reached, so the pass that picks the migration up
// afterwards asks the right question.
func TestTheRowRecordsWhetherTheAttemptsReachedTheServer(t *testing.T) {
	t.Run("a refused dial reached nothing", func(t *testing.T) {
		f := newApplierFixture(t)
		f.shards.dialErr = func(shard int32) error {
			if shard == 0 {
				return errors.New("connection refused")
			}
			return nil
		}
		id := f.queue(catalog.DDLMigration{Statement: "alter table t add column x int", Kind: "ALTER TABLE", Scope: "all"})
		f.run(t)
		m := f.store.get(t, id)
		if got := m.PerShard["0"]; got.State != catalog.ShardFailed || got.Ran {
			t.Fatalf("shard 0 = %+v, want failed with ran=false", got)
		}
	})
	t.Run("a lock timeout reached the server but committed nothing", func(t *testing.T) {
		f := newApplierFixture(t)
		f.shards.exec = func(shard int32, _ string) error {
			if shard == 0 {
				return pgErr("55P03", "canceling statement due to lock timeout")
			}
			return nil
		}
		id := f.queue(catalog.DDLMigration{Statement: "alter table t add column x int", Kind: "ALTER TABLE", Scope: "all"})
		f.run(t)
		m := f.store.get(t, id)
		if got := m.PerShard["0"]; got.State != catalog.ShardFailed || got.Ran {
			t.Fatalf("shard 0 = %+v, want failed with ran=false", got)
		}
	})
	t.Run("a lost connection may have committed", func(t *testing.T) {
		f := newApplierFixture(t)
		f.shards.exec = func(shard int32, _ string) error {
			if shard == 0 {
				return pgErr("08006", "connection failure")
			}
			return nil
		}
		id := f.queue(catalog.DDLMigration{Statement: "alter table t add column x int", Kind: "ALTER TABLE", Scope: "all"})
		f.run(t)
		m := f.store.get(t, id)
		if got := m.PerShard["0"]; got.State != catalog.ShardFailed || !got.Ran {
			t.Fatalf("shard 0 = %+v, want failed with ran=true", got)
		}
	})
}

// TestAFailedIndexBuildDropsOnlyItsOwnIndex (PGS-890): when CREATE INDEX
// CONCURRENTLY fails it leaves an invalid index behind, and the applier
// drops that one and tries again. It looked for an invalid index of that
// name anywhere on the search path and then dropped the name UNQUALIFIED --
// which PostgreSQL resolves by search_path, to the FIRST schema. With a
// valid index of the same name earlier on the path, the applier destroyed a
// user's index on another table and the retry then failed 42P07 on the one
// it had meant to drop.
//
// Against real PostgreSQL because the bug IS the name resolution: a fake
// that is handed a name and asked for an answer cannot have it.
func TestAFailedIndexBuildDropsOnlyItsOwnIndex(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	raw := connect(t, startPostgres(t))
	conn := pgxShardConn{raw}
	mustExec(t, raw, `CREATE SCHEMA app`)
	mustExec(t, raw, `SET search_path = app, public`)
	// The one that must survive: valid, on another table, earlier on the path.
	mustExec(t, raw, `CREATE TABLE app.other (y int)`)
	mustExec(t, raw, `CREATE INDEX idx ON app.other (y)`)
	// The wreckage: a unique build over duplicate rows leaves public.idx invalid.
	mustExec(t, raw, `CREATE TABLE public.t (x int)`)
	mustExec(t, raw, `INSERT INTO public.t VALUES (1), (1)`)
	if _, err := raw.Exec(ctx, `CREATE UNIQUE INDEX CONCURRENTLY idx ON public.t (x)`); err == nil {
		t.Fatal("the unique build over duplicate rows was expected to fail")
	}
	invalidBefore := queryOne[int64](t, raw, `SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'idx' AND NOT i.indisvalid`)
	if invalidBefore != 1 {
		t.Fatalf("the premise is an invalid public.idx; found %d", invalidBefore)
	}

	// The object carries no schema of its own, which is the common case.
	dropped, err := dropInvalidIndex(ctx, conn, catalog.MigrationObject{Kind: "relation", Name: "idx"})
	if err != nil || !dropped {
		t.Fatalf("dropInvalidIndex = %v, %v; want it to drop the invalid one", dropped, err)
	}
	if n := queryOne[int64](t, raw, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relname = 'idx'`); n != 0 {
		t.Errorf("the invalid public.idx survived its own cleanup")
	}
	if n := queryOne[int64](t, raw, `SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app' AND c.relname = 'idx' AND i.indisvalid`); n != 1 {
		t.Error("the valid app.idx on another table was dropped: an unqualified DROP resolved to it")
	}
}

// TestAPartitionedParentIndexIsNotWreckage (PGS-891): CREATE INDEX ... ON
// ONLY a partitioned table leaves the parent index invalid BY DESIGN until
// every partition's index is attached. A resumed migration read that as the
// wreckage of a failed build and ran DROP INDEX CONCURRENTLY, which
// PostgreSQL refuses for a partitioned index with 0A000 -- not transient, so
// the shard failed for good.
func TestAPartitionedParentIndexIsNotWreckage(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	raw := connect(t, startPostgres(t))
	conn := pgxShardConn{raw}
	mustExec(t, raw, `CREATE TABLE parted (id int, v text) PARTITION BY RANGE (id)`)
	mustExec(t, raw, `CREATE TABLE parted_1 PARTITION OF parted FOR VALUES FROM (0) TO (100)`)
	mustExec(t, raw, `CREATE INDEX parted_v ON ONLY parted (v)`)
	if valid := queryOne[bool](t, raw, `SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = 'parted_v'`); valid {
		t.Skip("this PostgreSQL marks an ON ONLY parent index valid; the premise does not hold")
	}
	dropped, err := dropInvalidIndex(ctx, conn, catalog.MigrationObject{Kind: "relation", Name: "parted_v"})
	if err != nil {
		t.Fatalf("a partitioned parent index was treated as wreckage: %v", err)
	}
	if dropped {
		t.Error("the partitioned parent index was dropped; it is invalid by design until its partitions attach")
	}
	if n := queryOne[int64](t, raw, `SELECT count(*) FROM pg_class WHERE relname = 'parted_v'`); n != 1 {
		t.Error("the partitioned parent index is gone")
	}
}

// TestAFailedIndexBuildDropsTheWreckageOnItsOwnTable (PGS-941): two invalid
// indexes can share a name -- one per table -- and the cleanup picked between
// them by SEARCH PATH POSITION. A failed build on a table in a later schema
// had the earlier schema's invalid index dropped instead, so its own wreckage
// survived and the retry failed 42P07 on it. PGS-890 stopped this destroying
// a VALID index; the invalid one it still got wrong.
//
// Against real PostgreSQL because the bug IS which row the catalog scan
// returns.
func TestAFailedIndexBuildDropsTheWreckageOnItsOwnTable(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	raw := connect(t, startPostgres(t))
	conn := pgxShardConn{raw}
	mustExec(t, raw, `CREATE SCHEMA app`)
	mustExec(t, raw, `SET search_path = app, public`)
	// Wreckage of someone else's failed build, EARLIER on the path.
	mustExec(t, raw, `CREATE TABLE app.dup (y int)`)
	mustExec(t, raw, `INSERT INTO app.dup VALUES (1), (1)`)
	if _, err := raw.Exec(ctx, `CREATE UNIQUE INDEX CONCURRENTLY idx ON app.dup (y)`); err == nil {
		t.Fatal("the unique build over duplicate rows was expected to fail")
	}
	// Our own failed build, on a table LATER on the path.
	mustExec(t, raw, `CREATE TABLE public.t (x int)`)
	mustExec(t, raw, `INSERT INTO public.t VALUES (1), (1)`)
	if _, err := raw.Exec(ctx, `CREATE UNIQUE INDEX CONCURRENTLY idx ON public.t (x)`); err == nil {
		t.Fatal("the unique build over duplicate rows was expected to fail")
	}
	invalid := func(schema string) int64 {
		return queryOne[int64](t, raw, `SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = 'idx' AND NOT i.indisvalid`, schema)
	}
	if invalid("app") != 1 || invalid("public") != 1 {
		t.Fatalf("the premise is one invalid idx in each schema; app=%d public=%d", invalid("app"), invalid("public"))
	}

	// The statement named the table unqualified, as a client on this path
	// would, so the object carries the table and no schema.
	dropped, err := dropInvalidIndex(ctx, conn, catalog.MigrationObject{Kind: "relation", Name: "idx", Table: "t"})
	if err != nil || !dropped {
		t.Fatalf("dropInvalidIndex = %v, %v; want it to drop this build's wreckage", dropped, err)
	}
	if invalid("public") != 0 {
		t.Error("the invalid public.idx -- this build's own wreckage -- survived; the retry will fail 42P07 on it")
	}
	if invalid("app") != 1 {
		t.Error("the invalid app.idx on another table was dropped; it was picked by path position, not by table")
	}
}
