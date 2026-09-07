package controller

import (
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
