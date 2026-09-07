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
