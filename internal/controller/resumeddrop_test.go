package controller

import (
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// A DROP the planner cannot mark with an object -- a trigger, a policy, a
// rule, a type, a sequence, or a DROP TABLE naming several tables -- has no
// Meta.Object, so the resume check that skips work already done never runs
// for it. The statement replays, PostgreSQL answers 42704, and the shard is
// recorded as failed although it is in exactly the state the migration
// asked for.
//
// A crash between the shard's COMMIT and the catalog write is all it takes,
// and the migration then reports failure for a cluster that is correct.
func TestAResumedDropOfSomethingAlreadyGoneIsNotAFailure(t *testing.T) {
	f := newApplierFixture(t)
	f.shards.exec = func(shard int32, sql string) error {
		if shard == 1 && strings.Contains(sql, "drop trigger") {
			return pgErr("42704", `trigger "t" for relation "orders" does not exist`)
		}
		return nil
	}
	id := f.queue(catalog.DDLMigration{
		Statement: "drop trigger t on orders", Kind: "DROP TRIGGER", Scope: "all", State: catalog.MigrationRunning,
		PerShard: map[string]catalog.ShardMigration{
			"0": {State: catalog.ShardApplied, Attempts: 1},
			"1": {State: catalog.ShardRunning, Attempts: 1},
			"2": {State: catalog.ShardPending},
		}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationComplete {
		t.Fatalf("migration %s (%s); every shard is in the state it asked for", m.State, states(m))
	}
}

// A shard that had never started is a different matter: nothing dropped
// anything there, so a missing object is the shard disagreeing with the
// others and the migration must say so.
func TestADropThatFailsOnAShardThatNeverRanStillFails(t *testing.T) {
	f := newApplierFixture(t)
	f.shards.exec = func(shard int32, sql string) error {
		if shard == 2 && strings.Contains(sql, "drop trigger") {
			return pgErr("42704", `trigger "t" for relation "orders" does not exist`)
		}
		return nil
	}
	id := f.queue(catalog.DDLMigration{
		Statement: "drop trigger t on orders", Kind: "DROP TRIGGER", Scope: "all", State: catalog.MigrationRunning,
		PerShard: map[string]catalog.ShardMigration{
			"0": {State: catalog.ShardApplied, Attempts: 1},
			"1": {State: catalog.ShardApplied, Attempts: 1},
			"2": {State: catalog.ShardPending},
		}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State == catalog.MigrationComplete {
		t.Fatalf("migration %s (%s); a shard that never ran the drop reported the object missing", m.State, states(m))
	}
}

// The forgiveness is for drops alone. A resumed statement that is not
// removing anything and finds its object missing has found a real problem:
// a table that is not on this shard is not the state an ALTER asked for,
// and marking it applied would report a migration complete that changed
// nothing there.
func TestAResumedStatementThatIsNotADropStillFailsOnAMissingObject(t *testing.T) {
	f := newApplierFixture(t)
	f.shards.exec = func(shard int32, sql string) error {
		if shard == 1 && strings.Contains(sql, "alter table") {
			return pgErr("42P01", `relation "orders" does not exist`)
		}
		return nil
	}
	id := f.queue(catalog.DDLMigration{
		Statement: "alter table orders add column note text", Kind: "ALTER TABLE", Scope: "all", State: catalog.MigrationRunning,
		PerShard: map[string]catalog.ShardMigration{
			"0": {State: catalog.ShardApplied, Attempts: 1},
			"1": {State: catalog.ShardRunning, Attempts: 1},
			"2": {State: catalog.ShardPending},
		}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State == catalog.MigrationComplete {
		t.Fatalf("migration %s (%s); the column was never added on shard 1", m.State, states(m))
	}
}

// resumed is set after EVERY attempt, so a dial failure or a lock timeout --
// neither of which can have committed anything -- makes the next attempt
// look like a resume. Forgiving a missing object there reports a DROP
// applied on a shard that never ran it, and a migration complete when
// nothing was dropped anywhere.
func TestARetryAfterADialFailureIsNotAResume(t *testing.T) {
	f := newApplierFixture(t)
	// Once per shard, so every shard's second attempt is the one that
	// looks like a resume.
	down := map[int32]bool{0: true, 1: true, 2: true}
	f.shards.dialErr = func(shard int32) error {
		if down[shard] {
			down[shard] = false
			return errors.New("connection refused")
		}
		return nil
	}
	f.shards.exec = func(_ int32, sql string) error {
		if strings.Contains(sql, "drop trigger") {
			return pgErr("42704", `trigger "t" for relation "orders" does not exist`)
		}
		return nil
	}
	id := f.queue(catalog.DDLMigration{
		Statement: "drop trigger t on orders", Kind: "DROP TRIGGER", Scope: "all", State: catalog.MigrationRunning,
		PerShard: map[string]catalog.ShardMigration{
			"0": {State: catalog.ShardPending},
			"1": {State: catalog.ShardPending},
			"2": {State: catalog.ShardPending},
		}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State == catalog.MigrationComplete {
		t.Fatalf("migration %s (%s); no shard ever dropped anything", m.State, states(m))
	}
}

// The forgiveness is for a missing object, not for any failure a resumed
// drop happens to hit. A dependent object or a permission error is a real
// refusal, and recording it applied would report a migration complete that
// dropped nothing on that shard.
func TestAResumedDropIsNotForgivenForOtherErrors(t *testing.T) {
	for _, code := range []string{"2BP01", "42501", "55P03"} {
		f := newApplierFixture(t)
		f.shards.exec = func(shard int32, sql string) error {
			if shard == 1 && strings.Contains(sql, "drop trigger") {
				return pgErr(code, "refused")
			}
			return nil
		}
		id := f.queue(catalog.DDLMigration{
			Statement: "drop trigger t on orders", Kind: "DROP TRIGGER", Scope: "all", State: catalog.MigrationRunning,
			PerShard: map[string]catalog.ShardMigration{
				"0": {State: catalog.ShardApplied, Attempts: 1},
				"1": {State: catalog.ShardRunning, Attempts: 1},
				"2": {State: catalog.ShardPending},
			}})
		f.run(t)
		if m := f.store.get(t, id); m.State == catalog.MigrationComplete {
			t.Errorf("%s: migration %s (%s); shard 1 refused the drop", code, m.State, states(m))
		}
	}
}
