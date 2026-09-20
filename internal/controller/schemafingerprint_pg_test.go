package controller

import (
	"testing"
)

// TestSchemaFingerprintIgnoresTheJournal: the journal table is created on
// the SOURCES at StepJournal, which runs after the fingerprints are taken
// at StepReverse. Counting it made every recorded fingerprint describe a
// source that did not yet have the journal, so a rollback compared against
// it saw drift and refused -- "schema changed since the switch ... needs
// reconciling by hand" -- on a set whose structure nobody had touched.
//
// It was invisible because the flip's re-carry rewound through StepReverse
// and re-took the fingerprints once the journal existed. A cutover that did
// not re-carry had no rollback, which is the safety net.
func TestSchemaFingerprintIgnoresTheJournal(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	conn := f.app(0)
	mustExec(t, conn, `CREATE TABLE kept (id bigint PRIMARY KEY, v text)`)

	before := queryOne[string](t, conn, schemaFingerprintSQL)
	mustExec(t, conn, `CREATE SCHEMA IF NOT EXISTS `+JournalSchema)
	mustExec(t, conn, `CREATE TABLE `+JournalSchema+`.resharding_journal (
		id uuid NOT NULL, source_shard int NOT NULL, generation bigint NOT NULL,
		participants int[] NOT NULL, targets jsonb NOT NULL,
		created_at timestamptz NOT NULL, PRIMARY KEY (id, source_shard))`)
	if after := queryOne[string](t, conn, schemaFingerprintSQL); after != before {
		t.Fatal("writing the journal must not read as a schema change: a rollback compares against a fingerprint taken before it exists")
	}
	// And it is not blind: a user table still moves it.
	mustExec(t, conn, `ALTER TABLE kept ADD COLUMN extra int`)
	if changed := queryOne[string](t, conn, schemaFingerprintSQL); changed == before {
		t.Fatal("a real schema change must move the fingerprint")
	}
}

// TestSchemaFingerprintSeesEnumValuesAndDomains (PGS-889): the rollback
// refuses when the schema changed since the switch, and the column line
// carries format_type -- the type's NAME. So adding a value to an enum, or
// a constraint to a domain, changed what rows are legal without changing
// any line the fingerprint had. A reverse apply that then met a row
// carrying the new enum value failed, and the rollback waited on catch-up
// for ever.
//
// Measured before the fix, all five unchanged where a new column moved it:
// enum value added, domain constraint added, new domain, new function, new
// extension. The first three are closed here. A new function or extension
// still does not move it, and correctly -- see
// TestSchemaFingerprintSeesTheBodyOfAFunctionAConstraintCalls.
func TestSchemaFingerprintSeesEnumValuesAndDomains(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	conn := f.app(0)
	mustExec(t, conn, `CREATE TYPE mood AS ENUM ('ok', 'bad')`)
	mustExec(t, conn, `CREATE DOMAIN positive AS int CHECK (VALUE > 0)`)
	mustExec(t, conn, `CREATE TABLE rows_of (id bigint PRIMARY KEY, m mood, p positive)`)

	for _, c := range []struct{ name, sql string }{
		{"a value added to an enum a replicated column uses", `ALTER TYPE mood ADD VALUE 'worse'`},
		{"a value added BEFORE another, which reorders the labels", `ALTER TYPE mood ADD VALUE 'mild' BEFORE 'bad'`},
		{"a constraint added to a domain", `ALTER DOMAIN positive ADD CONSTRAINT big CHECK (VALUE > 10)`},
		{"a new domain", `CREATE DOMAIN tiny AS int CHECK (VALUE < 5)`},
	} {
		before := queryOne[string](t, conn, schemaFingerprintSQL)
		mustExec(t, conn, c.sql)
		if after := queryOne[string](t, conn, schemaFingerprintSQL); after == before {
			t.Errorf("%s did not move the fingerprint, so a rollback would proceed past it and then wait on a reverse apply that cannot succeed: %s", c.name, c.sql)
		}
	}

	// And it is still not reading pgshard's own work as user drift: the
	// journal is created on the sources AFTER the fingerprints are taken.
	before := queryOne[string](t, conn, schemaFingerprintSQL)
	mustExec(t, conn, `CREATE SCHEMA IF NOT EXISTS `+JournalSchema)
	mustExec(t, conn, `CREATE TYPE `+JournalSchema+`.internal_mood AS ENUM ('a')`)
	mustExec(t, conn, `CREATE DOMAIN `+JournalSchema+`.internal_positive AS int CHECK (VALUE > 0)`)
	if after := queryOne[string](t, conn, schemaFingerprintSQL); after != before {
		t.Error("a type in the journal schema moved the fingerprint: a rollback would refuse on a set nobody touched")
	}
}

// TestSchemaFingerprintSeesTheBodyOfAFunctionAConstraintCalls (PGS-889): the
// half of this ticket left open, closed on evidence rather than by counting
// everything.
//
// CREATE OR REPLACE keeps a function's OID, so a CHECK that calls it keeps
// the same text -- and the same fingerprint line -- while what it accepts
// changes. DDL after the switch reaches only the targets, so a LOOSER body
// there admits rows the source's old body rejects, and the reverse apply
// fails on them and the rollback waits for ever. Measured on origin/main: the
// fingerprint did not move.
//
// The negatives are the design. A function nothing references cannot make a
// row illegal. A trigger added to the targets never touches the source, and
// the apply worker runs as session_replication_role = replica, so an
// ordinary one does not fire on apply at all; counting triggers would also
// read pgshard's own placement-fence triggers as user drift. An extension
// matters only through a column type or a called function, both already
// counted.
func TestSchemaFingerprintSeesTheBodyOfAFunctionAConstraintCalls(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	conn := f.app(0)
	mustExec(t, conn, `CREATE FUNCTION legal(int) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT $1 > 0'`)
	mustExec(t, conn, `CREATE FUNCTION small(int) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT $1 < 1000'`)
	mustExec(t, conn, `CREATE FUNCTION bucket(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 / 10'`)
	mustExec(t, conn, `CREATE FUNCTION unused(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1'`)
	mustExec(t, conn, `CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'`)
	mustExec(t, conn, `CREATE DOMAIN bounded AS int CHECK (small(VALUE))`)
	// A SQL-standard body (PostgreSQL 14+): its code lives in prosqlbody,
	// not prosrc, so a fingerprint that hashes prosrc alone cannot see it
	// change.
	mustExec(t, conn, `CREATE FUNCTION atomic(int) RETURNS bool LANGUAGE sql IMMUTABLE BEGIN ATOMIC SELECT $1 > 0; END`)
	mustExec(t, conn, `CREATE TABLE guarded (id bigint PRIMARY KEY, x int CHECK (legal(x)), b bounded, k int, a int CHECK (atomic(a)))`)
	mustExec(t, conn, `CREATE UNIQUE INDEX guarded_bucket ON guarded (bucket(k))`)

	for _, c := range []struct {
		name, sql string
		moves     bool
	}{
		{"the body of a function a table CHECK calls", `CREATE OR REPLACE FUNCTION legal(int) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT $1 > -100'`, true},
		{"the body of a function a domain CHECK calls", `CREATE OR REPLACE FUNCTION small(int) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT $1 < 5000'`, true},
		{"the SQL-standard body of a function a CHECK calls", `CREATE OR REPLACE FUNCTION atomic(int) RETURNS bool LANGUAGE sql IMMUTABLE BEGIN ATOMIC SELECT $1 > -100; END`, true},
		{"the body of a function a unique index is built on", `CREATE OR REPLACE FUNCTION bucket(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 / 100'`, true},
		{"the body of a function nothing references", `CREATE OR REPLACE FUNCTION unused(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 + 1'`, false},
		{"a trigger added to a replicated table", `CREATE TRIGGER t BEFORE INSERT ON guarded FOR EACH ROW EXECUTE FUNCTION trg()`, false},
		{"the body of a trigger's function", `CREATE OR REPLACE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN NEW.k := 0; RETURN NEW; END'`, false},
		{"an extension created", `CREATE EXTENSION IF NOT EXISTS pgcrypto`, false},
	} {
		before := queryOne[string](t, conn, schemaFingerprintSQL)
		mustExec(t, conn, c.sql)
		moved := queryOne[string](t, conn, schemaFingerprintSQL) != before
		switch {
		case c.moves && !moved:
			t.Errorf("%s did not move the fingerprint: a looser body on the targets admits rows the source rejects, and the rollback's reverse apply fails on them", c.name)
		case !c.moves && moved:
			t.Errorf("%s moved the fingerprint: it cannot make a row the source accepted become one it rejects, so a rollback would refuse on a set nothing broke", c.name)
		}
	}

	// And pgshard's own work is still not user drift.
	mustExec(t, conn, `CREATE SCHEMA IF NOT EXISTS `+JournalSchema)
	mustExec(t, conn, `CREATE FUNCTION `+JournalSchema+`.internal_ok(int) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT true'`)
	mustExec(t, conn, `CREATE TABLE `+JournalSchema+`.internal_t (x int CHECK (`+JournalSchema+`.internal_ok(x)))`)
	before := queryOne[string](t, conn, schemaFingerprintSQL)
	mustExec(t, conn, `CREATE OR REPLACE FUNCTION `+JournalSchema+`.internal_ok(int) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT false'`)
	if after := queryOne[string](t, conn, schemaFingerprintSQL); after != before {
		t.Error("a function in the journal schema moved the fingerprint: a rollback would refuse on a set nobody touched")
	}
}
