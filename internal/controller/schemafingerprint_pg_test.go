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
// extension. The first three are closed here. Functions and extensions are
// deliberately still out -- see the comment on schemaFingerprintSQL.
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
