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
