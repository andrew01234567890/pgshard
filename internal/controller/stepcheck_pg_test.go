package controller

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestStepChecksAnswerForTheRelationTheStatementMeans: a step's skip
// predicate has to name the relation its statement names. Partitions may
// live in schemas of their own, so one parent can have two children called
// the same thing, and matching a name across schemas answered for whichever
// of them the catalog scan found.
func TestStepChecksAnswerForTheRelationTheStatementMeans(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	conn := pgxShardConn{connect(t, startPostgres(t))}
	for _, ddl := range []string{
		`CREATE SCHEMA a`,
		`CREATE SCHEMA b`,
		`CREATE TABLE a.orders (id bigint, at date) PARTITION BY RANGE (at)`,
		`CREATE TABLE a.orders_1 PARTITION OF a.orders FOR VALUES FROM ('2020-01-01') TO ('2021-01-01')`,
		`CREATE TABLE b.orders_1 PARTITION OF a.orders FOR VALUES FROM ('2021-01-01') TO ('2022-01-01')`,
		`CREATE TABLE a."Orders_2" PARTITION OF a.orders FOR VALUES FROM ('2022-01-01') TO ('2023-01-01')`,
		`SET search_path = a, public`,
	} {
		mustExec(t, conn.Conn, ddl)
	}
	holds := func(c catalog.MigrationCheck) bool {
		ok, err := checkHolds(ctx, conn, c)
		if err != nil {
			t.Fatalf("%s check on %s.%s: %v", c.Kind, c.NameSchema, c.Name, err)
		}
		return ok
	}
	detached := catalog.MigrationCheck{Kind: "detached", Table: "orders", Name: "orders_1"}
	pending := catalog.MigrationCheck{Kind: "detach_pending", Table: "orders", Name: "orders_1"}
	if holds(detached) || holds(pending) {
		t.Fatal("a.orders_1 is attached, so neither detach step is done")
	}

	mustExec(t, conn.Conn, `ALTER TABLE a.orders DETACH PARTITION a.orders_1`)
	if !holds(detached) {
		t.Fatal("FINALIZE would run again on a detached partition because b.orders_1 shares its name")
	}
	if !holds(pending) {
		t.Fatal("DETACH would run again on a detached partition because b.orders_1 shares its name")
	}

	mustExec(t, conn.Conn, `SET search_path = b, a`)
	if holds(detached) {
		t.Fatal(`"orders_1" now means b.orders_1, which is still attached`)
	}
	mustExec(t, conn.Conn, `SET search_path = a, public`)
	if holds(catalog.MigrationCheck{Kind: "detached", Schema: "a", Table: "orders", Name: "orders_1", NameSchema: "b"}) {
		t.Fatal("b.orders_1 is attached, and the qualified name says b")
	}
	if holds(catalog.MigrationCheck{Kind: "detached", Table: "orders", Name: "Orders_2"}) {
		t.Fatal(`a."Orders_2" is attached; its name has to be quoted, not folded`)
	}

	for _, ddl := range []string{
		`CREATE TABLE a.t (id bigint)`,
		`CREATE TABLE b.t (id bigint)`,
		`ALTER TABLE b.t ADD CONSTRAINT t_id_key UNIQUE (id)`,
		`CREATE INDEX t_id_idx ON b.t (id)`,
		`SET search_path = a, b`,
	} {
		mustExec(t, conn.Conn, ddl)
	}
	constraint := catalog.MigrationCheck{Kind: "constraint", Table: "t", Name: "t_id_key"}
	index := catalog.MigrationCheck{Kind: "index_valid", Table: "t", Name: "t_id_idx"}
	if holds(constraint) || holds(index) {
		t.Fatal(`"t" means a.t, which has neither the constraint nor the index`)
	}
	mustExec(t, conn.Conn, `ALTER TABLE a.t ADD CONSTRAINT t_id_key UNIQUE (id)`)
	mustExec(t, conn.Conn, `CREATE INDEX t_id_idx ON a.t (id)`)
	if !holds(constraint) || !holds(index) {
		t.Fatal("a.t has both now")
	}
}
