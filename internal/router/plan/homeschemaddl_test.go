package plan

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestObjectsDeclaredInsideCreateSchemaAreRefused (PGS-969).
//
// CREATE SCHEMA takes optional element statements -- CREATE TABLE, CREATE
// VIEW, CREATE INDEX, GRANT -- and PostgreSQL executes them as part of the
// schema's creation. The planner sees one CreateSchemaStmt and plans one
// "CREATE SCHEMA" migration from its name alone, so the elements are never
// walked: a table inside it skips the rule that a sharded table's unique
// constraints include the shard key and is never declared in pgshard.tables,
// and a view inside it skips the refusal that stops a view reading one
// shard's rows of a distributed table. A local schema is worse still,
// because the whole statement is routed to the home shard and nothing looks
// inside at all.
func TestObjectsDeclaredInsideCreateSchemaAreRefused(t *testing.T) {
	s := fixture(t)
	db := s.Databases[fixtureDB]
	db.DefaultPlacement = "sharded"
	db.LocalSchemas = []string{"tool"}
	s.Databases[fixtureDB] = db
	p := New()

	for _, sql := range []string{
		`CREATE SCHEMA tool CREATE VIEW orders_v AS SELECT id, tenant_id FROM public.orders`,
		`CREATE SCHEMA tool CREATE TABLE saved (id int PRIMARY KEY, note text)`,
		`CREATE SCHEMA app CREATE TABLE t (id int UNIQUE, tenant_id int)`,
		`CREATE SCHEMA AUTHORIZATION reader CREATE TABLE t (id int)`,
	} {
		pl, err := p.Plan(context.Background(), session(s), sql)
		if err == nil {
			t.Errorf("%.70s planned %v (migration %v); its nested objects are created without the checks their own statements carry", sql, pl.Kind, pl.Migration != nil)
		}
	}

	// The bare form still works, on both paths: a local schema on the home
	// shard, an ordinary one as a fanned-out migration.
	if pl, err := p.Plan(context.Background(), session(s), `CREATE SCHEMA tool`); err != nil || pl.Migration != nil || pl.Kind != Unsharded {
		t.Errorf("CREATE SCHEMA tool: %v, kind %v, migration %v; want the home shard", err, pl.Kind, pl.Migration != nil)
	}
	if pl, err := p.Plan(context.Background(), session(s), `CREATE SCHEMA app`); err != nil || pl.Migration == nil {
		t.Errorf("CREATE SCHEMA app: %v, migration %v; want a fanned-out migration", err, pl.Migration != nil)
	}
}

// TestCreateTableAsIsMarkedAsHomeDDL (PGS-969).
//
// CREATE TABLE AS is the one DDL that does not reach migration(): derived()
// plans it as a plain unsharded statement on the home shard, in the client's
// own transaction. That is the right place to run it, but it left the plan
// unmarked, so the executor's gate on DDL that cannot wait its turn --
// checkHomeDDL -- never saw it. A reshard's copy replicates row changes, not
// new relations, so a table created on the source while a copy is running
// does not exist on the target and cutover loses it.
func TestCreateTableAsIsMarkedAsHomeDDL(t *testing.T) {
	s := fixture(t)
	db := s.Databases[fixtureDB]
	db.DefaultPlacement = "sharded"
	db.LocalSchemas = []string{"tool"}
	s.Databases[fixtureDB] = db
	p := New()

	for _, sql := range []string{
		`CREATE TABLE tool.saved AS SELECT id FROM public.items`,
		`CREATE MATERIALIZED VIEW tool.m AS SELECT id FROM public.items`,
		`SELECT id INTO tool.copied FROM public.items`,
	} {
		pl, err := p.Plan(context.Background(), session(s), sql)
		if err != nil {
			t.Errorf("refused: %.70s\n  %v", sql, err)
			continue
		}
		if !pl.HomeDDL {
			t.Errorf("%.70s planned %v on %v without HomeDDL; it creates a relation on the home shard and cannot wait its turn in the queue", sql, pl.Kind, pl.Shards)
		}
	}

	// The same in an ordinary database, where the relation it creates is
	// unsharded by default rather than by a local schema.
	plain := New()
	for _, sql := range []string{
		`CREATE TABLE saved AS SELECT id FROM public.items`,
		`SELECT id INTO saved FROM public.items`,
		`SELECT 1 AS n INTO saved`,
	} {
		pl, err := plain.Plan(context.Background(), session(fixture(t)), sql)
		if err != nil {
			t.Errorf("refused: %.70s\n  %v", sql, err)
			continue
		}
		if !pl.HomeDDL {
			t.Errorf("%.70s planned %v on %v without HomeDDL", sql, pl.Kind, pl.Shards)
		}
	}

	// The refusals derived() already made are unchanged: the mark is not a
	// way past them.
	for _, sql := range []string{
		`CREATE TABLE tool.saved AS SELECT tenant_id FROM public.orders`,
		`CREATE TABLE public.orders AS SELECT 1 AS tenant_id`,
	} {
		if _, err := p.Plan(context.Background(), session(s), sql); err == nil {
			t.Errorf("%.70s planned; it reads or writes a distributed table from the home shard alone", sql)
		}
	}

	// The grammar hangs the into clause off the LEFTMOST ARM of a set
	// operation, so reading it off the top node alone missed these --
	// they create a relation exactly as the plain form does.
	for _, sql := range []string{
		`SELECT 1 AS n INTO tool.u UNION ALL SELECT 2`,
		`SELECT 1 AS n INTO tool.i INTERSECT SELECT 1`,
		`SELECT 1 AS n INTO tool.e EXCEPT SELECT 2`,
		`SELECT 1 AS n INTO tool.u3 UNION ALL SELECT 2 UNION ALL SELECT 3`,
	} {
		pl, err := p.Plan(context.Background(), session(s), sql)
		if err != nil {
			t.Errorf("refused: %.70s\n  %v", sql, err)
			continue
		}
		if !pl.HomeDDL {
			t.Errorf("%.70s planned %v without HomeDDL; the into clause is on its leftmost arm", sql, pl.Kind)
		}
	}

	// EXPLAIN without ANALYZE runs nothing, so it creates no relation and
	// must not be refused while a reshard is copying. EXPLAIN ANALYZE does
	// run it.
	for _, c := range []struct {
		sql  string
		mark bool
	}{
		{`EXPLAIN CREATE TABLE tool.x AS SELECT id FROM public.items`, false},
		{`EXPLAIN SELECT id INTO tool.y FROM public.items`, false},
		{`EXPLAIN (ANALYZE false) CREATE TABLE tool.z AS SELECT 1 AS n`, false},
		{`EXPLAIN ANALYZE CREATE TABLE tool.a AS SELECT 1 AS n`, true},
		{`EXPLAIN (ANALYZE) SELECT 1 AS n INTO tool.b`, true},
		{`EXPLAIN (ANALYZE true, BUFFERS) CREATE TABLE tool.c AS SELECT 1 AS n`, true},
	} {
		pl, err := p.Plan(context.Background(), session(s), c.sql)
		if err != nil {
			t.Errorf("refused: %.70s\n  %v", c.sql, err)
			continue
		}
		if pl.HomeDDL != c.mark {
			t.Errorf("%.70s: HomeDDL %v, want %v", c.sql, pl.HomeDDL, c.mark)
		}
	}

	// A plain SELECT is not DDL and stays unmarked, so the gate does not
	// refuse reads while a reshard runs.
	if pl, err := p.Plan(context.Background(), session(s), `SELECT id FROM public.items`); err != nil || pl.HomeDDL {
		t.Errorf("SELECT: %v, HomeDDL %v; want an unmarked read", err, pl.HomeDDL)
	}
}

// TestALocalSchemaStatementIsMarkedAsHomeDDL (PGS-969): the same gate, for
// the path a local schema's DDL already took. migration() marks it, so this
// pins that it keeps doing so -- the distributed-view and shard-key checks
// are skipped for these schemas deliberately, and the reshard gate is what
// is left.
func TestALocalSchemaStatementIsMarkedAsHomeDDL(t *testing.T) {
	s := fixture(t)
	db := s.Databases[fixtureDB]
	db.DefaultPlacement = "sharded"
	db.LocalSchemas = []string{"tool"}
	s.Databases[fixtureDB] = db
	p := New()

	for _, sql := range []string{
		`CREATE TABLE tool.saved (id int PRIMARY KEY)`,
		`CREATE INDEX i ON tool.saved (id)`,
		`DROP TABLE tool.saved`,
		`CREATE FUNCTION tool.f() RETURNS int LANGUAGE sql AS 'select 1'`,
	} {
		pl, err := p.Plan(context.Background(), session(s), sql)
		if err != nil {
			t.Errorf("refused: %.70s\n  %v", sql, err)
			continue
		}
		if !pl.HomeDDL || pl.Migration != nil {
			t.Errorf("%.70s: HomeDDL %v, migration %v; want home DDL the reshard gate sees", sql, pl.HomeDDL, pl.Migration != nil)
		}
	}

	// A statement about the local schema that is not DDL is not marked.
	if pl, err := p.Plan(context.Background(), session(s), `SELECT * FROM tool.saved`); err != nil || pl.HomeDDL {
		t.Errorf("SELECT from a local schema: %v, HomeDDL %v", err, pl.HomeDDL)
	}
}

// TestTheNestedCreateSchemaRefusalNamesAWayOut: a refusal that does not say
// what to do instead reads as "pgshard cannot do this", and the statement
// can always be split.
func TestTheNestedCreateSchemaRefusalNamesAWayOut(t *testing.T) {
	s := fixture(t)
	_, err := New().Plan(context.Background(), session(s), `CREATE SCHEMA app CREATE TABLE t (id int)`)
	if err == nil {
		t.Fatal("planned")
	}
	if !strings.Contains(err.Error(), "CREATE SCHEMA") {
		t.Errorf("the refusal does not name the statement: %v", err)
	}
	var pgErr *pgwire.Error
	if !errors.As(err, &pgErr) {
		t.Fatalf("not a pgwire error: %T", err)
	}
	if pgErr.Hint == "" {
		t.Errorf("no way out named: %v", err)
	}
}
