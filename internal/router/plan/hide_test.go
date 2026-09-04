package plan

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// rewriteFixture puts orders under an online rewrite of column "amount".
func rewriteFixture(t testing.TB, visible ...string) *snapshot.Snapshot {
	t.Helper()
	s := fixture(t)
	key := snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "orders"}
	p := s.Tables[key]
	p.HiddenColumns = []string{"_pgshard_amount_deadbeef"}
	p.VisibleColumns = visible
	s.Tables[key] = p
	return s
}

func TestHiddenColumnExpansion(t *testing.T) {
	p := New()
	snap := rewriteFixture(t, "tenant_id", "id", "amount")
	cases := []struct {
		sql       string
		rewritten string
		refuse    string
	}{
		{sql: "select * from orders where tenant_id = 1",
			rewritten: "SELECT tenant_id, id, amount FROM orders WHERE tenant_id = 1"},
		{sql: "select o.* from orders o where o.tenant_id = 1",
			rewritten: "SELECT o.tenant_id, o.id, o.amount FROM orders o WHERE o.tenant_id = 1"},
		{sql: "select count(*) from orders where tenant_id = 1", rewritten: ""},
		{sql: "select id, amount from orders where tenant_id = 1", rewritten: ""},
		{sql: "select _pgshard_amount_deadbeef from orders where tenant_id = 1", refuse: "column \"_pgshard_amount_deadbeef\" does not exist"},
		{sql: "update orders set _pgshard_amount_deadbeef = 1 where tenant_id = 1", refuse: "column \"_pgshard_amount_deadbeef\" does not exist"},
		{sql: "insert into orders (tenant_id, _pgshard_amount_deadbeef) values (1, 2)", refuse: "column \"_pgshard_amount_deadbeef\" does not exist"},
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id where o.tenant_id = 1",
			refuse: "cannot span other tables"},
		{sql: "update orders set amount = 2 where tenant_id = 1 returning *",
			rewritten: "UPDATE orders SET amount = 2 WHERE tenant_id = 1 RETURNING tenant_id, id, amount"},
		{sql: "update orders o set amount = 2 where o.tenant_id = 1 returning o.*",
			rewritten: "UPDATE orders o SET amount = 2 WHERE o.tenant_id = 1 RETURNING o.tenant_id, o.id, o.amount"},
		{sql: "insert into orders (tenant_id, id, amount) values (1, 2, 3) returning *",
			rewritten: "INSERT INTO orders (tenant_id, id, amount) VALUES (1, 2, 3) RETURNING tenant_id, id, amount"},
		{sql: "delete from orders where tenant_id = 1 returning *",
			rewritten: "DELETE FROM orders WHERE tenant_id = 1 RETURNING tenant_id, id, amount"},
		{sql: "update orders set amount = 2 where tenant_id = 1 returning id, amount", rewritten: ""},
		{sql: "select to_jsonb(o) from orders o where o.tenant_id = 1", refuse: "whole-row reference"},
		{sql: "select row_to_json(orders) from orders where tenant_id = 1", refuse: "whole-row reference"},
		{sql: "select count(o.*) from orders o where o.tenant_id = 1", refuse: "whole-row reference"},
		{sql: "select o::text from orders o where o.tenant_id = 1", refuse: "whole-row reference"},
		{sql: "update orders o set amount = 2 where o.tenant_id = 1 returning to_jsonb(o)", refuse: "whole-row reference"},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			pl, err := p.Plan(context.Background(), session(snap), c.sql)
			if c.refuse != "" {
				var pe *pgwire.Error
				if err == nil || !errors.As(err, &pe) || !strings.Contains(pe.Message, c.refuse) {
					t.Fatalf("err = %v, want %q", err, c.refuse)
				}
				return
			}
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if pl.Rewritten != c.rewritten {
				t.Fatalf("rewritten = %q, want %q", pl.Rewritten, c.rewritten)
			}
		})
	}
}

func TestInsertColumnListExpansion(t *testing.T) {
	p := New()
	snap := fixture(t)
	key := snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "items"}
	pl := snap.Tables[key]
	pl.HiddenColumns = []string{"_pgshard_name_deadbeef"}
	pl.VisibleColumns = []string{"id", "name", "price"}
	snap.Tables[key] = pl
	cases := []struct {
		sql       string
		rewritten string
		refuse    string
	}{
		{sql: "insert into items values (1, 'a', 2)", rewritten: "INSERT INTO items (id, name, price) VALUES (1, 'a', 2)"},
		{sql: "insert into items values (1, 'a')", rewritten: "INSERT INTO items (id, name) VALUES (1, 'a')"},
		{sql: "insert into items values (1, 'a', 2, 3)", refuse: "more values than visible columns"},
		{sql: "insert into items (id) values (1)", rewritten: ""},
		{sql: "insert into items select 1", refuse: "INSERT without a column list"},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			got, err := p.Plan(context.Background(), session(snap), c.sql)
			if c.refuse != "" {
				var pe *pgwire.Error
				if err == nil || !errors.As(err, &pe) || !strings.Contains(pe.Message, c.refuse) {
					t.Fatalf("err = %v, want %q", err, c.refuse)
				}
				return
			}
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if got.Rewritten != c.rewritten {
				t.Fatalf("rewritten = %q, want %q", got.Rewritten, c.rewritten)
			}
		})
	}
}

func TestHiddenColumnsWithoutPublishedList(t *testing.T) {
	p := New()
	snap := rewriteFixture(t)
	_, err := p.Plan(context.Background(), session(snap), "select * from orders where tenant_id = 1")
	var pe *pgwire.Error
	if err == nil || !errors.As(err, &pe) || pe.Code != "55000" {
		t.Fatalf("err = %v, want 55000", err)
	}
	if _, err := p.Plan(context.Background(), session(snap), "select id from orders where tenant_id = 1"); err != nil {
		t.Fatalf("explicit columns must still plan: %v", err)
	}
}

func TestTablesNotUnderRewriteAreUntouched(t *testing.T) {
	p := New()
	pl, err := p.Plan(context.Background(), session(fixture(t)), "select * from orders where tenant_id = 1")
	if err != nil || pl.Rewritten != "" {
		t.Fatalf("plan = %+v err %v", pl, err)
	}
}

func TestRewriteClassification(t *testing.T) {
	p := New()
	snap := fixture(t)
	cases := []struct {
		sql                                string
		column, newType, using, def, table string
		add                                bool
	}{
		{sql: "alter table orders alter column amount type bigint",
			table: "orders", column: "amount", newType: "bigint", using: `CAST("amount" AS bigint)`},
		{sql: "alter table orders alter column note type integer using (note::integer + 1)",
			table: "orders", column: "note", newType: "int", using: "note::int + 1"},
		{sql: "alter table orders alter column v type varchar(20)",
			table: "orders", column: "v", newType: "varchar(20)", using: `CAST("v" AS varchar(20))`},
		{sql: "alter table orders add column token uuid default gen_random_uuid()",
			table: "orders", column: "token", newType: "uuid", def: "gen_random_uuid()", add: true},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			pl, err := p.Plan(context.Background(), session(snap), c.sql)
			if err != nil {
				t.Fatal(err)
			}
			m := pl.Migration
			if m == nil || m.Strategy != StrategyRewrite || m.Rewrite == nil {
				t.Fatalf("plan = %+v", pl)
			}
			rw := m.Rewrite
			if rw.Table != c.table || rw.Schema != "public" || rw.Column != c.column || rw.NewType != c.newType ||
				rw.Using != c.using || rw.Default != c.def || rw.Add != c.add {
				t.Fatalf("rewrite = %+v", rw)
			}
			if !strings.HasPrefix(rw.HiddenColumn("0123456789abcdef"), "_pgshard_"+c.column+"_01234567") {
				t.Fatalf("hidden column %q", rw.HiddenColumn("0123456789abcdef"))
			}
		})
	}
}

func TestVacuumFullBecomesRepack(t *testing.T) {
	p := New()
	snap := fixture(t)
	pl, err := p.Plan(context.Background(), session(snap), "vacuum (full) orders")
	if err != nil {
		t.Fatal(err)
	}
	if pl.Kind != MigrationKind || pl.Migration.Strategy != StrategyRepack || pl.Migration.Kind != "VACUUM" {
		t.Fatalf("plan = %+v mig %+v", pl, pl.Migration)
	}
	if pl.Migration.Object.Name != "orders" {
		t.Fatalf("object = %+v", pl.Migration.Object)
	}
	if _, err := p.Plan(context.Background(), session(snap), "vacuum orders"); err == nil {
		t.Fatal("plain VACUUM of a sharded table must still be refused")
	}
	pl, err = p.Plan(context.Background(), session(snap), "vacuum (full) items")
	if err != nil || pl.Kind != Unsharded {
		t.Fatalf("unsharded VACUUM FULL: %+v %v", pl, err)
	}
	pl, err = p.Plan(context.Background(), session(snap), "analyze orders")
	if err == nil {
		t.Fatal("ANALYZE of a sharded table must still be refused")
	}
	_ = pl
}

// TestMultiShardPlansCarryTheMaskingIntoShardSQL: the masking produced
// plan.Rewritten after the merge spec was already built from the client's
// tree, so Merge.ShardSQL -- which the scatter executor prefers -- still
// carried the bare star. Every multi-shard read of a table under an online
// rewrite therefore returned the working column to the client, while the
// same query with a shard-key predicate did not.
func TestMultiShardPlansCarryTheMaskingIntoShardSQL(t *testing.T) {
	p := New()
	snap := rewriteFixture(t, "tenant_id", "id", "amount")
	for _, c := range []struct {
		sql  string
		want string
	}{
		{"select * from orders limit 10", "SELECT tenant_id, id, amount FROM orders LIMIT 10"},
		{"select * from orders order by id limit 10", "SELECT tenant_id, id, amount FROM orders ORDER BY id LIMIT 10"},
		{"select o.* from orders o limit 10", "SELECT o.tenant_id, o.id, o.amount FROM orders o LIMIT 10"},
	} {
		t.Run(c.sql, func(t *testing.T) {
			pl, err := p.Plan(context.Background(), session(snap), c.sql)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if pl.Kind != Scatter {
				t.Fatalf("kind %s, want Scatter", pl.Kind)
			}
			m, err := pl.MultiShard()
			if err != nil {
				t.Fatal(err)
			}
			if m.ShardSQL != c.want {
				t.Fatalf("shard SQL %q, want %q", m.ShardSQL, c.want)
			}
			// Whatever a caller reaches for, no text with a bare star over
			// a table under rewrite may leave the router.
			for _, sql := range []string{m.ShardSQL, pl.Rewritten} {
				if strings.Contains(sql, "*") {
					t.Fatalf("a star reached the shards: %q", sql)
				}
			}
		})
	}
}

// TestReferenceWriteCarriesTheMasking: a reference write fans out to every
// shard, and its RETURNING * was expanded only into plan.Rewritten, which
// the extended-protocol reference path did not read.
func TestReferenceWriteCarriesTheMasking(t *testing.T) {
	s := fixture(t)
	key := snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "regions"}
	pr := s.Tables[key]
	pr.HiddenColumns = []string{"_pgshard_name_deadbeef"}
	pr.VisibleColumns = []string{"id", "name"}
	s.Tables[key] = pr

	pl, err := New().Plan(context.Background(), session(s), "insert into regions (id, name) values (1, 'a') returning *")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if pl.Kind != Reference {
		t.Fatalf("kind %s, want Reference", pl.Kind)
	}
	if want := "INSERT INTO regions (id, name) VALUES (1, 'a') RETURNING id, name"; pl.Rewritten != want {
		t.Fatalf("rewritten %q, want %q", pl.Rewritten, want)
	}
}

// TestUndeclaredReferenceWriteWaitsForItsInspection: a table that is a
// reference table only because its database defaults to reference
// placement is replicated to every shard and diverges in exactly the same
// ways as a declared one -- a default, a generated expression, an identity
// column, a trigger or a rule that each shard evaluates for itself. It has
// no catalog row, so the gate that holds a declared reference write until
// the shards have been inspected did not apply to it, and the write went
// out unchecked to write a different row on every shard.
func TestUndeclaredReferenceWriteWaitsForItsInspection(t *testing.T) {
	refDefault := func(t *testing.T) *snapshot.Snapshot {
		t.Helper()
		s := fixture(t)
		db := s.Databases[fixtureDB]
		db.DefaultPlacement = "reference"
		s.Databases[fixtureDB] = db
		return s
	}
	key := snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "undeclared"}

	// Never inspected: refused, and the refusal says what will clear it.
	pl, err := New().Plan(context.Background(), session(refDefault(t)), "insert into undeclared (id) values (1)")
	checkRefusal(t, pl, err, "a write to reference table \"undeclared\" cannot be planned until its shards have been inspected", "0A000")

	// Inspected and clean: the write plans onto every shard.
	clean := refDefault(t)
	clean.Tables[key] = snapshot.Placement{Placement: "reference", ReferenceChecked: true}
	pl, err = New().Plan(context.Background(), session(clean), "insert into undeclared (id) values (1)")
	if err != nil || pl.Kind != Reference {
		t.Fatalf("a clean undeclared reference table must accept writes: %+v %v", pl, err)
	}

	// Inspected and diverging: refused, naming what the shards would each
	// evaluate for themselves.
	hazard := refDefault(t)
	hazard.Tables[key] = snapshot.Placement{Placement: "reference", ReferenceChecked: true,
		ReferenceHazards: []string{"the default of column tag calls gen_random_uuid(), which pg_proc marks VOLATILE"}}
	pl, err = New().Plan(context.Background(), session(hazard), "insert into undeclared (id) values (1)")
	checkRefusal(t, pl, err, "a write to reference table \"undeclared\" would not write the same row on every shard", "0A000")

	// Reads are unaffected throughout.
	if _, err := New().Plan(context.Background(), session(refDefault(t)), "select * from undeclared"); err != nil {
		t.Fatalf("reads must not wait for the inspection: %v", err)
	}
}

// TestIntrospectionStillSeesAHiddenRewriteColumn DOCUMENTS A DEFECT, it does
// not assert desired behaviour. PGS-590 records it as unverified and says
// the probe decides whether the ticket is HIGH or MEDIUM; this is the probe.
//
// A table under an online rewrite carries a working column that the router
// hides: `select *` is rewritten to name the visible columns, and naming
// the column directly is refused as if it did not exist. Introspection goes
// somewhere else entirely. Explicit pg_catalog and information_schema
// relations short-circuit in the planner before any placement lookup
// (planner.go, the system-schema branch), so they route to the home shard
// untouched, and hide.go only ever rewrites the user table's own column
// lists.
//
// The result is that a client is told two different things about one table
// by one router. Anything that reads the schema and then writes SQL from it
// -- an ORM's model check, a migration diff, a schema dump -- sees a column
// the router will then refuse to let it name.
//
// When PGS-590 is fixed this test should fail. Invert it then: it exists so
// the fix has something to flip rather than something to remember.
func TestIntrospectionStillSeesAHiddenRewriteColumn(t *testing.T) {
	p := New()
	snap := rewriteFixture(t, "tenant_id", "id", "amount")
	ctx := context.Background()

	// The hiding that does work, as the contrast.
	pl, err := p.Plan(ctx, session(snap), "select * from orders where tenant_id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(pl.Rewritten, "_pgshard_amount_deadbeef") || pl.Rewritten == "" {
		t.Fatalf("the user-table path must hide the working column, got %q", pl.Rewritten)
	}

	// The same router, asked what columns the table has, does nothing at
	// all: no rewrite, and no filter on the rows the home shard will
	// return -- which include the working column, because it is a real
	// column on the real table.
	for _, sql := range []string{
		"select column_name from information_schema.columns where table_name = 'orders'",
		"select attname from pg_catalog.pg_attribute where attrelid = 'orders'::regclass",
	} {
		pl, err := p.Plan(ctx, session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if pl.Rewritten != "" {
			t.Fatalf("introspection is rewritten after all -- PGS-590 may be fixed; invert this test: %q", pl.Rewritten)
		}
		if pl.Kind != Unsharded {
			t.Fatalf("%s: kind = %v, want Unsharded (the home shard's own catalog)", sql, pl.Kind)
		}
	}
}
