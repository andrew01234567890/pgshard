package plan

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestDDLClassification pins the migration shape (kind, scope, strategy)
// or the refusal of every DDL/DCL form the router accepts.
func TestDDLClassification(t *testing.T) {
	cases := []struct {
		sql string
		// mig is "<kind> <scope>[ concurrent]"; refuse is the refusal
		// message prefix instead.
		mig    string
		refuse string
		object string
		// steps lists the skip-check kinds of a multistep plan, "!" marking
		// a step that runs outside a transaction.
		steps string
	}{
		{sql: "create table t1 (id int primary key)", mig: "CREATE TABLE home", object: "relation:t1:present"},
		{sql: "create table if not exists orders (tenant_id int8, id int, primary key (tenant_id, id))", mig: "CREATE TABLE all", object: "relation:orders:present"},
		{sql: "create table regions (id int primary key)", mig: "CREATE TABLE all", object: "relation:regions:present"},
		{sql: "create table orders (id int primary key, tenant_id int8)", refuse: "primary key or unique constraint (id) on sharded table"},
		{sql: "create table orders (id int)", refuse: "sharded table \"orders\" must define its shard key column"},
		{sql: "create unlogged table t2 (id int)", mig: "CREATE TABLE home", object: "relation:t2:present"},
		{sql: "create index items_name on items (name)", mig: "CREATE INDEX home", object: "relation:items_name:present"},
		{sql: "create index concurrently orders_id on orders (id)", mig: "CREATE INDEX all concurrent", object: "relation:orders_id:present"},
		{sql: "create unique index on orders (tenant_id, id)", mig: "CREATE INDEX all"},
		{sql: "create unique index on orders (id)", refuse: "primary key or unique constraint (id) on sharded table"},
		{sql: "create index on regions (name)", mig: "CREATE INDEX all"},
		{sql: "drop index concurrently orders_id", mig: "DROP INDEX existing concurrent", object: "relation:orders_id:absent"},
		{sql: "drop index if exists items_name", mig: "DROP INDEX existing concurrent", object: "relation:items_name:absent"},
		{sql: "drop index a, b", mig: "DROP INDEX existing"},
		{sql: "drop index a cascade", mig: "DROP INDEX existing", object: "relation:a:absent"},
		{sql: "reindex index orders_id", mig: "REINDEX existing concurrent"},
		{sql: "reindex table concurrently orders", mig: "REINDEX all concurrent"},
		{sql: "reindex table items", mig: "REINDEX home concurrent"},
		{sql: "reindex schema public", mig: "REINDEX all concurrent"},
		{sql: "reindex system app", mig: "REINDEX all"},
		{sql: "alter table orders add column note text default 'x'", mig: "ALTER TABLE all"},
		{sql: "alter table orders add column created timestamptz default now()", mig: "ALTER TABLE all"},
		{sql: "alter table orders add column created timestamptz default current_timestamp", mig: "ALTER TABLE all"},
		{sql: "alter table orders add column token uuid default gen_random_uuid()", mig: "ALTER TABLE all rewrite"},
		{sql: "alter table orders add column seen timestamptz default clock_timestamp()", mig: "ALTER TABLE all rewrite"},
		{sql: "alter table orders add column n bigint generated always as identity", refuse: "rewrite-class DDL is not available yet: ADD COLUMN ... GENERATED AS IDENTITY"},
		{sql: "alter table orders add column n serial", refuse: "rewrite-class DDL is not available yet: ADD COLUMN of a serial type"},
		{sql: "alter table orders add column total numeric generated always as (qty * price) stored", refuse: "rewrite-class DDL is not available yet: ADD COLUMN ... GENERATED ... STORED"},
		{sql: "alter table orders add column total numeric generated always as (qty * price) virtual", mig: "ALTER TABLE all"},
		{sql: "alter table orders alter column id type bigint", mig: "ALTER TABLE all rewrite"},
		{sql: "alter table orders alter column tenant_id type text", refuse: "the shard key column \"tenant_id\" of sharded table \"orders\" cannot be dropped"},
		{sql: "alter table items set unlogged", refuse: "rewrite-class DDL is not available yet: SET LOGGED / SET UNLOGGED"},
		{sql: "alter table items set tablespace fast", refuse: "rewrite-class DDL is not available yet: SET TABLESPACE"},
		{sql: "alter table orders drop column tenant_id", refuse: "the shard key column \"tenant_id\" of sharded table \"orders\" cannot be dropped"},
		{sql: "alter table orders drop column note", mig: "ALTER TABLE all"},
		{sql: "alter table orders rename column tenant_id to t", refuse: "the shard key column \"tenant_id\" of sharded table \"orders\" cannot be dropped"},
		{sql: "alter table orders rename column note to memo", mig: "ALTER TABLE all"},
		{sql: "alter table orders add primary key (id)", refuse: "primary key or unique constraint (id) on sharded table"},
		{sql: "alter table orders add primary key (tenant_id, id)", mig: "ALTER TABLE all multistep", steps: "notnull notnull_valid notnull notnull_valid index_valid! constraint"},
		{sql: "alter table orders add constraint u unique (tenant_id, sku)", mig: "ALTER TABLE all multistep", steps: "index_valid! constraint"},
		{sql: "alter table items add unique (sku) include (name)", mig: "ALTER TABLE home multistep", steps: "index_valid! constraint"},
		{sql: "alter table items add primary key using index items_pkey", mig: "ALTER TABLE home"},
		{sql: "alter table items add constraint u unique (a) with (fillfactor = 70)", mig: "ALTER TABLE home"},
		{sql: "alter table orders add check (amount > 0)", mig: "ALTER TABLE all multistep", steps: "constraint constraint_valid"},
		{sql: "alter table orders add constraint c check (amount > 0) not valid", mig: "ALTER TABLE all"},
		{sql: "alter table orders add foreign key (region) references regions (id)", mig: "ALTER TABLE all multistep", steps: "constraint constraint_valid"},
		{sql: "alter table orders add foreign key (tenant_id, doc) references docs (slug, id)", mig: "ALTER TABLE all multistep", steps: "constraint constraint_valid"},
		{sql: "alter table orders add foreign key (doc) references docs (id)", refuse: "cross-shard foreign key: the foreign key from sharded table \"orders\" must map its shard key"},
		{sql: "alter table orders add foreign key (item) references items (id)", refuse: "cross-shard foreign key: sharded table \"orders\" cannot reference unsharded table"},
		{sql: "alter table items add foreign key (o) references orders (id)", refuse: "cross-shard foreign key: an unsharded table cannot reference sharded table"},
		{sql: "alter table regions add foreign key (o) references orders (id)", refuse: "cross-shard foreign key: reference table \"regions\" can only reference another reference table"},
		{sql: "alter table items add foreign key (region) references regions (id) not valid", mig: "ALTER TABLE home"},
		{sql: "alter table orders alter column note set not null", mig: "ALTER TABLE all multistep", steps: "notnull notnull_valid"},
		{sql: "alter table orders alter column note drop not null", mig: "ALTER TABLE all"},
		{sql: "alter table orders detach partition orders_1", mig: "ALTER TABLE all multistep", steps: "detach_pending! detached"},
		{sql: "alter table orders detach partition orders_1 concurrently", mig: "ALTER TABLE all"},
		{sql: "alter table orders add column g int generated always as (id * 2) virtual", mig: "ALTER TABLE all"},
		{sql: "alter table orders add column a int, add check (a > 0)", refuse: "ALTER TABLE with several actions of which one is applied online in steps"},
		{sql: "alter table orders add column sku text unique", refuse: "primary key or unique constraint (sku) on sharded table"},
		{sql: "alter table items add column sku text unique", mig: "ALTER TABLE home"},
		{sql: "alter table items rename to stock", mig: "ALTER TABLE home"},
		{sql: "alter table orders rename to o2", refuse: "renaming the sharded or reference table \"orders\""},
		{sql: "alter table orders set schema archive", refuse: "moving the sharded or reference table \"orders\" to another schema"},
		{sql: "alter table items set schema archive", mig: "ALTER TABLE home"},
		{sql: "alter table orders owner to app", mig: "ALTER TABLE all"},
		{sql: "alter index orders_id rename to orders_id2", mig: "ALTER INDEX existing"},
		{sql: "alter sequence s restart", mig: "ALTER SEQUENCE all"},
		{sql: "drop table orders, docs", mig: "DROP TABLE all"},
		{sql: "drop table items, orders", refuse: "one DDL statement cannot touch both sharded and unsharded tables"},
		{sql: "drop table if exists gone", mig: "DROP TABLE home", object: "relation:gone:absent"},
		{sql: "create schema audit", mig: "CREATE SCHEMA all", object: "schema:audit:present"},
		{sql: "drop schema audit cascade", mig: "DROP SCHEMA all", object: "schema:audit:absent"},
		{sql: "create sequence invoice_no", mig: "CREATE SEQUENCE all", object: "relation:invoice_no:present"},
		{sql: "drop sequence invoice_no", mig: "DROP SEQUENCE all"},
		{sql: "create type mood as enum ('sad', 'ok')", mig: "CREATE TYPE all"},
		{sql: "create type pair as (a int, b int)", mig: "CREATE TYPE all"},
		{sql: "alter type mood add value 'happy'", mig: "ALTER TYPE all"},
		{sql: "drop type mood", mig: "DROP TYPE all"},
		{sql: "create view v as select * from items", mig: "CREATE VIEW home", object: "relation:v:present"},
		{sql: "create or replace view v as select * from orders where tenant_id = 1", mig: "CREATE VIEW all", object: "relation:v:present"},
		{sql: "drop view v", mig: "DROP VIEW existing", object: "relation:v:absent"},
		{sql: "create database shop", mig: "CREATE DATABASE all", object: "database:shop:present"},
		{sql: "drop database shop", mig: "DROP DATABASE all", object: "database:shop:absent"},
		{sql: "drop database app", refuse: "cannot drop the currently open database"},
		{sql: "create role analyst login", mig: "CREATE ROLE all", object: "role:analyst:present"},
		{sql: "create user analyst password 'secret'", mig: "CREATE ROLE all", object: "role:analyst:present"},
		{sql: "alter role analyst password 'other'", mig: "ALTER ROLE all"},
		{sql: "alter role analyst set search_path = app", mig: "ALTER ROLE all"},
		{sql: "drop role analyst", mig: "DROP ROLE all", object: "role:analyst:absent"},
		{sql: "grant select on orders to analyst", mig: "GRANT all"},
		{sql: "grant select on items to analyst", mig: "GRANT home"},
		{sql: "grant select on items, orders to analyst", refuse: "one DDL statement cannot touch both sharded and unsharded tables"},
		{sql: "revoke all on schema public from analyst", mig: "REVOKE all"},
		{sql: "grant all on all tables in schema public to analyst", mig: "GRANT all"},
		{sql: "grant analyst to app", mig: "GRANT ROLE all"},
		{sql: "revoke analyst from app", mig: "REVOKE ROLE all"},
		{sql: "create role boss superuser", refuse: "roles with the SUPERUSER attribute are not available through the router"},
		{sql: "alter role analyst replication", refuse: "roles with the REPLICATION attribute are not available through the router"},
		{sql: "create user x bypassrls", refuse: "roles with the BYPASSRLS attribute are not available through the router"},
		{sql: "create user x nosuperuser", mig: "CREATE ROLE all", object: "role:x:present"},
		{sql: "alter default privileges in schema public grant select on tables to analyst", refuse: "ALTER DEFAULT PRIVILEGES is not available through the router"},
		{sql: "reassign owned by analyst to app", refuse: "REASSIGN OWNED is not available through the router"},
		{sql: "drop owned by analyst", refuse: "DROP OWNED is not available through the router"},
		{sql: "alter role analyst rename to analyst2", refuse: "renaming a role is not available through the router"},
		{sql: "alter role analyst set work_mem from current", refuse: "ALTER ROLE ... SET ... FROM CURRENT is not available"},
		{sql: "alter role analyst in database app set work_mem = '64MB'", mig: "ALTER ROLE all"},
		{sql: "grant usage on schema public to analyst", mig: "GRANT all"},
		{sql: "grant select (id), update (name) on items to analyst, public", mig: "GRANT home"},
		{sql: "revoke grant option for select on orders from analyst", mig: "REVOKE all"},
	}
	if len(cases) < 30 {
		t.Fatalf("only %d cases", len(cases))
	}
	p := New()
	snap := fixture(t)
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			pl, err := p.Plan(context.Background(), session(snap), c.sql)
			if c.refuse != "" {
				if err == nil {
					t.Fatalf("expected refusal %q, got %+v", c.refuse, pl.Migration)
				}
				var pe *pgwire.Error
				if !errors.As(err, &pe) || !strings.HasPrefix(pe.Message, c.refuse) {
					t.Fatalf("refusal = %v, want prefix %q", err, c.refuse)
				}
				return
			}
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if pl.Kind != MigrationKind || len(pl.Shards) != 0 || !pl.Class.Write {
				t.Fatalf("plan = %+v, want a migration write plan", pl)
			}
			if got := migrationShape(pl); got != c.mig {
				t.Fatalf("migration = %q, want %q", got, c.mig)
			}
			if pl.Migration.Statement == "" {
				t.Fatal("migration has no statement")
			}
			o := pl.Migration.Object
			got := ""
			if o.Kind != "" {
				got = o.Kind + ":" + o.Name + ":" + o.Expect
			}
			if got != c.object {
				t.Fatalf("object = %q, want %q", got, c.object)
			}
			if got := stepShape(pl.Migration.Steps); got != c.steps {
				t.Fatalf("steps = %q, want %q", got, c.steps)
			}
		})
	}
}

func TestDDLRolePasswordBecomesVerifier(t *testing.T) {
	p := New()
	snap := fixture(t)
	for _, sql := range []string{"create user analyst password 'secret'", "alter role analyst with password 'secret' login"} {
		pl, err := p.Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatal(err)
		}
		m := pl.Migration
		if m.Role != "analyst" || m.Verifier == "" {
			t.Fatalf("%s: role %q verifier %q", sql, m.Role, m.Verifier)
		}
		v, err := pgwire.ParseSCRAMVerifier(m.Verifier)
		if err != nil {
			t.Fatalf("%s: verifier %q: %v", sql, m.Verifier, err)
		}
		same, err := pgwire.BuildSCRAMVerifier("secret", v.Salt, v.Iterations)
		if err != nil || same.String() != m.Verifier {
			t.Fatalf("%s: verifier does not match the password", sql)
		}
		if strings.Contains(m.Statement, "'secret'") || !strings.Contains(m.Statement, m.Verifier) {
			t.Fatalf("%s: statement %q still carries the plaintext password", sql, m.Statement)
		}
		if _, err := p.Plan(context.Background(), session(snap), m.Statement); err != nil {
			t.Fatalf("rewritten statement %q does not parse: %v", m.Statement, err)
		}
	}
	pl, err := p.Plan(context.Background(), session(snap), "create role r2 login")
	if err != nil || pl.Migration.Verifier != "" || pl.Migration.Statement != "create role r2 login" {
		t.Fatalf("no password: %+v %v", pl.Migration, err)
	}
	built, err := pgwire.BuildSCRAMVerifier("x", nil, pgwire.DefaultSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	pre := built.String()
	pl, err = p.Plan(context.Background(), session(snap), "create role r3 password '"+pre+"'")
	if err != nil || pl.Migration.Verifier != pre {
		t.Fatalf("pre-hashed verifier: %+v %v", pl.Migration, err)
	}
}

func TestRoleAndGrantNormalization(t *testing.T) {
	p := New()
	snap := fixture(t)
	plan := func(sql string) *catalog.RoleChanges {
		t.Helper()
		pl, err := p.Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return pl.Migration.Roles
	}
	js := func(v any) string {
		b, _ := json.Marshal(v)
		return string(b)
	}
	cases := []struct{ sql, want string }{
		{"create role analyst", `{"attributes":{"login":false}}`},
		{"create user analyst connection limit 5 valid until '2030-01-01' in role readers, writers admin boss", `{"attributes":{"connection_limit":5,"valid_until":"2030-01-01"},"grant_members":[{"role":"readers","member":"analyst"},{"role":"writers","member":"analyst"},{"role":"analyst","member":"boss","admin":true}]}`},
		{"create role g nologin nocreatedb createrole noinherit valid until 'infinity'", `{"attributes":{"login":false,"createdb":false,"createrole":true,"inherit":false,"valid_until":""}}`},
		{"alter role analyst password 'pw'", `null`},
		{"alter role analyst login", `{"attributes":{"login":true}}`},
		{"drop role a, b", `{"drop_roles":["a","b"]}`},
		{"grant readers to analyst with admin option", `{"grant_members":[{"role":"readers","member":"analyst","admin":true}]}`},
		{"grant readers, writers to a, b", `{"grant_members":[{"role":"readers","member":"a"},{"role":"readers","member":"b"},{"role":"writers","member":"a"},{"role":"writers","member":"b"}]}`},
		{"revoke admin option for readers from analyst", `{"revoke_members":[{"role":"readers","member":"analyst","admin_only":true}]}`},
		{"revoke readers from analyst", `{"revoke_members":[{"role":"readers","member":"analyst"}]}`},
		{"grant select on orders to analyst", `{"grants":[{"kind":"table","name":"orders","grantee":"analyst","privileges":["SELECT"]}]}`},
		{"grant all on table public.orders to analyst with grant option", `{"grants":[{"kind":"table","schema":"public","name":"orders","grantee":"analyst","privileges":["DELETE","INSERT","MAINTAIN","REFERENCES","SELECT","TRIGGER","TRUNCATE","UPDATE"],"grant_option":true}]}`},
		{"grant select, insert on orders, docs to analyst, public", `{"grants":[{"kind":"table","name":"orders","grantee":"analyst","privileges":["INSERT","SELECT"]},{"kind":"table","name":"docs","grantee":"analyst","privileges":["INSERT","SELECT"]}]}`},
		{"grant select (id), update (id, name) on items to analyst", `{"grants":[{"kind":"table","name":"items","column":"id","grantee":"analyst","privileges":["SELECT","UPDATE"]},{"kind":"table","name":"items","column":"name","grantee":"analyst","privileges":["UPDATE"]}]}`},
		{"grant all (name) on items to analyst", `{"grants":[{"kind":"table","name":"items","column":"name","grantee":"analyst","privileges":["INSERT","REFERENCES","SELECT","UPDATE"]}]}`},
		{"grant usage, create on schema audit to analyst", `{"grants":[{"kind":"schema","name":"audit","grantee":"analyst","privileges":["CREATE","USAGE"]}]}`},
		{"grant all on database app to analyst", `{"grants":[{"kind":"database","name":"app","grantee":"analyst","privileges":["CONNECT","CREATE","TEMPORARY"]}]}`},
		{"grant usage on sequence invoice_no to analyst", `{"grants":[{"kind":"sequence","name":"invoice_no","grantee":"analyst","privileges":["USAGE"]}]}`},
		{"grant execute on function audit.f(int) to analyst", `{"grants":[{"kind":"function","schema":"audit","name":"f","grantee":"analyst","privileges":["EXECUTE"]}]}`},
		{"grant usage on type mood to analyst", `{"grants":[{"kind":"type","name":"mood","grantee":"analyst","privileges":["USAGE"]}]}`},
		{"grant select on all tables in schema public to analyst", `null`},
		{"grant select on orders to public", `null`},
		{"revoke select, update on orders from analyst cascade", `{"revokes":[{"kind":"table","name":"orders","grantee":"analyst","privileges":["SELECT","UPDATE"]}]}`},
		{"revoke grant option for select on orders from analyst", `{"revokes":[{"kind":"table","name":"orders","grantee":"analyst","privileges":["SELECT"],"grant_option":true}]}`},
		{"alter role analyst set search_path = app, public", `{"settings":[{"role":"analyst","name":"search_path","value":"app, public"}]}`},
		{"alter role analyst in database app set work_mem to '64MB'", `{"settings":[{"role":"analyst","database":"app","name":"work_mem","value":"64MB"}]}`},
		{"alter role analyst set statement_timeout = 5000", `{"settings":[{"role":"analyst","name":"statement_timeout","value":"5000"}]}`},
		{"alter role analyst reset work_mem", `{"settings":[{"role":"analyst","name":"work_mem","reset":true}]}`},
		{"alter role analyst set work_mem to default", `{"settings":[{"role":"analyst","name":"work_mem","reset":true}]}`},
		{"alter role analyst reset all", `{"settings":[{"role":"analyst","reset_all":true}]}`},
		{"alter role all set work_mem = '1MB'", `null`},
	}
	for _, c := range cases {
		if got := js(plan(c.sql)); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.sql, got, c.want)
		}
	}
}

func stepShape(steps []Step) string {
	var parts []string
	for _, s := range steps {
		p := s.Skip.Kind
		if s.Concurrent {
			p += "!"
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, " ")
}

// TestDDLStepsSQL pins the statements of the multistep plans and their
// deterministic constraint names.
func TestDDLStepsSQL(t *testing.T) {
	p := New()
	snap := fixture(t)
	cases := []struct {
		sql   string
		steps []string
	}{
		{"alter table orders add check (amount > 0 and qty < 5)", []string{
			"ALTER TABLE orders ADD CONSTRAINT orders_amount_qty_check CHECK (amount > 0 AND qty < 5) NOT VALID",
			`ALTER TABLE "orders" VALIDATE CONSTRAINT "orders_amount_qty_check" | ALTER TABLE "orders" DROP CONSTRAINT IF EXISTS "orders_amount_qty_check"`}},
		{"alter table public.orders add constraint fk foreign key (region) references regions (id) on delete cascade", []string{
			"ALTER TABLE public.orders ADD CONSTRAINT fk FOREIGN KEY (region) REFERENCES regions (id) ON DELETE CASCADE NOT VALID",
			`ALTER TABLE "public"."orders" VALIDATE CONSTRAINT "fk" | ALTER TABLE "public"."orders" DROP CONSTRAINT IF EXISTS "fk"`}},
		{"alter table orders alter column note set not null", []string{
			`ALTER TABLE "orders" ADD CONSTRAINT "orders_note_not_null" NOT NULL "note" NOT VALID`,
			`ALTER TABLE "orders" VALIDATE CONSTRAINT "orders_note_not_null" | ALTER TABLE "orders" DROP CONSTRAINT IF EXISTS "orders_note_not_null"`}},
		{"alter table orders add constraint pk primary key (tenant_id, id) deferrable initially deferred", []string{
			`ALTER TABLE "orders" ADD CONSTRAINT "orders_tenant_id_not_null" NOT NULL "tenant_id" NOT VALID`,
			`ALTER TABLE "orders" VALIDATE CONSTRAINT "orders_tenant_id_not_null" | ALTER TABLE "orders" DROP CONSTRAINT IF EXISTS "orders_tenant_id_not_null"`,
			`ALTER TABLE "orders" ADD CONSTRAINT "orders_id_not_null" NOT NULL "id" NOT VALID`,
			`ALTER TABLE "orders" VALIDATE CONSTRAINT "orders_id_not_null" | ALTER TABLE "orders" DROP CONSTRAINT IF EXISTS "orders_id_not_null"`,
			`CREATE UNIQUE INDEX CONCURRENTLY "pk" ON "orders" ("tenant_id", "id") #pk`,
			`ALTER TABLE "orders" ADD CONSTRAINT "pk" PRIMARY KEY USING INDEX "pk" DEFERRABLE INITIALLY DEFERRED`}},
		{"alter table orders add unique nulls not distinct (tenant_id, sku) include (x)", []string{
			`CREATE UNIQUE INDEX CONCURRENTLY "orders_tenant_id_sku_key" ON "orders" ("tenant_id", "sku") INCLUDE ("x") NULLS NOT DISTINCT #orders_tenant_id_sku_key`,
			`ALTER TABLE "orders" ADD CONSTRAINT "orders_tenant_id_sku_key" UNIQUE USING INDEX "orders_tenant_id_sku_key"`}},
		{"alter table orders detach partition orders_1", []string{
			`ALTER TABLE "orders" DETACH PARTITION "orders_1" CONCURRENTLY`,
			`ALTER TABLE "orders" DETACH PARTITION "orders_1" FINALIZE`}},
	}
	for _, c := range cases {
		pl, err := p.Plan(context.Background(), session(snap), c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		var got []string
		for _, s := range pl.Migration.Steps {
			line := s.SQL
			if s.OnFail != "" {
				line += " | " + s.OnFail
			}
			if s.Index != "" {
				line += " #" + s.Index
			}
			got = append(got, line)
		}
		if strings.Join(got, "\n") != strings.Join(c.steps, "\n") {
			t.Errorf("%s:\n%s\nwant:\n%s", c.sql, strings.Join(got, "\n"), strings.Join(c.steps, "\n"))
		}
		for _, s := range pl.Migration.Steps {
			if _, err := p.Plan(context.Background(), session(snap), s.SQL); err != nil {
				t.Errorf("%s: step %q does not parse: %v", c.sql, s.SQL, err)
			}
		}
	}
}

func TestAutoNameIsDeterministicAndFits(t *testing.T) {
	long := strings.Repeat("column_", 12)
	a := autoName("a_rather_long_table_name", []string{long, "b"}, "check")
	b := autoName("a_rather_long_table_name", []string{long, "b"}, "check")
	if a != b || len(a) > 63 || !strings.HasSuffix(a, "_check") || !strings.HasPrefix(a, "a_rather_long_table_name_column_") {
		t.Fatalf("auto name %q (%d bytes)", a, len(a))
	}
	if other := autoName("a_rather_long_table_name", []string{long, "c"}, "check"); other == a {
		t.Fatalf("names for different columns collide: %q", a)
	}
	if got := autoName("t", []string{"a", "b"}, "fkey"); got != "t_a_b_fkey" {
		t.Fatalf("short name %q", got)
	}
	if got := autoName("t", nil, "pkey"); got != "t_pkey" {
		t.Fatalf("pkey %q", got)
	}
	exact := autoName(strings.Repeat("t", 63-len("_pkey")), nil, "pkey")
	if len(exact) != 63 || strings.Contains(exact, "_pkey") != true || exact != strings.Repeat("t", 58)+"_pkey" {
		t.Fatalf("63-byte name was changed: %q", exact)
	}
	over := autoName(strings.Repeat("t", 64-len("_pkey")), nil, "pkey")
	if len(over) > 63 || !strings.HasSuffix(over, "_pkey") || over == strings.Repeat("t", 59)+"_pkey" {
		t.Fatalf("64-byte name not hashed: %q (%d bytes)", over, len(over))
	}
	u := autoName(strings.Repeat("é", 40), nil, "key")
	if len(u) > 63 || !utf8.ValidString(u) {
		t.Fatalf("multibyte name %q (%d bytes)", u, len(u))
	}
}

func TestDetachStepsCarryThePartitionSchema(t *testing.T) {
	p := New()
	snap := fixture(t)
	pl, err := p.Plan(context.Background(), session(snap), "alter table orders detach partition public.orders_1")
	if err != nil {
		t.Fatal(err)
	}
	steps := pl.Migration.Steps
	if len(steps) != 2 {
		t.Fatalf("steps: %+v", steps)
	}
	for _, s := range steps {
		if s.Skip.Name != "orders_1" || s.Skip.NameSchema != "public" {
			t.Fatalf("%s check ignores the partition schema: %+v", s.Skip.Kind, s.Skip)
		}
	}
}
