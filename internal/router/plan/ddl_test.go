package plan

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

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
		{sql: "drop index if exists items_name", mig: "DROP INDEX existing", object: "relation:items_name:absent"},
		{sql: "reindex index orders_id", mig: "REINDEX existing"},
		{sql: "reindex table concurrently orders", mig: "REINDEX all concurrent"},
		{sql: "reindex table items", mig: "REINDEX home"},
		{sql: "reindex schema public", mig: "REINDEX all"},
		{sql: "alter table orders add column note text default 'x'", mig: "ALTER TABLE all"},
		{sql: "alter table orders add column created timestamptz default now()", mig: "ALTER TABLE all"},
		{sql: "alter table orders add column created timestamptz default current_timestamp", mig: "ALTER TABLE all"},
		{sql: "alter table orders add column token uuid default gen_random_uuid()", refuse: "rewrite-class DDL is not available yet: ADD COLUMN with a volatile DEFAULT"},
		{sql: "alter table orders add column seen timestamptz default clock_timestamp()", refuse: "rewrite-class DDL is not available yet: ADD COLUMN with a volatile DEFAULT"},
		{sql: "alter table orders add column n bigint generated always as identity", refuse: "rewrite-class DDL is not available yet: ADD COLUMN ... GENERATED AS IDENTITY"},
		{sql: "alter table orders add column n serial", refuse: "rewrite-class DDL is not available yet: ADD COLUMN of a serial type"},
		{sql: "alter table orders add column total numeric generated always as (qty * price) stored", refuse: "rewrite-class DDL is not available yet: ADD COLUMN ... GENERATED ... STORED"},
		{sql: "alter table orders add column total numeric generated always as (qty * price) virtual", mig: "ALTER TABLE all"},
		{sql: "alter table orders alter column id type bigint", refuse: "rewrite-class DDL is not available yet: ALTER COLUMN ... TYPE"},
		{sql: "alter table items set unlogged", refuse: "rewrite-class DDL is not available yet: SET LOGGED / SET UNLOGGED"},
		{sql: "alter table items set tablespace fast", refuse: "rewrite-class DDL is not available yet: SET TABLESPACE"},
		{sql: "alter table orders drop column tenant_id", refuse: "the shard key column \"tenant_id\" of sharded table \"orders\" cannot be dropped"},
		{sql: "alter table orders drop column note", mig: "ALTER TABLE all"},
		{sql: "alter table orders rename column tenant_id to t", refuse: "the shard key column \"tenant_id\" of sharded table \"orders\" cannot be dropped"},
		{sql: "alter table orders rename column note to memo", mig: "ALTER TABLE all"},
		{sql: "alter table orders add primary key (id)", refuse: "primary key or unique constraint (id) on sharded table"},
		{sql: "alter table orders add constraint u unique (tenant_id, sku)", mig: "ALTER TABLE all"},
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
