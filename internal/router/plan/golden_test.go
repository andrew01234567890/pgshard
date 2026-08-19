package plan

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// want is the expected plan of one statement. Shards are named by the key
// value they should hash to ("k:1", "k:acme"), by number, or "all".
type want struct {
	kind   Kind
	shards string
	// msg is the refusal message prefix for kind Refuse.
	msg string
	// deferred plans carry parameters; values resolves them at "bind".
	values map[int32]any
	sql    string
}

func golden() []want {
	return []want{
		// Unsharded and session-local statements.
		{sql: "select 1", kind: Unsharded, shards: "0"},
		{sql: "select * from items", kind: Unsharded, shards: "0"},
		{sql: "select * from public.items where id = 3", kind: Unsharded, shards: "0"},
		{sql: "insert into items values (1, 'x')", kind: Unsharded, shards: "0"},
		{sql: "update items set name = 'y' where id = 1", kind: Unsharded, shards: "0"},
		{sql: "delete from items", kind: Unsharded, shards: "0"},
		{sql: "select * from items i join items j on i.id = j.id", kind: Unsharded, shards: "0"},
		{sql: "select * from pg_catalog.pg_class", kind: Unsharded, shards: "0"},
		{sql: "select * from information_schema.tables", kind: Unsharded, shards: "0"},
		{sql: "select * from undeclared_table", kind: Unsharded, shards: "0"},
		{sql: "select * from other.orders where tenant_id = 1", kind: Unsharded, shards: "0"},
		{sql: "create table items (id int primary key, name text)", kind: Unsharded, shards: "0"},
		{sql: "create table newtab (id int primary key)", kind: Unsharded, shards: "0"},
		{sql: "create index on items (name)", kind: Unsharded, shards: "0"},
		{sql: "alter table items add column x int", kind: Unsharded, shards: "0"},
		{sql: "drop table items", kind: Unsharded, shards: "0"},
		{sql: "drop table if exists nothing_here", kind: Unsharded, shards: "0"},
		{sql: "truncate items", kind: Unsharded, shards: "0"},
		{sql: "copy items from stdin", kind: Unsharded, shards: "0"},
		{sql: "copy items to stdout", kind: Unsharded, shards: "0"},
		{sql: "copy (select * from items) to stdout", kind: Unsharded, shards: "0"},
		{sql: "explain select * from items", kind: Unsharded, shards: "0"},
		{sql: "prepare p as select * from items where id = $1", kind: Unsharded, shards: "0"},
		{sql: "create view v as select * from items", kind: Unsharded, shards: "0"},
		{sql: "vacuum items", kind: Unsharded, shards: "0"},
		{sql: "lock table items", kind: Unsharded, shards: "0"},
		{sql: "select nextval('s')", kind: Unsharded, shards: "0"},
		{sql: "this is not sql", kind: Unsharded, shards: "0"},
		{sql: "begin", kind: SessionLocal},
		{sql: "commit", kind: SessionLocal},
		{sql: "rollback", kind: SessionLocal},
		{sql: "savepoint a", kind: SessionLocal},
		{sql: "set application_name to 'x'", kind: SessionLocal},
		{sql: "set local search_path to app", kind: Refuse, msg: "SET LOCAL search_path"},
		{sql: "set search_path = 1", kind: Refuse, msg: "SET search_path with an unrecognised argument list"},
		{sql: "set local work_mem to '1MB'", kind: SessionLocal},
		{sql: "reset all", kind: SessionLocal},
		{sql: "show search_path", kind: SessionLocal},
		{sql: "discard all", kind: SessionLocal},
		{sql: "deallocate p", kind: SessionLocal},
		{sql: "execute p(1)", kind: SessionLocal},
		{sql: "close c", kind: SessionLocal},
		{sql: "fetch 10 from c", kind: SessionLocal},
		{sql: "", kind: SessionLocal},

		// Reference tables.
		{sql: "select * from regions", kind: Reference, shards: "ref"},
		{sql: "select r.name from regions r where r.id = 1", kind: Reference, shards: "ref"},
		{sql: "select * from regions r join regions s on r.id = s.parent", kind: Reference, shards: "ref"},
		{sql: "select * from items i join regions r on i.region = r.id", kind: Unsharded, shards: "0"},
		{sql: "insert into regions values (1, 'eu')", kind: Refuse, msg: "writes to reference tables are not available yet"},
		{sql: "update regions set name = 'x'", kind: Refuse, msg: "writes to reference tables are not available yet"},
		{sql: "delete from regions where id = 1", kind: Refuse, msg: "writes to reference tables are not available yet"},
		{sql: "copy regions from stdin", kind: Refuse, msg: "COPY on sharded and reference tables is not available yet"},
		{sql: "create table regions (id int primary key)", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "drop table regions", kind: Refuse, msg: "DDL fan-out is not available yet"},

		// Sharded: equality on the int8 key.
		{sql: "select * from orders where tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders where 1 = tenant_id", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders where tenant_id = 42", kind: EqualUnique, shards: "k:42"},
		{sql: "select * from orders where tenant_id = 7 and status = 'open'", kind: EqualUnique, shards: "k:7"},
		{sql: "select * from orders where status = 'open' and (tenant_id = 7)", kind: EqualUnique, shards: "k:7"},
		{sql: "select * from orders o where o.tenant_id = 100", kind: EqualUnique, shards: "k:100"},
		{sql: "select * from orders where orders.tenant_id = 100", kind: EqualUnique, shards: "k:100"},
		{sql: "select * from public.orders where tenant_id = 2", kind: EqualUnique, shards: "k:2"},
		{sql: "select * from audit.events where tenant_id = 2", kind: EqualUnique, shards: "k:2"},
		{sql: "select * from orders where tenant_id = 1::int8", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders where tenant_id = cast(3 as bigint)", kind: EqualUnique, shards: "k:3"},
		{sql: "select * from orders where tenant_id = '3'::int8", kind: EqualUnique, shards: "k:3"},
		{sql: "select * from orders where tenant_id = -5", kind: EqualUnique, shards: "k:-5"},
		{sql: "select * from orders where tenant_id = 9223372036854775807", kind: EqualUnique, shards: "k:9223372036854775807"},
		{sql: "select * from orders where tenant_id = 1 and tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select count(*) from orders where tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders where tenant_id = 1 order by id limit 5", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders where tenant_id = 1 for update", kind: EqualUnique, shards: "k:1"},
		{sql: "explain analyze select * from orders where tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "declare c cursor for select * from orders where tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "with recent as (select 1) select * from orders where tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "with recent as (select * from orders where tenant_id = 1) select * from recent", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from (select * from orders where tenant_id = 1) s", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders where tenant_id = 1 union all select * from orders where tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select (select count(*) from order_lines where tenant_id = 1) from orders where tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders where tenant_id = 1 and exists (select 1 from order_lines l where l.tenant_id = 1)", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders where tenant_id = 1 and id in (select order_id from order_lines where tenant_id = 1)", kind: EqualUnique, shards: "k:1"},

		// Sharded: text key.
		{sql: "select * from docs where slug = 'acme'", kind: EqualUnique, shards: "k:acme"},
		{sql: "select * from docs where slug = 'zeta'", kind: EqualUnique, shards: "k:zeta"},
		{sql: "select * from docs where slug = 'a'::text", kind: EqualUnique, shards: "k:a"},
		{sql: "select * from docs where slug = '123'::text", kind: EqualUnique, shards: "k:123s"},
		{sql: "select * from docs where slug = '123'", kind: Refuse, msg: "shard key literal '123' is untyped and looks numeric"},
		{sql: "select * from orders where tenant_id = '1'", kind: Refuse, msg: "shard key literal '1' is untyped and looks numeric"},
		{sql: "select * from docs where slug = 'a' or slug = 'b'", kind: Scatter, shards: "all"},

		// IN lists.
		{sql: "select * from orders where tenant_id in (1)", kind: In, shards: "k:1"},
		{sql: "select * from orders where tenant_id in (1, 42)", kind: In, shards: "k:1"},
		{sql: "select * from orders where tenant_id in (1, 2)", kind: In, shards: "k:1,k:2"},
		{sql: "select * from orders where tenant_id in (1, 2, 7)", kind: In, shards: "k:1,k:2,k:7"},
		{sql: "select * from docs where slug in ('a', 'acme')", kind: In, shards: "k:a,k:acme"},
		{sql: "delete from orders where tenant_id in (1, 42)", kind: In, shards: "k:1"},
		{sql: "update orders set status = 'x' where tenant_id in (1, 2)", kind: In, shards: "k:1,k:2"},
		{sql: "select * from orders where tenant_id in (select tenant_id from items)", kind: Refuse, msg: "cross-shard join"},

		// Parameters: deferred until bind.
		{sql: "select * from orders where tenant_id = $1", kind: EqualUnique, shards: "k:42", values: map[int32]any{1: int64(42)}},
		{sql: "select * from orders where $1 = tenant_id", kind: EqualUnique, shards: "k:42", values: map[int32]any{1: int64(42)}},
		{sql: "select * from orders where tenant_id = $1::int8", kind: EqualUnique, shards: "k:42", values: map[int32]any{1: int64(42)}},
		{sql: "select * from orders where tenant_id = $2 and status = $1", kind: EqualUnique, shards: "k:7", values: map[int32]any{1: "open", 2: int64(7)}},
		{sql: "select * from docs where slug = $1", kind: EqualUnique, shards: "k:acme", values: map[int32]any{1: "acme"}},
		{sql: "select * from orders where tenant_id in ($1, $2)", kind: In, shards: "k:1,k:2", values: map[int32]any{1: int64(1), 2: int64(2)}},
		{sql: "select * from orders where tenant_id in (1, $1)", kind: In, shards: "k:1", values: map[int32]any{1: int64(42)}},
		{sql: "update orders set status = $1 where tenant_id = $2", kind: EqualUnique, shards: "k:7", values: map[int32]any{1: "x", 2: int64(7)}},
		{sql: "delete from orders where tenant_id = $1", kind: EqualUnique, shards: "k:100", values: map[int32]any{1: int64(100)}},
		{sql: "insert into orders (tenant_id, id) values ($1, $2)", kind: EqualUnique, shards: "k:7", values: map[int32]any{1: int64(7), 2: int64(1)}},
		{sql: "insert into orders (id, tenant_id) values ($1, $2), ($3, $2)", kind: EqualUnique, shards: "k:7", values: map[int32]any{1: int64(1), 2: int64(7), 3: int64(2)}},
		{sql: "insert into orders (id, tenant_id) values ($1, $2), ($3, $4)", kind: Refuse, msg: "multi-row INSERT spanning shards", values: map[int32]any{1: int64(1), 2: int64(1), 3: int64(2), 4: int64(2)}},
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id where o.tenant_id = $1", kind: EqualUnique, shards: "k:1", values: map[int32]any{1: int64(1)}},
		{sql: "select * from orders o, order_lines l where o.tenant_id = $1 and l.tenant_id = $2", kind: Refuse, msg: "cross-shard join", values: map[int32]any{1: int64(1), 2: int64(2)}},
		{sql: "select * from orders o left join order_lines l on o.tenant_id = l.tenant_id and l.tenant_id = 7", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o right join order_lines l on o.tenant_id = l.tenant_id and o.tenant_id = 7", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o full join order_lines l on o.tenant_id = l.tenant_id and l.tenant_id in (7, 8)", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o left join order_lines l on o.tenant_id = l.tenant_id and l.tenant_id = 7 where o.tenant_id = 7", kind: EqualUnique, shards: "k:7"},
		{sql: "select * from orders o inner join order_lines l on o.tenant_id = l.tenant_id and l.tenant_id = 7", kind: EqualUnique, shards: "k:7"},
		{sql: "select * from orders where tenant_id = $1 and id = 1", kind: Refuse, msg: "parameter $1 cannot be a shard key", values: map[int32]any{1: nil}},

		// INSERT rules.
		{sql: "insert into orders (tenant_id, id) values (1, 2)", kind: EqualUnique, shards: "k:1"},
		{sql: "insert into orders (id, tenant_id) values (2, 42)", kind: EqualUnique, shards: "k:42"},
		{sql: "insert into orders (id, tenant_id) values (2, 42) returning id", kind: EqualUnique, shards: "k:42"},
		{sql: "insert into orders (id, tenant_id) values (2, 42) on conflict (tenant_id, id) do update set status = 'x'", kind: EqualUnique, shards: "k:42"},
		{sql: "insert into orders (id, tenant_id) values (2, 42) on conflict (tenant_id, id) do update set tenant_id = 1", kind: Refuse, msg: "shard key is immutable: ON CONFLICT"},
		{sql: "insert into orders (tenant_id, id) values (1, 2), (1, 3)", kind: EqualUnique, shards: "k:1"},
		{sql: "insert into orders (tenant_id, id) values (1, 2), (42, 3)", kind: EqualUnique, shards: "k:1"},
		{sql: "insert into orders (tenant_id, id) values (1, 2), (2, 3)", kind: Refuse, msg: "multi-row INSERT spanning shards is not available yet"},
		{sql: "insert into orders values (1, 2)", kind: Refuse, msg: "insert requires the shard key"},
		{sql: "insert into orders (id, status) values (1, 'x')", kind: Refuse, msg: "insert requires the shard key"},
		{sql: "insert into orders (id, tenant_id) values (1)", kind: Refuse, msg: "insert requires the shard key: VALUES row has fewer values than columns"},
		{sql: "insert into orders (id, tenant_id) values (1, default)", kind: Refuse, msg: "shard key of an INSERT must be a constant or a parameter"},
		{sql: "insert into orders (id, tenant_id) values (1, null)", kind: Refuse, msg: "shard key of an INSERT must be a constant or a parameter"},
		{sql: "insert into orders (id, tenant_id) values (1, 1 + 1)", kind: Refuse, msg: "shard key of an INSERT must be a constant or a parameter"},
		{sql: "insert into orders (id, tenant_id) select 1, 2", kind: Refuse, msg: "INSERT ... SELECT into a sharded table is not available yet"},
		{sql: "insert into orders (id, tenant_id) select id, tenant_id from order_lines where tenant_id = 1", kind: Refuse, msg: "INSERT ... SELECT into a sharded table is not available yet"},
		{sql: "insert into docs (slug, body) values ('acme', 'x')", kind: EqualUnique, shards: "k:acme"},
		{sql: "insert into docs (slug, body) values ('123', 'x')", kind: Refuse, msg: "shard key literal '123' is untyped and looks numeric"},
		{sql: "insert into docs (slug, body) values ('123'::text, 'x')", kind: EqualUnique, shards: "k:123s"},
		{sql: "insert into items (id) select id from orders where tenant_id = 1", kind: Refuse, msg: "cross-shard join"},

		// UPDATE and DELETE rules.
		{sql: "update orders set status = 'x' where tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "update orders set status = 'x' where tenant_id = 1 and id = 5", kind: EqualUnique, shards: "k:1"},
		{sql: "update orders set tenant_id = 2 where tenant_id = 1", kind: Refuse, msg: "shard key is immutable"},
		{sql: "update orders set tenant_id = tenant_id where tenant_id = 1", kind: Refuse, msg: "shard key is immutable"},
		{sql: "update orders set status = 'x'", kind: Refuse, msg: "scatter UPDATE without a shard key predicate is not available yet"},
		{sql: "update orders set status = 'x' where id = 5", kind: Refuse, msg: "scatter UPDATE without a shard key predicate is not available yet"},
		{sql: "update orders set status = 'x' where tenant_id = 1 or tenant_id = 2", kind: Refuse, msg: "scatter UPDATE"},
		{sql: "update orders o set status = 'x' from order_lines l where o.tenant_id = 1 and l.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "update orders o set status = 'x' from order_lines l where o.tenant_id = 1 and l.tenant_id = 2", kind: Refuse, msg: "cross-shard join"},
		{sql: "update orders o set status = 'x' from order_lines l where o.tenant_id = l.tenant_id and l.tenant_id = 7", kind: EqualUnique, shards: "k:7"},
		{sql: "update orders o set status = 'x' from items i where o.tenant_id = 1 and i.id = o.item", kind: Refuse, msg: "cross-shard join"},
		{sql: "update docs set body = 'y' where slug = 'acme'", kind: EqualUnique, shards: "k:acme"},
		{sql: "update docs set slug = 'other' where slug = 'acme'", kind: Refuse, msg: "shard key is immutable"},
		{sql: "delete from orders where tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "delete from orders where tenant_id = 1 returning id", kind: EqualUnique, shards: "k:1"},
		{sql: "delete from orders", kind: Refuse, msg: "scatter DELETE without a shard key predicate is not available yet"},
		{sql: "delete from orders where id = 1", kind: Refuse, msg: "scatter DELETE"},
		{sql: "delete from orders o using order_lines l where o.tenant_id = 1 and l.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "delete from orders o using order_lines l where o.tenant_id = 1 and l.tenant_id = 2", kind: Refuse, msg: "cross-shard join"},
		{sql: "delete from docs where slug = 'zeta'", kind: EqualUnique, shards: "k:zeta"},

		// Scatter reads: representable, executed later.
		{sql: "select * from orders", kind: Scatter, shards: "all"},
		{sql: "select id, status from orders where status = 'open'", kind: Scatter, shards: "all"},
		{sql: "select * from orders where tenant_id = 1 or tenant_id = 2", kind: Scatter, shards: "all"},
		{sql: "select * from orders where tenant_id > 1", kind: Scatter, shards: "all"},
		{sql: "select * from orders where tenant_id = id", kind: Scatter, shards: "all"},
		{sql: "select * from orders where tenant_id = abs(-1)", kind: Scatter, shards: "all"},
		{sql: "select * from orders where tenant_id <> 1", kind: Scatter, shards: "all"},
		{sql: "select * from orders where not (tenant_id = 1)", kind: Scatter, shards: "all"},
		{sql: "select * from docs", kind: Scatter, shards: "all"},
		{sql: "explain select * from orders", kind: Refuse, msg: "only a plain SELECT can run on multiple shards"},
		{sql: "select count(*) from orders", kind: Scatter, shards: "all"},
		{sql: "select max(id) from orders", kind: Scatter, shards: "all"},
		{sql: "select avg(id) from orders", kind: Refuse, msg: "multi-shard avg() is not available yet"},
		{sql: "select count(*) + 1 from orders", kind: Refuse, msg: "multi-shard aggregates must be top-level"},
		{sql: "select count(distinct id) from orders", kind: Refuse, msg: "multi-shard aggregates with DISTINCT, FILTER, ORDER BY or OVER are not available yet"},
		{sql: "select id from orders group by id", kind: Refuse, msg: "multi-shard GROUP BY without the shard key is not available yet"},
		{sql: "select tenant_id, count(*) from orders group by tenant_id", kind: Scatter, shards: "all"},
		{sql: "select distinct id from orders", kind: Refuse, msg: "multi-shard DISTINCT without the shard key is not available yet"},
		{sql: "select distinct tenant_id, id from orders", kind: Scatter, shards: "all"},
		{sql: "select * from orders order by id", kind: Scatter, shards: "all"},
		{sql: "select * from orders limit 10", kind: Scatter, shards: "all"},
		{sql: "select * from orders limit $1", kind: Refuse, msg: "multi-shard LIMIT must be an integer constant"},
		{sql: "select * from orders offset 10", kind: Scatter, shards: "all"},
		{sql: "select * from orders order by id limit 10", kind: Scatter, shards: "all"},
		{sql: "select row_number() over () from orders", kind: Refuse, msg: "multi-shard SELECT with window functions is not available yet"},
		{sql: "select * from orders for update", kind: Refuse, msg: "multi-shard SELECT with FOR UPDATE/SHARE is not available yet"},
		{sql: "select * from orders union all select * from order_lines", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders where tenant_id = 1 union all select * from orders", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders where tenant_id = 1 union select * from orders where tenant_id = 2", kind: Refuse, msg: "cross-shard join"},
		{sql: "with c as (select * from orders) select * from c", kind: Refuse, msg: "multi-shard SELECT with common table expressions is not available yet"},
		{sql: "select (select count(*) from order_lines) from orders", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from (select * from orders) s", kind: Refuse, msg: "multi-shard SELECT with subqueries is not available yet"},
		{sql: "select * from orders where id in (select order_id from order_lines)", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o cross join generate_series(1, 3) g", kind: Refuse, msg: "multi-shard SELECT with joins, function scans is not available yet"},

		// Joins.
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id where o.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o join order_lines l on l.tenant_id = o.tenant_id where l.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id and o.id = l.order_id where o.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o join order_lines l using (tenant_id) where tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o join order_lines l using (tenant_id, id) where o.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o natural join order_lines l where o.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o, order_lines l where o.tenant_id = l.tenant_id and o.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o, order_lines l where o.tenant_id = 1 and l.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o, order_lines l where o.tenant_id = 1 and l.tenant_id = 42", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o, order_lines l where o.tenant_id = 1 and l.tenant_id = 2", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o join order_lines l on o.id = l.order_id where o.tenant_id = 1", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o join order_lines l on o.id = l.order_id where o.tenant_id = 1 and l.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o join docs d on o.tenant_id = d.slug where o.tenant_id = 1", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o join docs d on o.id = d.id where o.tenant_id = 1 and d.slug = 'acme'", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o join docs d on o.id = d.id where o.tenant_id = 2 and d.slug = 'a'", kind: EqualUnique, shards: "k:2"},
		{sql: "select * from orders o join regions r on o.region = r.id where o.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o join items i on o.item = i.id where o.tenant_id = 1", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o join items i on o.item = i.id where o.tenant_id = 2", kind: EqualUnique, shards: "0"},
		{sql: "select * from orders o join items i on o.item = i.id where o.tenant_id in (2, 3)", kind: In, shards: "0"},
		{sql: "select * from orders o join items i on o.item = i.id where o.tenant_id in (1, 2)", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id where o.tenant_id in (1, 2)", kind: In, shards: "k:1,k:2"},
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id where o.tenant_id in (1, 2) and l.tenant_id in (2, 1)", kind: In, shards: "k:1,k:2"},
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id where o.tenant_id in (1, 2) and l.tenant_id in (1, 7)", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id where o.tenant_id = 1 and l.tenant_id = 2", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id join items i on i.id = o.item where o.tenant_id = 1", kind: Refuse, msg: "cross-shard join"},
		{sql: "select * from orders o left join order_lines l on o.tenant_id = l.tenant_id where o.tenant_id = 1", kind: EqualUnique, shards: "k:1"},
		{sql: "select * from orders o join order_lines l on o.tenant_id = l.tenant_id where o.tenant_id = 1 order by o.id limit 3", kind: EqualUnique, shards: "k:1"},

		// DDL.
		{sql: "create table orders (id int, tenant_id int8, primary key (tenant_id, id))", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "create table orders (id int, tenant_id int8 primary key)", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "create table orders (id int, tenant_id int8, unique (id, tenant_id))", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "create table orders (id int primary key, tenant_id int8)", kind: Refuse, msg: "primary key or unique constraint (id) on sharded table \"orders\" must include the shard key \"tenant_id\""},
		{sql: "create table orders (id int, tenant_id int8, primary key (id))", kind: Refuse, msg: "primary key or unique constraint (id) on sharded table \"orders\" must include the shard key \"tenant_id\""},
		{sql: "create table orders (id int, tenant_id int8, unique (id))", kind: Refuse, msg: "primary key or unique constraint (id) on sharded table \"orders\" must include the shard key"},
		{sql: "create table orders (id int, tenant_id int8, sku text unique)", kind: Refuse, msg: "primary key or unique constraint (sku) on sharded table \"orders\" must include the shard key"},
		{sql: "create table orders (id int, tenant_id int8, primary key (tenant_id, id), unique (sku))", kind: Refuse, msg: "primary key or unique constraint (sku) on sharded table"},
		{sql: "create table orders (id int primary key)", kind: Refuse, msg: "primary key or unique constraint (id) on sharded table"},
		{sql: "create table orders (id int)", kind: Refuse, msg: "sharded table \"orders\" must define its shard key column \"tenant_id\""},
		{sql: "create table docs (id int, slug text, primary key (slug))", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "create table docs (id int primary key, slug text)", kind: Refuse, msg: "primary key or unique constraint (id) on sharded table \"docs\" must include the shard key \"slug\""},
		{sql: "create table if not exists orders (id int, tenant_id int8)", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "create table public.orders (id int, tenant_id int8)", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "create table audit.events (id int, tenant_id int8)", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "create table other.orders (id int primary key)", kind: Unsharded, shards: "0"},
		{sql: "create temporary table orders (id int, tenant_id int8)", kind: Refuse, msg: "temporary tables are not supported through the router"},
		{sql: "create index on orders (id)", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "create unique index on orders (id)", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "alter table orders add column x int", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "alter table orders rename to orders2", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "drop table orders", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "drop table public.orders, items", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "drop table items, docs", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "drop index orders_idx", kind: Unsharded, shards: "0"},
		{sql: "truncate orders", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "truncate items, orders", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "vacuum orders", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "lock table orders", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "create table t2 as select * from orders where tenant_id = 1", kind: Refuse, msg: "CREATE TABLE AS over sharded or reference tables is not available yet"},
		{sql: "create table t2 as select * from items", kind: Unsharded, shards: "0"},
		{sql: "create table orders as select 1", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "create view v as select * from orders where tenant_id = 1", kind: Refuse, msg: "CREATE VIEW over sharded or reference tables is not available yet"},
		{sql: "create view v as select * from regions", kind: Refuse, msg: "CREATE VIEW over sharded or reference tables is not available yet"},
		{sql: "create view orders as select 1", kind: Refuse, msg: "DDL fan-out is not available yet"},
		{sql: "copy orders from stdin", kind: Refuse, msg: "COPY on sharded and reference tables is not available yet"},
		{sql: "copy orders to stdout", kind: Refuse, msg: "COPY on sharded and reference tables is not available yet"},
		{sql: "copy (select * from orders where tenant_id = 1) to stdout", kind: EqualUnique, shards: "k:1"},
		{sql: "prepare p as select * from orders where tenant_id = $1", kind: Refuse, msg: "SQL-level PREPARE touching sharded or reference tables is not available yet"},
		{sql: "prepare p as select * from regions", kind: Refuse, msg: "SQL-level PREPARE touching sharded or reference tables is not available yet"},
		{sql: "with d as (delete from orders where tenant_id = 1 returning *) select * from d", kind: Refuse, msg: "data-modifying statements in WITH are not available yet"},
		{sql: "listen ch", kind: Refuse, msg: "LISTEN is not supported through the router"},
		{sql: "notify ch", kind: Refuse, msg: "NOTIFY is not supported through the router"},
		{sql: "declare c cursor with hold for select 1", kind: Refuse, msg: "WITH HOLD cursors are not supported through the router"},
		{sql: "select 1; select 2", kind: Refuse, msg: "multi-statement queries are not supported through the router"},
	}
}

// staticParams serves the golden values; nil marks a NULL.
type staticParams map[int32]any

func (s staticParams) ShardKey(n int32, _ TypeHint) (any, error) {
	v, ok := s[n]
	if !ok {
		return nil, fmt.Errorf("parameter $%d was not bound", n)
	}
	if v == nil {
		return nil, errors.New("shard key must not be NULL")
	}
	return v, nil
}

func TestGoldenPlans(t *testing.T) {
	snap := fixture(t)
	p := New()
	cases := golden()
	if len(cases) < 100 {
		t.Fatalf("golden table has %d statements, want at least 100", len(cases))
	}
	seen := map[string]bool{}
	for _, c := range cases {
		if seen[c.sql] {
			t.Fatalf("duplicate golden statement %q", c.sql)
		}
		seen[c.sql] = true
		t.Run(c.sql, func(t *testing.T) {
			pl, err := p.Plan(context.Background(), session(snap), c.sql)
			if c.values != nil {
				if err != nil {
					t.Fatalf("plan: %v", err)
				}
				if !pl.Deferred {
					t.Fatalf("expected a deferred plan, got %+v", pl)
				}
				if len(pl.Shards) != 0 {
					t.Fatalf("deferred plan must not carry shards: %v", pl.Shards)
				}
				pl, err = pl.Resolve(staticParams(c.values))
			}
			if c.kind == Refuse {
				checkRefusal(t, pl, err, c.msg)
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if pl.Kind != c.kind {
				t.Fatalf("kind = %v, want %v (plan %+v)", pl.Kind, c.kind, pl)
			}
			if pl.Deferred {
				t.Fatalf("plan still deferred: %+v", pl)
			}
			if got, want := fmt.Sprint(pl.Shards), fmt.Sprint(expectShards(t, c.shards)); got != want {
				t.Fatalf("shards = %s, want %s (%s)", got, want, c.shards)
			}
			if pl.Generation != snap.ShardMapGeneration {
				t.Fatalf("generation = %d, want %d", pl.Generation, snap.ShardMapGeneration)
			}
		})
	}
}

func checkRefusal(t *testing.T, pl Plan, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected refusal %q, got plan %+v", msg, pl)
	}
	var pe *pgwire.Error
	if !errors.As(err, &pe) {
		t.Fatalf("refusal is not a pgwire error: %v", err)
	}
	if pe.Code != pgwire.CodeFeatureNotSupported {
		t.Fatalf("SQLSTATE = %s, want 0A000 (%v)", pe.Code, err)
	}
	if !strings.HasPrefix(pe.Message, msg) {
		t.Fatalf("message = %q, want prefix %q", pe.Message, msg)
	}
	if pl.Kind != Refuse || pl.Err == nil {
		t.Fatalf("refused plan must be Kind Refuse with Err: %+v", pl)
	}
}

// expectShards decodes the golden shard notation.
func expectShards(t *testing.T, spec string) []int32 {
	t.Helper()
	s := fixture(t)
	switch spec {
	case "":
		return nil
	case "all":
		return []int32{0, 1, 2, 3}
	case "ref":
		return []int32{int32(session(s).ID % 4)}
	}
	var out []int32
	for _, part := range strings.Split(spec, ",") {
		var id int32
		switch {
		case strings.HasPrefix(part, "k:"):
			key := strings.TrimPrefix(part, "k:")
			var v any = key
			if strings.HasSuffix(key, "s") && key != "s" && isDigits(strings.TrimSuffix(key, "s")) {
				v = strings.TrimSuffix(key, "s")
			} else if i, err := parseInt(key); err == nil {
				v = i
			}
			id = shardOf(t, s, v)
		default:
			i, err := parseInt(part)
			if err != nil {
				t.Fatalf("bad shard spec %q", spec)
			}
			id = int32(i)
		}
		out = appendUnique(out, id)
	}
	sortShards(out)
	return out
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func sortShards(s []int32) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
