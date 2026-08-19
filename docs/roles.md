# Roles, grants and drift

Roles are cluster-wide: a client role must exist with the same SCRAM
verifier, attributes, memberships, settings and grants on every shard, on
groups added later, and on the catalog (the router authenticates against
`pgshard.roles.verifier`; the pooler dials shards as the real user). The
catalog holds the desired state, the [migration applier](ddl.md) fans each
statement out, and the controller's role verifier keeps every group equal
to the desired state and repairs what drifts.

## Desired state

| Table | Written by | Holds |
|---|---|---|
| `pgshard.roles` | `CREATE`/`ALTER`/`DROP ROLE` | `verifier`, `login`, `createdb`, `createrole`, `inherit`, `connection_limit`, `valid_until` |
| `pgshard.role_members` | `GRANT`/`REVOKE <role> TO/FROM`, `CREATE ROLE … IN ROLE/ROLE/ADMIN` | `(rolname, member, admin_option)` |
| `pgshard.grants` | `GRANT`/`REVOKE … ON <object>` | `(rolname, database, object_kind, object_schema, object_name, column_name, privileges[], grant_option)` |
| `pgshard.role_settings` | `ALTER ROLE … [IN DATABASE] SET/RESET` | `(rolname, database, name, value)` |

The router normalizes each statement into a delta (`migrations.meta.roles`)
and the applier records it **after** the statement applied on every target:

* `PASSWORD 'plain'` is hashed in the router; every group and the catalog
  store one verifier. `CREATE ROLE` defaults to `NOLOGIN`, `CREATE USER` to
  `LOGIN`, as in PostgreSQL. `ALTER ROLE` records only the attributes it
  names.
* Grants are one row per `(object, column, grantee)`; `ALL [PRIVILEGES]` is
  expanded per object kind (table: `DELETE INSERT MAINTAIN REFERENCES SELECT
  TRIGGER TRUNCATE UPDATE`, column: `INSERT REFERENCES SELECT UPDATE`,
  sequence, schema, database, function, type, domain, language) so a later
  `REVOKE` of one privilege subtracts; a row with no privileges left is
  deleted; `REVOKE GRANT OPTION FOR` clears `grant_option` only. Grants to
  `PUBLIC`, to roles the catalog does not manage, `… ON ALL TABLES IN
  SCHEMA` and kinds not listed above are fanned out but not recorded.
* Memberships are additive: the catalog lists what must hold; a membership
  it does not list (for example the one PostgreSQL gives a `CREATEROLE`
  creator on the roles it creates) is not drift and never revoked. `REVOKE
  ADMIN OPTION FOR` clears `admin_option` only.
* `ALTER ROLE … SET name TO value` records the value as `SHOW` reports it
  (`SET … TO DEFAULT`/`RESET` delete the row, `RESET ALL` the role's rows);
  `SET … FROM CURRENT` is refused (`0A000`) — the router has no GUC to
  copy.

Refused with `0A000` and a hint: `SUPERUSER`, `REPLICATION`, `BYPASSRLS`
(they would grant server-wide rights on every shard; manage such roles on
the shards directly), `ALTER ROLE … RENAME` (the catalog keys roles,
memberships and grants by name; create the new role, drop the old),
`ALTER DEFAULT PRIVILEGES` (default ACLs are kept per creating role and
schema and cannot be expressed as desired rows; `GRANT` after creating the
objects), `REASSIGN OWNED` and `DROP OWNED` (act on every object of a role
on each shard; `ALTER … OWNER TO` / `REVOKE` explicitly).

## Fan-out

Role statements run on every shard of the set **and on the catalog group**
(`per_shard` key `"catalog"`, applied last and only when every shard
applied, as the controller's own catalog connection without `SET ROLE`).
The controller's catalog role therefore needs `CREATEROLE` (it is a
superuser in the dev stack). Object grants run on the shards only — the
catalog has no user tables. Roles are created on the shards as the client
role (`SET ROLE`), so the client needs `CREATEROLE` there, as in plain
PostgreSQL; declare it in `pgshard.roles.createrole` and the verifier
materializes it.

## Materializing and verifying (controller leader)

`RoleVerifier` (`internal/controller/roles.go`) runs every
`--verify-roles-interval` (15s) on the leader and before the applier's
passes between migrations:

1. **Groups behind the desired generation** — a new shard in
   `pgshard.shard_status` (a resharding target), the catalog, or every group
   after a role migration completed — are *materialized*:
   `MaterializeRoles` creates or alters every desired role with its
   attributes and verifier (`CREATE ROLE`/`ALTER ROLE … NOSUPERUSER
   NOREPLICATION NOBYPASSRLS … PASSWORD '<verifier>'`), grants every
   membership, sets every setting and, per database, re-grants every grant
   (idempotent; a grant whose object is missing on that group is reported,
   the rest applied). `pgshard.role_group_status` records the generation
   the group reached. `NewRolesMaterializer(dsn)` is the same step for a
   group given by DSN (resharding).
2. **Groups at the current generation** are *verified*: `pg_authid`
   (`rolcanlogin`, `rolcreatedb`, `rolcreaterole`, `rolinherit`,
   `rolconnlimit`, `rolvaliduntil`, `rolpassword`), `pg_auth_members` and
   the ACLs of tables, sequences, columns, schemas and databases are compared
   with the desired rows (verifiers by parsed SCRAM fields, so formatting
   differences are not drift). Per `(role, group)` the result lands in
   `pgshard.role_status`: `in_sync`, `drifted` (details: `attributes`,
   `verifier`, `missing_memberships`, `missing_grants`), `missing`, or
   `unmanaged` for a non-superuser role on the group that is not in
   `pgshard.roles` (reported, never touched). Drifted and missing managed
   roles are repaired at once by re-materializing just those roles; the row
   stays `drifted` with `details.repaired = true` until the next pass sees
   it in sync (`repair_error` names a failed repair).
3. The verifier waits while a role or grant migration is queued or running
   so it never races the applier; passes and the applier's materialization
   are serialized.

Roles the catalog does not manage are never altered or dropped. Extra
privileges or settings on a group are not removed (desired state is
additive; `REVOKE`/`RESET` through the router removes them everywhere).
Grants on functions, types, domains and languages are materialized but not
verified.

```sql
SELECT rolname, group_name, state, details, checked_at FROM pgshard.role_status ORDER BY 1, 2;
SELECT * FROM pgshard.role_group_status;
```

`Controller.ListRoleStatus` (gRPC) returns the same rows, optionally for
one role.

## Password changes

`ALTER ROLE r PASSWORD 'new'` through the router is one migration: every
shard and the catalog get the verifier, then `pgshard.roles.verifier` is
updated. Router logins see the new password once the migration completes
(the verifier cache reloads within its TTL, 5s); until then the old password
logs in. A password changed out of band on one shard is reported as
`drifted` / `verifier: differs` and put back at the next verifier pass; the
router never served it.

## DEGRADED

A role migration that failed on a shard leaves the desired state untouched
(the catalog step is skipped), so the router keeps authenticating with the
previous verifier; the shards that applied already carry the new one, are
reported `drifted` and put back to the desired (previous) verifier at the
next pass. Re-run the statement once the failing shard is fixed. A group the controller cannot reach keeps its last status row;
`checked_at` shows how stale it is.
