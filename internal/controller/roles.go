package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// Role status states in pgshard.role_status.
const (
	RoleInSync    = "in_sync"
	RoleDrifted   = "drifted"
	RoleMissing   = "missing"
	RoleUnmanaged = "unmanaged"
	// RoleUnmanagedSuperuser marks a role listed in pgshard.roles that is a
	// superuser on the group: it is reported and never altered.
	RoleUnmanagedSuperuser = "unmanaged_superuser"
)

// RoleStatus is a row of pgshard.role_status.
type RoleStatus struct {
	Role       string
	Group      string
	State      string
	Details    map[string]any
	Generation int64
	CheckedAt  time.Time
}

// RoleStore is the catalog side of the role verifier.
type RoleStore interface {
	Desired(ctx context.Context) (*catalog.DesiredRoles, error)
	Shards(ctx context.Context, shardSet string) ([]int32, error)
	// GroupGenerations maps group name to the generation it was last
	// materialized at.
	GroupGenerations(ctx context.Context) (map[string]int64, error)
	// SaveGroupStatus replaces the status rows of one group.
	SaveGroupStatus(ctx context.Context, group string, generation int64, rows []RoleStatus) error
	// RoleMigrationsPending reports whether a role or grant migration is
	// queued or running, in which case the verifier waits.
	RoleMigrationsPending(ctx context.Context) (bool, error)
	// LiveShardSets names every shard set whose groups are running, not
	// only the serving one: a reshard or upgrade target has the source's
	// schema materialized onto it during the copy, and that schema can name
	// roles, so the target needs them before then rather than after it
	// starts serving.
	LiveShardSets(ctx context.Context) ([]string, error)
	// ServingShardSet names the shard set currently serving; roles must be
	// materialized on the groups that are actually serving, not on a set a
	// reshard retired.
	ServingShardSet(ctx context.Context) (string, error)
}

// PGRoleStore is the RoleStore over the catalog pool.
type PGRoleStore struct {
	Pool *pgxpool.Pool
}

// Desired implements RoleStore.
func (s *PGRoleStore) Desired(ctx context.Context) (*catalog.DesiredRoles, error) {
	return catalog.LoadDesiredRoles(ctx, s.Pool)
}

// Shards implements RoleStore.
func (s *PGRoleStore) Shards(ctx context.Context, shardSet string) ([]int32, error) {
	return (&PGMigrationStore{Pool: s.Pool}).Shards(ctx, shardSet)
}

// ServingShardSet implements RoleStore.
func (s *PGRoleStore) ServingShardSet(ctx context.Context) (string, error) {
	return catalog.ServingShardSet(ctx, s.Pool)
}

// LiveShardSets implements RoleStore.
func (s *PGRoleStore) LiveShardSets(ctx context.Context) ([]string, error) {
	sets, err := catalog.ListShardSets(ctx, s.Pool)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(sets))
	for _, set := range sets {
		if set.State == catalog.ShardSetRetired {
			continue
		}
		out = append(out, set.Name)
	}
	return out, nil
}

// GroupGenerations implements RoleStore.
func (s *PGRoleStore) GroupGenerations(ctx context.Context) (map[string]int64, error) {
	rows, err := s.Pool.Query(ctx, `SELECT group_name, roles_generation FROM pgshard.role_group_status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var g string
		var gen int64
		if err := rows.Scan(&g, &gen); err != nil {
			return nil, err
		}
		out[g] = gen
	}
	return out, rows.Err()
}

// SaveGroupStatus implements RoleStore.
func (s *PGRoleStore) SaveGroupStatus(ctx context.Context, group string, generation int64, rows []RoleStatus) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		names = append(names, r.Role)
		details, err := json.Marshal(r.Details)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO pgshard.role_status (rolname, group_name, state, details, roles_generation, checked_at)
			VALUES ($1, $2, $3, $4, $5, now())
			ON CONFLICT (rolname, group_name) DO UPDATE SET state = EXCLUDED.state, details = EXCLUDED.details,
			roles_generation = EXCLUDED.roles_generation, checked_at = now()`, r.Role, group, r.State, details, generation); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM pgshard.role_status WHERE group_name = $1 AND rolname <> ALL ($2::text[])`, group, names); err != nil {
		return err
	}
	if generation >= 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO pgshard.role_group_status (group_name, roles_generation, materialized_at) VALUES ($1, $2, now())
			ON CONFLICT (group_name) DO UPDATE SET roles_generation = greatest(pgshard.role_group_status.roles_generation, EXCLUDED.roles_generation), materialized_at = now()`, group, generation); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// RoleMigrationsPending implements RoleStore.
func (s *PGRoleStore) RoleMigrationsPending(ctx context.Context) (bool, error) {
	rows, err := s.Pool.Query(ctx, `SELECT EXISTS (SELECT 1 FROM pgshard.migrations WHERE state IN ('queued', 'running')
		AND kind IN ('CREATE ROLE', 'ALTER ROLE', 'DROP ROLE', 'GRANT ROLE', 'REVOKE ROLE', 'GRANT', 'REVOKE'))`)
	if err != nil {
		return false, err
	}
	return pgx.CollectOneRow(rows, pgx.RowTo[bool])
}

// ListRoleStatus reads role_status rows, optionally for one role.
func ListRoleStatus(ctx context.Context, q catalog.Querier, role string) ([]RoleStatus, error) {
	rows, err := q.Query(ctx, `SELECT rolname, group_name, state, details, roles_generation, checked_at FROM pgshard.role_status
		WHERE $1 = '' OR rolname = $1 ORDER BY rolname, group_name`, role)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (RoleStatus, error) {
		var r RoleStatus
		var details []byte
		if err := row.Scan(&r.Role, &r.Group, &r.State, &details, &r.Generation, &r.CheckedAt); err != nil {
			return r, err
		}
		err := json.Unmarshal(details, &r.Details)
		return r, err
	})
}

// roleGroup is one PostgreSQL server the roles must exist on.
type roleGroup struct {
	name    string
	catalog bool
	dial    func(ctx context.Context, database string) (ShardConn, error)
}

// RoleVerifier keeps every group's roles, memberships, settings and grants
// equal to the catalog's desired state: it materializes them on groups that
// are behind, compares the rest periodically, repairs what drifted and
// records pgshard.role_status. Roles it does not manage are reported as
// unmanaged and never touched.
type RoleVerifier struct {
	Store   RoleStore
	Shards  DatabaseDialer
	Catalog func(ctx context.Context) (ShardConn, error)
	Logger  *slog.Logger
	// ShardSet defaults to "default".
	ShardSet string

	// mu serializes passes: the applier materializes stale groups between
	// migrations while the periodic pass verifies, and PostgreSQL refuses
	// concurrent ALTER ROLE on one role ("tuple concurrently updated").
	mu sync.Mutex
}

func (v *RoleVerifier) logger() *slog.Logger {
	if v.Logger == nil {
		return slog.Default()
	}
	return v.Logger
}

// shardSet is the configured override when there is one, otherwise
// whichever set is serving now.
// shardSets is every set whose groups are running. A reshard target needs the
// roles before its schema is materialized, which happens during the copy and so
// long before it serves; leaving it out meant any schema naming a role could
// not be materialized there at all.
func (v *RoleVerifier) shardSets(ctx context.Context) ([]string, error) {
	if v.ShardSet != "" {
		return []string{v.ShardSet}, nil
	}
	return v.Store.LiveShardSets(ctx)
}

func (v *RoleVerifier) groups(ctx context.Context) ([]roleGroup, error) {
	sets, err := v.shardSets(ctx)
	if err != nil {
		return nil, err
	}
	var out []roleGroup
	for _, set := range sets {
		ids, err := v.Store.Shards(ctx, set)
		if err != nil {
			return nil, fmt.Errorf("roles: shards of %s: %w", set, err)
		}
		for _, id := range ids {
			out = append(out, roleGroup{name: fmt.Sprintf("%s/%d", set, id), dial: func(ctx context.Context, db string) (ShardConn, error) {
				return v.Shards.DialDatabase(ctx, set, id, db)
			}})
		}
	}
	if v.Catalog != nil {
		out = append(out, roleGroup{name: CatalogGroup, catalog: true, dial: func(ctx context.Context, _ string) (ShardConn, error) { return v.Catalog(ctx) }})
	}
	return out, nil
}

// Run verifies every interval while leader() is true.
func (v *RoleVerifier) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	runLoop(ctx, interval, leader, v.logger, "role verification", func(ctx context.Context) {
		if err := v.RunOnce(ctx); err != nil && ctx.Err() == nil {
			v.logger().Warn("role verification failed", "err", err)
		}
	})
}

// MaterializeStale applies the desired roles to every group whose recorded
// generation is behind (new groups included).
func (v *RoleVerifier) MaterializeStale(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	desired, err := v.Store.Desired(ctx)
	if err != nil {
		return err
	}
	gens, err := v.Store.GroupGenerations(ctx)
	if err != nil {
		return err
	}
	groups, err := v.groups(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, g := range groups {
		if gen, ok := gens[g.name]; ok && gen >= desired.Generation {
			continue
		}
		if err := v.materializeGroup(ctx, g, desired); err != nil {
			errs = append(errs, fmt.Errorf("group %s: %w", g.name, err))
		}
	}
	return errors.Join(errs...)
}

func (v *RoleVerifier) materializeGroup(ctx context.Context, g roleGroup, desired *catalog.DesiredRoles) error {
	v.logger().Info("materializing roles", "group", g.name, "generation", desired.Generation, "roles", len(desired.Roles))
	if err := MaterializeRoles(ctx, g.dial, desired, !g.catalog); err != nil {
		return err
	}
	rows, err := v.verifyGroup(ctx, g, desired, false)
	if err != nil {
		return err
	}
	return v.Store.SaveGroupStatus(ctx, g.name, desired.Generation, rows)
}

// RunOnce compares every group with the desired state, repairs drifted and
// missing managed roles and records the status rows.
func (v *RoleVerifier) RunOnce(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	pending, err := v.Store.RoleMigrationsPending(ctx)
	if err != nil {
		return err
	}
	if pending {
		return nil
	}
	desired, err := v.Store.Desired(ctx)
	if err != nil {
		return err
	}
	gens, err := v.Store.GroupGenerations(ctx)
	if err != nil {
		return err
	}
	groups, err := v.groups(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, g := range groups {
		if gen, ok := gens[g.name]; !ok || gen < desired.Generation {
			if err := v.materializeGroup(ctx, g, desired); err != nil {
				errs = append(errs, fmt.Errorf("group %s: %w", g.name, err))
			}
			continue
		}
		rows, err := v.verifyGroup(ctx, g, desired, true)
		if err != nil {
			errs = append(errs, fmt.Errorf("group %s: %w", g.name, err))
			continue
		}
		if err := v.Store.SaveGroupStatus(ctx, g.name, -1, rows); err != nil {
			errs = append(errs, fmt.Errorf("group %s: %w", g.name, err))
		}
	}
	return errors.Join(errs...)
}

// verifyGroup observes one group, diffs it against desired and, when
// repair is set, re-materializes the managed roles that drifted. A repaired
// role stays "drifted" with details.repaired until the next pass sees it in
// sync.
func (v *RoleVerifier) verifyGroup(ctx context.Context, g roleGroup, desired *catalog.DesiredRoles, repair bool) ([]RoleStatus, error) {
	obs, err := observeGroup(ctx, g.dial, desired, !g.catalog)
	if err != nil {
		return nil, err
	}
	rows := diffGroup(g.name, desired, obs, !g.catalog)
	if !repair {
		return rows, nil
	}
	var fix []string
	for _, r := range rows {
		if r.State == RoleDrifted || r.State == RoleMissing {
			fix = append(fix, r.Role)
		}
	}
	if len(fix) == 0 {
		return rows, nil
	}
	v.logger().Warn("repairing drifted roles", "group", g.name, "roles", fix)
	sub := subsetRoles(desired, fix)
	rerr := MaterializeRoles(ctx, g.dial, sub, !g.catalog)
	for i := range rows {
		if rows[i].State == RoleDrifted || rows[i].State == RoleMissing {
			if rows[i].Details == nil {
				rows[i].Details = map[string]any{}
			}
			if rerr != nil {
				rows[i].Details["repair_error"] = rerr.Error()
			} else {
				rows[i].Details["repaired"] = true
			}
		}
	}
	return rows, nil
}

// subsetRoles keeps the desired state of the named roles only.
func subsetRoles(d *catalog.DesiredRoles, names []string) *catalog.DesiredRoles {
	keep := map[string]bool{}
	for _, n := range names {
		keep[n] = true
	}
	sub := &catalog.DesiredRoles{Generation: d.Generation}
	for _, r := range d.Roles {
		if keep[r.Name] {
			sub.Roles = append(sub.Roles, r)
		}
	}
	for _, m := range d.Members {
		if keep[m.Member] || keep[m.Role] {
			sub.Members = append(sub.Members, m)
		}
	}
	for _, g := range d.Grants {
		if keep[g.Grantee] {
			sub.Grants = append(sub.Grants, g)
		}
	}
	for _, s := range d.Settings {
		if keep[s.Role] {
			sub.Settings = append(sub.Settings, s)
		}
	}
	return sub
}

// MaterializeRoles applies the desired roles, memberships, settings and
// (withGrants) object grants to one group idempotently: roles are created
// or altered to the desired attributes and verifier, memberships granted,
// settings set, grants re-granted. Grants whose object is missing on the
// group are reported in the returned error after everything else applied.
func MaterializeRoles(ctx context.Context, dial func(ctx context.Context, database string) (ShardConn, error), d *catalog.DesiredRoles, withGrants bool) error {
	conn, err := dial(ctx, "")
	if err != nil {
		return &dialError{err}
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	superusers := map[string]bool{}
	for _, r := range d.Roles {
		rows, err := conn.Query(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = $1`, r.Name)
		if err != nil {
			return err
		}
		super, err := pgx.CollectOneRow(rows, pgx.RowTo[bool])
		exists := err == nil
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if exists && super {
			superusers[r.Name] = true
			continue
		}
		sql := "CREATE ROLE " + pgx.Identifier{r.Name}.Sanitize() + " " + RoleOptionsSQL(r) + " NOSUPERUSER NOREPLICATION NOBYPASSRLS"
		if exists {
			sql = "ALTER ROLE " + pgx.Identifier{r.Name}.Sanitize() + " " + RoleOptionsSQL(r)
		}
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("role %s: %w", r.Name, err)
		}
	}
	for _, m := range d.Members {
		if superusers[m.Member] {
			continue
		}
		sql := "GRANT " + pgx.Identifier{m.Role}.Sanitize() + " TO " + pgx.Identifier{m.Member}.Sanitize()
		if m.Admin {
			sql += " WITH ADMIN OPTION"
		}
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("membership %s in %s: %w", m.Member, m.Role, err)
		}
	}
	for _, s := range d.Settings {
		if superusers[s.Role] {
			continue
		}
		sql := "ALTER ROLE " + pgx.Identifier{s.Role}.Sanitize()
		if s.Database != "" {
			sql += " IN DATABASE " + pgx.Identifier{s.Database}.Sanitize()
		}
		sql += " SET " + pgx.Identifier{s.Name}.Sanitize() + " TO " + quoteLiteral(s.Value)
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("setting %s of %s: %w", s.Name, s.Role, err)
		}
	}
	if !withGrants {
		return nil
	}
	var errs []error
	for _, db := range grantDatabases(d.Grants) {
		dbConn, err := dial(ctx, db)
		if err != nil {
			// A reshard target has no application database until the copier
			// creates it, and grants cannot be applied to a database that is
			// not there yet. Skip quietly rather than report a failure every
			// pass for something that resolves itself.
			if absentDatabase(err) {
				continue
			}
			errs = append(errs, fmt.Errorf("database %s: %w", db, err))
			continue
		}
		for _, g := range d.Grants {
			if g.Database != db {
				continue
			}
			// The privileges are rendered into the statement by
			// concatenation, and the row they came from can be written
			// directly by an administrator who is deliberately not a
			// superuser on the shards. A privilege that is not a privilege
			// is refused here rather than sent, whatever wrote the row.
			if err := catalog.CheckPrivileges(g.Kind, g.Column, g.Privileges); err != nil {
				errs = append(errs, fmt.Errorf("grant on %s %s to %s: %w", g.Kind, g.Name, g.Grantee, err))
				continue
			}
			if _, err := dbConn.Exec(ctx, GrantSQL(g)); err != nil {
				errs = append(errs, fmt.Errorf("grant on %s %s to %s: %w", g.Kind, g.Name, g.Grantee, err))
			}
		}
		_ = dbConn.Close(context.WithoutCancel(ctx))
	}
	return errors.Join(errs...)
}

// absentDatabase reports whether err is PostgreSQL refusing a connection
// because the database does not exist (3D000, invalid_catalog_name).
func absentDatabase(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "3D000"
}

func grantDatabases(grants []catalog.DesiredGrant) []string {
	seen := map[string]bool{}
	var out []string
	for _, g := range grants {
		if !seen[g.Database] {
			seen[g.Database] = true
			out = append(out, g.Database)
		}
	}
	sort.Strings(out)
	return out
}

// RoleOptionsSQL renders the attribute and password options of a desired
// role for CREATE/ALTER ROLE. NOSUPERUSER, NOREPLICATION and NOBYPASSRLS
// are not among them: a role observed as superuser is never altered, and
// CREATE ROLE adds them itself.
func RoleOptionsSQL(r catalog.DesiredRole) string {
	opt := func(on bool, yes, no string) string {
		if on {
			return yes
		}
		return no
	}
	parts := []string{
		opt(r.Login, "LOGIN", "NOLOGIN"),
		opt(r.CreateDB, "CREATEDB", "NOCREATEDB"),
		opt(r.CreateRole, "CREATEROLE", "NOCREATEROLE"),
		opt(r.Inherit, "INHERIT", "NOINHERIT"),
		fmt.Sprintf("CONNECTION LIMIT %d", r.ConnectionLimit),
	}
	if r.ValidUntil != nil {
		parts = append(parts, "VALID UNTIL "+quoteLiteral(r.ValidUntil.UTC().Format(time.RFC3339Nano)))
	} else {
		parts = append(parts, "VALID UNTIL 'infinity'")
	}
	if r.Verifier != "" {
		parts = append(parts, "PASSWORD "+quoteLiteral(r.Verifier))
	}
	return strings.Join(parts, " ")
}

// GrantSQL renders one desired grant.
func GrantSQL(g catalog.DesiredGrant) string {
	privs := strings.Join(g.Privileges, ", ")
	if g.Column != "" {
		var cols []string
		for _, p := range g.Privileges {
			cols = append(cols, p+" ("+pgx.Identifier{g.Column}.Sanitize()+")")
		}
		privs = strings.Join(cols, ", ")
	}
	name := pgx.Identifier{g.Name}.Sanitize()
	if g.Schema != "" {
		name = pgx.Identifier{g.Schema, g.Name}.Sanitize()
	}
	sql := "GRANT " + privs + " ON " + strings.ToUpper(g.Kind) + " " + name + " TO " + pgx.Identifier{g.Grantee}.Sanitize()
	if g.GrantOption {
		sql += " WITH GRANT OPTION"
	}
	return sql
}

// observedRole is one row of pg_authid.
type observedRole struct {
	Name            string
	Login           bool
	CreateDB        bool
	CreateRole      bool
	Inherit         bool
	Super           bool
	ConnectionLimit int32
	ValidUntil      *time.Time
	Verifier        string
	// VerifierKnown is false when rolpassword could not be read.
	VerifierKnown bool
}

type membershipKey struct{ Role, Member string }

type grantKey struct{ Database, Kind, Schema, Name, Column, Grantee string }

// observedGroup is what one group reports.
type observedGroup struct {
	Roles   map[string]observedRole
	Members map[membershipKey]bool
	// Grants maps a desired grant to the privileges (and grant options)
	// present; a key missing means the object could not be inspected.
	Grants map[grantKey]map[string]bool
}

func observeGroup(ctx context.Context, dial func(ctx context.Context, database string) (ShardConn, error), d *catalog.DesiredRoles, withGrants bool) (*observedGroup, error) {
	conn, err := dial(ctx, "")
	if err != nil {
		return nil, &dialError{err}
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	obs := &observedGroup{Roles: map[string]observedRole{}, Members: map[membershipKey]bool{}, Grants: map[grantKey]map[string]bool{}}
	rows, err := conn.Query(ctx, `SELECT rolname, rolcanlogin, rolcreatedb, rolcreaterole, rolinherit, rolsuper, rolconnlimit, CASE WHEN isfinite(rolvaliduntil) THEN rolvaliduntil END, coalesce(rolpassword, '') FROM pg_authid WHERE rolname NOT LIKE 'pg\_%'`)
	known := true
	if err != nil {
		if sqlState(err) != "42501" {
			return nil, err
		}
		known = false
		rows, err = conn.Query(ctx, `SELECT rolname, rolcanlogin, rolcreatedb, rolcreaterole, rolinherit, rolsuper, rolconnlimit, CASE WHEN isfinite(rolvaliduntil) THEN rolvaliduntil END, '' FROM pg_roles WHERE rolname NOT LIKE 'pg\_%'`)
		if err != nil {
			return nil, err
		}
	}
	list, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (observedRole, error) {
		var r observedRole
		err := row.Scan(&r.Name, &r.Login, &r.CreateDB, &r.CreateRole, &r.Inherit, &r.Super, &r.ConnectionLimit, &r.ValidUntil, &r.Verifier)
		r.VerifierKnown = known
		return r, err
	})
	if err != nil {
		return nil, err
	}
	for _, r := range list {
		obs.Roles[r.Name] = r
	}
	rows, err = conn.Query(ctx, `SELECT r.rolname, m.rolname, am.admin_option FROM pg_auth_members am
		JOIN pg_roles r ON r.oid = am.roleid JOIN pg_roles m ON m.oid = am.member`)
	if err != nil {
		return nil, err
	}
	type mem struct {
		role, member string
		admin        bool
	}
	mems, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (mem, error) {
		var m mem
		err := row.Scan(&m.role, &m.member, &m.admin)
		return m, err
	})
	if err != nil {
		return nil, err
	}
	for _, m := range mems {
		obs.Members[membershipKey{m.role, m.member}] = obs.Members[membershipKey{m.role, m.member}] || m.admin
	}
	if !withGrants {
		return obs, nil
	}
	for _, db := range grantDatabases(d.Grants) {
		dbConn, err := dial(ctx, db)
		if err != nil {
			continue
		}
		for _, g := range d.Grants {
			if g.Database != db {
				continue
			}
			privs, err := observeGrant(ctx, dbConn, g)
			if err != nil {
				continue
			}
			obs.Grants[grantKey{g.Database, g.Kind, g.Schema, g.Name, g.Column, g.Grantee}] = privs
		}
		_ = dbConn.Close(context.WithoutCancel(ctx))
	}
	return obs, nil
}

// observeGrant returns the privileges (and "<PRIV>*" for grantable ones)
// the grantee holds directly on the object; nil, nil when the kind is not
// inspected.
func observeGrant(ctx context.Context, conn ShardConn, g catalog.DesiredGrant) (map[string]bool, error) {
	var sql string
	args := []any{g.Grantee, g.Name, g.Schema}
	const schemaMatch = `($3 = '' AND n.nspname = ANY (current_schemas(false)) OR n.nspname = $3)`
	switch g.Kind {
	case "table", "sequence":
		if g.Column != "" {
			sql = `SELECT a.privilege_type, a.is_grantable FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				JOIN pg_attribute att ON att.attrelid = c.oid AND att.attname = $4,
				LATERAL aclexplode(coalesce(att.attacl, '{}'::aclitem[]) || coalesce(c.relacl, '{}'::aclitem[])) a JOIN pg_roles r ON r.oid = a.grantee
				WHERE c.relname = $2 AND ` + schemaMatch + ` AND r.rolname = $1`
			args = append(args, g.Column)
		} else {
			sql = `SELECT a.privilege_type, a.is_grantable FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace,
				LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a JOIN pg_roles r ON r.oid = a.grantee
				WHERE c.relname = $2 AND ` + schemaMatch + ` AND r.rolname = $1`
		}
	case "schema":
		sql = `SELECT a.privilege_type, a.is_grantable FROM pg_namespace n, LATERAL aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) a
			JOIN pg_roles r ON r.oid = a.grantee WHERE n.nspname = $2 AND r.rolname = $1 AND $3 = $3`
	case "database":
		sql = `SELECT a.privilege_type, a.is_grantable FROM pg_database d, LATERAL aclexplode(coalesce(d.datacl, acldefault('d', d.datdba))) a
			JOIN pg_roles r ON r.oid = a.grantee WHERE d.datname = $2 AND r.rolname = $1 AND $3 = $3`
	default:
		return nil, nil
	}
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	type priv struct {
		name      string
		grantable bool
	}
	list, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (priv, error) {
		var p priv
		err := row.Scan(&p.name, &p.grantable)
		return p, err
	})
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, p := range list {
		out[p.name] = true
		if p.grantable {
			out[p.name+"*"] = true
		}
	}
	return out, nil
}

// diffGroup turns desired and observed into one status row per desired
// role plus an "unmanaged" row per other non-system role. Memberships are
// additive: a membership the catalog does not list (for example the one
// PostgreSQL grants a CREATEROLE creator) is not drift.
func diffGroup(group string, d *catalog.DesiredRoles, obs *observedGroup, withGrants bool) []RoleStatus {
	var rows []RoleStatus
	managed := map[string]bool{}
	for _, r := range d.Roles {
		managed[r.Name] = true
	}
	for _, r := range d.Roles {
		st := RoleStatus{Role: r.Name, Group: group, State: RoleInSync, Details: map[string]any{}}
		o, ok := obs.Roles[r.Name]
		if !ok {
			st.State = RoleMissing
			rows = append(rows, st)
			continue
		}
		if o.Super {
			st.State = RoleUnmanagedSuperuser
			st.Details["superuser"] = true
			st.Details["note"] = "superuser on this group; never altered"
			rows = append(rows, st)
			continue
		}
		var attrs []string
		if o.Login != r.Login {
			attrs = append(attrs, "login")
		}
		if o.CreateDB != r.CreateDB {
			attrs = append(attrs, "createdb")
		}
		if o.CreateRole != r.CreateRole {
			attrs = append(attrs, "createrole")
		}
		if o.Inherit != r.Inherit {
			attrs = append(attrs, "inherit")
		}
		if o.ConnectionLimit != r.ConnectionLimit {
			attrs = append(attrs, "connection_limit")
		}
		if !sameTime(o.ValidUntil, r.ValidUntil) {
			attrs = append(attrs, "valid_until")
		}
		if len(attrs) > 0 {
			st.Details["attributes"] = attrs
		}
		if o.VerifierKnown && r.Verifier != "" && !SameVerifier(o.Verifier, r.Verifier) {
			st.Details["verifier"] = "differs"
		}
		var missingMembers []string
		for _, m := range d.Members {
			if m.Member != r.Name {
				continue
			}
			admin, ok := obs.Members[membershipKey{m.Role, m.Member}]
			if !ok || m.Admin && !admin {
				missingMembers = append(missingMembers, m.Role)
			}
		}
		sort.Strings(missingMembers)
		if len(missingMembers) > 0 {
			st.Details["missing_memberships"] = missingMembers
		}
		if withGrants {
			var missingGrants []string
			for _, g := range d.Grants {
				if g.Grantee != r.Name {
					continue
				}
				have, ok := obs.Grants[grantKey{g.Database, g.Kind, g.Schema, g.Name, g.Column, g.Grantee}]
				if !ok {
					continue
				}
				for _, p := range g.Privileges {
					want := p
					if g.GrantOption {
						want += "*"
					}
					if !have[want] {
						missingGrants = append(missingGrants, fmt.Sprintf("%s ON %s %s", want, g.Kind, objectName(g)))
					}
				}
			}
			if len(missingGrants) > 0 {
				st.Details["missing_grants"] = missingGrants
			}
		}
		if len(st.Details) > 0 {
			st.State = RoleDrifted
		}
		rows = append(rows, st)
	}
	var others []string
	for name, o := range obs.Roles {
		if !managed[name] && !o.Super {
			others = append(others, name)
		}
	}
	sort.Strings(others)
	for _, name := range others {
		rows = append(rows, RoleStatus{Role: name, Group: group, State: RoleUnmanaged, Details: map[string]any{"note": "not in pgshard.roles; left alone"}})
	}
	return rows
}

func objectName(g catalog.DesiredGrant) string {
	n := g.Name
	if g.Schema != "" {
		n = g.Schema + "." + n
	}
	if g.Column != "" {
		n += "(" + g.Column + ")"
	}
	return n
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// SameVerifier reports whether two SCRAM verifiers are the same secret:
// equal after parsing (salt, iterations, stored and server keys), so
// formatting differences do not count as drift.
func SameVerifier(a, b string) bool {
	if a == b {
		return true
	}
	va, erra := pgwire.ParseSCRAMVerifier(a)
	vb, errb := pgwire.ParseSCRAMVerifier(b)
	if erra != nil || errb != nil {
		return false
	}
	return va.String() == vb.String()
}

// NewRolesMaterializer returns a function that materializes desired roles
// on the group reachable at dsn (the maintenance database of the DSN; other
// databases of the group are dialed by name).
func NewRolesMaterializer(dsn string) func(ctx context.Context, d *catalog.DesiredRoles) error {
	return func(ctx context.Context, d *catalog.DesiredRoles) error {
		return MaterializeRoles(ctx, func(ctx context.Context, database string) (ShardConn, error) {
			cfg, err := pgx.ParseConfig(dsn)
			if err != nil {
				return nil, err
			}
			if database != "" {
				cfg.Database = database
			}
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				return nil, err
			}
			return pgxShardConn{conn}, nil
		}, d, true)
	}
}

// CatalogDialer dials the catalog group through the catalog pool.
func CatalogDialer(pool *pgxpool.Pool) func(ctx context.Context) (ShardConn, error) {
	return func(ctx context.Context) (ShardConn, error) {
		c, err := pool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		return poolShardConn{c}, nil
	}
}

type poolShardConn struct{ *pgxpool.Conn }

func (c poolShardConn) Exec(ctx context.Context, sql string, args ...any) (pgconnTag, error) {
	return c.Conn.Exec(ctx, sql, args...)
}

// Close resets the session settings a step set (lock_timeout, role) so the
// pooled connection returns clean.
func (c poolShardConn) Close(ctx context.Context) error {
	if _, err := c.Conn.Exec(ctx, "RESET ALL"); err != nil {
		_ = c.Conn.Conn().Close(ctx)
	}
	c.Release()
	return nil
}
