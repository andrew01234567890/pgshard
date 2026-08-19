package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// DatabaseDialer opens a connection to one database of a shard's primary;
// database "" means the maintenance database of the DSN. DialDatabaseAs
// opens it with different login credentials than the DSN carries.
type DatabaseDialer interface {
	DialDatabase(ctx context.Context, shardSet string, shardID int32, database string) (ShardConn, error)
	DialDatabaseAs(ctx context.Context, shardSet string, shardID int32, database, user, password string) (ShardConn, error)
}

// DefaultDDLRole is the non-superuser login client statements run through.
const DefaultDDLRole = "pgshard_ddl"

// MigrationStore is the catalog side of the applier.
type MigrationStore interface {
	// Pending lists queued and running migrations oldest first.
	Pending(ctx context.Context) ([]catalog.DDLMigration, error)
	// Save writes state, per-shard detail and error of m.
	Save(ctx context.Context, m catalog.DDLMigration) error
	// Shards lists the shard ids of a shard set.
	Shards(ctx context.Context, shardSet string) ([]int32, error)
	// Exec runs a catalog statement (desired-state mirroring).
	Exec(ctx context.Context, sql string, args ...any) error
}

// PGMigrationStore is the MigrationStore over the catalog pool.
type PGMigrationStore struct {
	Pool *pgxpool.Pool
}

// Pending implements MigrationStore.
func (s *PGMigrationStore) Pending(ctx context.Context) ([]catalog.DDLMigration, error) {
	return catalog.PendingMigrations(ctx, s.Pool)
}

// Save implements MigrationStore.
func (s *PGMigrationStore) Save(ctx context.Context, m catalog.DDLMigration) error {
	return catalog.SaveMigrationProgress(ctx, s.Pool, m)
}

// Shards implements MigrationStore.
func (s *PGMigrationStore) Shards(ctx context.Context, shardSet string) ([]int32, error) {
	rows, err := s.Pool.Query(ctx, `SELECT shard_id FROM pgshard.shard_status WHERE shard_set = $1 ORDER BY shard_id`, shardSet)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int32])
}

// Exec implements MigrationStore.
func (s *PGMigrationStore) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := s.Pool.Exec(ctx, sql, args...)
	return err
}

// Backoff bounds the retries of a shard step that could not take its lock.
type Backoff struct {
	Min   time.Duration
	Max   time.Duration
	Total time.Duration
}

// DefaultBackoff retries from 0.5s doubling to 30s for up to five minutes.
var DefaultBackoff = Backoff{Min: 500 * time.Millisecond, Max: 30 * time.Second, Total: 5 * time.Minute}

// DefaultLockTimeout is the lock_timeout every shard step runs under.
const DefaultLockTimeout = 2 * time.Second

// Applier drives queued migrations across their target shards, one
// migration at a time, oldest first. It is safe to restart at any point:
// per-shard progress is in the migration row and every step is re-driven
// idempotently.
type Applier struct {
	Store MigrationStore
	// Shards dials shard primaries as a superuser; it only provisions the
	// DDL role, never runs client statements.
	Shards DatabaseDialer
	// Catalog, when set, dials the catalog group so role statements also
	// apply there (the router authenticates against catalog verifiers and
	// the pooler dials shards as the real user).
	Catalog func(ctx context.Context) (ShardConn, error)
	// Roles, when set, materializes the desired roles on groups that are
	// behind before migrations run.
	Roles  *RoleVerifier
	Logger *slog.Logger
	// DDLRole is the non-superuser login every client statement runs
	// through (SET ROLE into the client role from there), so a function a
	// statement evaluates can RESET ROLE only into a plain role. It
	// defaults to DefaultDDLRole; the applier creates it on every shard and
	// sets a fresh password per process before first use.
	DDLRole string
	// ShardSet defaults to "default".
	ShardSet string
	// LockTimeout defaults to DefaultLockTimeout.
	LockTimeout time.Duration
	// Backoff defaults to DefaultBackoff.
	Backoff Backoff
	// Sleep overrides waiting between retries in tests.
	Sleep func(ctx context.Context, d time.Duration) error
	// Now overrides the clock in tests.
	Now func() time.Time

	ddlMu       sync.Mutex
	ddlPassword string
	ddlReady    map[int32]bool
	leader      func() bool
}

func (a *Applier) lostLeadership() bool { return a.leader != nil && !a.leader() }

func (a *Applier) ddlRole() string {
	if a.DDLRole == "" {
		return DefaultDDLRole
	}
	return a.DDLRole
}

func (a *Applier) logger() *slog.Logger {
	if a.Logger == nil {
		return slog.Default()
	}
	return a.Logger
}

func (a *Applier) shardSet() string {
	if a.ShardSet == "" {
		return decisionShardSet
	}
	return a.ShardSet
}

func (a *Applier) backoff() Backoff {
	b := a.Backoff
	if b.Min <= 0 {
		b.Min = DefaultBackoff.Min
	}
	if b.Max <= 0 {
		b.Max = DefaultBackoff.Max
	}
	if b.Total <= 0 {
		b.Total = DefaultBackoff.Total
	}
	return b
}

func (a *Applier) sleep(ctx context.Context, d time.Duration) error {
	if a.Sleep != nil {
		return a.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (a *Applier) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

// Run drives pending migrations every interval while leader() is true.
func (a *Applier) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	a.leader = leader
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if leader != nil && !leader() {
			continue
		}
		if _, err := a.RunOnce(ctx); err != nil && ctx.Err() == nil {
			a.logger().Warn("applier pass failed", "err", err)
		}
	}
}

// RunOnce drives every pending migration to completion or failure and
// returns how many it finished.
func (a *Applier) RunOnce(ctx context.Context) (int, error) {
	pending, err := a.Store.Pending(ctx)
	if err != nil {
		return 0, fmt.Errorf("applier: pending migrations: %w", err)
	}
	if a.Roles != nil && len(pending) == 0 {
		if err := a.Roles.MaterializeStale(ctx); err != nil {
			a.logger().Warn("materializing roles on stale groups failed", "err", err)
		}
	}
	done := 0
	for _, m := range pending {
		if err := a.drive(ctx, m); err != nil {
			return done, err
		}
		done++
	}
	return done, nil
}

// drive runs one migration to a final state.
func (a *Applier) drive(ctx context.Context, m catalog.DDLMigration) error {
	logger := a.logger().With("migration", m.ID, "kind", m.Kind, "database", m.Database)
	if m.State == catalog.MigrationQueued {
		targets, err := a.targets(ctx, m)
		if err != nil {
			return err
		}
		m.PerShard = map[string]catalog.ShardMigration{}
		for _, id := range targets {
			m.PerShard[shardKey(id)] = catalog.ShardMigration{State: catalog.ShardPending}
		}
		if a.Catalog != nil && roleStatement(m.Kind) {
			m.PerShard[catalogKey] = catalog.ShardMigration{State: catalog.ShardPending}
		}
		m.State = catalog.MigrationRunning
		if err := a.Store.Save(ctx, m); err != nil {
			return err
		}
		logger.Info("migration started", "shards", len(targets))
	}
	for _, key := range sortedShardKeys(m.PerShard) {
		s := m.PerShard[key]
		if s.State == catalog.ShardApplied || s.State == catalog.ShardSkipped || s.State == catalog.ShardFailed {
			continue
		}
		if key == catalogKey && anyFailed(m.PerShard) {
			s.State, s.Error = catalog.ShardFailed, "not applied: a shard failed"
			m.PerShard[key] = s
			continue
		}
		if a.lostLeadership() {
			return errNotLeader
		}
		id, _ := strconv.ParseInt(key, 10, 32)
		s = a.applyOn(ctx, logger, &m, key, int32(id), s)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		m.PerShard[key] = s
		if err := a.Store.Save(ctx, m); err != nil {
			return err
		}
	}
	m.State, m.Error = catalog.MigrationComplete, ""
	applied := 0
	for _, key := range sortedShardKeys(m.PerShard) {
		s := m.PerShard[key]
		switch s.State {
		case catalog.ShardApplied:
			applied++
		case catalog.ShardFailed:
			if m.State != catalog.MigrationFailed {
				m.State, m.Error = catalog.MigrationFailed, fmt.Sprintf("shard %s: %s", key, s.Error)
			}
		}
	}
	if m.State == catalog.MigrationComplete && applied == 0 && len(m.PerShard) > 0 {
		m.State = catalog.MigrationFailed
		for _, key := range sortedShardKeys(m.PerShard) {
			s := m.PerShard[key]
			s.State = catalog.ShardFailed
			m.PerShard[key] = s
			if m.Error == "" {
				m.Error = fmt.Sprintf("shard %s: %s", key, s.Error)
			}
		}
	}
	if m.State == catalog.MigrationComplete {
		if err := a.mirror(ctx, m); err != nil {
			return err
		}
	}
	if err := a.Store.Save(ctx, m); err != nil {
		return err
	}
	logger.Info("migration finished", "state", m.State, "error", m.Error)
	return nil
}

func shardKey(id int32) string { return strconv.FormatInt(int64(id), 10) }

// catalogKey is the per_shard entry of the catalog group; it sorts last so
// the catalog is touched only after every shard applied.
const catalogKey = "catalog"

// roleStatement reports the kinds that must reach the catalog group too.
func roleStatement(kind string) bool {
	switch kind {
	case "CREATE ROLE", "ALTER ROLE", "DROP ROLE", "GRANT ROLE", "REVOKE ROLE":
		return true
	}
	return false
}

func anyFailed(per map[string]catalog.ShardMigration) bool {
	for _, s := range per {
		if s.State == catalog.ShardFailed {
			return true
		}
	}
	return false
}

func sortedShardKeys(per map[string]catalog.ShardMigration) []string {
	keys := make([]string, 0, len(per))
	for k := range per {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, aerr := strconv.Atoi(keys[i])
		b, berr := strconv.Atoi(keys[j])
		if (aerr == nil) != (berr == nil) {
			return aerr == nil
		}
		if aerr != nil {
			return keys[i] < keys[j]
		}
		return a < b
	})
	return keys
}

// targets picks the shards a migration runs on from its scope.
func (a *Applier) targets(ctx context.Context, m catalog.DDLMigration) ([]int32, error) {
	if m.Scope == "home" {
		return []int32{m.HomeShard}, nil
	}
	ids, err := a.Store.Shards(ctx, a.shardSet())
	if err != nil {
		return nil, fmt.Errorf("applier: shards: %w", err)
	}
	if len(ids) == 0 {
		return nil, errors.New("applier: no shards in shard_status")
	}
	return ids, nil
}

// applyOn runs one shard step, retrying lock and connection failures with
// backoff, and returns the shard's final entry.
func (a *Applier) applyOn(ctx context.Context, logger *slog.Logger, m *catalog.DDLMigration, key string, id int32, s catalog.ShardMigration) catalog.ShardMigration {
	resumed := s.State == catalog.ShardRunning || s.State == catalog.ShardRetrying
	b := a.backoff()
	start, wait := a.now(), b.Min
	for {
		s.Attempts++
		s.State = catalog.ShardRunning
		m.PerShard[key] = s
		if err := a.Store.Save(ctx, *m); err != nil {
			logger.Warn("progress not saved", "err", err)
		}
		outcome, err := a.step(ctx, m, key, id, resumed)
		resumed = true
		if err == nil {
			s.State, s.Error, s.SQLState = outcome, "", ""
			return s
		}
		if ctx.Err() != nil {
			return s
		}
		s.Error, s.SQLState = err.Error(), sqlState(err)
		var skip *skippedError
		if errors.As(err, &skip) {
			s.State, s.Error = catalog.ShardSkipped, skip.err.Error()
			return s
		}
		if !transient(err) {
			s.State = catalog.ShardFailed
			logger.Warn("shard step failed", "shard", id, "err", err)
			return s
		}
		if a.now().Sub(start) >= b.Total {
			s.State = catalog.ShardFailed
			logger.Warn("shard step gave up", "shard", id, "attempts", s.Attempts, "err", err)
			return s
		}
		s.State = catalog.ShardRetrying
		m.PerShard[key] = s
		if err := a.Store.Save(ctx, *m); err != nil {
			logger.Warn("progress not saved", "err", err)
		}
		logger.Info("shard step retrying", "shard", id, "attempt", s.Attempts, "wait", wait, "err", err)
		if err := a.sleep(ctx, wait); err != nil {
			return s
		}
		if a.lostLeadership() {
			return s
		}
		wait *= 2
		if wait > b.Max {
			wait = b.Max
		}
	}
}

// prepare opens the session a statement runs on: for a shard, the DDL
// role logged into the target database with the client's role assumed;
// for the catalog group, a catalog connection as is. lock_timeout is set
// on both.
func (a *Applier) prepare(ctx context.Context, m *catalog.DDLMigration, key string, id int32, db string) (ShardConn, error) {
	var conn ShardConn
	if key == catalogKey {
		c, err := a.Catalog(ctx)
		if err != nil {
			return nil, &dialError{err}
		}
		conn = c
	} else {
		password, err := a.provisionDDLRole(ctx, id, m.Meta.RunAs)
		if err != nil {
			return nil, err
		}
		conn, err = a.Shards.DialDatabaseAs(ctx, a.shardSet(), id, db, a.ddlRole(), password)
		if err != nil {
			return nil, &dialError{err}
		}
	}
	timeout := a.LockTimeout
	if timeout <= 0 {
		timeout = DefaultLockTimeout
	}
	if _, err := conn.Exec(ctx, "SET lock_timeout = "+quoteLiteral(fmt.Sprint(timeout.Milliseconds())+"ms")); err != nil {
		_ = conn.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	if m.Meta.RunAs != "" && key != catalogKey {
		if _, err := conn.Exec(ctx, "SET ROLE "+pgx.Identifier{m.Meta.RunAs}.Sanitize()); err != nil {
			_ = conn.Close(context.WithoutCancel(ctx))
			return nil, err
		}
	}
	return conn, nil
}

// step executes the statement on one shard once. A resumed step first
// checks whether the object already matches (the previous attempt may have
// committed before its progress was saved); an index left invalid by an
// interrupted CREATE INDEX CONCURRENTLY is dropped and built again.
func (a *Applier) step(ctx context.Context, m *catalog.DDLMigration, key string, id int32, resumed bool) (string, error) {
	db := m.Database
	if m.Meta.Object.Kind == "role" || m.Meta.Object.Kind == "database" || strings.HasSuffix(m.Kind, "ROLE") {
		db = ""
	}
	conn, err := a.prepare(ctx, m, key, id, db)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if resumed && m.Meta.Object.Kind != "" {
		switch {
		case m.Kind == "CREATE INDEX" && m.Meta.Object.Name != "":
			dropped, err := dropInvalidIndex(ctx, conn, m.Meta.Object)
			if err != nil {
				return "", err
			}
			if !dropped {
				matches, err := objectMatches(ctx, conn, m.Meta.Object)
				if err != nil {
					return "", err
				}
				if matches {
					return catalog.ShardApplied, nil
				}
			}
		default:
			matches, err := objectMatches(ctx, conn, m.Meta.Object)
			if err != nil {
				return "", err
			}
			if matches {
				return catalog.ShardApplied, nil
			}
		}
	}
	if m.Strategy == "concurrent" || outsideTransaction(m.Kind) {
		err = a.concurrently(ctx, conn, m)
	} else {
		err = inTransaction(ctx, conn, m.Statement)
	}
	if err != nil {
		if m.Scope == "existing" && missingObject(err) {
			return "", &skippedError{err}
		}
		return "", err
	}
	return catalog.ShardApplied, nil
}

// provisionDDLRole makes sure the DDL role exists on shard id with this
// process's password (once per shard) and may SET ROLE into runAs, then
// returns the password. A superuser runAs is refused: the DDL session must
// never be able to become one.
func (a *Applier) provisionDDLRole(ctx context.Context, id int32, runAs string) (string, error) {
	a.ddlMu.Lock()
	defer a.ddlMu.Unlock()
	if a.ddlPassword == "" {
		buf := make([]byte, 24)
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}
		a.ddlPassword = hex.EncodeToString(buf)
		a.ddlReady = map[int32]bool{}
	}
	if a.ddlReady[id] && runAs == "" {
		return a.ddlPassword, nil
	}
	conn, err := a.Shards.DialDatabase(ctx, a.shardSet(), id, "")
	if err != nil {
		return "", &dialError{err}
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	role := pgx.Identifier{a.ddlRole()}.Sanitize()
	if !a.ddlReady[id] {
		for _, sql := range []string{
			fmt.Sprintf(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s) THEN
				CREATE ROLE %s LOGIN NOSUPERUSER NOINHERIT NOBYPASSRLS NOREPLICATION CREATEDB CREATEROLE; END IF; END $$`, quoteLiteral(a.ddlRole()), role),
			"ALTER ROLE " + role + " LOGIN NOSUPERUSER NOINHERIT NOBYPASSRLS NOREPLICATION CREATEDB CREATEROLE PASSWORD " + quoteLiteral(a.ddlPassword),
		} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				return "", fmt.Errorf("provisioning %s: %w", a.ddlRole(), err)
			}
		}
		a.ddlReady[id] = true
	}
	if runAs == "" {
		return a.ddlPassword, nil
	}
	rows, err := conn.Query(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = $1`, runAs)
	if err != nil {
		return "", err
	}
	super, err := pgx.CollectOneRow(rows, pgx.RowTo[bool])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", &pgconn.PgError{Severity: "ERROR", Code: "42704", Message: fmt.Sprintf("role %q does not exist on the shard", runAs)}
		}
		return "", err
	}
	if super {
		return "", &pgconn.PgError{Severity: "ERROR", Code: "42501", Message: fmt.Sprintf("role %q is a superuser: DDL through the router runs as a plain role only", runAs)}
	}
	if _, err := conn.Exec(ctx, "GRANT "+pgx.Identifier{runAs}.Sanitize()+" TO "+role+" WITH SET TRUE, INHERIT FALSE"); err != nil {
		return "", fmt.Errorf("granting %s to %s: %w", runAs, a.ddlRole(), err)
	}
	return a.ddlPassword, nil
}

// outsideTransaction lists the kinds PostgreSQL refuses inside a
// transaction block.
func outsideTransaction(kind string) bool {
	return kind == "CREATE DATABASE" || kind == "DROP DATABASE"
}

func inTransaction(ctx context.Context, conn ShardConn, sql string) error {
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, sql); err != nil {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "ROLLBACK")
		return err
	}
	_, err := conn.Exec(ctx, "COMMIT")
	return err
}

// concurrently runs a CONCURRENTLY statement outside a transaction. When
// CREATE INDEX CONCURRENTLY fails it leaves an invalid index behind; that
// index is dropped and the statement run once more before giving up.
func (a *Applier) concurrently(ctx context.Context, conn ShardConn, m *catalog.DDLMigration) error {
	_, err := conn.Exec(ctx, m.Statement)
	if err == nil || m.Kind != "CREATE INDEX" || m.Meta.Object.Name == "" {
		return err
	}
	invalid, ierr := invalidIndex(ctx, conn, m.Meta.Object)
	if ierr != nil || !invalid {
		return err
	}
	if _, derr := conn.Exec(context.WithoutCancel(ctx), "DROP INDEX CONCURRENTLY IF EXISTS "+qualified(m.Meta.Object.Schema, m.Meta.Object.Name)); derr != nil {
		return err
	}
	_, err = conn.Exec(ctx, m.Statement)
	return err
}

// dropInvalidIndex removes idx when it exists but is invalid and reports
// whether it did.
func dropInvalidIndex(ctx context.Context, conn ShardConn, idx catalog.MigrationObject) (bool, error) {
	invalid, err := invalidIndex(ctx, conn, idx)
	if err != nil || !invalid {
		return false, err
	}
	_, err = conn.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+qualified(idx.Schema, idx.Name))
	return err == nil, err
}

func qualified(schema, name string) string {
	if schema != "" {
		return pgx.Identifier{schema, name}.Sanitize()
	}
	return pgx.Identifier{name}.Sanitize()
}

func invalidIndex(ctx context.Context, conn ShardConn, o catalog.MigrationObject) (bool, error) {
	rows, err := conn.Query(ctx, `SELECT NOT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = $1 AND ($2 = '' AND n.nspname = ANY (current_schemas(false)) OR n.nspname = $2)`, o.Name, o.Schema)
	if err != nil {
		return false, err
	}
	flags, err := pgx.CollectRows(rows, pgx.RowTo[bool])
	if err != nil {
		return false, err
	}
	for _, f := range flags {
		if f {
			return true, nil
		}
	}
	return false, nil
}

// objectMatches reports whether o is present or absent as expected.
func objectMatches(ctx context.Context, conn ShardConn, o catalog.MigrationObject) (bool, error) {
	var sql string
	args := []any{o.Name}
	switch o.Kind {
	case "relation":
		sql = `SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname = $1 AND ($2 = '' AND n.nspname = ANY (current_schemas(false)) OR n.nspname = $2))`
		args = append(args, o.Schema)
	case "schema":
		sql = `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`
	case "type":
		sql = `SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = $1)`
	case "role":
		sql = `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`
	case "database":
		sql = `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`
	default:
		return false, nil
	}
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return false, err
	}
	exists, err := pgx.CollectOneRow(rows, pgx.RowTo[bool])
	if err != nil {
		return false, err
	}
	return exists == (o.Expect == "present"), nil
}

// mirror writes the desired-state rows a completed DCL statement implies.
func (a *Applier) mirror(ctx context.Context, m catalog.DDLMigration) error {
	meta := m.Meta
	stmts := catalog.RoleMirrorStatements(m.Database, meta)
	switch meta.DatabaseOp {
	case "create":
		stmts = append(stmts, catalog.Statement{SQL: `INSERT INTO pgshard.databases (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, Args: []any{meta.Database}})
	case "drop":
		stmts = append(stmts, catalog.Statement{SQL: `DELETE FROM pgshard.databases WHERE name = $1`, Args: []any{meta.Database}})
	}
	for _, st := range stmts {
		if err := a.Store.Exec(ctx, st.SQL, st.Args...); err != nil {
			return fmt.Errorf("applier: mirroring %s into the catalog: %w", m.Kind, err)
		}
	}
	return nil
}

// skippedError marks a shard step whose object does not exist on that
// shard under scope "existing".
type skippedError struct{ err error }

func (e *skippedError) Error() string { return e.err.Error() }
func (e *skippedError) Unwrap() error { return e.err }

type dialError struct{ err error }

func (e *dialError) Error() string { return "connect: " + e.err.Error() }
func (e *dialError) Unwrap() error { return e.err }

func sqlState(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// transient reports the errors a shard step retries: lock timeouts, lost
// connections and shards that cannot be reached right now.
func transient(err error) bool {
	var de *dialError
	if errors.As(err, &de) {
		return true
	}
	if pgconn.SafeToRetry(err) {
		return true
	}
	switch code := sqlState(err); {
	case code == "55P03", code == "40P01", code == "57P01", code == "57P03", code == "40001":
		return true
	case strings.HasPrefix(code, "08"):
		return true
	case code == "":
		return true
	}
	return false
}

func missingObject(err error) bool {
	switch sqlState(err) {
	case "42704", "42P01", "42883", "3F000":
		return true
	}
	return false
}

// DialDatabase implements DatabaseDialer over the shard DSNs.
func (d *PgxShardDialer) DialDatabase(ctx context.Context, shardSet string, shardID int32, database string) (ShardConn, error) {
	return d.DialDatabaseAs(ctx, shardSet, shardID, database, "", "")
}

// DialDatabaseAs implements DatabaseDialer; an empty user keeps the DSN's
// credentials.
func (d *PgxShardDialer) DialDatabaseAs(ctx context.Context, shardSet string, shardID int32, database, user, password string) (ShardConn, error) {
	dsn, err := d.dsn(ctx, shardSet, shardID)
	if err != nil {
		return nil, err
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("shard %s/%d: %w", shardSet, shardID, err)
	}
	if database != "" {
		cfg.Database = database
	}
	if user != "" {
		cfg.User, cfg.Password = user, password
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("shard %s/%d: %w", shardSet, shardID, err)
	}
	return pgxShardConn{conn}, nil
}
