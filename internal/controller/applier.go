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

// DefaultShardConnectTimeout bounds one attempt to reach a shard whose DSN
// does not bound it itself. An explicit connect_timeout=0 means "wait for
// ever", which is the failure this exists to prevent, so it is overridden
// like an absent one.
const DefaultShardConnectTimeout = 10 * time.Second

// MigrationStore is the catalog side of the applier.
type MigrationStore interface {
	// Pending lists queued and running migrations oldest first.
	Pending(ctx context.Context) ([]catalog.DDLMigration, error)
	// Save writes state, per-shard detail and error of m.
	Save(ctx context.Context, m catalog.DDLMigration) error
	// Shards lists the shard ids of a shard set.
	Shards(ctx context.Context, shardSet string) ([]int32, error)
	// Databases lists the logical databases of the catalog.
	Databases(ctx context.Context) ([]string, error)
	// SaveMeta rewrites the meta of a migration.
	SaveMeta(ctx context.Context, id string, meta catalog.MigrationMeta) error
	// Exec runs a catalog statement (desired-state mirroring).
	Exec(ctx context.Context, sql string, args ...any) error
	// LockedDatabases lists the databases a workflow currently holds the
	// DDL lock on.
	LockedDatabases(ctx context.Context) (map[string]string, error)
	// ServingShardSet names the shard set currently serving. It is read
	// per pass rather than fixed at start-up: a reshard or major upgrade
	// retires one set and promotes another, and work aimed at the retired
	// one reaches shards nobody is reading from.
	ServingShardSet(ctx context.Context) (string, error)
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

// Databases implements MigrationStore.
func (s *PGMigrationStore) Databases(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT name FROM pgshard.databases ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// LockedDatabases implements MigrationStore. A reshard or upgrade cutover
// takes these when it fences, precisely so that schema does not move under
// a copy that is comparing the two sides.
func (s *PGMigrationStore) LockedDatabases(ctx context.Context) (map[string]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT key, workflow_id::text FROM pgshard.workflow_locks WHERE kind = 'ddl'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	held := map[string]string{}
	for rows.Next() {
		var db, wf string
		if err := rows.Scan(&db, &wf); err != nil {
			return nil, err
		}
		held[db] = wf
	}
	return held, rows.Err()
}

// ServingShardSet implements MigrationStore.
func (s *PGMigrationStore) ServingShardSet(ctx context.Context) (string, error) {
	return catalog.ServingShardSet(ctx, s.Pool)
}

// SaveMeta implements MigrationStore.
func (s *PGMigrationStore) SaveMeta(ctx context.Context, id string, meta catalog.MigrationMeta) error {
	return catalog.SaveMigrationMeta(ctx, s.Pool, id, meta)
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
	// ShardSet pins the shard set to apply to; empty means whichever set
	// is serving at the time of each pass.
	ShardSet string
	// LockTimeout defaults to DefaultLockTimeout.
	LockTimeout time.Duration
	// Backoff defaults to DefaultBackoff.
	Backoff Backoff
	// RewriteSettle overrides DefaultRewriteSettle; negative disables the
	// wait (tests).
	RewriteSettle time.Duration
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

// shardSet is the set to apply to: the configured override when there is
// one, otherwise whichever set is serving now.
func (a *Applier) shardSet(ctx context.Context) (string, error) {
	if a.ShardSet != "" {
		return a.ShardSet, nil
	}
	return a.Store.ServingShardSet(ctx)
}

// migrationSet is the set a migration runs against: the one pinned when it
// started, so that a cutover part-way through cannot silently redirect the
// remaining shards at a different set's databases.
func (a *Applier) migrationSet(ctx context.Context, m *catalog.DDLMigration) (string, error) {
	if m != nil && m.Meta.ShardSet != "" {
		return m.Meta.ShardSet, nil
	}
	return a.shardSet(ctx)
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
	runLoop(ctx, interval, leader, a.logger, "applier", func(ctx context.Context) {
		if _, err := a.RunOnce(ctx); err != nil && ctx.Err() == nil {
			a.logger().Warn("applier pass failed", "err", err)
		}
	})
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
	if len(pending) == 0 {
		if err := a.SweepRewriteArtifacts(ctx); err != nil && ctx.Err() == nil {
			a.logger().Warn("sweeping rewrite artifacts failed", "err", err)
		}
	}
	// A cutover writes a DDL lock per database when it fences, so that the
	// schema cannot move under a copy that is comparing the two sides. The
	// locks were being written and never read: the migration simply ran.
	// A migration already part-applied is driven to a final state rather
	// than abandoned half-way, since stopping there is worse.
	held, err := a.Store.LockedDatabases(ctx)
	if err != nil {
		return 0, fmt.Errorf("applier: ddl locks: %w", err)
	}
	done := 0
	for _, m := range pending {
		if wf, locked := held[m.Database]; locked && m.State == catalog.MigrationQueued {
			a.logger().Info("holding a migration while a workflow has the database's DDL lock",
				"migration", m.ID, "database", m.Database, "workflow", wf)
			continue
		}
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
	serving, err := a.shardSet(ctx)
	if err != nil {
		return fmt.Errorf("applier: serving shard set: %w", err)
	}
	if m.State == catalog.MigrationRunning && m.Meta.ShardSet != "" && m.Meta.ShardSet != serving {
		if touched(m.PerShard) {
			m.State = catalog.MigrationFailed
			m.Error = fmt.Sprintf("planned against shard set %s, which is no longer serving (%s is); "+
				"per-shard progress cannot be read against another set, so this migration needs reconciling by hand",
				m.Meta.ShardSet, serving)
			logger.Error("migration straddled a cutover", "planned", m.Meta.ShardSet, "serving", serving)
			return a.Store.Save(ctx, m)
		}
		logger.Info("replanning migration onto the serving shard set", "planned", m.Meta.ShardSet, "serving", serving)
		m.State, m.Meta.ShardSet = catalog.MigrationQueued, ""
	}
	if m.State == catalog.MigrationQueued {
		targets, err := a.targets(ctx, m, serving)
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
		m.State, m.Meta.ShardSet = catalog.MigrationRunning, serving
		if err := a.Store.Save(ctx, m); err != nil {
			return err
		}
		logger.Info("migration started", "shards", len(targets), "shard_set", serving)
	}
	if m.Strategy == catalog.StrategyRewrite {
		return a.driveRewrite(ctx, logger, &m)
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

// touched reports whether any shard of the migration has been driven past
// pending, which is what makes its per-shard progress unreadable against a
// different set.
func touched(per map[string]catalog.ShardMigration) bool {
	for _, s := range per {
		if s.State != catalog.ShardPending {
			return true
		}
	}
	return false
}

// targets picks the shards a migration runs on from its scope.
func (a *Applier) targets(ctx context.Context, m catalog.DDLMigration, set string) ([]int32, error) {
	if m.Scope == "home" {
		return []int32{m.HomeShard}, nil
	}
	ids, err := a.Store.Shards(ctx, set)
	if err != nil {
		return nil, fmt.Errorf("applier: shards: %w", err)
	}
	if len(ids) == 0 {
		return nil, errors.New("applier: no shards in shard_status")
	}
	return ids, nil
}

// applyOn drives one shard to its final entry: a single statement, or the
// steps of a multistep migration one after the other, each retried on lock
// and connection failures with backoff.
func (a *Applier) applyOn(ctx context.Context, logger *slog.Logger, m *catalog.DDLMigration, key string, id int32, s catalog.ShardMigration) catalog.ShardMigration {
	if m.Strategy != "multistep" {
		resumed := s.State == catalog.ShardRunning || s.State == catalog.ShardRetrying
		return a.retrying(ctx, logger, m, key, id, s, func() (string, error) {
			defer func() { resumed = true }()
			return a.step(ctx, m, key, id, resumed)
		})
	}
	steps := m.Meta.Steps
	for s.Step < len(steps) {
		st := steps[s.Step]
		s = a.retrying(ctx, logger, m, key, id, s, func() (string, error) { return a.runStep(ctx, m, id, st) })
		if s.State != catalog.ShardApplied {
			return s
		}
		s.Step++
		m.PerShard[key] = s
		if err := a.Store.Save(ctx, *m); err != nil {
			logger.Warn("progress not saved", "err", err)
		}
	}
	return s
}

// retrying runs one shard step until it ends, retrying transient failures
// with backoff, and returns the shard's entry.
func (a *Applier) retrying(ctx context.Context, logger *slog.Logger, m *catalog.DDLMigration, key string, id int32, s catalog.ShardMigration, run func() (string, error)) catalog.ShardMigration {
	b := a.backoff()
	start, wait := a.now(), b.Min
	for {
		s.Attempts++
		s.State = catalog.ShardRunning
		m.PerShard[key] = s
		if err := a.Store.Save(ctx, *m); err != nil {
			logger.Warn("progress not saved", "err", err)
		}
		outcome, err := run()
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
			logger.Warn("shard step failed", "shard", id, "step", s.Step, "err", err)
			return s
		}
		if a.now().Sub(start) >= b.Total {
			s.State = catalog.ShardFailed
			logger.Warn("shard step gave up", "shard", id, "step", s.Step, "attempts", s.Attempts, "err", err)
			return s
		}
		s.State = catalog.ShardRetrying
		m.PerShard[key] = s
		if err := a.Store.Save(ctx, *m); err != nil {
			logger.Warn("progress not saved", "err", err)
		}
		logger.Info("shard step retrying", "shard", id, "step", s.Step, "attempt", s.Attempts, "wait", wait, "err", err)
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
		set, serr := a.migrationSet(ctx, m)
		if serr != nil {
			return nil, serr
		}
		password, err := a.provisionDDLRole(ctx, set, id, m.Meta.RunAs)
		if err != nil {
			return nil, err
		}
		conn, err = a.Shards.DialDatabaseAs(ctx, set, id, db, a.ddlRole(), password)
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
	if key != catalogKey {
		defer a.releaseDDLRole(ctx, m, id, m.Meta.RunAs)
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
				// A resumed DROP under scope "existing" cannot tell "we
				// dropped it" from "it never existed on this shard": count
				// it skipped so a migration where no shard had the object
				// still fails instead of reporting applied.
				if m.Scope == "existing" && m.Meta.Object.Expect == "absent" {
					return "", &skippedError{fmt.Errorf("%s %q is not present on this shard", m.Meta.Object.Kind, m.Meta.Object.Name)}
				}
				return catalog.ShardApplied, nil
			}
		}
	}
	switch {
	case m.Meta.Repack:
		err = a.repackStep(ctx, conn, m)
	case m.Strategy == "concurrent" || outsideTransaction(m.Kind):
		err = a.concurrently(ctx, conn, m)
	default:
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
// process's password and no memberships (once per shard) and may SET ROLE
// into runAs, then returns the password. A superuser runAs is refused: the
// DDL session must never be able to become one.
func (a *Applier) provisionDDLRole(ctx context.Context, set string, id int32, runAs string) (string, error) {
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
	conn, err := a.Shards.DialDatabase(ctx, set, id, "")
	if err != nil {
		return "", &dialError{err}
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	role := pgx.Identifier{a.ddlRole()}.Sanitize()
	if !a.ddlReady[id] {
		for _, sql := range []string{
			fmt.Sprintf(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s) THEN
				CREATE ROLE %s LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION; END IF; END $$`, quoteLiteral(a.ddlRole()), role),
			"ALTER ROLE " + role + " LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION PASSWORD " + quoteLiteral(a.ddlPassword),
			revokeAllMemberships(a.ddlRole()),
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

// revokeAllMemberships drops every membership the DDL role holds: a
// previous process may have died between a grant and its revoke.
func revokeAllMemberships(ddlRole string) string {
	return fmt.Sprintf(`DO $$ DECLARE r text; BEGIN FOR r IN SELECT roleid::regrole::text FROM pg_auth_members WHERE member = %s::regrole LOOP
		EXECUTE format('REVOKE %%s FROM %%I', r, %s); END LOOP; END $$`, quoteLiteral(ddlRole), quoteLiteral(ddlRole))
}

// releaseDDLRole revokes runAs from the DDL role on shard id once the
// statement that needed it finished, however it ended, so a later session
// cannot SET ROLE into a tenant it is not running for.
func (a *Applier) releaseDDLRole(ctx context.Context, m *catalog.DDLMigration, id int32, runAs string) {
	if runAs == "" {
		return
	}
	ctx = context.WithoutCancel(ctx)
	set, serr := a.migrationSet(ctx, m)
	if serr != nil {
		a.logger().Warn("revoking DDL role membership: shard set", "shard", id, "role", runAs, "err", serr)
		return
	}
	conn, err := a.Shards.DialDatabase(ctx, set, id, "")
	if err != nil {
		a.logger().Warn("revoking DDL role membership: connect failed", "shard", id, "role", runAs, "err", err)
		return
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "REVOKE "+pgx.Identifier{runAs}.Sanitize()+" FROM "+pgx.Identifier{a.ddlRole()}.Sanitize()); err != nil {
		a.logger().Warn("revoking DDL role membership failed", "shard", id, "role", runAs, "err", err)
	}
}

// runStep executes one step of a multistep migration on one shard. The
// step is skipped when its check says it already happened; a CREATE INDEX
// CONCURRENTLY step drops an invalid leftover before running, rebuilds
// once on failure and leaves no invalid index behind; a step with OnFail
// runs it after a hard failure so a re-run starts clean.
func (a *Applier) runStep(ctx context.Context, m *catalog.DDLMigration, id int32, st catalog.MigrationStep) (string, error) {
	defer a.releaseDDLRole(ctx, m, id, m.Meta.RunAs)
	conn, err := a.prepare(ctx, m, shardKey(id), id, m.Database)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	done, err := checkHolds(ctx, conn, st.Skip)
	if err != nil {
		return "", err
	}
	if done {
		return catalog.ShardApplied, nil
	}
	run := func() error {
		if st.Concurrent {
			_, err := conn.Exec(ctx, st.SQL)
			return err
		}
		return inTransaction(ctx, conn, st.SQL)
	}
	if st.Index != "" {
		idx := catalog.MigrationObject{Kind: "relation", Schema: st.Skip.Schema, Name: st.Index}
		if _, err := dropInvalidIndex(ctx, conn, idx); err != nil {
			return "", err
		}
		err = run()
		if err != nil && !transient(err) && ctx.Err() == nil {
			if _, derr := dropInvalidIndex(ctx, conn, idx); derr == nil {
				err = run()
			}
		}
		if err != nil && ctx.Err() == nil {
			_, _ = dropInvalidIndex(context.WithoutCancel(ctx), conn, idx)
		}
	} else {
		err = run()
	}
	if err != nil {
		if st.OnFail != "" && !transient(err) && ctx.Err() == nil {
			_, _ = conn.Exec(context.WithoutCancel(ctx), st.OnFail)
		}
		return "", err
	}
	return catalog.ShardApplied, nil
}

// checkHolds evaluates a step's skip predicate on the shard.
func checkHolds(ctx context.Context, conn ShardConn, c catalog.MigrationCheck) (bool, error) {
	// to_regclass resolves a name through the session's search_path exactly
	// as the statement being skipped does. Matching pg_class.relname and a
	// namespace instead would answer for a same-named relation in another
	// schema: a partition may live in a different schema from its parent,
	// so one parent can have two children called the same thing.
	const rel = `pg_catalog.to_regclass(CASE WHEN $2 = '' THEN pg_catalog.quote_ident($1)
		ELSE pg_catalog.quote_ident($2) || '.' || pg_catalog.quote_ident($1) END)::oid`
	const part = `pg_catalog.to_regclass(CASE WHEN $4 = '' THEN pg_catalog.quote_ident($3)
		ELSE pg_catalog.quote_ident($4) || '.' || pg_catalog.quote_ident($3) END)::oid`
	var sql string
	switch c.Kind {
	case "":
		return false, nil
	case "constraint":
		sql = `SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = $3 AND conrelid = ` + rel + `)`
	case "constraint_valid":
		sql = `SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = $3 AND convalidated AND conrelid = ` + rel + `)`
	case "notnull":
		sql = `SELECT EXISTS (SELECT 1 FROM pg_constraint k JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = ANY (k.conkey)
			WHERE k.contype = 'n' AND a.attname = $3 AND k.conrelid = ` + rel + `)`
	case "notnull_valid":
		sql = `SELECT EXISTS (SELECT 1 FROM pg_constraint k JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = ANY (k.conkey)
			WHERE k.contype = 'n' AND k.convalidated AND a.attname = $3 AND k.conrelid = ` + rel + `)`
	case "index_valid":
		sql = `SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			WHERE i.indisvalid AND c.relname = $3 AND i.indrelid = ` + rel + `)`
	case "detached":
		sql = `SELECT NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = ` + part + ` AND inhparent = ` + rel + `)`
	case "detach_pending":
		sql = `SELECT NOT EXISTS (SELECT 1 FROM pg_inherits
			WHERE inhrelid = ` + part + ` AND NOT inhdetachpending AND inhparent = ` + rel + `)`
	default:
		return false, fmt.Errorf("unknown step check %q", c.Kind)
	}
	args := []any{c.Table, c.Schema, c.Name}
	if c.Kind == "detached" || c.Kind == "detach_pending" {
		args = append(args, c.NameSchema)
	}
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return false, err
	}
	return pgx.CollectOneRow(rows, pgx.RowTo[bool])
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

// shardConnConfig renders the connection settings of one shard dial. An
// empty database or user keeps the DSN's own.
func shardConnConfig(dsn, database, user, password string) (*pgx.ConnConfig, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.ConnectTimeout == 0 {
		// A PostgreSQL that is still starting accepts the connection and
		// then says nothing. The controller's loops are single goroutines
		// carrying no deadline of their own, so an unbounded dial stops one
		// of them for the life of the process.
		cfg.ConnectTimeout = DefaultShardConnectTimeout
	}
	if database != "" {
		cfg.Database = database
	}
	if user != "" {
		cfg.User, cfg.Password = user, password
	}
	return cfg, nil
}

// DialDatabaseAs implements DatabaseDialer; an empty user keeps the DSN's
// credentials.
func (d *PgxShardDialer) DialDatabaseAs(ctx context.Context, shardSet string, shardID int32, database, user, password string) (ShardConn, error) {
	dsn, err := d.dsn(ctx, shardSet, shardID)
	if err != nil {
		return nil, err
	}
	cfg, err := shardConnConfig(dsn, database, user, password)
	if err != nil {
		return nil, fmt.Errorf("shard %s/%d: %w", shardSet, shardID, err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("shard %s/%d: %w", shardSet, shardID, err)
	}
	if user == "" {
		// The controller's own session, as opposed to one impersonating a
		// client: mark it so the placement write fence lets it through.
		// Routers never dial through here, and the planner refuses a client
		// SET of anything under the pgshard namespace, so a client cannot
		// exempt itself.
		if _, err := conn.Exec(ctx, `SET `+MaintenanceGUC+` = 'on'`); err != nil {
			_ = conn.Close(ctx)
			return nil, fmt.Errorf("shard %s/%d: %w", shardSet, shardID, err)
		}
	}
	return pgxShardConn{conn}, nil
}
