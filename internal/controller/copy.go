package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// Copy-phase stages of a reshard workflow.
const (
	StageCopying      = "copying"
	StageCatchUpDone  = "catch_up_done"
	StageCancelling   = "cancelling"
	StageCancelled    = "cancelled"
	DefaultLagBytes   = 1 << 20
	DefaultThrottleHi = 256 << 20
	DefaultThrottleLo = 64 << 20
	// DefaultPreparedWait bounds how long a copy waits for in-doubt
	// prepared transactions on a source before the workflow fails.
	DefaultPreparedWait = 10 * time.Minute
	// createSubscriptionTimeout bounds one CREATE SUBSCRIPTION: slot
	// creation waits for every running transaction on the source.
	createSubscriptionTimeout = 2 * time.Minute
)

// Copier drives reshard workflows through the copy phase: schema
// materialization on the targets, row-filter publications on the sources,
// native subscriptions on the targets, progress, throttling and cancel.
// Every step is idempotent against the catalogs of the shards, so a restart
// re-drives a workflow from wherever it stopped.
type Copier struct {
	Pool   *pgxpool.Pool
	Shards ShardDBDialer
	// Schema materializes schemas on targets; nil refuses and fails the
	// workflow with a clear message.
	Schema SchemaMaterializer
	// SourceConnInfo renders the libpq connection string a target uses to
	// subscribe to one database of a source.
	SourceConnInfo func(ctx context.Context, source ShardRef, database string) (string, error)
	// Resolver finishes in-doubt two-phase commits that block slot
	// creation; nil means wait without driving.
	Resolver *Resolver
	Logger   *slog.Logger
	// LagBytes is the apply lag under which the copy counts as caught up.
	LagBytes int64
	// ThrottleHigh and ThrottleLow are the standby-lag watermarks that pause
	// and resume the subscriptions.
	ThrottleHigh, ThrottleLow int64
	// PreparedWait bounds the wait for in-doubt prepared transactions.
	PreparedWait time.Duration
	// SlotFailover requests failover slots (PG 17+ subscription option).
	SlotFailover bool
	// Now overrides the clock in tests.
	Now func() time.Time
}

// CopyOutcome counts one pass.
type CopyOutcome struct {
	Driven    int
	Advanced  int
	Cancelled int
	Failed    int
}

// copyState is the copy phase's record under workflows.status->'copy'.
type copyState struct {
	Schema              map[string]map[string]bool `json:"schema"`
	Paused              bool                       `json:"paused"`
	BlockedBy           string                     `json:"blocked_by,omitempty"`
	BlockedSince        *time.Time                 `json:"blocked_since,omitempty"`
	ReplicaIdentityFull []string                   `json:"replica_identity_full,omitempty"`
	Progress            CopyProgress               `json:"progress"`
	Skipped             []string                   `json:"skipped,omitempty"`
}

type copyWorkflow struct {
	id     string
	state  string
	stage  string
	set    string
	gen    int64
	ranges placement.RangeSet
	ids    []int32
	copy   copyState
}

// Run drives every interval until ctx ends.
func (c *Copier) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if _, err := c.Pass(ctx); err != nil {
			c.logger().Warn("copy pass failed", "err", err)
		}
	}
}

func (c *Copier) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func (c *Copier) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *Copier) lagBytes() int64 {
	if c.LagBytes > 0 {
		return c.LagBytes
	}
	return DefaultLagBytes
}

func (c *Copier) watermarks() (int64, int64) {
	hi, lo := c.ThrottleHigh, c.ThrottleLow
	if hi <= 0 {
		hi = DefaultThrottleHi
	}
	if lo <= 0 || lo > hi {
		lo = hi / 4
	}
	return hi, lo
}

func (c *Copier) preparedWait() time.Duration {
	if c.PreparedWait > 0 {
		return c.PreparedWait
	}
	return DefaultPreparedWait
}

func (c *Copier) schema() SchemaMaterializer {
	if c.Schema != nil {
		return c.Schema
	}
	return noMaterializer{}
}

// Pass drives every reshard workflow in a copy stage once.
func (c *Copier) Pass(ctx context.Context) (CopyOutcome, error) {
	var out CopyOutcome
	wfs, err := c.listCopyWorkflows(ctx)
	if err != nil {
		return out, err
	}
	for i := range wfs {
		wf := &wfs[i]
		out.Driven++
		var err error
		if wf.stage == StageCancelling {
			err = c.cancel(ctx, wf)
			if err == nil {
				out.Cancelled++
			}
		} else {
			var advanced bool
			advanced, err = c.drive(ctx, wf)
			if advanced {
				out.Advanced++
			}
		}
		if err != nil {
			var fatal *fatalError
			if errors.As(err, &fatal) {
				out.Failed++
				c.logger().Error("reshard copy failed", "workflow", wf.id, "err", err)
				if ferr := c.fail(ctx, wf, err); ferr != nil {
					return out, ferr
				}
				continue
			}
			c.logger().Warn("reshard copy pass incomplete", "workflow", wf.id, "err", err)
			if serr := c.save(ctx, wf, "", err.Error()); serr != nil {
				return out, serr
			}
		}
	}
	return out, nil
}

// fatalError fails the workflow instead of retrying next pass.
type fatalError struct{ err error }

func (e *fatalError) Error() string { return e.err.Error() }
func (e *fatalError) Unwrap() error { return e.err }

func fatal(format string, args ...any) error { return &fatalError{fmt.Errorf(format, args...)} }

func (c *Copier) listCopyWorkflows(ctx context.Context) ([]copyWorkflow, error) {
	rows, err := c.Pool.Query(ctx, `SELECT id::text, state, coalesce(status->>'stage', ''), spec, coalesce(status->'copy', '{}'::jsonb)
		FROM pgshard.workflows
		WHERE kind = $1 AND ((state = $2 AND status->>'stage' = ANY($3)) OR status->>'stage' = $4)
		ORDER BY created_at`, KindReshard, StateRunning, []string{StageReadyForCopy, StageCopying, StageCatchUpDone}, StageCancelling)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []copyWorkflow
	for rows.Next() {
		var wf copyWorkflow
		var spec, cp []byte
		if err := rows.Scan(&wf.id, &wf.state, &wf.stage, &spec, &cp); err != nil {
			return nil, err
		}
		var s struct {
			ShardSet   string `json:"shard_set"`
			Generation int64  `json:"generation"`
			Ranges     []struct {
				ShardID int32  `json:"shard_id"`
				Lower   *int64 `json:"lower"`
				Upper   *int64 `json:"upper"`
			} `json:"ranges"`
		}
		if err := json.Unmarshal(spec, &s); err != nil {
			return nil, fmt.Errorf("workflow %s spec: %w", wf.id, err)
		}
		if err := json.Unmarshal(cp, &wf.copy); err != nil {
			return nil, fmt.Errorf("workflow %s copy state: %w", wf.id, err)
		}
		if wf.copy.Schema == nil {
			wf.copy.Schema = map[string]map[string]bool{}
		}
		wf.set, wf.gen = s.ShardSet, s.Generation
		var ranges []catalog.ShardRange
		for _, r := range s.Ranges {
			ranges = append(ranges, catalog.ShardRange{ShardSet: s.ShardSet, ShardID: r.ShardID, Lower: r.Lower, Upper: r.Upper})
			wf.ids = append(wf.ids, r.ShardID)
		}
		wf.ranges = catalog.RangeSet(ranges)
		out = append(out, wf)
	}
	return out, rows.Err()
}

func (c *Copier) save(ctx context.Context, wf *copyWorkflow, stage, message string) error {
	patch := map[string]any{"copy": wf.copy, "message": message, "progress": wf.copy.Progress}
	if stage != "" {
		patch["stage"] = stage
	}
	_, err := c.Pool.Exec(ctx, `UPDATE pgshard.workflows SET status = status || $2::jsonb, updated_at = now() WHERE id = $1::uuid`, wf.id, mustJSON(patch))
	return err
}

func (c *Copier) fail(ctx context.Context, wf *copyWorkflow, cause error) error {
	_, err := c.Pool.Exec(ctx, `UPDATE pgshard.workflows SET state = $2, error = $3, status = status || $4::jsonb, updated_at = now() WHERE id = $1::uuid`,
		wf.id, StateFailed, cause.Error(), mustJSON(map[string]any{"stage": "failed", "copy": wf.copy, "message": cause.Error()}))
	return err
}

// sources are the shards of the serving set the copy reads from.
func (c *Copier) sources(ctx context.Context) (string, []int32, error) {
	var set string
	err := c.Pool.QueryRow(ctx, `SELECT shard_set FROM pgshard.shard_sets WHERE state = $1 ORDER BY generation DESC LIMIT 1`, catalog.ShardSetServing).Scan(&set)
	if err != nil {
		return "", nil, fmt.Errorf("serving shard set: %w", err)
	}
	rows, err := c.Pool.Query(ctx, `SELECT shard_id FROM pgshard.shard_status WHERE shard_set = $1 ORDER BY shard_id`, set)
	if err != nil {
		return "", nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		return "", nil, err
	}
	if len(ids) == 0 {
		return "", nil, fmt.Errorf("serving set %s has no shards in shard_status", set)
	}
	return set, ids, nil
}

type dbPlan struct {
	name      string
	home      int32
	sharded   []catalog.Table
	reference []catalog.Table
}

func (c *Copier) databases(ctx context.Context) ([]dbPlan, error) {
	dbs, err := catalog.ListDatabases(ctx, c.Pool)
	if err != nil {
		return nil, err
	}
	var out []dbPlan
	for _, d := range dbs {
		p := dbPlan{name: d.Name, home: d.HomeShard}
		tables, err := catalog.ListTables(ctx, c.Pool, d.Name)
		if err != nil {
			return nil, err
		}
		for _, t := range tables {
			switch t.Placement {
			case "sharded":
				p.sharded = append(p.sharded, t)
			case "reference":
				p.reference = append(p.reference, t)
			}
		}
		out = append(out, p)
	}
	return out, nil
}

// drive advances one workflow through the copy phase; it reports whether
// the stage changed.
func (c *Copier) drive(ctx context.Context, wf *copyWorkflow) (bool, error) {
	srcSet, srcIDs, err := c.sources(ctx)
	if err != nil {
		return false, err
	}
	dbs, err := c.databases(ctx)
	if err != nil {
		return false, err
	}
	advanced := false
	if wf.stage == StageReadyForCopy {
		wf.stage = StageCopying
		advanced = true
		if err := c.save(ctx, wf, StageCopying, "copy started"); err != nil {
			return advanced, err
		}
	}
	if err := c.materializeSchemas(ctx, wf, srcSet, dbs); err != nil {
		return advanced, err
	}
	if err := c.ensurePublications(ctx, wf, srcSet, srcIDs, dbs); err != nil {
		return advanced, err
	}
	if err := c.ensureSubscriptions(ctx, wf, srcSet, srcIDs, dbs); err != nil {
		return advanced, err
	}
	progress, err := c.observe(ctx, wf, srcSet, srcIDs, dbs)
	if err != nil {
		return advanced, err
	}
	if err := c.throttle(ctx, wf, srcSet, srcIDs, dbs); err != nil {
		return advanced, err
	}
	wf.copy.Progress = progress
	stage := ""
	msg := fmt.Sprintf("copying: %d/%d tables ready, lag %d bytes", progress.TablesReady, progress.TablesTotal, progress.LagBytes)
	if wf.copy.Paused {
		msg += " (paused: source standby lag over watermark)"
	}
	if wf.stage != StageCatchUpDone && progress.CaughtUp(c.lagBytes()) {
		stage = StageCatchUpDone
		advanced = true
		msg = fmt.Sprintf("caught up: %d tables ready, lag %d bytes", progress.TablesReady, progress.LagBytes)
	}
	return advanced, c.save(ctx, wf, stage, msg)
}

func targetKey(id int32) string { return fmt.Sprint(id) }

// materializeSchemas creates every user database on every target and copies
// its schema from the database's home shard. A database whose flag is not
// set yet is dropped and recreated so a half-applied dump never survives.
func (c *Copier) materializeSchemas(ctx context.Context, wf *copyWorkflow, srcSet string, dbs []dbPlan) error {
	for _, db := range dbs {
		for _, t := range wf.ids {
			if wf.copy.Schema[db.name][targetKey(t)] {
				continue
			}
			target := ShardRef{Set: wf.set, ID: t}
			conn, err := c.Shards.DialDatabase(ctx, wf.set, t, "")
			if err != nil {
				return err
			}
			err = c.resetDatabase(ctx, conn, db.name)
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("target %s/%d: %w", wf.set, t, err)
			}
			src, err := c.SourceConnInfo(ctx, ShardRef{Set: srcSet, ID: db.home}, db.name)
			if err != nil {
				return err
			}
			if err := c.schema().MaterializeSchema(ctx, target, db.name, src); err != nil {
				if errors.Is(err, errNoMaterializer) {
					return fatal("schema of %s on %s/%d: %w", db.name, wf.set, t, err)
				}
				return fmt.Errorf("schema of %s on %s/%d: %w", db.name, wf.set, t, err)
			}
			if wf.copy.Schema[db.name] == nil {
				wf.copy.Schema[db.name] = map[string]bool{}
			}
			wf.copy.Schema[db.name][targetKey(t)] = true
			if err := c.save(ctx, wf, "", fmt.Sprintf("schema of %s materialized on %s/%d", db.name, wf.set, t)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Copier) resetDatabase(ctx context.Context, conn ShardConn, database string) error {
	rows, err := conn.Query(ctx, `SELECT 1 FROM pg_database WHERE datname = $1`, database)
	if err != nil {
		return err
	}
	exists, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		return err
	}
	if len(exists) > 0 {
		if _, err := conn.Exec(ctx, "DROP DATABASE "+QuoteIdent(database)+" WITH (FORCE)"); err != nil {
			return err
		}
	}
	_, err = conn.Exec(ctx, "CREATE DATABASE "+QuoteIdent(database))
	return err
}

// sourceTable is a table as found on a source shard.
type sourceTable struct {
	schema, name string
	keyType      string
	replIdentOK  bool
}

// ensurePublications creates the publications of every database on every
// source: one per target with the row filter of the target's range, plus
// the reference and unsharded publications on the database's home shard.
func (c *Copier) ensurePublications(ctx context.Context, wf *copyWorkflow, srcSet string, srcIDs []int32, dbs []dbPlan) error {
	for _, db := range dbs {
		for _, s := range srcIDs {
			if err := c.publishOn(ctx, wf, srcSet, s, db); err != nil {
				return fmt.Errorf("publications of %s on %s/%d: %w", db.name, srcSet, s, err)
			}
		}
	}
	return nil
}

func (c *Copier) publishOn(ctx context.Context, wf *copyWorkflow, srcSet string, s int32, db dbPlan) error {
	want := map[string][]PublishedTable{}
	conn, err := c.Shards.DialDatabase(ctx, srcSet, s, db.name)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	existing, err := existingPublications(ctx, conn, wf.gen)
	if err != nil {
		return err
	}
	shardedNames := map[string]bool{}
	for _, t := range db.sharded {
		shardedNames[t.SchemaName+"."+t.TableName] = true
	}
	refNames := map[string]bool{}
	for _, t := range db.reference {
		refNames[t.SchemaName+"."+t.TableName] = true
	}
	var shardedPub []struct {
		table catalog.Table
		hash  string
	}
	for _, t := range db.sharded {
		st, err := describeTable(ctx, conn, t.SchemaName, t.TableName, *t.ShardKey)
		if errors.Is(err, pgx.ErrNoRows) {
			wf.copy.Skipped = appendUnique(wf.copy.Skipped, fmt.Sprintf("%s.%s.%s: not present on %s/%d", db.name, t.SchemaName, t.TableName, srcSet, s))
			continue
		}
		if err != nil {
			return err
		}
		hash, err := KeyHashExpr(*t.ShardKey, st.keyType)
		if err != nil {
			return fatal("%s.%s.%s: %w", db.name, t.SchemaName, t.TableName, err)
		}
		if !st.replIdentOK {
			if err := c.replicaIdentityFull(ctx, wf, conn, db.name, t.SchemaName, t.TableName); err != nil {
				return err
			}
		}
		shardedPub = append(shardedPub, struct {
			table catalog.Table
			hash  string
		}{t, hash})
	}
	for i, t := range wf.ids {
		var tables []PublishedTable
		for _, sp := range shardedPub {
			tables = append(tables, PublishedTable{Schema: sp.table.SchemaName, Name: sp.table.TableName, Filter: RangeFilter(sp.hash, wf.ranges[i])})
		}
		want[PublicationName(wf.gen, t)] = tables
	}
	if s == db.home {
		others, err := listTables(ctx, conn)
		if err != nil {
			return err
		}
		var ref, home []PublishedTable
		for _, o := range others {
			key := o.schema + "." + o.name
			if shardedNames[key] {
				continue
			}
			if !o.replIdentOK {
				if err := c.replicaIdentityFull(ctx, wf, conn, db.name, o.schema, o.name); err != nil {
					return err
				}
			}
			pt := PublishedTable{Schema: o.schema, Name: o.name}
			if refNames[key] {
				ref = append(ref, pt)
			} else {
				home = append(home, pt)
			}
		}
		want[ReferencePublicationName(wf.gen)] = ref
		want[HomePublicationName(wf.gen)] = home
	}
	for _, name := range sortedKeys(want) {
		if existing[name] {
			continue
		}
		if _, err := conn.Exec(ctx, CreatePublicationSQL(name, want[name])); err != nil {
			return err
		}
	}
	return nil
}

func (c *Copier) replicaIdentityFull(ctx context.Context, wf *copyWorkflow, conn ShardConn, db, schema, name string) error {
	if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY FULL", QuoteIdent(schema), QuoteIdent(name))); err != nil {
		return err
	}
	wf.copy.ReplicaIdentityFull = appendUnique(wf.copy.ReplicaIdentityFull, db+"."+schema+"."+name)
	return nil
}

func appendUnique(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

func existingPublications(ctx context.Context, conn ShardConn, gen int64) (map[string]bool, error) {
	rows, err := conn.Query(ctx, `SELECT pubname FROM pg_publication WHERE pubname LIKE $1`, fmt.Sprintf("pgshard\\_reshard\\_g%d\\_%%", gen))
	if err != nil {
		return nil, err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, n := range names {
		out[n] = true
	}
	return out, nil
}

// replicaIdentitySQL reports, per table, whether UPDATE and DELETE can be
// published with a row filter on column $key: the identity is FULL, or the
// identity index (primary key by default) covers the key.
const replicaIdentitySQL = `
	c.relreplident = 'f' OR EXISTS (
		SELECT 1 FROM pg_index i
		WHERE i.indrelid = c.oid AND ((i.indisprimary AND c.relreplident = 'd') OR i.indisreplident)
		  AND ($KEY = '' OR EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum = ANY (i.indkey) AND a.attname = $KEY)))`

func describeTable(ctx context.Context, conn ShardConn, schema, name, key string) (sourceTable, error) {
	sql := `SELECT format_type(a.atttypid, a.atttypmod), ` + strings.ReplaceAll(replicaIdentitySQL, "$KEY", "$3") + `
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = $3 AND NOT a.attisdropped
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'`
	rows, err := conn.Query(ctx, sql, schema, name, key)
	if err != nil {
		return sourceTable{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if rows.Err() != nil {
			return sourceTable{}, rows.Err()
		}
		return sourceTable{}, pgx.ErrNoRows
	}
	st := sourceTable{schema: schema, name: name}
	if err := rows.Scan(&st.keyType, &st.replIdentOK); err != nil {
		return sourceTable{}, err
	}
	return st, nil
}

// listTables lists every ordinary table outside the system schemas.
func listTables(ctx context.Context, conn ShardConn) ([]sourceTable, error) {
	sql := `SELECT n.nspname, c.relname, ` + strings.ReplaceAll(replicaIdentitySQL, "$KEY", "''") + `
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgshard') AND n.nspname NOT LIKE 'pg\_%'
		ORDER BY 1, 2`
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sourceTable
	for rows.Next() {
		var t sourceTable
		if err := rows.Scan(&t.schema, &t.name, &t.replIdentOK); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// publicationsFor lists the publications target t subscribes to on source s
// for database db.
func (c *Copier) publicationsFor(wf *copyWorkflow, db dbPlan, t, s int32) []string {
	pubs := []string{PublicationName(wf.gen, t)}
	if s == db.home {
		pubs = append(pubs, ReferencePublicationName(wf.gen))
		if t == wf.ids[HomeTarget(wf.ranges)] {
			pubs = append(pubs, HomePublicationName(wf.gen))
		}
	}
	return pubs
}

// ensureSubscriptions creates one subscription per (target, source) pair in
// every database. Slot creation on a source waits for every running
// transaction, so a source holding prepared transactions is resolved first
// and, if any remain, the pair is retried next pass.
func (c *Copier) ensureSubscriptions(ctx context.Context, wf *copyWorkflow, srcSet string, srcIDs []int32, dbs []dbPlan) error {
	for _, db := range dbs {
		for _, t := range wf.ids {
			conn, err := c.Shards.DialDatabase(ctx, wf.set, t, db.name)
			if err != nil {
				return err
			}
			err = c.subscribeOn(ctx, wf, conn, srcSet, srcIDs, db, t)
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("subscriptions of %s on %s/%d: %w", db.name, wf.set, t, err)
			}
		}
	}
	return nil
}

func (c *Copier) subscribeOn(ctx context.Context, wf *copyWorkflow, conn ShardConn, srcSet string, srcIDs []int32, db dbPlan, t int32) error {
	rows, err := conn.Query(ctx, `SELECT subname FROM pg_subscription WHERE subname LIKE $1`, fmt.Sprintf("pgshard\\_reshard\\_g%d\\_t%d\\_%%", wf.gen, t))
	if err != nil {
		return err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for _, n := range names {
		existing[n] = true
	}
	for _, s := range srcIDs {
		name := SubscriptionName(wf.gen, t, s)
		if existing[name] {
			continue
		}
		blocked, err := c.preparedOn(ctx, srcSet, s)
		if err != nil {
			return err
		}
		if len(blocked) > 0 {
			return c.blockedBy(wf, srcSet, s, blocked)
		}
		wf.copy.BlockedBy, wf.copy.BlockedSince = "", nil
		conninfo, err := c.SourceConnInfo(ctx, ShardRef{Set: srcSet, ID: s}, db.name)
		if err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, createSubscriptionTimeout)
		_, err = conn.Exec(cctx, CreateSubscriptionSQL(name, conninfo, c.publicationsFor(wf, db, t, s), SubscriptionOptions{Slot: name, Failover: c.SlotFailover}))
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// preparedOn returns the prepared transactions on a source after one
// resolver pass.
func (c *Copier) preparedOn(ctx context.Context, srcSet string, s int32) ([]string, error) {
	list := func() ([]string, error) {
		conn, err := c.Shards.Dial(ctx, srcSet, s)
		if err != nil {
			return nil, err
		}
		defer func() { _ = conn.Close(ctx) }()
		rows, err := conn.Query(ctx, `SELECT gid FROM pg_prepared_xacts ORDER BY prepared`)
		if err != nil {
			return nil, err
		}
		return pgx.CollectRows(rows, pgx.RowTo[string])
	}
	gids, err := list()
	if err != nil || len(gids) == 0 || c.Resolver == nil {
		return gids, err
	}
	if _, err := c.Resolver.Resolve(ctx, srcSet); err != nil {
		c.logger().Warn("resolver pass before slot creation failed", "err", err)
	}
	return list()
}

func (c *Copier) blockedBy(wf *copyWorkflow, srcSet string, s int32, gids []string) error {
	now := c.now()
	if wf.copy.BlockedSince == nil {
		wf.copy.BlockedSince = &now
	}
	wf.copy.BlockedBy = fmt.Sprintf("%s/%d: %s", srcSet, s, strings.Join(gids, ","))
	if now.Sub(*wf.copy.BlockedSince) > c.preparedWait() {
		return fatal("slot creation on %s/%d blocked by prepared transactions %v for longer than %s", srcSet, s, gids, c.preparedWait())
	}
	return fmt.Errorf("slot creation on %s/%d waits for prepared transactions %v", srcSet, s, gids)
}

// observe reads pg_subscription_rel and pg_stat_subscription on every
// target and the WAL position of every source.
func (c *Copier) observe(ctx context.Context, wf *copyWorkflow, srcSet string, srcIDs []int32, dbs []dbPlan) (CopyProgress, error) {
	sourceLSN := map[int32]int64{}
	for _, s := range srcIDs {
		conn, err := c.Shards.Dial(ctx, srcSet, s)
		if err != nil {
			return CopyProgress{}, err
		}
		rows, err := conn.Query(ctx, `SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint`)
		if err == nil {
			var lsn int64
			lsn, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[int64])
			sourceLSN[s] = lsn
		}
		_ = conn.Close(ctx)
		if err != nil {
			return CopyProgress{}, err
		}
	}
	var reports []SubscriptionProgress
	for _, db := range dbs {
		for _, t := range wf.ids {
			conn, err := c.Shards.DialDatabase(ctx, wf.set, t, db.name)
			if err != nil {
				return CopyProgress{}, err
			}
			rs, err := subscriptionReports(ctx, conn, wf.gen, t, srcIDs, sourceLSN)
			_ = conn.Close(ctx)
			if err != nil {
				return CopyProgress{}, fmt.Errorf("progress of %s on %s/%d: %w", db.name, wf.set, t, err)
			}
			reports = append(reports, rs...)
		}
	}
	return Aggregate(reports), nil
}

func subscriptionReports(ctx context.Context, conn ShardConn, gen int64, t int32, srcIDs []int32, sourceLSN map[int32]int64) ([]SubscriptionProgress, error) {
	rows, err := conn.Query(ctx, `
		SELECT s.subname, s.subenabled, coalesce(r.srsubstate, ''), count(r.srrelid),
		       (SELECT (st.latest_end_lsn - '0/0'::pg_lsn)::bigint FROM pg_stat_subscription st WHERE st.subid = s.oid AND st.relid IS NULL AND st.latest_end_lsn IS NOT NULL LIMIT 1)
		FROM pg_subscription s LEFT JOIN pg_subscription_rel r ON r.srsubid = s.oid
		WHERE s.subname LIKE $1
		GROUP BY s.oid, s.subname, s.subenabled, r.srsubstate`, fmt.Sprintf("pgshard\\_reshard\\_g%d\\_t%d\\_%%", gen, t))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	bySub := map[string]*SubscriptionProgress{}
	for rows.Next() {
		var name, state string
		var enabled bool
		var n int
		var applied *int64
		if err := rows.Scan(&name, &enabled, &state, &n, &applied); err != nil {
			return nil, err
		}
		p := bySub[name]
		if p == nil {
			p = &SubscriptionProgress{Rels: map[RelState]int{}, Enabled: enabled, LagBytes: LagUnknown}
			bySub[name] = p
			if applied != nil && enabled {
				if lsn, ok := sourceLSN[sourceOf(name, gen, t, srcIDs)]; ok {
					p.LagBytes = max(lsn-*applied, 0)
				}
			}
		}
		if state != "" {
			p.Rels[RelState(state[0])] += n
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []SubscriptionProgress
	for _, name := range sortedKeys(bySub) {
		out = append(out, *bySub[name])
	}
	return out, nil
}

func sourceOf(sub string, gen int64, t int32, srcIDs []int32) int32 {
	for _, s := range srcIDs {
		if SubscriptionName(gen, t, s) == sub {
			return s
		}
	}
	return -1
}

// throttle pauses or resumes every subscription of the workflow by the
// largest physical standby lag over the sources.
func (c *Copier) throttle(ctx context.Context, wf *copyWorkflow, srcSet string, srcIDs []int32, dbs []dbPlan) error {
	var lag int64
	for _, s := range srcIDs {
		conn, err := c.Shards.Dial(ctx, srcSet, s)
		if err != nil {
			return err
		}
		rows, err := conn.Query(ctx, `SELECT coalesce(max(pg_current_wal_lsn() - replay_lsn), 0)::bigint FROM pg_stat_replication
			WHERE application_name NOT LIKE 'pgshard\_reshard\_%'`)
		if err == nil {
			var l int64
			l, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[int64])
			lag = max(lag, l)
		}
		_ = conn.Close(ctx)
		if err != nil {
			return err
		}
	}
	hi, lo := c.watermarks()
	paused := Throttle(wf.copy.Paused, lag, hi, lo)
	if paused == wf.copy.Paused {
		return nil
	}
	verb := "ENABLE"
	if paused {
		verb = "DISABLE"
	}
	if err := c.forEachSubscription(ctx, wf, srcIDs, dbs, func(conn ShardConn, name string) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s %s", QuoteIdent(name), verb))
		return err
	}); err != nil {
		return err
	}
	wf.copy.Paused = paused
	c.logger().Info("reshard copy throttle", "workflow", wf.id, "paused", paused, "standby_lag_bytes", lag)
	return nil
}

func (c *Copier) forEachSubscription(ctx context.Context, wf *copyWorkflow, srcIDs []int32, dbs []dbPlan, fn func(conn ShardConn, name string) error) error {
	for _, db := range dbs {
		for _, t := range wf.ids {
			conn, err := c.Shards.DialDatabase(ctx, wf.set, t, db.name)
			if err != nil {
				return err
			}
			for _, s := range srcIDs {
				if err == nil {
					err = fn(conn, SubscriptionName(wf.gen, t, s))
				}
			}
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// cancel drops the subscriptions on the targets that are still reachable,
// then the slots and publications on the sources, and marks the workflow
// cancelled. Targets the operator already deleted are skipped.
func (c *Copier) cancel(ctx context.Context, wf *copyWorkflow) error {
	srcSet, srcIDs, err := c.sources(ctx)
	if err != nil {
		return err
	}
	dbs, err := c.databases(ctx)
	if err != nil {
		return err
	}
	for _, db := range dbs {
		for _, t := range wf.ids {
			conn, err := c.Shards.DialDatabase(ctx, wf.set, t, db.name)
			if err != nil {
				c.logger().Info("reshard cancel: target unreachable, skipping subscription cleanup", "workflow", wf.id, "target", t, "err", err)
				continue
			}
			err = dropSubscriptions(ctx, conn, wf.gen, t)
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	for _, s := range srcIDs {
		conn, err := c.Shards.Dial(ctx, srcSet, s)
		if err != nil {
			return err
		}
		err = dropSlots(ctx, conn, wf.gen)
		_ = conn.Close(ctx)
		if err != nil {
			return err
		}
		for _, db := range dbs {
			conn, err := c.Shards.DialDatabase(ctx, srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = dropPublications(ctx, conn, wf.gen)
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	_, err = c.Pool.Exec(ctx, `UPDATE pgshard.workflows SET state = $2, status = status || $3::jsonb, updated_at = now() WHERE id = $1::uuid`,
		wf.id, StateCancelled, mustJSON(map[string]any{"stage": StageCancelled, "message": "copy cancelled: subscriptions, slots and publications dropped"}))
	return err
}

func dropSubscriptions(ctx context.Context, conn ShardConn, gen int64, t int32) error {
	rows, err := conn.Query(ctx, `SELECT subname FROM pg_subscription WHERE subname LIKE $1`, fmt.Sprintf("pgshard\\_reshard\\_g%d\\_t%d\\_%%", gen, t))
	if err != nil {
		return err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, n := range names {
		for _, stmt := range []string{"ALTER SUBSCRIPTION %s DISABLE", "ALTER SUBSCRIPTION %s SET (slot_name = NONE)", "DROP SUBSCRIPTION %s"} {
			if _, err := conn.Exec(ctx, fmt.Sprintf(stmt, QuoteIdent(n))); err != nil {
				return err
			}
		}
	}
	return nil
}

func dropSlots(ctx context.Context, conn ShardConn, gen int64) error {
	rows, err := conn.Query(ctx, `SELECT slot_name, active_pid FROM pg_replication_slots WHERE slot_name LIKE $1`, fmt.Sprintf("pgshard\\_reshard\\_g%d\\_%%", gen))
	if err != nil {
		return err
	}
	type slot struct {
		Name string
		PID  *int32
	}
	slots, err := pgx.CollectRows(rows, pgx.RowToStructByPos[slot])
	if err != nil {
		return err
	}
	for _, s := range slots {
		if s.PID != nil {
			if _, err := conn.Exec(ctx, `SELECT pg_terminate_backend($1)`, *s.PID); err != nil {
				return err
			}
		}
		var err error
		for attempt := 0; attempt < 20; attempt++ {
			if _, err = conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, s.Name); err == nil || !strings.Contains(err.Error(), "is active") {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func dropPublications(ctx context.Context, conn ShardConn, gen int64) error {
	existing, err := existingPublications(ctx, conn, gen)
	if err != nil {
		return err
	}
	for _, n := range sortedKeys(existing) {
		if _, err := conn.Exec(ctx, "DROP PUBLICATION "+QuoteIdent(n)); err != nil {
			return err
		}
	}
	return nil
}
