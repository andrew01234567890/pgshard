package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
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
	// OwnerLease is how long this replica's claim on a workflow stands
	// without being refreshed; zero means DefaultOwnerLease. Replica
	// identifies this process in a claim; zero means a token generated
	// once per process.
	OwnerLease time.Duration
	Replica    string

	// progress is the last copy state logged per workflow, so a pass that
	// found no change stays quiet.
	progress map[string]string
	// SlotFailover requests failover slots (PG 17+ subscription option).
	SlotFailover bool
	// CutoverTimeout bounds the fence of one switch attempt; CutoverAttempts
	// how many undone attempts fail the workflow.
	CutoverTimeout  time.Duration
	CutoverAttempts int
	// LockTimeout bounds each SHARE lock of the sweep.
	LockTimeout time.Duration
	// Now overrides the clock in tests.
	Now func() time.Time

	cutoverStore cutoverStore
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
	// Targets is the same progress per target shard, keyed by its id. The
	// aggregate says the copy is behind; this says which target is, which
	// is what an operator does something about -- and it is what the admin
	// reshard panel reads.
	Targets map[string]CopyProgress `json:"targets,omitempty"`
	Skipped []string                `json:"skipped,omitempty"`
}

type copyWorkflow struct {
	id      string
	kind    string
	state   string
	stage   string
	set     string
	gen     int64
	ranges  placement.RangeSet
	ids     []int32
	copy    copyState
	spec    cutoverSpec
	cutover cutoverState
	// owner is this pass's claim on the workflow and fence the state it
	// started from; every write it makes requires both to still hold, so a
	// workflow taken over stops the old pass instead of racing the new one.
	owner string
	fence string
}

// sourceSet is the shard set the workflow copies from: recorded in the
// spec at creation, or the serving set of the first cutover pass for
// older workflows.
func (wf *copyWorkflow) sourceSet() string {
	if wf.spec.SourceSet != "" {
		return wf.spec.SourceSet
	}
	return wf.cutover.SourceSet
}

var copyStages = []string{StageReadyForCopy, StageCopying, StageCatchUpDone}
var cutoverStages = []string{StageCatchUpDone, StageAwaitingSwitch, StageSwitching, StageSwitched, StageRollingBack, StageCompleting}

// Run drives a copy pass on every tick while this replica is the leader.
// Only the leader may run one: a pass creates and drops publications and
// subscriptions and moves data, so two replicas doing it at once produce
// half-built replication and failed cutovers.
func (c *Copier) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	runLoop(ctx, interval, leader, c.logger, "reshard copy", func(ctx context.Context) {
		if _, err := c.Pass(ctx); err != nil {
			c.logger().Warn("copy pass failed", "err", err)
		}
	})
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
		var err error
		var held bool
		if wf.owner, held, err = claimWorkflow(ctx, c.Pool, c.Replica, wf.id, c.OwnerLease); err != nil {
			return out, err
		} else if !held {
			continue
		}
		wf.fence = wf.state
		out.Driven++
		if wf.stage == StageCancelling {
			err = c.cancel(ctx, wf)
			if err == nil {
				out.Cancelled++
			}
		} else {
			var advanced bool
			if slices.Contains(copyStages, wf.stage) {
				advanced, err = c.drive(ctx, wf)
			}
			if err == nil && slices.Contains(cutoverStages, wf.stage) {
				var more bool
				more, err = c.driveCutover(ctx, wf)
				advanced = advanced || more
			}
			if advanced {
				out.Advanced++
			}
		}
		if err != nil {
			if errors.Is(err, errNotOwner) {
				c.logger().Info("reshard copy pass handed over", "workflow", wf.id)
				continue
			}
			var fatal *fatalError
			if errors.As(err, &fatal) {
				out.Failed++
				c.logger().Error("reshard copy failed", "workflow", wf.id, "err", err)
				if ferr := c.fail(ctx, wf, err); ferr != nil {
					if errors.Is(ferr, errNotOwner) {
						continue
					}
					return out, ferr
				}
				continue
			}
			if wf.cutover.stalled(c.now()) {
				c.logger().Error("cutover step is not advancing", "workflow", wf.id, "stage", wf.stage,
					"step", wf.cutover.Step, "since", wf.cutover.StepSince, "retries", wf.cutover.StepRetries,
					"past_journal", wf.cutover.JournalID != "", "err", err)
			} else {
				c.logger().Warn("reshard copy pass incomplete", "workflow", wf.id, "err", err)
			}
			if serr := c.save(ctx, wf, "", err.Error()); serr != nil && !errors.Is(serr, errNotOwner) {
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
	rows, err := c.Pool.Query(ctx, `SELECT id::text, kind, state, coalesce(status->>'stage', ''), spec, coalesce(status->'copy', '{}'::jsonb), coalesce(status->'cutover', '{}'::jsonb)
		FROM pgshard.workflows
		WHERE kind = ANY($1) AND ((state = $2 AND status->>'stage' = ANY($3)) OR status->>'stage' = $4)
		ORDER BY created_at`, copyKinds, StateRunning, slices.Concat(copyStages, cutoverStages), StageCancelling)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []copyWorkflow
	for rows.Next() {
		var wf copyWorkflow
		var spec, cp, co []byte
		if err := rows.Scan(&wf.id, &wf.kind, &wf.state, &wf.stage, &spec, &cp, &co); err != nil {
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
		if err := json.Unmarshal(co, &wf.cutover); err != nil {
			return nil, fmt.Errorf("workflow %s cutover state: %w", wf.id, err)
		}
		if err := json.Unmarshal(spec, &wf.spec); err != nil {
			return nil, fmt.Errorf("workflow %s cutover spec: %w", wf.id, err)
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
	// progress and targets are lifted to the top of status because that is
	// where the admin panel and the operator read them; copy keeps the
	// whole phase record.
	patch := map[string]any{"copy": wf.copy, "message": message, "progress": wf.copy.Progress}
	if len(wf.copy.Targets) > 0 {
		patch["targets"] = wf.copy.Targets
	}
	if stage != "" {
		patch["stage"] = stage
	}
	return ownedExec(ctx, c.Pool, wf.owner,
		`UPDATE pgshard.workflows SET status = status || $2::jsonb, updated_at = now()
		 WHERE id = $1::uuid AND ($3::text IS NULL OR (owner = $3 AND state = $4))`,
		wf.id, mustJSON(patch), nullIfEmpty(wf.owner), wf.fence)
}

func (c *Copier) fail(ctx context.Context, wf *copyWorkflow, cause error) error {
	if err := ownedExec(ctx, c.Pool, wf.owner,
		`UPDATE pgshard.workflows SET state = $2, error = $3, status = status || $4::jsonb, updated_at = now()
		 WHERE id = $1::uuid AND ($5::text IS NULL OR (owner = $5 AND state = $6))`,
		wf.id, StateFailed, cause.Error(), mustJSON(map[string]any{"stage": "failed", "copy": wf.copy, "message": cause.Error()}),
		nullIfEmpty(wf.owner), wf.fence); err != nil {
		return err
	}
	wf.fence = StateFailed
	return nil
}

// sources are the shards of the serving set the copy reads from.
// pinSource resolves the set a workflow copies from once and records it before
// the workflow takes its first side effect, so later passes cannot drift onto a
// different set. Resolving it live on every pass is not safe: a workflow that
// has already built publications and subscriptions against one set would
// rediscover whichever set is serving now, leaving its replication attached to
// the old one, and would let the old one be reshaped underneath it.
func (c *Copier) pinSource(ctx context.Context, wf *copyWorkflow) (string, []int32, error) {
	set := wf.sourceSet()
	if set == "" {
		candidate, _, err := c.sources(ctx)
		if err != nil {
			return "", nil, err
		}
		// Copier passes are not leader-gated, so two of them can resolve
		// different serving sets around a flip. Claim the source only if none
		// is recorded, and take whatever value is stored afterwards, so every
		// pass agrees on the one that won.
		err = c.Pool.QueryRow(ctx, `UPDATE pgshard.workflows
			   SET status = status || jsonb_build_object('cutover', coalesce(status->'cutover', '{}'::jsonb) || jsonb_build_object('source_set', $2::text)),
			       updated_at = now()
			 WHERE id = $1::uuid AND coalesce(status->'cutover'->>'source_set', '') = ''
			RETURNING status->'cutover'->>'source_set'`, wf.id, candidate).Scan(&set)
		if errors.Is(err, pgx.ErrNoRows) {
			// Someone else claimed it, or the workflow is gone. Read it back in
			// a separate statement: a query in the same statement as the update
			// would share its snapshot and could not see the winner's commit.
			err = c.Pool.QueryRow(ctx, `SELECT coalesce(status->'cutover'->>'source_set', '')
				FROM pgshard.workflows WHERE id = $1::uuid`, wf.id).Scan(&set)
			if errors.Is(err, pgx.ErrNoRows) {
				return "", nil, fmt.Errorf("record copy source: workflow %s is gone", wf.id)
			}
		}
		if err != nil {
			return "", nil, fmt.Errorf("record copy source: %w", err)
		}
		if set == "" {
			return "", nil, fmt.Errorf("record copy source: workflow %s recorded no source", wf.id)
		}
		wf.cutover.SourceSet = set
	}
	ids, err := c.sourceIDs(ctx, set)
	if err != nil {
		return "", nil, err
	}
	return set, ids, nil
}

// sourceIDs is the shard IDs of a set, taken from the ranges rather than from
// shard_status: the ranges are what a workflow owns and cannot be reshaped
// underneath it, while a status row can be missing for a shard that still
// exists. Acting on a subset would create replication for some shards only,
// and restoring the row later would not repair it.
func (c *Copier) sourceIDs(ctx context.Context, set string) ([]int32, error) {
	ranges, err := catalog.ListShardRanges(ctx, c.Pool, set)
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("source shard set %s has no ranges", set)
	}
	ids := make([]int32, 0, len(ranges))
	for _, r := range ranges {
		ids = append(ids, r.ShardID)
	}
	rows, err := c.Pool.Query(ctx, `SELECT shard_id FROM pgshard.shard_status WHERE shard_set = $1 ORDER BY shard_id`, set)
	if err != nil {
		return nil, err
	}
	status, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		return nil, err
	}
	if !slices.Equal(ids, status) {
		return nil, fmt.Errorf("source shard set %s has shards %v but status rows %v", set, ids, status)
	}
	return ids, nil
}

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
	// One read for every database's tables rather than one per database:
	// this runs on every pass of every active workflow, whether or not the
	// declarations changed.
	byDatabase, err := catalog.ListTablesByDatabase(ctx, c.Pool)
	if err != nil {
		return nil, err
	}
	var out []dbPlan
	for _, d := range dbs {
		p := dbPlan{name: d.Name, home: d.HomeShard}
		for _, t := range byDatabase[d.Name] {
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
	if err := holdClaim(ctx, c.Pool, wf.id, wf.owner); err != nil {
		return false, err
	}
	srcSet, srcIDs, err := c.pinSource(ctx, wf)
	if err != nil {
		return false, err
	}
	dbs, err := c.databases(ctx)
	if err != nil {
		return false, err
	}
	advanced := false
	if wf.stage == StageReadyForCopy {
		var placements int
		if err := c.Pool.QueryRow(ctx, `SELECT count(*) FROM pgshard.workflows WHERE kind = $1 AND state = ANY($2)`, KindTablePlacement, activeStates).Scan(&placements); err != nil {
			return false, err
		}
		if placements > 0 {
			return false, fmt.Errorf("waiting for %d active table placement workflow(s)", placements)
		}
		if wf.kind == KindUpgrade {
			if err := c.upgradePreconditions(ctx, wf, srcSet, srcIDs, dbs); err != nil {
				return false, err
			}
		}
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
	progress, byTarget, err := c.observe(ctx, wf, srcSet, srcIDs, dbs)
	if err != nil {
		return advanced, err
	}
	if err := c.throttle(ctx, wf, srcSet, srcIDs, dbs); err != nil {
		return advanced, err
	}
	wf.copy.Progress, wf.copy.Targets = progress, byTarget
	stage := ""
	msg := "copying: " + progress.Describe()
	if wf.copy.Paused {
		msg += " (paused: source standby lag over watermark)"
	}
	if wf.stage != StageCatchUpDone && progress.CaughtUp(c.lagBytes()) {
		stage = StageCatchUpDone
		advanced = true
		msg = fmt.Sprintf("caught up: %d tables ready, lag %d bytes", progress.TablesReady, progress.LagBytes)
	}
	// A copy that stops short of caught up otherwise says so only in the
	// catalog: the pass succeeds, logs nothing, and the workflow sits at
	// the same stage with no way to tell which of the conditions is unmet
	// without reading the row.
	if c.progress == nil {
		c.progress = map[string]string{}
	}
	if c.progress[wf.id] != msg {
		c.progress[wf.id] = msg
		c.logger().Info("reshard copy progress", "workflow", wf.id, "state", msg)
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
	partitioned  bool
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
		table       catalog.Table
		hash        string
		partitioned bool
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
			table       catalog.Table
			hash        string
			partitioned bool
		}{t, hash, st.partitioned})
	}
	for i, t := range wf.ids {
		var tables []PublishedTable
		for _, sp := range shardedPub {
			tables = append(tables, PublishedTable{Schema: sp.table.SchemaName, Name: sp.table.TableName, Filter: RangeFilter(sp.hash, wf.ranges[i]), Partitioned: sp.partitioned})
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
			pt := PublishedTable{Schema: o.schema, Name: o.name, Partitioned: o.partitioned}
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
	if err := setReplicaIdentityFull(ctx, conn, schema, name); err != nil {
		return err
	}
	wf.copy.ReplicaIdentityFull = appendUnique(wf.copy.ReplicaIdentityFull, db+"."+schema+"."+name)
	return nil
}

// setReplicaIdentityFull sets REPLICA IDENTITY FULL on a table and, when it is
// partitioned, on every leaf partition too: ALTER TABLE ... REPLICA IDENTITY
// never recurses, and a via-root filtered publication validates and encodes
// changes using each leaf's identity, so leaving leaves at the default breaks
// UPDATE/DELETE. pg_partition_tree returns the table itself for a plain table,
// so this also covers the non-partitioned case.
func setReplicaIdentityFull(ctx context.Context, conn ShardConn, schema, name string) error {
	if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY FULL", QuoteIdent(schema), QuoteIdent(name))); err != nil {
		return err
	}
	// pg_partition_tree returns no rows for a plain table and the full tree
	// (rooted at $1) for a partitioned one; the root is already handled above,
	// so ALTER only the leaves it reports.
	qual := QuoteIdent(schema) + "." + QuoteIdent(name)
	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_partition_tree($1::regclass) t
		JOIN pg_class c ON c.oid = t.relid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE t.isleaf`, qual)
	if err != nil {
		return err
	}
	type rel struct{ schema, name string }
	leaves, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (rel, error) {
		var x rel
		return x, r.Scan(&x.schema, &x.name)
	})
	if err != nil {
		return err
	}
	for _, r := range leaves {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY FULL", QuoteIdent(r.schema), QuoteIdent(r.name))); err != nil {
			return err
		}
	}
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
	sql := `SELECT format_type(a.atttypid, a.atttypmod), c.relkind = 'p', ` + strings.ReplaceAll(replicaIdentitySQL, "$KEY", "$3") + `
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = $3 AND NOT a.attisdropped
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')`
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
	if err := rows.Scan(&st.keyType, &st.partitioned, &st.replIdentOK); err != nil {
		return sourceTable{}, err
	}
	return st, nil
}

// listTables lists every ordinary table outside the system schemas.
func listTables(ctx context.Context, conn ShardConn) ([]sourceTable, error) {
	sql := `SELECT n.nspname, c.relname, c.relkind = 'p', ` + strings.ReplaceAll(replicaIdentitySQL, "$KEY", "''") + `
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r', 'p') AND c.relispartition = false AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgshard') AND n.nspname NOT LIKE 'pg\_%'
		ORDER BY 1, 2`
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sourceTable
	for rows.Next() {
		var t sourceTable
		if err := rows.Scan(&t.schema, &t.name, &t.partitioned, &t.replIdentOK); err != nil {
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
// observe reads the copy's progress: the aggregate, and the same per target
// shard. Flattening to one maximum told an operator the copy was behind
// without saying which target was behind, which is the thing they act on.
func (c *Copier) observe(ctx context.Context, wf *copyWorkflow, srcSet string, srcIDs []int32, dbs []dbPlan) (CopyProgress, map[string]CopyProgress, error) {
	sourceLSN := map[int32]int64{}
	for _, s := range srcIDs {
		conn, err := c.Shards.Dial(ctx, srcSet, s)
		if err != nil {
			return CopyProgress{}, nil, err
		}
		rows, err := conn.Query(ctx, `SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint`)
		if err == nil {
			var lsn int64
			lsn, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[int64])
			sourceLSN[s] = lsn
		}
		_ = conn.Close(ctx)
		if err != nil {
			return CopyProgress{}, nil, err
		}
	}
	var reports []SubscriptionProgress
	perTarget := map[int32][]SubscriptionProgress{}
	for _, db := range dbs {
		for _, t := range wf.ids {
			conn, err := c.Shards.DialDatabase(ctx, wf.set, t, db.name)
			if err != nil {
				return CopyProgress{}, nil, err
			}
			rs, err := subscriptionReports(ctx, conn, db.name, wf.gen, t, srcIDs, sourceLSN)
			_ = conn.Close(ctx)
			if err != nil {
				return CopyProgress{}, nil, fmt.Errorf("progress of %s on %s/%d: %w", db.name, wf.set, t, err)
			}
			reports = append(reports, rs...)
			perTarget[t] = append(perTarget[t], rs...)
		}
	}
	byTarget := make(map[string]CopyProgress, len(perTarget))
	for t, rs := range perTarget {
		byTarget[strconv.Itoa(int(t))] = Aggregate(rs)
	}
	return Aggregate(reports), byTarget, nil
}

// subscriptionReports reads one database's subscriptions on one target.
// Every report is named database/subscription: the subscription name
// carries the target and source shards but not the database, and a copy
// moves the same table names in each of them.
func subscriptionReports(ctx context.Context, conn ShardConn, db string, gen int64, t int32, srcIDs []int32, sourceLSN map[int32]int64) ([]SubscriptionProgress, error) {
	rows, err := conn.Query(ctx, `
		SELECT s.subname, s.subenabled, coalesce(r.srsubstate, ''), count(r.srrelid),
		       (SELECT (st.latest_end_lsn - '0/0'::pg_lsn)::bigint FROM pg_stat_subscription st WHERE st.subid = s.oid AND st.relid IS NULL AND st.latest_end_lsn IS NOT NULL LIMIT 1),
		       -- Name the relations that are NOT ready, bounded: "5/8 tables
		       -- ready" alone does not say which three are stuck.
		       (array_agg(c.relnamespace::regnamespace || '.' || c.relname ORDER BY c.relname)
		          FILTER (WHERE r.srsubstate IS DISTINCT FROM 'r'))[1:3]
		FROM pg_subscription s LEFT JOIN pg_subscription_rel r ON r.srsubid = s.oid
		     LEFT JOIN pg_class c ON c.oid = r.srrelid
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
		var stuck []string
		if err := rows.Scan(&name, &enabled, &state, &n, &applied, &stuck); err != nil {
			return nil, err
		}
		p := bySub[name]
		if p == nil {
			p = &SubscriptionProgress{Name: db + "/" + name, Rels: map[RelState]int{}, Enabled: enabled, LagBytes: LagUnknown}
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
		for _, rel := range stuck {
			if len(p.Blockers) >= BlockerSamples {
				break
			}
			p.Blockers = append(p.Blockers, fmt.Sprintf("%s(%s)", rel, state))
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
	// Cleanup has to run against the source this workflow actually built its
	// publications and slots on. Asking which set is serving now would leave
	// them behind on the old one, holding WAL for good.
	srcSet, srcIDs, err := c.pinSource(ctx, wf)
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
	message := "copy cancelled: subscriptions, slots and publications dropped"
	// Past the fence the forward cleanup above is not the whole undo: the
	// write fence is still raised on the sources and the reverse replication
	// still attached to them, and neither is anyone else's to remove. Without
	// this the sources stay unwritable with no workflow left to lift them.
	if fencedStage(wf.stage) {
		ops, err := c.pgCutover(ctx, wf)
		if err != nil {
			return err
		}
		if err := ops.Unwind(ctx); err != nil {
			return fmt.Errorf("cancel: undoing the started cutover: %w", err)
		}
		message = "cutover cancelled: fence lifted, reverse replication and forward objects dropped"
	}
	if err := ownedExec(ctx, c.Pool, wf.owner,
		`UPDATE pgshard.workflows SET state = $2, status = status || $3::jsonb, updated_at = now()
		 WHERE id = $1::uuid AND ($4::text IS NULL OR (owner = $4 AND state = $5))`,
		wf.id, StateCancelled, mustJSON(map[string]any{"stage": StageCancelled, "message": message}), nullIfEmpty(wf.owner), wf.fence); err != nil {
		return err
	}
	wf.fence = StateCancelled
	return nil
}

// fencedStage reports whether a workflow's stage means a cutover has started,
// so the fence may be raised and reverse replication may exist.
func fencedStage(stage string) bool {
	switch stage {
	case StageSwitching, StageSwitched, StageRollingBack, StageCompleting:
		return true
	}
	return false
}

func dropSubscriptions(ctx context.Context, conn ShardConn, gen int64, t int32) error {
	return dropSubscriptionsLike(ctx, conn, fmt.Sprintf("pgshard\\_reshard\\_g%d\\_t%d\\_%%", gen, t))
}

func dropSubscriptionsLike(ctx context.Context, conn ShardConn, pattern string) error {
	rows, err := conn.Query(ctx, `SELECT subname FROM pg_subscription WHERE subname LIKE $1`, pattern)
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
