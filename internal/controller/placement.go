package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// Stages of a table placement workflow (pgshard.workflows.status->>'stage').
const (
	StagePlacementPreparing = "preparing"
	StagePlacementShadow    = "shadow"
	StagePlacementCopying   = "copying"
	StagePlacementCatchUp   = "catch_up"
	StagePlacementBuffering = "buffering"
	StagePlacementSwapping  = "swapping"
	StagePlacementRetiring  = "retiring"
)

// Placement workflow limits.
const (
	// DefaultBufferTimeout bounds the table-scoped write pause of one swap
	// attempt; a longer pause releases the fence and catches up again.
	DefaultBufferTimeout = 30 * time.Second
	// DefaultBufferAttempts is how many released pauses fail the workflow.
	DefaultBufferAttempts = 3
	// DefaultDropOldAfter is how long the renamed old tables stay.
	DefaultDropOldAfter = time.Hour
	// DefaultCopyBatch is the keyset page of the initial copy.
	DefaultCopyBatch = 1000
	// ShadowSuffix and OldSuffix name the shadow table a placement workflow
	// fills and the previous table it leaves behind after the swap.
	ShadowSuffix = "__pgshard_new"
	OldSuffix    = "__pgshard_old"
	// LockKindTable is the pgshard.workflow_locks kind a placement
	// workflow holds on database.schema.table.
	LockKindTable = "table"
)

// placementSpec is the spec the reconciler writes for a placement workflow.
type placementSpec struct {
	Database          string         `json:"database"`
	SchemaName        string         `json:"schema_name"`
	TableName         string         `json:"table_name"`
	From              TablePlacement `json:"from"`
	To                TablePlacement `json:"to"`
	DesiredGeneration int64          `json:"desired_generation"`
	// DropOldAfterSeconds overrides the grace before the old tables drop.
	DropOldAfterSeconds *int64 `json:"drop_old_after_seconds,omitempty"`
}

func (s placementSpec) table() string { return s.Database + "." + s.SchemaName + "." + s.TableName }

// placementState is the workflow's record under status->'placement'.
type placementState struct {
	SourceSet    string   `json:"source_set,omitempty"`
	Sources      []int32  `json:"sources,omitempty"`
	Targets      []int32  `json:"targets,omitempty"`
	Holders      []int32  `json:"holders,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	Identity     []string `json:"identity,omitempty"`
	TableComment *string  `json:"table_comment,omitempty"`
	// RowSecurity and ForceRowSecurity are the source table's RLS flags.
	// They are applied at the SWAP, not on the shadow: enabling row-level
	// security while the copy is running would filter the copier's own
	// rows, and FORCE applies to the table owner too.
	RowSecurity      bool `json:"row_security,omitempty"`
	ForceRowSecurity bool `json:"force_row_security,omitempty"`
	// Owner is the source table's owner and Grants the statements that
	// reproduce its table and column privileges. Both are applied at the
	// SWAP, after the rename, so they name the table clients see and the
	// controller keeps the rights it needs while it is still building.
	Owner  string   `json:"owner,omitempty"`
	Grants []string `json:"grants,omitempty"`
	// Triggers is each user trigger's pg_trigger.tgenabled on the source.
	// Applied at the SWAP for the same reason: a trigger firing while the
	// copy writes the shadow runs for every copied row, which is a row the
	// source has already triggered on.
	Triggers  map[string]string `json:"triggers,omitempty"`
	PK        []string          `json:"pk,omitempty"`
	KeyType   string            `json:"key_type,omitempty"`
	Copied    map[string]bool   `json:"copied,omitempty"`
	Applied   map[string]int64  `json:"applied,omitempty"`
	LagBytes  int64             `json:"lag_bytes"`
	FencedAt  *time.Time        `json:"fenced_at,omitempty"`
	SwappedAt *time.Time        `json:"swapped_at,omitempty"`
	// Swapped lists the shards whose swap transaction was started; the
	// marker is persisted before the first rename on a shard, so only a
	// resume that finds it may treat a missing shadow as already swapped.
	Swapped  []int32  `json:"swapped,omitempty"`
	PauseMS  int64    `json:"pause_ms,omitempty"`
	Attempts int      `json:"attempts,omitempty"`
	Aborts   []string `json:"aborts,omitempty"`
	// ReplicaIdentityFull lists the sources whose table got REPLICA
	// IDENTITY FULL for the run.
	ReplicaIdentityFull []int32 `json:"replica_identity_full,omitempty"`
	Quiet               int     `json:"quiet,omitempty"`
}

type placementWorkflow struct {
	id    string
	state string
	stage string
	spec  placementSpec
	st    placementState
	// owner is this pass's claim on the workflow and fence the state it
	// started from; every write it makes requires both to still hold, so a
	// workflow taken over stops the old pass instead of racing the new one.
	owner string
	fence string
	rt    *placementRouter
	from  *placementRouter
	shape rowShape
}

func (wf *placementWorkflow) shadow() string { return wf.spec.TableName + ShadowSuffix }
func (wf *placementWorkflow) old() string    { return wf.spec.TableName + OldSuffix }

// slotName names the slot and publication of one source in a run: the
// first 8 hex digits of the workflow id keep two runs on one table apart.
func (wf *placementWorkflow) slotName(source int32) string {
	return fmt.Sprintf("pgshard_place_%s_s%d", strings.ReplaceAll(wf.id, "-", "")[:8], source)
}

func (wf *placementWorkflow) publicationName() string {
	return fmt.Sprintf("pgshard_place_%s", strings.ReplaceAll(wf.id, "-", "")[:8])
}

// Placer drives table placement workflows: shadow tables on the shards
// the new placement uses, an initial copy routed by the new placement, a
// pgoutput catch-up applied to the shadows, a table-scoped write pause,
// and the swap that publishes the new placement. Every stage is idempotent
// against the catalogs of the shards, so a restarted controller resumes
// where the previous one stopped.
type Placer struct {
	Pool   *pgxpool.Pool
	Shards ShardDBDialer
	Logger *slog.Logger
	// LagBytes is the slot lag under which the catch-up counts as done.
	LagBytes int64
	// SlotFailoverDisabled turns off failover slots for the placement's
	// own slots, and reads backwards for the same reason Copier's does:
	// the zero value has to be the safe one.
	SlotFailoverDisabled bool
	// BufferTimeout bounds one write pause; BufferAttempts how many
	// released pauses fail the workflow.
	BufferTimeout  time.Duration
	BufferAttempts int
	// OwnerLease is how long this replica's claim on a workflow stands
	// without being refreshed; zero means DefaultOwnerLease. Replica
	// identifies this process in a claim; zero means a token generated
	// once per process.
	OwnerLease time.Duration
	Replica    string
	// DropOldAfter is the grace before the old tables drop.
	DropOldAfter time.Duration
	// CopyBatch is the keyset page of the initial copy.
	CopyBatch int
	// Now overrides the clock in tests.
	Now func() time.Time
}

// PlacementOutcome counts one pass.
type PlacementOutcome struct {
	Driven    int
	Advanced  int
	Completed int
	Cancelled int
	Failed    int
}

// Run drives a placement pass on every tick while this replica is the
// leader. Only the leader may run one: a pass builds shadow tables and
// renames them into place.
func (p *Placer) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	runLoop(ctx, interval, leader, p.logger, "placement", func(ctx context.Context) {
		if _, err := p.Pass(ctx); err != nil {
			p.logger().Warn("placement pass failed", "err", err)
		}
	})
}

func (p *Placer) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return slog.Default()
}

func (p *Placer) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Placer) lagBytes() int64 {
	if p.LagBytes > 0 {
		return p.LagBytes
	}
	return DefaultLagBytes
}

func (p *Placer) bufferTimeout() time.Duration {
	if p.BufferTimeout > 0 {
		return p.BufferTimeout
	}
	return DefaultBufferTimeout
}

func (p *Placer) bufferAttempts() int {
	if p.BufferAttempts > 0 {
		return p.BufferAttempts
	}
	return DefaultBufferAttempts
}

func (p *Placer) copyBatch() int {
	if p.CopyBatch > 0 {
		return p.CopyBatch
	}
	return DefaultCopyBatch
}

func (p *Placer) dropOldAfter(wf *placementWorkflow) time.Duration {
	if wf.spec.DropOldAfterSeconds != nil {
		return time.Duration(*wf.spec.DropOldAfterSeconds) * time.Second
	}
	if p.DropOldAfter > 0 {
		return p.DropOldAfter
	}
	return DefaultDropOldAfter
}

// Pass drives every placement workflow once.
func (p *Placer) Pass(ctx context.Context) (PlacementOutcome, error) {
	var out PlacementOutcome
	wfs, err := p.list(ctx)
	if err != nil {
		return out, err
	}
	for i := range wfs {
		wf := &wfs[i]
		var err error
		var held bool
		if wf.owner, held, err = claimWorkflow(ctx, p.Pool, p.Replica, wf.id, p.OwnerLease); err != nil {
			return out, err
		} else if !held {
			continue
		}
		wf.fence = wf.state
		out.Driven++
		advanced, err := p.drive(ctx, wf)
		if advanced {
			out.Advanced++
		}
		switch {
		case err == nil && wf.stage == StageCompleted:
			out.Completed++
		case err == nil && wf.stage == StageCancelled:
			out.Cancelled++
		}
		if err == nil {
			continue
		}
		if errors.Is(err, errNotOwner) {
			p.logger().Info("table placement pass handed over", "workflow", wf.id, "table", wf.spec.table())
			continue
		}
		if isFatal(err) {
			out.Failed++
			p.logger().Error("table placement failed", "workflow", wf.id, "table", wf.spec.table(), "err", err)
			if ferr := p.fail(ctx, wf, err); ferr != nil {
				if errors.Is(ferr, errNotOwner) {
					continue
				}
				return out, ferr
			}
			continue
		}
		p.logger().Warn("table placement pass incomplete", "workflow", wf.id, "table", wf.spec.table(), "err", err)
		if serr := p.save(ctx, wf, err.Error()); serr != nil && !errors.Is(serr, errNotOwner) {
			return out, serr
		}
	}
	return out, nil
}

func (p *Placer) list(ctx context.Context) ([]placementWorkflow, error) {
	rows, err := p.Pool.Query(ctx, `SELECT id::text, state, coalesce(status->>'stage', ''), spec, coalesce(status->'placement', '{}'::jsonb)
		FROM pgshard.workflows
		WHERE kind = $1 AND (state = ANY($2) OR status->>'stage' = $3)
		ORDER BY created_at`, KindTablePlacement, []string{StatePending, StateRunning}, StageCancelling)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []placementWorkflow
	for rows.Next() {
		var wf placementWorkflow
		var spec, st []byte
		if err := rows.Scan(&wf.id, &wf.state, &wf.stage, &spec, &st); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(spec, &wf.spec); err != nil {
			return nil, fmt.Errorf("workflow %s spec: %w", wf.id, err)
		}
		if err := json.Unmarshal(st, &wf.st); err != nil {
			return nil, fmt.Errorf("workflow %s placement state: %w", wf.id, err)
		}
		if wf.st.Copied == nil {
			wf.st.Copied = map[string]bool{}
		}
		if wf.st.Applied == nil {
			wf.st.Applied = map[string]int64{}
		}
		out = append(out, wf)
	}
	return out, rows.Err()
}

func (p *Placer) save(ctx context.Context, wf *placementWorkflow, message string) error {
	patch := map[string]any{"stage": wf.stage, "placement": wf.st, "message": message}
	if err := ownedExec(ctx, p.Pool, wf.owner,
		`UPDATE pgshard.workflows SET state = $2, status = status || $3::jsonb, updated_at = now()
		 WHERE id = $1::uuid AND ($4::text IS NULL OR (owner = $4 AND state = $5))`,
		wf.id, wf.state, mustJSON(patch), nullIfEmpty(wf.owner), wf.fence); err != nil {
		return err
	}
	wf.fence = wf.state
	return nil
}

func (p *Placer) finish(ctx context.Context, wf *placementWorkflow, state, message string) error {
	wf.state = state
	return p.save(ctx, wf, message)
}

// fail ends the workflow; a fence or lock it holds never outlives it.
func (p *Placer) fail(ctx context.Context, wf *placementWorkflow, cause error) error {
	// A pass that lost the claim must not lift the fence or drop the lock:
	// the replica that owns the workflow now is relying on both.
	if err := holdClaim(ctx, p.Pool, wf.id, wf.owner); err != nil {
		return err
	}
	if wf.rt != nil {
		if err := p.releaseFence(ctx, wf); err != nil {
			return err
		}
	} else if err := p.releaseShardFence(ctx, wf); err != nil {
		// A failure raised before the routing was built still has to give
		// the table back: a failed workflow is never revisited, so a fence
		// left on would shut the table to every client for good.
		return err
	}
	if err := p.unlock(ctx, wf); err != nil {
		return err
	}
	wf.stage, wf.state = StageFailed, StateFailed
	if err := ownedExec(ctx, p.Pool, wf.owner,
		`UPDATE pgshard.workflows SET state = $2, error = $3, status = status || $4::jsonb, updated_at = now()
		 WHERE id = $1::uuid AND ($5::text IS NULL OR (owner = $5 AND state = $6))`,
		wf.id, StateFailed, cause.Error(), mustJSON(map[string]any{"stage": StageFailed, "placement": wf.st, "message": cause.Error()}),
		nullIfEmpty(wf.owner), wf.fence); err != nil {
		return err
	}
	wf.fence = wf.state
	return nil
}

// drive advances one workflow by one pass; it reports whether the stage
// changed.
func (p *Placer) drive(ctx context.Context, wf *placementWorkflow) (bool, error) {
	if err := holdClaim(ctx, p.Pool, wf.id, wf.owner); err != nil {
		return false, err
	}
	if wf.stage == "" {
		wf.stage = StagePlacementPreparing
	}
	if wf.stage != StagePlacementPreparing {
		if err := p.load(ctx, wf); err != nil {
			return false, err
		}
	}
	switch wf.stage {
	case StagePlacementPreparing:
		return p.prepare(ctx, wf)
	case StagePlacementShadow:
		if err := p.ensureShadows(ctx, wf); err != nil {
			return false, err
		}
		return p.advance(ctx, wf, StagePlacementCopying, "shadow tables created; copying")
	case StagePlacementCopying:
		if err := p.copyAll(ctx, wf); err != nil {
			return false, err
		}
		return p.advance(ctx, wf, StagePlacementCatchUp, "initial copy done; catching up")
	case StagePlacementCatchUp:
		lag, _, err := p.catchUp(ctx, wf, false)
		if err != nil {
			return false, err
		}
		wf.st.LagBytes = lag
		if lag > p.lagBytes() {
			return false, p.save(ctx, wf, fmt.Sprintf("catching up: lag %d bytes", lag))
		}
		return p.advance(ctx, wf, StagePlacementBuffering, "caught up; pausing writes to the table")
	case StagePlacementBuffering:
		return p.buffer(ctx, wf)
	case StagePlacementSwapping:
		// The shards refuse writes from here until publish: the drain is
		// done, and from the first rename onwards a router still holding
		// the old view would otherwise write the live name on a shard that
		// has already swapped. Once the swap is published there is nothing
		// left to protect, and re-arming on a retry of a later step would
		// shut the table to every client over one unreachable shard.
		if wf.st.SwappedAt == nil {
			if err := p.fenceShards(ctx, wf); err != nil {
				return false, err
			}
		}
		if _, _, err := p.catchUp(ctx, wf, true); err != nil {
			return false, err
		}
		if err := p.verifyPlacement(ctx, wf); err != nil {
			return false, err
		}
		if err := p.swapAll(ctx, wf); err != nil {
			return false, err
		}
		if err := p.publish(ctx, wf); err != nil {
			return false, err
		}
		// publish clears the catalog flag itself; the shards' own fence has
		// to come off too or every write to the table keeps being refused.
		if err := p.releaseShardFence(ctx, wf); err != nil {
			return false, err
		}
		if err := p.dropReplication(ctx, wf); err != nil {
			return false, err
		}
		return p.advance(ctx, wf, StagePlacementRetiring, fmt.Sprintf("new placement published: pause %dms; old tables drop after %s", wf.st.PauseMS, p.dropOldAfter(wf)))
	case StagePlacementRetiring:
		if wf.st.SwappedAt != nil {
			if remaining := wf.st.SwappedAt.Add(p.dropOldAfter(wf)).Sub(p.now()); remaining > 0 {
				return false, p.save(ctx, wf, fmt.Sprintf("retiring: old tables drop in %s", remaining.Round(time.Second)))
			}
		}
		if err := p.dropOld(ctx, wf); err != nil {
			return false, err
		}
		wf.stage = StageCompleted
		return true, p.finish(ctx, wf, StateCompleted, "placement completed: old tables dropped")
	case StageCancelling:
		if err := p.cleanup(ctx, wf); err != nil {
			return false, err
		}
		wf.stage = StageCancelled
		return true, p.finish(ctx, wf, StateCancelled, "placement cancelled: shadow tables and replication objects dropped")
	}
	return false, nil
}

func (p *Placer) advance(ctx context.Context, wf *placementWorkflow, stage, message string) (bool, error) {
	wf.stage = stage
	return true, p.save(ctx, wf, message)
}

// prepare waits for reshards, validates the table against a source shard,
// takes the per-table lock and records the routing plan.
func (p *Placer) prepare(ctx context.Context, wf *placementWorkflow) (bool, error) {
	var reshards int
	if err := p.Pool.QueryRow(ctx, `SELECT count(*) FROM pgshard.workflows WHERE kind = ANY($1) AND state = ANY($2)`, copyKinds, activeStates).Scan(&reshards); err != nil {
		return false, err
	}
	if reshards > 0 {
		return false, fmt.Errorf("waiting for %d active reshard workflow(s)", reshards)
	}
	if err := p.load(ctx, wf); err != nil {
		return false, err
	}
	if err := p.describe(ctx, wf); err != nil {
		return false, err
	}
	if _, err := p.Pool.Exec(ctx, `INSERT INTO pgshard.workflow_locks (kind, key, workflow_id) VALUES ($1, $2, $3::uuid)
		ON CONFLICT (kind, key) DO UPDATE SET workflow_id = EXCLUDED.workflow_id WHERE pgshard.workflow_locks.workflow_id = EXCLUDED.workflow_id`,
		LockKindTable, wf.spec.table(), wf.id); err != nil {
		return false, err
	}
	var holder string
	if err := p.Pool.QueryRow(ctx, `SELECT workflow_id::text FROM pgshard.workflow_locks WHERE kind = $1 AND key = $2`, LockKindTable, wf.spec.table()).Scan(&holder); err != nil {
		return false, err
	}
	if holder != wf.id {
		return false, fmt.Errorf("table %s is locked by workflow %s", wf.spec.table(), holder)
	}
	wf.state = StateRunning
	return p.advance(ctx, wf, StagePlacementShadow, fmt.Sprintf("%s -> %s: sources %v, targets %v", describePlacement(wf.spec.From), describePlacement(wf.spec.To), wf.st.Sources, wf.st.Targets))
}

func describePlacement(t TablePlacement) string {
	if t.Placement == "sharded" {
		return "sharded(" + t.key() + ")"
	}
	return t.Placement
}

// load resolves the serving map and builds the routers of both placements.
func (p *Placer) load(ctx context.Context, wf *placementWorkflow) error {
	var set string
	if err := p.Pool.QueryRow(ctx, `SELECT shard_set FROM pgshard.shard_sets WHERE state = $1 ORDER BY generation DESC LIMIT 1`, catalog.ShardSetServing).Scan(&set); err != nil {
		return fmt.Errorf("serving shard set: %w", err)
	}
	if wf.st.SourceSet != "" && wf.st.SourceSet != set {
		return fatal("serving shard set changed from %s to %s during the workflow", wf.st.SourceSet, set)
	}
	wf.st.SourceSet = set
	ranges, err := catalog.ListShardRanges(ctx, p.Pool, set)
	if err != nil {
		return err
	}
	var home int32
	if err := p.Pool.QueryRow(ctx, `SELECT home_shard FROM pgshard.databases WHERE name = $1`, wf.spec.Database).Scan(&home); errors.Is(err, pgx.ErrNoRows) {
		return fatal("database %s is not registered", wf.spec.Database)
	} else if err != nil {
		return err
	}
	var ids []int32
	for _, r := range ranges {
		ids = append(ids, r.ShardID)
	}
	rs := catalog.RangeSet(ranges)
	wf.rt = &placementRouter{placement: wf.spec.To, home: home, ids: ids, ranges: rs, keyIndex: -1}
	wf.from = &placementRouter{placement: wf.spec.From, home: home, ids: ids, ranges: rs, keyIndex: -1}
	wf.shape = rowShape{Schema: wf.spec.SchemaName, Name: wf.spec.TableName, Columns: wf.st.Columns, Identity: wf.st.Identity, PK: wf.st.PK}
	wf.rt.keyIndex = slices.Index(wf.st.Columns, wf.spec.To.key())
	wf.from.keyIndex = slices.Index(wf.st.Columns, wf.spec.From.key())
	wf.rt.keyType, wf.from.keyType = wf.st.KeyType, wf.st.KeyType
	if wf.spec.From.Placement == "sharded" && wf.spec.From.key() != wf.spec.To.key() {
		wf.from.keyType = ""
	}
	return nil
}

// buffer raises the table fence, drains the last changes and hands over to
// the swap once the sources stood still through a whole catch-up.
func (p *Placer) buffer(ctx context.Context, wf *placementWorkflow) (bool, error) {
	if wf.st.FencedAt == nil {
		if err := p.fence(ctx, wf); err != nil {
			return false, err
		}
		now := p.now()
		wf.st.FencedAt = &now
		wf.st.Quiet = 0
		if err := p.save(ctx, wf, "writes to the table paused; draining"); err != nil {
			return false, err
		}
	}
	deadline := wf.st.FencedAt.Add(p.bufferTimeout())
	for {
		lag, applied, err := p.catchUp(ctx, wf, true)
		if err != nil {
			return false, err
		}
		wf.st.LagBytes = lag
		if applied == 0 && lag == 0 {
			wf.st.Quiet++
		} else {
			wf.st.Quiet = 0
		}
		if wf.st.Quiet >= 2 {
			return p.advance(ctx, wf, StagePlacementSwapping, "sources drained; swapping tables")
		}
		if p.now().After(deadline) {
			return p.abortBuffer(ctx, wf, fmt.Sprintf("sources did not drain within %s", p.bufferTimeout()))
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (p *Placer) abortBuffer(ctx context.Context, wf *placementWorkflow, reason string) (bool, error) {
	if err := holdClaim(ctx, p.Pool, wf.id, wf.owner); err != nil {
		return false, err
	}
	if err := p.releaseFence(ctx, wf); err != nil {
		return false, err
	}
	wf.st.Attempts++
	wf.st.Aborts = append(wf.st.Aborts, fmt.Sprintf("%s: %s", p.now().Format(time.RFC3339), reason))
	wf.st.FencedAt, wf.st.Quiet = nil, 0
	if wf.st.Attempts >= p.bufferAttempts() {
		return false, fatal("write pause released %d times, last: %s", wf.st.Attempts, reason)
	}
	return p.advance(ctx, wf, StagePlacementCatchUp, "write pause released ("+reason+"); catching up again")
}

// cleanup undoes a cancelled run: shadow tables, slots, publications,
// fence and lock.
func (p *Placer) cleanup(ctx context.Context, wf *placementWorkflow) error {
	if err := holdClaim(ctx, p.Pool, wf.id, wf.owner); err != nil {
		return err
	}
	if err := p.releaseFence(ctx, wf); err != nil {
		return err
	}
	if err := p.dropReplication(ctx, wf); err != nil {
		return err
	}
	for _, t := range wf.rt.ids {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, t, wf.spec.Database)
		if err != nil {
			return err
		}
		dropped, derr := dropArtifactTable(ctx, conn, wf.spec.SchemaName, wf.shadow(), wf.placementMarker())
		if !dropped && derr == nil {
			// Same reasoning as the retirement path: a shadow left behind
			// because it is not ours is the right call, and saying nothing
			// about it is not.
			if left, lerr := tableExists(ctx, conn, wf.spec.SchemaName, wf.shadow()); lerr == nil && left {
				p.logger().Warn("cancellation left a shadow in place: it does not carry this workflow's marker",
					"shard", fmt.Sprintf("%s/%d", wf.st.SourceSet, t), "table", wf.shape.qualified(wf.shadow()), "workflow", wf.id)
			}
		}
		_ = conn.Close(ctx)
		if derr != nil {
			return derr
		}
	}
	return p.unlock(ctx, wf)
}

// cancelPlacement marks an active placement workflow for cleanup unless it
// passed the swap; it reports whether it did.
func cancelPlacement(ctx context.Context, tx pgx.Tx, id string) (bool, error) {
	tag, err := tx.Exec(ctx, `UPDATE pgshard.workflows SET state = $2, status = status || $3::jsonb, updated_at = now()
		WHERE id = $1::uuid AND kind = $4 AND state = ANY($5) AND coalesce(status->>'stage', '') <> ALL($6)`,
		id, StateCancelled, mustJSON(map[string]any{"stage": StageCancelling, "reason": "desired placement reverted"}),
		KindTablePlacement, activeStates, []string{StagePlacementSwapping, StagePlacementRetiring})
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
