// Package controller reconciles the desired-state tables of the pgshard
// catalog into status tables and workflows.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// Workflow kinds and states written to pgshard.workflows.
const (
	KindTablePlacement = "table_placement"
	KindReshard        = "reshard"
	// KindUpgrade is a blue/green major-version replacement: the reshard
	// machinery with a 1:1 range map onto groups of the new major.
	KindUpgrade = "upgrade"

	StatePending = "pending"
	// StateProvisioning is a reshard whose target groups are being created.
	StateProvisioning = "provisioning"
	StateRunning      = "running"
	StatePaused       = "paused"
	StateCompleted    = "completed"
	StateFailed       = "failed"
	StateCancelled    = "cancelled"
)

// copyKinds are the workflow kinds the copier and cutover drive.
var copyKinds = []string{KindReshard, KindUpgrade}

// Serving states the controller writes to pgshard.shard_status.
const (
	ServingProvisioning = "provisioning"
	ServingServing      = "serving"
	ServingRetired      = "retired"
)

// Result summarises one reconciliation pass.
type Result struct {
	TablesMadeEffective int
	WorkflowsCreated    int
	ShardSetsPopulated  int
	ShardsMadeServing   int
	ReshardsAdvanced    int
	ReshardsCancelled   int
	PlacementsCancelled int
	GenerationBumped    bool
	Invalid             []string
}

// Reconcile runs one pass in a single transaction on conn.
func Reconcile(ctx context.Context, conn *pgx.Conn, logger *slog.Logger) (Result, error) {
	if logger == nil {
		logger = slog.Default()
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var res Result
	if err := reconcileTables(ctx, tx, &res); err != nil {
		return Result{}, fmt.Errorf("controller: tables: %w", err)
	}
	if err := reconcileShardSets(ctx, tx, &res); err != nil {
		return Result{}, fmt.Errorf("controller: shard sets: %w", err)
	}
	if err := reconcileReshards(ctx, tx, &res); err != nil {
		return Result{}, fmt.Errorf("controller: reshards: %w", err)
	}
	if res.GenerationBumped {
		if _, err := tx.Exec(ctx, `UPDATE pgshard.shard_map_generation SET generation = generation + 1, updated_at = now()`); err != nil {
			return Result{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	for _, msg := range res.Invalid {
		logger.Warn("desired state rejected", "reason", msg)
	}
	return res, nil
}

type tableKey struct{ db, schema, name string }

func validateTable(t catalog.Table) error {
	switch t.Placement {
	case "sharded":
		if t.ShardKey == nil || *t.ShardKey == "" {
			return errors.New("sharded table without shard_key")
		}
		if t.HashVersion != 1 {
			return fmt.Errorf("unsupported hash_version %d", t.HashVersion)
		}
	case "reference", "unsharded":
		if t.ShardKey != nil {
			return fmt.Errorf("%s table with shard_key", t.Placement)
		}
	default:
		return fmt.Errorf("unknown placement %q", t.Placement)
	}
	return nil
}

func reconcileTables(ctx context.Context, tx pgx.Tx, res *Result) error {
	tables, err := catalog.ListAllTables(ctx, tx)
	if err != nil {
		return err
	}
	statuses, err := catalog.ListAllTableStatus(ctx, tx)
	if err != nil {
		return err
	}
	status := map[tableKey]catalog.TableStatus{}
	for _, s := range statuses {
		status[tableKey{s.Database, s.SchemaName, s.TableName}] = s
	}
	for _, t := range tables {
		name := fmt.Sprintf("%s.%s.%s", t.Database, t.SchemaName, t.TableName)
		if err := validateTable(t); err != nil {
			res.Invalid = append(res.Invalid, fmt.Sprintf("table %s: %v", name, err))
			continue
		}
		s, ok := status[tableKey{t.Database, t.SchemaName, t.TableName}]
		switch {
		case !ok || s.EffectivePlacement == nil:
			if _, err := tx.Exec(ctx, `
				INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, effective_shard_key, effective_generation)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (database, schema_name, table_name) DO UPDATE
				SET effective_placement = EXCLUDED.effective_placement, effective_shard_key = EXCLUDED.effective_shard_key,
				    effective_generation = EXCLUDED.effective_generation, updated_at = now()`,
				t.Database, t.SchemaName, t.TableName, t.Placement, t.ShardKey, t.DesiredGeneration); err != nil {
				return err
			}
			res.TablesMadeEffective++
		case *s.EffectivePlacement == t.Placement && equalPtr(s.EffectiveShardKey, t.ShardKey):
			if s.WorkflowID != nil {
				cancelled, err := cancelPlacement(ctx, tx, *s.WorkflowID)
				if err != nil {
					return err
				}
				if cancelled {
					res.PlacementsCancelled++
				}
			}
			if s.EffectiveGeneration != t.DesiredGeneration {
				if _, err := tx.Exec(ctx, `UPDATE pgshard.table_status SET effective_generation = $4, updated_at = now()
					WHERE database = $1 AND schema_name = $2 AND table_name = $3`,
					t.Database, t.SchemaName, t.TableName, t.DesiredGeneration); err != nil {
					return err
				}
			}
		default:
			if s.WorkflowID != nil {
				active, err := workflowActive(ctx, tx, *s.WorkflowID)
				if err != nil {
					return err
				}
				if active {
					continue
				}
				failed, err := placementFailed(ctx, tx, *s.WorkflowID, t.DesiredGeneration)
				if err != nil {
					return err
				}
				if failed {
					continue
				}
			}
			spec := map[string]any{
				"database": t.Database, "schema_name": t.SchemaName, "table_name": t.TableName,
				"from":               map[string]any{"placement": *s.EffectivePlacement, "shard_key": s.EffectiveShardKey},
				"to":                 map[string]any{"placement": t.Placement, "shard_key": t.ShardKey},
				"desired_generation": t.DesiredGeneration,
			}
			id, err := createWorkflow(ctx, tx, KindTablePlacement, spec)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE pgshard.table_status SET workflow_id = $4, updated_at = now()
				WHERE database = $1 AND schema_name = $2 AND table_name = $3`,
				t.Database, t.SchemaName, t.TableName, id); err != nil {
				return err
			}
			res.WorkflowsCreated++
		}
	}
	return nil
}

func equalPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// activeStates are the workflow states that still have work to do.
var activeStates = []string{StatePending, StateProvisioning, StateRunning, StatePaused}

func workflowActive(ctx context.Context, tx pgx.Tx, id string) (bool, error) {
	var active bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pgshard.workflows WHERE id = $1::uuid AND state = ANY($2))`,
		id, activeStates).Scan(&active)
	return active, err
}

// placementFailed reports whether workflow id failed for the same desired
// generation: the change is not retried until the row is edited again.
func placementFailed(ctx context.Context, tx pgx.Tx, id string, generation int64) (bool, error) {
	var failed bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pgshard.workflows WHERE id = $1::uuid AND state = $2 AND (spec->>'desired_generation')::bigint = $3)`,
		id, StateFailed, generation).Scan(&failed)
	return failed, err
}

func createWorkflow(ctx context.Context, tx pgx.Tx, kind string, spec map[string]any) (string, error) {
	body, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	var id string
	err = tx.QueryRow(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec) VALUES (gen_random_uuid(), $1, $2, $3) RETURNING id::text`,
		kind, StatePending, body).Scan(&id)
	return id, err
}

func reconcileShardSets(ctx context.Context, tx pgx.Tx, res *Result) error {
	ranges, err := catalog.ListAllShardRanges(ctx, tx)
	if err != nil {
		return err
	}
	bySet := map[string][]catalog.ShardRange{}
	var order []string
	for _, r := range ranges {
		if _, ok := bySet[r.ShardSet]; !ok {
			order = append(order, r.ShardSet)
		}
		bySet[r.ShardSet] = append(bySet[r.ShardSet], r)
	}
	statuses, err := catalog.ListAllShardStatus(ctx, tx)
	if err != nil {
		return err
	}
	statusBySet := map[string][]catalog.ShardStatus{}
	for _, s := range statuses {
		statusBySet[s.ShardSet] = append(statusBySet[s.ShardSet], s)
	}
	sets, err := catalog.ListShardSets(ctx, tx)
	if err != nil {
		return err
	}
	// Only serving sets get shard_status rows here: pending sets are the
	// reshard workflow's, retired sets keep whatever the cutover left.
	pending := map[string]bool{}
	for _, ss := range sets {
		if ss.State != catalog.ShardSetServing {
			pending[ss.Name] = true
		}
	}
	for _, set := range order {
		if pending[set] {
			continue
		}
		desired := bySet[set]
		if err := catalog.ValidateShardRanges(desired); err != nil {
			res.Invalid = append(res.Invalid, fmt.Sprintf("shard set %s: %v", set, err))
			continue
		}
		var maxGen int64
		for _, r := range desired {
			maxGen = max(maxGen, r.DesiredGeneration)
		}
		existing := statusBySet[set]
		if len(existing) == 0 {
			for _, r := range desired {
				if _, err := tx.Exec(ctx, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state)
					VALUES ($1, $2, $3, $4)`, set, r.ShardID, groupName(set, r.ShardID), ServingProvisioning); err != nil {
					return err
				}
			}
			if err := publishServing(ctx, tx, set, maxGen); err != nil {
				return err
			}
			res.ShardSetsPopulated++
			res.GenerationBumped = true
			continue
		}
		for _, s := range existing {
			if s.ServingState == ServingProvisioning && s.PrimaryEndpoint != nil && *s.PrimaryEndpoint != "" {
				if _, err := tx.Exec(ctx, `UPDATE pgshard.shard_status SET serving_state = $3, updated_at = now()
					WHERE shard_set = $1 AND shard_id = $2`, set, s.ShardID, ServingServing); err != nil {
					return err
				}
				res.ShardsMadeServing++
				res.GenerationBumped = true
			}
		}
		var served int64
		err := tx.QueryRow(ctx, `SELECT generation FROM pgshard.serving WHERE shard_set = $1`, set).Scan(&served)
		if errors.Is(err, pgx.ErrNoRows) {
			served = 0
		} else if err != nil {
			return err
		}
		if maxGen <= served {
			continue
		}
		var active bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pgshard.workflows
			WHERE kind = ANY($1) AND spec->>'shard_set' = $2 AND state = ANY($3))`,
			copyKinds, set, activeStates).Scan(&active); err != nil {
			return err
		}
		if active {
			continue
		}
		spec := map[string]any{"shard_set": set, "desired_generation": maxGen, "ranges": specRanges(desired)}
		if _, err := createWorkflow(ctx, tx, KindReshard, spec); err != nil {
			return err
		}
		res.WorkflowsCreated++
	}
	return nil
}

func specRanges(ranges []catalog.ShardRange) []map[string]any {
	out := make([]map[string]any, 0, len(ranges))
	for _, r := range ranges {
		out = append(out, map[string]any{"shard_id": r.ShardID, "lower": r.Lower, "upper": r.Upper})
	}
	return out
}

func groupName(set string, id int32) string {
	if set == "default" {
		return fmt.Sprintf("shard%d", id)
	}
	return fmt.Sprintf("%s-shard%d", set, id)
}

func publishServing(ctx context.Context, tx pgx.Tx, set string, generation int64) error {
	_, err := tx.Exec(ctx, `INSERT INTO pgshard.serving (shard_set, generation) VALUES ($1, $2)
		ON CONFLICT (shard_set) DO UPDATE SET generation = EXCLUDED.generation, published_at = now()`, set, generation)
	return err
}
