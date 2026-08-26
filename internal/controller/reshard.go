package controller

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// Reshard workflow stages recorded in pgshard.workflows.status->>'stage'.
// Provisioning waits for the operator to bring every target group up; the
// copy, verify and switch stages are the extension points of later steps.
const (
	StageProvisioning = "provisioning"
	StageReadyForCopy = "ready_for_copy"
)

type reshardWorkflow struct {
	ID        string
	State     string
	ShardSet  string
	SourceSet string
}

// upgradeSet reports whether the pending set ss is a major-version
// replacement of the serving set: its pg_major is set and differs from the
// serving set's (a serving set without one predates upgrades and counts as
// the pending major's predecessor only when the pending major is set).
func upgradeSet(ss catalog.ShardSet, serving *catalog.ShardSet) bool {
	if ss.PGMajor == nil {
		return false
	}
	if serving == nil || serving.PGMajor == nil {
		return true
	}
	return *ss.PGMajor != *serving.PGMajor
}

// reconcileReshards drives pending shard sets through the reshard workflow:
// a desired set gets a provisioning workflow, a provisioning set whose
// targets all have a primary endpoint hands over to the copy stage, and a
// set that vanished cancels its workflow.
func reconcileReshards(ctx context.Context, tx pgx.Tx, res *Result) error {
	sets, err := catalog.ListShardSets(ctx, tx)
	if err != nil {
		return err
	}
	setByName := map[string]catalog.ShardSet{}
	for _, ss := range sets {
		setByName[ss.Name] = ss
	}
	workflows, err := activeReshards(ctx, tx)
	if err != nil {
		return err
	}
	wfBySet := map[string]reshardWorkflow{}
	for _, w := range workflows {
		wfBySet[w.ShardSet] = w
		_, targetLives := setByName[w.ShardSet]
		_, sourceLives := setByName[w.SourceSet]
		sourceGone := w.SourceSet != "" && !sourceLives
		if targetLives && !sourceGone {
			continue
		}
		reason := "shard set removed"
		// Every cutover pass rebuilds the source's shards from its ranges, so
		// a workflow whose source is gone fails the same way on every pass
		// with nothing left to clean up there. Terminate it rather than let it
		// retry forever.
		stage := StageCancelled
		if targetLives {
			reason = "source shard set removed"
		}
		switch w.State {
		case StateProvisioning:
		case StateRunning, StatePaused:
			if !sourceGone {
				// The copier drops subscriptions, slots and publications, then
				// moves the stage to cancelled.
				stage = StageCancelling
			}
		default:
			continue
		}
		if err := setWorkflowState(ctx, tx, w.ID, StateCancelled, map[string]any{"stage": stage, "reason": reason}); err != nil {
			return err
		}
		if !targetLives {
			if _, err := tx.Exec(ctx, `DELETE FROM pgshard.shard_status WHERE shard_set = $1`, w.ShardSet); err != nil {
				return err
			}
		}
		res.ReshardsCancelled++
	}
	for _, ss := range sets {
		switch ss.State {
		case catalog.ShardSetDesired:
			if _, ok := wfBySet[ss.Name]; ok {
				continue
			}
			source, err := catalog.ServingShardSet(ctx, tx)
			if err != nil {
				return err
			}
			// The workflow will own both sets, so both have to be locked
			// before it is inserted, in a fixed order so two reconcilers
			// cannot deadlock against each other.
			if err := catalog.LockShardRangesOf(ctx, tx, ss.Name, source); err != nil {
				return err
			}
			ranges, err := catalog.ListShardRanges(ctx, tx, ss.Name)
			if err != nil {
				return err
			}
			if err := catalog.ValidateShardRanges(ranges); err != nil {
				res.Invalid = append(res.Invalid, fmt.Sprintf("shard set %s: %v", ss.Name, err))
				continue
			}
			kind := KindReshard
			spec := map[string]any{"shard_set": ss.Name, "generation": ss.Generation, "desired_generation": ss.DesiredGeneration, "ranges": specRanges(ranges), "source_set": source}
			srv := setByName[source]
			if upgradeSet(ss, &srv) {
				kind = KindUpgrade
				spec["pg_major"] = *ss.PGMajor
			}
			body, err := json.Marshal(spec)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec, status)
				VALUES (gen_random_uuid(), $1, $2, $3, $4)`,
				kind, StateProvisioning, body, mustJSON(map[string]any{"stage": StageProvisioning})); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE pgshard.shard_sets SET state = $2 WHERE shard_set = $1`, ss.Name, catalog.ShardSetProvisioning); err != nil {
				return err
			}
			res.WorkflowsCreated++
		case catalog.ShardSetProvisioning:
			w, ok := wfBySet[ss.Name]
			if !ok || w.State != StateProvisioning {
				continue
			}
			ready, err := targetsReady(ctx, tx, ss.Name)
			if err != nil {
				return err
			}
			if !ready {
				continue
			}
			if err := setWorkflowState(ctx, tx, w.ID, StateRunning, map[string]any{"stage": StageReadyForCopy}); err != nil {
				return err
			}
			res.ReshardsAdvanced++
		}
	}
	return nil
}

// targetsReady is true once every shard of set has a status row with a
// primary endpoint: the operator's signal that the target group is up.
func targetsReady(ctx context.Context, tx pgx.Tx, set string) (bool, error) {
	var ready bool
	err := tx.QueryRow(ctx, `
		SELECT count(*) > 0 AND count(*) = count(s.primary_endpoint) FILTER (WHERE s.primary_endpoint <> '')
		FROM pgshard.shard_ranges r
		LEFT JOIN pgshard.shard_status s ON s.shard_set = r.shard_set AND s.shard_id = r.shard_id
		WHERE r.shard_set = $1`, set).Scan(&ready)
	return ready, err
}

func activeReshards(ctx context.Context, tx pgx.Tx) ([]reshardWorkflow, error) {
	rows, err := tx.Query(ctx, `SELECT id::text, state, coalesce(spec->>'shard_set', ''),
			coalesce(spec->>'source_set', status->'cutover'->>'source_set', '')
		FROM pgshard.workflows
		WHERE kind = ANY($1) AND state = ANY($2) ORDER BY created_at`, copyKinds, activeStates)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[reshardWorkflow])
}

func setWorkflowState(ctx context.Context, tx pgx.Tx, id, state string, status map[string]any) error {
	_, err := tx.Exec(ctx, `UPDATE pgshard.workflows SET state = $2, status = status || $3::jsonb, updated_at = now() WHERE id = $1::uuid`,
		id, state, mustJSON(status))
	return err
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
