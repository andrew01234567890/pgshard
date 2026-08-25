package controller

import (
	"context"
	"fmt"
)

// MaintenanceGUC marks a session the placement write fence lets through.
// The controller sets it on its own shard connections; a router's session
// never has it, which is what makes the fence hold against a router that
// has not yet seen the table move.
const MaintenanceGUC = "pgshard.maintenance"

// placementFenceTrigger names the trigger the fence installs.
const placementFenceTrigger = "pgshard_placement_fence"

// fenceFunctionSQL creates the trigger function in schema. A router still
// holding the pre-move view would otherwise write the live name on a shard
// that has already swapped, and neither the routing generation nor the
// primary epoch has moved yet to refuse it: the row is not replayed and
// ends up on the wrong shard. The shard itself refusing is the only check
// that does not depend on some other participant being up to date.
func fenceFunctionSQL(schema string) string {
	return `CREATE OR REPLACE FUNCTION ` + QuoteIdent(schema) + `.` + QuoteIdent(placementFenceTrigger) + `()
		RETURNS trigger LANGUAGE plpgsql AS $fence$
		BEGIN
			IF coalesce(current_setting('` + MaintenanceGUC + `', true), '') = 'on' THEN
				IF TG_OP = 'DELETE' THEN
					RETURN OLD;
				END IF;
				RETURN NEW;
			END IF;
			RAISE EXCEPTION 'table %.% is being moved; writes are paused', TG_TABLE_SCHEMA, TG_TABLE_NAME
				USING ERRCODE = '55000',
				      HINT = 'retry once the placement change is published';
		END
		$fence$`
}

// fenceTriggerSQL arms the fence on one table.
func fenceTriggerSQL(schema, qualified string) string {
	return `CREATE TRIGGER ` + QuoteIdent(placementFenceTrigger) + `
		BEFORE INSERT OR UPDATE OR DELETE ON ` + qualified + `
		FOR EACH ROW EXECUTE FUNCTION ` + QuoteIdent(schema) + `.` + QuoteIdent(placementFenceTrigger) + `()`
}

// unfenceTriggerSQL disarms it.
func unfenceTriggerSQL(qualified string) string {
	return `DROP TRIGGER IF EXISTS ` + QuoteIdent(placementFenceTrigger) + ` ON ` + qualified
}

// present reports whether qualified names a relation on this shard. An
// unsharded or reference table lives on only some shards, and a shadow
// exists only where the workflow built one, so a name that is not there is
// simply nothing to fence - a write cannot land on a relation that does
// not exist.
func present(ctx context.Context, conn ShardConn, qualified string) (bool, error) {
	rows, err := conn.Query(ctx, `SELECT to_regclass($1) IS NOT NULL`, qualified)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, rows.Err()
	}
	var ok bool
	if err := rows.Scan(&ok); err != nil {
		return false, err
	}
	return ok, rows.Err()
}

// fenceTables arms the fence on every named table of one shard that has it.
// Both the live table and its shadow are fenced: the swap renames the
// shadow into the live name and a trigger follows its own relation, so
// fencing only the live one would leave a shard unfenced again the moment
// it swapped.
func fenceTables(ctx context.Context, conn ShardConn, schema string, qualified ...string) error {
	if _, err := conn.Exec(ctx, fenceFunctionSQL(schema)); err != nil {
		return fmt.Errorf("create placement fence: %w", err)
	}
	for _, q := range qualified {
		switch there, err := present(ctx, conn, q); {
		case err != nil:
			return fmt.Errorf("look for %s: %w", q, err)
		case !there:
			continue
		}
		if _, err := conn.Exec(ctx, unfenceTriggerSQL(q)); err != nil {
			return fmt.Errorf("re-arm placement fence on %s: %w", q, err)
		}
		if _, err := conn.Exec(ctx, fenceTriggerSQL(schema, q)); err != nil {
			return fmt.Errorf("arm placement fence on %s: %w", q, err)
		}
	}
	return nil
}

// unfenceTables drops the fence from every named table that still has it.
func unfenceTables(ctx context.Context, conn ShardConn, qualified ...string) error {
	for _, q := range qualified {
		switch there, err := present(ctx, conn, q); {
		case err != nil:
			return fmt.Errorf("look for %s: %w", q, err)
		case !there:
			continue
		}
		if _, err := conn.Exec(ctx, unfenceTriggerSQL(q)); err != nil {
			return fmt.Errorf("release placement fence on %s: %w", q, err)
		}
	}
	return nil
}
