package operator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgsequence"
)

// CatalogUpgradePublication and CatalogUpgradeSubscription name the
// logical replication objects of a catalog group upgrade.
const (
	CatalogUpgradePublication  = "pgshard_catalog_upgrade"
	CatalogUpgradeSubscription = "pgshard_catalog_upgrade"
	// CatalogRollbackPublication and CatalogRollbackSubscription name the
	// reverse pair, new catalog back to old, that keeps a rollback from
	// losing everything written after the cutover.
	CatalogRollbackPublication  = "pgshard_catalog_rollback"
	CatalogRollbackSubscription = "pgshard_catalog_rollback"
	// catalogFenceTimeout bounds a drain in either direction.
	catalogFenceTimeout = 2 * time.Minute
)

// fenceCatalog makes conn's database refuse writes and disconnects the
// clients that were writing. A re-run connects to an already fenced server
// whose default is read-only, so the fence is lifted session-locally first
// or ALTER SYSTEM is refused.
func fenceCatalog(ctx context.Context, conn *pgx.Conn) error {
	for _, q := range []string{
		`SET default_transaction_read_only = off`,
		`ALTER SYSTEM SET default_transaction_read_only = on`,
		`SELECT pg_reload_conf()`,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			WHERE pid <> pg_backend_pid() AND backend_type = 'client backend'`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// unfenceCatalog lets the database accept writes again.
func unfenceCatalog(ctx context.Context, conn *pgx.Conn) error {
	for _, q := range []string{
		`SET default_transaction_read_only = off`,
		`ALTER SYSTEM RESET default_transaction_read_only`,
		`SELECT pg_reload_conf()`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// drainSlot waits until slot on conn has confirmed everything written up to
// lsn, so the subscriber that reads it has applied the whole fenced history.
func drainSlot(ctx context.Context, conn *pgx.Conn, slot, lsn string, now func() time.Time) error {
	deadline := now().Add(catalogFenceTimeout)
	for {
		var drained bool
		if err := conn.QueryRow(ctx, `SELECT confirmed_flush_lsn >= $2::pg_lsn FROM pg_replication_slots WHERE slot_name = $1`,
			slot, lsn).Scan(&drained); err != nil {
			return fmt.Errorf("drain %s: %w", slot, err)
		}
		if drained {
			return nil
		}
		if now().After(deadline) {
			return fmt.Errorf("%s did not drain to %s in time", slot, lsn)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// carrySequences copies every sequence position from one catalog to the
// other. Logical replication does not replicate sequences, so whichever
// side is about to serve has to be told where they got to. The catalog's
// own pgshard schema is carried too: its sequences are catalog data.
func carrySequences(ctx context.Context, from, to *pgx.Conn) error {
	values, err := pgsequence.Snapshot(ctx, from, nil)
	if err != nil {
		return err
	}
	return pgsequence.Apply(ctx, to, values)
}

// ServerMajor implements Prober.
func (PgxProber) ServerMajor(ctx context.Context, dsn string) (int, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var num int
	if err := conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&num); err != nil {
		return 0, err
	}
	return num / 10000, nil
}

// CatalogSide is one side of a catalog upgrade: the DSN the operator dials
// it with, and the conninfo the other side's apply worker dials it with.
//
// In a cluster those are the same in-cluster Service DSN, which is why the
// two uses were one string. They are not the same thing, and nowhere else
// can they coincide: a test process reaches a container through a published
// 127.0.0.1 port while the peer container reaches it by name on a docker
// network, and no single address works from both. internal/controller has
// the same split as Copier.SourceConnInfo.
type CatalogSide struct {
	// DSN is what the operator connects with.
	DSN string
	// ConnInfo is what goes into CREATE SUBSCRIPTION on the other side.
	// Empty means the DSN, which is what a cluster wants.
	ConnInfo string
}

func (s CatalogSide) connInfo() string {
	if s.ConnInfo != "" {
		return s.ConnInfo
	}
	return s.DSN
}

// EnsureCatalogCopy implements Prober.
func (p PgxProber) EnsureCatalogCopy(ctx context.Context, source, target CatalogSide) error {
	src, err := pgx.Connect(ctx, source.DSN)
	if err != nil {
		return fmt.Errorf("catalog source: %w", err)
	}
	defer func() { _ = src.Close(ctx) }()
	var pubExists bool
	if err := src.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, CatalogUpgradePublication).Scan(&pubExists); err != nil {
		return err
	}
	if !pubExists {
		if _, err := src.Exec(ctx, `CREATE PUBLICATION `+CatalogUpgradePublication+` FOR TABLES IN SCHEMA pgshard`); err != nil {
			return fmt.Errorf("create publication: %w", err)
		}
	}
	tgt, err := pgx.Connect(ctx, target.DSN)
	if err != nil {
		return fmt.Errorf("catalog target: %w", err)
	}
	defer func() { _ = tgt.Close(ctx) }()
	var subExists bool
	if err := tgt.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)`, CatalogUpgradeSubscription).Scan(&subExists); err != nil {
		return err
	}
	if subExists {
		return nil
	}
	if err := clearCatalogSchema(ctx, tgt, "pgshard"); err != nil {
		return err
	}
	if _, err := tgt.Exec(ctx, fmt.Sprintf(`CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (copy_data = true, streaming = parallel)`,
		CatalogUpgradeSubscription, quoteLiteral(source.connInfo()), CatalogUpgradePublication)); err != nil {
		return fmt.Errorf("create subscription: %w", err)
	}
	return nil
}

// mapTablesInDeleteOrder are the tables that refuse TRUNCATE, children
// first. Row-level constraint triggers never fire on TRUNCATE, so emptying
// the shard map that way would go past the coverage, numbering and
// workflow-ownership checks that guard it -- a refusal the catalog is
// right to make, and one a freshly migrated target trips over, because it
// carries a bootstrap shard set row that the copy has to clear first.
// DELETE fires those checks and they are satisfied by an empty map.
// The names are as quote_ident renders them, which is unquoted for these:
// the same form schemaTables and truncateGuardedTables return, so they
// compare.
var mapTablesInDeleteOrder = []string{"pgshard.shard_ranges", "pgshard.shard_sets"}

// clearCatalogSchema empties the schema so the copy can be re-run: TRUNCATE
// where it is allowed, DELETE for the shard map. A table that refuses
// TRUNCATE and is not one this knows how to delete is an error rather than
// a silent skip -- the copy would then start against rows it did not
// clear.
func clearCatalogSchema(ctx context.Context, conn *pgx.Conn, schema string) error {
	all, err := schemaTables(ctx, conn, schema)
	if err != nil {
		return err
	}
	refusing, err := truncateGuardedTables(ctx, conn, schema)
	if err != nil {
		return err
	}
	for _, t := range refusing {
		if !slices.Contains(mapTablesInDeleteOrder, t) {
			return fmt.Errorf("catalog target: %s refuses TRUNCATE and the copy does not know how to clear it", t)
		}
	}
	for _, t := range mapTablesInDeleteOrder {
		if !slices.Contains(all, t) {
			continue
		}
		if _, err := conn.Exec(ctx, `DELETE FROM `+t); err != nil {
			return fmt.Errorf("clear catalog target %s: %w", t, err)
		}
	}
	var truncate []string
	for _, t := range all {
		if !slices.Contains(refusing, t) {
			truncate = append(truncate, t)
		}
	}
	if len(truncate) > 0 {
		if _, err := conn.Exec(ctx, `TRUNCATE `+strings.Join(truncate, ", ")+` RESTART IDENTITY CASCADE`); err != nil {
			return fmt.Errorf("truncate catalog target: %w", err)
		}
	}
	return nil
}

// truncateGuardedTables lists the schema's tables carrying a user BEFORE
// TRUNCATE trigger, so the set is read from the catalog rather than
// remembered here.
func truncateGuardedTables(ctx context.Context, conn *pgx.Conn, schema string) ([]string, error) {
	rows, err := conn.Query(ctx, `SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname)
		FROM pg_trigger g JOIN pg_class c ON c.oid = g.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND NOT g.tgisinternal AND (g.tgtype & 32) <> 0
		GROUP BY 1 ORDER BY 1`, schema)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func schemaTables(ctx context.Context, conn *pgx.Conn, schema string) ([]string, error) {
	rows, err := conn.Query(ctx, `SELECT quote_ident(schemaname) || '.' || quote_ident(tablename) FROM pg_tables WHERE schemaname = $1 ORDER BY 1`, schema)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// CatalogCopyCaughtUp implements Prober: the subscription's slot on the
// source must have confirmed the source's current WAL insert position.
func (PgxProber) CatalogCopyCaughtUp(ctx context.Context, srcDSN string) (bool, string, error) {
	conn, err := pgx.Connect(ctx, srcDSN)
	if err != nil {
		return false, "", err
	}
	defer func() { _ = conn.Close(ctx) }()
	var syncing int
	var lag *int64
	err = conn.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE $1 || '%' AND slot_name <> $1),
			(SELECT (pg_current_wal_lsn() - confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_name = $1)`,
		CatalogUpgradeSubscription).Scan(&syncing, &lag)
	if err != nil {
		return false, "", err
	}
	if lag == nil {
		return false, "subscription slot missing on the source", nil
	}
	if syncing > 0 {
		return false, fmt.Sprintf("%d table sync worker(s) still copying", syncing), nil
	}
	if *lag > 0 {
		return false, fmt.Sprintf("%d bytes of WAL behind", *lag), nil
	}
	return true, "", nil
}

// catalogCutoverResume decides what a (re)run of the catalog cutover still
// has to do: with the slot on the source and the subscription on the
// target the drain proceeds; with both gone a previous run finished and
// the cutover is a no-op; anything in between is broken state to surface.
func catalogCutoverResume(slotOnSource, subOnTarget bool) (alreadyCutOver bool, err error) {
	switch {
	case slotOnSource && subOnTarget:
		return false, nil
	case !slotOnSource && !subOnTarget:
		return true, nil
	case !slotOnSource:
		return false, fmt.Errorf("catalog subscription present on the target but its slot is gone on the source")
	default:
		return false, fmt.Errorf("catalog slot present on the source but the subscription is gone on the target")
	}
}

// CutoverCatalog implements Prober.
func (p PgxProber) CutoverCatalog(ctx context.Context, source, target CatalogSide) error {
	src, err := pgx.Connect(ctx, source.DSN)
	if err != nil {
		return fmt.Errorf("catalog source: %w", err)
	}
	defer func() { _ = src.Close(ctx) }()
	if err := fenceCatalog(ctx, src); err != nil {
		return fmt.Errorf("fence catalog source: %w", err)
	}
	tgt, err := pgx.Connect(ctx, target.DSN)
	if err != nil {
		return fmt.Errorf("catalog target: %w", err)
	}
	defer func() { _ = tgt.Close(ctx) }()
	var slotOnSource, subOnTarget bool
	if err := src.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, CatalogUpgradeSubscription).Scan(&slotOnSource); err != nil {
		return err
	}
	if err := tgt.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)`, CatalogUpgradeSubscription).Scan(&subOnTarget); err != nil {
		return err
	}
	alreadyCutOver, err := catalogCutoverResume(slotOnSource, subOnTarget)
	if err != nil {
		return err
	}
	if alreadyCutOver {
		// The previous run dropped the subscription (and its slot) after
		// carrying the sequences: nothing to drain, and the live target's
		// sequences must not be rewound to the fenced source's values. The
		// reverse pair still has to be armed - a run that died between the
		// two would otherwise leave no way back, which is the data loss
		// this whole path exists to prevent.
		return ensureCatalogRollback(ctx, src, tgt, target.connInfo())
	}
	var fence string
	if err := src.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&fence); err != nil {
		return err
	}
	if err := drainSlot(ctx, src, CatalogUpgradeSubscription, fence, time.Now); err != nil {
		return err
	}
	if err := carryShardMapGeneration(ctx, src, tgt); err != nil {
		return err
	}
	if err := carryHashVersions(ctx, src, tgt); err != nil {
		return err
	}
	if err := catalogTargetIsReadable(ctx, tgt); err != nil {
		return err
	}
	if err := carrySequences(ctx, src, tgt); err != nil {
		return err
	}
	if _, err := tgt.Exec(ctx, `DROP SUBSCRIPTION IF EXISTS `+CatalogUpgradeSubscription); err != nil {
		return fmt.Errorf("drop subscription: %w", err)
	}
	// Arm the reverse pair before the new catalog serves a single write. It
	// stays disabled while the old group is fenced - an apply worker cannot
	// write to a read-only database - but its slot is created now, so a
	// rollback replays everything the new catalog accepted rather than
	// throwing it away.
	if err := ensureCatalogRollback(ctx, src, tgt, target.connInfo()); err != nil {
		return err
	}
	return nil
}

// carryShardMapGeneration restores the target's shard_map_generation row.
//
// The row is a singleton, and the ONLY thing that ever inserts one is the
// schema migration (0003_status.sql). Everything afterwards is an UPDATE,
// which quietly affects nothing when the row is gone. EnsureCatalogCopy
// clears the target's whole pgshard schema before the copy, so between the
// clear and the copy delivering it the target has no generation at all --
// and if the copy never delivers it, nothing else ever will.
//
// Carried explicitly for the same reason sequences are: it is catalog state
// the copy is not guaranteed to bring across, and the new catalog cannot
// serve without it. A router that cannot read the generation refuses to
// plan and leaves the Service, so an unrepaired target means every router
// stops, not one.
//
// Repair, not overwrite: a target that already has its row keeps it,
// including the write-fence columns that share it. Only a missing row is
// filled, and it is filled from the source, which is fenced by now and
// therefore final.
func carryShardMapGeneration(ctx context.Context, src, tgt *pgx.Conn) error {
	var generation int64
	var fence bool
	var reason, owner string
	if err := src.QueryRow(ctx,
		`SELECT generation, write_fence, write_fence_reason, write_fence_owner FROM pgshard.shard_map_generation`,
	).Scan(&generation, &fence, &reason, &owner); err != nil {
		return fmt.Errorf("read shard map generation from the catalog source: %w", err)
	}
	if _, err := tgt.Exec(ctx,
		`INSERT INTO pgshard.shard_map_generation (singleton, generation, write_fence, write_fence_reason, write_fence_owner)
		 VALUES (true, $1, $2, $3, $4) ON CONFLICT (singleton) DO NOTHING`,
		generation, fence, reason, owner); err != nil {
		return fmt.Errorf("restore shard map generation on the catalog target: %w", err)
	}
	return nil
}

// carryHashVersions restores the target's hash_versions rows, for the same
// reason carryShardMapGeneration restores the generation: the rows are
// seeded by the schema migration (0001_roles_and_schema.sql) and
// clearCatalogSchema TRUNCATEs them off the target before the copy.
//
// This one bites later rather than immediately. pgshard.tables.hash_version
// is a FOREIGN KEY onto it, so a new catalog missing the row does not fail
// on its own -- it fails the next time anybody declares a sharded table,
// which is long after the upgrade looked successful.
//
// Repair, not overwrite: rows the target already has are kept, and a
// version the source does not know about is left alone rather than deleted.
func carryHashVersions(ctx context.Context, src, tgt *pgx.Conn) error {
	rows, err := src.Query(ctx, `SELECT version, description FROM pgshard.hash_versions ORDER BY version`)
	if err != nil {
		return fmt.Errorf("read hash versions from the catalog source: %w", err)
	}
	type hashVersion struct {
		Version     int32
		Description string
	}
	versions, err := pgx.CollectRows(rows, pgx.RowToStructByPos[hashVersion])
	if err != nil {
		return fmt.Errorf("read hash versions from the catalog source: %w", err)
	}
	for _, v := range versions {
		if _, err := tgt.Exec(ctx,
			`INSERT INTO pgshard.hash_versions (version, description) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
			v.Version, v.Description); err != nil {
			return fmt.Errorf("restore hash version %d on the catalog target: %w", v.Version, err)
		}
	}
	return nil
}

// catalogTargetIsReadable refuses a cutover onto a catalog a router could
// not read.
//
// The gate before this one, CatalogCopyCaughtUp, asks only the SOURCE:
// no table-sync workers left and zero WAL lag. Both can be true while the
// target holds nothing, because EnsureCatalogCopy clears the target's whole
// pgshard schema before the copy and the copy is what puts it back. If it
// did not, the flip strands every router: shard_map_generation is a
// singleton, an empty one makes catalog.Generations return
// ErrNoShardMapGeneration, and a router that cannot read the generation
// refuses to plan and leaves the Service. Observed exactly that way -- all
// three routers unable to load a snapshot, writes stopped, while the
// cluster reported CatalogReady.
//
// Run after the drain, so everything the fenced source held has been
// applied and a missing row means the copy is broken rather than late.
func catalogTargetIsReadable(ctx context.Context, tgt *pgx.Conn) error {
	if _, _, err := catalog.Generations(ctx, tgt); err != nil {
		return fmt.Errorf("catalog target is not readable, so the cutover onto it is refused: %w", err)
	}
	return nil
}

// ensureCatalogRollback publishes the new catalog and subscribes the old one
// to it, disabled. currentConninfo is the new catalog's DSN as the old
// group's pod resolves it, because the old group is the subscriber.
func ensureCatalogRollback(ctx context.Context, old, current *pgx.Conn, currentConninfo string) error {
	var pubExists bool
	if err := current.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, CatalogRollbackPublication).Scan(&pubExists); err != nil {
		return err
	}
	if !pubExists {
		if _, err := current.Exec(ctx, `CREATE PUBLICATION `+CatalogRollbackPublication+` FOR TABLES IN SCHEMA pgshard`); err != nil {
			return fmt.Errorf("create rollback publication: %w", err)
		}
	}
	var subExists bool
	if err := old.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)`, CatalogRollbackSubscription).Scan(&subExists); err != nil {
		return err
	}
	if subExists {
		return nil
	}
	// The old catalog is fenced read-only; DDL needs the session default off.
	if _, err := old.Exec(ctx, `SET default_transaction_read_only = off`); err != nil {
		return err
	}
	if _, err := old.Exec(ctx, fmt.Sprintf(
		`CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (copy_data = false, enabled = false, origin = none, two_phase = false, failover = true)`,
		CatalogRollbackSubscription, quoteLiteral(currentConninfo), CatalogRollbackPublication)); err != nil {
		return fmt.Errorf("create rollback subscription: %w", err)
	}
	return nil
}

// RollbackCatalog implements Prober. It reverses a cutover without losing
// what the new catalog accepted: the new group is fenced, the reverse
// subscription replays every write it took since the cutover into the old
// group, sequences are carried back, and only then does the old group start
// serving again. Idempotent: a re-run finds the subscription already gone
// and just makes sure the old group accepts writes.
func (PgxProber) RollbackCatalog(ctx context.Context, oldDSN, newDSN string) error {
	old, err := pgx.Connect(ctx, oldDSN)
	if err != nil {
		return fmt.Errorf("old catalog: %w", err)
	}
	defer func() { _ = old.Close(ctx) }()
	var subExists bool
	if err := old.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)`, CatalogRollbackSubscription).Scan(&subExists); err != nil {
		return err
	}
	current, err := pgx.Connect(ctx, newDSN)
	if err != nil {
		return fmt.Errorf("new catalog: %w", err)
	}
	defer func() { _ = current.Close(ctx) }()
	if !subExists {
		// Two different states look the same from the old group. Dropping
		// the subscription is the last thing a finished replay does, and it
		// leaves the publication behind, so the publication is what tells
		// them apart: with it, this is a re-run after the replay completed;
		// without it, the cutover predates the reverse pair and there is no
		// way to recover what the new catalog took. Refuse rather than
		// serve a catalog that is missing writes.
		var pubExists bool
		if err := current.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, CatalogRollbackPublication).Scan(&pubExists); err != nil {
			return err
		}
		if !pubExists {
			return fmt.Errorf("this catalog was cut over without a rollback stream, so everything written since cannot be recovered; finish the upgrade instead")
		}
		return unfenceCatalog(ctx, old)
	}
	// A slot the serving catalog invalidated can never be drained, and
	// fencing before finding that out would leave the catalog read-only
	// with no way forward.
	var walStatus string
	if err := current.QueryRow(ctx, `SELECT coalesce(wal_status, '') FROM pg_replication_slots WHERE slot_name = $1`, CatalogRollbackSubscription).Scan(&walStatus); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if walStatus == "lost" {
		return fmt.Errorf("the rollback stream was invalidated (slot %s lost its WAL), so the old catalog cannot be caught up; finish the upgrade instead", CatalogRollbackSubscription)
	}
	if err := fenceCatalog(ctx, current); err != nil {
		return fmt.Errorf("fence new catalog: %w", err)
	}
	var fence string
	if err := current.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&fence); err != nil {
		return err
	}
	// The apply worker writes, so the old group cannot stay read-only. It
	// is not serving yet - the endpoint still points at the new group - so
	// nothing else can write to it in the meantime.
	if err := unfenceCatalog(ctx, old); err != nil {
		return fmt.Errorf("unfence old catalog: %w", err)
	}
	if _, err := old.Exec(ctx, `ALTER SUBSCRIPTION `+CatalogRollbackSubscription+` ENABLE`); err != nil {
		return fmt.Errorf("enable rollback subscription: %w", err)
	}
	if err := drainSlot(ctx, current, CatalogRollbackSubscription, fence, time.Now); err != nil {
		return err
	}
	if err := carrySequences(ctx, current, old); err != nil {
		return err
	}
	if _, err := old.Exec(ctx, `DROP SUBSCRIPTION IF EXISTS `+CatalogRollbackSubscription); err != nil {
		return fmt.Errorf("drop rollback subscription: %w", err)
	}
	return nil
}

// DropCatalogRollback implements Prober. Retiring the old group leaves the
// reverse slot behind on the catalog that is now serving, and an unused
// slot pins WAL forever, so it is dropped when the rollback window closes.
func (PgxProber) DropCatalogRollback(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	var active bool
	err = conn.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name = $1`, CatalogRollbackSubscription).Scan(&active)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Already gone.
	case err != nil:
		return err
	case active:
		// A rollback is streaming from it. Say so and come back rather than
		// dropping the publication out from under the walsender.
		return fmt.Errorf("the rollback stream is still in use")
	default:
		if _, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, CatalogRollbackSubscription); err != nil {
			return fmt.Errorf("drop rollback slot: %w", err)
		}
	}
	if _, err := conn.Exec(ctx, `DROP PUBLICATION IF EXISTS `+CatalogRollbackPublication); err != nil {
		return fmt.Errorf("drop rollback publication: %w", err)
	}
	return nil
}

// DisableCatalogRollback implements Prober. An abandoned rollback leaves
// the reverse subscription applying into a group that is no longer going to
// serve; disabling it stops that and releases the slot on the live catalog.
func (PgxProber) DisableCatalogRollback(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)`, CatalogRollbackSubscription).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if _, err := conn.Exec(ctx, `SET default_transaction_read_only = off`); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `ALTER SUBSCRIPTION `+CatalogRollbackSubscription+` DISABLE`)
	return err
}

// ReleaseCatalog implements Prober.
func (PgxProber) ReleaseCatalog(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	// The session inherits the fence's read-only default; lift it locally
	// so ALTER SYSTEM is allowed.
	if _, err := conn.Exec(ctx, `SET default_transaction_read_only = off`); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `ALTER SYSTEM RESET default_transaction_read_only`); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `SELECT pg_reload_conf()`)
	return err
}
