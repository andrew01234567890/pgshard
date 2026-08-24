package operator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// CatalogUpgradePublication and CatalogUpgradeSubscription name the
// logical replication objects of a catalog group upgrade.
const (
	CatalogUpgradePublication  = "pgshard_catalog_upgrade"
	CatalogUpgradeSubscription = "pgshard_catalog_upgrade"
)

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

// EnsureCatalogCopy implements Prober. The subscription's conninfo is the
// source DSN as the target pod resolves it (a Service name), so it must be
// the in-cluster form.
func (p PgxProber) EnsureCatalogCopy(ctx context.Context, srcDSN, tgtDSN string) error {
	src, err := pgx.Connect(ctx, srcDSN)
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
	tgt, err := pgx.Connect(ctx, tgtDSN)
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
	tables, err := schemaTables(ctx, tgt, "pgshard")
	if err != nil {
		return err
	}
	if len(tables) > 0 {
		if _, err := tgt.Exec(ctx, `TRUNCATE `+strings.Join(tables, ", ")+` RESTART IDENTITY CASCADE`); err != nil {
			return fmt.Errorf("truncate catalog target: %w", err)
		}
	}
	if _, err := tgt.Exec(ctx, fmt.Sprintf(`CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (copy_data = true, streaming = parallel)`,
		CatalogUpgradeSubscription, quoteLiteral(srcDSN), CatalogUpgradePublication)); err != nil {
		return fmt.Errorf("create subscription: %w", err)
	}
	return nil
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

// CutoverCatalog implements Prober.
func (p PgxProber) CutoverCatalog(ctx context.Context, srcDSN, tgtDSN string) error {
	src, err := pgx.Connect(ctx, srcDSN)
	if err != nil {
		return fmt.Errorf("catalog source: %w", err)
	}
	defer func() { _ = src.Close(ctx) }()
	for _, q := range []string{
		`ALTER SYSTEM SET default_transaction_read_only = on`,
		`SELECT pg_reload_conf()`,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			WHERE pid <> pg_backend_pid() AND backend_type = 'client backend'`,
	} {
		if _, err := src.Exec(ctx, q); err != nil {
			return fmt.Errorf("fence catalog source: %w", err)
		}
	}
	var fence string
	if err := src.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&fence); err != nil {
		return err
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var drained bool
		if err := src.QueryRow(ctx, `SELECT confirmed_flush_lsn >= $2::pg_lsn FROM pg_replication_slots WHERE slot_name = $1`,
			CatalogUpgradeSubscription, fence).Scan(&drained); err != nil {
			return fmt.Errorf("catalog drain: %w", err)
		}
		if drained {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("catalog subscription did not drain to %s in time", fence)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	sequences := map[string]int64{}
	rows, err := src.Query(ctx, `SELECT quote_ident(schemaname) || '.' || quote_ident(sequencename), last_value
		FROM pg_sequences WHERE last_value IS NOT NULL`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			rows.Close()
			return err
		}
		sequences[name] = v
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	tgt, err := pgx.Connect(ctx, tgtDSN)
	if err != nil {
		return fmt.Errorf("catalog target: %w", err)
	}
	defer func() { _ = tgt.Close(ctx) }()
	for name, v := range sequences {
		if _, err := tgt.Exec(ctx, `SELECT pg_catalog.setval(oid, $2, true) FROM pg_class WHERE oid = to_regclass($1) AND relkind = 'S'`, name, v); err != nil {
			return fmt.Errorf("carry sequence %s: %w", name, err)
		}
	}
	if _, err := tgt.Exec(ctx, `DROP SUBSCRIPTION IF EXISTS `+CatalogUpgradeSubscription); err != nil {
		return fmt.Errorf("drop subscription: %w", err)
	}
	return nil
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
