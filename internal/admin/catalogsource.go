package admin

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/controller"
)

// DefaultShardSet is the shard set the topology page shows from the catalog.
const DefaultShardSet = "default"

// PgxCatalog reads the shard status snapshot over a fresh connection per call.
type PgxCatalog struct {
	DSN      string
	ShardSet string
	Timeout  time.Duration
}

// ShardStatus implements CatalogSource.
func (p PgxCatalog) ShardStatus(ctx context.Context) ([]catalog.ShardStatus, error) {
	set := p.ShardSet
	if set == "" {
		set = DefaultShardSet
	}
	return withConn(ctx, p, func(ctx context.Context, conn *pgx.Conn) ([]catalog.ShardStatus, error) {
		return catalog.ListShardStatus(ctx, conn, set)
	})
}

// RestorePoints implements CatalogSource.
func (p PgxCatalog) RestorePoints(ctx context.Context) ([]controller.RestorePoint, error) {
	return withConn(ctx, p, func(ctx context.Context, conn *pgx.Conn) ([]controller.RestorePoint, error) {
		return controller.ListRestorePoints(ctx, conn, true)
	})
}

func withConn[T any](ctx context.Context, p PgxCatalog, fn func(context.Context, *pgx.Conn) (T, error)) (T, error) {
	var zero T
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		return zero, err
	}
	defer func() { _ = conn.Close(ctx) }()
	return fn(ctx, conn)
}

// ListMigrations implements MigrationSource.
func (p PgxCatalog) ListMigrations(ctx context.Context, f catalog.MigrationFilter) ([]catalog.DDLMigration, int, error) {
	type page struct {
		rows  []catalog.DDLMigration
		total int
	}
	out, err := withConn(ctx, p, func(ctx context.Context, conn *pgx.Conn) (page, error) {
		rows, total, err := catalog.ListMigrations(ctx, conn, f)
		return page{rows, total}, err
	})
	return out.rows, out.total, err
}

// LoadMigration implements MigrationSource.
func (p PgxCatalog) LoadMigration(ctx context.Context, id string) (catalog.DDLMigration, error) {
	return withConn(ctx, p, func(ctx context.Context, conn *pgx.Conn) (catalog.DDLMigration, error) {
		return catalog.LoadMigration(ctx, conn, id)
	})
}

// CountMigrations implements MigrationSource.
func (p PgxCatalog) CountMigrations(ctx context.Context) (catalog.MigrationCounts, error) {
	return withConn(ctx, p, func(ctx context.Context, conn *pgx.Conn) (catalog.MigrationCounts, error) {
		return catalog.CountMigrations(ctx, conn)
	})
}
