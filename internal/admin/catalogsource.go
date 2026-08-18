package admin

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
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
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	set := p.ShardSet
	if set == "" {
		set = DefaultShardSet
	}
	return catalog.ListShardStatus(ctx, conn, set)
}
