package router

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// DevBootstrap prepares a single-shard development stack: it migrates the
// catalog, registers a database, a role with a SCRAM verifier and the shard's
// serving status, and creates the same role and database on the shard.
// It is idempotent. Not for production: the operator owns these records.
type DevBootstrap struct {
	CatalogDSN string
	// ShardDSN is a superuser DSN of shard 0 of the default shard set.
	ShardDSN string
	Database string
	Role     string
	Password string
	// PoolerEndpoint is published as shard_status.primary_endpoint.
	PoolerEndpoint string
	Epoch          int64
}

// Run applies the bootstrap.
func (b DevBootstrap) Run(ctx context.Context) error {
	if b.Database == "" || b.Role == "" || b.Password == "" {
		return errors.New("router: dev bootstrap needs Database, Role and Password")
	}
	verifier, err := pgwire.BuildSCRAMVerifier(b.Password, nil, pgwire.DefaultSCRAMIterations)
	if err != nil {
		return err
	}
	cat, err := pgx.Connect(ctx, b.CatalogDSN)
	if err != nil {
		return fmt.Errorf("router: catalog: %w", err)
	}
	defer func() { _ = cat.Close(ctx) }()
	if err := catalog.Migrate(ctx, cat); err != nil {
		return err
	}
	stmts := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO pgshard.databases (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, []any{b.Database}},
		{`INSERT INTO pgshard.roles (rolname, verifier) VALUES ($1, $2)
		  ON CONFLICT (rolname) DO UPDATE SET verifier = EXCLUDED.verifier, updated_at = now()`, []any{b.Role, verifier.String()}},
		{`INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint)
		  VALUES ($1, 0, 'shard0', 'serving', $2, $3)
		  ON CONFLICT (shard_set, shard_id) DO UPDATE SET primary_epoch = EXCLUDED.primary_epoch,
		    primary_endpoint = EXCLUDED.primary_endpoint, updated_at = now()`, []any{DefaultShardSet, b.Epoch, b.PoolerEndpoint}},
	}
	for _, s := range stmts {
		if _, err := cat.Exec(ctx, s.sql, s.args...); err != nil {
			return fmt.Errorf("router: catalog bootstrap: %w", err)
		}
	}

	shard, err := pgx.Connect(ctx, b.ShardDSN)
	if err != nil {
		return fmt.Errorf("router: shard: %w", err)
	}
	defer func() { _ = shard.Close(ctx) }()
	role, db := pgx.Identifier{b.Role}.Sanitize(), pgx.Identifier{b.Database}.Sanitize()
	var exists bool
	if err := shard.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, b.Role).Scan(&exists); err != nil {
		return err
	}
	verb := "CREATE ROLE " + role + " LOGIN PASSWORD "
	if exists {
		verb = "ALTER ROLE " + role + " LOGIN PASSWORD "
	}
	if _, err := shard.Exec(ctx, verb+quoteLiteral(verifier.String())); err != nil {
		return fmt.Errorf("router: shard role: %w", err)
	}
	if err := shard.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, b.Database).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err := shard.Exec(ctx, "CREATE DATABASE "+db+" OWNER "+role); err != nil {
			return fmt.Errorf("router: shard database: %w", err)
		}
	}
	cfg, err := pgx.ParseConfig(b.ShardDSN)
	if err != nil {
		return err
	}
	cfg.Database = b.Database
	app, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("router: shard database: %w", err)
	}
	defer func() { _ = app.Close(ctx) }()
	if _, err := app.Exec(ctx, "GRANT ALL ON SCHEMA public TO "+role); err != nil {
		return fmt.Errorf("router: shard grants: %w", err)
	}
	return nil
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
