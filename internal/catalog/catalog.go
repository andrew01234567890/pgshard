// Package catalog defines the pgshard control-plane schema and a small typed
// API over it.
package catalog

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Schema is the PostgreSQL schema that holds every catalog object.
const Schema = "pgshard"

// DesiredChannel is the NOTIFY channel fired after every statement that
// changes a desired-state table. The payload is "<table>:<generation>".
const DesiredChannel = "pgshard_desired"

// ServingChannel is the NOTIFY channel fired after every statement that
// changes shard_status, table_status or shard_map_generation. The payload is
// the table name.
const ServingChannel = "pgshard_serving"

// Role names created by the first migration.
const (
	RoleSystem = "pgshard_system"
	RoleAdmin  = "pgshard_admin"
	RoleReader = "pgshard_reader"
)

//go:embed schema/*.sql
var schemaFS embed.FS

// ErrChecksumMismatch is returned when an applied migration's embedded text
// no longer matches the checksum recorded in the database.
var ErrChecksumMismatch = errors.New("catalog: applied migration checksum changed")

// Migration is one embedded schema file.
type Migration struct {
	Version  int
	Name     string
	SQL      string
	Checksum string
}

// Migrations returns the embedded migrations in version order.
func Migrations() ([]Migration, error) {
	entries, err := fs.ReadDir(schemaFS, "schema")
	if err != nil {
		return nil, err
	}
	out := make([]Migration, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		var version int
		if _, err := fmt.Sscanf(name, "%04d_", &version); err != nil || version <= 0 {
			return nil, fmt.Errorf("catalog: bad migration file name %q", name)
		}
		body, err := schemaFS.ReadFile("schema/" + name)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(body)
		out = append(out, Migration{
			Version:  version,
			Name:     strings.TrimSuffix(name, ".sql"),
			SQL:      string(body),
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i := range out {
		if out[i].Version != i+1 {
			return nil, fmt.Errorf("catalog: migration versions are not contiguous at %d", out[i].Version)
		}
	}
	return out, nil
}

// Migrate brings the catalog schema up to date. It is idempotent: already
// applied migrations are verified against their recorded checksum and skipped,
// and every pending migration runs in its own transaction.
func Migrate(ctx context.Context, conn *pgx.Conn) error {
	migrations, err := Migrations()
	if err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS pgshard;
		CREATE TABLE IF NOT EXISTS pgshard.schema_migrations (
			version    integer     PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now(),
			checksum   text        NOT NULL
		)`); err != nil {
		return fmt.Errorf("catalog: prepare schema_migrations: %w", err)
	}
	for _, m := range migrations {
		if err := applyMigration(ctx, conn, m); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, conn *pgx.Conn, m Migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `LOCK TABLE pgshard.schema_migrations IN EXCLUSIVE MODE`); err != nil {
		return err
	}
	var recorded string
	err = tx.QueryRow(ctx, `SELECT checksum FROM pgshard.schema_migrations WHERE version = $1`, m.Version).Scan(&recorded)
	switch {
	case err == nil:
		if recorded != m.Checksum {
			return fmt.Errorf("%w: version %d (%s)", ErrChecksumMismatch, m.Version, m.Name)
		}
		return nil
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return err
	}
	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return fmt.Errorf("catalog: migration %d (%s): %w", m.Version, m.Name, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO pgshard.schema_migrations (version, checksum) VALUES ($1, $2)`,
		m.Version, m.Checksum); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
