package snapshot

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestLoadRefusesACatalogFromANewerBinary.
//
// CheckCompatible existed for exactly this and had ONE caller -- Migrate --
// which only the operator's probe and the router's dev bootstrap reach. So a
// router, pooler or controller still on the previous release read a catalog
// the operator had already migrated and failed somewhere deep in a query
// with a raw "column does not exist", naming neither the version gap nor the
// component that opened it.
//
// The window is not rare: the operator migrates the catalog and THEN rolls
// the components, so every upgrade has one.
func TestLoadRefusesACatalogFromANewerBinary(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}

	// A load of the catalog this binary knows must work, or the assertion
	// below would pass for the wrong reason.
	if _, err := Load(ctx, conn); err != nil {
		t.Fatalf("a catalog this binary migrated must load: %v", err)
	}

	// A newer binary has since migrated it: a version this one has never
	// heard of is recorded.
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.schema_migrations (version, checksum) VALUES (999999, 'from-a-newer-binary')`); err != nil {
		t.Fatal(err)
	}
	_, err = Load(ctx, conn)
	if err == nil {
		t.Fatal("a catalog migrated by a newer binary was loaded; the skew stays silent until some query fails on a column that is not there")
	}
	if !strings.Contains(err.Error(), "999999") {
		t.Fatalf("the refusal must name the version gap, or it is no better than the raw error it replaces: %v", err)
	}
}
