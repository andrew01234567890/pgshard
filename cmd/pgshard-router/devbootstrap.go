package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/andrew01234567890/pgshard/internal/cli"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// runDevBootstrap implements `pgshard-router dev-bootstrap`: it prepares a
// catalog and one shard for a local single-shard stack. The role password is
// read from PGSHARD_DEV_PASSWORD so it never appears in process listings.
func runDevBootstrap(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pgshard-router dev-bootstrap", flag.ContinueOnError)
	fs.SetOutput(stderr)
	b := router.DevBootstrap{Password: os.Getenv("PGSHARD_DEV_PASSWORD")}
	fs.StringVar(&b.CatalogDSN, "catalog-dsn", "", "superuser DSN of the catalog")
	fs.StringVar(&b.ShardDSN, "shard-dsn", "", "superuser DSN of shard 0")
	fs.StringVar(&b.Database, "database", "app", "database to register and create")
	fs.StringVar(&b.Role, "role", "app", "login role to register and create")
	fs.StringVar(&b.PoolerEndpoint, "pooler-endpoint", "", "shard 0 pooler address published in shard_status")
	fs.Int64Var(&b.Epoch, "epoch", 1, "primary epoch to publish for shard 0")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if fs.NArg() != 0 || b.CatalogDSN == "" || b.ShardDSN == "" || b.PoolerEndpoint == "" {
		fmt.Fprintln(stderr, "pgshard-router dev-bootstrap: --catalog-dsn, --shard-dsn and --pooler-endpoint are required")
		return cli.ExitUsage
	}
	if b.Password == "" {
		fmt.Fprintln(stderr, "pgshard-router dev-bootstrap: PGSHARD_DEV_PASSWORD must be set")
		return cli.ExitUsage
	}
	if err := b.Run(context.Background()); err != nil {
		fmt.Fprintf(stderr, "pgshard-router dev-bootstrap: %v\n", err)
		return cli.ExitNotReady
	}
	fmt.Fprintf(stdout, "pgshard-router dev-bootstrap: database %s and role %s ready\n", b.Database, b.Role)
	return cli.ExitOK
}
