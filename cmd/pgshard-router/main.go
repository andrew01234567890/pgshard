// Command pgshard-router is the entry point for the pgshard router binary.
package main

import (
	"os"

	"github.com/andrew01234567890/pgshard/internal/cli"
)

func main() {
	os.Exit(cli.RunWith("pgshard-router", os.Args[1:], os.Stdout, os.Stderr, map[string]cli.Subcommand{
		"serve":         serve,
		"parse":         runParse,
		"catalog-watch": runCatalogWatch,
	}))
}
