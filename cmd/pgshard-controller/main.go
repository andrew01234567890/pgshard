// Command pgshard-controller is the entry point for the pgshard controller binary.
package main

import (
	"os"

	"github.com/andrew01234567890/pgshard/internal/cli"
)

func main() {
	os.Exit(cli.RunWith("pgshard-controller", os.Args[1:], os.Stdout, os.Stderr, map[string]cli.Subcommand{
		"run": run,
	}))
}
