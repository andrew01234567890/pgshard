// Command pgshard-agent is the entry point for the pgshard agent binary.
package main

import (
	"os"

	"github.com/andrew01234567890/pgshard/internal/cli"
)

func main() {
	os.Exit(cli.Run("pgshard-agent", os.Args[1:], os.Stdout, os.Stderr))
}
