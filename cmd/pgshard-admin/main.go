// Command pgshard-admin is the entry point for the pgshard admin binary.
package main

import (
	"os"

	"github.com/andrew01234567890/pgshard/internal/cli"
)

func main() {
	os.Exit(cli.Run("pgshard-admin", os.Args[1:], os.Stdout, os.Stderr))
}
