// Command pgshard-operator is the entry point for the pgshard operator binary.
package main

import (
	"os"

	"github.com/andrew01234567890/pgshard/internal/cli"
)

func main() {
	os.Exit(cli.Run("pgshard-operator", os.Args[1:], os.Stdout, os.Stderr))
}
