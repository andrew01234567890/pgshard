// Command pgshard-operator is the entry point for the pgshard operator binary.
package main

import (
	"fmt"
	"os"

	"github.com/andrew01234567890/pgshard/internal/cli"
	"github.com/andrew01234567890/pgshard/internal/operator"
)

func main() {
	if _, err := operator.NewScheme(); err != nil {
		fmt.Fprintf(os.Stderr, "pgshard-operator: register scheme: %v\n", err)
		os.Exit(cli.ExitNotReady)
	}
	os.Exit(cli.Run("pgshard-operator", os.Args[1:], os.Stdout, os.Stderr))
}
