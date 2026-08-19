// Command pgshard-operator is the entry point for the pgshard operator binary.
package main

import (
	"fmt"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/andrew01234567890/pgshard/internal/cli"
	"github.com/andrew01234567890/pgshard/internal/operator"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "run" {
		os.Exit(run(os.Args[2:]))
	}
	os.Exit(cli.Run("pgshard-operator", os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string) int {
	opts, err := operator.ParseFlags(args, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgshard-operator run: %v\n", err)
		return cli.ExitUsage
	}
	if err := operator.Run(ctrl.SetupSignalHandler(), opts); err != nil {
		fmt.Fprintf(os.Stderr, "pgshard-operator run: %v\n", err)
		return cli.ExitNotReady
	}
	return cli.ExitOK
}
