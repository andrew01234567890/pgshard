// Command pgshard-admin is the entry point for the pgshard admin binary.
package main

import (
	"fmt"
	"io"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/andrew01234567890/pgshard/internal/admin"
	"github.com/andrew01234567890/pgshard/internal/cli"
)

func main() {
	os.Exit(cli.RunWith("pgshard-admin", os.Args[1:], os.Stdout, os.Stderr, map[string]cli.Subcommand{"serve": serve}))
}

func serve(args []string, _ io.Writer, stderr io.Writer) int {
	opts, err := admin.ParseFlags(args, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-admin serve: %v\n", err)
		return cli.ExitUsage
	}
	if err := admin.Run(ctrl.SetupSignalHandler(), opts); err != nil {
		fmt.Fprintf(stderr, "pgshard-admin serve: %v\n", err)
		return cli.ExitNotReady
	}
	return cli.ExitOK
}
