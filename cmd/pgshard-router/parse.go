package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/cli"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
)

// runParse implements the hidden diagnostic subcommand
// `pgshard-router parse "SQL"`: it prints the fingerprint and the statement
// kinds of the bound PostgreSQL grammar, or the parse error.
func runParse(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintf(stderr, "Usage: pgshard-router parse \"SQL\"\n")
		return cli.ExitUsage
	}
	sql := args[0]
	res, err := pgparser.New(pgparser.Options{}).Parse(context.Background(), sql)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router: parse: %v\n", err)
		return cli.ExitNotReady
	}
	fp, err := pgparser.Fingerprint(sql)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router: parse: %v\n", err)
		return cli.ExitNotReady
	}
	fmt.Fprintf(stdout, "grammar: postgresql %d\nfingerprint: %s\nstatements: %d\nkinds: %s\n",
		pgparser.Major, fp, len(res.Stmts), strings.Join(res.Kinds(), ","))
	return cli.ExitOK
}
