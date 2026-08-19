package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/cli"
)

// runCatalogWatch implements the hidden diagnostic subcommand
// `pgshard-router catalog-watch DSN`: it follows the catalog and prints
// every snapshot generation change and consistency transition until
// interrupted.
func runCatalogWatch(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintf(stderr, "Usage: pgshard-router catalog-watch DSN\n")
		return cli.ExitUsage
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w := snapshot.NewWatcher(args[0], snapshot.Options{
		Logf: func(format string, a ...any) { fmt.Fprintf(stderr, "pgshard-router: "+format+"\n", a...) },
	})
	changes, unsubscribe := w.Subscribe()
	defer unsubscribe()
	cw := snapshot.NewConsistencyWatcher()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-changes:
				s := w.Current()
				fmt.Fprintf(stdout, "%s\n", s)
				for _, t := range cw.Observe(s) {
					fmt.Fprintf(stdout, "shard set %s: %s -> %s (blocking %v)\n", t.ShardSet, t.From, t.To, t.Blocking)
				}
			}
		}
	}()
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintf(stderr, "pgshard-router: catalog-watch: %v\n", err)
		return cli.ExitNotReady
	}
	return cli.ExitOK
}
