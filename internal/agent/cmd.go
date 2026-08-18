package agent

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
)

// RunCommand implements "pgshard-agent run --pgdata … --config …".
func RunCommand(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pgshard-agent run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	pgdata := fs.String("pgdata", "", "PostgreSQL data directory (overrides config)")
	cfgPath := fs.String("config", "", "path to the agent JSON config")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *cfgPath == "" {
		fmt.Fprintln(stderr, "pgshard-agent run: --config is required")
		return 2
	}
	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-agent run: %v\n", err)
		return 2
	}
	if *pgdata != "" {
		cfg.PGData = *pgdata
	}
	log := slog.New(slog.NewTextHandler(stdout, nil))
	if err := Run(context.Background(), cfg, log); err != nil {
		log.Error("agent exited", "err", err)
		return 1
	}
	return 0
}
