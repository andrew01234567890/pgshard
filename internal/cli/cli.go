// Package cli implements the shared entry point for every pgshard command.
package cli

import (
	"fmt"
	"io"

	"github.com/andrew01234567890/pgshard/internal/buildinfo"
)

// Exit codes shared by every command.
const (
	ExitOK          = 0
	ExitNotReady    = 1
	ExitUsage       = 2
	notImplemented  = "runtime services are not implemented yet"
	unknownArgument = "unknown argument"
)

var descriptions = map[string]string{
	"pgshard-router":     "Stateless PostgreSQL wire-protocol router that scatters queries across shards.",
	"pgshard-agent":      "Per-node agent that manages a local PostgreSQL instance and its replication.",
	"pgshard-pooler":     "Connection pooler that fronts a single shard's PostgreSQL primary and replicas.",
	"pgshard-controller": "Cluster controller that owns topology, shard placement and resharding.",
	"pgshard-operator":   "Kubernetes operator that reconciles pgshard clusters from custom resources.",
	"pgshard-admin":      "Administrative CLI for inspecting and changing a pgshard cluster.",
}

// Run executes command name with args and returns the process exit code.
func Run(name string, args []string, stdout, stderr io.Writer) int {
	desc, ok := descriptions[name]
	if !ok {
		fmt.Fprintf(stderr, "%s: unknown command\n", name)
		return ExitUsage
	}
	switch {
	case len(args) == 0:
		fmt.Fprintf(stderr, "%s: %s\n", name, notImplemented)
		return ExitNotReady
	case len(args) == 1 && (args[0] == "--help" || args[0] == "-h"):
		fmt.Fprintf(stdout, "%s\n\nUsage: %s [--help | --version]\n", desc, name)
		return ExitOK
	case len(args) == 1 && args[0] == "--version":
		fmt.Fprintf(stdout, "%s %s\n", name, buildinfo.String())
		return ExitOK
	default:
		fmt.Fprintf(stderr, "%s: %s %q\nUsage: %s [--help | --version]\n", name, unknownArgument, args[0], name)
		return ExitUsage
	}
}
