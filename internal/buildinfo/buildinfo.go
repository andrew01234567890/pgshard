// Package buildinfo contains the metadata and help text shared by pgshard
// command skeletons.
package buildinfo

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
)

// These variables can be replaced by release builds with -ldflags. The
// defaults intentionally identify an unconfigured development build.
var (
	Version   = "0.0.0-dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// Info describes one pgshard command build.
type Info struct {
	Component string
	Version   string
	Commit    string
	BuildDate string
}

// For returns the build information for component.
func For(component string) Info {
	return Info{
		Component: component,
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
	}
}

// VersionText returns a stable, human-readable version line.
func (i Info) VersionText() string {
	return fmt.Sprintf("%s %s (commit %s, built %s)", i.Component, i.Version, i.Commit, i.BuildDate)
}

// UsageText returns the shared command skeleton usage text.
func (i Info) UsageText() string {
	return fmt.Sprintf("Usage: %s [--help] [--version]\n\n%s\nStatus: not yet configured; no service is started.\n", i.Component, i.VersionText())
}

// Run handles the common command-skeleton contract and returns the process
// exit code. It deliberately accepts writers and arguments so the contract
// can be tested without starting a service or terminating the test process.
func Run(info Info, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet(info.Component, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() { _, _ = fmt.Fprint(stderr, info.UsageText()) }
	showHelp := flags.Bool("help", false, "print help information")
	showShortHelp := flags.Bool("h", false, "print help information")
	showVersion := flags.Bool("version", false, "print version information")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "%s: unexpected argument(s): %s\n", info.Component, strings.Join(flags.Args(), " "))
		_, _ = fmt.Fprint(stderr, info.UsageText())
		return 2
	}
	if *showHelp || *showShortHelp {
		_, _ = fmt.Fprint(stdout, info.UsageText())
		return 0
	}
	if *showVersion {
		_, _ = fmt.Fprintln(stdout, info.VersionText())
		return 0
	}
	_, _ = fmt.Fprint(stdout, info.UsageText())
	return 0
}
