// Package buildinfo contains the metadata and help text shared by pgshard
// command skeletons.
package buildinfo

import "fmt"

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
