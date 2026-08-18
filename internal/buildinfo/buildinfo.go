// Package buildinfo carries version metadata injected at link time via -ldflags -X.
package buildinfo

// Build metadata; overridden at link time.
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// String formats the build metadata as "<version> (<commit>, <date>)".
func String() string {
	return Version + " (" + Commit + ", " + Date + ")"
}
