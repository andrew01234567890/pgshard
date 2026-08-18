package buildinfo

import "testing"

func TestDefaults(t *testing.T) {
	if Version != "dev" || Commit != "none" || Date != "unknown" {
		t.Fatalf("unexpected defaults: %q %q %q", Version, Commit, Date)
	}
	if got := String(); got != "dev (none, unknown)" {
		t.Fatalf("String() = %q", got)
	}
}

func TestStringUsesOverrides(t *testing.T) {
	v, c, d := Version, Commit, Date
	t.Cleanup(func() { Version, Commit, Date = v, c, d })
	Version, Commit, Date = "1.2.3", "abc123", "2026-01-01"
	if got := String(); got != "1.2.3 (abc123, 2026-01-01)" {
		t.Fatalf("String() = %q", got)
	}
}
