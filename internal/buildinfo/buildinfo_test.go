package buildinfo

import (
	"strings"
	"testing"
)

var versionTextBenchmarkSink string

func TestForUsesBuildMetadata(t *testing.T) {
	info := For("pgshard-test")

	if info.Component != "pgshard-test" {
		t.Fatalf("component = %q, want %q", info.Component, "pgshard-test")
	}
	if info.Version != Version || info.Commit != Commit || info.BuildDate != BuildDate {
		t.Fatalf("For returned %+v, want package metadata", info)
	}
}

func TestInfoText(t *testing.T) {
	info := Info{
		Component: "pgshard-test",
		Version:   "0.0.0-test",
		Commit:    "abc123",
		BuildDate: "2026-08-15T00:00:00Z",
	}

	wantVersion := "pgshard-test 0.0.0-test (commit abc123, built 2026-08-15T00:00:00Z)"
	if got := info.VersionText(); got != wantVersion {
		t.Fatalf("VersionText() = %q, want %q", got, wantVersion)
	}

	usage := info.UsageText()
	for _, want := range []string{
		"Usage: pgshard-test [--help] [--version]",
		wantVersion,
		"Status: not yet configured; no service is started.",
	} {
		if !strings.Contains(usage, want) {
			t.Errorf("UsageText() = %q, missing %q", usage, want)
		}
	}
}

func BenchmarkVersionText(b *testing.B) {
	info := Info{
		Component: "pgshard-test",
		Version:   "0.0.0-test",
		Commit:    "abc123",
		BuildDate: "2026-08-15T00:00:00Z",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		versionTextBenchmarkSink = info.VersionText()
	}
}
