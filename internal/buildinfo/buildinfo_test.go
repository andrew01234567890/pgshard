package buildinfo

import (
	"bytes"
	"fmt"
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

func TestRunCommandContract(t *testing.T) {
	info := Info{
		Component: "pgshard-test",
		Version:   "0.0.0-test",
		Commit:    "abc123",
		BuildDate: "2026-08-15T00:00:00Z",
	}

	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{
			name:       "no args shows honest skeleton status",
			wantCode:   0,
			wantStdout: info.UsageText(),
		},
		{
			name:       "help",
			args:       []string{"--help"},
			wantCode:   0,
			wantStdout: info.UsageText(),
		},
		{
			name:       "version",
			args:       []string{"--version"},
			wantCode:   0,
			wantStdout: info.VersionText() + "\n",
		},
		{
			name:       "unknown option",
			args:       []string{"--unknown"},
			wantCode:   2,
			wantStderr: "flag provided but not defined: -unknown\n" + info.UsageText(),
		},
		{
			name:       "stray argument",
			args:       []string{"unexpected"},
			wantCode:   2,
			wantStderr: fmt.Sprintf("%s: unexpected argument(s): unexpected\n%s", info.Component, info.UsageText()),
		},
		{
			name:       "unexpected argument with version",
			args:       []string{"--version", "unexpected"},
			wantCode:   2,
			wantStderr: fmt.Sprintf("%s: unexpected argument(s): unexpected\n%s", info.Component, info.UsageText()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := Run(info, test.args, &stdout, &stderr); got != test.wantCode {
				t.Fatalf("Run() code = %d, want %d", got, test.wantCode)
			}
			if got := stdout.String(); got != test.wantStdout {
				t.Errorf("stdout = %q, want %q", got, test.wantStdout)
			}
			if got := stderr.String(); got != test.wantStderr {
				t.Errorf("stderr = %q, want %q", got, test.wantStderr)
			}
		})
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
