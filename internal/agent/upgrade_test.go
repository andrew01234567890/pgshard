package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUpgradeCommandLine(t *testing.T) {
	root := t.TempDir()
	for _, major := range []string{"18", "19"} {
		if err := os.MkdirAll(filepath.Join(root, major, "bin"), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	argv, err := UpgradeCommandLine(root, 18, 19, "/old", "/new", false)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{filepath.Join(root, "19/bin/pg_upgrade"), "--link",
		"--old-bindir", filepath.Join(root, "18/bin"), "--new-bindir", filepath.Join(root, "19/bin"),
		"--old-datadir", "/old", "--new-datadir", "/new"}, " ")
	if got := strings.Join(argv, " "); got != want {
		t.Fatalf("got %q want %q", got, want)
	}

	argv, err = UpgradeCommandLine(root, 18, 19, "/old", "/new", true)
	if err != nil || argv[len(argv)-1] != "--check" {
		t.Fatalf("check flag: %v %v", argv, err)
	}
}

func TestUpgradeCommandLineRefusals(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "18", "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := UpgradeCommandLine(root, 18, 19, "/old", "/new", false); err == nil || !strings.Contains(err.Error(), "combined image") {
		t.Fatalf("single-major image must be refused with the combined-image requirement: %v", err)
	}
	if _, err := UpgradeCommandLine(root, 19, 18, "/old", "/new", false); err == nil {
		t.Fatal("downgrade must be refused")
	}
	if _, err := UpgradeCommandLine(root, 18, 19, "", "/new", false); err == nil {
		t.Fatal("missing data dirs must be refused")
	}
}
