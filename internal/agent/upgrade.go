package agent

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

// UpgradeCommand implements "pgshard-agent upgrade": the entrypoint of the
// offline pg_upgrade --link Job. It needs an image that carries the
// binaries of both majors; the standard per-major image holds one, so a
// missing bindir is refused with a message naming the requirement.
func UpgradeCommand(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pgshard-agent upgrade", flag.ContinueOnError)
	fs.SetOutput(stderr)
	oldMajor := fs.Int("old-major", 0, "major version of the current data directory")
	newMajor := fs.Int("new-major", 0, "major version to upgrade to")
	oldData := fs.String("old-data", "", "current data directory")
	newData := fs.String("new-data", "", "initialized data directory of the new major")
	binRoot := fs.String("bin-root", DefaultBinRoot, "directory holding per-major bin directories (<root>/<major>/bin)")
	check := fs.Bool("check", false, "run pg_upgrade --check only")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	cmdline, err := UpgradeCommandLine(*binRoot, *oldMajor, *newMajor, *oldData, *newData, *check)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-agent upgrade: %v\n", err)
		return 2
	}
	cmd := exec.Command(cmdline[0], cmdline[1:]...)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.Dir = filepath.Dir(*newData)
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(stderr, "pgshard-agent upgrade: pg_upgrade: %v\n", err)
		return 1
	}
	return 0
}

// DefaultBinRoot is where Debian-family PostgreSQL images install per-major
// binaries.
const DefaultBinRoot = "/usr/lib/postgresql"

// UpgradeCommandLine validates the inputs and renders the pg_upgrade --link
// invocation. Both majors' bin directories must exist under binRoot: the
// offline strategy requires a combined image (docs/upgrade.md).
func UpgradeCommandLine(binRoot string, oldMajor, newMajor int, oldData, newData string, check bool) ([]string, error) {
	if oldMajor <= 0 || newMajor <= oldMajor {
		return nil, fmt.Errorf("--new-major (%d) must be greater than --old-major (%d)", newMajor, oldMajor)
	}
	if oldData == "" || newData == "" {
		return nil, errors.New("--old-data and --new-data are required")
	}
	oldBin := filepath.Join(binRoot, strconv.Itoa(oldMajor), "bin")
	newBin := filepath.Join(binRoot, strconv.Itoa(newMajor), "bin")
	for _, bin := range []string{oldBin, newBin} {
		if st, err := os.Stat(bin); err != nil || !st.IsDir() {
			return nil, fmt.Errorf("%s not found: the offline strategy needs an image with the binaries of majors %d and %d; the per-major pgshard-postgres image carries one — build a combined image or use the online strategy", bin, oldMajor, newMajor)
		}
	}
	argv := []string{filepath.Join(newBin, "pg_upgrade"),
		"--link",
		"--old-bindir", oldBin,
		"--new-bindir", newBin,
		"--old-datadir", oldData,
		"--new-datadir", newData,
	}
	if check {
		argv = append(argv, "--check")
	}
	return argv, nil
}
