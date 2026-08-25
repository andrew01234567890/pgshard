package schemacopy

import (
	"os/exec"
	"strings"
	"testing"
)

func TestArgs(t *testing.T) {
	if got := strings.Join(DumpArgs("host=src dbname=app"), " "); got != "--schema-only --no-publications --no-subscriptions --dbname=host=src dbname=app" {
		t.Fatal(got)
	}
	if got := strings.Join(RestoreArgs("host=/tmp dbname=app"), " "); got != "-X -q -v ON_ERROR_STOP=1 --dbname=host=/tmp dbname=app" {
		t.Fatal(got)
	}
}

func TestRunPipesDumpIntoRestore(t *testing.T) {
	var tracked []int
	start := func(cmd *exec.Cmd) (func(), error) {
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		pid := cmd.Process.Pid
		tracked = append(tracked, pid)
		return func() { tracked = append(tracked, -pid) }, nil
	}
	out := t.TempDir() + "/out"
	if err := Run(exec.Command("sh", "-c", "printf 'CREATE TABLE t ();\\n'"), exec.Command("sh", "-c", "cat > "+out), start); err != nil {
		t.Fatal(err)
	}
	if len(tracked) != 4 || tracked[0] != -tracked[3] || tracked[1] != -tracked[2] {
		t.Fatalf("pids tracked and untracked in order: %v", tracked)
	}
	b, err := exec.Command("cat", out).Output()
	if err != nil || string(b) != "CREATE TABLE t ();\n" {
		t.Fatalf("restore saw %q %v", b, err)
	}

	err = Run(exec.Command("sh", "-c", "echo dump-ok"), exec.Command("sh", "-c", "echo 'ERROR: boom' >&2; exit 3"), nil)
	if err == nil || !strings.HasPrefix(err.Error(), "psql: ") || !strings.Contains(err.Error(), "ERROR: boom") {
		t.Fatalf("restore failure: %v", err)
	}
	err = Run(exec.Command("sh", "-c", "echo 'pg_dump: error: no such database' >&2; exit 1"), exec.Command("cat"), nil)
	if err == nil || !strings.HasPrefix(err.Error(), "pg_dump: ") || !strings.Contains(err.Error(), "no such database") {
		t.Fatalf("dump failure: %v", err)
	}
	if err := Run(exec.Command("/nonexistent/pg_dump"), exec.Command("cat"), nil); err == nil {
		t.Fatal("missing binary must fail")
	}
}
