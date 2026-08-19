// Package schemacopy copies the schema of one database into another by
// piping pg_dump --schema-only into psql. Agents run it inside their pod;
// a controller with PostgreSQL binaries at hand runs it locally.
package schemacopy

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

// DumpArgs are the pg_dump arguments of a schema copy: schema only, without
// the replication objects of the source so a reshard's own publications are
// never dumped into a target.
func DumpArgs(source string) []string {
	return []string{"--schema-only", "--no-publications", "--no-subscriptions", "--dbname=" + source}
}

// RestoreArgs are the psql arguments applying a dump: stop at the first
// error so a half-applied schema is reported instead of silently skipped.
func RestoreArgs(target string) []string {
	return []string{"-X", "-q", "-v", "ON_ERROR_STOP=1", "--dbname=" + target}
}

// Started observes the processes once they run; it lets a supervisor keep
// their pids out of its reaper. It may be nil.
type Started func(pid int) (untrack func())

// Run pipes dump into restore and waits for both. restore's failure wins
// because a failing psql makes pg_dump fail on the broken pipe.
func Run(dump, restore *exec.Cmd, started Started) error {
	pipe, err := dump.StdoutPipe()
	if err != nil {
		return err
	}
	var dumpErr, restoreOut bytes.Buffer
	dump.Stderr = &dumpErr
	restore.Stdin = pipe
	restore.Stdout = &restoreOut
	restore.Stderr = &restoreOut
	if err := dump.Start(); err != nil {
		return err
	}
	if started != nil {
		defer started(dump.Process.Pid)()
	}
	if err := restore.Start(); err != nil {
		_ = dump.Process.Kill()
		_ = dump.Wait()
		return err
	}
	if started != nil {
		defer started(restore.Process.Pid)()
	}
	restoreErr := restore.Wait()
	dumpWaitErr := dump.Wait()
	switch {
	case restoreErr != nil:
		return fmt.Errorf("psql: %w: %s", restoreErr, strings.TrimSpace(restoreOut.String()))
	case dumpWaitErr != nil:
		return fmt.Errorf("pg_dump: %w: %s", dumpWaitErr, strings.TrimSpace(dumpErr.String()))
	}
	return nil
}
