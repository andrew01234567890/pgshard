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

// OwnerSchema is the schema in which each shard records the keyspace range
// it owns. It is per shard, so it is never copied: a reshard target that
// inherited it would believe it owned its source's range.
const OwnerSchema = "pgshard_owner"

// DumpArgs are the pg_dump arguments of a schema copy: schema only, without
// the replication objects of the source so a reshard's own publications are
// never dumped into a target, and without the source's owned range.
//
// The per-table checks that read the range are dumped: they are plpgsql,
// whose body is not resolved when it is created, so they restore before the
// target has a range and read the target's own once it does.
func DumpArgs(source string) []string {
	return []string{"--schema-only", "--no-publications", "--no-subscriptions", "--exclude-schema=" + OwnerSchema, "--dbname=" + source}
}

// RestoreArgs are the psql arguments applying a dump: stop at the first
// error so a half-applied schema is reported instead of silently skipped.
func RestoreArgs(target string) []string {
	return []string{"-X", "-q", "-v", "ON_ERROR_STOP=1", "--dbname=" + target}
}

// StartFunc starts a command and returns a stop function to run after Wait.
// It lets a supervisor start and reaper-track a child atomically, closing the
// window in which a child that exits before it is tracked would be reaped out
// from under Wait. A nil StartFunc starts the command directly.
type StartFunc func(cmd *exec.Cmd) (stop func(), err error)

// Run pipes dump into restore and waits for both. restore's failure wins
// because a failing psql makes pg_dump fail on the broken pipe.
func Run(dump, restore *exec.Cmd, start StartFunc) error {
	if start == nil {
		start = func(cmd *exec.Cmd) (func(), error) { return func() {}, cmd.Start() }
	}
	pipe, err := dump.StdoutPipe()
	if err != nil {
		return err
	}
	var dumpErr, restoreOut bytes.Buffer
	dump.Stderr = &dumpErr
	restore.Stdin = pipe
	restore.Stdout = &restoreOut
	restore.Stderr = &restoreOut
	stopDump, err := start(dump)
	if err != nil {
		return err
	}
	defer stopDump()
	stopRestore, err := start(restore)
	if err != nil {
		_ = dump.Process.Kill()
		_ = dump.Wait()
		return err
	}
	defer stopRestore()
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
