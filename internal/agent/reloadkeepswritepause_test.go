package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReloadKeepsTheWritePause: the barrier and the reshard cutover pause
// writes with ALTER SYSTEM SET default_transaction_read_only = on, which
// lands in postgresql.auto.conf. Reload used to rewrite that file down to
// its header and then pg_ctl reload, so a rollout pass whose settings hash
// moved during a cutover unpaused the shard in the same breath -- and it
// then accepted writes until the operator's next reconcile read the raised
// fence and reapplied the pause.
//
// The truncation exists for clone tools: pg_basebackup and pgBackRest write
// into this file, and bootstrap, promotion and restore have to neutralise
// what they left. A reload is none of those.
//
// This asserts the bytes survive the write. That the POSTMASTER still has
// the setting afterwards rests on postgres parsing postgresql.auto.conf
// after postgresql.conf (guc.c), which a file test cannot see -- the
// integration flow reloads a live primary under a pause and writes to it.
// TestWriteConfigRendersFilesAndResetsAutoConf holds the other side, that a
// clone's leftovers are still scrubbed by the paths that clone.
func TestReloadKeepsTheWritePause(t *testing.T) {
	in := newTestInstance(t)
	fakePgCtl(t, in)
	path := filepath.Join(in.cfg.PGData, "postgresql.auto.conf")
	pause := "default_transaction_read_only = 'on'\n"
	if err := os.WriteFile(path, []byte(autoConfHeader+pause), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := in.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), pause) {
		t.Fatalf("the reload dropped the write pause; postgresql.auto.conf is now:\n%s", body)
	}
	// The rendered files are still written: this must not have become a
	// reload that reloads nothing.
	for _, name := range []string{"postgresql.conf", "pg_hba.conf"} {
		if _, err := os.Stat(filepath.Join(in.cfg.PGData, name)); err != nil {
			t.Errorf("%s was not written: %v", name, err)
		}
	}
}

// fakePgCtl puts a pg_ctl on the supervisor's path so Reload can get as far
// as asking the postmaster to reread: the file is what is under test, and a
// missing binary would fail the test before it got there.
func fakePgCtl(t *testing.T, in *Instance) {
	t.Helper()
	path := filepath.Join(in.sup.binDir, "pg_ctl")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
}
