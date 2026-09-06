package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The controller's catalog password comes from a mounted Secret rather than
// argv, where /proc/<pid>/cmdline exposes it to anything in the pod, and
// rather than PGPASSWORD, which is the superuser's -- libpq would apply that
// to the catalog connection too, and the catalog login is not that role.
func TestTheCatalogPasswordComesFromItsFile(t *testing.T) {
	dir := t.TempDir()
	for _, pw := range []string{
		"plain",
		"has a space",
		`quote'and\backslash`,
		"'",
	} {
		path := filepath.Join(dir, "password")
		if err := os.WriteFile(path, []byte(pw+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		dsn, err := withPasswordFile("host=h user=pgshard_controller dbname=postgres", path)
		if err != nil {
			t.Fatalf("%q: %v", pw, err)
		}
		// The proof is that libpq's own parser recovers the password, not
		// that the string looks right: the quoting rules are its rules.
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatalf("%q: %v", pw, err)
		}
		if cfg.ConnConfig.Password != pw {
			t.Errorf("password parsed back as %q, want %q", cfg.ConnConfig.Password, pw)
		}
		if cfg.ConnConfig.User != "pgshard_controller" {
			t.Errorf("user %q", cfg.ConnConfig.User)
		}
	}
}

// No file leaves the DSN alone: the flag is optional, so a controller run by
// hand against a DSN that already carries its credential still works.
func TestNoPasswordFileLeavesTheDSNAlone(t *testing.T) {
	const dsn = "host=h user=u"
	if got, err := withPasswordFile(dsn, ""); err != nil || got != dsn {
		t.Fatalf("got %q, %v", got, err)
	}
}

// A password file that is missing or empty is a startup failure, not a
// connection attempt with no password: the second reaches the catalog as
// whatever PGPASSWORD holds, which is the superuser.
func TestAnUnreadablePasswordFileIsRefused(t *testing.T) {
	dir := t.TempDir()
	if _, err := withPasswordFile("host=h", filepath.Join(dir, "absent")); err == nil {
		t.Error("a missing password file was accepted")
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := withPasswordFile("host=h", empty); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("an empty password file gave %v", err)
	}
}
