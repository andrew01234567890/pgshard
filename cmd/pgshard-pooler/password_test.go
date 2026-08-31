package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTheCatalogPasswordComesFromAFile: the pooler holds two credentials.
// PGPASSWORD is the superuser's, for the local socket that creates and reads
// replication slots; the catalog connection reads the shard map as the
// router's login role and must not use it. libpq applies PGPASSWORD to every
// connection, so the catalog one carries its own password in the DSN --
// read from a mounted file, never written into a pod spec.
func TestTheCatalogPasswordComesFromAFile(t *testing.T) {
	dir := t.TempDir()
	const dsn = "host=c.svc port=5432 user=pgshard_router dbname=postgres"

	if got, err := withPasswordFile(dsn, ""); err != nil || got != dsn {
		t.Fatalf("no file must leave the DSN alone: %q %v", got, err)
	}

	path := filepath.Join(dir, "password")
	if err := os.WriteFile(path, []byte("s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := withPasswordFile(dsn, path)
	if err != nil {
		t.Fatal(err)
	}
	if got != dsn+" password='s3cr3t'" {
		t.Fatalf("dsn = %q", got)
	}

	// A password is not a well-behaved identifier: quotes and backslashes
	// end the value early in libpq's keyword/value form, so they are
	// escaped rather than assumed away.
	awkward := filepath.Join(dir, "awkward")
	if err := os.WriteFile(awkward, []byte(`a'b\c`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err = withPasswordFile(dsn, awkward)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, `password='a\'b\\c'`) {
		t.Fatalf("escaping: %q", got)
	}

	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := withPasswordFile(dsn, empty); err == nil {
		t.Fatal("an empty password file must fail rather than connect without one")
	}
	if _, err := withPasswordFile(dsn, filepath.Join(dir, "missing")); err == nil {
		t.Fatal("a missing password file must fail")
	}
}
