package plan

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/dockertest"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// TestKeyNormalisationMatchesPostgres pins the contract normaliseKey exists
// to keep: for a character column, what the router hashes must be what
// PostgreSQL stores and casts to text.
//
// internal/placement's differential test already compares the hash
// FUNCTIONS against PostgreSQL's. It says nothing about normalisation,
// which is the step where a divergence is silent -- the router would route
// a key to one shard while the row lives on another, with no error
// anywhere. The varchar(n) normalisation that ships today is covered only
// by unit tests over the Go side, which cannot notice if the Go and
// PostgreSQL notions of the ::text cast disagree.
//
// The bpchar rows are the ones PGS-258 needs before blank-padded keys can
// be allowed at all; they are here so the contract is written down before
// the feature leans on it.
func TestKeyNormalisationMatchesPostgres(t *testing.T) {
	for _, img := range []struct{ label, name string }{
		{"pg18", "ghcr.io/andrew01234567890/pgshard-postgres:18"},
		{"pg19", "ghcr.io/andrew01234567890/pgshard-postgres:19"},
	} {
		t.Run(img.label, func(t *testing.T) {
			conn := startPostgresForKeys(t, img.name)
			for _, tc := range []struct {
				name, columnType, value string
			}{
				{"varchar exact", "character varying(8)", "abcdefgh"},
				{"varchar short", "character varying(8)", "abc"},
				{"varchar overlength, excess all spaces", "character varying(3)", "abc   "},
				{"varchar trailing space within limit", "character varying(8)", "abc "},
				{"varchar empty", "character varying(8)", ""},
				{"varchar unicode", "character varying(16)", "wíld ünicode"},
				{"text is untouched", "text", "abc   "},
			} {
				t.Run(tc.name, func(t *testing.T) {
					assertRouterAgreesWithPostgres(t, conn, tc.columnType, tc.value)
				})
			}
		})
	}
}

// assertRouterAgreesWithPostgres stores value in a column of columnType and
// compares PostgreSQL's hash of what it stored with the router's hash of
// what it would have routed.
func assertRouterAgreesWithPostgres(t *testing.T, conn *pgx.Conn, columnType, value string) {
	t.Helper()
	ctx := context.Background()
	if _, err := conn.Exec(ctx, `DROP TABLE IF EXISTS k; CREATE TABLE k (v `+columnType+`)`); err != nil {
		t.Fatalf("create %s: %v", columnType, err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO k VALUES ($1)`, value); err != nil {
		t.Fatalf("insert %q into %s: %v", value, columnType, err)
	}
	var stored string
	var want int64
	if err := conn.QueryRow(ctx,
		`SELECT v::text, hashtextextended(v::text, $1) FROM k`, int64(placement.PartitionSeed),
	).Scan(&stored, &want); err != nil {
		t.Fatalf("hash from %s: %v", columnType, err)
	}
	got, err := placement.KeyspaceID(normaliseKey(value, columnType))
	if err != nil {
		t.Fatalf("KeyspaceID: %v", err)
	}
	if got != want {
		t.Fatalf("%s value %q: router hashes to %d, PostgreSQL stores %q and hashes to %d\n"+
			"the router would route this key to a different shard from the one holding the row",
			columnType, value, got, stored, want)
	}
}

func startPostgresForKeys(t *testing.T, image string) *pgx.Conn {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable; skipping key-normalisation differential test")
	}
	if exec.Command("docker", "image", "inspect", image).Run() != nil {
		if out, err := exec.Command("docker", "pull", image).CombinedOutput(); err != nil {
			dockertest.Unavailable(t, "image %s unavailable: %v: %s", image, err, out)
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port),
		"--entrypoint", "sh", image, "-ec",
		`initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 echo "host all all all trust" >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*'`).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(cctx, dsn)
		cancel()
		if err == nil {
			t.Cleanup(func() { _ = conn.Close(context.Background()) })
			return conn
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("postgres did not become ready")
	return nil
}
