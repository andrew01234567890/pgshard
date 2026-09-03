package pgrepl

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/dockertest"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/pgoutput"
)

var update = flag.Bool("update", false, "rewrite the pgoutput golden captures")

var images = []struct{ name, label string }{
	{"ghcr.io/andrew01234567890/pgshard-postgres:18", "pg18"},
	{"postgres:18", "pg18"},
	{"ghcr.io/andrew01234567890/pgshard-postgres:19", "pg19"},
	{"postgres:19", "pg19"},
}

func TestLSN(t *testing.T) {
	for _, s := range []string{"0/0", "0/16B3748", "1A/2B3C4D5E"} {
		l, err := ParseLSN(s)
		if err != nil {
			t.Fatal(err)
		}
		if l.String() != s {
			t.Fatalf("round trip %s -> %s", s, l)
		}
	}
	for _, s := range []string{"", "0", "x/0", "0/y"} {
		if _, err := ParseLSN(s); err == nil {
			t.Fatalf("parsed %q", s)
		}
	}
	if l, _ := ParseLSN("1/2"); l != 1<<32|2 {
		t.Fatalf("1/2 = %d", l)
	}
}

func TestParseFrame(t *testing.T) {
	if _, err := parseFrame(nil); err == nil {
		t.Fatal("empty frame accepted")
	}
	if _, err := parseFrame([]byte{'w', 1, 2}); err == nil {
		t.Fatal("short xlogdata accepted")
	}
	if _, err := parseFrame([]byte{'k', 1}); err == nil {
		t.Fatal("short keepalive accepted")
	}
	if _, err := parseFrame([]byte{'z'}); err == nil {
		t.Fatal("unknown frame accepted")
	}
	w := make([]byte, 25+3)
	w[0] = 'w'
	binary.BigEndian.PutUint64(w[1:], 10)
	binary.BigEndian.PutUint64(w[9:], 20)
	binary.BigEndian.PutUint64(w[17:], 1_000_000)
	copy(w[25:], "abc")
	m, err := parseFrame(w)
	if err != nil {
		t.Fatal(err)
	}
	x := m.(*XLogData)
	if x.WALStart != 10 || x.ServerWALEnd != 20 || string(x.Data) != "abc" || !x.ServerTime.Equal(pgEpoch.Add(time.Second)) {
		t.Fatalf("xlogdata %+v", x)
	}
	k := make([]byte, 18)
	k[0] = 'k'
	binary.BigEndian.PutUint64(k[1:], 30)
	k[17] = 1
	m, err = parseFrame(k)
	if err != nil {
		t.Fatal(err)
	}
	ka := m.(*PrimaryKeepalive)
	if ka.ServerWALEnd != 30 || !ka.ReplyRequested {
		t.Fatalf("keepalive %+v", ka)
	}
}

func TestPGTimeRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	if got := pgTime(toPGTime(now)); !got.Equal(now) {
		t.Fatalf("%s != %s", got, now)
	}
}

func TestQuoteLiteral(t *testing.T) {
	if got := quoteLiteral("it's"); got != "'it''s'" {
		t.Fatal(got)
	}
}

// TestCaptures streams real pgoutput traffic from PostgreSQL 18 and 19 and
// checks (or with -update rewrites) the golden captures under
// internal/pgoutput/testdata that the decoder tests replay offline.
func TestCaptures(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	seen := map[string]bool{}
	ran := 0
	for _, img := range images {
		if seen[img.label] {
			continue
		}
		// Pull rather than skip. These captures are what the offline
		// decoder tests replay, so an image that is merely not cached yet
		// meant the goldens were never checked against real PostgreSQL --
		// and a golden nothing verifies is just a file.
		if exec.Command("docker", "image", "inspect", img.name).Run() != nil {
			if err := exec.Command("docker", "pull", img.name).Run(); err != nil {
				t.Logf("image %s is not present and could not be pulled: %v", img.name, err)
				continue
			}
		}
		seen[img.label] = true
		ran++
		t.Run(img.label, func(t *testing.T) { runCaptures(t, img.name, img.label) })
	}
	if ran == 0 {
		dockertest.Unavailable(t, "no PostgreSQL 18/19 image available and none could be pulled")
	}
}

func startPostgres(t *testing.T, image string) string {
	t.Helper()
	// Admission here rather than in each test: a test that starts a
	// container is exactly the one that needs a slot, and putting it at
	// the one place that starts them means a new test cannot forget.
	dockertest.Parallel(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	script := `initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 printf 'host all all all trust\nhost replication all all trust\n' >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*' -c wal_level=logical -c max_prepared_transactions=16 -c logical_decoding_work_mem=64kB`
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port), "--user", "postgres", "--entrypoint", "sh", image, "-ec", script).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			_ = conn.Close(context.Background())
			return dsn
		}
		time.Sleep(300 * time.Millisecond)
	}
	logs, _ := exec.Command("docker", "logs", id).CombinedOutput()
	t.Fatalf("postgres not ready:\n%s", logs)
	return ""
}

type scenario struct {
	name string
	sql  []string
}

var scenarios = []scenario{
	{"dml", []string{
		"CREATE TABLE t (id int primary key, note text, big text); ALTER TABLE t ALTER COLUMN big SET STORAGE EXTERNAL",
		"INSERT INTO t VALUES (1, 'one', repeat('x', 10000))",
		"UPDATE t SET note = 'uno' WHERE id = 1",
		"UPDATE t SET id = 2 WHERE id = 1",
		"DELETE FROM t WHERE id = 2",
		"CREATE TABLE f (a int, b text); ALTER TABLE f REPLICA IDENTITY FULL",
		"INSERT INTO f VALUES (1, 'x'), (2, NULL)",
		"UPDATE f SET b = 'y' WHERE a = 1",
		"DELETE FROM f WHERE a = 2",
		"TRUNCATE t, f RESTART IDENTITY",
	}},
	{"messages", []string{
		"SELECT pg_logical_emit_message(true, 'txn', 'inside')",
		"BEGIN; SELECT pg_logical_emit_message(true, 'txn', 'a'); SELECT pg_logical_emit_message(false, 'now', 'b'); COMMIT",
		"SELECT pg_logical_emit_message(false, 'plain', 'outside')",
	}},
	{"streaming", []string{
		"CREATE TABLE s (id int primary key, v text)",
		"INSERT INTO s SELECT g, repeat('y', 100) FROM generate_series(1, 600) g",
		"BEGIN; INSERT INTO s SELECT g, 'z' FROM generate_series(601, 1200) g; ROLLBACK",
		"BEGIN; INSERT INTO s VALUES (9000, 'kept'); SAVEPOINT a; INSERT INTO s SELECT g, repeat('w', 100) FROM generate_series(5000, 5600) g; ROLLBACK TO SAVEPOINT a; INSERT INTO s VALUES (9001, 'after'); COMMIT",
	}},
	{"twophase", []string{
		"CREATE TABLE p (id int primary key)",
		"BEGIN; INSERT INTO p VALUES (1); PREPARE TRANSACTION 'gid-commit'",
		"COMMIT PREPARED 'gid-commit'",
		"BEGIN; INSERT INTO p VALUES (2); PREPARE TRANSACTION 'gid-abort'",
		"ROLLBACK PREPARED 'gid-abort'",
		"BEGIN; INSERT INTO p SELECT g FROM generate_series(10, 1500) g; PREPARE TRANSACTION 'gid-stream'",
		"COMMIT PREPARED 'gid-stream'",
	}},
	{"ddl", []string{
		"CREATE TABLE d (id int primary key)",
		"INSERT INTO d VALUES (1)",
		"ALTER TABLE d ADD COLUMN extra text DEFAULT 'dflt'",
		"INSERT INTO d VALUES (2, 'two')",
		"CREATE TYPE mood AS ENUM ('happy'); ALTER TABLE d ADD COLUMN m mood",
		"UPDATE d SET m = 'happy' WHERE id = 2",
	}},
}

func runCaptures(t *testing.T, image, label string) {
	ctx := context.Background()
	dsn := startPostgres(t, image)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close(ctx) }()
	if _, err := admin.Exec(ctx, "CREATE PUBLICATION pgshard_all FOR ALL TABLES"); err != nil {
		t.Fatal(err)
	}
	rc, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close(ctx) }()
	sys, err := rc.IdentifySystem(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sys.Timeline != 1 || sys.DBName != "postgres" || sys.XLogPos == 0 || sys.SystemID == "" {
		t.Fatalf("identify: %+v", sys)
	}
	dir := filepath.Join("..", "pgoutput", "testdata", label)
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			frames := capture(t, rc, admin, sc)
			var raw bytes.Buffer
			dec := pgoutput.NewDecoder()
			var golden strings.Builder
			for _, f := range frames {
				_ = binary.Write(&raw, binary.BigEndian, uint32(len(f)))
				raw.Write(f)
				m, err := dec.Decode(f)
				if err != nil {
					t.Fatalf("decode %q: %v", f, err)
				}
				golden.WriteString(pgoutput.Format(m))
				golden.WriteByte('\n')
			}
			binPath := filepath.Join(dir, sc.name+".bin")
			goldenPath := filepath.Join(dir, sc.name+".golden")
			if *update {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(binPath, raw.Bytes(), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(goldenPath, []byte(golden.String()), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("missing golden (run with -update): %v", err)
			}
			if normalize(string(want)) != normalize(golden.String()) {
				t.Errorf("live capture differs from golden shape:\n--- golden\n%s\n--- live\n%s", want, golden.String())
			}
		})
	}
}

// normalize strips run-specific numbers (xids, lsns, oids, timestamps) so a
// fresh capture can be compared with the stored golden by shape.
func normalize(s string) string {
	var b strings.Builder
	digit := false
	for _, c := range s {
		if (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') && digit {
			if !digit {
				b.WriteByte('#')
			}
			digit = true
			continue
		}
		digit = false
		b.WriteRune(c)
	}
	return b.String()
}

func capture(t *testing.T, rc *Conn, admin *pgx.Conn, sc scenario) [][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	slot := "cap_" + sc.name
	info, err := rc.CreateLogicalSlot(ctx, slot, "pgoutput", SlotOptions{TwoPhase: true, Failover: true, Snapshot: "nothing"})
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != slot || info.OutputPlugin != "pgoutput" || info.ConsistentPoint == 0 || info.SnapshotName != "" {
		t.Fatalf("create slot: %+v", info)
	}
	var failover, twoPhase bool
	if err := admin.QueryRow(ctx, "SELECT failover, two_phase FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&failover, &twoPhase); err != nil {
		t.Fatal(err)
	}
	if !failover || !twoPhase {
		t.Fatalf("slot flags failover=%t two_phase=%t", failover, twoPhase)
	}
	for _, q := range sc.sql {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if _, err := admin.Exec(ctx, "SELECT pg_logical_emit_message(false, 'pgshard', 'end')"); err != nil {
		t.Fatal(err)
	}
	opts := map[string]string{"proto_version": "4", "publication_names": "pgshard_all", "streaming": "parallel", "messages": "on", "two_phase": "on"}
	if err := rc.StartReplication(ctx, slot, 0, opts); err != nil {
		t.Fatal(err)
	}
	var frames [][]byte
	var last LSN
	dec := pgoutput.NewDecoder()
	for {
		msg, err := rc.Receive(ctx)
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		switch m := msg.(type) {
		case *PrimaryKeepalive:
			if m.ReplyRequested {
				if err := rc.SendStandbyStatus(StandbyStatus{Written: last, Flushed: last, Applied: last}); err != nil {
					t.Fatal(err)
				}
			}
			continue
		case *XLogData:
			frames = append(frames, m.Data)
			last = m.ServerWALEnd
			d, err := dec.Decode(m.Data)
			if err != nil {
				t.Fatalf("decode during capture: %v", err)
			}
			if lm, ok := d.(*pgoutput.LogicalMessage); ok && !lm.Transactional && lm.Prefix == "pgshard" && string(lm.Content) == "end" {
				goto done
			}
		}
	}
done:
	if err := rc.SendStandbyStatus(StandbyStatus{Written: last, Flushed: last, Applied: last, ReplyRequested: true}); err != nil {
		t.Fatal(err)
	}
	// Ending the stream needs a fresh connection: the server only leaves
	// CopyBoth on its own terms, so reconnect for the next scenario.
	_ = rc.Close(ctx)
	nc, err := Connect(ctx, admin.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	*rc = *nc
	if err := rc.DropSlot(ctx, slot, true); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&n); err != nil || n != 0 {
		t.Fatalf("slot still present (%d, %v)", n, err)
	}
	return frames
}
