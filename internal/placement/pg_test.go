package placement

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

var pgImages = []struct{ label, name string }{
	{"pg18", "ghcr.io/andrew01234567890/pgshard-postgres:18"},
	{"pg19", "ghcr.io/andrew01234567890/pgshard-postgres:19"},
}

func startPostgres(t *testing.T, image string) *pgx.Conn {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker unavailable; skipping differential hash tests")
	}
	if exec.Command("docker", "image", "inspect", image).Run() != nil {
		if out, err := exec.Command("docker", "pull", image).CombinedOutput(); err != nil {
			t.Skipf("image %s unavailable: %v: %s", image, err, out)
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
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
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

const sampleCount = 10000

func TestDifferentialAgainstPostgres(t *testing.T) {
	for _, img := range pgImages {
		t.Run(img.label, func(t *testing.T) {
			conn := startPostgres(t, img.name)
			runDifferential(t, conn)
		})
	}
}

func pgHashes[T any](t *testing.T, conn *pgx.Conn, fn, typ string, values []T, seed uint64) []int64 {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		fmt.Sprintf(`SELECT %s(v, $2) FROM unnest($1::%s[]) WITH ORDINALITY AS u(v, n) ORDER BY n`, fn, typ),
		values, int64(seed))
	if err != nil {
		t.Fatalf("%s: %v", fn, err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		t.Fatalf("%s: %v", fn, err)
	}
	if len(got) != len(values) {
		t.Fatalf("%s: %d results for %d values", fn, len(got), len(values))
	}
	return got
}

func compare[T any](t *testing.T, name string, values []T, want []int64, f func(T) int64) {
	t.Helper()
	mismatches := 0
	for i, v := range values {
		if got := f(v); got != want[i] {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("%s(%v): go %d, postgres %d", name, v, got, want[i])
			}
		}
	}
	if mismatches > 0 {
		t.Errorf("%s: %d/%d mismatches", name, mismatches, len(values))
	}
}

func runDifferential(t *testing.T, conn *pgx.Conn) {
	rng := rand.New(rand.NewSource(1))
	seeds := []uint64{PartitionSeed, 0, uint64(rng.Int63())}
	var pgSeed int64
	if err := conn.QueryRow(context.Background(), `SELECT hashint8extended(42::int8, $1::int8)`, int64(PartitionSeed)).Scan(&pgSeed); err != nil {
		t.Fatal(err)
	}
	if pgSeed != 7363975540656877951 {
		t.Fatalf("golden hashint8extended(42, seed) on this server = %d", pgSeed)
	}
	for _, seed := range seeds {
		int8s := make([]int64, sampleCount)
		int4s := make([]int32, sampleCount)
		int2s := make([]int16, sampleCount)
		texts := make([]string, sampleCount)
		uuids := make([][16]byte, sampleCount)
		chars := make([]string, sampleCount)
		stamps := make([]time.Time, sampleCount)
		for i := range int8s {
			switch i % 4 {
			case 0:
				int8s[i] = int64(rng.Uint64())
			case 1:
				int8s[i] = int64(rng.Int31()) - 1<<30
			case 2:
				int8s[i] = int64(i) - 5
			default:
				int8s[i] = -int64(rng.Uint64() >> 1)
			}
			int4s[i] = int32(rng.Uint32())
			int2s[i] = int16(rng.Uint32())
			n := rng.Intn(40)
			b := make([]byte, n)
			for j := range b {
				b[j] = "abcdefghijklmnopqrstuvwxyz0123456789 _-"[rng.Intn(39)]
			}
			texts[i] = string(b)
			rng.Read(uuids[i][:])
			chars[i] = string(rune('!' + rng.Intn(90)))
			stamps[i] = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(rng.Int63n(1<<50)-1<<49) * time.Microsecond)
		}
		int8s[0], int8s[1], int8s[2] = 0, -1, 1<<63-1
		int8s[3] = -1 << 63
		texts[0] = ""

		compare(t, "hashint8extended", int8s, pgHashes(t, conn, "hashint8extended", "int8", int8s, seed),
			func(v int64) int64 { return HashInt8Extended(v, seed) })
		compare(t, "hashint4extended", int4s, pgHashes(t, conn, "hashint4extended", "int4", int4s, seed),
			func(v int32) int64 { return HashInt4Extended(v, seed) })
		compare(t, "hashint2extended", int2s, pgHashes(t, conn, "hashint2extended", "int2", int2s, seed),
			func(v int16) int64 { return HashInt2Extended(v, seed) })
		compare(t, "hashtextextended", texts, pgHashes(t, conn, "hashtextextended", "text", texts, seed),
			func(v string) int64 { return HashTextExtended(v, seed) })
		compare(t, "uuid_hash_extended", uuids, pgHashes(t, conn, "uuid_hash_extended", "uuid", uuids, seed),
			func(v [16]byte) int64 { return HashUUIDExtended(v, seed) })
		compare(t, "hashcharextended", chars, pgHashes(t, conn, "hashcharextended", `"char"`, chars, seed),
			func(v string) int64 { return HashCharExtended(v[0], seed) })
		compare(t, "timestamp_hash_extended", stamps, pgHashes(t, conn, "timestamp_hash_extended", "timestamp", stamps, seed),
			func(v time.Time) int64 {
				return HashInt8Extended(v.Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Microseconds(), seed)
			})
	}
}
