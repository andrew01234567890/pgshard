package router

import (
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/dockertest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// flipProxy is a TCP proxy with a switchable backend, standing in for the
// stable catalog Service the operator repoints during a catalog upgrade.
type flipProxy struct {
	l net.Listener

	mu      sync.Mutex
	backend string
	conns   map[net.Conn]struct{}
}

func newFlipProxy(t *testing.T, backend string) *flipProxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &flipProxy{l: l, backend: backend, conns: map[net.Conn]struct{}{}}
	t.Cleanup(func() { _ = l.Close() })
	go p.run()
	return p
}

func (p *flipProxy) addr() string { return p.l.Addr().String() }

func (p *flipProxy) run() {
	for {
		c, err := p.l.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		backend := p.backend
		p.conns[c] = struct{}{}
		p.mu.Unlock()
		go func() {
			defer func() {
				p.mu.Lock()
				delete(p.conns, c)
				p.mu.Unlock()
				_ = c.Close()
			}()
			b, err := net.Dial("tcp", backend)
			if err != nil {
				return
			}
			defer func() { _ = b.Close() }()
			done := make(chan struct{})
			go func() { _, _ = io.Copy(b, c); _ = b.(*net.TCPConn).CloseWrite(); close(done) }()
			_, _ = io.Copy(c, b)
			<-done
		}()
	}
}

// flip repoints the proxy and severs live connections, as the Service
// selector change plus backend termination does in the cluster.
func (p *flipProxy) flip(backend string) {
	p.mu.Lock()
	p.backend = backend
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
}

func startCatalogContainer(t *testing.T, image string) string {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable; skipping catalog flip test")
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
	args := []string{"run", "-d", "--rm", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port), "--user", "postgres",
		"--entrypoint", "sh", image, "-ec",
		`initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 echo "host all all all trust" >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*'`}
	out, err := exec.Command("docker", args...).CombinedOutput()
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
	t.Fatal("postgres did not become ready")
	return ""
}

func seedCatalog(t *testing.T, dsn, verifier string, mapGeneration int64) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO pgshard.roles (rolname, verifier) VALUES ('app', '` + verifier + `')`,
		fmt.Sprintf(`UPDATE pgshard.shard_map_generation SET generation = %d`, mapGeneration),
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// TestRouterSurvivesCatalogFlip18To19 stands a router's catalog clients
// (SCRAM role cache, snapshot watcher, global sequence allocator) in front
// of a PostgreSQL 18 catalog behind a repointable endpoint, flips the
// endpoint to a PostgreSQL 19 catalog carrying the copied state, severs
// every live connection, and asserts all three keep serving without a
// restart: the operator's catalog-group upgrade repoints the stable
// catalog Service the same way.
func TestRouterSurvivesCatalogFlip18To19(t *testing.T) {
	const verifier = "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy"
	oldDSN := startCatalogContainer(t, "ghcr.io/andrew01234567890/pgshard-postgres:18")
	newDSN := startCatalogContainer(t, "ghcr.io/andrew01234567890/pgshard-postgres:19")
	seedCatalog(t, oldDSN, verifier, 7)
	seedCatalog(t, newDSN, verifier, 8)

	oldHostPort := strings.Split(strings.TrimPrefix(oldDSN, "postgres://postgres@"), "/")[0]
	newHostPort := strings.Split(strings.TrimPrefix(newDSN, "postgres://postgres@"), "/")[0]
	proxy := newFlipProxy(t, oldHostPort)
	proxyDSN := fmt.Sprintf("postgres://postgres@%s/postgres?sslmode=disable", proxy.addr())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, proxyDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	roles := NewRoleCache(pool, time.Millisecond)
	if v, err := roles.Lookup(ctx, "app"); err != nil || v != verifier {
		t.Fatalf("role lookup on the old catalog: %q, %v", v, err)
	}
	seqs := NewSequenceAllocator(&PGBlockSource{Pool: pool})
	before, err := seqs.Next(ctx, "invoices", 1)
	if err != nil {
		t.Fatalf("sequence on the old catalog: %v", err)
	}

	w := snapshot.NewWatcher(proxyDSN, snapshot.Options{ReloadInterval: 200 * time.Millisecond, DisableListen: true})
	go func() { _ = w.Run(ctx) }()
	waitFor(t, 30*time.Second, func() bool {
		s := w.Current()
		return s != nil && s.ShardMapGeneration == 7
	}, "snapshot from the old catalog")

	// The sequence position must carry over the flip, as the catalog
	// cutover's sequence handoff does.
	carrySequences(t, oldDSN, newDSN)
	proxy.flip(newHostPort)

	waitFor(t, 30*time.Second, func() bool {
		s := w.Current()
		return s != nil && s.ShardMapGeneration == 8
	}, "snapshot from the new catalog")
	// The first acquires after the flip may hand back severed pooled
	// connections; the cache must recover on retry, as a router serving
	// logins does.
	waitFor(t, 30*time.Second, func() bool {
		v, err := roles.Lookup(context.Background(), "app")
		return err == nil && v == verifier
	}, "role lookup after the flip")
	// Drain past the cached block (1000 by default) so at least one fresh
	// block is allocated from the new catalog, and prove the sequence never
	// goes backwards or errors across the flip.
	last := before[0]
	granted := 0
	deadline := time.Now().Add(30 * time.Second)
	for granted < 2500 {
		vals, err := seqs.Next(ctx, "invoices", 200)
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatalf("sequence after the flip: %v", err)
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, v := range vals {
			if v <= last {
				t.Fatalf("sequence went backwards across the flip: %d then %d", last, v)
			}
			last = v
		}
		granted += len(vals)
	}
}

func carrySequences(t *testing.T, fromDSN, toDSN string) {
	t.Helper()
	ctx := context.Background()
	from, err := pgx.Connect(ctx, fromDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = from.Close(ctx) }()
	to, err := pgx.Connect(ctx, toDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = to.Close(ctx) }()
	rows, err := from.Query(ctx, `SELECT name, next_value FROM pgshard.sequences`)
	if err != nil {
		t.Fatal(err)
	}
	type seq struct {
		Name string
		Next int64
	}
	seqsRows, err := pgx.CollectRows(rows, pgx.RowToStructByPos[seq])
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range seqsRows {
		if _, err := to.Exec(ctx, `INSERT INTO pgshard.sequences (name, next_value) VALUES ($1, $2)
			ON CONFLICT (name) DO UPDATE SET next_value = GREATEST(pgshard.sequences.next_value, EXCLUDED.next_value)`, s.Name, s.Next); err != nil {
			t.Fatal(err)
		}
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
