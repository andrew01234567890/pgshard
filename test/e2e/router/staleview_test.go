//go:build integration

package router

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// staleViewSQLState is what a router answers once its catalog view is past
// snapshot.MaxAge; internal/router keeps it as codeStaleGeneration, which
// is unexported.
const staleViewSQLState = "55000"

// cuttableProxy forwards TCP to target until it is cut, then refuses and
// drops everything. It stands in for a network partition that leaves the
// client-to-router path up: iptables and Toxiproxy both do this, and
// neither is available to an integration test that starts processes on the
// host.
type cuttableProxy struct {
	addr string

	mu    sync.Mutex
	down  bool
	conns map[net.Conn]struct{}
}

func startCuttableProxy(tb testing.TB, target string) *cuttableProxy {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	p := &cuttableProxy{addr: l.Addr().String(), conns: map[net.Conn]struct{}{}}
	tb.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go p.serve(c, target)
		}
	}()
	return p
}

func (p *cuttableProxy) serve(client net.Conn, target string) {
	p.mu.Lock()
	down := p.down
	if !down {
		p.conns[client] = struct{}{}
	}
	p.mu.Unlock()
	if down {
		_ = client.Close()
		return
	}
	server, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	p.conns[server] = struct{}{}
	p.mu.Unlock()
	go func() { _, _ = io.Copy(server, client); _ = server.Close() }()
	_, _ = io.Copy(client, server)
	_ = client.Close()
}

// cut drops every connection through the proxy and refuses new ones. The
// existing connections matter: the watcher keeps one for LISTEN, and a
// partition that only refused new dials would leave it reading happily.
func (p *cuttableProxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

func (p *cuttableProxy) restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = false
}

// dsnVia rewrites dsn to reach the same database through addr.
func dsnVia(tb testing.TB, dsn, addr string) string {
	tb.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		tb.Fatal(err)
	}
	u.Host = addr
	return u.String()
}

// TestRouterPartitionedFromTheCatalogStopsServing is the evidence PGS-447
// asked for and did not get. The unit bound says Stale() turns true past
// MaxAge, which is arithmetic; the property is that a router in this state
// cannot answer a statement.
//
// The case that matters is the second one. A router whose reloads fail
// keeps the same snapshot pointer, so nothing on the extended path looks
// stale to a comparison: a statement prepared while the view was fresh
// could be bound and executed against a view of any age. A test that only
// drove fresh simple queries would pass against the version without the
// refusal in replanStale.
func TestRouterPartitionedFromTheCatalogStopsServing(t *testing.T) {
	s := startStack(t)
	ctx := context.Background()

	catalogHost := dsnHost(t, s.catalogDSN)
	proxy := startCuttableProxy(t, catalogHost)
	p := s.startRouter(t, 21, nil, "--catalog-dsn", dsnVia(t, s.catalogDSN, proxy.addr))

	conn := s.connectTo(t, p.addr)
	if _, err := conn.Exec(ctx, "select 1", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("the router did not serve before the partition: %v", err)
	}
	// Prepared while the view was fresh, so the plan is cached and the
	// extended path has something to bind later.
	if _, err := conn.Prepare(ctx, "fresh", "select 1"); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := conn.Exec(ctx, "fresh"); err != nil {
		t.Fatalf("the prepared statement did not run before the partition: %v", err)
	}

	proxy.cut()

	// Past MaxAge, both forms must refuse. Polled rather than slept for
	// exactly MaxAge: the bound is measured from the last successful load,
	// which happened some unknown moment before the cut.
	deadline := time.Now().Add(snapshot.MaxAge + 45*time.Second)
	var lastSimple, lastPrepared error
	for {
		_, lastSimple = conn.Exec(ctx, "select 1", pgx.QueryExecModeSimpleProtocol)
		_, lastPrepared = conn.Exec(ctx, "fresh")
		// 55000 specifically, not merely an error: a client has to be able
		// to tell a router that stopped on purpose from one that fell over,
		// and a transport failure here would mean the partition took the
		// client path down too and the test proved nothing.
		if sqlstate(lastSimple) == staleViewSQLState && sqlstate(lastPrepared) == staleViewSQLState {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the router kept serving %s after losing the catalog\nsimple: %v\nprepared: %v\nrouter log:\n%s",
				snapshot.MaxAge, lastSimple, lastPrepared, p.log.String())
		}
		time.Sleep(time.Second)
	}

	// The refusal is counted under its own SQLSTATE, so an operator can see
	// a router that has stopped trusting its view without reading logs.
	if n := refusalCount(t, p.healthAddr, staleViewSQLState); n == 0 {
		t.Errorf("pgshard_router_refusals_total{sqlstate=%q} is 0 after the router refused twice", staleViewSQLState)
	}

	proxy.restore()

	// And it comes back: the bound is a pause, not a latch.
	recovered := time.Now().Add(2 * snapshot.MaxAge)
	for {
		_, err := conn.Exec(ctx, "select 1", pgx.QueryExecModeSimpleProtocol)
		if err == nil {
			break
		}
		if time.Now().After(recovered) {
			t.Fatalf("the router never recovered after the partition lifted: %v\nrouter log:\n%s", err, p.log.String())
		}
		time.Sleep(time.Second)
	}
}

// dsnHost is the host:port a DSN points at.
func dsnHost(tb testing.TB, dsn string) string {
	tb.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		tb.Fatal(err)
	}
	if u.Port() == "" {
		return fmt.Sprintf("%s:5432", u.Hostname())
	}
	return u.Host
}

// refusalCount reads pgshard_router_refusals_total for one SQLSTATE from
// the router's own /metrics, rather than a counter the test could only
// assert against itself.
func refusalCount(tb testing.TB, healthAddr, sqlstate string) float64 {
	tb.Helper()
	resp, err := http.Get("http://" + healthAddr + "/metrics")
	if err != nil {
		tb.Fatalf("reading router metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		tb.Fatalf("reading router metrics: %v", err)
	}
	want := `pgshard_router_refusals_total{sqlstate="` + sqlstate + `"}`
	for _, line := range strings.Split(string(body), "\n") {
		rest, ok := strings.CutPrefix(line, want)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			tb.Fatalf("metric line %q: %v", line, err)
		}
		return v
	}
	return 0
}
