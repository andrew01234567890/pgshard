package router

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/cancelpeer"
)

// servePeer exposes h's local cancel path over RouterPeer.
func servePeer(t *testing.T, h *harness) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	pgshardv1.RegisterRouterPeerServer(g, &cancelpeer.Server{Local: func(ctx context.Context, key pgwire.CancelKey) bool {
		return h.r.CancelLocal(ctx, h.srv, key)
	}})
	go func() { _ = g.Serve(l) }()
	t.Cleanup(g.Stop)
	return l.Addr().String()
}

// twoRouters starts routers A and B on one fake pooler; B forwards cancels
// it does not own to A.
func twoRouters(t *testing.T) (a, b *harness) {
	t.Helper()
	fp := newFakePooler()
	poolerAddr := startFakePooler(t, fp)
	a = newHarnessWith(t, fp, poolerAddr, nil)
	peerA := servePeer(t, a)
	b = newHarnessWith(t, fp, poolerAddr, func(cfg *Config) {
		f, err := cancelpeer.New(cancelpeer.Config{Self: 2, Static: map[uint32]string{1: peerA}, Creds: insecure.NewCredentials()})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(f.Close)
		cfg.Peers = f
	})
	return a, b
}

// cancelVia sends key as a CancelRequest to the router at addr.
func cancelVia(t *testing.T, addr string, pid uint32, secret []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	buf := make([]byte, 12+len(secret))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(buf)))
	binary.BigEndian.PutUint32(buf[4:8], 80877102)
	binary.BigEndian.PutUint32(buf[8:12], pid)
	copy(buf[12:], secret)
	if _, err := conn.Write(buf); err != nil {
		t.Fatal(err)
	}
}

func waitSleeping(t *testing.T, fp *fakePooler) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fp.mu.Lock()
		n := len(fp.sleeping)
		fp.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("query never reached the pooler")
}

func TestCancelForwardedToOwningRouter(t *testing.T) {
	a, b := twoRouters(t)
	ctx := context.Background()
	conn := a.connect(t, a.dsn("app", "secret", "app"))
	pid, secret := conn.PgConn().PID(), conn.PgConn().SecretKey()
	go func() {
		waitSleeping(t, a.fp)
		cancelVia(t, b.addr, pid, secret)
	}()
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := conn.Exec(qctx, "select pg_sleep(10)"); sqlstate(err) != "57014" {
		t.Fatalf("cancel via peer: %v", err)
	}
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("session after cancel: %v", err)
	}
}

func TestCancelWithWrongSecretIsIgnoredEverywhere(t *testing.T) {
	a, b := twoRouters(t)
	ctx := context.Background()
	conn := a.connect(t, a.dsn("app", "secret", "app"))
	pid, secret := conn.PgConn().PID(), conn.PgConn().SecretKey()
	bogus := make([]byte, len(secret))
	for i := range secret {
		bogus[i] = ^secret[i]
	}
	done := make(chan error, 1)
	go func() {
		waitSleeping(t, a.fp)
		cancelVia(t, b.addr, pid, bogus)
		cancelVia(t, a.addr, pid, bogus)
		time.Sleep(200 * time.Millisecond)
		cancels := len(a.fp.cancelled())
		if cancels != 0 {
			t.Errorf("pooler saw %d cancels for a bogus key", cancels)
		}
		_ = conn.PgConn().CancelRequest(ctx)
	}()
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := conn.Exec(qctx, "select pg_sleep(10)")
	done <- err
	if sqlstate(<-done) != "57014" {
		t.Fatalf("real cancel: %v", err)
	}
}

// TestPgxSpeaksProtocol30 pins the assumption behind the tests above: pgx
// keys carry no instance prefix, so forwarding takes the broadcast path.
func TestPgxSpeaksProtocol30(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	if len(conn.PgConn().SecretKey()) != pgwire.CancelKeyLen30 {
		t.Fatalf("secret len %d", len(conn.PgConn().SecretKey()))
	}
}
