package pooler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// A Cancel can overlap the pool taking back a backend the session still
// names: a relay discarding a backend with unflushed messages returns it to
// the pool before it clears the session's reference. The cancel asks whether
// the backend was returned under the server's lock, but the pool records
// that under its own, so the question has to be synchronised with the pool's
// write rather than with the server's.
func TestACancelOverlappingTheBackendsReturnIsNotARace(t *testing.T) {
	port := startCancelPort(t, nil)
	for range 10 {
		pg := newFakePG()
		pool := newPool(PoolConfig{MaxBackends: 1, MaxPerRole: 1, AcquireTimeout: 2 * time.Second}, pg.dial)
		s := NewServer(Config{Logger: slog.New(slog.DiscardHandler), Pool: pool, Database: "db", Dialer: port.dialer()})
		se, err := s.attachSession("x", "alice", "db", sessionCred("alice", testKey(0x11), testKey(0x22)))
		if err != nil {
			t.Fatal(err)
		}
		b, err := pool.Acquire(context.Background(), "db", "alice", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		se.b = b
		s.mu.Unlock()

		abandoned := make(chan struct{})
		go func() {
			defer close(abandoned)
			pool.Abandon(b)
		}()
		if _, err := s.Cancel(context.Background(), &pgshardv1.CancelRequest{SessionId: "x"}); err != nil {
			t.Fatal(err)
		}
		<-abandoned
		select {
		case <-port.arrived:
		default:
		}
		pool.Close()
	}
}
