package operator

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// probeIdleTimeout is how long a probe connection may sit unused before it
// is closed. It is the whole answer to "do not hold a backend on a member
// the operator is about to fence": the operator simply stops probing a
// member it has taken out, and the backend goes away on its own. An
// explicit close at every point that stops probing would be a second
// mechanism to keep correct, and one that is wrong in the direction of
// holding a connection for ever if a caller is missed.
const probeIdleTimeout = 90 * time.Second

// probeMaxConns is per DSN. Probes are one short read and the reconciler
// runs them one member at a time, so two is enough to let a pass overlap
// the tail of the previous one without holding a backend per goroutine.
const probeMaxConns = 2

// probePools keeps one small pool per DSN so a probe does not pay a TCP
// handshake, TLS and SCRAM before every question it asks. It is package
// level because PgxProber is a zero-size value constructed inline in
// several places and copied freely; making it carry the state would mean
// changing every one of those into a shared pointer, which is a larger
// change than the caching it enables.
//
// pgxpool is what makes this safe rather than a map of connections: it
// health-checks on acquire, replaces a connection that failed, and closes
// idle ones. A probe that inherits a session setting or an open
// transaction from a previous use is the hazard here, and probes only ever
// run read-only SELECTs -- they set nothing and begin nothing, so there is
// nothing to inherit. Anything that does needs its own connection, not
// this.
var probePools sync.Map // dsn -> *pgxpool.Pool

// probeConn acquires a pooled connection for dsn. The caller releases it.
func probeConn(ctx context.Context, dsn string) (*pgxpool.Conn, error) {
	if p, ok := probePools.Load(dsn); ok {
		return p.(*pgxpool.Pool).Acquire(ctx)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = probeMaxConns
	cfg.MinConns = 0
	cfg.MaxConnIdleTime = probeIdleTimeout
	// A pool created and then lost to a concurrent creation would leak its
	// connections, so the loser closes what it built.
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if existing, loaded := probePools.LoadOrStore(dsn, pool); loaded {
		pool.Close()
		pool = existing.(*pgxpool.Pool)
	}
	return pool.Acquire(ctx)
}

// withProbeConn runs fn on a pooled connection, once more on a fresh one if
// the first had gone bad.
//
// The retry is the difference between pooling and not. A backend
// terminated while its connection sat idle in the pool -- a member fenced,
// restarted or rewound, which is routine here -- is not detected until
// something reads from it, so the first use after that fails with 57P01.
// Reconnecting is what the caller did before there was a pool, and a probe
// that reports a member down because the operator was holding a stale
// connection to it would be a failure the operator itself caused.
func withProbeConn(ctx context.Context, dsn string, fn func(*pgxpool.Conn) error) error {
	var err error
	for attempt := range 2 {
		var conn *pgxpool.Conn
		conn, err = probeConn(ctx, dsn)
		if err != nil {
			return err
		}
		err = fn(conn)
		broken := conn.Conn().IsClosed()
		conn.Release()
		if err == nil || !broken || attempt == 1 {
			return err
		}
		// Release destroys a connection whose underlying one is closed, so
		// the next acquire builds a new one.
	}
	return err
}
