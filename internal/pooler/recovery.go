package pooler

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// DefaultRecoveryInterval is how often a RecoveryProbe asks its server
// whether it is in recovery. A promotion is rare and the question is one
// round trip on a unix socket, so this is short enough that a demoted
// member stops answering as the primary within a second or two and cheap
// enough to leave running for the life of the process.
const DefaultRecoveryInterval = 2 * time.Second

// RecoveryProbe reports whether the PostgreSQL this pooler stands in front
// of is in recovery.
//
// It exists because everything else the pooler fences with comes from the
// catalog. The epoch it compares is the catalog's for the shard, and so is
// the one the router stamps, so both sides of that comparison come from one
// row: it catches a router that is behind the catalog and never a pooler
// standing in front of a member that is no longer the primary. Recovery
// state is the one fact about this member that the catalog cannot supply.
type RecoveryProbe struct {
	// DSN reaches the local server over the pod's socket. pg_is_in_recovery
	// needs no privilege, so this is the change stream's own login.
	DSN string
	// Interval defaults to DefaultRecoveryInterval.
	Interval time.Duration
	// Logf, when set, reports a probe that could not reach its server.
	Logf func(format string, args ...any)

	// state is 0 unknown, 1 primary, 2 in recovery. Unknown is not
	// "standby": a probe that cannot reach its server has learned nothing,
	// and refusing on that would take a shard out on a socket hiccup when
	// the requests it refuses would fail on their own anyway.
	state atomic.Int32
}

const (
	recoveryUnknown int32 = iota
	recoveryPrimary
	recoveryStandby
)

// InRecovery reports the last answer and whether there is one.
func (p *RecoveryProbe) InRecovery() (inRecovery, known bool) {
	switch p.state.Load() {
	case recoveryPrimary:
		return false, true
	case recoveryStandby:
		return true, true
	default:
		return false, false
	}
}

// Run polls until ctx is done. It opens a connection per poll rather than
// holding one: a connection held across a promotion is exactly the one that
// would answer from before it.
func (p *RecoveryProbe) Run(ctx context.Context) {
	interval := p.Interval
	if interval <= 0 {
		interval = DefaultRecoveryInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		p.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (p *RecoveryProbe) poll(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := pgconn.Connect(ctx, p.DSN)
	if err != nil {
		p.unknown(err)
		return
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	res, err := conn.Exec(ctx, "SELECT pg_is_in_recovery()").ReadAll()
	if err != nil || len(res) != 1 || len(res[0].Rows) != 1 || len(res[0].Rows[0]) != 1 {
		p.unknown(err)
		return
	}
	if string(res[0].Rows[0][0]) == "t" {
		p.state.Store(recoveryStandby)
		return
	}
	p.state.Store(recoveryPrimary)
}

// unknown forgets the last answer rather than keeping it. A member that has
// stopped answering may be the one that was demoted, and the previous
// answer is the one that says it is still the primary.
func (p *RecoveryProbe) unknown(err error) {
	if p.state.Swap(recoveryUnknown) != recoveryUnknown && p.Logf != nil {
		p.Logf("recovery probe lost contact with the local server: %v", err)
	}
}
