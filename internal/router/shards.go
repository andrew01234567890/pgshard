package router

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/pooler"
)

// SnapshotFunc returns the current catalog snapshot, nil before the first
// load.
type SnapshotFunc func() *snapshot.Snapshot

// Poolers resolves shards to pooler gRPC clients. Static endpoints win over
// the primary_endpoint published in the catalog's shard_status.
type Poolers struct {
	static   map[Shard]string
	snapshot SnapshotFunc
	creds    credentials.TransportCredentials

	// Now overrides the clock in tests.
	Now func() time.Time

	mu    sync.Mutex
	conns map[string]*poolerConn
	swept time.Time
}

// poolerConn is one endpoint's client and when a shard last resolved to
// it. A router used to keep every endpoint it had ever seen: each
// failover, pod replacement and rollout publishes a new one, so its
// connections grew with the cluster's lifetime churn rather than with the
// shards it actually serves, and each one holds transport state,
// goroutines and descriptors.
type poolerConn struct {
	cc   *grpc.ClientConn
	used time.Time
}

const (
	// poolerIdleTimeout lets gRPC drop the transport of a connection
	// nothing is using, keeping the client usable and reconnecting on the
	// next call.
	poolerIdleTimeout = 5 * time.Minute
	// poolerRetireAfter is how long an endpoint no shard resolves to is
	// kept before its client is closed, and poolerSweepEvery how often
	// that is looked for.
	poolerRetireAfter = 2 * time.Minute
	poolerSweepEvery  = time.Minute
)

// NewPoolers builds a resolver; static may be nil.
func NewPoolers(static map[Shard]string, snap SnapshotFunc, creds credentials.TransportCredentials) *Poolers {
	if static == nil {
		static = map[Shard]string{}
	}
	return &Poolers{static: static, snapshot: snap, creds: creds, conns: map[string]*poolerConn{}}
}

// Endpoint returns the pooler address for sh, or "" when none is known.
func (p *Poolers) Endpoint(sh Shard) string {
	if ep, ok := p.static[sh]; ok {
		return ep
	}
	if s := p.snapshot(); s != nil {
		return s.Serving[snapshot.ShardKey{ShardSet: sh.Set, ShardID: sh.ID}].PrimaryEndpoint
	}
	return ""
}

// Client returns a shared client for the pooler of sh.
func (p *Poolers) Client(sh Shard) (pgshardv1.PoolerClient, error) {
	ep := p.Endpoint(sh)
	if ep == "" {
		return nil, pgwire.Errorf("57P03", "no pooler endpoint known for shard %s/%d", sh.Set, sh.ID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	c, ok := p.conns[ep]
	if !ok {
		cc, err := grpc.NewClient(ep, grpc.WithTransportCredentials(p.creds), grpc.WithIdleTimeout(poolerIdleTimeout),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(pooler.MaxMessageBytes), grpc.MaxCallSendMsgSize(pooler.MaxMessageBytes)))
		if err != nil {
			return nil, fmt.Errorf("router: dial pooler %s: %w", ep, err)
		}
		c = &poolerConn{cc: cc}
		p.conns[ep] = c
	}
	c.used = now
	p.retire(now)
	return pgshardv1.NewPoolerClient(c.cc), nil
}

// retire closes the clients of endpoints no shard resolves to any more.
// Call with mu held.
//
// Only an idle connection is closed, and only after it has gone unused for
// poolerRetireAfter: closing a gRPC client cancels the RPCs on it, and a
// scatter or a change stream opened before a failover may still be
// draining through an endpoint the catalog has already moved off.
func (p *Poolers) retire(now time.Time) {
	if now.Sub(p.swept) < poolerSweepEvery {
		return
	}
	p.swept = now
	serving := p.servingEndpoints()
	for ep, c := range p.conns {
		if serving[ep] || now.Sub(c.used) < poolerRetireAfter {
			continue
		}
		if c.cc.GetState() != connectivity.Idle {
			continue
		}
		_ = c.cc.Close()
		delete(p.conns, ep)
	}
}

// servingEndpoints is every endpoint a shard resolves to right now.
func (p *Poolers) servingEndpoints() map[string]bool {
	out := make(map[string]bool, len(p.static))
	for _, ep := range p.static {
		out[ep] = true
	}
	if s := p.snapshot(); s != nil {
		for _, sv := range s.Serving {
			if sv.PrimaryEndpoint != "" {
				out[sv.PrimaryEndpoint] = true
			}
		}
	}
	return out
}

func (p *Poolers) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

// Generation stamps a request for sh with the snapshot's shard map
// generation and the shard's primary epoch.
func (p *Poolers) Generation(sh Shard) *pgshardv1.Generation {
	g := &pgshardv1.Generation{}
	if s := p.snapshot(); s != nil {
		g.ShardMapGeneration = uint64(s.ShardMapGeneration)
		g.PrimaryEpoch = uint64(s.Serving[snapshot.ShardKey{ShardSet: sh.Set, ShardID: sh.ID}].Epoch)
	}
	return g
}

// Close closes every pooler connection.
func (p *Poolers) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for ep, c := range p.conns {
		_ = c.cc.Close()
		delete(p.conns, ep)
	}
}
