package router

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
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

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPoolers builds a resolver; static may be nil.
func NewPoolers(static map[Shard]string, snap SnapshotFunc, creds credentials.TransportCredentials) *Poolers {
	if static == nil {
		static = map[Shard]string{}
	}
	return &Poolers{static: static, snapshot: snap, creds: creds, conns: map[string]*grpc.ClientConn{}}
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
	cc, ok := p.conns[ep]
	if !ok {
		var err error
		cc, err = grpc.NewClient(ep, grpc.WithTransportCredentials(p.creds))
		if err != nil {
			return nil, fmt.Errorf("router: dial pooler %s: %w", ep, err)
		}
		p.conns[ep] = cc
	}
	return pgshardv1.NewPoolerClient(cc), nil
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
	for ep, cc := range p.conns {
		_ = cc.Close()
		delete(p.conns, ep)
	}
}
