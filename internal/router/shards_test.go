package router

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// TestPoolersRetireEndpointsThatStoppedServing: a router kept a client for
// every endpoint it had ever resolved, and only shutdown removed them.
// Each failover, pod replacement and rollout publishes a new endpoint, so
// the connections grew with the cluster's lifetime churn rather than with
// the shards actually served -- and every one of them holds transport
// state, goroutines and descriptors.
func TestPoolersRetireEndpointsThatStoppedServing(t *testing.T) {
	key := snapshot.ShardKey{ShardSet: DefaultShardSet, ShardID: 0}
	snap := &snapshot.Snapshot{Serving: map[snapshot.ShardKey]snapshot.Serving{}}
	p := NewPoolers(nil, func() *snapshot.Snapshot { return snap }, insecure.NewCredentials())
	defer p.Close()
	now := time.Unix(1000, 0)
	p.Now = func() time.Time { return now }
	sh := Shard{Set: DefaultShardSet, ID: 0}

	// One shard rotated through a hundred endpoints, as a hundred
	// failovers would.
	for i := range 100 {
		snap.Serving[key] = snapshot.Serving{PrimaryEndpoint: fmt.Sprintf("127.0.0.1:%d", 15000+i)}
		if _, err := p.Client(sh); err != nil {
			t.Fatal(err)
		}
		now = now.Add(10 * time.Second)
	}

	// Past the retirement grace period, one more resolution sweeps the
	// endpoints nothing resolves to any more.
	now = now.Add(2 * poolerRetireAfter)
	if _, err := p.Client(sh); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	live := len(p.conns)
	_, current := p.conns[snap.Serving[key].PrimaryEndpoint]
	p.mu.Unlock()
	if live > 2 {
		t.Errorf("%d pooler clients after rotating one shard through 100 endpoints", live)
	}
	if !current {
		t.Error("the endpoint the shard resolves to now was retired")
	}
}

// TestPoolersKeepAnEndpointStillInUse: closing a gRPC client cancels the
// calls on it, and a scatter or a change stream opened before a failover
// may still be draining through an endpoint the catalog has moved off.
func TestPoolersKeepAnEndpointStillInUse(t *testing.T) {
	key := snapshot.ShardKey{ShardSet: DefaultShardSet, ShardID: 0}
	snap := &snapshot.Snapshot{Serving: map[snapshot.ShardKey]snapshot.Serving{
		key: {PrimaryEndpoint: "127.0.0.1:15900"},
	}}
	p := NewPoolers(nil, func() *snapshot.Snapshot { return snap }, insecure.NewCredentials())
	defer p.Close()
	now := time.Unix(1000, 0)
	p.Now = func() time.Time { return now }
	sh := Shard{Set: DefaultShardSet, ID: 0}
	if _, err := p.Client(sh); err != nil {
		t.Fatal(err)
	}

	// The shard moves, but the old endpoint was used a moment ago.
	snap.Serving[key] = snapshot.Serving{PrimaryEndpoint: "127.0.0.1:15901"}
	now = now.Add(poolerSweepEvery + time.Second)
	if _, err := p.Client(sh); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	_, old := p.conns["127.0.0.1:15900"]
	p.mu.Unlock()
	if !old {
		t.Error("an endpoint used moments ago was closed under whatever is still draining on it")
	}
}
