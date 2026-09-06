package pooler

import (
	"testing"
	"time"
)

// TestTheKeepalivePairAgrees: the client half and the server half are one
// setting. gRPC's server default answers a client that pings more often than
// every five minutes, or pings at all without an active stream, with GOAWAY
// too_many_pings and closes the connection -- so a client configured to ping
// every 20s against an unconfigured server is worse than no keepalive at
// all: it turns a healthy idle stream into a reconnect loop.
func TestTheKeepalivePairAgrees(t *testing.T) {
	if Keepalive.Time < KeepaliveEnforcement.MinTime {
		t.Errorf("the router pings every %s but the pooler permits one only every %s; the server will answer with GOAWAY too_many_pings",
			Keepalive.Time, KeepaliveEnforcement.MinTime)
	}
	if Keepalive.PermitWithoutStream && !KeepaliveEnforcement.PermitWithoutStream {
		t.Error("the router pings without an active stream and the pooler does not permit it")
	}
	// The point of the setting: a dead pooler is noticed in seconds, not in
	// the kernel's retransmit timeout.
	if d := Keepalive.Time + Keepalive.Timeout; d > time.Minute {
		t.Errorf("a dead pooler takes %s to notice; that is the failure this setting exists to shorten", d)
	}
}
