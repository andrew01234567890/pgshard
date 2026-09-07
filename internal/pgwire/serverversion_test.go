package pgwire

import (
	"sync/atomic"
	"testing"
)

// The advertised version used to be read once, from a string fixed at
// startup. A cluster's SQL surface moves under a running router when its
// shard sets change major, and a client that connected after that has to be
// told what it will actually get, not what the last restart believed.
func TestServerVersionIsAskedForEveryConnection(t *testing.T) {
	var version atomic.Value
	version.Store("18.0 (pgshard)")
	ts := startServer(t, Config{ServerVersion: func() string { return version.Load().(string) }})

	first := dialRaw(t, ts.addr).startup(ProtocolVersion30)
	if got := first.params["server_version"]; got != "18.0 (pgshard)" {
		t.Fatalf("server_version = %q, want the value in force when it connected", got)
	}

	version.Store("19.0 (pgshard)")
	second := dialRaw(t, ts.addr).startup(ProtocolVersion30)
	if got := second.params["server_version"]; got != "19.0 (pgshard)" {
		t.Fatalf("server_version = %q, want the value in force now; it was read once at startup", got)
	}
}
