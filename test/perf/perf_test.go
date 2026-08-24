package perf

import (
	"testing"

	"github.com/andrew01234567890/pgshard/internal/buildinfo"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

func BenchmarkNoop(b *testing.B) {
	for b.Loop() {
	}
}

var sink string

func BenchmarkBuildinfoString(b *testing.B) {
	for b.Loop() {
		sink = buildinfo.String()
	}
}

var sinkID int64

// BenchmarkGateKeyspaceID guards the placement hash on the router hot path.
func BenchmarkGateKeyspaceID(b *testing.B) {
	for b.Loop() {
		id, err := placement.KeyspaceID(int64(4242))
		if err != nil {
			b.Fatal(err)
		}
		sinkID = id
	}
}
