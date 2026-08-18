package perf

import (
	"testing"

	"github.com/andrew01234567890/pgshard/internal/buildinfo"
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
