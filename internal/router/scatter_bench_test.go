package router

import (
	"testing"

	"google.golang.org/protobuf/proto"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// benchRequest is a Bind of the shape a scatter actually fans out: a
// statement name, several parameters and a format vector, all identical on
// every shard.
func benchRequest() *pgshardv1.ExecuteRequest {
	params := make([]*pgshardv1.Value, 8)
	for i := range params {
		params[i] = &pgshardv1.Value{Data: []byte("a moderately sized parameter value")}
	}
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Bind{Bind: &pgshardv1.Bind{
		Statement: "stmt_for_the_scatter", Params: params,
		ParamFormats: []int32{0, 0, 0, 0, 1, 1, 1, 1}, ResultFormats: []int32{0, 1, 0, 1},
	}}}
}

// sink keeps the results reachable so the compiler cannot discard the work
// being measured.
var sink *pgshardv1.ExecuteRequest

// BenchmarkPerShardHeader measures what a scatter pays per shard per
// protocol operation. The payload is the same on every shard; only the
// header differs, so copying the payload is work the fan-out does not need.
func BenchmarkPerShardHeader(b *testing.B) {
	req := benchRequest()
	b.Run("share", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sink = perShard(req)
		}
	})
	b.Run("deep_clone", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sink = proto.Clone(req).(*pgshardv1.ExecuteRequest)
		}
	})
}
