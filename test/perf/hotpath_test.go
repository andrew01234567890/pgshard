package perf

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/placement"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// The hot-path benchmarks isolate the per-transaction costs of an unsharded
// prepared SELECT through router+pooler, the workload of docs/perf-profile.md.

var sinkBytes []byte

func BenchmarkPgwireDataRowEncode1Col(b *testing.B) {
	row := &pgproto3.DataRow{Values: [][]byte{[]byte("12345")}}
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		var err error
		buf, err = row.Encode(buf[:0])
		if err != nil {
			b.Fatal(err)
		}
	}
	sinkBytes = buf
}

func BenchmarkPgwireDataRowEncode8Col(b *testing.B) {
	vals := make([][]byte, 8)
	for i := range vals {
		vals[i] = []byte("column-value-0123456789")
	}
	row := &pgproto3.DataRow{Values: vals}
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		var err error
		buf, err = row.Encode(buf[:0])
		if err != nil {
			b.Fatal(err)
		}
	}
	sinkBytes = buf
}

// bindExecuteSyncFrames is one pgbench-style prepared transaction as raw
// frontend bytes: Bind, Describe(portal), Execute, Sync.
func bindExecuteSyncFrames(b *testing.B) []byte {
	b.Helper()
	var buf []byte
	for _, m := range []pgproto3.FrontendMessage{
		&pgproto3.Bind{PreparedStatement: "P0_1", Parameters: [][]byte{[]byte("424242")}},
		&pgproto3.Describe{ObjectType: 'P'},
		&pgproto3.Execute{},
		&pgproto3.Sync{},
	} {
		var err error
		buf, err = m.Encode(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
	return buf
}

func BenchmarkPgwireBindExecuteSyncDecode(b *testing.B) {
	frames := bindExecuteSyncFrames(b)
	r := bytes.NewReader(frames)
	be := pgproto3.NewBackend(r, io.Discard)
	b.ReportAllocs()
	for b.Loop() {
		r.Reset(frames)
		for range 4 {
			if _, err := be.Receive(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func benchSnapshot(b *testing.B) *snapshot.Snapshot {
	b.Helper()
	ranges, err := placement.Split(4)
	if err != nil {
		b.Fatal(err)
	}
	s := &snapshot.Snapshot{
		ShardMapGeneration: 11,
		ShardSets:          map[string][]snapshot.Range{},
		Databases:          map[string]catalog.Database{"app": {Name: "app", DefaultPlacement: "unsharded", HomeShard: 0}},
		Tables:             map[snapshot.TableKey]snapshot.Placement{},
	}
	for i, r := range ranges {
		s.ShardSets[plan.DefaultShardSet] = append(s.ShardSets[plan.DefaultShardSet],
			snapshot.Range{ShardID: int32(i), Start: r.Start, End: r.End})
	}
	s.Tables[snapshot.TableKey{Database: "app", SchemaName: "public", TableName: "pgbench_accounts"}] =
		snapshot.Placement{Placement: "unsharded", Generation: 3}
	s.Tables[snapshot.TableKey{Database: "app", SchemaName: "public", TableName: "orders"}] =
		snapshot.Placement{Placement: "sharded", ShardKey: "tenant_id", Generation: 3}
	return s
}

const unshardedSelect = "SELECT abalance FROM pgbench_accounts WHERE aid = $1"

var sinkPlan plan.Plan

// BenchmarkPlannerPlanUnshardedSelect is the per-Bind replanning cost of a
// prepared unsharded SELECT with a warm parse cache.
func BenchmarkPlannerPlanUnshardedSelect(b *testing.B) {
	p := plan.New()
	sess := plan.Session{Database: "app", HomeShard: 0, ID: 5, Snapshot: benchSnapshot(b)}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		pl, err := p.Plan(ctx, sess, unshardedSelect)
		if err != nil {
			b.Fatal(err)
		}
		sinkPlan = pl
	}
}

func BenchmarkPlannerPlanShardedSelect(b *testing.B) {
	p := plan.New()
	sess := plan.Session{Database: "app", HomeShard: 0, ID: 5, Snapshot: benchSnapshot(b)}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		pl, err := p.Plan(ctx, sess, "SELECT * FROM orders WHERE tenant_id = $1")
		if err != nil {
			b.Fatal(err)
		}
		sinkPlan = pl
	}
}

// BenchmarkParseCacheHit is the parser LRU lookup alone (warm cache).
func BenchmarkParseCacheHit(b *testing.B) {
	p := pgparser.New(pgparser.Options{})
	ctx := context.Background()
	if _, err := p.Parse(ctx, unshardedSelect); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := p.Parse(ctx, unshardedSelect); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseUncached is a full grammar parse (cold cache), the cost the
// LRU saves.
func BenchmarkParseUncached(b *testing.B) {
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		i++
		sql := fmt.Sprintf("SELECT abalance FROM pgbench_accounts WHERE aid = %d", i)
		if _, err := pgparser.Parse(sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSnapshotLocate(b *testing.B) {
	s := benchSnapshot(b)
	b.ReportAllocs()
	for b.Loop() {
		id, err := placement.KeyspaceID(int64(4242))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := s.Locate(plan.DefaultShardSet, id); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScramParseVerifier(b *testing.B) {
	v, err := pgwire.BuildSCRAMVerifier("app-secret", []byte("0123456789abcdef"), 4096)
	if err != nil {
		b.Fatal(err)
	}
	stored := v.String()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := pgwire.ParseSCRAMVerifier(stored); err != nil {
			b.Fatal(err)
		}
	}
}

// echoPooler answers each Sync of a Bind/Describe/Execute/Sync batch with
// the responses of a one-row SELECT, isolating the router->pooler gRPC hop
// (proto encode, HTTP/2 framing, decode) without PostgreSQL.
type echoPooler struct {
	pgshardv1.UnimplementedPoolerServer
}

func (echoPooler) Execute(stream grpc.BidiStreamingServer[pgshardv1.ExecuteRequest, pgshardv1.ExecuteResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		if _, ok := req.Message.(*pgshardv1.ExecuteRequest_Sync); !ok {
			continue
		}
		for _, resp := range []*pgshardv1.ExecuteResponse{
			{Message: &pgshardv1.ExecuteResponse_BindComplete{BindComplete: &pgshardv1.BindComplete{}}},
			{Message: &pgshardv1.ExecuteResponse_RowDescription{RowDescription: &pgshardv1.RowDescription{
				Fields: []*pgshardv1.FieldDescription{{Name: "abalance", TypeOid: 23, TypeSize: 4}}}}},
			{Message: &pgshardv1.ExecuteResponse_DataRow{DataRow: &pgshardv1.DataRow{
				Columns: []*pgshardv1.Value{{Data: []byte("12345")}}}}},
			{Message: &pgshardv1.ExecuteResponse_CommandComplete{CommandComplete: &pgshardv1.CommandComplete{Tag: "SELECT 1"}}},
			{Message: &pgshardv1.ExecuteResponse_ReadyForQuery{ReadyForQuery: &pgshardv1.ReadyForQuery{
				TxnStatus: pgshardv1.ReadyForQuery_TXN_STATUS_IDLE}}},
		} {
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

// BenchmarkGRPCPoolerHop is one prepared transaction over the Execute
// stream: 3 requests out, 5 responses back, in-memory transport.
func BenchmarkGRPCPoolerHop(b *testing.B) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pgshardv1.RegisterPoolerServer(srv, echoPooler{})
	go func() { _ = srv.Serve(lis) }()
	b.Cleanup(srv.Stop)
	cc, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.DialContext(context.Background()) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = cc.Close() })
	stream, err := pgshardv1.NewPoolerClient(cc).Execute(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		reqs := []*pgshardv1.ExecuteRequest{
			{SessionId: "s1", Message: &pgshardv1.ExecuteRequest_Bind{Bind: &pgshardv1.Bind{
				Statement: "P0_1", Params: []*pgshardv1.Value{{Data: []byte("424242")}}}}},
			{SessionId: "s1", Message: &pgshardv1.ExecuteRequest_Execute{Execute: &pgshardv1.ExecutePortal{}}},
			{SessionId: "s1", Message: &pgshardv1.ExecuteRequest_Sync{Sync: &pgshardv1.Sync{}}},
		}
		for _, r := range reqs {
			if err := stream.Send(r); err != nil {
				b.Fatal(err)
			}
		}
		for range 5 {
			if _, err := stream.Recv(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkParserCacheMiss is an uncached parse through Parser.Parse, the
// entry the router actually calls. BenchmarkParseUncached above calls the
// package-level Parse directly and so measures the parser alone, missing
// the cache bookkeeping and the context handling around it.
func BenchmarkParserCacheMiss(b *testing.B) {
	p := pgparser.New(pgparser.Options{CacheEntries: 4096, CacheBytes: 32 << 20})
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		i++
		sql := fmt.Sprintf("SELECT abalance FROM pgbench_accounts WHERE aid = %d", i)
		if _, err := p.Parse(ctx, sql); err != nil {
			b.Fatal(err)
		}
	}
}
