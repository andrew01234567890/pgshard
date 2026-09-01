package vstream

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// fakePooler serves Stream and Ack for any slot: batches are fed per stream
// name through feed, ended with finish, and every Stream call and Ack is
// recorded.
type fakePooler struct {
	pgshardv1.UnimplementedPoolerServer
	mu      sync.Mutex
	feeds   map[string]chan feedItem
	starts  []uint64
	acks    map[string][]uint64
	ackWake chan struct{}
	client  pgshardv1.PoolerClient
	// copyPlan answers CopyTables: the messages to send and an error to end
	// with after failAfter messages (failAfter < 0 sends everything).
	copyPlan  func(req *pgshardv1.CopyTablesRequest) copyScript
	copyReqs  []*pgshardv1.CopyTablesRequest
	copyStart chan struct{}
}

type copyScript struct {
	msgs      []*pgshardv1.CopyTablesResponse
	failAfter int
	err       error
}

func (f *fakePooler) CopyTables(req *pgshardv1.CopyTablesRequest, srv pgshardv1.Pooler_CopyTablesServer) error {
	f.mu.Lock()
	f.copyReqs = append(f.copyReqs, req)
	plan := f.copyPlan
	f.mu.Unlock()
	select {
	case f.copyStart <- struct{}{}:
	default:
	}
	if plan == nil {
		return status.Error(codes.Unimplemented, "no copy plan")
	}
	sc := plan(req)
	for i, m := range sc.msgs {
		if sc.failAfter >= 0 && i == sc.failAfter {
			return sc.err
		}
		if err := srv.Send(m); err != nil {
			return err
		}
	}
	if sc.failAfter >= len(sc.msgs) {
		return sc.err
	}
	return nil
}

func (f *fakePooler) copyRequests() []*pgshardv1.CopyTablesRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pgshardv1.CopyTablesRequest(nil), f.copyReqs...)
}

// Copy message builders.

func cpSnapshot(lsn uint64, streamSlot bool) *pgshardv1.CopyTablesResponse {
	return &pgshardv1.CopyTablesResponse{Response: &pgshardv1.CopyTablesResponse_Snapshot_{Snapshot: &pgshardv1.CopyTablesResponse_Snapshot{Slot: "s", StreamSlot: streamSlot, ConsistentPoint: lsn, SnapshotName: "snap"}}}
}

func cpTable(table string, cols ...string) *pgshardv1.CopyTablesResponse {
	return &pgshardv1.CopyTablesResponse{Response: &pgshardv1.CopyTablesResponse_TableBegin_{TableBegin: &pgshardv1.CopyTablesResponse_TableBegin{Relation: evRelation(0, table, cols...).GetRelation()}}}
}

// cpKeylessTable is a table the pooler reports as paginated by ctid,
// because it has no unique key to resume from.
func cpKeylessTable(table string, cols ...string) *pgshardv1.CopyTablesResponse {
	m := cpTable(table, cols...)
	m.GetTableBegin().ByCtid = true
	return m
}

func cpRows(lastpk string, ids ...string) *pgshardv1.CopyTablesResponse {
	rows := &pgshardv1.CopyTablesResponse_Rows{Lastpk: []byte(lastpk)}
	for _, id := range ids {
		rows.Rows = append(rows.Rows, &pgshardv1.CopyTablesResponse_Row{Values: []*pgshardv1.Value{{Data: []byte(id)}, {Null: true}}})
	}
	return &pgshardv1.CopyTablesResponse{Response: &pgshardv1.CopyTablesResponse_Rows_{Rows: rows}}
}

func cpTableDone(table string) *pgshardv1.CopyTablesResponse {
	return &pgshardv1.CopyTablesResponse{Response: &pgshardv1.CopyTablesResponse_TableDone_{TableDone: &pgshardv1.CopyTablesResponse_TableDone{Schema: "public", Table: table}}}
}

func cpDone() *pgshardv1.CopyTablesResponse {
	return &pgshardv1.CopyTablesResponse{Response: &pgshardv1.CopyTablesResponse_Done_{Done: &pgshardv1.CopyTablesResponse_Done{}}}
}

func script(msgs ...*pgshardv1.CopyTablesResponse) copyScript {
	return copyScript{msgs: msgs, failAfter: -1}
}

type feedItem struct {
	batch *pgshardv1.ChangeBatch
	err   error
}

func newFakePooler(t *testing.T) *fakePooler {
	t.Helper()
	f := &fakePooler{feeds: map[string]chan feedItem{}, acks: map[string][]uint64{}, ackWake: make(chan struct{}, 64), copyStart: make(chan struct{}, 64)}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	pgshardv1.RegisterPoolerServer(g, f)
	go func() { _ = g.Serve(l) }()
	cc, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close(); g.Stop() })
	f.client = pgshardv1.NewPoolerClient(cc)
	return f
}

func (f *fakePooler) feedOf(stream string) chan feedItem {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch, ok := f.feeds[stream]
	if !ok {
		ch = make(chan feedItem, 256)
		f.feeds[stream] = ch
	}
	return ch
}

func (f *fakePooler) feed(stream string, b *pgshardv1.ChangeBatch) {
	f.feedOf(stream) <- feedItem{batch: b}
}

// fail ends the current Stream of the plain stream with err.
func (f *fakePooler) fail(err error) { f.feedOf("plain") <- feedItem{err: err} }

func (f *fakePooler) Stream(req *pgshardv1.StreamRequest, srv pgshardv1.Pooler_StreamServer) error {
	f.mu.Lock()
	f.starts = append(f.starts, req.GetStartLsn())
	f.mu.Unlock()
	ch := f.feedOf(req.GetStream())
	for {
		select {
		case <-srv.Context().Done():
			return nil
		case it := <-ch:
			if it.err != nil {
				return it.err
			}
			if err := srv.Send(it.batch); err != nil {
				return err
			}
		}
	}
}

func (f *fakePooler) Ack(_ context.Context, req *pgshardv1.AckRequest) (*pgshardv1.AckResponse, error) {
	f.mu.Lock()
	f.acks[req.GetStream()] = append(f.acks[req.GetStream()], req.GetLsn())
	f.mu.Unlock()
	select {
	case f.ackWake <- struct{}{}:
	default:
	}
	return &pgshardv1.AckResponse{}, nil
}

func (f *fakePooler) startLSNs() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.starts...)
}

func (f *fakePooler) ackedLSNs() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.acks["plain"]...)
}

// fakeTopology maps every shard to a pooler and lets tests bump epochs,
// swap poolers and change the shard map generation.
type fakeTopology struct {
	mu      sync.Mutex
	shards  []router.Shard
	gen     uint64
	epochs  map[router.Shard]uint64
	poolers map[router.Shard]*fakePooler
	// serving is what an omitted shard set resolves to; empty means the
	// default set, which is what a cluster that never resharded reports.
	serving string
}

func (t *fakeTopology) ServingSet() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.serving == "" {
		return router.DefaultShardSet
	}
	return t.serving
}

func (t *fakeTopology) Shards(set string) []router.Shard {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []router.Shard
	for _, sh := range t.shards {
		if sh.Set == set {
			out = append(out, sh)
		}
	}
	return out
}

func (t *fakeTopology) Generation() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.gen
}

func (t *fakeTopology) Epoch(sh router.Shard) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.epochs[sh]
}

func (t *fakeTopology) Client(sh router.Shard) (pgshardv1.PoolerClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.poolers[sh].client, nil
}

func (t *fakeTopology) promote(sh router.Shard, p *fakePooler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.epochs[sh]++
	t.poolers[sh] = p
}

func (t *fakeTopology) reshard() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gen++
}

type fakeCatalog struct{ streams map[string]catalog.Stream }

func (c fakeCatalog) Lookup(_ context.Context, name string) (catalog.Stream, error) {
	st, ok := c.streams[name]
	if !ok {
		return catalog.Stream{}, ErrUnknownStream
	}
	return st, nil
}

func (c fakeCatalog) List(context.Context) ([]catalog.Stream, []catalog.StreamStatus, error) {
	var out []catalog.Stream
	for _, st := range c.streams {
		out = append(out, st)
	}
	return out, []catalog.StreamStatus{{Stream: "orders", ShardSet: "default", ShardID: 0, Slot: "pgshard_orders_shard0", WALStatus: "reserved"}}, nil
}

// harness is a VStream server over fake poolers with a connected client.
type harness struct {
	t      *testing.T
	topo   *fakeTopology
	pool   []*fakePooler
	server *Server
	client pgshardv1.VStreamClient
}

var (
	shard0 = router.Shard{Set: "default", ID: 0}
	shard1 = router.Shard{Set: "default", ID: 1}
)

func newHarness(t *testing.T, shards int) *harness {
	t.Helper()
	h := &harness{t: t, topo: &fakeTopology{gen: 7, epochs: map[router.Shard]uint64{}, poolers: map[router.Shard]*fakePooler{}}}
	for i := 0; i < shards; i++ {
		sh := router.Shard{Set: "default", ID: int32(i)}
		p := newFakePooler(t)
		h.pool = append(h.pool, p)
		h.topo.shards = append(h.topo.shards, sh)
		h.topo.epochs[sh] = 1
		h.topo.poolers[sh] = p
	}
	h.server = &Server{Topology: h.topo, Catalog: fakeCatalog{streams: map[string]catalog.Stream{
		"orders": {Name: "orders", Database: "app", TwoPhase: true, State: catalog.StreamActive},
		"plain":  {Name: "plain", Database: "app", State: catalog.StreamActive},
	}}, BufferUnits: 4}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	pgshardv1.RegisterVStreamServer(g, h.server)
	go func() { _ = g.Serve(l) }()
	cc, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close(); g.Stop() })
	h.client = pgshardv1.NewVStreamClient(cc)
	return h
}

func (h *harness) open(ctx context.Context, start *pgshardv1.VStreamRequest_Start) pgshardv1.VStream_StreamClient {
	h.t.Helper()
	st, err := h.client.Stream(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Start_{Start: start}}); err != nil {
		h.t.Fatal(err)
	}
	return st
}

// Event builders for fake batches.

func evBegin(xid uint32, ts int64) *pgshardv1.ChangeEvent {
	return &pgshardv1.ChangeEvent{Xid: xid, Event: &pgshardv1.ChangeEvent_Begin_{Begin: &pgshardv1.ChangeEvent_Begin{Xid: xid, CommitTs: ts}}}
}

func evRelation(id uint32, table string, cols ...string) *pgshardv1.ChangeEvent {
	rel := &pgshardv1.ChangeEvent_Relation{RelationId: id, Schema: "public", Table: table, ReplicaIdentity: "d"}
	for i, c := range cols {
		rel.Columns = append(rel.Columns, &pgshardv1.ChangeEvent_Relation_Column{Name: c, TypeOid: 23, TypeModifier: -1, Key: i == 0})
	}
	return &pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_Relation_{Relation: rel}}
}

func evInsert(rel uint32, xid uint32, vals ...string) *pgshardv1.ChangeEvent {
	row := &pgshardv1.ChangeEvent_Row{Schema: "public", Kind: pgshardv1.ChangeEvent_Row_KIND_INSERT, RelationId: rel}
	for _, v := range vals {
		row.New = append(row.New, &pgshardv1.Value{Data: []byte(v)})
	}
	return &pgshardv1.ChangeEvent{Xid: xid, Event: &pgshardv1.ChangeEvent_Row_{Row: row}}
}

func evCommit(lsn, end uint64) *pgshardv1.ChangeEvent {
	return &pgshardv1.ChangeEvent{Lsn: lsn, Event: &pgshardv1.ChangeEvent_Commit_{Commit: &pgshardv1.ChangeEvent_Commit{CommitLsn: lsn, EndLsn: end}}}
}

func evKeepalive(end uint64) *pgshardv1.ChangeEvent {
	return &pgshardv1.ChangeEvent{Lsn: end, Event: &pgshardv1.ChangeEvent_Keepalive_{Keepalive: &pgshardv1.ChangeEvent_Keepalive{WalEnd: end}}}
}

func batch(end uint64, evs ...*pgshardv1.ChangeEvent) *pgshardv1.ChangeBatch {
	return &pgshardv1.ChangeBatch{Events: evs, EndLsn: end}
}

// txn is one whole insert transaction on relation rel in a single batch.
func txn(rel, xid uint32, ts int64, end uint64, vals ...string) *pgshardv1.ChangeBatch {
	return batch(end, evBegin(xid, ts), evInsert(rel, 0, vals...), evCommit(end-8, end))
}
