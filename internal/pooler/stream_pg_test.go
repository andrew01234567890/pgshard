package pooler

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func (h *pgHarness) testStream(t *testing.T) {
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE PUBLICATION pgshard_all FOR ALL TABLES",
		"CREATE TABLE orders (id int primary key, name text)",
		"SELECT pg_create_logical_replication_slot('pgshard_orders_shard0', 'pgoutput', false, true, true)",
		"INSERT INTO orders VALUES (1, 'one')",
		"INSERT INTO orders VALUES (2, 'two')",
		"BEGIN; INSERT INTO orders VALUES (3, 'prepared'); PREPARE TRANSACTION 'g1'",
		"COMMIT PREPARED 'g1'",
	} {
		if _, err := h.admin.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		_, _ = h.admin.Exec(ctx, "SELECT pg_drop_replication_slot('pgshard_orders_shard0')")
	})

	if _, err := h.client.Ack(ctx, &pgshardv1.AckRequest{Generation: gen(3, 1)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ack without slot: %v", err)
	}
	if r, err := h.client.Ack(ctx, &pgshardv1.AckRequest{Stream: "orders", Lsn: 1, Generation: gen(3, 1)}); err != nil || r.GetError() == nil {
		t.Fatalf("ack without reader: %v %v", r, err)
	}
	if s, err := h.client.Stream(ctx, &pgshardv1.StreamRequest{Slot: "nope", Generation: gen(3, 1)}); err == nil {
		if _, err := s.Recv(); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("missing slot: %v", err)
		}
	}

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := h.client.Stream(sctx, &pgshardv1.StreamRequest{Stream: "orders", Options: map[string]string{"two_phase": "on"}, Generation: gen(3, 1)})
	if err != nil {
		t.Fatal(err)
	}
	recv := func(s pgshardv1.Pooler_StreamClient) *pgshardv1.ChangeBatch {
		t.Helper()
		b, err := s.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		return b
	}
	first := recv(stream)
	kinds := func(b *pgshardv1.ChangeBatch) []string {
		var out []string
		for _, e := range b.Events {
			out = append(out, fmt.Sprintf("%T", e.Event))
		}
		return out
	}
	if len(first.Events) != 4 || first.GetEvents()[0].GetBegin() == nil || first.GetEvents()[1].GetRelation() == nil ||
		first.GetEvents()[2].GetRow() == nil || first.GetEvents()[3].GetCommit() == nil || first.EndLsn == 0 {
		t.Fatalf("first batch: %v", kinds(first))
	}
	row := first.GetEvents()[2].GetRow()
	if row.Schema != "public" || row.Table != "orders" || row.Kind != pgshardv1.ChangeEvent_Row_KIND_INSERT || len(row.New) != 2 ||
		string(row.New[0].Data) != "1" || string(row.New[1].Data) != "one" || row.RelationId == 0 {
		t.Fatalf("row: %v", row)
	}
	rel := first.GetEvents()[1].GetRelation()
	if rel.Table != "orders" || len(rel.Columns) != 2 || !rel.Columns[0].Key || rel.Columns[1].Key || rel.ReplicaIdentity != "d" {
		t.Fatalf("relation: %v", rel)
	}
	if first.GetEvents()[0].GetBegin().GetXid() == 0 || first.GetEvents()[0].Xid == 0 {
		t.Fatalf("begin xid: %v", first.GetEvents()[0])
	}
	second := recv(stream)
	if len(second.Events) != 3 || string(second.GetEvents()[1].GetRow().GetNew()[0].Data) != "2" {
		t.Fatalf("second batch: %v", kinds(second))
	}
	prep := recv(stream)
	if n := len(prep.Events); n != 3 || prep.GetEvents()[0].GetBeginPrepare().GetGid() != "g1" || prep.GetEvents()[2].GetPrepare().GetGid() != "g1" {
		t.Fatalf("prepare batch: %v", kinds(prep))
	}
	cp := recv(stream)
	if len(cp.Events) != 1 || cp.GetEvents()[0].GetCommitPrepared().GetGid() != "g1" {
		t.Fatalf("commit prepared batch: %v", kinds(cp))
	}

	// A second reader on the same slot is refused.
	if dup, err := h.client.Stream(ctx, &pgshardv1.StreamRequest{Stream: "orders", Generation: gen(3, 1)}); err == nil {
		if _, err := dup.Recv(); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("concurrent reader: %v", err)
		}
	}

	// Heartbeats arrive while idle.
	hb := recv(stream)
	if len(hb.Events) != 1 || hb.GetEvents()[0].GetKeepalive() == nil {
		t.Fatalf("heartbeat: %v", kinds(hb))
	}

	ack, err := h.client.Ack(ctx, &pgshardv1.AckRequest{Stream: "orders", Lsn: cp.EndLsn, Generation: gen(3, 1)})
	if err != nil || ack.GetError() != nil {
		t.Fatalf("ack: %v %v", ack, err)
	}
	// The ack reaches the walsender as a standby status update, which
	// PostgreSQL applies to the slot on its own schedule: the RPC returning
	// says the ack was sent, not that the slot has moved.
	var confirmed uint64
	for deadline := time.Now().Add(5 * time.Second); ; {
		if err := h.admin.QueryRow(ctx, "SELECT confirmed_flush_lsn - '0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name = 'pgshard_orders_shard0'").Scan(&confirmed); err != nil {
			t.Fatal(err)
		}
		if confirmed >= cp.EndLsn {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("confirmed_flush_lsn %d < acked %d", confirmed, cp.EndLsn)
		}
		time.Sleep(10 * time.Millisecond)
	}
	const overAck = uint64(1) << 62
	if ack, err = h.client.Ack(ctx, &pgshardv1.AckRequest{Stream: "orders", Lsn: overAck, Generation: gen(3, 1)}); err != nil || ack.GetError() != nil {
		t.Fatalf("over-ack: %v %v", ack, err)
	}
	// A single read here would pass whether the over-ack was clamped or
	// merely not applied yet, so hold the invariant over a window instead.
	var walEnd uint64
	for settle := time.Now().Add(time.Second); time.Now().Before(settle); time.Sleep(50 * time.Millisecond) {
		if err := h.admin.QueryRow(ctx, "SELECT confirmed_flush_lsn - '0/0'::pg_lsn, pg_current_wal_lsn() - '0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name = 'pgshard_orders_shard0'").Scan(&confirmed, &walEnd); err != nil {
			t.Fatal(err)
		}
		if confirmed > walEnd || confirmed >= overAck {
			t.Fatalf("over-ack moved confirmed_flush_lsn to %d (wal end %d)", confirmed, walEnd)
		}
	}
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.srv.mu.Lock()
		n := len(h.srv.readers)
		h.srv.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reader not released after cancel")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A restarted reader resumes after the ack: only the new transaction shows.
	if _, err := h.admin.Exec(ctx, "INSERT INTO orders VALUES (4, 'after-ack')"); err != nil {
		t.Fatal(err)
	}
	stream2, err := h.client.Stream(ctx, &pgshardv1.StreamRequest{Slot: "pgshard_orders_shard0", BatchBytes: 1, Generation: gen(3, 1)})
	if err != nil {
		t.Fatal(err)
	}
	b := recv(stream2)
	if len(b.Events) != 1 || b.GetEvents()[0].GetBegin() == nil {
		t.Fatalf("resumed stream with batch_bytes=1 must deliver one event per batch: %v", kinds(b))
	}
	b = recv(stream2)
	if b.GetEvents()[0].GetRelation() == nil {
		t.Fatalf("resumed stream: %v", kinds(b))
	}
	b = recv(stream2)
	if r := b.GetEvents()[0].GetRow(); r == nil || string(r.GetNew()[0].Data) != "4" {
		t.Fatalf("resumed stream must start after the acked position: %v", kinds(b))
	}

	// The per-event RPC delivers the same stream one event at a time.
	single, err := h.client.StreamChanges(ctx, &pgshardv1.StreamRequest{Slot: "pgshard_orders_shard0", Generation: gen(3, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := single.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StreamChanges must be refused while Stream holds the slot: %v", err)
	}
}

// A change stream is a long-lived call, and the fence that matters for one
// is the one that ends it. The router's own check runs only after a batch
// has been received -- one batch too late, since those commits have already
// reached the consumer and a position has been recorded for them -- so the
// pooler re-checks its view on every pass of the receive loop.
//
// The first batch is taken before the view moves, deliberately: it proves
// the stream was established and running, so what ends it is the re-check
// and not the one at the open.
//
// The commit is made AFTER the view moves so that this asks the stronger
// question -- no change from beyond the fence may reach the consumer, not
// merely "the stream stops eventually". It still does not distinguish the
// loop's check from the one in the delivery path: with the second removed
// this passed five runs out of five, because the loop notices at the top of
// a pass before the commit's WAL arrives. PostgreSQL's own keepalives wake
// Receive often enough that the loop always gets there first.
//
// Pinning the delivery-path check would need a batch to arrive inside the
// very Receive the view moved under, and a client cannot place the move
// there: its view of where the server has got to is always behind what the
// server has already sent. It would mean injecting a fake replication
// connection into the stream path, which is a larger change than a
// one-batch window is worth.
func (h *pgHarness) testStreamEndsOnEpochChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Its own publication as well as its own slot: with only the slot this
	// subtest passed on keepalives alone when run by itself, because the
	// publication it decoded through belonged to another subtest.
	for _, sql := range []string{
		"create table fenced (id int primary key)",
		"create publication pgshard_fenced for table fenced",
		"select pg_create_logical_replication_slot('pgshard_fenced_shard0', 'pgoutput')",
	} {
		if _, err := h.admin.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		_, _ = h.admin.Exec(context.Background(), "select pg_drop_replication_slot('pgshard_fenced_shard0')")
		_, _ = h.admin.Exec(context.Background(), "drop publication if exists pgshard_fenced")
	})
	t.Cleanup(func() {
		h.src.Set(View{Generation: 3, Epoch: 1, Role: pgshardv1.HealthStatus_ROLE_PRIMARY})
	})

	stream, err := h.client.Stream(ctx, &pgshardv1.StreamRequest{Stream: "fenced", Publication: "pgshard_fenced", Generation: gen(3, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("the stream never started: %v", err)
	}
	// The shard fails over under the running stream, and only THEN is the
	// row committed: its WAL arrives inside a Receive the loop opened
	// before the view moved, which is the batch the loop's own check --
	// made before that Receive -- cannot cover. Not one commit from after
	// the move may reach the consumer; it would record a position on a
	// timeline the shard has left. A keepalive is tolerated: one may
	// already have been in the transport when the view moved.
	h.src.Set(View{Generation: 3, Epoch: 2, Role: pgshardv1.HealthStatus_ROLE_PRIMARY})
	if _, err := h.admin.Exec(ctx, "insert into fenced values (424242)"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		batch, err := stream.Recv()
		if err != nil {
			if !strings.Contains(err.Error(), "stale primary epoch") {
				t.Fatalf("the stream ended with %v, want the epoch fence", err)
			}
			return
		}
		for _, ev := range batch.GetEvents() {
			if ev.GetKeepalive() == nil {
				t.Fatalf("a change from after the epoch moved reached the consumer: %v", ev)
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the stream went on delivering after the shard's epoch moved")
		}
	}
}
