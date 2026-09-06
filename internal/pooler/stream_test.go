package pooler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgoutput"
)

func relationMsg(id uint32) *pgoutput.Relation {
	return &pgoutput.Relation{ID: id, Namespace: "public", Name: "t", ReplicaIdentity: 'd',
		Columns: []pgoutput.Column{{Key: true, Name: "id", TypeOID: 23, TypeMod: -1}, {Name: "v", TypeOID: 25, TypeMod: -1}}}
}

func decoderWith(t *testing.T, rel *pgoutput.Relation) *pgoutput.Decoder {
	t.Helper()
	d := pgoutput.NewDecoder()
	// Feed a real Relation message so the cache is populated.
	raw := []byte{'R', 0, 0, 0, byte(rel.ID), 'p', 'u', 'b', 'l', 'i', 'c', 0, 't', 0, 'd', 0, 2,
		1, 'i', 'd', 0, 0, 0, 0, 23, 0xff, 0xff, 0xff, 0xff,
		0, 'v', 0, 0, 0, 0, 25, 0xff, 0xff, 0xff, 0xff}
	if _, err := d.Decode(raw); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestConvertRows(t *testing.T) {
	d := decoderWith(t, relationMsg(7))
	text := func(s string) pgoutput.TupleColumn {
		return pgoutput.TupleColumn{Kind: pgoutput.ColumnText, Data: []byte(s)}
	}
	ev, boundary, err := convert(d, &pgoutput.Insert{Xid: 3, RelationID: 7, New: pgoutput.Tuple{Columns: []pgoutput.TupleColumn{text("1"), {Kind: pgoutput.ColumnUnchanged}}}}, 99)
	if err != nil || boundary || ev.Lsn != 99 || ev.Xid != 3 {
		t.Fatalf("insert: %v %v %v", ev, boundary, err)
	}
	row := ev.GetRow()
	if row.Kind != pgshardv1.ChangeEvent_Row_KIND_INSERT || row.Schema != "public" || row.Table != "t" || row.RelationId != 7 ||
		len(row.New) != 2 || string(row.New[0].Data) != "1" || len(row.UnchangedToast) != 1 || row.UnchangedToast[0] != 1 || row.New[1].Null {
		t.Fatalf("insert row: %v", row)
	}
	key := &pgoutput.Tuple{Columns: []pgoutput.TupleColumn{text("1"), {Kind: pgoutput.ColumnNull}}}
	ev, _, err = convert(d, &pgoutput.Update{RelationID: 7, Key: key, New: pgoutput.Tuple{Columns: []pgoutput.TupleColumn{text("2"), text("x")}}}, 1)
	if err != nil || ev.GetRow().Kind != pgshardv1.ChangeEvent_Row_KIND_UPDATE || !ev.GetRow().OldIsKey || !ev.GetRow().Old[1].Null {
		t.Fatalf("update: %v %v", ev, err)
	}
	ev, _, err = convert(d, &pgoutput.Delete{RelationID: 7, Old: key}, 1)
	if err != nil || ev.GetRow().Kind != pgshardv1.ChangeEvent_Row_KIND_DELETE || ev.GetRow().OldIsKey || len(ev.GetRow().Old) != 2 || ev.GetRow().New != nil {
		t.Fatalf("delete: %v %v", ev, err)
	}
	if _, _, err := convert(d, &pgoutput.Insert{RelationID: 8}, 1); err == nil {
		t.Fatal("unknown relation accepted")
	}
	if ev, _, err := convert(d, &pgoutput.Type{}, 1); err != nil || ev != nil {
		t.Fatal("type message must be dropped")
	}
	ev, _, _ = convert(d, relationMsg(9), 5)
	if r := ev.GetRelation(); r.RelationId != 9 || len(r.Columns) != 2 || !r.Columns[0].Key || r.Columns[0].TypeOid != 23 || r.ReplicaIdentity != "d" {
		t.Fatalf("relation: %v", r)
	}
}

func TestConvertBoundaries(t *testing.T) {
	d := pgoutput.NewDecoder()
	boundaries := map[pgoutput.Message]bool{
		&pgoutput.Begin{Xid: 1}:                       false,
		&pgoutput.Commit{CommitLSN: 1, EndLSN: 2}:     true,
		&pgoutput.Origin{Name: "o"}:                   false,
		&pgoutput.Truncate{RelationIDs: []uint32{1}}:  false,
		&pgoutput.LogicalMessage{Transactional: true}: false,
		&pgoutput.LogicalMessage{}:                    true,
		&pgoutput.StreamStart{Xid: 1}:                 false,
		&pgoutput.StreamStop{}:                        true,
		&pgoutput.StreamCommit{Xid: 1}:                true,
		&pgoutput.StreamAbort{Xid: 1, SubXid: 2}:      true,
		&pgoutput.BeginPrepare{Gid: "g"}:              false,
		&pgoutput.Prepare{Gid: "g"}:                   true,
		&pgoutput.CommitPrepared{Gid: "g"}:            true,
		&pgoutput.RollbackPrepared{Gid: "g"}:          true,
		&pgoutput.StreamPrepare{Gid: "g"}:             true,
	}
	for m, want := range boundaries {
		ev, got, err := convert(d, m, 1)
		if err != nil || ev == nil || ev.Event == nil {
			t.Fatalf("%T: %v %v", m, ev, err)
		}
		if got != want {
			t.Errorf("%T boundary %t want %t", m, got, want)
		}
	}
	ev, _, _ := convert(d, &pgoutput.StreamAbort{Xid: 1, SubXid: 2, AbortLSN: 3}, 1)
	if a := ev.GetStreamAbort(); a.Xid != 1 || a.Subxid != 2 || a.AbortLsn != 3 || ev.Xid != 1 {
		t.Fatalf("abort: %v", ev)
	}
	ev, _, _ = convert(d, &pgoutput.RollbackPrepared{Gid: "g", RollbackEndLSN: 9}, 1)
	if ev.GetRollbackPrepared().RollbackLsn != 9 {
		t.Fatalf("rollback prepared: %v", ev)
	}
	ev, _, _ = convert(d, &pgoutput.LogicalMessage{Prefix: "p", Content: []byte("c"), Transactional: true}, 1)
	if m := ev.GetMessage(); m.Prefix != "p" || string(m.Content) != "c" || !m.Transactional {
		t.Fatalf("message: %v", ev)
	}
	type unknown struct{ pgoutput.Message }
	if _, _, err := convert(d, unknown{}, 1); err == nil {
		t.Fatal("unknown message accepted")
	}
}

func TestBatcher(t *testing.T) {
	var sent []*pgshardv1.ChangeBatch
	b := &batcher{emit: func(cb *pgshardv1.ChangeBatch) error { sent = append(sent, cb); return nil }, max: 100}
	row := func(n int) *pgshardv1.ChangeEvent {
		return &pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_Row_{Row: &pgshardv1.ChangeEvent_Row{New: []*pgshardv1.Value{{Data: make([]byte, n)}}}}}
	}
	if err := b.add(row(10), 5, false); err != nil || len(sent) != 0 || b.empty() {
		t.Fatal("no flush before a boundary")
	}
	if err := b.add(row(10), 3, true); err != nil || len(sent) != 1 || len(sent[0].Events) != 2 || sent[0].EndLsn != 5 || !b.empty() {
		t.Fatalf("boundary flush: %v", sent)
	}
	if err := b.add(row(200), 7, false); err != nil || len(sent) != 2 || sent[1].EndLsn != 7 {
		t.Fatal("size cap flush")
	}
	if err := b.flush(); err != nil || len(sent) != 2 {
		t.Fatal("empty flush must not emit")
	}
	pe := &batcher{emit: func(cb *pgshardv1.ChangeBatch) error { sent = append(sent, cb); return nil }, max: 100, perEvent: true}
	if err := pe.add(row(1), 1, false); err != nil || len(sent) != 3 {
		t.Fatal("perEvent flush")
	}
	if err := b.sendNow(row(1), 9); err != nil || sent[3].EndLsn != 9 || len(sent[3].Events) != 1 {
		t.Fatal("sendNow")
	}
	msg := &pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_Message_{Message: &pgshardv1.ChangeEvent_Message{Content: make([]byte, 50)}}}
	if eventBytes(msg) < 50 || eventBytes(row(10)) < 10 {
		t.Fatal("eventBytes ignores payload")
	}
}

func TestStreamRefusals(t *testing.T) {
	s := NewServer(Config{Source: NewStaticSource(View{})})
	if _, err := s.slotOf("", "Bad Name"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad stream name: %v", err)
	}
	if slot, err := s.slotOf("explicit", "x"); err != nil || slot != "explicit" {
		t.Fatal("explicit slot wins")
	}
	s.cfg.Stream.Shard = "shard0"
	if slot, _ := s.slotOf("", "orders"); slot != "pgshard_orders_shard0" {
		t.Fatal(slot)
	}
	err := s.runStream(context.Background(), &pgshardv1.StreamRequest{Stream: "orders"}, nil, false)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("no DSN: %v", err)
	}
	s.cfg.Stream.DSN = "postgres://localhost/x"
	s.draining.Store(true)
	if err := s.runStream(context.Background(), &pgshardv1.StreamRequest{Stream: "orders"}, nil, false); status.Code(err) != codes.Unavailable {
		t.Fatalf("draining: %v", err)
	}
	s.draining.Store(false)
	r, err := s.claimSlot("a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.claimSlot("a", 0); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second claim: %v", err)
	}
	s.releaseSlot("a", &streamReader{})
	if _, err := s.claimSlot("a", 0); err == nil {
		t.Fatal("release by a different reader must not free the slot")
	}
	s.releaseSlot("a", r)
	if _, err := s.claimSlot("a", 0); err != nil {
		t.Fatal("slot not released")
	}
	d := s.streamDefaults()
	if d.Heartbeat != 5*time.Second || d.ReceiveTimeout != 250*time.Millisecond || d.MaxBatchBytes != 64<<10 {
		t.Fatalf("defaults: %+v", d)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Ack(ctx, &pgshardv1.AckRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ack without slot: %v", err)
	}
	unconfirmed := &streamReader{wake: make(chan struct{}, 1)}
	unconfirmed.delivered.Store(5)
	s.mu.Lock()
	s.readers["b"] = unconfirmed
	s.mu.Unlock()
	if _, err := s.Ack(ctx, &pgshardv1.AckRequest{Slot: "b", Lsn: 5}); err == nil {
		t.Fatal("ack must give up when no reader confirms")
	}
}

func TestAckClampsToDelivered(t *testing.T) {
	s := NewServer(Config{Source: NewStaticSource(View{})})
	r := &streamReader{wake: make(chan struct{}, 1)}
	r.delivered.Store(100)
	r.flushed.Store(100)
	s.mu.Lock()
	s.readers = map[string]*streamReader{"b": r}
	s.mu.Unlock()
	resp, err := s.Ack(context.Background(), &pgshardv1.AckRequest{Slot: "b", Lsn: 250})
	if err != nil || resp.GetError() != nil {
		t.Fatalf("over-ack: %v %v", resp, err)
	}
	if got := r.acked.Load(); got != 100 {
		t.Fatalf("acked %d, want clamped to delivered 100", got)
	}
	r.delivered.Store(300)
	r.flushed.Store(300)
	if _, err := s.Ack(context.Background(), &pgshardv1.AckRequest{Slot: "b", Lsn: 250}); err != nil {
		t.Fatal(err)
	}
	if got := r.acked.Load(); got != 250 {
		t.Fatalf("acked %d, want 250", got)
	}
}

// TestStreamRejectsUnsafeSlotAndOptionNames: START_REPLICATION is a
// replication-protocol command built as text on the pooler's superuser
// connection, and neither the slot name nor an option's key can be sent as
// a parameter. slotOf returned a caller-supplied slot verbatim and the
// option key half was rendered literally while only the value was quoted,
// so both were a route into that query.
func TestStreamRejectsUnsafeSlotAndOptionNames(t *testing.T) {
	s := NewServer(Config{Source: NewStaticSource(View{})})
	for _, slot := range []string{
		"has space", "UPPER", "has-dash", "quote'", "semi;colon", "paren)", "back\\slash", "new\nline", "",
	} {
		if slot == "" {
			continue
		}
		if _, err := s.slotOf(slot, ""); status.Code(err) != codes.InvalidArgument {
			t.Errorf("slot %q was accepted: %v", slot, err)
		}
	}
	for _, slot := range []string{"pgshard_orders_shard0", "a", "s_1", strings.Repeat("a", 63)} {
		if got, err := s.slotOf(slot, ""); err != nil || got != slot {
			t.Errorf("slot %q was rejected: %v", slot, err)
		}
	}
	if _, err := s.slotOf(strings.Repeat("a", 64), ""); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a 64-character slot name was accepted: %v", err)
	}
	for _, key := range []string{"has space", "UPPER", "1leading", "quote'", "paren)", "comma,"} {
		if optionKeyRE.MatchString(key) {
			t.Errorf("option key %q was accepted", key)
		}
	}
	for _, key := range []string{"proto_version", "publication_names", "streaming", "messages", "_x"} {
		if !optionKeyRE.MatchString(key) {
			t.Errorf("option key %q was rejected; the pgoutput options pgshard itself sets must pass", key)
		}
	}
}

// TestOnlyAGonePositionSaysPositionTooOld: a consumer told POSITION_TOO_OLD
// throws its checkpoints away and copies everything again. Saying it about
// a missing publication or a rejected option buys a full re-snapshot for a
// configuration mistake, so the reason belongs only to the failures that
// mean the position itself is gone.
func TestOnlyAGonePositionSaysPositionTooOld(t *testing.T) {
	gone := []*pgconn.PgError{
		{Code: "55000", Message: `can no longer get changes from replication slot "s"`},
		{Code: "55000", Message: `replication slot "s" has been invalidated`},
		{Code: "42704", Message: `replication slot "s" does not exist`},
		{Code: "58P01", Message: `requested WAL segment 000000010000000000000002 has already been removed`},
		{Code: "XX000", Message: `requested WAL segment has already been removed`},
	}
	for _, e := range gone {
		if !positionGone(e) {
			t.Errorf("%s %q: want the position reported gone", e.Code, e.Message)
		}
	}
	kept := []*pgconn.PgError{
		{Code: "42704", Message: `publication "pgshard_all" does not exist`},
		{Code: "42501", Message: `permission denied for replication`},
		{Code: "22023", Message: `option "proto_version" is not supported`},
		{Code: "55000", Message: `replication slot "s" is active for PID 42`},
		{Code: "XX000", Message: `internal error`},
	}
	for _, e := range kept {
		if positionGone(e) {
			t.Errorf("%s %q: a consumer must not be told to re-snapshot for this", e.Code, e.Message)
		}
	}
}

// TestAnAckAtTheStartPositionIsNotClampedToNothing: a reader began with
// delivered == 0, so an ack that arrived before its first batch -- which is
// exactly what a router sends after reconnecting at the position it already
// holds -- was clamped to zero, reported as confirmed, and never asked for
// again. The slot stayed where it was and the shard's WAL was retained until
// the next commit on it.
func TestAnAckAtTheStartPositionIsNotClampedToNothing(t *testing.T) {
	s := &Server{}
	s.cfg.Stream.Shard = "shard0"
	r, err := s.claimSlot("pgshard_orders_shard0", 4000)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.delivered.Load(); got != 4000 {
		t.Fatalf("a reader claimed at 4000 starts delivered at %d", got)
	}
	// The reader confirms whatever it is asked for, as the real one does
	// once it has sent its standby status.
	go func() {
		<-r.wake
		r.flushed.Store(r.acked.Load())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := s.Ack(ctx, &pgshardv1.AckRequest{Stream: "orders", Lsn: 4000})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetError() != nil {
		t.Fatalf("ack refused: %v", resp.GetError())
	}
	if resp.GetConfirmedLsn() != 4000 {
		t.Fatalf("confirmed %d, want the 4000 that was asked for", resp.GetConfirmedLsn())
	}
}

// TestAnAckBeyondWhatWasDeliveredSaysWhatItConfirmed: the clamp is right --
// confirmed_flush must not overtake what the client has seen -- but the
// caller has to be told, or it records the LSN it asked for as done and
// never asks again.
func TestAnAckBeyondWhatWasDeliveredSaysWhatItConfirmed(t *testing.T) {
	s := &Server{}
	s.cfg.Stream.Shard = "shard0"
	r, err := s.claimSlot("pgshard_orders_shard0", 100)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		<-r.wake
		r.flushed.Store(r.acked.Load())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := s.Ack(ctx, &pgshardv1.AckRequest{Stream: "orders", Lsn: 9999})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetConfirmedLsn() != 100 {
		t.Fatalf("confirmed %d, want the delivered 100", resp.GetConfirmedLsn())
	}
}
