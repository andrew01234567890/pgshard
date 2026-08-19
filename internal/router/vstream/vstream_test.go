package vstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

var update = os.Getenv("UPDATE_GOLDEN") != ""

// describe renders one event as a stable line.
func describe(ev *pgshardv1.VEvent) string {
	switch e := ev.GetEvent().(type) {
	case *pgshardv1.VEvent_Begin_:
		s := fmt.Sprintf("begin shard=%d xid=%d ts=%d", e.Begin.GetShard().GetShardId(), e.Begin.GetXid(), e.Begin.GetCommitTs())
		if e.Begin.GetGid() != "" {
			s += " gid=" + e.Begin.GetGid()
		}
		return s
	case *pgshardv1.VEvent_Relation_:
		var cols []string
		for _, c := range e.Relation.GetColumns() {
			cols = append(cols, fmt.Sprintf("%s:%d:%t", c.GetName(), c.GetTypeOid(), c.GetKey()))
		}
		return fmt.Sprintf("relation %s.%s identity=%s cols=%s", e.Relation.GetSchema(), e.Relation.GetTable(), e.Relation.GetReplicaIdentity(), strings.Join(cols, ","))
	case *pgshardv1.VEvent_Row_:
		var vals []string
		for _, c := range e.Row.GetNew().GetColumns() {
			switch {
			case c.GetNull():
				vals = append(vals, "NULL")
			case c.GetUnchangedToast():
				vals = append(vals, "<toast>")
			default:
				vals = append(vals, string(c.GetValue()))
			}
		}
		s := fmt.Sprintf("row shard=%d %s %s.%s new=[%s]", e.Row.GetShard().GetShardId(), strings.TrimPrefix(e.Row.GetKind().String(), "KIND_"), e.Row.GetSchema(), e.Row.GetTable(), strings.Join(vals, ","))
		if e.Row.GetCopy() {
			s += " copy"
		}
		if e.Row.GetOld() != nil {
			s += fmt.Sprintf(" old_cols=%d key=%t", len(e.Row.GetOld().GetColumns()), e.Row.GetOldIsKey())
		}
		return s
	case *pgshardv1.VEvent_Truncate_:
		var ts []string
		for _, t := range e.Truncate.GetTables() {
			ts = append(ts, t.GetSchema()+"."+t.GetTable())
		}
		return fmt.Sprintf("truncate shard=%d %s", e.Truncate.GetShard().GetShardId(), strings.Join(ts, ","))
	case *pgshardv1.VEvent_Message_:
		return fmt.Sprintf("message shard=%d %s=%q transactional=%t", e.Message.GetShard().GetShardId(), e.Message.GetPrefix(), e.Message.GetContent(), e.Message.GetTransactional())
	case *pgshardv1.VEvent_Commit_:
		return fmt.Sprintf("commit shard=%d lsn=%d end=%d", e.Commit.GetShard().GetShardId(), e.Commit.GetLsn(), e.Commit.GetEndLsn())
	case *pgshardv1.VEvent_Prepare_:
		return fmt.Sprintf("prepare shard=%d gid=%s lsn=%d", e.Prepare.GetShard().GetShardId(), e.Prepare.GetGid(), e.Prepare.GetLsn())
	case *pgshardv1.VEvent_CommitPrepared_:
		return fmt.Sprintf("commit_prepared shard=%d gid=%s lsn=%d", e.CommitPrepared.GetShard().GetShardId(), e.CommitPrepared.GetGid(), e.CommitPrepared.GetLsn())
	case *pgshardv1.VEvent_RollbackPrepared_:
		return fmt.Sprintf("rollback_prepared shard=%d gid=%s lsn=%d", e.RollbackPrepared.GetShard().GetShardId(), e.RollbackPrepared.GetGid(), e.RollbackPrepared.GetLsn())
	case *pgshardv1.VEvent_Vgtid:
		return "vgtid " + describePos(e.Vgtid.GetPosition())
	case *pgshardv1.VEvent_Heartbeat_:
		return "heartbeat " + describePos(e.Heartbeat.GetPosition())
	case *pgshardv1.VEvent_Journal_:
		return fmt.Sprintf("journal participants=%d targets=%d gen=%d", len(e.Journal.GetParticipants()), len(e.Journal.GetTargets()), e.Journal.GetShardMapGeneration())
	case *pgshardv1.VEvent_Error_:
		return fmt.Sprintf("error %s shard=%d", strings.TrimPrefix(e.Error.GetCode().String(), "CODE_"), e.Error.GetShard().GetShardId())
	case *pgshardv1.VEvent_CopyBegin_:
		return fmt.Sprintf("copy_begin shard=%d %s.%s", e.CopyBegin.GetShard().GetShardId(), e.CopyBegin.GetSchema(), e.CopyBegin.GetTable())
	case *pgshardv1.VEvent_CopyCompleted_:
		switch {
		case e.CopyCompleted.GetShard() == nil:
			return "copy_completed stream"
		case e.CopyCompleted.GetTable() == "":
			return fmt.Sprintf("copy_completed shard=%d", e.CopyCompleted.GetShard().GetShardId())
		}
		return fmt.Sprintf("copy_completed shard=%d %s.%s", e.CopyCompleted.GetShard().GetShardId(), e.CopyCompleted.GetSchema(), e.CopyCompleted.GetTable())
	}
	return fmt.Sprintf("%T", ev.GetEvent())
}

func describePos(p *pgshardv1.VPosition) string {
	var parts []string
	for _, s := range p.GetShards() {
		parts = append(parts, fmt.Sprintf("%d:%d", s.GetShard().GetShardId(), s.GetLsn()))
	}
	out := fmt.Sprintf("gen=%d {%s}", p.GetShardMapGeneration(), strings.Join(parts, " "))
	for _, c := range p.GetCopyState() {
		cur := "-"
		if c.GetCurrent() != nil {
			cur = fmt.Sprintf("%s.%s@%s", c.GetCurrent().GetSchema(), c.GetCurrent().GetTable(), c.GetCurrent().GetLastpk())
		}
		out += fmt.Sprintf(" copy[%d done=%s cur=%s]", c.GetShard().GetShardId(), strings.Join(c.GetDone(), ","), cur)
	}
	return out
}

// recvN receives n events or fails after the timeout.
func recvN(t *testing.T, st pgshardv1.VStream_StreamClient, n int, timeout time.Duration) []*pgshardv1.VEvent {
	t.Helper()
	type res struct {
		ev  *pgshardv1.VEvent
		err error
	}
	var out []*pgshardv1.VEvent
	deadline := time.After(timeout)
	for len(out) < n {
		ch := make(chan res, 1)
		go func() {
			ev, err := st.Recv()
			ch <- res{ev, err}
		}()
		select {
		case r := <-ch:
			if r.err != nil {
				t.Fatalf("recv after %d events: %v", len(out), r.err)
			}
			out = append(out, r.ev)
		case <-deadline:
			var got []string
			for _, ev := range out {
				got = append(got, describe(ev))
			}
			t.Fatalf("timed out after %d/%d events:\n%s", len(out), n, strings.Join(got, "\n"))
		}
	}
	return out
}

func lines(evs []*pgshardv1.VEvent) []string {
	var out []string
	for _, ev := range evs {
		out = append(out, describe(ev))
	}
	return out
}

func checkGolden(t *testing.T, name string, got []string) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	text := strings.Join(got, "\n") + "\n"
	if update {
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with UPDATE_GOLDEN=1)", err)
	}
	if string(want) != text {
		t.Fatalf("golden %s differs:\n--- want\n%s--- got\n%s", name, want, text)
	}
}

func TestFanInKeepsTransactionsWholeAndPositionsVectors(t *testing.T) {
	h := newHarness(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Options: &pgshardv1.VStreamOptions{HeartbeatIntervalMs: 200}})

	// Shard 0's first transaction arrives in two batches with a shard 1
	// transaction (same table, same columns) in between; a keepalive on
	// shard 1 advances its position silently.
	h.pool[0].feed("plain", batch(0, evBegin(10, 1000), evRelation(16384, "t", "id", "v"), evInsert(16384, 0, "1", "a")))
	// A keepalive inside an open transaction must not move the position.
	h.pool[0].feed("plain", batch(9000, evKeepalive(9000)))
	h.pool[1].feed("plain", batch(0, evRelation(99, "t", "id", "v")))
	h.pool[1].feed("plain", txn(99, 21, 1600, 5100, "3", "c"))
	h.pool[0].feed("plain", batch(2000, evInsert(16384, 0, "4", "d"), evCommit(1990, 2000)))
	h.pool[1].feed("plain", batch(6000, evKeepalive(6000)))

	got := recvN(t, st, 10, 5*time.Second)
	// Shard 1's txn 21 and shard 0's txn 10 are both whole; their relative
	// order depends on arrival, so normalise by splitting into transactions.
	txns := splitTxns(lines(got))
	if len(txns) != 2 {
		t.Fatalf("want 2 transactions, got %d:\n%s", len(txns), strings.Join(lines(got), "\n"))
	}
	for _, tx := range txns {
		if !strings.HasPrefix(tx[0], "begin") && !strings.HasPrefix(tx[0], "relation") {
			t.Fatalf("transaction must start with begin: %v", tx)
		}
		if !strings.HasPrefix(tx[len(tx)-1], "vgtid") {
			t.Fatalf("transaction must end with vgtid: %v", tx)
		}
		shard := tx[len(tx)-2][len("commit shard="):][:1]
		for _, l := range tx[1 : len(tx)-1] {
			if strings.HasPrefix(l, "row") && !strings.Contains(l, "shard="+shard) {
				t.Fatalf("interleaved shards in %v", tx)
			}
		}
	}
	relations := 0
	for _, l := range lines(got) {
		if strings.HasPrefix(l, "relation") {
			relations++
		}
	}
	if relations != 1 {
		t.Fatalf("relation public.t must be sent once across shards, got %d:\n%s", relations, strings.Join(lines(got), "\n"))
	}
	// Shard 1's keepalive at 6000 may land before or after shard 0's commit.
	last := describe(got[len(got)-1])
	if !strings.Contains(last, "0:2000") || (!strings.Contains(last, "1:5100") && !strings.Contains(last, "1:6000")) {
		t.Fatalf("final vgtid must carry both shards: %s", last)
	}

	hb := recvN(t, st, 1, 2*time.Second)
	if want := "heartbeat gen=7 {0:2000 1:6000}"; describe(hb[0]) != want {
		t.Fatalf("heartbeat = %q, want %q", describe(hb[0]), want)
	}

	if err := st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Ack{Ack: &pgshardv1.VPosition{Shards: []*pgshardv1.VPosition_Shard{
		{Shard: shardRef(shard0), Lsn: 2000}, {Shard: shardRef(shard1), Lsn: 9999}}}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(h.pool[0].ackedLSNs()) == 1 && len(h.pool[1].ackedLSNs()) == 1 })
	if a := h.pool[0].ackedLSNs(); a[0] != 2000 {
		t.Fatalf("shard 0 ack = %v", a)
	}
	if a := h.pool[1].ackedLSNs(); a[0] != 6000 {
		t.Fatalf("shard 1 ack must be clamped to the delivered position: %v", a)
	}
}

func splitTxns(ls []string) [][]string {
	var out [][]string
	var cur []string
	for _, l := range ls {
		if strings.HasPrefix(l, "heartbeat") {
			continue
		}
		cur = append(cur, l)
		if strings.HasPrefix(l, "vgtid") {
			out = append(out, cur)
			cur = nil
		}
	}
	return out
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestGoldenSingleShardSequence(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "orders", Options: &pgshardv1.VStreamOptions{TwoPhase: true}})
	p := h.pool[0]
	p.feed("orders", batch(0, evRelation(1, "orders", "id", "name")))
	p.feed("orders", batch(100, evBegin(5, 10),
		&pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_Row_{Row: &pgshardv1.ChangeEvent_Row{RelationId: 1, Kind: pgshardv1.ChangeEvent_Row_KIND_UPDATE,
			Old: []*pgshardv1.Value{{Data: []byte("1")}}, OldIsKey: true, New: []*pgshardv1.Value{{Data: []byte("1")}, {}}, UnchangedToast: []uint32{1}}}},
		&pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_Row_{Row: &pgshardv1.ChangeEvent_Row{RelationId: 1, Kind: pgshardv1.ChangeEvent_Row_KIND_DELETE,
			Old: []*pgshardv1.Value{{Data: []byte("2")}, {Null: true}}}}},
		&pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_Message_{Message: &pgshardv1.ChangeEvent_Message{Prefix: "p", Content: []byte("in"), Transactional: true}}},
		&pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_Truncate_{Truncate: &pgshardv1.ChangeEvent_Truncate{RelationIds: []uint32{1}}}},
		evCommit(90, 100)))
	p.feed("orders", batch(0, &pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_Message_{Message: &pgshardv1.ChangeEvent_Message{Prefix: "p", Content: []byte("out")}}}))
	p.feed("orders", batch(200,
		&pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_BeginPrepare_{BeginPrepare: &pgshardv1.ChangeEvent_BeginPrepare{Gid: "g1", Xid: 6, PrepareLsn: 190, PrepareTs: 20}}},
		evInsert(1, 0, "3", "x"),
		&pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_Prepare_{Prepare: &pgshardv1.ChangeEvent_Prepare{Gid: "g1", PrepareLsn: 190, EndLsn: 200}}}))
	p.feed("orders", batch(300, &pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_CommitPrepared_{CommitPrepared: &pgshardv1.ChangeEvent_CommitPrepared{Gid: "g1", CommitLsn: 290, EndLsn: 300}}}))
	p.feed("orders", batch(400, &pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_RollbackPrepared_{RollbackPrepared: &pgshardv1.ChangeEvent_RollbackPrepared{Gid: "g2", RollbackLsn: 390, EndLsn: 400}}}))
	// A relation change (new column) resends the relation before the row.
	p.feed("orders", batch(0, evRelation(1, "orders", "id", "name", "extra")))
	p.feed("orders", txn(1, 7, 30, 500, "4", "y", "z"))
	// Streamed transaction: two segments, an aborted subtransaction, commit.
	p.feed("orders", batch(0,
		&pgshardv1.ChangeEvent{Xid: 8, Event: &pgshardv1.ChangeEvent_StreamStart_{StreamStart: &pgshardv1.ChangeEvent_StreamStart{Xid: 8, FirstSegment: true}}},
		evInsert(1, 8, "5", "s", "s"),
		evInsert(1, 9, "6", "sub", "sub"),
		&pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_StreamStop_{StreamStop: &pgshardv1.ChangeEvent_StreamStop{}}}))
	p.feed("orders", batch(0, &pgshardv1.ChangeEvent{Xid: 8, Event: &pgshardv1.ChangeEvent_StreamAbort_{StreamAbort: &pgshardv1.ChangeEvent_StreamAbort{Xid: 8, Subxid: 9}}}))
	p.feed("orders", batch(0,
		&pgshardv1.ChangeEvent{Xid: 8, Event: &pgshardv1.ChangeEvent_StreamStart_{StreamStart: &pgshardv1.ChangeEvent_StreamStart{Xid: 8}}},
		evInsert(1, 8, "7", "t", "t"),
		&pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_StreamStop_{StreamStop: &pgshardv1.ChangeEvent_StreamStop{}}}))
	p.feed("orders", batch(600, &pgshardv1.ChangeEvent{Xid: 8, Event: &pgshardv1.ChangeEvent_StreamCommit_{StreamCommit: &pgshardv1.ChangeEvent_StreamCommit{Xid: 8, CommitLsn: 590, EndLsn: 600, CommitTs: 40}}}))
	got := recvN(t, st, 27, 5*time.Second)
	checkGolden(t, "single_shard", lines(got))
}

func TestResumeFromVectorRestartsEachShardAtItsLSN(t *testing.T) {
	h := newHarness(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pos := &pgshardv1.VPosition{ShardMapGeneration: 7, Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard1), Lsn: 4242}}}
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Position: pos, Options: &pgshardv1.VStreamOptions{HeartbeatIntervalMs: 100}})
	hb := recvN(t, st, 1, 2*time.Second)
	if want := "heartbeat gen=7 {1:4242}"; describe(hb[0]) != want {
		t.Fatalf("heartbeat = %q, want %q", describe(hb[0]), want)
	}
	waitFor(t, func() bool { return len(h.pool[0].startLSNs()) == 1 && len(h.pool[1].startLSNs()) == 1 })
	if a, b := h.pool[0].startLSNs(), h.pool[1].startLSNs(); a[0] != 0 || b[0] != 4242 {
		t.Fatalf("start lsns = %v %v", a, b)
	}
	// A stale keepalive below the resume point never moves the vector back.
	h.pool[1].feed("plain", batch(100, evKeepalive(100)))
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))
	h.pool[0].feed("plain", txn(1, 1, 1, 50, "1"))
	got := recvN(t, st, 5, 2*time.Second)
	if want := "vgtid gen=7 {0:50 1:4242}"; describe(got[4]) != want {
		t.Fatalf("vgtid = %q, want %q", describe(got[4]), want)
	}
}

func TestStalePositionGenerationEndsWithReshardError(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pos := &pgshardv1.VPosition{ShardMapGeneration: 6}
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Position: pos})
	got := recvN(t, st, 1, 2*time.Second)
	if describe(got[0]) != "error RESHARDED shard=0" {
		t.Fatalf("got %s", describe(got[0]))
	}
	if _, err := st.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream must end cleanly, got %v", err)
	}
	st = h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Position: pos, Options: &pgshardv1.VStreamOptions{StopOnReshard: true}})
	got = recvN(t, st, 1, 2*time.Second)
	if describe(got[0]) != "journal participants=1 targets=0 gen=7" {
		t.Fatalf("got %s", describe(got[0]))
	}
}

func TestGenerationChangeMidStream(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Options: &pgshardv1.VStreamOptions{StopOnReshard: true, HeartbeatIntervalMs: 50}})
	recvN(t, st, 1, 2*time.Second)
	h.topo.reshard()
	h.pool[0].feed("plain", batch(10, evKeepalive(10)))
	var last *pgshardv1.VEvent
	for {
		ev, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		last = ev
	}
	if describe(last) != "journal participants=1 targets=0 gen=8" {
		t.Fatalf("last event = %s", describe(last))
	}
}

func TestShardStreamErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"slot invalidated", status.Error(codes.FailedPrecondition, "start replication: can no longer get changes from replication slot (55000)"), "error POSITION_TOO_OLD shard=0"},
		{"not configured", status.Error(codes.FailedPrecondition, "change streams are not configured on this pooler"), "error INTERNAL shard=0"},
		{"unavailable past the window", status.Error(codes.Unavailable, "replication connection: refused"), "error SHARD_UNAVAILABLE shard=0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, 1)
			h.server.ReconnectWindow = 300 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
			h.pool[0].fail(c.err)
			h.pool[0].fail(c.err)
			h.pool[0].fail(c.err)
			h.pool[0].fail(c.err)
			got := recvN(t, st, 1, 5*time.Second)
			if describe(got[0]) != c.want {
				t.Fatalf("got %s, want %s", describe(got[0]), c.want)
			}
			if _, err := st.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("stream must end after the error, got %v", err)
			}
		})
	}
}

func TestTransientPoolerErrorReconnectsAtDeliveredPosition(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))
	h.pool[0].feed("plain", txn(1, 1, 1, 700, "1"))
	recvN(t, st, 5, 2*time.Second)
	h.pool[0].fail(status.Error(codes.FailedPrecondition, "slot pgshard_plain_shard0 already has an active reader"))
	waitFor(t, func() bool { return len(h.pool[0].startLSNs()) == 2 })
	if s := h.pool[0].startLSNs(); s[1] != 700 {
		t.Fatalf("reconnect must resume at the delivered position: %v", s)
	}
	// The relation is resent by the new connection but not to the client.
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))
	h.pool[0].feed("plain", txn(1, 2, 2, 800, "2"))
	got := recvN(t, st, 4, 2*time.Second)
	if describe(got[0]) != "begin shard=0 xid=2 ts=2" || describe(got[3]) != "vgtid gen=7 {0:800}" {
		t.Fatalf("after reconnect: %v", lines(got))
	}
}

func TestFailoverMovesTheShardStreamToTheNewPrimary(t *testing.T) {
	h := newHarness(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))
	h.pool[0].feed("plain", txn(1, 1, 1, 1000, "1"))
	recvN(t, st, 5, 2*time.Second)

	promoted := newFakePooler(t)
	h.topo.promote(shard0, promoted)
	// The old primary's pooler drops the stream; the reader must come back
	// on the promoted pooler at the delivered LSN.
	h.pool[0].fail(status.Error(codes.Unavailable, "pooler draining"))
	waitFor(t, func() bool { return len(promoted.startLSNs()) == 1 })
	if s := promoted.startLSNs(); s[0] != 1000 {
		t.Fatalf("new primary start lsn = %v", s)
	}
	promoted.feed("plain", batch(0, evRelation(1, "t", "id")))
	promoted.feed("plain", txn(1, 2, 2, 1100, "2"))
	got := recvN(t, st, 4, 2*time.Second)
	if describe(got[3]) != "vgtid gen=7 {0:1100}" {
		t.Fatalf("after failover: %v", lines(got))
	}
	// Acks now land on the promoted pooler.
	if err := st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Ack{Ack: &pgshardv1.VPosition{Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: 1100}}}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(promoted.ackedLSNs()) == 1 })
	if len(h.pool[0].ackedLSNs()) != 0 {
		t.Fatal("old pooler must not be acked")
	}

	// An epoch bump with a still-open stream also reconnects.
	again := newFakePooler(t)
	h.topo.promote(shard0, again)
	promoted.feed("plain", batch(1200, evKeepalive(1200)))
	waitFor(t, func() bool { return len(again.startLSNs()) == 1 })
	if s := again.startLSNs(); s[0] != 1100 {
		t.Fatalf("second promotion start lsn = %v", s)
	}
}

func TestAlignSkewHoldsFastShardUntilSlowCatchesUpOrTimesOut(t *testing.T) {
	h := newHarness(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Options: &pgshardv1.VStreamOptions{AlignSkew: true, AlignSkewMs: 1000, AlignTimeoutMs: 700}})
	for _, p := range h.pool {
		p.feed("plain", batch(0, evRelation(1, "t", "id")))
	}
	sec := int64(1_000_000)
	h.pool[0].feed("plain", txn(1, 1, 1*sec, 100, "a1"))
	h.pool[1].feed("plain", txn(1, 2, 1*sec, 100, "b1"))
	got := recvN(t, st, 9, 2*time.Second)
	if len(splitTxns(lines(got))) != 2 {
		t.Fatalf("first two: %v", lines(got))
	}
	// Shard 1 leaps 10s ahead: held while shard 0 is at 1s.
	h.pool[1].feed("plain", txn(1, 3, 11*sec, 200, "b2"))
	h.pool[0].feed("plain", txn(1, 4, 2*sec, 200, "a2"))
	got = recvN(t, st, 4, 2*time.Second)
	if describe(got[0]) != "begin shard=0 xid=4 ts=2000000" {
		t.Fatalf("slow shard must go first: %v", lines(got))
	}
	// Nothing more from shard 0: the held transaction is released by the
	// timeout, not before ~700ms.
	start := time.Now()
	got = recvN(t, st, 4, 3*time.Second)
	if describe(got[0]) != "begin shard=1 xid=3 ts=11000000" {
		t.Fatalf("held shard must be released: %v", lines(got))
	}
	if held := time.Since(start); held < 400*time.Millisecond {
		t.Fatalf("released after %s, want the alignment timeout", held)
	}
	// Once shard 0 catches up, shard 1 flows without a hold.
	h.pool[0].feed("plain", txn(1, 5, 12*sec, 300, "a3"))
	recvN(t, st, 4, 2*time.Second)
	h.pool[1].feed("plain", txn(1, 6, 12*sec+500_000, 300, "b3"))
	start = time.Now()
	got = recvN(t, st, 4, 2*time.Second)
	if describe(got[0]) != "begin shard=1 xid=6 ts=12500000" || time.Since(start) > 300*time.Millisecond {
		t.Fatalf("aligned shard must flow: %v after %s", lines(got), time.Since(start))
	}
}

func TestStreamRequestValidation(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, _ := h.client.Stream(ctx)
	_ = st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Ack{Ack: &pgshardv1.VPosition{}}})
	if _, err := st.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ack first: %v", err)
	}
	st = h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "nope"})
	if _, err := st.Recv(); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown stream: %v", err)
	}
	st = h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Options: &pgshardv1.VStreamOptions{TwoPhase: true}})
	if _, err := st.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("two_phase on a plain stream: %v", err)
	}
	st = h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Options: &pgshardv1.VStreamOptions{ShardSet: "other"}})
	if _, err := st.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("empty shard set: %v", err)
	}
	if _, err := h.client.Create(ctx, &pgshardv1.CreateVStreamRequest{Stream: "x", Database: "app"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("create without controller: %v", err)
	}
	if _, err := h.client.Drop(ctx, &pgshardv1.DropVStreamRequest{Stream: "x"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("drop without controller: %v", err)
	}
	if _, err := h.client.Create(ctx, &pgshardv1.CreateVStreamRequest{Stream: "Bad Name"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad name: %v", err)
	}
	list, err := h.client.List(ctx, &pgshardv1.ListVStreamsRequest{})
	if err != nil || len(list.GetStreams()) != 2 {
		t.Fatalf("list: %v %v", list, err)
	}
	for _, s := range list.GetStreams() {
		if s.GetStream() == "orders" && (len(s.GetSlots()) != 1 || s.GetSlots()[0].GetSlot() != "pgshard_orders_shard0") {
			t.Fatalf("orders slots: %v", s)
		}
	}
	ack, err := h.client.Ack(ctx, &pgshardv1.VStreamAckRequest{Stream: "plain", Position: &pgshardv1.VPosition{Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: 5}}}})
	if err != nil || ack.GetError() != nil || h.pool[0].ackedLSNs()[0] != 5 {
		t.Fatalf("unary ack: %v %v %v", ack, err, h.pool[0].ackedLSNs())
	}
}

func TestBackpressureBoundsShardBuffering(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))
	big := strings.Repeat("x", 64<<10)
	for i := uint32(1); i <= 200; i++ {
		h.pool[0].feed("plain", txn(1, i, int64(i), uint64(i)*10, big))
	}
	// Without the consumer reading, the reader takes only its buffer (plus
	// what gRPC flow control lets through); the rest stays in the pooler.
	time.Sleep(500 * time.Millisecond)
	if n := len(h.pool[0].feedOf("plain")); n < 100 {
		t.Fatalf("reader ran ahead of the consumer: %d batches left", n)
	}
	got := recvN(t, st, 1+200*4, 20*time.Second)
	if describe(got[len(got)-1]) != "vgtid gen=7 {0:2000}" {
		t.Fatalf("last = %s", describe(got[len(got)-1]))
	}
}
