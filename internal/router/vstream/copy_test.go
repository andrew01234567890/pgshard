package vstream

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/router"
)

func copyStart(pos *pgshardv1.VPosition) *pgshardv1.VStreamRequest_Start {
	return &pgshardv1.VStreamRequest_Start{Stream: "plain", Position: pos,
		Options: &pgshardv1.VStreamOptions{HeartbeatIntervalMs: 5000, StartFrom: pgshardv1.StartFrom_START_FROM_COPY, CopyBatchRows: 2}}
}

func TestInitialCopyPrecedesStreamingAndCheckpointsEveryBatch(t *testing.T) {
	h := newHarness(t, 1)
	h.pool[0].copyPlan = func(*pgshardv1.CopyTablesRequest) copyScript {
		return script(cpSnapshot(1000, true), cpTable("t", "id", "v"), cpRows(`["2"]`, "1", "2"), cpRows(`["3"]`, "3"), cpTableDone("t"),
			cpTable("u", "id"), cpTableDone("u"), cpDone())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, copyStart(nil))
	// Streaming data is queued before the copy finishes and must follow it.
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id", "v")))
	h.pool[0].feed("plain", txn(1, 1, 1, 1100, "4", "x"))
	got := lines(recvN(t, st, 21, 5*time.Second))
	checkGolden(t, "initial_copy", got)
	req0 := h.pool[0].copyRequests()
	if len(req0) != 1 || req0[0].GetBatchRows() != 2 || req0[0].GetStream() != "plain" || req0[0].GetDatabase() != "app" || req0[0].GetResumeTable() != "" || len(req0[0].GetDoneTables()) != 0 {
		t.Fatalf("copy request: %v", req0)
	}
	if a := h.pool[0].startLSNs(); a[0] != 1000 {
		t.Fatalf("streaming must start at the consistent point, got %v", a)
	}
}

func TestTwoShardCopiesNeverInterleaveWithTheirOwnStream(t *testing.T) {
	h := newHarness(t, 2)
	h.pool[0].copyPlan = func(*pgshardv1.CopyTablesRequest) copyScript {
		return script(cpSnapshot(1000, true), cpTable("t", "id", "v"), cpRows(`["2"]`, "1", "2"), cpRows(`["3"]`, "3"), cpTableDone("t"), cpDone())
	}
	h.pool[1].copyPlan = func(*pgshardv1.CopyTablesRequest) copyScript {
		return script(cpSnapshot(2000, true), cpTable("t", "id", "v"), cpRows(`["10"]`, "10"), cpTableDone("t"), cpDone())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, copyStart(nil))
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id", "v")))
	h.pool[0].feed("plain", txn(1, 1, 1, 1100, "4", "x"))
	h.pool[1].feed("plain", batch(0, evRelation(1, "t", "id", "v")))
	h.pool[1].feed("plain", txn(1, 2, 2, 2100, "11", "y"))
	got := lines(recvN(t, st, 29, 5*time.Second))
	copyDone := map[string]bool{}
	streamDone := -1
	for i, l := range got {
		for _, sh := range []string{"0", "1"} {
			if l == "copy_completed shard="+sh {
				copyDone[sh] = true
			}
			if strings.HasPrefix(l, "begin shard="+sh) && !copyDone[sh] {
				t.Fatalf("shard %s streamed before its copy completed:\n%s", sh, strings.Join(got, "\n"))
			}
		}
		if l == "copy_completed stream" {
			streamDone = i
			if len(copyDone) != 2 {
				t.Fatalf("stream copy completed before both shards:\n%s", strings.Join(got, "\n"))
			}
		}
		if strings.HasPrefix(l, "vgtid") && streamDone >= 0 && strings.Contains(l, "copy[") {
			t.Fatalf("copy state after completion: %s", l)
		}
	}
	if streamDone < 0 || !strings.HasPrefix(got[streamDone-1], "vgtid gen=7 {0:1") || !strings.Contains(got[streamDone-1], " 1:2") || strings.Contains(got[streamDone-1], "copy[") {
		t.Fatalf("no stream-level copy completion after the final vector:\n%s", strings.Join(got, "\n"))
	}
	if a, b := h.pool[0].startLSNs(), h.pool[1].startLSNs(); a[0] != 1000 || b[0] != 2000 {
		t.Fatalf("streaming must start at the consistent points, got %v %v", a, b)
	}
}

func TestCopyResumesFromCarriedCheckpointAndKeepsTheOriginalPosition(t *testing.T) {
	h := newHarness(t, 2)
	h.pool[0].copyPlan = func(*pgshardv1.CopyTablesRequest) copyScript {
		return script(cpSnapshot(5000, false), cpTable("t", "id", "v"), cpRows(`["4"]`, "3", "4"), cpTableDone("t"), cpDone())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pos := &pgshardv1.VPosition{ShardMapGeneration: 7,
		Shards:    []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: 1000}, {Shard: shardRef(shard1), Lsn: 4242}},
		CopyState: []*pgshardv1.VCopyState{{Shard: shardRef(shard0), Done: []string{"public.a"}, Current: &pgshardv1.VCopyState_Table{Schema: "public", Table: "t", Lastpk: []byte(`["2"]`)}}}}
	st := h.open(ctx, copyStart(pos))
	got := lines(recvN(t, st, 9, 5*time.Second))
	checkGolden(t, "copy_resume", got)
	reqs := h.pool[0].copyRequests()
	if len(reqs) != 1 || reqs[0].GetResumeSchema() != "public" || reqs[0].GetResumeTable() != "t" || string(reqs[0].GetResumeLastpk()) != `["2"]` || strings.Join(reqs[0].GetDoneTables(), ",") != "public.a" {
		t.Fatalf("resume request: %v", reqs)
	}
	waitFor(t, func() bool { return len(h.pool[0].startLSNs()) == 1 && len(h.pool[1].startLSNs()) == 1 })
	if a, b := h.pool[0].startLSNs(), h.pool[1].startLSNs(); a[0] != 1000 || b[0] != 4242 {
		t.Fatalf("start lsns = %v %v: the original consistent point wins over the resume snapshot; a positioned shard never copies", a, b)
	}
	if n := len(h.pool[1].copyRequests()); n != 0 {
		t.Fatalf("shard 1 copied %d times although it had a position", n)
	}
}

func TestCopyTransientFailureContinuesAfterTheLastBatch(t *testing.T) {
	h := newHarness(t, 1)
	calls := 0
	h.pool[0].copyPlan = func(*pgshardv1.CopyTablesRequest) copyScript {
		calls++
		if calls == 1 {
			return copyScript{msgs: []*pgshardv1.CopyTablesResponse{cpSnapshot(1000, true), cpTable("t", "id", "v"), cpRows(`["2"]`, "1", "2")},
				failAfter: 3, err: status.Error(codes.Unavailable, "pooler restarting")}
		}
		return script(cpSnapshot(3000, false), cpTable("t", "id", "v"), cpRows(`["4"]`, "3", "4"), cpTableDone("t"), cpDone())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, copyStart(nil))
	got := lines(recvN(t, st, 14, 5*time.Second))
	checkGolden(t, "copy_retry", got)
	reqs := h.pool[0].copyRequests()
	if len(reqs) != 2 || string(reqs[1].GetResumeLastpk()) != `["2"]` || reqs[1].GetResumeTable() != "t" {
		t.Fatalf("requests: %v", reqs)
	}
	waitFor(t, func() bool { return len(h.pool[0].startLSNs()) == 1 })
	if a := h.pool[0].startLSNs(); a[0] != 1000 {
		t.Fatalf("streaming starts at %v, want the first snapshot's consistent point", a)
	}
}

func TestCopyFatalErrorsEndTheStream(t *testing.T) {
	h := newHarness(t, 1)
	h.pool[0].copyPlan = func(*pgshardv1.CopyTablesRequest) copyScript {
		return copyScript{failAfter: 0, err: status.Error(codes.FailedPrecondition, "create slot: wal_level is not logical")}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, copyStart(nil))
	got := recvN(t, st, 1, 5*time.Second)
	if describe(got[0]) != "error INTERNAL shard=0" {
		t.Fatalf("got %q", describe(got[0]))
	}
}

func TestAcksAreHeldForShardsStillCopying(t *testing.T) {
	h := newHarness(t, 1)
	h.pool[0].copyPlan = func(*pgshardv1.CopyTablesRequest) copyScript {
		return copyScript{msgs: []*pgshardv1.CopyTablesResponse{cpSnapshot(1000, true), cpTable("t", "id", "v"), cpRows(`["1"]`, "1")}, failAfter: 3, err: errors.New("hang up")}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, copyStart(nil))
	recvN(t, st, 4, 5*time.Second)
	if err := st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Ack{Ack: &pgshardv1.VPosition{Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: 1000}}}}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if acks := h.pool[0].ackedLSNs(); len(acks) != 0 {
		t.Fatalf("acks forwarded during copy: %v", acks)
	}
}

func TestCopyPhaseStateRoundTrip(t *testing.T) {
	sh := router.Shard{Set: "default", ID: 3}
	st := &pgshardv1.VCopyState{Shard: shardRef(sh), Done: []string{"public.a", "public.b"}, Current: &pgshardv1.VCopyState_Table{Schema: "public", Table: "c", Lastpk: []byte(`["7","x"]`)}}
	p := copyPhaseFrom(st, 500)
	back := p.state(sh)
	if describePos(&pgshardv1.VPosition{CopyState: []*pgshardv1.VCopyState{back}}) != describePos(&pgshardv1.VPosition{CopyState: []*pgshardv1.VCopyState{st}}) {
		t.Fatalf("round trip: %v vs %v", back, st)
	}
	req := p.request("s", "db", true)
	if req.GetResumeTable() != "c" || string(req.GetResumeLastpk()) != `["7","x"]` || len(req.GetDoneTables()) != 2 || req.GetBatchRows() != 500 || !req.GetTwoPhase() {
		t.Fatalf("request: %v", req)
	}
	p.current.Lastpk = nil
	if r := p.request("s", "db", false); r.GetResumeTable() != "" {
		t.Fatalf("a table without a delivered key restarts from its beginning: %v", r)
	}
	if !p.isDone("public", "a") || p.isDone("public", "c") {
		t.Fatal("isDone")
	}
	empty := copyPhaseFrom(nil, 0)
	if len(empty.done) != 0 || empty.current != nil || empty.state(sh).GetCurrent() != nil {
		t.Fatalf("empty phase: %v", empty)
	}
}
