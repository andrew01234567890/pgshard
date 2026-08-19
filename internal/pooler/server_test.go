package pooler

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func TestFencingRefusesBeforeBackend(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	cases := []struct {
		name string
		gen  *pgshardv1.Generation
		want string
	}{
		{"stale generation", gen(6, 3), "stale routing generation"},
		{"future generation", gen(8, 3), "stale routing generation"},
		{"stale epoch", gen(7, 2), "stale primary epoch"},
		{"missing", nil, "missing routing generation"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream, err := h.client.Execute(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rs := roundTrip(t, stream, queryReq("s-"+tc.name, "select 1", tc.gen, identity("alice")))
			e := firstError(rs)
			if e == nil || e.Sqlstate != "55000" || e.Message != tc.want {
				t.Fatalf("got %v, want 55000 %q", e, tc.want)
			}
			if !strings.Contains(e.Detail, "pooler") && tc.gen != nil {
				t.Fatalf("detail should name both sides: %q", e.Detail)
			}
			_ = stream.CloseSend()
		})
	}
	if n := h.pg.queries.Load(); n != 0 {
		t.Fatalf("PostgreSQL saw %d queries; fenced requests must never reach it", n)
	}
	if n := h.pg.dials.Load(); n != 0 {
		t.Fatalf("PostgreSQL saw %d dials; fenced requests must never dial", n)
	}

	stream, err := h.client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rs := roundTrip(t, stream, queryReq("ok", "select 1", gen(7, 3), identity("alice")))
	if e := firstError(rs); e != nil {
		t.Fatalf("matching generation refused: %v", e)
	}
	if h.pg.queries.Load() != 1 {
		t.Fatal("matching generation must reach PostgreSQL")
	}
	h.src.Set(View{Generation: 8, Epoch: 3})
	rs = roundTrip(t, stream, queryReq("ok", "select 2", gen(7, 3), nil))
	if e := firstError(rs); e == nil || e.Message != "stale routing generation" {
		t.Fatalf("mid-stream generation change not fenced: %v", e)
	}
	if h.pg.queries.Load() != 1 {
		t.Fatal("stale mid-stream request reached PostgreSQL")
	}
	res, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "r", Generation: gen(7, 3)})
	if err != nil || res.Error == nil || res.Error.Sqlstate != "55000" {
		t.Fatalf("Reserve with stale generation: %v %v", res, err)
	}
}

func TestRegularBackendReturnsToPoolAfterIdle(t *testing.T) {
	h := startHarness(t, PoolConfig{MaxBackends: 1, AcquireTimeout: 200 * time.Millisecond})
	ctx := context.Background()
	a, _ := h.client.Execute(ctx)
	b, _ := h.client.Execute(ctx)
	roundTrip(t, a, queryReq("a", "select 1", gen(7, 3), identity("alice")))
	roundTrip(t, b, queryReq("b", "select 1", gen(7, 3), identity("alice")))
	roundTrip(t, a, queryReq("a", "select 1", gen(7, 3), nil))
	if d := h.pg.dials.Load(); d != 1 {
		t.Fatalf("dials = %d, want 1 (idle backend reused across sessions)", d)
	}
	roundTrip(t, a, queryReq("a", "begin", gen(7, 3), nil))
	rs := roundTrip(t, b, queryReq("b", "select 1", gen(7, 3), nil))
	if e := firstError(rs); e == nil || e.Sqlstate != "53300" {
		t.Fatalf("second session should hit the budget while a is in a transaction: %v", e)
	}
	roundTrip(t, a, queryReq("a", "commit", gen(7, 3), nil))
	rs = roundTrip(t, b, queryReq("b", "select 1", gen(7, 3), nil))
	if e := firstError(rs); e != nil {
		t.Fatalf("backend not returned after commit: %v", e)
	}
}

func TestReserveAndRelease(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	res, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "s", Generation: gen(7, 3)})
	if err != nil || res.Error != nil {
		t.Fatal(res, err)
	}
	stream, _ := h.client.Execute(ctx)
	roundTrip(t, stream, queryReq("s", "set x = 1", gen(7, 3), identity("alice")))
	roundTrip(t, stream, queryReq("s", "begin", gen(7, 3), nil))
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = h.client.Release(shortCtx, &pgshardv1.ReleaseRequest{SessionId: "s"})
	cancel()
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Release with a live stream must wait for it to detach: %v", err)
	}
	if h.srv.held() != 1 {
		t.Fatal("reserved session must hold its backend between batches")
	}
	res, _ = h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "s", Generation: gen(7, 3)})
	if res.BackendPid != 4242 {
		t.Fatalf("pid = %d", res.BackendPid)
	}
	_ = stream.CloseSend()
	waitFor(t, func() bool { return !h.attached("s") })
	if h.srv.held() != 1 {
		t.Fatal("reserved backend must survive the stream")
	}
	if _, err := h.client.Release(ctx, &pgshardv1.ReleaseRequest{SessionId: "s"}); err != nil {
		t.Fatal(err)
	}
	if !h.pg.sawQuery("ROLLBACK") || !h.pg.sawQuery("DISCARD ALL") {
		t.Fatalf("release must roll back and discard: %v", h.pg.seen)
	}
	if h.srv.lookup("s") != nil || h.srv.held() != 0 {
		t.Fatal("session should be gone after release")
	}
	if _, idle := h.srv.cfg.Pool.Stats(); idle != 1 {
		t.Fatalf("idle = %d, want the released backend back in the pool", idle)
	}
}

func TestDrainLetsInFlightTransactionCommit(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	stream, _ := h.client.Execute(ctx)
	roundTrip(t, stream, queryReq("s", "begin", gen(7, 3), identity("alice")))

	done := make(chan error, 1)
	go func() { done <- h.srv.Drain(context.Background()) }()
	waitFor(t, h.srv.draining.Load)

	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "n", Generation: gen(7, 3)}); status.Code(err) != codes.Unavailable {
		t.Fatalf("new reservation during drain: %v", err)
	}
	fresh, _ := h.client.Execute(ctx)
	if err := fresh.Send(queryReq("n", "select 1", gen(7, 3), identity("bob"))); err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("new session during drain: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("drain finished while a transaction was open: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	rs := roundTrip(t, stream, queryReq("s", "commit", gen(7, 3), nil))
	if e := firstError(rs); e != nil {
		t.Fatalf("commit during drain refused: %v", e)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not finish after commit")
	}
	rs = roundTrip(t, stream, queryReq("s", "select 1", gen(7, 3), nil))
	if e := firstError(rs); e == nil || e.Sqlstate != "57P03" {
		t.Fatalf("new batch after drain: %v", e)
	}
}

func TestDrainDeadlineForcesClose(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	stream, _ := h.client.Execute(context.Background())
	roundTrip(t, stream, queryReq("s", "begin", gen(7, 3), identity("alice")))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := h.srv.Drain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if h.srv.held() != 0 {
		t.Fatal("backends must be closed after the deadline")
	}
	if _, err := h.srv.cfg.Pool.Acquire(context.Background(), "app", "alice", nil, nil); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("pool should be closed: %v", err)
	}
}

func TestKeysZeroisedAndNeverLogged(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	stream, _ := h.client.Execute(context.Background())
	id := identity("alice")
	roundTrip(t, stream, queryReq("s", "select 1", gen(7, 3), id))
	h.src.Set(View{Generation: 1})
	roundTrip(t, stream, queryReq("s", "select 1", gen(7, 3), nil))
	_ = stream.CloseSend()
	waitFor(t, func() bool { return h.srv.lookup("s") == nil })

	first := queryReq("z", "select 1", gen(1, 0), identity("alice"))
	rec := &recordingStream{ctx: context.Background(), in: []*pgshardv1.ExecuteRequest{first}}
	if err := h.srv.Execute(rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.out) == 0 || rec.out[len(rec.out)-1].GetReadyForQuery() == nil {
		t.Fatalf("in-process execute responses: %v", rec.out)
	}
	for _, k := range [][]byte{first.User.ScramClientKey, first.User.ScramServerKey} {
		for _, b := range k {
			if b != 0 {
				t.Fatal("request keys must be zeroised once copied")
			}
		}
	}
	h.pg.mu.Lock()
	dialed := [][]byte{h.pg.lastCK, h.pg.lastSK}
	h.pg.mu.Unlock()
	for _, k := range dialed {
		if len(k) != 32 {
			t.Fatal("relay keys not captured")
		}
		for _, b := range k {
			if b != 0 {
				t.Fatal("session key copies must be zeroised when the stream ends")
			}
		}
	}
	logs := h.logs.String()
	for _, k := range [][]byte{testKey(0x11), testKey(0x22)} {
		for _, enc := range []string{hex.EncodeToString(k), base64.StdEncoding.EncodeToString(k)} {
			if strings.Contains(logs, enc) {
				t.Fatalf("log leaks a key: %s", logs)
			}
		}
	}
}

func TestHealthStreamReflectsSourceAndDrain(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := h.client.Health(ctx, &pgshardv1.HealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	st, err := stream.Recv()
	if err != nil || st.Generation != 7 || st.Epoch != 3 || !st.Serving || st.Role != pgshardv1.HealthStatus_ROLE_PRIMARY {
		t.Fatalf("first status %v %v", st, err)
	}
	h.src.Set(View{Generation: 9, Epoch: 4, Serving: true})
	if err := h.srv.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, err = stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if st.Generation == 9 && st.Epoch == 4 && !st.Serving {
			return
		}
	}
	t.Fatalf("last status %v", st)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func parseReq(session, sql string, user *pgshardv1.UserIdentity) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{SessionId: session, Generation: gen(7, 3), User: user,
		Message: &pgshardv1.ExecuteRequest_Parse{Parse: &pgshardv1.Parse{Name: "st1", Sql: sql}}}
}

func syncReq(session string) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{SessionId: session, Generation: gen(7, 3),
		Message: &pgshardv1.ExecuteRequest_Sync{Sync: &pgshardv1.Sync{}}}
}

func closeReq(session, name string) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{SessionId: session, Generation: gen(7, 3),
		Message: &pgshardv1.ExecuteRequest_Close{Close: &pgshardv1.Close{Kind: pgshardv1.Close_KIND_STATEMENT, Name: name}}}
}

func parseBatch(t *testing.T, stream pgshardv1.Pooler_ExecuteClient, reqs ...*pgshardv1.ExecuteRequest) []*pgshardv1.ExecuteResponse {
	t.Helper()
	for _, req := range reqs {
		if err := stream.Send(req); err != nil {
			t.Fatal(err)
		}
	}
	return collect(t, stream)
}

func kinds(rs []*pgshardv1.ExecuteResponse) []string {
	var out []string
	for _, r := range rs {
		switch {
		case r.GetParseComplete() != nil:
			out = append(out, "parse")
		case r.GetError() != nil:
			out = append(out, "error "+r.GetError().Error.Sqlstate)
		case r.GetReadyForQuery() != nil:
			out = append(out, "ready")
		}
	}
	return out
}

func TestReusedBackendNeverReparsesAHeldStatement(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "s", Generation: gen(7, 3)}); err != nil {
		t.Fatal(err)
	}
	stream, _ := h.client.Execute(ctx)
	rs := parseBatch(t, stream, parseReq("s", "select 1", identity("alice")), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("first parse: %s", got)
	}

	// The stream is dropped without Release; the session keeps its backend
	// and the replay re-parses the same name with the same SQL.
	_ = stream.CloseSend()
	waitFor(t, func() bool { return !h.attached("s") })
	stream, _ = h.client.Execute(ctx)
	rs = parseBatch(t, stream, parseReq("s", "select 1", identity("alice")), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("identical re-parse must be answered, got %s (%v)", got, h.pg.seen)
	}
	if n := h.pg.count("PARSE st1"); n != 1 {
		t.Fatalf("PostgreSQL saw %d parses of st1, want 1 (identical statement is skipped)", n)
	}

	// Same name, different SQL: the old statement is closed first.
	rs = parseBatch(t, stream, parseReq("s", "select 2", nil), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("re-parse with new SQL: %s (%v)", got, h.pg.seen)
	}
	if h.pg.count("CLOSE S st1") != 1 || h.pg.count("PARSE st1") != 2 {
		t.Fatalf("expected one Close and a second Parse: %v", h.pg.seen)
	}

	// A router Close is relayed as-is; a failed parse leaves the name in
	// doubt and the next parse closes before parsing.
	rs = parseBatch(t, stream, closeReq("s", "st1"), parseReq("s", "select syntax error", nil), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[error 42601 ready]" {
		t.Fatalf("close then failed parse: %s", got)
	}
	rs = parseBatch(t, stream, parseReq("s", "select 3", nil), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("parse after doubt: %s (%v)", got, h.pg.seen)
	}
	if h.pg.count("CLOSE S st1") != 4 {
		t.Fatalf("uncertain name must be closed before parsing: %v", h.pg.seen)
	}

	// Release hands the backend to the pool clean; the next session parses
	// the same name afresh.
	_ = stream.CloseSend()
	waitFor(t, func() bool { return !h.attached("s") })
	if _, err := h.client.Release(ctx, &pgshardv1.ReleaseRequest{SessionId: "s"}); err != nil {
		t.Fatal(err)
	}
	stream, _ = h.client.Execute(ctx)
	rs = parseBatch(t, stream, parseReq("t", "select 1", identity("alice")), syncReq("t"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" || h.pg.dials.Load() != 1 {
		t.Fatalf("fresh session on the recycled backend: %s dials=%d", got, h.pg.dials.Load())
	}
}

func TestUnreservedBatchLeavesNoStatementsInThePool(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	stream, _ := h.client.Execute(ctx)
	rs := parseBatch(t, stream, parseReq("s", "select 1", identity("alice")), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("parse: %s", got)
	}
	waitFor(t, h.pg.sawQueryFn("DISCARD ALL"))
	if _, idle := h.srv.cfg.Pool.Stats(); idle != 1 {
		t.Fatalf("idle = %d", idle)
	}
}
