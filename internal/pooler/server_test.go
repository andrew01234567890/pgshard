package pooler

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
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
	// The statement plus the DISCARD ALL that resets the backend before it
	// can be handed to another logical session. The reset is sent after
	// the response, so sampling the count here raced the pooler and this
	// assertion failed on a slow runner for no reason of its own.
	waitFor(t, func() bool { return h.pg.queries.Load() == 2 })
	h.src.Set(View{Generation: 8, Epoch: 3})
	rs = roundTrip(t, stream, queryReq("ok", "select 2", gen(7, 3), nil))
	if e := firstError(rs); e == nil || e.Message != "stale routing generation" {
		t.Fatalf("mid-stream generation change not fenced: %v", e)
	}
	// Still the two from the accepted statement: the refused one reached
	// nothing.
	if h.pg.queries.Load() != 2 {
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
	waitFor(t, func() bool { return !h.attached() })
	if h.srv.held() != 1 {
		t.Fatal("reserved backend must survive the stream")
	}
	if _, err := h.client.Release(ctx, &pgshardv1.ReleaseRequest{SessionId: "s"}); err != nil {
		t.Fatal(err)
	}
	if !h.pg.sawQuery("ROLLBACK") || !h.pg.sawQuery("DISCARD ALL") {
		t.Fatalf("release must roll back and discard: %v", h.pg.log())
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
	dialed := h.pg.keys()
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
	waitFor(t, func() bool { return !h.attached() })
	stream, _ = h.client.Execute(ctx)
	rs = parseBatch(t, stream, parseReq("s", "select 1", identity("alice")), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("identical re-parse must be answered, got %s (%v)", got, h.pg.log())
	}
	if n := h.pg.count("PARSE st1"); n != 1 {
		t.Fatalf("PostgreSQL saw %d parses of st1, want 1 (identical statement is skipped)", n)
	}

	// Same name, different SQL: the old statement is closed first.
	rs = parseBatch(t, stream, parseReq("s", "select 2", nil), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("re-parse with new SQL: %s (%v)", got, h.pg.log())
	}
	if h.pg.count("CLOSE S st1") != 1 || h.pg.count("PARSE st1") != 2 {
		t.Fatalf("expected one Close and a second Parse: %v", h.pg.log())
	}

	// A router Close is relayed as-is; a failed parse leaves the name in
	// doubt and the next parse closes before parsing.
	rs = parseBatch(t, stream, closeReq("s", "st1"), parseReq("s", "select syntax error", nil), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[error 42601 ready]" {
		t.Fatalf("close then failed parse: %s", got)
	}
	rs = parseBatch(t, stream, parseReq("s", "select 3", nil), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("parse after doubt: %s (%v)", got, h.pg.log())
	}
	if h.pg.count("CLOSE S st1") != 4 {
		t.Fatalf("uncertain name must be closed before parsing: %v", h.pg.log())
	}

	// Release hands the backend to the pool clean; the next session parses
	// the same name afresh.
	_ = stream.CloseSend()
	waitFor(t, func() bool { return !h.attached() })
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
	waitFor(t, func() bool { _, idle := h.srv.cfg.Pool.Stats(); return idle == 1 })
	if !h.pg.sawQuery("DISCARD ALL") {
		t.Fatalf("the backend must be reset before it returns to the pool: %v", h.pg.log())
	}
}

func TestDeallocateThroughExtendedProtocolDoubtsHeldStatements(t *testing.T) {
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
	unnamed := &pgshardv1.ExecuteRequest{SessionId: "s", Generation: gen(7, 3),
		Message: &pgshardv1.ExecuteRequest_Parse{Parse: &pgshardv1.Parse{Name: "", Sql: "DEALLOCATE ALL"}}}
	rs = parseBatch(t, stream, unnamed, syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("deallocate parse: %s", got)
	}
	rs = parseBatch(t, stream, parseReq("s", "select 1", nil), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("parse after deallocate: %s (%v)", got, h.pg.log())
	}
	if h.pg.count("PARSE st1") != 2 {
		t.Fatalf("a statement deallocated through the extended protocol must be parsed again: %v", h.pg.log())
	}
}

func TestDrainStartedDuringAcquireIsNotAdopted(t *testing.T) {
	pg := newFakePG()
	var srv *Server
	dial := func(ctx context.Context, db, role string, ck, sk []byte) (*Backend, error) {
		b, err := pg.dial(ctx, db, role, ck, sk)
		srv.draining.Store(true)
		return b, err
	}
	src := NewStaticSource(View{Generation: 7, Epoch: 3, Role: pgshardv1.HealthStatus_ROLE_PRIMARY, Serving: true})
	srv = NewServer(Config{Pool: newPool(PoolConfig{}, dial), Source: src, Database: "app", Logger: slog.New(slog.DiscardHandler)})
	stream := &recordingStream{ctx: context.Background(), in: []*pgshardv1.ExecuteRequest{queryReq("s", "select 1", gen(7, 3), identity("alice"))}}
	if err := srv.Execute(stream); err != nil {
		t.Fatal(err)
	}
	if e := firstError(stream.out); e == nil || e.Sqlstate != "57P03" {
		t.Fatalf("statement adopted a backend after drain began: %v", e)
	}
	if srv.held() != 0 || pg.queries.Load() != 0 {
		t.Fatalf("held %d, queries %d: the backend ran after Drain", srv.held(), pg.queries.Load())
	}
}

func TestReservationWithoutStreamExpires(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "s", Generation: gen(7, 3)}); err != nil {
		t.Fatal(err)
	}
	stream, _ := h.client.Execute(ctx)
	roundTrip(t, stream, queryReq("s", "begin", gen(7, 3), identity("alice")))
	_ = stream.CloseSend()
	waitFor(t, func() bool { return !h.attached() })
	h.srv.expireReservations(time.Now().Add(h.srv.cfg.ReserveTimeout / 2))
	if h.srv.lookup("s") == nil || h.srv.held() != 1 {
		t.Fatal("a reservation younger than the timeout must be kept")
	}
	h.srv.expireReservations(time.Now().Add(h.srv.cfg.ReserveTimeout))
	if h.srv.lookup("s") != nil || h.srv.held() != 0 {
		t.Fatal("expired reservation must be released")
	}
	if !h.pg.sawQuery("ROLLBACK") || !h.pg.sawQuery("DISCARD ALL") {
		t.Fatalf("expiry must roll back and discard: %v", h.pg.log())
	}
	if _, idle := h.srv.cfg.Pool.Stats(); idle != 1 {
		t.Fatalf("idle = %d, want the backend back in the pool", idle)
	}
	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "n", Generation: gen(7, 3)}); err != nil {
		t.Fatal(err)
	}
	h.srv.expireReservations(time.Now().Add(h.srv.cfg.ReserveTimeout))
	if h.srv.lookup("n") != nil {
		t.Fatal("a reservation whose stream never came must expire too")
	}
}

func TestNotificationsAreForwarded(t *testing.T) {
	msg := toResponse(&pgproto3.NotificationResponse{PID: 7, Channel: "events", Payload: "hello"}, false)
	n := msg.GetNotification()
	if n == nil || n.Pid != 7 || n.Channel != "events" || n.Payload != "hello" {
		t.Fatalf("notification = %v", msg)
	}
}

func TestDetachForgetsTheSessionBeforeANewExecuteCanReattach(t *testing.T) {
	s := NewServer(Config{Logger: slog.New(slog.DiscardHandler)})
	se := s.session("x")
	s.mu.Lock()
	se.attached = true
	se.detached = make(chan struct{})
	s.mu.Unlock()
	var se2 *session
	s.detachUnlocked = func() {
		se2 = s.session("x")
		s.mu.Lock()
		se2.attached = true
		se2.detached = make(chan struct{})
		s.mu.Unlock()
	}
	s.detach(se)
	if se2 == se {
		t.Fatal("a new Execute reattached to the detaching session")
	}
	if got := s.lookup("x"); got != se2 {
		t.Fatalf("the reattached session was forgotten: lookup returned %p, want %p", got, se2)
	}
}

func TestSQLLevelPrepareIsClosedBeforeAParseAndResetsTheBackend(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "s", Generation: gen(7, 3)}); err != nil {
		t.Fatal(err)
	}
	stream, _ := h.client.Execute(ctx)
	unnamed := &pgshardv1.ExecuteRequest{SessionId: "s", Generation: gen(7, 3), User: identity("alice"),
		Message: &pgshardv1.ExecuteRequest_Parse{Parse: &pgshardv1.Parse{Name: "", Sql: "PREPARE st1 AS SELECT 1"}}}
	rs := parseBatch(t, stream, unnamed, syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("sql-level prepare: %s", got)
	}
	rs = parseBatch(t, stream, parseReq("s", "select 1", nil), syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("parse after sql-level prepare: %s (%v)", got, h.pg.log())
	}
	if h.pg.count("CLOSE S st1") != 1 {
		t.Fatalf("a name a SQL-level PREPARE may hold must be closed before parsing: %v", h.pg.log())
	}
	_ = stream.CloseSend()
	waitFor(t, func() bool { return !h.attached() })
	if _, err := h.client.Release(ctx, &pgshardv1.ReleaseRequest{SessionId: "s"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, h.pg.sawQueryFn("DISCARD ALL"))
}

func TestUnreservedSQLLevelPrepareLeavesNoStatementsInThePool(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	stream, _ := h.client.Execute(ctx)
	unnamed := &pgshardv1.ExecuteRequest{SessionId: "s", Generation: gen(7, 3), User: identity("alice"),
		Message: &pgshardv1.ExecuteRequest_Parse{Parse: &pgshardv1.Parse{Name: "", Sql: "PREPARE plan1 AS SELECT 1"}}}
	rs := parseBatch(t, stream, unnamed, syncReq("s"))
	if got := fmt.Sprint(kinds(rs)); got != "[parse ready]" {
		t.Fatalf("parse: %s", got)
	}
	waitFor(t, func() bool { _, idle := h.srv.cfg.Pool.Stats(); return idle == 1 })
	if !h.pg.sawQuery("DISCARD ALL") {
		t.Fatalf("the backend must be reset before it returns to the pool: %v", h.pg.log())
	}
}

func TestCreatesPrepared(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{"PREPARE plan1 AS SELECT 1", true},
		{"prepare plan1 (int) as select $1", true},
		{"PREPARE TRANSACTION 'gid'", false},
		{"prepare  transaction 'gid'", false},
		{"BEGIN; PREPARE plan1 AS SELECT 1; PREPARE TRANSACTION 'gid'", true},
		{"SELECT 'prepared'", false},
		{"SELECT 1", false},
	}
	for _, c := range cases {
		if got := createsPrepared(c.sql); got != c.want {
			t.Errorf("createsPrepared(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestAttachSessionIsAtomicWithLookup(t *testing.T) {
	s := NewServer(Config{Logger: slog.New(slog.DiscardHandler)})
	se, err := s.attachSession("x", "alice", "", sessionCred("alice", testKey(0x11), testKey(0x22)))
	if err != nil {
		t.Fatal(err)
	}
	if got := s.lookup("x"); got != se {
		t.Fatal("attachSession returned a session that is not the registered one")
	}
	if _, err := s.attachSession("x", "alice", "", sessionCred("alice", testKey(0x11), testKey(0x22))); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second attach = %v, want FailedPrecondition", err)
	}
	var se2 *session
	var err2 error
	s.detachUnlocked = func() {
		se2, err2 = s.attachSession("x", "alice", "", sessionCred("alice", testKey(0x11), testKey(0x22)))
	}
	s.detach(se)
	if err2 != nil {
		t.Fatal(err2)
	}
	if se2 == se {
		t.Fatal("a new Execute reattached to the detaching session")
	}
	if got := s.lookup("x"); got != se2 || !se2.attached {
		t.Fatalf("the session attached mid-detach is not the registered attached one: lookup %p, attached %p", got, se2)
	}
}

func TestExpiryClaimsTheBackendUnderTheLock(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "s", Generation: gen(7, 3)}); err != nil {
		t.Fatal(err)
	}
	stream, _ := h.client.Execute(ctx)
	roundTrip(t, stream, queryReq("s", "begin", gen(7, 3), identity("alice")))
	_ = stream.CloseSend()
	waitFor(t, func() bool { return !h.attached() })
	se := h.srv.lookup("s")
	if se == nil {
		t.Fatal("reserved session gone before expiry")
	}
	h.srv.expireReservations(time.Now().Add(h.srv.cfg.ReserveTimeout))
	h.srv.mu.Lock()
	held := se.b
	h.srv.mu.Unlock()
	if held != nil {
		t.Fatal("expiry recycled the backend without claiming it: a racing Release would recycle it again")
	}
	if _, err := h.client.Release(ctx, &pgshardv1.ReleaseRequest{SessionId: "s"}); err != nil {
		t.Fatal(err)
	}
	if _, idle := h.srv.cfg.Pool.Stats(); idle != 1 {
		t.Fatalf("idle = %d, want exactly one backend back in the pool", idle)
	}
}

func databaseQueryReq(session, database string, g *pgshardv1.Generation, user *pgshardv1.UserIdentity) *pgshardv1.ExecuteRequest {
	req := queryReq(session, "select 1", g, user)
	req.Database = database
	return req
}

func TestSessionsDialTheirOwnDatabase(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	a, _ := h.client.Execute(ctx)
	b, _ := h.client.Execute(ctx)
	c, _ := h.client.Execute(ctx)
	roundTrip(t, a, databaseQueryReq("a", "db1", gen(7, 3), identity("alice")))
	roundTrip(t, b, databaseQueryReq("b", "db2", gen(7, 3), identity("alice")))
	roundTrip(t, c, queryReq("c", "select 1", gen(7, 3), identity("alice")))
	h.pg.mu.Lock()
	dialed := append([]string(nil), h.pg.dialed...)
	h.pg.mu.Unlock()
	want := []string{"db1", "db2", "app"}
	if len(dialed) != 3 || dialed[0] != want[0] || dialed[1] != want[1] || dialed[2] != want[2] {
		t.Fatalf("dialed = %v, want %v", dialed, want)
	}
}

func TestIdleBackendIsNeverReusedAcrossDatabases(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	a, _ := h.client.Execute(ctx)
	roundTrip(t, a, databaseQueryReq("a", "db1", gen(7, 3), identity("alice")))
	_ = a.CloseSend()
	waitFor(t, func() bool { return !h.attached() })
	b, _ := h.client.Execute(ctx)
	roundTrip(t, b, databaseQueryReq("b", "db2", gen(7, 3), identity("alice")))
	if d := h.pg.dials.Load(); d != 2 {
		t.Fatalf("dials = %d, want 2 (a db1 backend must not serve db2)", d)
	}
	h.pg.mu.Lock()
	second := h.pg.dialed[1]
	h.pg.mu.Unlock()
	if second != "db2" {
		t.Fatalf("second dial went to %q, want db2", second)
	}
}
func TestReattachRequiresMatchingCredentials(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "s", Generation: gen(7, 3)}); err != nil {
		t.Fatal(err)
	}
	stream, _ := h.client.Execute(ctx)
	roundTrip(t, stream, queryReq("s", "begin", gen(7, 3), identity("alice")))
	_ = stream.CloseSend()
	waitFor(t, func() bool { return !h.attached() })
	if h.srv.held() != 1 {
		t.Fatal("reserved backend must survive the stream")
	}

	attach := func(user *pgshardv1.UserIdentity) error {
		s2, err := h.client.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := s2.Send(queryReq("s", "select 1", gen(7, 3), user)); err != nil {
			return err
		}
		_, err = s2.Recv()
		return err
	}

	wrong := identity("alice")
	wrong.ScramClientKey = testKey(0x99)
	if err := attach(wrong); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reattach with different keys: %v", err)
	}
	if h.srv.held() != 0 || h.srv.lookup("s") != nil {
		t.Fatal("mismatched reattach must discard the pinned backend and forget the session")
	}

	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "r", Generation: gen(7, 3)}); err != nil {
		t.Fatal(err)
	}
	stream, _ = h.client.Execute(ctx)
	roundTrip(t, stream, queryReq("r", "begin", gen(7, 3), identity("bob")))
	_ = stream.CloseSend()
	waitFor(t, func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		se := h.srv.sessions["r"]
		return se != nil && !se.attached
	})

	empty := &pgshardv1.UserIdentity{Username: "bob"}
	s3, _ := h.client.Execute(ctx)
	if err := s3.Send(queryReq("r", "select 1", gen(7, 3), empty)); err == nil {
		if _, err = s3.Recv(); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("attach with empty keys: %v", err)
		}
	}
	otherRole := &pgshardv1.UserIdentity{Username: "mallory", ScramClientKey: testKey(0x11), ScramServerKey: testKey(0x22)}
	if err := attachAs(t, h, "r", otherRole); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reattach with different role: %v", err)
	}
}

func attachAs(t *testing.T, h *harness, session string, user *pgshardv1.UserIdentity) error {
	t.Helper()
	s, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(queryReq(session, "select 1", gen(7, 3), user)); err != nil {
		return err
	}
	_, err = s.Recv()
	return err
}

// TestCopyInReachesPostgresBeforeItEnds: an upload was buffered whole. COPY
// IN is answered by nothing until CopyDone, so pooler memory grew with the
// upload and PostgreSQL did not start ingesting until the client finished.
func TestCopyInReachesPostgresBeforeItEnds(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	stream, err := h.client.Execute(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	g, user := gen(7, 3), identity("app")
	roundTrip(t, stream, queryReq("copy", "copy t from stdin", g, user))

	chunk := bytes.Repeat([]byte("x"), 8<<10)
	for range 16 { // 128 KiB, past the flush threshold
		req := &pgshardv1.ExecuteRequest{SessionId: "copy", Generation: g, User: user,
			Message: &pgshardv1.ExecuteRequest_CopyData{CopyData: &pgshardv1.CopyData{Data: chunk}}}
		if err := stream.Send(req); err != nil {
			t.Fatal(err)
		}
	}
	// The upload has not ended, so anything PostgreSQL holds got there
	// while the copy was still running.
	deadline := time.Now().Add(5 * time.Second)
	for h.pg.copied.Load() < int64(len(chunk)) {
		if time.Now().After(deadline) {
			t.Fatal("the upload was still in the pooler after 128 KiB had been sent")
		}
		time.Sleep(10 * time.Millisecond)
	}
	roundTrip(t, stream, &pgshardv1.ExecuteRequest{SessionId: "copy", Generation: g, User: user,
		Message: &pgshardv1.ExecuteRequest_CopyDone{CopyDone: &pgshardv1.CopyDone{}}})
	if got := h.pg.copied.Load(); got != int64(16*len(chunk)) {
		t.Fatalf("PostgreSQL received %d bytes of a %d byte upload", got, 16*len(chunk))
	}
}

// BenchmarkSessionLookupWithReservations measures what establishing one
// Execute stream costs while N reservations are held. Every stream used to
// scan the whole session map looking for expiries, on the same mutex that
// serializes establishment, so a ramp of N sessions paid N squared checks.
// The cost should not grow with N.
func BenchmarkSessionLookupWithReservations(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			h := startHarness(b, PoolConfig{})
			for i := range n {
				id := "held-" + strconv.Itoa(i)
				h.srv.mu.Lock()
				h.srv.sessions[id] = &session{id: id, reserved: true, detachedAt: time.Now()}
				h.srv.noteExpiry(time.Now())
				h.srv.mu.Unlock()
			}
			b.ReportAllocs()
			for b.Loop() {
				h.srv.session("probe")
			}
		})
	}
}
