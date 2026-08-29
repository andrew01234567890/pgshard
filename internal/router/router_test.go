package router

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

type harness struct {
	fp         *fakePooler
	poolerAddr string
	addr       string
	r          *Router
	srv        *pgwire.Server
	snapp      atomic.Pointer[snapshot.Snapshot]
	subsMu     sync.Mutex
	subs       map[chan snapshot.Change]struct{}
}

func (h *harness) snap() *snapshot.Snapshot { return h.snapp.Load() }

// setSnap publishes s and wakes buffered statements.
func (h *harness) setSnap(s *snapshot.Snapshot) {
	h.snapp.Store(s)
	h.subsMu.Lock()
	defer h.subsMu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- snapshot.Change{ShardMapGeneration: s.ShardMapGeneration}:
		default:
		}
	}
}

func (h *harness) subscribe() (<-chan snapshot.Change, func()) {
	ch := make(chan snapshot.Change, 1)
	h.subsMu.Lock()
	h.subs[ch] = struct{}{}
	h.subsMu.Unlock()
	return ch, func() {
		h.subsMu.Lock()
		delete(h.subs, ch)
		h.subsMu.Unlock()
	}
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	fp := newFakePooler()
	return newHarnessWith(t, fp, startFakePooler(t, fp), nil)
}

// newHarnessWith runs a router in front of the fake pooler at poolerAddr;
// mutate may adjust the router configuration.
func newHarnessWith(t *testing.T, fp *fakePooler, poolerAddr string, mutate func(*Config)) *harness {
	t.Helper()
	snap := &snapshot.Snapshot{
		ShardMapGeneration: 7,
		Serving:            map[snapshot.ShardKey]snapshot.Serving{{ShardSet: DefaultShardSet, ShardID: 0}: {Epoch: 2, PrimaryEndpoint: poolerAddr, State: "serving"}},
		Databases:          map[string]catalog.Database{"app": {Name: "app", HomeShard: 0}},
	}
	h := &harness{fp: fp, poolerAddr: poolerAddr, subs: map[chan snapshot.Change]struct{}{}}
	h.snapp.Store(snap)
	poolers := NewPoolers(nil, h.snap, insecure.NewCredentials())
	t.Cleanup(poolers.Close)
	cfg := Config{Snapshot: h.snap, Poolers: poolers, Logger: slog.New(slog.DiscardHandler),
		Buffering: Buffering{Window: 700 * time.Millisecond, Poll: 20 * time.Millisecond, PerShardCap: 2,
			Changes: h.subscribe}}
	if mutate != nil {
		mutate(&cfg)
	}
	startHarness(t, h, cfg)
	return h
}

// startHarness builds the router from cfg and serves it on a loopback port.
func startHarness(t testing.TB, h *harness, cfg Config) {
	t.Helper()
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.r = r
	verifier, err := pgwire.BuildSCRAMVerifier("secret", nil, 4096)
	if err != nil {
		t.Fatal(err)
	}
	lookup := func(_ context.Context, user string) (string, error) {
		if user == "app" {
			return verifier.String(), nil
		}
		return "", ErrUnknownRole
	}
	pcfg := pgwire.Config{Authenticator: pgwire.SCRAMAuthenticator{Lookup: lookup}, NewExecutor: r.NewExecutor}
	var srv *pgwire.Server
	pcfg.CancelHandler = func(ctx context.Context, key pgwire.CancelKey) { r.CancelHandler(srv)(ctx, key) }
	srv, err = pgwire.NewServer(pcfg)
	if err != nil {
		t.Fatal(err)
	}
	h.srv = srv
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	h.addr = l.Addr().String()
}

func (h *harness) connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func (h *harness) dsn(user, password, db string) string {
	return "postgres://" + user + ":" + password + "@" + h.addr + "/" + db + "?sslmode=disable&default_query_exec_mode=exec"
}

func sqlstate(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestSelectOneSimpleAndExtended(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("extended select 1: %d %v", n, err)
	}
	if err := conn.QueryRow(ctx, "select 1", pgx.QueryExecModeSimpleProtocol).Scan(&n); err != nil || n != 1 {
		t.Fatalf("simple select 1: %d %v", n, err)
	}
	if got := h.fp.users; len(got) == 0 || got[0] != "app" {
		t.Fatalf("pooler saw identity %v", got)
	}
	if h.fp.reserves != nil {
		t.Fatalf("stateless session must not reserve: %v", h.fp.reserves)
	}
}

func TestAuthAndDatabaseErrors(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, err := pgx.Connect(ctx, h.dsn("app", "wrong", "app"))
	if sqlstate(err) != "28P01" {
		t.Fatalf("wrong password: %v", err)
	}
	_, err = pgx.Connect(ctx, h.dsn("nobody", "secret", "app"))
	if sqlstate(err) != "28P01" {
		t.Fatalf("unknown user: %v", err)
	}
	_, err = pgx.Connect(ctx, h.dsn("app", "secret", "missing"))
	if sqlstate(err) != "3D000" {
		t.Fatalf("unknown database: %v", err)
	}
	if h.r.Sessions() != 0 {
		t.Fatalf("no executor should survive failed connects, got %d", h.r.Sessions())
	}
}

func TestRefusedStatements(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	cases := map[string]string{
		"listen ch":  "LISTEN",
		"notify ch":  "NOTIFY",
		"unlisten *": "UNLISTEN",
		"declare c cursor with hold for select 1": "WITH HOLD",
		"create temp table t (x int)":             "temporary",
		"create temporary table t as select 1":    "temporary",
	}
	for sql, want := range cases {
		for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement} {
			_, err := conn.Exec(ctx, sql, mode)
			if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), want) {
				t.Fatalf("%s (mode %v): %v", sql, mode, err)
			}
		}
	}
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("session unusable after refusals: %v", err)
	}
	if h.fp.reserves != nil {
		t.Fatalf("refused statements must not reach the pooler: reserves %v", h.fp.reserves)
	}
	// A refused Parse must not leave a phantom prepared statement to replay.
	h.r.mu.Lock()
	for _, e := range h.r.sessions {
		for name := range e.stmts {
			if name != "" {
				t.Errorf("phantom statement %q recorded", name)
			}
		}
	}
	h.r.mu.Unlock()
}

func TestBackendErrorsAndNotices(t *testing.T) {
	h := newHarness(t)
	var notices []string
	cfg, _ := pgx.ParseConfig(h.dsn("app", "secret", "app"))
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "select bad"); sqlstate(err) != "42P01" {
		t.Fatalf("relayed error: %v", err)
	}
	if _, err := conn.Exec(ctx, "select notice"); err != nil {
		t.Fatal(err)
	}
	if len(notices) != 1 || notices[0] != "hello" {
		t.Fatalf("notices %v", notices)
	}
}

func TestTransactionKeepsBackendAndRollbackDiscardsGUCs(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "set application_name to 'outside'"); err != nil {
		t.Fatal(err)
	}
	if len(h.fp.reserves) != 1 {
		t.Fatalf("SET must pin the backend: reserves %v", h.fp.reserves)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "set application_name to 'inside'"); err != nil {
		t.Fatal(err)
	}
	var v string
	if err := tx.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "inside" {
		t.Fatalf("inside txn: %q %v", v, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if len(h.fp.releases) != 1 {
		t.Fatalf("transaction end must release the pinned backend: releases %v", h.fp.releases)
	}
	if err := conn.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "outside" {
		t.Fatalf("after rollback and replay: %q %v", v, err)
	}
	if len(h.fp.reserves) != 2 {
		t.Fatalf("replay must re-pin: reserves %v", h.fp.reserves)
	}
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "set application_name to 'committed'"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "committed" {
		t.Fatalf("after commit and replay: %q %v", v, err)
	}
	if _, err := conn.Exec(ctx, "select bad"); sqlstate(err) != "42P01" {
		t.Fatalf("expected relayed error, got %v", err)
	}
	if err := conn.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "committed" {
		t.Fatalf("after error: %q %v", v, err)
	}
}

func TestPreparedStatementsSurviveRelease(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Prepare(ctx, "q1", "select 1"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := conn.QueryRow(ctx, "q1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("q1: %d %v", n, err)
	}
	if len(h.fp.reserves) != 1 {
		t.Fatalf("named statements must pin: %v", h.fp.reserves)
	}
	h.fp.mu.Lock()
	var names []string
	for _, b := range h.fp.backends {
		for name := range b.stmts {
			if name != "" {
				names = append(names, name)
			}
		}
	}
	h.fp.mu.Unlock()
	if len(names) != 1 || !strings.HasPrefix(names[0], "pgshard_") || !strings.HasSuffix(names[0], "_q1") {
		t.Fatalf("physical statement names %v", names)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if len(h.fp.releases) != 1 {
		t.Fatalf("releases %v", h.fp.releases)
	}
	if err := conn.QueryRow(ctx, "q1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("q1 after replay: %d %v", n, err)
	}
	if err := conn.Deallocate(ctx, "q1"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Prepare(ctx, "q1", "select 1"); err != nil {
		t.Fatalf("re-prepare after deallocate: %v", err)
	}
}

func TestLongStatementNamesAreHashed(t *testing.T) {
	e := &Executor{info: pgwire.SessionInfo{ID: 12}}
	long := strings.Repeat("s", 70)
	p := e.physical(long)
	if len(p) > maxIdentifierLen || !strings.HasPrefix(p, "pgshard_12_h") || p == e.physical(long+"x") {
		t.Fatalf("physical(long)=%q", p)
	}
	if e.physical("") != "" || e.physical("q") != "pgshard_12_q" {
		t.Fatalf("physical short: %q %q", e.physical(""), e.physical("q"))
	}
}

func TestCancelReachesPooler(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			h.fp.mu.Lock()
			sleeping := len(h.fp.sleeping)
			h.fp.mu.Unlock()
			if sleeping > 0 {
				_ = conn.PgConn().CancelRequest(ctx)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	_, err := conn.Exec(ctx, "select pg_sleep(10)")
	if sqlstate(err) != "57014" {
		t.Fatalf("cancel: %v", err)
	}
	if c := h.fp.cancelled(); len(c) != 1 {
		t.Fatalf("pooler cancels %v", c)
	}
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("session after cancel: %v", err)
	}
}

func TestCopyFromStdin(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	tag, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("1\n2\n3\n"), "copy t from stdin")
	if err != nil || tag.String() != "COPY 3" {
		t.Fatalf("copy: %v %v", tag, err)
	}
	var n int
	if err := conn.QueryRow(ctx, "select rows").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 && n != 0 {
		t.Fatalf("rows after copy: %d", n)
	}
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("session after copy: %v", err)
	}
}

func TestStaleGenerationSurfaces(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	fresh := h.snap()
	stale := *fresh
	stale.ShardMapGeneration = 6
	h.setSnap(&stale)
	start := time.Now()
	if _, err := conn.Exec(ctx, "select 1"); sqlstate(err) != "55000" {
		t.Fatalf("stale generation: %v", err)
	}
	if time.Since(start) < 700*time.Millisecond {
		t.Fatalf("stale generation was reported after %s without waiting for the buffer window", time.Since(start))
	}
	h.setSnap(fresh)
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("after reload: %v", err)
	}
}

func TestPoolerStreamLossReportsAndRecovers(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "set application_name to 'x'"); err != nil {
		t.Fatal(err)
	}
	h.fp.setDropAfter("select 1")
	_, err := conn.Exec(ctx, "select 1", pgx.QueryExecModeSimpleProtocol)
	if sqlstate(err) != "08006" {
		t.Fatalf("stream loss: %v", err)
	}
	h.fp.setDropAfter("")
	var v string
	if err := conn.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "x" {
		t.Fatalf("after recovery: %q %v", v, err)
	}
	if h.fp.dropCount() != 1 {
		t.Fatalf("dropped %d", h.fp.dropCount())
	}
}

func TestSessionEndReleasesPinnedBackend(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "set application_name to 'x'"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.fp.mu.Lock()
		n := len(h.fp.releases)
		h.fp.mu.Unlock()
		if n == 1 && h.r.Sessions() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("releases %v sessions %d", h.fp.releases, h.r.Sessions())
}

func TestRefusedParseDropsWholeBatch(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	pipe := conn.PgConn().StartPipeline(ctx)
	pipe.SendPrepare("good", "select 1", nil)
	pipe.SendPrepare("bad", "listen ch", nil)
	pipe.SendPipelineSync()
	if err := pipe.Sync(); err != nil {
		t.Fatal(err)
	}
	var err error
	for {
		res, rerr := pipe.GetResults()
		if rerr != nil && err == nil {
			err = rerr
		}
		if res == nil {
			break
		}
		if _, ok := res.(*pgconn.PipelineSync); ok {
			break
		}
	}
	_ = pipe.Close()
	if sqlstate(err) != "0A000" {
		t.Fatalf("batch with refused statement: %v", err)
	}
	h.r.mu.Lock()
	defer h.r.mu.Unlock()
	for _, e := range h.r.sessions {
		for name := range e.stmts {
			if name != "" {
				t.Errorf("phantom statement %q survived a refused batch", name)
			}
		}
		if len(e.batch) != 0 || e.batchFailed {
			t.Errorf("batch state not reset: %d %v", len(e.batch), e.batchFailed)
		}
	}
}

func TestExtendedSetIsStaged(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Prepare(ctx, "setapp", "set application_name to 'ext'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.PgConn().ExecPrepared(ctx, "setapp", nil, nil, nil).Close(); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var v string
	if err := conn.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "ext" {
		t.Fatalf("extended SET not replayed: %q %v", v, err)
	}
}

func TestRollbackToSavepointDropsSettingsStagedAfterIt(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	for _, sql := range []string{"begin", "set application_name to 'before'", "savepoint sp", "set application_name to 'after'", "rollback to savepoint sp", "commit"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	var v string
	if err := conn.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "before" {
		t.Fatalf("replayed after rollback to savepoint: %q %v", v, err)
	}
	for _, sql := range []string{"begin", "savepoint sp", "set application_name to 'released'", "release savepoint sp", "commit"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if err := conn.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "released" {
		t.Fatalf("replayed after release: %q %v", v, err)
	}
}

func TestSQLPrepareIsPinnedAndReplayed(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "prepare one as select 1"); err != nil {
		t.Fatal(err)
	}
	if len(h.fp.reserves) != 1 {
		t.Fatalf("SQL PREPARE must pin: %v", h.fp.reserves)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if len(h.fp.releases) != 1 {
		t.Fatalf("transaction end must release: %v", h.fp.releases)
	}
	var n int
	if err := conn.QueryRow(ctx, "execute one").Scan(&n); err != nil || n != 1 {
		t.Fatalf("execute after replay: %d %v", n, err)
	}
	if _, err := conn.Exec(ctx, "deallocate one"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "prepare one as select 1"); err != nil {
		t.Fatalf("re-prepare after deallocate: %v", err)
	}
	if _, err := conn.Exec(ctx, "discard all"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "prepare one as select 1"); err != nil {
		t.Fatalf("re-prepare after discard all: %v", err)
	}
}

func TestDeallocateOfProtocolStatementStopsItsReplay(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Prepare(ctx, "q1", "select 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "deallocate q1"); err != nil {
		t.Fatalf("deallocate must address the physical statement: %v", err)
	}
	if !slices.ContainsFunc(h.fp.executedQueries(), func(q string) bool {
		return strings.HasPrefix(q, `deallocate "pgshard_`) && strings.HasSuffix(q, `_q1"`)
	}) {
		t.Fatalf("deallocate was not rewritten to the physical name: %v", h.fp.executedQueries())
	}
	for _, sql := range []string{"begin", "commit", "select 1"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	if names := h.fp.namedStatements(); len(names) != 0 {
		t.Fatalf("deallocated statement was replayed: %v", names)
	}
}

func (f *fakePooler) executedQueries() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.executed)
}

func (f *fakePooler) namedStatements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for _, b := range f.backends {
		for name := range b.stmts {
			if name != "" {
				names = append(names, name)
			}
		}
	}
	return names
}

func TestNotificationsReachTheClient(t *testing.T) {
	h := newHarness(t)
	cfg, err := pgx.ParseConfig(h.dsn("app", "secret", "app"))
	if err != nil {
		t.Fatal(err)
	}
	var got []*pgconn.Notification
	cfg.OnNotification = func(_ *pgconn.PgConn, n *pgconn.Notification) { got = append(got, n) }
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(context.Background(), "select notify"); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PID != 9 || got[0].Channel != "events" || got[0].Payload != "hello" {
		t.Fatalf("notifications %+v", got)
	}
}

// TestOneCancelPerStatementAcrossPumps: a statement pumps more than once
// -- the backend is acquired, the session state replayed, the transaction
// prelude reopened, and only then the statement itself runs. The dedupe
// used to be armed by every pump, so a cancellation seen either side of
// one of those boundaries sent a second Cancel. The pooler cancels by
// session, so the second lands on whatever that backend is running when it
// arrives, which may be the next statement.
func TestOneCancelPerStatementAcrossPumps(t *testing.T) {
	fp := newFakePooler()
	h := newHarnessWith(t, fp, startFakePooler(t, fp), nil)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	if _, err := conn.Exec(context.Background(), "select 1"); err != nil {
		t.Fatal(err)
	}
	h.r.mu.Lock()
	var e *Executor
	for _, live := range h.r.sessions {
		e = live
		break
	}
	h.r.mu.Unlock()
	if e == nil {
		t.Fatal("no executor for the session")
	}

	stmt, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Every pump of one statement arms it once between them.
	for range 4 {
		e.beginStatement(stmt)
		e.cancelBackend(context.Background())
	}
	if n := len(fp.cancelled()); n != 1 {
		t.Errorf("one statement sent %d cancels; the pooler cancels by session, so the extras land on the next statement", n)
	}

	// The next statement is cancellable in its own right.
	next, cancelNext := context.WithCancel(context.Background())
	defer cancelNext()
	e.beginStatement(next)
	e.cancelBackend(context.Background())
	if n := len(fp.cancelled()); n != 2 {
		t.Errorf("after a second statement: %d cancels, want 2", n)
	}
}
