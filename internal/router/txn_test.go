package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// fakeDecisionLog records the coordinator's decision-log calls together
// with what the shards held at that moment, so the order of the protocol
// steps is observable.
type fakeDecisionLog struct {
	h *shardedHarness

	mu     sync.Mutex
	events []string
	rows   map[string]string
	fail   map[string]error
	// xids collects the participant transaction ids Begin was given.
	xids []string
	// abortAfterBegin models the resolver deciding abort right after the
	// row was written.
	abortAfterBegin bool
	// heartbeats counts Heartbeat calls that found the row preparing.
	heartbeats int
	// waitHeartbeat makes Commit wait until a heartbeat arrived, so a test
	// can observe the coordinator beating while it is between the
	// decision-log write and the decision.
	waitHeartbeat bool
}

func (l *fakeDecisionLog) preparedCount() int {
	n := 0
	for _, fp := range l.h.poolers {
		n += len(fp.preparedGIDs())
	}
	return n
}

func (l *fakeDecisionLog) record(op string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, fmt.Sprintf("%s(prepared=%d)", op, l.preparedCount()))
}

func (l *fakeDecisionLog) Begin(_ context.Context, gid string, participants []int32, xids []string) error {
	if err := l.fail["begin"]; err != nil {
		return err
	}
	l.record("begin")
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rows[gid] = fmt.Sprintf("preparing%v", participants)
	l.xids = append(l.xids, xids...)
	if l.abortAfterBegin {
		l.rows[gid] = "abort"
	}
	return nil
}

func (l *fakeDecisionLog) Heartbeat(_ context.Context, gid string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if strings.HasPrefix(l.rows[gid], "preparing") {
		l.heartbeats++
	}
	return nil
}

func (l *fakeDecisionLog) heartbeatCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.heartbeats
}

func (l *fakeDecisionLog) Commit(_ context.Context, gid string) (bool, error) {
	if err := l.fail["commit"]; err != nil {
		return false, err
	}
	if l.waitHeartbeat {
		for i := 0; i < 2000 && l.heartbeatCount() == 0; i++ {
			time.Sleep(time.Millisecond)
		}
	}
	l.record("commit")
	l.mu.Lock()
	defer l.mu.Unlock()
	if !strings.HasPrefix(l.rows[gid], "preparing") {
		return false, nil
	}
	l.rows[gid] = "commit"
	return true, nil
}

func (l *fakeDecisionLog) Abort(_ context.Context, gid string) error {
	l.record("abort")
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rows[gid] = "abort"
	return nil
}

func (l *fakeDecisionLog) Delete(_ context.Context, gid string) error {
	l.record("delete")
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.rows, gid)
	return nil
}

func (l *fakeDecisionLog) log() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

type txnHarness struct {
	*shardedHarness
	log *fakeDecisionLog
}

func newTxnHarness(t *testing.T) *txnHarness {
	t.Helper()
	log := &fakeDecisionLog{rows: map[string]string{}, fail: map[string]error{}}
	h := newShardedHarnessWith(t, Config{Decisions: log})
	log.h = h
	return &txnHarness{shardedHarness: h, log: log}
}

func (h *txnHarness) prepared() int { return h.log.preparedCount() }

func (h *txnHarness) allRan(needle string) []int {
	var out []int
	for i := range h.poolers {
		if h.ranOn(i, needle) {
			out = append(out, i)
		}
	}
	return out
}

func TestCrossShardCommitIsTwoPhaseAndDecidedBeforeCommitPrepared(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	sa, sb := h.shardOf(t, a), h.shardOf(t, b)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", b); err != nil {
		t.Fatalf("second writable shard must escalate, not refuse: %v", err)
	}
	if got := h.log.log(); len(got) != 0 {
		t.Fatalf("decision log touched before COMMIT: %v", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{"begin(prepared=0)", "commit(prepared=2)", "delete(prepared=0)"}
	if got := h.log.log(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("decision log events %v, want %v", got, want)
	}
	for _, s := range []int{sa, sb} {
		if !h.ranOn(s, "prepare transaction 'pgshard-") || !h.ranOn(s, "commit prepared 'pgshard-") {
			t.Fatalf("shard %d did not prepare and commit: %v", s, h.poolers[s].ran())
		}
	}
	if got := h.allRan("prepare transaction"); len(got) != 2 {
		t.Fatalf("shards that prepared: %v, want exactly the two writers", got)
	}
	if h.prepared() != 0 {
		t.Fatalf("%d prepared transactions left behind", h.prepared())
	}
	if h.r.InDoubt() != 0 {
		t.Fatalf("in-doubt counter %d", h.r.InDoubt())
	}
	// The session is usable and single-shard again.
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 3)", a); err != nil {
		t.Fatal(err)
	}
}

func TestReadOnlyParticipantDoesNotPrepare(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "select * from orders where tenant_id = $1", b)
	if err != nil {
		t.Fatalf("read on another shard inside the transaction: %v", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.log.log(); len(got) != 0 {
		t.Fatalf("single writer must not touch the decision log: %v", got)
	}
	if got := h.allRan("prepare transaction"); len(got) != 0 {
		t.Fatalf("shards prepared: %v", got)
	}
	if !h.ranOn(h.shardOf(t, a), "commit") || !h.ranOn(h.shardOf(t, b), "rollback") {
		t.Fatalf("writer must COMMIT and reader ROLLBACK: %v / %v", h.poolers[h.shardOf(t, a)].ran(), h.poolers[h.shardOf(t, b)].ran())
	}
}

func TestSingleModeRefusesSecondWritableShard(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	if _, err := conn.Exec(ctx, "set pgshard.transaction_mode = single"); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", b)
	pe := expectRefusal(t, err, "transaction already writes to shard")
	if !strings.Contains(pe.Hint, "twopc") {
		t.Fatalf("hint %q", pe.Hint)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.log.log(); len(got) != 0 {
		t.Fatalf("decision log touched: %v", got)
	}
	_, err = conn.Exec(ctx, "set pgshard.transaction_mode = sideways")
	if pe := expectRefusalCode(t, err, "22023"); !strings.Contains(pe.Message, "sideways") {
		t.Fatalf("message %q", pe.Message)
	}
	// Reads on other shards stay allowed in single mode; twopc turns
	// escalation back on for the same session.
	if _, err := conn.Exec(ctx, "set pgshard.transaction_mode to 'twopc'"); err != nil {
		t.Fatal(err)
	}
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", b); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.log.log(); len(got) != 3 {
		t.Fatalf("decision log %v", got)
	}
}

func TestPrepareFailureAbortsEverywhere(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	h.poolers[h.shardOf(t, b)].failPrepare = true
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []int64{a, b} {
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", tenant); err != nil {
			t.Fatal(err)
		}
	}
	err = tx.Commit(ctx)
	if pe := expectRefusalCode(t, err, "55000"); !strings.Contains(pe.Message, "prepare refused") {
		t.Fatalf("message %q", pe.Message)
	}
	want := "begin(prepared=0),abort(prepared=0),delete(prepared=0)"
	if got := strings.Join(h.log.log(), ","); got != want {
		t.Fatalf("decision log %s, want %s", got, want)
	}
	if !h.ranOn(h.shardOf(t, a), "rollback prepared 'pgshard-") {
		t.Fatalf("prepared shard was not rolled back: %v", h.poolers[h.shardOf(t, a)].ran())
	}
	if h.prepared() != 0 {
		t.Fatalf("%d prepared transactions left", h.prepared())
	}
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil {
		t.Fatalf("session unusable after the abort: %v", err)
	}
}

func TestDecisionLogFailureBeforePrepareRollsBack(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	h.log.fail["begin"] = errors.New("catalog down")
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []int64{a, b} {
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", tenant); err != nil {
			t.Fatal(err)
		}
	}
	err = tx.Commit(ctx)
	if pe := expectRefusalCode(t, err, "08006"); !strings.Contains(pe.Message, "transaction rolled back") {
		t.Fatalf("message %q", pe.Message)
	}
	if got := h.allRan("prepare transaction"); len(got) != 0 {
		t.Fatalf("shards prepared without a decision row: %v", got)
	}
	for _, s := range []int{h.shardOf(t, a), h.shardOf(t, b)} {
		if !h.ranOn(s, "rollback") {
			t.Fatalf("shard %d not rolled back: %v", s, h.poolers[s].ran())
		}
	}
}

func TestDecisionUnknownLeavesParticipantsPreparedAndReportsInDoubt(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	h.log.fail["commit"] = errors.New("catalog down")
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []int64{a, b} {
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", tenant); err != nil {
			t.Fatal(err)
		}
	}
	err = tx.Commit(ctx)
	if pe := expectRefusalCode(t, err, "08007"); !strings.Contains(pe.Message, "outcome of transaction pgshard-") {
		t.Fatalf("message %q", pe.Message)
	}
	if h.prepared() != 2 {
		t.Fatalf("participants must stay prepared for the resolver, %d prepared", h.prepared())
	}
	if h.r.InDoubt() != 1 {
		t.Fatalf("in-doubt counter %d", h.r.InDoubt())
	}
	if got := h.allRan("rollback prepared"); len(got) != 0 {
		t.Fatalf("an undecided transaction must not be rolled back by the router: %v", got)
	}
}

func TestResolverAbortRaceIsRolledBack(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []int64{a, b} {
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", tenant); err != nil {
			t.Fatal(err)
		}
	}
	// The resolver decides abort while the router is preparing: the
	// guarded commit UPDATE then finds no preparing row.
	h.log.abortAfterBegin = true
	err = tx.Commit(ctx)
	if pe := expectRefusalCode(t, err, "40000"); !strings.Contains(pe.Message, "aborted by the resolver") {
		t.Fatalf("message %q", pe.Message)
	}
	if h.prepared() != 0 {
		t.Fatalf("%d prepared transactions left", h.prepared())
	}
}

func TestMultiShardRollbackAndSavepointRefusal(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []int64{a, b} {
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", tenant); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, "savepoint s1")
	_ = expectRefusal(t, err, "savepoints are not available once a transaction spans several shards")
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, s := range []int{h.shardOf(t, a), h.shardOf(t, b)} {
		if !h.ranOn(s, "rollback") {
			t.Fatalf("shard %d not rolled back: %v", s, h.poolers[s].ran())
		}
	}
	if got := h.log.log(); len(got) != 0 {
		t.Fatalf("decision log %v", got)
	}
}

func TestExtendedProtocolCommitOfMultiShardTransaction(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	if _, err := conn.Prepare(ctx, "c", "commit"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []int64{a, b} {
		if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", tenant); err != nil {
			t.Fatal(err)
		}
	}
	tag, err := conn.Exec(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if tag.String() != "COMMIT" {
		t.Fatalf("tag %q", tag)
	}
	if got := h.log.log(); len(got) != 3 {
		t.Fatalf("decision log %v", got)
	}
	if h.prepared() != 0 {
		t.Fatalf("%d prepared transactions left", h.prepared())
	}
}

func TestShardWithoutPreparedCapacityRefusesEscalation(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	h.poolers[h.shardOf(t, b)].maxPrepared = "0"
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", b)
	_ = expectRefusal(t, err, fmt.Sprintf("shard default/%d has max_prepared_transactions = 0", h.shardOf(t, b)))
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.log.log(); len(got) != 0 {
		t.Fatalf("decision log %v", got)
	}
}

func TestClientPreparedTransactionStatementsAreReserved(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	for _, sql := range []string{"prepare transaction 'x'", "commit prepared 'x'", "rollback prepared 'x'"} {
		_, err := conn.Exec(ctx, sql)
		_ = expectRefusal(t, err, "PREPARE TRANSACTION, COMMIT PREPARED and ROLLBACK PREPARED are reserved")
	}
}

func expectRefusalCode(t *testing.T, err error, code string) *pgconn.PgError {
	t.Helper()
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("expected a %s error, got %v", code, err)
	}
	if pe.Code != code {
		t.Fatalf("expected %s, got %s %q", code, pe.Code, pe.Message)
	}
	return pe
}

func TestReaderThatWroteThroughAFunctionJoinsTwoPhaseCommit(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	sa, sb := h.shardOf(t, a), h.shardOf(t, b)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "select write_fn() from orders where tenant_id = $1", b)
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit must escalate to two-phase, not roll the function's write back: %v", err)
	}
	for _, s := range []int{sa, sb} {
		if !h.ranOn(s, "prepare transaction 'pgshard-") || !h.ranOn(s, "commit prepared 'pgshard-") {
			t.Fatalf("shard %d did not prepare and commit: %v", s, h.poolers[s].ran())
		}
	}
	if h.ranOn(sb, "rollback") {
		t.Fatalf("hidden writer was rolled back: %v", h.poolers[sb].ran())
	}
	if got := h.log.log(); len(got) != 3 {
		t.Fatalf("decision log %v", got)
	}
	if h.prepared() != 0 {
		t.Fatalf("%d prepared transactions left behind", h.prepared())
	}

	// A plain reader is still probed and still not prepared.
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", a); err != nil {
		t.Fatal(err)
	}
	rows, err = tx.Query(ctx, "select * from orders where tenant_id = $1", b)
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.allRan("prepare transaction"); len(got) != 2 {
		t.Fatalf("prepares after a plain read: %v", got)
	}
	if !h.ranOn(sb, strings.ToLower(hiddenWriteProbe)) {
		t.Fatalf("reader not probed for a hidden write: %v", h.poolers[sb].ran())
	}
}

func TestSingleModeRefusesCommitAfterHiddenWrite(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	if _, err := conn.Exec(ctx, "set pgshard.transaction_mode = single"); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "select write_fn() from orders where tenant_id = $1", b)
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	err = tx.Commit(ctx)
	_ = expectRefusal(t, err, "transaction wrote on shard")
	if got := h.allRan("prepare transaction"); len(got) != 0 {
		t.Fatalf("prepared in single mode: %v", got)
	}
	for _, s := range []int{h.shardOf(t, a), h.shardOf(t, b)} {
		if !h.ranOn(s, "rollback") || h.ranOn(s, "commit") {
			t.Fatalf("shard %d must roll back: %v", s, h.poolers[s].ran())
		}
	}
	if _, err := conn.Exec(ctx, "select 1"); err != nil {
		t.Fatalf("session unusable after refused commit: %v", err)
	}
}

func TestCoordinatorHeartbeatsWhilePreparing(t *testing.T) {
	old := decisionHeartbeatInterval
	decisionHeartbeatInterval = time.Millisecond
	t.Cleanup(func() { decisionHeartbeatInterval = old })
	h := newTxnHarness(t)
	h.log.waitHeartbeat = true
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, tenant := range []int64{a, b} {
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", tenant, i+1); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if h.log.heartbeatCount() == 0 {
		t.Fatal("coordinator never heartbeat its preparing decision row: the resolver would abort a slow live coordinator")
	}
}
