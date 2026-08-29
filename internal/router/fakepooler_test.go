package router

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// fakePooler scripts a shard: it models one backend per attached session with
// GUCs and prepared statements so replay after Release is observable.
type fakePooler struct {
	pgshardv1.UnimplementedPoolerServer
	gen, epoch uint64

	mu       sync.Mutex
	backends map[string]*fakeBackend
	reserved map[string]bool
	reserves []string
	releases []string
	cancels  []string
	users    []string
	sleeping map[string]chan struct{}
	attached map[string]chan struct{}
	// holdDetach, when set, keeps the Execute handler attached until it is
	// closed, so a test can hold a session across the router's abort.
	holdDetach chan struct{}
	// fenced is closed the first time a request is refused for a stale
	// generation, so a test can wait for the refusal instead of guessing.
	fenced    chan struct{}
	dropAfter string
	dropped   int
	// executed records every statement text this shard ran, in order;
	// bound records each extended-protocol execution with its parameters.
	executed []string
	bound    []string
	// scripts answers exact (lowercased) statements with canned results.
	scripts map[string]script
	// maxPrepared answers SHOW max_prepared_transactions ("64" when empty);
	// failPrepare makes PREPARE TRANSACTION fail; prepared lists the gids
	// currently prepared on the shard.
	maxPrepared string
	failPrepare bool
	prepared    []string
	// gate, when set, holds every Reserve until several shards are
	// reserving at once.
	gate *reserveGate
	// legacyRows answers with a Value submessage per column whatever the
	// router asked for, as a pooler that predates the packed shape does.
	legacyRows bool
}

// reserveGate holds every Reserve until gateWidth shards are reserving at
// once, so a test can tell a concurrent fan-out from a serial one without
// timing it: a serial setup never gets a second shard to the gate and
// every call falls out on the timeout instead.
type reserveGate struct {
	mu     sync.Mutex
	width  int
	count  int
	open   chan struct{}
	opened bool
}

func (g *reserveGate) wait() {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.open == nil {
		g.open = make(chan struct{})
	}
	open := g.open
	g.count++
	if g.count >= g.width {
		g.opened = true
		close(open)
	}
	g.mu.Unlock()
	select {
	case <-open:
	case <-time.After(500 * time.Millisecond):
	}
	g.mu.Lock()
	g.count--
	g.mu.Unlock()
}

func (g *reserveGate) reached() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.opened
}

func (f *fakePooler) preparedGIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.prepared...)
}

func (f *fakePooler) dropPrepared(gid string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, g := range f.prepared {
		if g == gid {
			f.prepared = append(f.prepared[:i], f.prepared[i+1:]...)
			return true
		}
	}
	return false
}

// script is a canned answer: an error, or typed columns and rows where the
// value "NULL" is a NULL.
type script struct {
	cols []scriptCol
	rows [][]string
	err  string
	// delay holds this shard back, so a test can decide which shard
	// answers first rather than leaving it to the scheduler.
	delay time.Duration
}

type scriptCol struct {
	name string
	oid  uint32
}

func (f *fakePooler) script(sql string, sc script) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.scripts == nil {
		f.scripts = map[string]script{}
	}
	f.scripts[strings.ToLower(sql)] = sc
}

func (s *fakeStream) scriptedDesc(sc script) error {
	fields := make([]*pgshardv1.FieldDescription, len(sc.cols))
	for i, c := range sc.cols {
		fields[i] = &pgshardv1.FieldDescription{Name: c.name, TypeOid: c.oid, TypeSize: -1, TypeModifier: -1}
		if s.binaryCol(i) {
			fields[i].Format = 1
		}
	}
	return s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_RowDescription{RowDescription: &pgshardv1.RowDescription{Fields: fields}}})
}

// scripted answers with the canned result; described says whether a
// portal Describe already sent the RowDescription (as PostgreSQL then omits
// it on Execute).
func (s *fakeStream) scripted(sc script, described bool) error {
	if sc.delay > 0 {
		time.Sleep(sc.delay)
	}
	if sc.err != "" {
		return s.errorf("42P01", sc.err)
	}
	if !described {
		if err := s.scriptedDesc(sc); err != nil {
			return err
		}
	}
	for _, r := range sc.rows {
		cols := make([]*pgshardv1.Value, len(r))
		for i, v := range r {
			switch {
			case v == "NULL":
				cols[i] = &pgshardv1.Value{Null: true}
			case s.binaryCol(i) && (sc.cols[i].oid == 23 || sc.cols[i].oid == 20):
				n, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return err
				}
				var buf [8]byte
				binary.BigEndian.PutUint64(buf[:], uint64(n))
				if sc.cols[i].oid == 23 {
					cols[i] = &pgshardv1.Value{Data: buf[4:]}
				} else {
					cols[i] = &pgshardv1.Value{Data: buf[:]}
				}
			default:
				cols[i] = &pgshardv1.Value{Data: []byte(v)}
			}
		}
		if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_DataRow{DataRow: s.dataRow(cols)}}); err != nil {
			return err
		}
	}
	return s.complete(fmt.Sprintf("SELECT %d", len(sc.rows)))
}

type fakeBackend struct {
	gucs  map[string]string
	stmts map[string]string
	tx    byte
	rows  int
	// xidAssigned mirrors pg_current_xact_id_if_assigned(): set by DML and
	// by "select write_fn()".
	xidAssigned bool
}

// fakeXIDs hands out distinct transaction ids across every fake shard.
var fakeXIDs atomic.Int64

// The fake pooler serves shard map generation 7 at primary epoch 2, the
// pair every harness snapshot starts from.
func newFakePooler() *fakePooler {
	return &fakePooler{gen: 7, epoch: 2, backends: map[string]*fakeBackend{}, reserved: map[string]bool{}, sleeping: map[string]chan struct{}{}, attached: map[string]chan struct{}{}}
}

func startFakePooler(t testing.TB, fp *fakePooler) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	pgshardv1.RegisterPoolerServer(g, fp)
	go func() { _ = g.Serve(l) }()
	t.Cleanup(g.Stop)
	return l.Addr().String()
}

func (f *fakePooler) backend(sid string) *fakeBackend {
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.backends[sid]
	if b == nil {
		b = &fakeBackend{gucs: map[string]string{}, stmts: map[string]string{}, tx: 'I'}
		f.backends[sid] = b
	}
	return b
}

// attach mirrors the real pooler: a session may have only one Execute stream
// at a time (internal/pooler/server.go attachSession). Serializing instead
// would let the tests pass on an overlap production rejects.
func (f *fakePooler) attach(sid string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.attached[sid]; ok {
		return status.Error(codes.FailedPrecondition, "session already has an Execute stream")
	}
	f.attached[sid] = make(chan struct{})
	return nil
}

func (f *fakePooler) detach(sid string) {
	f.mu.Lock()
	ch := f.attached[sid]
	delete(f.attached, sid)
	// Like the real detach: a session nobody reserved gives its backend up
	// when its stream ends, so the next stream starts clean and a missing
	// replay shows up instead of being served from the old state.
	if !f.reserved[sid] {
		delete(f.backends, sid)
	}
	f.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (f *fakePooler) fence(g *pgshardv1.Generation) *pgshardv1.Error {
	if g == nil || g.ShardMapGeneration != f.gen || g.PrimaryEpoch != f.epoch {
		return &pgshardv1.Error{Sqlstate: "55000", Message: "stale routing generation"}
	}
	return nil
}

func (f *fakePooler) Reserve(_ context.Context, req *pgshardv1.ReserveRequest) (*pgshardv1.ReserveResponse, error) {
	f.gate.wait()
	if e := f.fence(req.Generation); e != nil {
		return &pgshardv1.ReserveResponse{Error: e}, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reserved[req.SessionId] = true
	f.reserves = append(f.reserves, req.SessionId)
	return &pgshardv1.ReserveResponse{BackendPid: 42}, nil
}

func (f *fakePooler) Release(ctx context.Context, req *pgshardv1.ReleaseRequest) (*pgshardv1.ReleaseResponse, error) {
	f.mu.Lock()
	f.releases = append(f.releases, req.SessionId)
	// Like the real Release: while the session still has an Execute stream
	// the caller waits for it to detach, which is what orders a reacquire
	// behind the stream the router aborted.
	if ch, ok := f.attached[req.SessionId]; ok {
		delete(f.reserved, req.SessionId)
		f.mu.Unlock()
		select {
		case <-ch:
			return &pgshardv1.ReleaseResponse{}, nil
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	defer f.mu.Unlock()
	delete(f.reserved, req.SessionId)
	delete(f.backends, req.SessionId)
	return &pgshardv1.ReleaseResponse{}, nil
}

func (f *fakePooler) Cancel(_ context.Context, req *pgshardv1.CancelRequest) (*pgshardv1.CancelResponse, error) {
	f.mu.Lock()
	f.cancels = append(f.cancels, req.SessionId)
	ch := f.sleeping[req.SessionId]
	delete(f.sleeping, req.SessionId)
	f.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return &pgshardv1.CancelResponse{}, nil
}

type fakeStream struct {
	f      *fakePooler
	sid    string
	stream pgshardv1.Pooler_ExecuteServer
	batch  []*pgshardv1.ExecuteRequest
	copyIn []byte
	inCopy bool
	// packed answers rows the way a current pooler does once the router
	// has asked, so the whole suite exercises that shape rather than only
	// the one a pooler that predates the request field sends.
	packed bool
	// binary is set while a Bind asked for binary results; described while
	// the portal was described before Execute.
	formats   []int32
	described bool
	// params renders the parameters bound to each portal.
	params map[string]string
}

func (f *fakePooler) boundExecs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.bound...)
}

// binaryCol reports whether the last Bind asked for column i in binary.
func (s *fakeStream) binaryCol(i int) bool {
	switch len(s.formats) {
	case 0:
		return false
	case 1:
		return s.formats[0] == 1
	}
	return i < len(s.formats) && s.formats[i] == 1
}

func (s *fakeStream) send(m *pgshardv1.ExecuteResponse) error {
	m.SessionId = s.sid
	return s.stream.Send(m)
}

func (s *fakeStream) rfq() error {
	b := s.f.backend(s.sid)
	st := pgshardv1.ReadyForQuery_TXN_STATUS_IDLE
	switch b.tx {
	case 'T':
		st = pgshardv1.ReadyForQuery_TXN_STATUS_IN_TRANSACTION
	case 'E':
		st = pgshardv1.ReadyForQuery_TXN_STATUS_FAILED
	}
	if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_ReadyForQuery{ReadyForQuery: &pgshardv1.ReadyForQuery{TxnStatus: st}}}); err != nil {
		return err
	}
	if b.tx == 'I' && !s.f.isReserved(s.sid) {
		s.f.mu.Lock()
		delete(s.f.backends, s.sid)
		s.f.mu.Unlock()
	}
	return nil
}

// shouldDrop reports whether sql matches dropAfter, counting the drop. The
// test goroutine writes dropAfter while this runs on the gRPC server
// goroutine, and the loopback socket gives the race detector no
// happens-before edge between them.
func (f *fakePooler) shouldDrop(sql string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dropAfter == "" || !strings.Contains(sql, f.dropAfter) {
		return false
	}
	f.dropped++
	return true
}

// setDropAfter arms the drop from the test goroutine.
func (f *fakePooler) setDropAfter(sub string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropAfter = sub
}

// dropCount reports how many streams were dropped.
func (f *fakePooler) dropCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dropped
}

// cancelled reports the session ids cancelled so far.
func (f *fakePooler) cancelled() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cancels...)
}

// ran reports the statements this shard executed.
func (f *fakePooler) ran() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.executed...)
}

func (f *fakePooler) isReserved(sid string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reserved[sid]
}

func (s *fakeStream) errorf(code, msg string) error {
	b := s.f.backend(s.sid)
	if b.tx == 'T' {
		b.tx = 'E'
	}
	return s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_Error{Error: &pgshardv1.ErrorResponse{Error: &pgshardv1.Error{Sqlstate: code, Message: msg}}}})
}

func (s *fakeStream) complete(tag string) error {
	return s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_CommandComplete{CommandComplete: &pgshardv1.CommandComplete{Tag: tag}}})
}

func (s *fakeStream) rowDesc(name string, oid uint32) error {
	return s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_RowDescription{RowDescription: &pgshardv1.RowDescription{
		Fields: []*pgshardv1.FieldDescription{{Name: name, TypeOid: oid, TypeSize: -1, TypeModifier: -1}}}}})
}

func (s *fakeStream) row(v string) error {
	return s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_DataRow{DataRow: s.dataRow([]*pgshardv1.Value{{Data: []byte(v)}})}})
}

// dataRow answers in whichever shape the router asked for.
func (s *fakeStream) dataRow(cols []*pgshardv1.Value) *pgshardv1.DataRow {
	if !s.packed {
		return &pgshardv1.DataRow{Columns: cols}
	}
	out := &pgshardv1.DataRow{Packed: make([][]byte, len(cols))}
	for i, c := range cols {
		if c.Null {
			out.Nulls = append(out.Nulls, uint32(i))
			continue
		}
		out.Packed[i] = c.Data
	}
	return out
}

// query answers one SQL statement; ready reports whether ReadyForQuery must
// follow (false while a COPY is open).
func (s *fakeStream) query(ctx context.Context, sql string) (ready bool, err error) {
	b := s.f.backend(s.sid)
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";")))
	s.f.mu.Lock()
	s.f.executed = append(s.f.executed, q)
	sc, scripted := s.f.scripts[q]
	s.f.mu.Unlock()
	if scripted {
		return true, s.scripted(sc, s.described)
	}
	if b.tx == 'E' && q != "rollback" && q != "commit" {
		return true, s.errorf("25P02", "current transaction is aborted")
	}
	switch {
	case q == "begin", strings.HasPrefix(q, "begin isolation level "):
		b.tx = 'T'
		return true, s.complete("BEGIN")
	case q == "commit":
		tag := "COMMIT"
		if b.tx == 'E' {
			tag = "ROLLBACK"
		}
		b.tx, b.xidAssigned = 'I', false
		return true, s.complete(tag)
	case q == "rollback":
		b.tx, b.xidAssigned = 'I', false
		return true, s.complete("ROLLBACK")
	case strings.HasPrefix(q, "savepoint "):
		return true, s.complete("SAVEPOINT")
	case strings.HasPrefix(q, "rollback to "):
		return true, s.complete("ROLLBACK")
	case strings.HasPrefix(q, "release "):
		return true, s.complete("RELEASE")
	case strings.HasPrefix(q, "prepare ") && strings.Contains(q, " as "):
		name, body, _ := strings.Cut(strings.TrimPrefix(q, "prepare "), " as ")
		b.stmts[strings.TrimSpace(name)] = strings.TrimSpace(body)
		return true, s.complete("PREPARE")
	case strings.HasPrefix(q, "execute "):
		body, ok := b.stmts[strings.TrimSpace(strings.TrimPrefix(q, "execute "))]
		if !ok {
			return true, s.errorf("26000", "prepared statement does not exist")
		}
		return s.query(ctx, body)
	case q == "deallocate all":
		b.stmts = map[string]string{}
		return true, s.complete("DEALLOCATE ALL")
	case strings.HasPrefix(q, "deallocate "):
		name := strings.Trim(strings.TrimSpace(strings.TrimPrefix(q, "deallocate ")), `"`)
		if _, ok := b.stmts[name]; !ok {
			return true, s.errorf("26000", "prepared statement \""+name+"\" does not exist")
		}
		delete(b.stmts, name)
		return true, s.complete("DEALLOCATE")
	case q == "discard all":
		b.gucs, b.stmts = map[string]string{}, map[string]string{}
		return true, s.complete("DISCARD ALL")
	case q == "select pg_current_xact_id_if_assigned() is not null":
		if err := s.rowDesc("?column?", 16); err != nil {
			return true, err
		}
		v := "f"
		if b.xidAssigned {
			v = "t"
		}
		if err := s.row(v); err != nil {
			return true, err
		}
		return true, s.complete("SELECT 1")
	case strings.HasPrefix(q, "select write_fn()"):
		b.rows++
		b.xidAssigned = true
		if err := s.rowDesc("write_fn", 23); err != nil {
			return true, err
		}
		if err := s.row("1"); err != nil {
			return true, err
		}
		return true, s.complete("SELECT 1")
	case q == "show max_prepared_transactions":
		v := s.f.maxPrepared
		if v == "" {
			v = "64"
		}
		if err := s.rowDesc("max_prepared_transactions", 25); err != nil {
			return true, err
		}
		if err := s.row(v); err != nil {
			return true, err
		}
		return true, s.complete("SHOW")
	case q == "select pg_current_xact_id()::text":
		if err := s.rowDesc("pg_current_xact_id", 25); err != nil {
			return true, err
		}
		if err := s.row(fmt.Sprint(1000 + fakeXIDs.Add(1))); err != nil {
			return true, err
		}
		return true, s.complete("SELECT 1")
	case strings.HasPrefix(q, "prepare transaction '"):
		gid := strings.TrimSuffix(strings.TrimPrefix(q, "prepare transaction '"), "'")
		b.tx, b.xidAssigned = 'I', false
		if s.f.failPrepare {
			return true, s.errorf("55000", "prepare refused by the fake shard")
		}
		s.f.mu.Lock()
		s.f.prepared = append(s.f.prepared, gid)
		s.f.mu.Unlock()
		return true, s.complete("PREPARE TRANSACTION")
	case strings.HasPrefix(q, "commit prepared '"), strings.HasPrefix(q, "rollback prepared '"):
		verb, rest, _ := strings.Cut(q, " prepared '")
		gid := strings.TrimSuffix(rest, "'")
		if !s.f.dropPrepared(gid) {
			return true, s.errorf("42704", "prepared transaction with identifier \""+gid+"\" does not exist")
		}
		return true, s.complete(strings.ToUpper(verb) + " PREPARED")
	case strings.HasPrefix(q, "set "):
		name, val, _ := strings.Cut(strings.TrimPrefix(q, "set "), " to ")
		if !strings.Contains(q, " to ") {
			name, val, _ = strings.Cut(strings.TrimPrefix(q, "set "), " = ")
		}
		b.gucs[strings.TrimSpace(name)] = strings.Trim(strings.TrimSpace(val), "'")
		return true, s.complete("SET")
	case q == "reset all":
		b.gucs = map[string]string{}
		return true, s.complete("RESET")
	case strings.HasPrefix(q, "reset "):
		delete(b.gucs, strings.TrimSpace(strings.TrimPrefix(q, "reset ")))
		return true, s.complete("RESET")
	case strings.HasPrefix(q, "select set_config('search_path', '"):
		val := strings.TrimPrefix(q, "select set_config('search_path', '")
		val, _, _ = strings.Cut(val, "', false)")
		b.gucs["search_path"] = strings.ReplaceAll(val, "''", "'")
		if err := s.rowDesc("set_config", 25); err != nil {
			return true, err
		}
		if err := s.row(b.gucs["search_path"]); err != nil {
			return true, err
		}
		return true, s.complete("SELECT 1")
	case strings.HasPrefix(q, "select current_setting('"):
		name := strings.TrimSuffix(strings.TrimPrefix(q, "select current_setting('"), "')")
		if err := s.rowDesc("current_setting", 25); err != nil {
			return true, err
		}
		if err := s.row(b.gucs[name]); err != nil {
			return true, err
		}
		return true, s.complete("SELECT 1")
	case q == "select 1":
		if err := s.rowDesc("?column?", 23); err != nil {
			return true, err
		}
		if err := s.row("1"); err != nil {
			return true, err
		}
		return true, s.complete("SELECT 1")
	case strings.HasPrefix(q, "select pg_sleep(10)"):
		ch := make(chan struct{})
		s.f.mu.Lock()
		s.f.sleeping[s.sid] = ch
		s.f.mu.Unlock()
		select {
		case <-ch:
			return true, s.errorf("57014", "canceling statement due to user request")
		case <-ctx.Done():
			return true, ctx.Err()
		}
	case q == "copy t from stdin":
		s.inCopy = true
		return false, s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_CopyInResponse{CopyInResponse: &pgshardv1.CopyInResponse{}}})
	case q == "select midrow_stale":
		if err := s.rowDesc("n", 23); err != nil {
			return true, err
		}
		if err := s.row("1"); err != nil {
			return true, err
		}
		return true, s.errorf("55000", "stale routing generation")
	case q == "select bad":
		return true, s.errorf("42P01", "relation \"bad\" does not exist")
	case q == "select notify":
		if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_Notification{Notification: &pgshardv1.NotificationResponse{Pid: 9, Channel: "events", Payload: "hello"}}}); err != nil {
			return true, err
		}
		return true, s.complete("SELECT 0")
	case q == "select notice":
		if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_Notice{Notice: &pgshardv1.NoticeResponse{Notice: &pgshardv1.Error{Sqlstate: "00000", Message: "hello"}}}}); err != nil {
			return true, err
		}
		return true, s.complete("SELECT 0")
	case q == "select rows":
		if err := s.rowDesc("n", 23); err != nil {
			return true, err
		}
		if err := s.row(fmt.Sprint(b.rows)); err != nil {
			return true, err
		}
		return true, s.complete("SELECT 1")
	case strings.HasPrefix(q, "insert into "):
		b.rows++
		b.xidAssigned = true
		return true, s.complete("INSERT 0 1")
	case strings.HasPrefix(q, "update "):
		b.xidAssigned = true
		return true, s.complete("UPDATE 1")
	case strings.HasPrefix(q, "delete from "):
		b.xidAssigned = true
		return true, s.complete("DELETE 1")
	case strings.HasPrefix(q, "select * from "):
		if err := s.rowDesc("id", 23); err != nil {
			return true, err
		}
		if err := s.row(fmt.Sprint(b.rows)); err != nil {
			return true, err
		}
		return true, s.complete("SELECT 1")
	}
	return true, s.errorf("42601", "fake pooler does not understand: "+q)
}

func (s *fakeStream) handle(ctx context.Context, req *pgshardv1.ExecuteRequest) error {
	if e := s.f.fence(req.Generation); e != nil {
		s.f.mu.Lock()
		if s.f.fenced != nil {
			close(s.f.fenced)
			s.f.fenced = nil
		}
		s.f.mu.Unlock()
		if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_Error{Error: &pgshardv1.ErrorResponse{Error: e}}}); err != nil {
			return err
		}
		return s.rfq()
	}
	b := s.f.backend(s.sid)
	switch m := req.Message.(type) {
	case *pgshardv1.ExecuteRequest_SimpleQuery:
		var results []string
		for _, part := range strings.Split(m.SimpleQuery.Sql, ";") {
			if strings.TrimSpace(part) == "" {
				continue
			}
			ready, err := s.query(ctx, part)
			if err != nil {
				return err
			}
			if !ready {
				return nil
			}
			results = append(results, part)
		}
		_ = results
		return s.rfq()
	case *pgshardv1.ExecuteRequest_CopyData:
		s.copyIn = append(s.copyIn, m.CopyData.Data...)
		return nil
	case *pgshardv1.ExecuteRequest_CopyDone:
		n := strings.Count(strings.TrimSpace(string(s.copyIn)), "\n") + 1
		if len(strings.TrimSpace(string(s.copyIn))) == 0 {
			n = 0
		}
		b.rows += n
		s.copyIn, s.inCopy = nil, false
		if err := s.complete(fmt.Sprintf("COPY %d", n)); err != nil {
			return err
		}
		return s.rfq()
	case *pgshardv1.ExecuteRequest_CopyFail:
		s.copyIn, s.inCopy = nil, false
		if err := s.errorf("57014", "COPY from stdin failed: "+m.CopyFail.Message); err != nil {
			return err
		}
		return s.rfq()
	case *pgshardv1.ExecuteRequest_Sync:
		if err := s.runBatch(ctx); err != nil {
			return err
		}
		return s.rfq()
	case *pgshardv1.ExecuteRequest_Flush:
		// Like Sync but with the pooler's own marker instead of
		// ReadyForQuery, which is what a Flush produces on the wire.
		if err := s.runBatch(ctx); err != nil {
			return err
		}
		return s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_FlushComplete{FlushComplete: &pgshardv1.FlushComplete{}}})
	default:
		s.batch = append(s.batch, req)
		return nil
	}
}

func (s *fakeStream) runBatch(ctx context.Context) error {
	batch := s.batch
	s.batch = nil
	b := s.f.backend(s.sid)
	portals := map[string]string{}
	failed := false
	for _, req := range batch {
		if failed {
			break
		}
		switch m := req.Message.(type) {
		case *pgshardv1.ExecuteRequest_Parse:
			if _, exists := b.stmts[m.Parse.Name]; exists && m.Parse.Name != "" {
				failed = true
				if err := s.errorf("42P05", "prepared statement already exists"); err != nil {
					return err
				}
				continue
			}
			b.stmts[m.Parse.Name] = m.Parse.Sql
			if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_ParseComplete{ParseComplete: &pgshardv1.ParseComplete{}}}); err != nil {
				return err
			}
		case *pgshardv1.ExecuteRequest_Bind:
			if _, ok := b.stmts[m.Bind.Statement]; !ok {
				failed = true
				if err := s.errorf("26000", "prepared statement does not exist"); err != nil {
					return err
				}
				continue
			}
			portals[m.Bind.Portal] = m.Bind.Statement
			var vals []string
			for _, p := range m.Bind.Params {
				if p.Null {
					vals = append(vals, "NULL")
				} else {
					vals = append(vals, string(p.Data))
				}
			}
			if s.params == nil {
				s.params = map[string]string{}
			}
			s.params[m.Bind.Portal] = strings.Join(vals, ",")
			s.formats = m.Bind.ResultFormats
			s.described = false
			if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_BindComplete{BindComplete: &pgshardv1.BindComplete{}}}); err != nil {
				return err
			}
		case *pgshardv1.ExecuteRequest_Describe:
			var sql string
			if m.Describe.Kind == pgshardv1.Describe_KIND_STATEMENT {
				sql = b.stmts[m.Describe.Name]
				if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_ParameterDescription{ParameterDescription: &pgshardv1.ParameterDescription{ParamOids: paramOIDs(sql)}}}); err != nil {
					return err
				}
			} else {
				sql = b.stmts[portals[m.Describe.Name]]
			}
			s.f.mu.Lock()
			sc, scripted := s.f.scripts[strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";")))]
			s.f.mu.Unlock()
			if scripted && sc.err == "" {
				if err := s.scriptedDesc(sc); err != nil {
					return err
				}
				if m.Describe.Kind == pgshardv1.Describe_KIND_PORTAL {
					s.described = true
				}
				continue
			}
			if strings.HasPrefix(strings.ToLower(sql), "select") {
				if err := s.rowDesc("?column?", 23); err != nil {
					return err
				}
			} else if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_NoData{NoData: &pgshardv1.NoData{}}}); err != nil {
				return err
			}
		case *pgshardv1.ExecuteRequest_Execute:
			sql := b.stmts[portals[m.Execute.Portal]]
			s.f.mu.Lock()
			s.f.bound = append(s.f.bound, strings.ToLower(sql)+" <- "+s.params[m.Execute.Portal])
			s.f.mu.Unlock()
			// A row limit suspends the portal instead of completing it,
			// which is what a real backend does once it has sent the
			// limit's worth of rows.
			if m.Execute.MaxRows > 0 {
				if err := s.rowDesc("id", 23); err != nil {
					return err
				}
				for i := range int(m.Execute.MaxRows) {
					if err := s.row(fmt.Sprint(i)); err != nil {
						return err
					}
				}
				if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_PortalSuspended{PortalSuspended: &pgshardv1.PortalSuspended{}}}); err != nil {
					return err
				}
				s.described, s.formats = false, nil
				continue
			}
			ready, err := s.query(ctx, sql)
			s.described, s.formats = false, nil
			if err != nil {
				return err
			}
			if !ready {
				return errors.New("fake pooler: COPY through the extended protocol is not scripted")
			}
			if b.tx == 'E' {
				failed = true
			}
		case *pgshardv1.ExecuteRequest_Close:
			if m.Close.Kind == pgshardv1.Close_KIND_STATEMENT {
				delete(b.stmts, m.Close.Name)
			}
		}
	}
	return nil
}

// paramOIDs mimics the backend's parameter inference: statements over the
// text-keyed docs table take text parameters, everything else int8.
func paramOIDs(sql string) []uint32 {
	n := strings.Count(sql, "$")
	oid := uint32(20)
	if strings.Contains(sql, "docs") {
		oid = 25
	}
	out := make([]uint32, n)
	for i := range out {
		out[i] = oid
	}
	return out
}

func (f *fakePooler) Execute(stream pgshardv1.Pooler_ExecuteServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.User == nil || first.User.Username == "" || len(first.User.ScramClientKey) == 0 {
		return errors.New("first message must carry the identity")
	}
	f.mu.Lock()
	f.users = append(f.users, first.User.Username)
	f.mu.Unlock()
	if err := f.attach(first.SessionId); err != nil {
		return err
	}
	defer func() {
		f.mu.Lock()
		hold := f.holdDetach
		f.mu.Unlock()
		if hold != nil {
			<-hold
		}
		f.detach(first.SessionId)
	}()
	s := &fakeStream{f: f, sid: first.SessionId, stream: stream, packed: first.PackedRows && !f.legacyRows}
	req := first
	for {
		// NOTE: dropAfter is read WITHOUT f.mu, and that is load-bearing
		// rather than an oversight. Reading it under the lock makes the drop
		// fire deterministically where it previously raced, and
		// TestStreamLossAfterSendIsNotRetried then loops forever: the router
		// reconnects, the pooler drops again, and the test hangs. Closing
		// this race needs the test's own synchronisation rethought, not a
		// mutex here -- see PGS-305.
		if q := req.GetSimpleQuery(); q != nil && f.shouldDrop(q.Sql) {
			return errors.New("fake pooler: dropping stream")
		}
		s.packed = s.packed || (req.PackedRows && !f.legacyRows)
		if err := s.handle(stream.Context(), req); err != nil {
			return err
		}
		req, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
