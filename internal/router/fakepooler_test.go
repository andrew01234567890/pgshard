package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// fakePooler scripts a shard: it models one backend per attached session with
// GUCs and prepared statements so replay after Release is observable.
type fakePooler struct {
	pgshardv1.UnimplementedPoolerServer
	gen, epoch uint64

	mu        sync.Mutex
	backends  map[string]*fakeBackend
	reserved  map[string]bool
	reserves  []string
	releases  []string
	cancels   []string
	users     []string
	sleeping  map[string]chan struct{}
	dropAfter string
	dropped   int
}

type fakeBackend struct {
	gucs  map[string]string
	stmts map[string]string
	tx    byte
	rows  int
}

func newFakePooler(gen, epoch uint64) *fakePooler {
	return &fakePooler{gen: gen, epoch: epoch, backends: map[string]*fakeBackend{}, reserved: map[string]bool{}, sleeping: map[string]chan struct{}{}}
}

func startFakePooler(t *testing.T, fp *fakePooler) string {
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

func (f *fakePooler) fence(g *pgshardv1.Generation) *pgshardv1.Error {
	if g == nil || g.ShardMapGeneration != f.gen || g.PrimaryEpoch != f.epoch {
		return &pgshardv1.Error{Sqlstate: "55000", Message: "stale routing generation"}
	}
	return nil
}

func (f *fakePooler) Reserve(_ context.Context, req *pgshardv1.ReserveRequest) (*pgshardv1.ReserveResponse, error) {
	if e := f.fence(req.Generation); e != nil {
		return &pgshardv1.ReserveResponse{Error: e}, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reserved[req.SessionId] = true
	f.reserves = append(f.reserves, req.SessionId)
	return &pgshardv1.ReserveResponse{BackendPid: 42}, nil
}

func (f *fakePooler) Release(_ context.Context, req *pgshardv1.ReleaseRequest) (*pgshardv1.ReleaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases = append(f.releases, req.SessionId)
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
	return s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_DataRow{DataRow: &pgshardv1.DataRow{Columns: []*pgshardv1.Value{{Data: []byte(v)}}}}})
}

// query answers one SQL statement; ready reports whether ReadyForQuery must
// follow (false while a COPY is open).
func (s *fakeStream) query(ctx context.Context, sql string) (ready bool, err error) {
	b := s.f.backend(s.sid)
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";")))
	if b.tx == 'E' && q != "rollback" && q != "commit" {
		return true, s.errorf("25P02", "current transaction is aborted")
	}
	switch {
	case q == "begin":
		b.tx = 'T'
		return true, s.complete("BEGIN")
	case q == "commit":
		tag := "COMMIT"
		if b.tx == 'E' {
			tag = "ROLLBACK"
		}
		b.tx = 'I'
		return true, s.complete(tag)
	case q == "rollback":
		b.tx = 'I'
		return true, s.complete("ROLLBACK")
	case strings.HasPrefix(q, "set "):
		name, val, _ := strings.Cut(strings.TrimPrefix(q, "set "), " to ")
		if !strings.Contains(q, " to ") {
			name, val, _ = strings.Cut(strings.TrimPrefix(q, "set "), " = ")
		}
		b.gucs[strings.TrimSpace(name)] = strings.Trim(strings.TrimSpace(val), "'")
		return true, s.complete("SET")
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
	case q == "select pg_sleep(10)":
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
	case q == "select bad":
		return true, s.errorf("42P01", "relation \"bad\" does not exist")
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
	}
	return true, s.errorf("42601", "fake pooler does not understand: "+q)
}

func (s *fakeStream) handle(ctx context.Context, req *pgshardv1.ExecuteRequest) error {
	if e := s.f.fence(req.Generation); e != nil {
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
			if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_BindComplete{BindComplete: &pgshardv1.BindComplete{}}}); err != nil {
				return err
			}
		case *pgshardv1.ExecuteRequest_Describe:
			var sql string
			if m.Describe.Kind == pgshardv1.Describe_KIND_STATEMENT {
				sql = b.stmts[m.Describe.Name]
				if err := s.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_ParameterDescription{ParameterDescription: &pgshardv1.ParameterDescription{}}}); err != nil {
					return err
				}
			} else {
				sql = b.stmts[portals[m.Describe.Name]]
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
			ready, err := s.query(ctx, sql)
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
	s := &fakeStream{f: f, sid: first.SessionId, stream: stream}
	req := first
	for {
		if q := req.GetSimpleQuery(); q != nil && f.dropAfter != "" && strings.Contains(q.Sql, f.dropAfter) {
			f.mu.Lock()
			f.dropped++
			f.mu.Unlock()
			return errors.New("fake pooler: dropping stream")
		}
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
