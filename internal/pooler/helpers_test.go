package pooler

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// fakePG is an in-memory PostgreSQL stand-in: BEGIN/COMMIT/ROLLBACK track
// transaction status, "SELECT pg_sleep" blocks until cancelled or closed,
// everything else completes immediately.
type fakePG struct {
	queries atomic.Int64
	dials   atomic.Int64
	mu      sync.Mutex
	seen    []string
	block   chan struct{}
	// lastCK/lastSK alias the key slices handed to the most recent dial.
	lastCK, lastSK []byte
}

func newFakePG() *fakePG { return &fakePG{block: make(chan struct{})} }

func (f *fakePG) dial(_ context.Context, _, role string, ck, sk []byte) (*Backend, error) {
	f.dials.Add(1)
	f.mu.Lock()
	f.lastCK, f.lastSK = ck, sk
	f.mu.Unlock()
	client, server := net.Pipe()
	go f.serve(server)
	b := &Backend{conn: client, role: role, born: time.Now(), lastUsed: time.Now(), txStatus: 'I', pid: 4242, secret: []byte{1, 2, 3, 4}}
	b.fe = pgproto3.NewFrontend(bufio.NewReader(client), client)
	return b, nil
}

func (f *fakePG) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	be := pgproto3.NewBackend(bufio.NewReader(conn), conn)
	tx := byte('I')
	prepared := map[string]bool{}
	failed := false
	for {
		msg, err := be.Receive()
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *pgproto3.Parse:
			f.mu.Lock()
			f.seen = append(f.seen, "PARSE "+m.Name+" "+m.Query)
			f.mu.Unlock()
			if failed {
				continue
			}
			if m.Name != "" && prepared[m.Name] {
				failed = true
				be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05", Message: "prepared statement \"" + m.Name + "\" already exists"})
				continue
			}
			if strings.Contains(m.Query, "syntax error") {
				failed = true
				be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42601", Message: "syntax error"})
				continue
			}
			prepared[m.Name] = true
			be.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Close:
			f.mu.Lock()
			f.seen = append(f.seen, "CLOSE "+string(m.ObjectType)+" "+m.Name)
			f.mu.Unlock()
			if failed {
				continue
			}
			if m.ObjectType == 'S' {
				delete(prepared, m.Name)
			}
			be.Send(&pgproto3.CloseComplete{})
		case *pgproto3.Sync:
			failed = false
			be.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
			_ = be.Flush()
		case *pgproto3.Query:
			f.queries.Add(1)
			f.mu.Lock()
			f.seen = append(f.seen, m.String)
			f.mu.Unlock()
			q := strings.ToUpper(strings.TrimSpace(m.String))
			switch {
			case q == "DISCARD ALL":
				clear(prepared)
			case q == "BEGIN":
				tx = 'T'
			case q == "COMMIT" || q == "ROLLBACK":
				tx = 'I'
			case strings.HasPrefix(q, "SELECT PG_SLEEP"):
				<-f.block
				be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "canceling statement due to user request"})
				be.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
				_ = be.Flush()
				continue
			}
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte(q)})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
			_ = be.Flush()
		case *pgproto3.Terminate:
			return
		}
	}
}

func (f *fakePG) count(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, q := range f.seen {
		if strings.HasPrefix(q, prefix) {
			n++
		}
	}
	return n
}

func (f *fakePG) sawQueryFn(sql string) func() bool { return func() bool { return f.sawQuery(sql) } }

func (f *fakePG) sawQuery(sql string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, q := range f.seen {
		if strings.EqualFold(q, sql) {
			return true
		}
	}
	return false
}

type harness struct {
	pg     *fakePG
	src    *StaticSource
	srv    *Server
	client pgshardv1.PoolerClient
	logs   *strings.Builder
}

func startHarness(t *testing.T, cfg PoolConfig) *harness {
	t.Helper()
	pg := newFakePG()
	src := NewStaticSource(View{Generation: 7, Epoch: 3, Role: pgshardv1.HealthStatus_ROLE_PRIMARY, Serving: true})
	logs := &strings.Builder{}
	srv := NewServer(Config{Pool: newPool(cfg, pg.dial), Source: src, Database: "app",
		Logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})), HealthInterval: 20 * time.Millisecond})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	srv.Register(g)
	go func() { _ = g.Serve(l) }()
	conn, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		g.Stop()
		close(pg.block)
	})
	return &harness{pg: pg, src: src, srv: srv, client: pgshardv1.NewPoolerClient(conn), logs: logs}
}

func gen(g, e uint64) *pgshardv1.Generation {
	return &pgshardv1.Generation{ShardMapGeneration: g, PrimaryEpoch: e}
}

func identity(user string) *pgshardv1.UserIdentity {
	return &pgshardv1.UserIdentity{Username: user, ScramClientKey: testKey(0x11), ScramServerKey: testKey(0x22)}
}

func testKey(fill byte) []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = fill
	}
	return k
}

func queryReq(session, sql string, g *pgshardv1.Generation, user *pgshardv1.UserIdentity) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{SessionId: session, Generation: g, User: user,
		Message: &pgshardv1.ExecuteRequest_SimpleQuery{SimpleQuery: &pgshardv1.SimpleQuery{Sql: sql}}}
}

// roundTrip sends one request and collects responses through ReadyForQuery.
func roundTrip(t *testing.T, stream pgshardv1.Pooler_ExecuteClient, req *pgshardv1.ExecuteRequest) []*pgshardv1.ExecuteResponse {
	t.Helper()
	if err := stream.Send(req); err != nil {
		t.Fatal(err)
	}
	return collect(t, stream)
}

func collect(t *testing.T, stream pgshardv1.Pooler_ExecuteClient) []*pgshardv1.ExecuteResponse {
	t.Helper()
	var out []*pgshardv1.ExecuteResponse
	for {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v (got %d responses)", err, len(out))
		}
		out = append(out, resp)
		if resp.GetReadyForQuery() != nil {
			return out
		}
	}
}

func firstError(rs []*pgshardv1.ExecuteResponse) *pgshardv1.Error {
	for _, r := range rs {
		if e := r.GetError(); e != nil {
			return e.Error
		}
	}
	return nil
}

// recordingStream drives Server.Execute in-process so the request object the
// server mutates is observable.
type recordingStream struct {
	grpc.ServerStream
	ctx context.Context
	in  []*pgshardv1.ExecuteRequest
	out []*pgshardv1.ExecuteResponse
}

func (r *recordingStream) Context() context.Context { return r.ctx }

func (r *recordingStream) Send(m *pgshardv1.ExecuteResponse) error {
	r.out = append(r.out, m)
	return nil
}

func (r *recordingStream) Recv() (*pgshardv1.ExecuteRequest, error) {
	if len(r.in) == 0 {
		return nil, io.EOF
	}
	m := r.in[0]
	r.in = r.in[1:]
	return m, nil
}

func (h *harness) attached() bool {
	h.srv.mu.Lock()
	defer h.srv.mu.Unlock()
	se := h.srv.sessions["s"]
	return se != nil && se.attached
}
