package pooler

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// copyBackend is a PostgreSQL stand-in for COPY FROM STDIN: it answers
// every CopyData with a NOTICE of noticeBytes and keeps writing whether or
// not anyone reads, and after failAfter chunks (when non-zero) it ends the
// COPY with an error the way a constraint violation does.
func copyBackend(t *testing.T, noticeBytes, failAfter int) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				be := pgproto3.NewBackend(bufio.NewReader(conn), conn)
				inCopy, chunks := false, 0
				for {
					msg, err := be.Receive()
					if err != nil {
						return
					}
					switch m := msg.(type) {
					case *pgproto3.Query:
						if strings.HasPrefix(strings.ToLower(m.String), "copy") {
							inCopy, chunks = true, 0
							be.Send(&pgproto3.CopyInResponse{})
						} else {
							be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
							be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
						}
					case *pgproto3.CopyData:
						if !inCopy {
							continue
						}
						chunks++
						if failAfter > 0 && chunks == failAfter {
							inCopy = false
							be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "23505", Message: "duplicate key"})
							be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
							break
						}
						if noticeBytes > 0 {
							be.Send(&pgproto3.NoticeResponse{Severity: "NOTICE", Code: "00000", Message: strings.Repeat("n", noticeBytes)})
						}
					case *pgproto3.CopyDone:
						if !inCopy {
							continue
						}
						inCopy = false
						be.Send(&pgproto3.CommandComplete{CommandTag: []byte("COPY 1")})
						be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					case *pgproto3.Terminate:
						return
					}
					if err := be.Flush(); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

func copyPooler(t *testing.T, backendAddr string) pgshardv1.PoolerClient {
	t.Helper()
	dial := func(_ context.Context, database, role string, _, _ []byte) (*Backend, error) {
		client, err := net.Dial("tcp", backendAddr)
		if err != nil {
			return nil, err
		}
		b := &Backend{conn: client, role: role, database: database, born: time.Now(), lastUsed: time.Now(), txStatus: 'I', pid: 1, secret: []byte{1, 2, 3, 4}}
		b.fe = pgproto3.NewFrontend(bufio.NewReader(client), client)
		return b, nil
	}
	src := NewStaticSource(View{Generation: 7, Epoch: 3, Role: pgshardv1.HealthStatus_ROLE_PRIMARY, Serving: true})
	srv := NewServer(Config{Pool: newPool(PoolConfig{}, dial), Source: src, Database: "app",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), HealthInterval: 20 * time.Millisecond})
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
	t.Cleanup(func() { _ = conn.Close(); g.Stop() })
	return pgshardv1.NewPoolerClient(conn)
}

func copyDataReq(gn *pgshardv1.Generation, user *pgshardv1.UserIdentity, data []byte) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{SessionId: "copy", Generation: gn, User: user,
		Message: &pgshardv1.ExecuteRequest_CopyData{CopyData: &pgshardv1.CopyData{Data: data}}}
}

// A backend that raises a NOTICE per chunk used to block writing to a
// socket the pooler did not read until CopyDone; it then stopped reading
// CopyData and the load wedged. The notices now reach the router while the
// rows are still arriving, and the COPY ends normally.
func TestAPoolerRelaysNoticesWhileACopyIsLoading(t *testing.T) {
	client := copyPooler(t, copyBackend(t, 64<<10, 0))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gn, user := gen(7, 3), identity("app")
	if err := stream.Send(queryReq("copy", "copy t from stdin", gn, user)); err != nil {
		t.Fatal(err)
	}
	if r, err := stream.Recv(); err != nil || r.GetCopyInResponse() == nil {
		t.Fatalf("%v %v", r, err)
	}
	var notices atomic.Int64
	tag := make(chan string, 1)
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				return
			}
			switch {
			case r.GetNotice() != nil:
				notices.Add(1)
			case r.GetCommandComplete() != nil:
				tag <- r.GetCommandComplete().GetTag()
			}
		}
	}()
	chunk := bytes.Repeat([]byte("x"), 64<<10)
	sent := make(chan error, 1)
	go func() {
		for range 400 {
			if err := stream.Send(copyDataReq(gn, user, chunk)); err != nil {
				sent <- err
				return
			}
		}
		sent <- stream.Send(&pgshardv1.ExecuteRequest{SessionId: "copy", Generation: gn, User: user,
			Message: &pgshardv1.ExecuteRequest_CopyDone{CopyDone: &pgshardv1.CopyDone{}}})
	}()
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("the load wedged: %d notices relayed", notices.Load())
	}
	select {
	case got := <-tag:
		if got != "COPY 1" {
			t.Fatalf("tag %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the COPY's end never arrived")
	}
	if notices.Load() < 400 {
		t.Fatalf("%d notices relayed, want one per chunk", notices.Load())
	}
}

// A backend that ends the COPY with an error in the middle of the load has
// answered its ReadyForQuery. The rows still arriving and the CopyDone
// have nowhere to go, and the session's next statement is answered in step.
func TestAPoolerDropsWhatArrivesForACopyItsBackendAlreadyEnded(t *testing.T) {
	client := copyPooler(t, copyBackend(t, 0, 3))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gn, user := gen(7, 3), identity("app")
	if err := stream.Send(queryReq("copy", "copy t from stdin", gn, user)); err != nil {
		t.Fatal(err)
	}
	if r, err := stream.Recv(); err != nil || r.GetCopyInResponse() == nil {
		t.Fatalf("%v %v", r, err)
	}
	// The pooler writes CopyData to the backend in 64 KiB batches, so each
	// chunk here is one batch the backend reads.
	chunk := bytes.Repeat([]byte("x"), 64<<10)
	for range 3 {
		if err := stream.Send(copyDataReq(gn, user, chunk)); err != nil {
			t.Fatal(err)
		}
	}
	var code string
	for {
		r, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if e := r.GetError(); e != nil {
			code = e.GetError().GetSqlstate()
		}
		if r.GetReadyForQuery() != nil {
			break
		}
	}
	if code != "23505" {
		t.Fatalf("error %q, want the backend's 23505", code)
	}
	// Stragglers for the ended COPY, then the next statement.
	for _, req := range []*pgshardv1.ExecuteRequest{
		copyDataReq(gn, user, []byte("2\n")),
		{SessionId: "copy", Generation: gn, User: user, Message: &pgshardv1.ExecuteRequest_CopyDone{CopyDone: &pgshardv1.CopyDone{}}},
		queryReq("copy", "select 1", gn, user),
	} {
		if err := stream.Send(req); err != nil {
			t.Fatal(err)
		}
	}
	r, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if cc := r.GetCommandComplete(); cc == nil || cc.GetTag() != "SELECT 1" {
		t.Fatalf("the next statement's first answer is %v, want its own SELECT 1", r)
	}
}
