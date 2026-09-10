package pooler

import (
	"bufio"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"google.golang.org/grpc"
)

// failingStream fails every Send, standing in for a router that has gone.
type failingStream struct{ grpc.ServerStream }

func (failingStream) Send(*pgshardv1.ExecuteResponse) error { return errors.New("router gone") }
func (failingStream) Recv() (*pgshardv1.ExecuteRequest, error) {
	return nil, errors.New("router gone")
}

// TestASendFailureDiscardsTheBackend.
//
// When sending a reply to the router fails, the REST of that reply is still
// queued on the backend -- and nothing else can tell. hasUnflushed watches
// the WRITE side, and idle() reads the last observed txStatus, which is
// stale precisely because the ReadyForQuery that would update it is one of
// the messages still queued.
//
// So the backend looked clean: recycle ran DISCARD ALL, whose drain stopped
// at the aborted statement's ReadyForQuery and returned before DISCARD ALL's
// own reply arrived. It went back to the pool with someone else's messages
// in front, and the next session read them as its own -- desynchronising
// every session that reused it afterwards.
func TestASendFailureDiscardsTheBackend(t *testing.T) {
	for _, c := range []struct {
		name     string
		awaiting int
		pump     func(r *relay, b *Backend) error
	}{
		{"pump", 0, func(r *relay, b *Backend) error { return r.pump(b) }},
		// pumpFlush only reads while a batch is outstanding.
		{"pumpFlush", 1, func(r *relay, b *Backend) error { return r.pumpFlush(b) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			client, server := net.Pipe()
			t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
			// A server with a reply waiting: the relay reads the first
			// message and fails to forward it.
			go func() {
				be := pgproto3.NewBackend(bufio.NewReader(server), server)
				for {
					be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("x")}}})
					if err := be.Flush(); err != nil {
						return
					}
					time.Sleep(time.Millisecond)
				}
			}()
			b := &Backend{conn: client, role: "app", database: "app", born: time.Now(), lastUsed: time.Now(), txStatus: 'I'}
			b.fe = pgproto3.NewFrontend(bufio.NewReader(client), client)

			r := &relay{se: &session{id: "s1"}, stream: failingStream{}, awaiting: c.awaiting}
			if err := c.pump(r, b); err == nil {
				t.Fatal("the pump returned no error when the send failed")
			}
			// The premise: neither existing check can see the problem, which
			// is why the explicit mark is needed at all.
			if b.hasUnflushed() {
				t.Fatal("this test is meant to cover the case the unflushed check MISSES")
			}
			if !b.broken {
				t.Fatal("the backend went back to the pool with an unread reply still queued on it")
			}
		})
	}
}
