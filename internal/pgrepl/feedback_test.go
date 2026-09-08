package pgrepl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// deafServer completes a PostgreSQL startup handshake and then never reads
// from the connection again. It is the wedged walsender: the socket is open,
// the peer is alive, and nothing it is sent is ever consumed.
func deafServer(t *testing.T) string { return fakeServer(t, false) }

// attentiveServer completes the same handshake and then drains whatever it
// is sent, which is what a healthy walsender does with feedback.
func attentiveServer(t *testing.T) string { return fakeServer(t, true) }

func fakeServer(t *testing.T, drain bool) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		c, err := lis.Accept()
		if err != nil {
			return
		}
		// Held open, never read from, until the test ends.
		t.Cleanup(func() { _ = c.Close() })
		// A small send buffer so the client's writes fill it in
		// kilobytes rather than megabytes.
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetReadBuffer(4096)
		}
		be := pgproto3.NewBackend(c, c)
		if _, err := be.ReceiveStartupMessage(); err != nil {
			return
		}
		be.Send(&pgproto3.AuthenticationOk{})
		be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "18.6"})
		be.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		be.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
		be.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
		if drain {
			go func() { _, _ = io.Copy(io.Discard, c) }()
		}
		<-stop
	}()
	return fmt.Sprintf("postgres://postgres@%s/postgres?sslmode=disable", lis.Addr().String())
}

// TestAFeedbackWriteToADeafServerFails: a walsender that has stopped reading
// must not park the caller. The change stream's loop sends standby status
// from the goroutine that owns the stream, so a write that never returns
// takes the slot's release with it and the next consumer of that slot is
// refused for the life of the process.
func TestAFeedbackWriteToADeafServerFails(t *testing.T) {
	if os.Getenv("PGSHARD_SKIP_NETWORK_TESTS") != "" {
		t.Skip("network tests disabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Connect(ctx, deafServer(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	// Bounded, because Close writes a Terminate frame and this server
	// does not read that either.
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), time.Second)
		defer ccancel()
		_ = c.Close(cctx)
	})

	old := FeedbackWriteTimeout
	FeedbackWriteTimeout = 200 * time.Millisecond
	t.Cleanup(func() { FeedbackWriteTimeout = old })

	// Writes succeed into the kernel's buffers until they are full; the
	// bound is only reachable once they are, so send until one fails.
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 1_000_000; i++ {
			if err := c.SendStandbyStatus(StandbyStatus{Written: LSN(i)}); err != nil {
				done <- err
				return
			}
		}
		done <- errors.New("a million writes to a server that never reads all succeeded")
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("no error")
		}
		if !isNetTimeout(err) {
			t.Fatalf("want a write timeout, got %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("a feedback write to a server that never reads did not return")
	}
}

func isNetTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// TestAFeedbackWriteClearsItsDeadline: the bound belongs to that one write.
// Left on the socket it outlives the call and expires while the stream sits
// idle, and the next thing to write without setting its own deadline --
// START_REPLICATION, or the Terminate frame Close sends -- fails on a
// connection that is perfectly healthy. Another status update would not
// notice, because it sets a fresh deadline of its own first.
func TestAFeedbackWriteClearsItsDeadline(t *testing.T) {
	if os.Getenv("PGSHARD_SKIP_NETWORK_TESTS") != "" {
		t.Skip("network tests disabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Connect(ctx, attentiveServer(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	old := FeedbackWriteTimeout
	FeedbackWriteTimeout = 50 * time.Millisecond
	t.Cleanup(func() { FeedbackWriteTimeout = old })

	if err := c.SendStandbyStatus(StandbyStatus{Written: 1}); err != nil {
		t.Fatalf("status: %v", err)
	}
	// Past that write's deadline, which is the state an idle stream is in
	// when anything else on the connection next writes.
	time.Sleep(2 * FeedbackWriteTimeout)
	// A writer that sets no deadline of its own -- which is what
	// StartReplication's flush is.
	fe := c.pc.Frontend()
	fe.SendQuery(&pgproto3.Query{String: "SELECT 1"})
	if err := fe.Flush(); err != nil {
		t.Fatalf("write after an idle stream: %v", err)
	}
	cctx, ccancel := context.WithTimeout(context.Background(), time.Second)
	defer ccancel()
	_ = c.Close(cctx)
}
