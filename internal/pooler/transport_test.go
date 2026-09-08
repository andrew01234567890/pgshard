package pooler_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pooler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// countingListener records the size of every write the gRPC server makes to
// a socket, which is the only place the transport write buffer is visible:
// the framer fills it and hands the kernel one write per flush.
type countingListener struct {
	net.Listener
	writes, bytes *atomic.Int64
}

func (l countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return countingConn{Conn: c, writes: l.writes, bytes: l.bytes}, nil
}

type countingConn struct {
	net.Conn
	writes, bytes *atomic.Int64
}

func (c countingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.writes.Add(1)
	c.bytes.Add(int64(n))
	return n, err
}

type rowFlood struct {
	pgshardv1.UnimplementedPoolerServer
	rows int
}

func (f rowFlood) Execute(stream grpc.BidiStreamingServer[pgshardv1.ExecuteRequest, pgshardv1.ExecuteResponse]) error {
	if _, err := stream.Recv(); err != nil {
		return nil
	}
	row := make([]byte, 4096)
	for range f.rows {
		if err := stream.Send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_DataRow{
			DataRow: &pgshardv1.DataRow{Columns: []*pgshardv1.Value{{Data: row}}}}}); err != nil {
			return err
		}
	}
	return stream.Send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_ReadyForQuery{
		ReadyForQuery: &pgshardv1.ReadyForQuery{}}})
}

// flood streams 8 MiB of rows through a server built with srvOpts and
// returns the mean size of the socket writes the server made.
func flood(t *testing.T, srvOpts []grpc.ServerOption, dialOpts []grpc.DialOption) int64 {
	t.Helper()
	const rows = 2048
	var writes, bytes atomic.Int64

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lis := countingListener{Listener: raw, writes: &writes, bytes: &bytes}
	srv := grpc.NewServer(srvOpts...)
	pgshardv1.RegisterPoolerServer(srv, rowFlood{rows: rows})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	cc, err := grpc.NewClient(raw.Addr().String(), dialOpts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	stream, err := pgshardv1.NewPoolerClient(cc).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pgshardv1.ExecuteRequest{SessionId: "s",
		Message: &pgshardv1.ExecuteRequest_Sync{Sync: &pgshardv1.Sync{}}}); err != nil {
		t.Fatal(err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if _, done := resp.Message.(*pgshardv1.ExecuteResponse_ReadyForQuery); done {
			break
		}
	}
	return bytes.Load() / max(writes.Load(), 1)
}

// A pooler built from ServerOptions hands the kernel larger writes than one
// built from grpc-go's defaults. That is the only observable consequence of
// the transport buffer, so it is what stops the option being dropped from
// the helper without anything noticing.
//
// The assertion is a ratio rather than a byte count because the writes do
// not reach the buffer size: the flow-control window, which is left at
// grpc-go's dynamic default on purpose, stops the framer before the buffer
// fills. Fewer, larger writes is the whole of the effect and all that is
// being claimed.
func TestThePoolerWritesInLargerPiecesThanTheDefault(t *testing.T) {
	insec := insecure.NewCredentials()
	base := flood(t,
		[]grpc.ServerOption{grpc.Creds(insec)},
		[]grpc.DialOption{grpc.WithTransportCredentials(insec)})
	tuned := flood(t,
		pooler.ServerOptions(grpc.Creds(insec)),
		pooler.DialOptions(grpc.WithTransportCredentials(insec)))
	if tuned < base+base/8 {
		t.Fatalf("mean socket write %d bytes with the pooler's options and %d with grpc-go's defaults: the transport buffer is not in effect",
			tuned, base)
	}
	t.Logf("mean socket write: %d bytes default, %d bytes with TransportBufferBytes=%d", base, tuned, pooler.TransportBufferBytes)
}
