package pooler

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// MaxMessageBytes is the largest protobuf message the router and pooler
// exchange, and so the largest a single Bind value, DataRow or COPY chunk
// may be once encoded.
//
// It is 4 MiB because that is grpc-go's default and therefore what pgshard
// has always enforced -- silently, since nothing set it. Naming it changes
// no behaviour; it makes the boundary a decision that can be found, tested
// and reported instead of a dependency's default surfacing as a lost
// connection.
//
// PostgreSQL itself accepts protocol messages up to 1 GiB, so this is a
// real narrowing of the contract and is documented as such.
//
// What used to block raising it no longer does: rows and COPY chunks were
// bounded by item count, so a larger limit would have let a handful of
// wide rows hold hundreds of megabytes in the router. They are bounded by
// bytes now -- the pgwire writer flushes on a byte threshold, the pooler
// batches rows to a byte cap, and a router's read-ahead on a pooler stream
// spends a byte credit per message.
//
// What still argues for a number rather than PostgreSQL's is that one
// message is still decoded whole: the bounds stop many large messages
// accumulating, not one large message existing. Raising this is now a
// judgement about that single allocation, taken against a measurement,
// rather than a thing that cannot be done.
const MaxMessageBytes = 4 << 20

// TransportBufferBytes is the read and write buffer grpc-go puts between
// the HTTP/2 framer and the socket, on both halves of a router-to-pooler
// connection.
//
// grpc-go defaults to 32 KiB. On a stream of 4 KiB rows -- a scatter
// draining a shard, a COPY, a change stream -- 128 KiB moves the same
// bytes about 20% faster, because the framer stops handing the kernel a
// write every eight rows. 256 KiB measured no better than 128 KiB, so this
// is the knee rather than the largest number that helped.
//
// It costs a fixed read buffer of this size per connection, and a write
// buffer only while a connection is mid-flush: grpc-go's SharedWriteBuffer
// is on by default, so the write side is taken from a pool at the first
// write and returned on Flush. A router holds one connection per pooler
// endpoint, so either way the footprint is bounded by the topology rather
// than by the session count.
//
// The flow-control windows are deliberately left alone. Raising them is
// worth a further ~16% on the same stream, but grpc-go's options for it
// also switch off the BDP estimator, and the measured cost is that one
// stalled consumer parks its whole stream window instead of the 192 KiB it
// parks today: 4 MiB windows made it 4.05 MiB. A router fans a scatter
// across every shard and holds a change stream per consumer, so that
// multiplies by exactly the thing that is already the memory pressure.
//
// MaxConnectionAge is left alone for the same kind of reason: it exists to
// rebalance long-lived connections, and the connections here are long-lived
// on purpose. A change stream that is cut every N minutes has to re-derive
// its position, which is a real cost in exchange for a rebalance the
// catalog's endpoint map already performs.
const TransportBufferBytes = 128 << 10

// Keepalive is how the router keeps a pooler connection honest, and
// KeepaliveEnforcement is what the pooler's server must permit for it. They
// live together because they are one setting with two halves: gRPC's DEFAULT
// enforcement answers a client pinging more often than every five minutes,
// or pinging at all without an active stream, with GOAWAY too_many_pings and
// tears the connection down. Setting only the client half is worse than
// setting neither.
//
// The reason for pinging at all is a change stream: the reader sits in
// stream.Recv() until a batch arrives and compares the shard's epoch only
// after Recv returns, so a pooler whose host has vanished -- power loss, a
// partition -- leaves a half-open connection that Recv waits on for the
// kernel's retransmit timeout, minutes during which the reconnect window has
// not started counting and the consumer sees a healthy idle stream.
var (
	Keepalive = keepalive.ClientParameters{
		Time:                20 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}
	KeepaliveEnforcement = keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}
)

// ServerOptions and DialOptions are the two halves of the router-to-pooler
// transport, together in one place because several of them only work in
// pairs: the keepalive enforcement answers the client's pings, and a buffer
// or message cap raised on one side alone still meets the other side's.
//
// Credentials are the caller's -- they differ between a pooler serving
// mTLS and a test dialing loopback -- and everything else is fixed here so
// that the settings are one decision rather than two that drift.
func ServerOptions(creds ...grpc.ServerOption) []grpc.ServerOption {
	// Full slice expression: a caller's slice with spare capacity would
	// otherwise have this appended into its own array, so a second call
	// would rewrite the first result's tail.
	return append(creds[:len(creds):len(creds)],
		grpc.KeepaliveEnforcementPolicy(KeepaliveEnforcement),
		grpc.ReadBufferSize(TransportBufferBytes), grpc.WriteBufferSize(TransportBufferBytes),
		grpc.MaxRecvMsgSize(MaxMessageBytes), grpc.MaxSendMsgSize(MaxMessageBytes))
}

// DialOptions is the client half. See ServerOptions.
func DialOptions(creds ...grpc.DialOption) []grpc.DialOption {
	// See ServerOptions on the full slice expression.
	return append(creds[:len(creds):len(creds)],
		grpc.WithKeepaliveParams(Keepalive),
		grpc.WithReadBufferSize(TransportBufferBytes), grpc.WithWriteBufferSize(TransportBufferBytes),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageBytes), grpc.MaxCallSendMsgSize(MaxMessageBytes)))
}
