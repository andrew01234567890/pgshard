package pooler

import (
	"time"

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
