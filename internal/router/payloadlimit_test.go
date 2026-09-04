package router

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/pooler"
)

// TestATooLargeMessageIsNotALostConnection: grpc-go's default 4 MiB
// receive limit is what pgshard has always enforced between router and
// pooler, silently, because nothing set it. A client that hit it was told
// the connection was lost -- untrue, and unactionable: reconnecting
// changes nothing about a value that is too big.
func TestATooLargeMessageIsNotALostConnection(t *testing.T) {
	tooBig := status.Errorf(codes.ResourceExhausted,
		"grpc: received message larger than max (%d vs. %d)", pooler.MaxMessageBytes+1, pooler.MaxMessageBytes)
	var pe *pgwire.Error
	if !errors.As(poolerTransportError("shard default/0", tooBig), &pe) {
		t.Fatal("not a protocol error")
	}
	if pe.Code != codeTooLarge {
		t.Fatalf("code %s, want %s (program_limit_exceeded)", pe.Code, codeTooLarge)
	}
	if pe.Detail == "" || pe.Hint == "" {
		t.Fatal("a size limit has to say it is pgshard's and what to do about it")
	}

	// Everything else still reads as the transport fault it is.
	for _, err := range []error{
		status.Error(codes.Unavailable, "connection refused"),
		status.Error(codes.Canceled, "context canceled"),
		errors.New("read: connection reset by peer"),
	} {
		if !errors.As(poolerTransportError("shard default/0", err), &pe) {
			t.Fatalf("not a protocol error: %v", err)
		}
		if pe.Code != codeConnectionFailure {
			t.Fatalf("%v gave %s, want %s", err, pe.Code, codeConnectionFailure)
		}
	}
}

// TestTheLimitIsTheOneBothSidesEnforce: the router's call options and the
// pooler's server options have to name the same number, or one side
// accepts what the other refuses.
func TestTheLimitIsTheOneBothSidesEnforce(t *testing.T) {
	// 4 MiB is grpc-go's default, which is what makes this a naming of
	// existing behaviour rather than a change to it. Moving it is now a
	// judgement about the one message decoded whole, not a thing blocked
	// on byte-weighted bounds: those exist.
	if pooler.MaxMessageBytes != 4<<20 {
		t.Fatalf("MaxMessageBytes = %d; the boundary tests and the documented contract move with it", pooler.MaxMessageBytes)
	}
}

// TestEveryPathReportsTheSizeLimitTheSameWay: the scatter path, the
// transaction path and the session path each turn a gRPC failure into a
// client error, and a size limit reported as a lost connection on any of
// them tells a client to reconnect over a value that will be exactly as
// large next time.
func TestEveryPathReportsTheSizeLimitTheSameWay(t *testing.T) {
	for _, direction := range []string{
		"grpc: received message larger than max (5000000 vs. 4194304)",
		"trying to send message larger than max (5000000 vs. 4194304)",
	} {
		err := status.Error(codes.ResourceExhausted, direction)
		var pe *pgwire.Error
		if !errors.As(poolerTransportError("shard default/0", err), &pe) || pe.Code != codeTooLarge {
			t.Fatalf("scatter and transaction paths: %v", err)
		}
		if pe := tooLargeError("pooler stream", err); pe == nil || pe.Code != codeTooLarge {
			t.Fatalf("session path: %v", err)
		}
	}
	if tooLargeError("x", status.Error(codes.Unavailable, "connection refused")) != nil {
		t.Fatal("a lost connection was reported as a size limit")
	}
}
