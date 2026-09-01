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
	// existing behaviour rather than a change to it. If this ever moves,
	// the byte-weighted bounds have to move first.
	if pooler.MaxMessageBytes != 4<<20 {
		t.Fatalf("MaxMessageBytes = %d; raising it needs byte-weighted admission first (PGS-499)", pooler.MaxMessageBytes)
	}
}
