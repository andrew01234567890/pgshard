package vstream

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// Naming the shard must not cost the refusal its details. Rebuilding the
// status from its code and message alone -- which is what
// status.Errorf(status.Code(err), ...) does -- drops them, and the details
// are where the pooler says the refusal was a stale generation. Without
// them a consumer sees FailedPrecondition, which this RPC also returns for
// a stream open on another router, and cannot tell the two apart.
func TestNamingTheShardKeepsTheRefusalWhole(t *testing.T) {
	sh := router.Shard{Set: "default", ID: 3}
	refusal, err := status.New(codes.FailedPrecondition, "stale routing generation").WithDetails(
		&pgshardv1.Error{Sqlstate: "55000", Message: "stale routing generation", Reason: pgshardv1.Reason_REASON_STALE_GENERATION})
	if err != nil {
		t.Fatal(err)
	}

	got := shardAckErr(sh, refusal.Err())
	st := status.Convert(got)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code = %v, want the shard's own", st.Code())
	}
	if !strings.Contains(st.Message(), "shard default/3") {
		t.Fatalf("message = %q, want the shard named", st.Message())
	}
	if strings.Count(st.Message(), "rpc error") > 0 {
		t.Fatalf("message = %q, want the shard's message rather than its rendered error", st.Message())
	}
	var reason pgshardv1.Reason
	for _, d := range st.Details() {
		if e, ok := d.(*pgshardv1.Error); ok {
			reason = e.GetReason()
		}
	}
	if reason != pgshardv1.Reason_REASON_STALE_GENERATION {
		t.Fatalf("reason = %v, want STALE_GENERATION to survive", reason)
	}
}

// An error that is not a status at all still has to come back as one that
// names the shard.
func TestNamingTheShardOnAPlainError(t *testing.T) {
	got := shardAckErr(router.Shard{Set: "default", ID: 0}, errors.New("no client for shard"))
	st := status.Convert(got)
	if !strings.Contains(st.Message(), "shard default/0") || !strings.Contains(st.Message(), "no client") {
		t.Fatalf("message = %q", st.Message())
	}
}
