package pooler

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestDataRowPacksOnlyWhenAsked: the packed shape carries the same
// columns without an object per column, but it can only tell an empty
// value from a NULL because it names the NULL columns separately. A
// pooler must not pack unless the router asked, or a router that predates
// the shape reads a row with no columns at all.
func TestDataRowPacksOnlyWhenAsked(t *testing.T) {
	values := [][]byte{[]byte("a"), nil, {}}

	legacy := dataRow(values, false)
	if len(legacy.Packed) != 0 || len(legacy.Nulls) != 0 {
		t.Errorf("unasked row packed: %+v", legacy)
	}
	if len(legacy.Columns) != 3 || !legacy.Columns[1].Null || string(legacy.Columns[0].Data) != "a" {
		t.Errorf("columns = %+v", legacy.Columns)
	}
	if legacy.Columns[2].Null || len(legacy.Columns[2].Data) != 0 {
		t.Errorf("an empty value became NULL: %+v", legacy.Columns[2])
	}

	packed := dataRow(values, true)
	if len(packed.Columns) != 0 {
		t.Errorf("packed row also sent submessages: %+v", packed.Columns)
	}
	if len(packed.Packed) != 3 || string(packed.Packed[0]) != "a" {
		t.Errorf("packed = %+v", packed.Packed)
	}
	if len(packed.Nulls) != 1 || packed.Nulls[0] != 1 {
		t.Errorf("nulls = %v, want just the NULL column", packed.Nulls)
	}
	if packed.Packed[2] == nil {
		t.Error("the empty value must stay an empty value, not become the NULL it is not")
	}
}

// TestARelayedBackendErrorSaysItIsTheBackends.
//
// The pooler uses 55000 for every fencing refusal, and PostgreSQL answers
// 55000 for the placement-fence trigger and for conditions that are not a
// fence at all. The router reads the SQLSTATE, so an unmarked backend error
// was taken for a topology change: inside a transaction that becomes 40001
// "retry the transaction", and the client loops on something retrying cannot
// fix.
//
// The pooler is the only party that knows which it is, because it either
// refused the request or relayed an answer.
func TestARelayedBackendErrorSaysItIsTheBackends(t *testing.T) {
	r := toResponse(&pgproto3.ErrorResponse{
		Code: "55000", Message: "table public.orders is being moved; writes are paused",
		Hint: "retry once the placement change is published"}, false)
	got := r.GetError().GetError()
	if got == nil {
		t.Fatalf("no error in %+v", r)
	}
	if got.GetReason() != pgshardv1.Reason_REASON_BACKEND_ERROR {
		t.Fatalf("reason = %v, want REASON_BACKEND_ERROR; without it the router reads the 55000 as a fence", got.GetReason())
	}
	if got.GetSqlstate() != "55000" || got.GetHint() != "retry once the placement change is published" {
		t.Fatalf("the error itself must be relayed unchanged: %+v", got)
	}
}
