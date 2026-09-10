package pgwire

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestBindCompletePrecedesTheRowsItBound.
//
// Rows are encoded into a slab of their own and everything else into the
// backend's buffer, and the order between the two is kept by each side
// flushing the other before it writes. dispatch wrote BindComplete straight
// to the backend instead, so the buffer held a message it did not know
// about: the rows were written first and the client saw DataRow before
// BindComplete.
//
// lib/pq reads exactly one message after Bind and fails the connection on
// anything but BindComplete -- so every driver on lib/pq, pgroll included,
// died with "unexpected Bind response 'D'" on the first parameterised
// query it sent without a Describe. pgx survives only by accident: its
// Describe between Bind and Execute goes through the ordered path and
// flushes the buffer on the way.
func TestBindCompletePrecedesTheRowsItBound(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)

	// lib/pq's sequence: the statement is described on its own, then bound
	// and executed with NO describe in the batch.
	c.send(&pgproto3.Parse{Query: "select 1"}, &pgproto3.Describe{ObjectType: 'S'}, &pgproto3.Sync{})
	for {
		if _, done := c.recv().(*pgproto3.ReadyForQuery); done {
			break
		}
	}

	c.send(&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	var got []string
	for {
		m := c.recv()
		got = append(got, msgKind(m))
		if _, done := m.(*pgproto3.ReadyForQuery); done {
			break
		}
	}
	// Execute sends no RowDescription of its own; the statement was
	// described before it was bound.
	want := []string{"BindComplete", "DataRow", "CommandComplete", "ReadyForQuery"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func msgKind(m pgproto3.BackendMessage) string {
	switch m.(type) {
	case *pgproto3.BindComplete:
		return "BindComplete"
	case *pgproto3.RowDescription:
		return "RowDescription"
	case *pgproto3.DataRow:
		return "DataRow"
	case *pgproto3.CommandComplete:
		return "CommandComplete"
	case *pgproto3.ReadyForQuery:
		return "ReadyForQuery"
	case *pgproto3.ErrorResponse:
		return "ErrorResponse"
	}
	return "other"
}
