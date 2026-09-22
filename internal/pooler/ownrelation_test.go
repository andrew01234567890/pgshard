package pooler

import (
	"encoding/binary"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgoutput"
	"github.com/andrew01234567890/pgshard/internal/schemacopy"
)

// TestAStreamDoesNotCarryTheOwnedRange (PGS-878): each shard records the
// range it owns in a table of its own, and a change stream decodes a FOR ALL
// TABLES publication that cannot leave it out. Its changes reached every
// consumer as rows of a table the consumer never created, and one reading
// each row as its own failed on the shard set's name where it expected a
// number. They are dropped at the source; a user table's are not.
func TestAStreamDoesNotCarryTheOwnedRange(t *testing.T) {
	d := pgoutput.NewDecoder()
	relation := func(id uint32, namespace, name string) *pgoutput.Relation {
		t.Helper()
		raw := []byte{'R'}
		raw = binary.BigEndian.AppendUint32(raw, id)
		raw = append(raw, namespace...)
		raw = append(raw, 0)
		raw = append(raw, name...)
		raw = append(raw, 0, 'd', 0, 1, 1, 'l', 'o', 0, 0, 0, 0, 20, 0xff, 0xff, 0xff, 0xff)
		m, err := d.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		return m.(*pgoutput.Relation)
	}
	row := pgoutput.Tuple{Columns: []pgoutput.TupleColumn{{Kind: pgoutput.ColumnText, Data: []byte("1")}}}

	own := relation(41, schemacopy.OwnerSchema, "owned_range")
	if ev, _, err := convert(d, own, 1); err != nil || ev != nil {
		t.Fatalf("the owned range's relation reached the stream: %v %v", ev, err)
	}
	for _, m := range []pgoutput.Message{
		&pgoutput.Insert{RelationID: 41, New: row},
		&pgoutput.Update{RelationID: 41, New: row, Old: &row},
		&pgoutput.Delete{RelationID: 41, Old: &row},
	} {
		if ev, _, err := convert(d, m, 1); err != nil || ev != nil {
			t.Fatalf("a %T of the owned range reached the stream: %v %v", m, ev, err)
		}
	}

	users := relation(42, "public", "orders")
	if ev, _, err := convert(d, users, 1); err != nil || ev == nil {
		t.Fatalf("a user table's relation was dropped: %v %v", ev, err)
	}
	if ev, _, err := convert(d, &pgoutput.Insert{RelationID: 42, New: row}, 1); err != nil || ev == nil || ev.GetRow() == nil {
		t.Fatalf("a user table's row was dropped: %v %v", ev, err)
	}
}
