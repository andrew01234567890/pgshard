package pooler

import (
	"fmt"
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"google.golang.org/grpc"
)

type sentStream struct {
	grpc.ServerStream
	sent []*pgshardv1.ExecuteResponse
}

func (s *sentStream) Send(m *pgshardv1.ExecuteResponse) error {
	s.sent = append(s.sent, m)
	return nil
}
func (s *sentStream) Recv() (*pgshardv1.ExecuteRequest, error) { panic("unused") }

func batchRelay(t *testing.T) (*relay, *sentStream) {
	t.Helper()
	st := &sentStream{}
	return &relay{se: &session{id: "s1"}, stream: st, batched: true}, st
}

func row(width int) *pgshardv1.ExecuteResponse {
	return &pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_DataRow{
		DataRow: &pgshardv1.DataRow{Packed: [][]byte{make([]byte, width)}}}}
}

func complete() *pgshardv1.ExecuteResponse {
	return &pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_CommandComplete{
		CommandComplete: &pgshardv1.CommandComplete{Tag: "SELECT 1"}}}
}

func rowsIn(m *pgshardv1.ExecuteResponse) int {
	switch x := m.GetMessage().(type) {
	case *pgshardv1.ExecuteResponse_DataRow:
		return 1
	case *pgshardv1.ExecuteResponse_DataRows:
		return len(x.DataRows.GetRows())
	}
	return 0
}

func TestNarrowRowsTravelInOneMessage(t *testing.T) {
	r, st := batchRelay(t)
	for range 10 {
		if err := r.send(row(8)); err != nil {
			t.Fatal(err)
		}
	}
	if len(st.sent) != 0 {
		t.Fatalf("rows went out before anything forced them: %d messages", len(st.sent))
	}
	if err := r.send(complete()); err != nil {
		t.Fatal(err)
	}
	if len(st.sent) != 2 {
		t.Fatalf("10 rows and a tag took %d messages, want 2", len(st.sent))
	}
	if got := rowsIn(st.sent[0]); got != 10 {
		t.Fatalf("the batch carried %d rows, want 10", got)
	}
	if _, ok := st.sent[1].GetMessage().(*pgshardv1.ExecuteResponse_CommandComplete); !ok {
		t.Fatal("the tag must follow the rows, not precede them")
	}
}

func TestALoneRowIsNotWrappedInABatch(t *testing.T) {
	r, st := batchRelay(t)
	if err := r.send(row(8)); err != nil {
		t.Fatal(err)
	}
	if err := r.send(complete()); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.sent[0].GetMessage().(*pgshardv1.ExecuteResponse_DataRow); !ok {
		t.Fatalf("a single row was wrapped in a batch: %T", st.sent[0].GetMessage())
	}
}

func TestAWideRowGoesOnItsOwnAndKeepsItsPlace(t *testing.T) {
	r, st := batchRelay(t)
	if err := r.send(row(8)); err != nil {
		t.Fatal(err)
	}
	if err := r.send(row(batchWideRow)); err != nil {
		t.Fatal(err)
	}
	if err := r.send(row(8)); err != nil {
		t.Fatal(err)
	}
	if err := r.send(complete()); err != nil {
		t.Fatal(err)
	}
	if n := len(st.sent); n != 4 {
		t.Fatalf("got %d messages, want 4: the narrow row before, the wide row, the narrow row after, the tag", n)
	}
	if got := rowsIn(st.sent[1]); got != 1 || len(st.sent[1].GetMessage().(*pgshardv1.ExecuteResponse_DataRow).DataRow.Packed[0]) != batchWideRow {
		t.Fatal("the wide row did not go out on its own")
	}
	total := 0
	for _, m := range st.sent {
		total += rowsIn(m)
	}
	if total != 3 {
		t.Fatalf("%d rows reached the client, want 3", total)
	}
}

func TestABatchIsBoundedByBothCountAndBytes(t *testing.T) {
	r, st := batchRelay(t)
	for range batchRowCount + 1 {
		if err := r.send(row(8)); err != nil {
			t.Fatal(err)
		}
	}
	if len(st.sent) != 1 || rowsIn(st.sent[0]) != batchRowCount {
		t.Fatalf("the count bound did not close a batch at %d rows", batchRowCount)
	}

	r, st = batchRelay(t)
	wide := batchWideRow - 1
	for range batchRowBytes/wide + 1 {
		if err := r.send(row(wide)); err != nil {
			t.Fatal(err)
		}
	}
	if len(st.sent) != 1 {
		t.Fatalf("the byte bound did not close a batch: %d messages", len(st.sent))
	}
}

func TestRowsAreNotBatchedUntilTheRouterAsks(t *testing.T) {
	st := &sentStream{}
	r := &relay{se: &session{id: "s1"}, stream: st}
	for range 5 {
		if err := r.send(row(8)); err != nil {
			t.Fatal(err)
		}
	}
	if len(st.sent) != 5 {
		t.Fatalf("a router that never asked got %d messages for 5 rows", len(st.sent))
	}
}

// TestABatchSurvivesTheBufferItWasDecodedFrom is the defect that batching
// introduced and that only a real backend caught: pgproto3 hands out
// column values pointing into its receive buffer and overwrites that
// buffer on the next message. Sending each row as it was decoded stayed
// inside that window. Holding rows for a batch does not, and the rows
// already in hand were overwritten by the rows still arriving -- which
// reached the client as other rows' bytes, not as an error.
func TestABatchSurvivesTheBufferItWasDecodedFrom(t *testing.T) {
	r, st := batchRelay(t)
	shared := []byte("first")
	send := func() {
		row := &pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_DataRow{
			DataRow: &pgshardv1.DataRow{Packed: [][]byte{shared}}}}
		if err := r.send(row); err != nil {
			t.Fatal(err)
		}
	}
	send()
	copy(shared, "SECND") // the next message lands in the same buffer
	send()
	if err := r.send(complete()); err != nil {
		t.Fatal(err)
	}
	rows := st.sent[0].GetMessage().(*pgshardv1.ExecuteResponse_DataRows).DataRows.GetRows()
	if len(rows) != 2 {
		t.Fatalf("batch holds %d rows, want 2", len(rows))
	}
	if got := string(rows[0].Packed[0]); got != "first" {
		t.Fatalf("the first row read %q; the second row overwrote it in place", got)
	}
	if got := string(rows[1].Packed[0]); got != "SECND" {
		t.Fatalf("the second row read %q", got)
	}
}

func TestCopyRowCarriesNullsAndTheUnpackedShape(t *testing.T) {
	src := &pgshardv1.DataRow{Columns: []*pgshardv1.Value{
		{Data: []byte("a")}, {Null: true}, {Data: []byte("")}}, Nulls: []uint32{7}}
	got := copyRow(src)
	if len(got.Columns) != 3 || string(got.Columns[0].Data) != "a" {
		t.Fatalf("columns: %+v", got.Columns)
	}
	if !got.Columns[1].Null || got.Columns[2].Null {
		t.Fatal("a NULL column and an empty one are not the same thing")
	}
	if len(got.Nulls) != 1 || got.Nulls[0] != 7 {
		t.Fatalf("nulls: %v", got.Nulls)
	}
}

func BenchmarkCopyRow(b *testing.B) {
	for _, cols := range []int{1, 16} {
		b.Run(fmt.Sprintf("cols=%d", cols), func(b *testing.B) {
			src := &pgshardv1.DataRow{Packed: make([][]byte, cols)}
			for i := range src.Packed {
				src.Packed[i] = make([]byte, 16)
			}
			b.ReportAllocs()
			for b.Loop() {
				_ = copyRow(src)
			}
		})
	}
}
