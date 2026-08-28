package pgwire

import (
	"bytes"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// countingConn records what actually reached the client.
type countingConn struct {
	writes int
	bytes  int
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.writes++
	c.bytes += len(p)
	return len(p), nil
}

func newTestWriter(out *countingConn) *resultWriter {
	return &resultWriter{s: &session{be: pgproto3.NewBackend(bytes.NewReader(nil), out)}}
}

// TestResultsFlushOnBytesNotRowCount: rows used to reach the client every
// 256 of them, which says nothing about how much was queued. A megabyte
// per row meant a quarter of a gigabyte held in the backend buffer, and
// the client waited for the 256th row to see the first.
func TestResultsFlushOnBytesNotRowCount(t *testing.T) {
	var out countingConn
	w := newTestWriter(&out)
	wide := [][]byte{make([]byte, 100<<10)}
	for range 4 {
		if err := w.DataRow(wide); err != nil {
			t.Fatal(err)
		}
	}
	if out.writes == 0 {
		t.Fatal("four 100KiB rows reached the client only when the statement ended")
	}
	if w.s.queued >= flushEveryBytes {
		t.Errorf("%d bytes still queued, more than the %d-byte bound", w.s.queued, flushEveryBytes)
	}

	// Narrow rows must not pay for a write each: the bound is what is
	// owed, not how many rows it took to owe it.
	var narrow countingConn
	w = newTestWriter(&narrow)
	row := [][]byte{[]byte("42")}
	const rows = 300
	for range rows {
		if err := w.DataRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if narrow.writes != 0 {
		t.Errorf("%d rows of two bytes cost %d writes; %d bytes were queued", rows, narrow.writes, rows*dataRowBytes(row))
	}
}

// TestCopyOutAndRowsShareTheFlushBound: a COPY used to count its own bytes
// against its own limit, so a result that mixed the two could hold both.
func TestCopyOutAndRowsShareTheFlushBound(t *testing.T) {
	var out countingConn
	w := newTestWriter(&out)
	if err := w.DataRow([][]byte{make([]byte, 40<<10)}); err != nil {
		t.Fatal(err)
	}
	if out.writes != 0 {
		t.Fatalf("40KiB flushed early: %d writes", out.writes)
	}
	if err := w.CopyData(make([]byte, 40<<10)); err != nil {
		t.Fatal(err)
	}
	if out.writes == 0 {
		t.Error("80KiB across a row and a COPY chunk never reached the client")
	}
}
