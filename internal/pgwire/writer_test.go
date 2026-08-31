package pgwire

import (
	"bytes"
	"net"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// countingConn records what actually reached the client. Rows are written
// to the connection rather than through the pgproto3 backend, so a test
// writer needs one; net.Conn is embedded for the methods nothing calls.
type countingConn struct {
	net.Conn
	writes int
	bytes  int
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.writes++
	c.bytes += len(p)
	return len(p), nil
}

func newTestWriter(out *countingConn) *resultWriter {
	return &resultWriter{s: &session{conn: out, be: pgproto3.NewBackend(bytes.NewReader(nil), out)}}
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

// TestASessionDoesNotKeepAnOversizedRowSlab: rows are encoded into a slab
// the session keeps, so it does not pass through pgproto3's write buffer --
// which is thrown away above a kilobyte on every flush. Keeping the slab is
// the point; keeping an arbitrarily large one would trade a copy for
// resident memory on every session that ever returned a wide result.
func TestASessionDoesNotKeepAnOversizedRowSlab(t *testing.T) {
	var out countingConn
	w := newTestWriter(&out)

	// One row far past the bound, then the flush that ends a result.
	if err := w.DataRow([][]byte{make([]byte, 4*maxRowSlab)}); err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	if c := cap(w.s.rows); c > maxRowSlab {
		t.Errorf("the session kept a %d-byte slab after a large result; the bound is %d", c, maxRowSlab)
	}

	// A slab within the bound is kept, which is what makes the next result
	// cheaper than the last.
	small := [][]byte{make([]byte, 512)}
	for range 4 {
		if err := w.DataRow(small); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	if cap(w.s.rows) == 0 {
		t.Error("a slab within the bound must be kept for the next result")
	}
}

// TestRowsAndOtherMessagesKeepTheirOrder: rows go to the connection and
// everything else goes through pgproto3, so only one of the two may hold
// pending bytes at a time. Getting that wrong reorders the stream -- a
// CommandComplete ahead of the rows it completes -- which no test of either
// buffer alone would catch.
func TestRowsAndOtherMessagesKeepTheirOrder(t *testing.T) {
	out := &orderedConn{}
	w := &resultWriter{s: &session{conn: out, be: pgproto3.NewBackend(bytes.NewReader(nil), out)}}
	if err := w.RowDescription([]pgproto3.FieldDescription{{Name: []byte("c")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.DataRow([][]byte{[]byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := w.CommandComplete("SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	got := out.buf.String()
	iRow := strings.IndexByte(got, 'D')
	iDesc := strings.IndexByte(got, 'T')
	iDone := strings.IndexByte(got, 'C')
	if iDesc < 0 || iRow < 0 || iDone < 0 || iDesc >= iRow || iRow >= iDone {
		t.Fatalf("wire order T=%d D=%d C=%d in %q", iDesc, iRow, iDone, got)
	}
}

// orderedConn keeps what was written, in the order it was written, from
// both the row slab and the pgproto3 backend.
type orderedConn struct {
	net.Conn
	buf bytes.Buffer
}

func (c *orderedConn) Write(p []byte) (int, error) { return c.buf.Write(p) }
