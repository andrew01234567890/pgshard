package copysplit

import (
	"strings"
	"testing"
)

// collect drives the splitter with the chunking a caller chooses, which is
// the thing most likely to break it: CopyData arrives on no particular
// boundary, so a row can be split across any number of chunks.
func collect(t *testing.T, keyCol, cols int, chunks ...string) []Row {
	t.Helper()
	s := New(keyCol, cols)
	var out []Row
	for _, c := range chunks {
		s.Write([]byte(c))
		for {
			r, ok, err := s.Next()
			if err != nil {
				t.Fatalf("split: %v", err)
			}
			if !ok {
				break
			}
			out = append(out, r)
		}
	}
	return out
}

func TestSplitsRowsAndReadsTheKey(t *testing.T) {
	rows := collect(t, 0, 3, "1\ta\tx\n2\tb\ty\n")
	if len(rows) != 2 {
		t.Fatalf("%d rows", len(rows))
	}
	if rows[0].Key != "1" || rows[1].Key != "2" {
		t.Fatalf("keys %q %q", rows[0].Key, rows[1].Key)
	}
	if string(rows[0].Bytes) != "1\ta\tx\n" {
		t.Fatalf("row bytes %q", rows[0].Bytes)
	}
}

// The chunking is the caller's, not the format's.
func TestARowSplitAcrossChunks(t *testing.T) {
	rows := collect(t, 1, 3, "1\t", "42", "\tx\n1", "0\t7\ty\n")
	if len(rows) != 2 {
		t.Fatalf("%d rows: %v", len(rows), rows)
	}
	if rows[0].Key != "42" || rows[1].Key != "7" {
		t.Fatalf("keys %q %q", rows[0].Key, rows[1].Key)
	}
}

// A newline inside a value is never sent raw: COPY writes it as the two
// characters \n, which is not a row end. This is what makes splitting on
// the raw byte safe rather than a shortcut.
func TestAnEscapedNewlineDoesNotEndTheRow(t *testing.T) {
	rows := collect(t, 0, 2, `7	line one\nline two`+"\n")
	if len(rows) != 1 {
		t.Fatalf("%d rows, want 1: %+v", len(rows), rows)
	}
	if rows[0].Key != "7" {
		t.Fatalf("key %q", rows[0].Key)
	}
}

// The same for a tab: \t in a value is two characters, so the column does
// not end there and the key is the whole value, with the escape resolved.
func TestAnEscapedTabDoesNotEndTheColumn(t *testing.T) {
	rows := collect(t, 0, 2, `a\tb	second`+"\n")
	if len(rows) != 1 || rows[0].Key != "a\tb" {
		t.Fatalf("rows %v", rows)
	}
}

// The key is hashed, so it has to be the value the shard will store: the
// escapes are resolved rather than passed through.
func TestTheKeyIsUnescaped(t *testing.T) {
	rows := collect(t, 0, 2, `a\\b	x`+"\n")
	if rows[0].Key != `a\b` {
		t.Fatalf("key %q, want a backslash", rows[0].Key)
	}
}

// \N is NULL, and is not the two-character string "\N" -- a row whose key
// is NULL cannot be routed and the caller has to say so.
func TestTheNullMarkerIsDistinguished(t *testing.T) {
	rows := collect(t, 0, 2, "\\N\tx\n"+`\\N	y`+"\n")
	if !rows[0].KeyIsNull {
		t.Errorf("row 0 key %q was not reported NULL", rows[0].Key)
	}
	if rows[1].KeyIsNull || rows[1].Key != `\N` {
		t.Errorf("row 1 key %q null=%v, want the literal string", rows[1].Key, rows[1].KeyIsNull)
	}
}

// The key is not always the first column.
func TestAKeyInTheMiddleAndAtTheEnd(t *testing.T) {
	if rows := collect(t, 1, 3, "a\tKEY\tc\n"); rows[0].Key != "KEY" {
		t.Errorf("middle: %q", rows[0].Key)
	}
	if rows := collect(t, 2, 3, "a\tb\tKEY\n"); rows[0].Key != "KEY" {
		t.Errorf("last: %q", rows[0].Key)
	}
}

// A row with fewer columns than the statement names is an error rather than
// a row routed on whatever happened to be in that position.
func TestAShortRowIsAnError(t *testing.T) {
	// The key column itself is missing.
	s := New(2, 3)
	s.Write([]byte("a\tb\n"))
	if _, _, err := s.Next(); err != ErrShortRow {
		t.Fatalf("missing key column: err %v, want ErrShortRow", err)
	}
	// And the case the first check does not catch: the key is there, but
	// the row is still short. Sending it would have the shard reject the
	// whole COPY after the router had already fanned other rows out.
	s = New(0, 3)
	s.Write([]byte("a\tb\n"))
	if _, _, err := s.Next(); err != ErrShortRow {
		t.Fatalf("short row with a readable key: err %v, want ErrShortRow", err)
	}
}

// The end-of-data marker is a line of its own and carries no columns.
func TestTheEndOfDataMarkerIsNotARow(t *testing.T) {
	s := New(0, 2)
	s.Write([]byte("1\tx\n\\.\n"))
	if _, ok, err := s.Next(); !ok || err != nil {
		t.Fatalf("first row: %v %v", ok, err)
	}
	r, ok, err := s.Next()
	if !ok || err != nil {
		t.Fatalf("marker: %v %v", ok, err)
	}
	if r.Key != "" || !strings.Contains(string(r.Bytes), `\.`) {
		t.Fatalf("marker read as a row: %+v", r)
	}
}

// A trailing row with no newline is left in the buffer for the caller.
func TestAnUnterminatedRowIsLeftForTheCaller(t *testing.T) {
	s := New(0, 2)
	s.Write([]byte("1\tx\n2\ty"))
	if _, ok, _ := s.Next(); !ok {
		t.Fatal("the first row is complete")
	}
	if _, ok, _ := s.Next(); ok {
		t.Fatal("the second row has no newline and is not complete")
	}
	if string(s.Rest()) != "2\ty" {
		t.Fatalf("rest %q", s.Rest())
	}
}
