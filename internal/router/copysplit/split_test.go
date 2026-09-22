package copysplit

import (
	"errors"
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
	if _, _, err := s.Next(); !errors.Is(err, ErrShortRow) {
		t.Fatalf("missing key column: err %v, want ErrShortRow", err)
	}
	// And the case the first check does not catch: the key is there, but
	// the row is still short. Sending it would have the shard reject the
	// whole COPY after the router had already fanned other rows out.
	s = New(0, 3)
	s.Write([]byte("a\tb\n"))
	if _, _, err := s.Next(); !errors.Is(err, ErrShortRow) {
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

// PostgreSQL reads octal and hex escapes in a value, so the key it stores
// for \101 or \x41 is "A", and the router has to hash "A" as well.
func TestOctalAndHexEscapesAreResolvedAsPostgreSQLResolvesThem(t *testing.T) {
	for raw, want := range map[string]string{
		`\101`:   "A",
		`\x41`:   "A",
		`\x4`:    "\x04",
		`\1011`:  "A1",
		`\x41z`:  "Az",
		`\xz`:    "xz",
		`a\\b`:   `a\b`,
		`\q`:     "q",
		`\7`:     "\x07",
		`\x7f\1`: "\x7f\x01",
	} {
		rows := collect(t, 0, 2, raw+"\tv\n")
		if len(rows) != 1 || rows[0].Key != want {
			t.Fatalf("%q: rows %+v, want key %q", raw, rows, want)
		}
	}
}

// A backslash escapes the byte after it, a raw tab or newline included, as
// in PostgreSQL's reader: neither ends the column or the row.
func TestABackslashEscapesARawTabAndARawNewline(t *testing.T) {
	rows := collect(t, 0, 2, "a\\\tb\tv\n")
	if len(rows) != 1 || rows[0].Key != "a\tb" {
		t.Fatalf("escaped raw tab: %+v", rows)
	}
	rows = collect(t, 0, 2, "a\\\nb\tv\n", "c\td\n")
	if len(rows) != 2 || rows[0].Key != "a\nb" || rows[1].Key != "c" {
		t.Fatalf("escaped raw newline: %+v", rows)
	}
	// A chunk ending in the backslash does not end the row at the newline
	// that starts the next chunk.
	rows = collect(t, 0, 2, "a\\", "\nb\tv\n")
	if len(rows) != 1 || rows[0].Key != "a\nb" {
		t.Fatalf("escape split across chunks: %+v", rows)
	}
}

func TestARowWithTooManyColumnsIsAnError(t *testing.T) {
	s := New(0, 2)
	s.Write([]byte("1\t2\t3\n"))
	if _, _, err := s.Next(); !errors.Is(err, ErrLongRow) {
		t.Fatalf("err = %v, want ErrLongRow", err)
	}
}

func TestAKeyWithAZeroByteIsAnError(t *testing.T) {
	s := New(0, 2)
	s.Write([]byte(`a\000b` + "\tv\n"))
	if _, _, err := s.Next(); !errors.Is(err, ErrNulInKey) {
		t.Fatalf("err = %v, want ErrNulInKey", err)
	}
}

func TestACRLFRowEndsWithoutItsCarriageReturn(t *testing.T) {
	rows := collect(t, 1, 2, "v\tk\r\n")
	if len(rows) != 1 || rows[0].Key != "k" {
		t.Fatalf("%+v", rows)
	}
}

func collectEnded(t *testing.T, keyCol, cols int, data string) ([]Row, error) {
	t.Helper()
	s := New(keyCol, cols)
	s.Write([]byte(data))
	s.End()
	var out []Row
	for {
		r, ok, err := s.Next()
		if err != nil || !ok {
			return out, err
		}
		out = append(out, r)
	}
}

// PostgreSQL takes a stream's line ending from its first row, and \r on
// its own is one of them.
func TestACarriageReturnOnlyStreamSplitsIntoRows(t *testing.T) {
	rows, err := collectEnded(t, 0, 2, "a\t1\rb\t2\rc\t3")
	if err != nil || len(rows) != 3 || rows[0].Key != "a" || rows[1].Key != "b" || rows[2].Key != "c" {
		t.Fatalf("rows %+v err %v", rows, err)
	}
	if string(rows[0].Bytes) != "a\t1\r" {
		t.Fatalf("row bytes %q, want the CR kept for the shard", rows[0].Bytes)
	}
}

// Whether \r is a line ending or the start of \r\n is only known once the
// next byte arrives.
func TestACarriageReturnAtAChunkEndWaitsForTheNextByte(t *testing.T) {
	rows := collect(t, 0, 2, "a\t1\r", "\nb\t2\r\n")
	if len(rows) != 2 || string(rows[0].Bytes) != "a\t1\r\n" || rows[1].Key != "b" {
		t.Fatalf("%+v", rows)
	}
}

func TestMixedLineEndingsAreRefusedAsPostgreSQLRefusesThem(t *testing.T) {
	for _, data := range []string{"a\t1\nb\t2\r\n", "a\t1\r\nb\t2\n", "a\t1\rb\t2\n"} {
		if _, err := collectEnded(t, 0, 2, data); !errors.Is(err, ErrMixedLineEndings) {
			t.Fatalf("%q: err = %v", data, err)
		}
	}
	// Escaped, a carriage return is data in any stream.
	rows, err := collectEnded(t, 0, 2, "a\\\rb\t1\n")
	if err != nil || len(rows) != 1 || rows[0].Key != "a\rb" {
		t.Fatalf("escaped CR: %+v %v", rows, err)
	}
}
