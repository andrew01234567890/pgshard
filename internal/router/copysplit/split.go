// Package copysplit divides a COPY ... FROM STDIN stream into rows and
// reads one column out of each, so the router can send every row to the
// shard its key belongs to.
package copysplit

import (
	"bytes"
	"errors"
)

// ErrShortRow is a row with fewer columns than the COPY names.
var ErrShortRow = errors.New("copy row has fewer columns than the statement names")

// Splitter accumulates CopyData chunks, which arrive on no particular
// boundary, and yields whole rows.
//
// PostgreSQL's COPY text format is line-oriented: rows end at a newline and
// columns are separated by a tab. A newline or tab INSIDE a value is never
// sent raw -- COPY writes them as the two characters \n and \t -- so
// splitting on the raw bytes is what PostgreSQL itself does, and a value
// can never contain one to be cut in half by.
//
// The escapes still matter for the KEY, which is hashed: the two characters
// \t have to become a tab before the value is hashed, or the row goes to
// the shard that holds a different string.
type Splitter struct {
	buf    []byte
	keyCol int
	cols   int
}

// New returns a Splitter that reads column keyCol (zero-based) of each row,
// out of cols columns.
func New(keyCol, cols int) *Splitter {
	return &Splitter{keyCol: keyCol, cols: cols}
}

// Write adds a chunk of the stream.
func (s *Splitter) Write(p []byte) { s.buf = append(s.buf, p...) }

// Row is one complete row of the stream: its bytes, including the trailing
// newline, and the text of its shard-key column.
type Row struct {
	Bytes []byte
	Key   string
	// KeyIsNull is set when the key column held the NULL marker \N, which
	// is not the same as the two-character string "\N".
	KeyIsNull bool
}

// Next returns the next complete row, or ok=false when the buffer holds
// only part of one.
func (s *Splitter) Next() (Row, bool, error) {
	end := rowEnd(s.buf)
	if end < 0 {
		return Row{}, false, nil
	}
	row := s.buf[:end]
	s.buf = s.buf[end:]
	// The end-of-data marker is a line of its own and carries no columns.
	if bytes.Equal(bytes.TrimRight(row, "\r\n"), []byte(`\.`)) {
		return Row{Bytes: row}, true, nil
	}
	key, null, err := column(row, s.keyCol, s.cols)
	if err != nil {
		return Row{}, false, err
	}
	return Row{Bytes: row, Key: key, KeyIsNull: null}, true, nil
}

// Rest is whatever is left when the stream ends: a final row without its
// newline, or nothing.
func (s *Splitter) Rest() []byte { return s.buf }

// rowEnd is the index just past the newline that ends the first row, or -1
// when the buffer does not hold a whole one.
//
// No backslash tracking: a raw newline inside a value cannot occur in this
// format, because COPY writes one as the two characters \n. PostgreSQL's
// own reader ends the row at the raw byte too, so tracking escapes here
// would differ from it on malformed input and agree with it on nothing.
func rowEnd(b []byte) int {
	if i := bytes.IndexByte(b, '\n'); i >= 0 {
		return i + 1
	}
	return -1
}

// column returns the text of column n, and whether it was the NULL marker.
func column(row []byte, n, cols int) (string, bool, error) {
	fields := bytes.Split(bytes.TrimRight(row, "\r\n"), []byte{'\t'})
	if n >= len(fields) || len(fields) < cols {
		return "", false, ErrShortRow
	}
	raw := fields[n]
	if bytes.Equal(raw, []byte(`\N`)) {
		return "", true, nil
	}
	return unescape(raw), false, nil
}

// unescape resolves the backslash escapes COPY text format defines, so the
// value the router hashes is the value the shard will store.
func unescape(b []byte) string {
	if bytes.IndexByte(b, '\\') < 0 {
		return string(b)
	}
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' || i+1 >= len(b) {
			out = append(out, b[i])
			continue
		}
		i++
		switch b[i] {
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case 'v':
			out = append(out, '\v')
		default:
			out = append(out, b[i])
		}
	}
	return string(out)
}
