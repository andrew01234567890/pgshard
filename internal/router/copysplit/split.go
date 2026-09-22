// Package copysplit divides a COPY ... FROM STDIN stream into rows and
// reads one column out of each, so the router can send every row to the
// shard its key belongs to.
package copysplit

import (
	"bytes"
	"errors"
	"strings"
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
	// eol is the line ending the stream uses, decided by its first row as
	// PostgreSQL decides it, and ended says no more data will come.
	eol   eol
	ended bool
}

// eol is a COPY stream's line ending: PostgreSQL takes it from the first
// line (copyfromparse.c, CopyReadLineText) and refuses a stream that mixes
// them.
type eol int

const (
	eolUnknown eol = iota
	eolNL
	eolCR
	eolCRNL
)

// ErrMixedLineEndings is a raw newline or carriage return that does not
// match the stream's line ending, which PostgreSQL refuses as literal data.
var ErrMixedLineEndings = errors.New("copy row has a literal carriage return or newline that does not match the stream's line ending; write it as \\r or \\n")

// New returns a Splitter that reads column keyCol (zero-based) of each row,
// out of cols columns.
func New(keyCol, cols int) *Splitter {
	return &Splitter{keyCol: keyCol, cols: cols}
}

// Write adds a chunk of the stream.
func (s *Splitter) Write(p []byte) { s.buf = append(s.buf, p...) }

// End says the stream is over, so a carriage return at the very end ends a
// row and whatever follows the last line ending is the last row.
func (s *Splitter) End() { s.ended = true }

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
	end, err := s.rowEnd()
	if err != nil {
		return Row{}, false, err
	}
	if end < 0 {
		if !s.ended || len(s.buf) == 0 {
			return Row{}, false, nil
		}
		// The last row need not end in a line ending, to PostgreSQL either.
		end = len(s.buf)
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

// rowEnd is the index just past the line ending that ends the first row,
// or -1 when the buffer does not hold a whole one.
//
// A backslash escapes the byte after it, a newline included: PostgreSQL's
// reader (copyfromparse.c, CopyReadLineText) steps over the byte that
// follows a backslash before it looks for the end of the line, so a
// backslash-newline is part of a value and not the end of a row. A buffer
// ending in a backslash does not hold a whole row yet.
//
// The line ending is the first row's -- \n, \r or \r\n -- and a raw one
// of another kind later is refused, as PostgreSQL refuses it.
func (s *Splitter) rowEnd() (int, error) {
	b := s.buf
	for i := 0; i < len(b); i++ {
		switch b[i] {
		case '\\':
			i++
		case '\n':
			if s.eol == eolUnknown {
				s.eol = eolNL
			}
			if s.eol != eolNL {
				return 0, ErrMixedLineEndings
			}
			return i + 1, nil
		case '\r':
			switch s.eol {
			case eolNL:
				return 0, ErrMixedLineEndings
			case eolCR:
				return i + 1, nil
			}
			if i+1 >= len(b) {
				// Whether a \n follows is in the next chunk.
				if !s.ended {
					return -1, nil
				}
				if s.eol == eolUnknown {
					s.eol = eolCR
				}
				return i + 1, nil
			}
			if b[i+1] == '\n' {
				if s.eol == eolUnknown {
					s.eol = eolCRNL
				}
				return i + 2, nil
			}
			if s.eol == eolCRNL {
				return 0, ErrMixedLineEndings
			}
			s.eol = eolCR
			return i + 1, nil
		}
	}
	return -1, nil
}

// ErrLongRow is a row with more columns than the COPY names.
var ErrLongRow = errors.New("copy row has more columns than the statement names")

// ErrNulInKey is a key whose escapes produce a zero byte, which PostgreSQL
// refuses in text.
var ErrNulInKey = errors.New("copy row's shard key contains a zero byte")

// column returns the text of column n, and whether it was the NULL marker.
//
// Columns end at a tab that no backslash escapes, as in PostgreSQL's
// CopyReadAttributesText, and the row has to have exactly cols of them:
// PostgreSQL refuses a row with more or fewer, and so does the router,
// before sending it anywhere.
func column(row []byte, n, cols int) (string, bool, error) {
	line := bytes.TrimSuffix(row, []byte{'\n'})
	if l := len(line); l > 0 && line[l-1] == '\r' && !escaped(line, l-1) {
		line = line[:l-1]
	}
	var fields [][]byte
	start := 0
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case '\\':
			i++
		case '\t':
			fields = append(fields, line[start:i])
			start = i + 1
		}
	}
	fields = append(fields, line[start:])
	switch {
	case len(fields) < cols:
		return "", false, ErrShortRow
	case len(fields) > cols:
		return "", false, ErrLongRow
	}
	raw := fields[n]
	if bytes.Equal(raw, []byte(`\N`)) {
		return "", true, nil
	}
	key := unescape(raw)
	if strings.IndexByte(key, 0) >= 0 {
		return "", false, ErrNulInKey
	}
	return key, false, nil
}

// escaped reports that the byte at i is preceded by an odd run of
// backslashes, which makes it part of an escape.
func escaped(b []byte, i int) bool {
	n := 0
	for j := i - 1; j >= 0 && b[j] == '\\'; j-- {
		n++
	}
	return n%2 == 1
}

// unescape resolves the backslash escapes COPY text format defines, so the
// value the router hashes is the value the shard will store: the named
// ones, octal \NNN and hex \xHH as PostgreSQL reads them (one to three
// octal digits, one or two hex digits), and any other escaped byte as
// itself.
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
		switch c := b[i]; {
		case c >= '0' && c <= '7':
			v := int(c - '0')
			for k := 0; k < 2 && i+1 < len(b) && b[i+1] >= '0' && b[i+1] <= '7'; k++ {
				i++
				v = v<<3 + int(b[i]-'0')
			}
			out = append(out, byte(v&0377))
		case c == 'x' && i+1 < len(b) && isHex(b[i+1]):
			i++
			v := hexVal(b[i])
			if i+1 < len(b) && isHex(b[i+1]) {
				i++
				v = v<<4 + hexVal(b[i])
			}
			out = append(out, byte(v))
		case c == 'b':
			out = append(out, '\b')
		case c == 'f':
			out = append(out, '\f')
		case c == 'n':
			out = append(out, '\n')
		case c == 'r':
			out = append(out, '\r')
		case c == 't':
			out = append(out, '\t')
		case c == 'v':
			out = append(out, '\v')
		default:
			out = append(out, c)
		}
	}
	return string(out)
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func hexVal(c byte) int {
	switch {
	case c >= 'a':
		return int(c-'a') + 10
	case c >= 'A':
		return int(c-'A') + 10
	}
	return int(c - '0')
}
