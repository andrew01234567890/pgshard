package controller

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// The pgoutput decoder here is a deliberately reduced protocol version 1
// decoder for the placement applier; internal/pgoutput is the full v4
// decoder used by streams, whose message-per-type API does not fit the
// applier's row-change loop.
// It reads messages as
// pg_logical_slot_peek_binary_changes returns them for a pgoutput slot.
// Only what the placement applier needs is kept: relations, and the text
// tuples of inserts, updates and deletes. Binary tuples are refused (the
// slot is read with binary=off).

// ChangeOp is the kind of a decoded row change.
type ChangeOp byte

// Row change kinds.
const (
	OpInsert ChangeOp = 'I'
	OpUpdate ChangeOp = 'U'
	OpDelete ChangeOp = 'D'
)

// Relation is a pgoutput Relation message.
type Relation struct {
	ID      uint32
	Schema  string
	Name    string
	Columns []string
	// Key marks the columns of the replica identity.
	Key []bool
}

// Tuple is one decoded row: nil for NULL, Unchanged for a TOASTed column
// the update did not touch.
type Tuple struct {
	Values    []*string
	Unchanged []bool
}

// Change is one decoded row change of a relation.
type Change struct {
	Op       ChangeOp
	Relation *Relation
	// Old is the replica identity (or full old row) of an update or delete;
	// nil when the message carried none.
	Old *Tuple
	// New is the new row of an insert or update.
	New *Tuple
}

// Decoder keeps the relations announced on a slot across messages.
type Decoder struct {
	relations map[uint32]*Relation
}

// NewDecoder returns an empty decoder.
func NewDecoder() *Decoder { return &Decoder{relations: map[uint32]*Relation{}} }

var errShortMessage = errors.New("pgoutput: short message")

// Decode parses one message. It returns a change for I, U and D messages,
// (nil, committed=true) for C, and (nil, false) for every other message.
func (d *Decoder) Decode(msg []byte) (change *Change, committed bool, err error) {
	if len(msg) == 0 {
		return nil, false, errShortMessage
	}
	r := reader{buf: msg[1:]}
	switch msg[0] {
	case 'R':
		rel := &Relation{}
		rel.ID = r.uint32()
		rel.Schema = r.cstring()
		rel.Name = r.cstring()
		r.byte()
		n := int(r.uint16())
		for range n {
			flags := r.byte()
			rel.Columns = append(rel.Columns, r.cstring())
			rel.Key = append(rel.Key, flags&1 == 1)
			r.uint32()
			r.uint32()
		}
		if r.err != nil {
			return nil, false, r.err
		}
		d.relations[rel.ID] = rel
		return nil, false, nil
	case 'I', 'U', 'D':
		id := r.uint32()
		rel := d.relations[id]
		if rel == nil {
			return nil, false, fmt.Errorf("pgoutput: %c message for unknown relation %d", msg[0], id)
		}
		c := &Change{Op: ChangeOp(msg[0]), Relation: rel}
		for r.err == nil && len(r.buf) > 0 {
			kind := r.byte()
			t := r.tuple(len(rel.Columns))
			switch kind {
			case 'N':
				c.New = t
			case 'K', 'O':
				c.Old = t
			default:
				return nil, false, fmt.Errorf("pgoutput: unexpected tuple kind %q", kind)
			}
		}
		if r.err != nil {
			return nil, false, r.err
		}
		if (c.Op == OpDelete && c.Old == nil) || (c.Op != OpDelete && c.New == nil) {
			return nil, false, fmt.Errorf("pgoutput: %c message without its tuple", msg[0])
		}
		return c, false, nil
	case 'C':
		return nil, true, nil
	case 'T':
		return nil, false, errors.New("pgoutput: TRUNCATE is not replicated by a placement workflow")
	}
	return nil, false, nil
}

type reader struct {
	buf []byte
	err error
}

func (r *reader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if len(r.buf) < n {
		r.err = errShortMessage
		return false
	}
	return true
}

func (r *reader) byte() byte {
	if !r.need(1) {
		return 0
	}
	b := r.buf[0]
	r.buf = r.buf[1:]
	return b
}

func (r *reader) uint16() uint16 {
	if !r.need(2) {
		return 0
	}
	v := binary.BigEndian.Uint16(r.buf)
	r.buf = r.buf[2:]
	return v
}

func (r *reader) uint32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(r.buf)
	r.buf = r.buf[4:]
	return v
}

func (r *reader) cstring() string {
	if r.err != nil {
		return ""
	}
	i := strings.IndexByte(string(r.buf), 0)
	if i < 0 {
		r.err = errShortMessage
		return ""
	}
	s := string(r.buf[:i])
	r.buf = r.buf[i+1:]
	return s
}

func (r *reader) tuple(ncols int) *Tuple {
	n := int(r.uint16())
	if r.err != nil {
		return nil
	}
	if n != ncols {
		r.err = fmt.Errorf("pgoutput: tuple has %d columns, relation %d", n, ncols)
		return nil
	}
	t := &Tuple{Values: make([]*string, n), Unchanged: make([]bool, n)}
	for i := range n {
		switch r.byte() {
		case 'n':
		case 'u':
			t.Unchanged[i] = true
		case 't':
			l := int(r.uint32())
			if !r.need(l) {
				return nil
			}
			v := string(r.buf[:l])
			r.buf = r.buf[l:]
			t.Values[i] = &v
		case 'b':
			r.err = errors.New("pgoutput: binary tuple values are not supported")
			return nil
		default:
			if r.err == nil {
				r.err = errors.New("pgoutput: unknown column kind")
			}
			return nil
		}
	}
	return t
}

// ParseLSN turns a pg_lsn text (X/Y) into its byte position.
func ParseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("pgoutput: malformed lsn %q", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("pgoutput: malformed lsn %q", s)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("pgoutput: malformed lsn %q", s)
	}
	return h<<32 | l, nil
}

// FormatLSN renders a byte position as pg_lsn text.
func FormatLSN(v uint64) string { return fmt.Sprintf("%X/%X", v>>32, uint32(v)) }
