// Package pgoutput decodes the PostgreSQL pgoutput logical replication
// protocol, version 4 (streaming of in-progress transactions in parallel
// mode, two-phase commit and logical decoding messages), into Go values.
package pgoutput

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// Message is one decoded pgoutput message.
type Message interface{ pgoutputMessage() }

// Tuple column kinds.
const (
	ColumnNull      byte = 'n'
	ColumnUnchanged byte = 'u'
	ColumnText      byte = 't'
	ColumnBinary    byte = 'b'
)

// TupleColumn is one column of a tuple image.
type TupleColumn struct {
	// Kind is ColumnNull, ColumnUnchanged (TOAST datum not sent), ColumnText
	// or ColumnBinary.
	Kind byte
	Data []byte
}

// Tuple is a row image.
type Tuple struct {
	Columns []TupleColumn
}

// Column describes a relation column.
type Column struct {
	// Key is true when the column is part of the replica identity.
	Key     bool
	Name    string
	TypeOID uint32
	TypeMod int32
}

// Relation describes a table; sent before its first change and after its
// definition changes.
type Relation struct {
	Xid             uint32
	ID              uint32
	Namespace       string
	Name            string
	ReplicaIdentity byte
	Columns         []Column
}

// Type describes a non-builtin type used by a relation.
type Type struct {
	Xid       uint32
	ID        uint32
	Namespace string
	Name      string
}

// Begin opens a transaction.
type Begin struct {
	FinalLSN   uint64
	CommitTime time.Time
	Xid        uint32
}

// Commit closes a transaction.
type Commit struct {
	Flags      uint8
	CommitLSN  uint64
	EndLSN     uint64
	CommitTime time.Time
}

// Origin names the replication origin of the following changes.
type Origin struct {
	CommitLSN uint64
	Name      string
}

// Insert is a new row.
type Insert struct {
	Xid        uint32
	RelationID uint32
	New        Tuple
}

// Update is a changed row; Key or Old carry the previous image when the
// replica identity provides it.
type Update struct {
	Xid        uint32
	RelationID uint32
	Key        *Tuple
	Old        *Tuple
	New        Tuple
}

// Delete is a removed row identified by Key or Old.
type Delete struct {
	Xid        uint32
	RelationID uint32
	Key        *Tuple
	Old        *Tuple
}

// Truncate empties relations.
type Truncate struct {
	Xid             uint32
	Cascade         bool
	RestartIdentity bool
	RelationIDs     []uint32
}

// LogicalMessage is a pg_logical_emit_message payload.
type LogicalMessage struct {
	Xid           uint32
	Transactional bool
	LSN           uint64
	Prefix        string
	Content       []byte
}

// StreamStart opens a segment of a streamed in-progress transaction.
type StreamStart struct {
	Xid          uint32
	FirstSegment bool
}

// StreamStop closes a streamed segment.
type StreamStop struct{}

// StreamCommit commits a streamed transaction.
type StreamCommit struct {
	Xid        uint32
	Flags      uint8
	CommitLSN  uint64
	EndLSN     uint64
	CommitTime time.Time
}

// StreamAbort aborts a streamed transaction (SubXid == Xid) or one of its
// subtransactions. AbortLSN and AbortTime are sent in parallel streaming
// mode only and are zero otherwise.
type StreamAbort struct {
	Xid       uint32
	SubXid    uint32
	AbortLSN  uint64
	AbortTime time.Time
}

// BeginPrepare opens a two-phase transaction.
type BeginPrepare struct {
	PrepareLSN  uint64
	EndLSN      uint64
	PrepareTime time.Time
	Xid         uint32
	Gid         string
}

// Prepare ends the changes of a two-phase transaction.
type Prepare struct {
	Flags       uint8
	PrepareLSN  uint64
	EndLSN      uint64
	PrepareTime time.Time
	Xid         uint32
	Gid         string
}

// CommitPrepared commits a prepared transaction.
type CommitPrepared struct {
	Flags      uint8
	CommitLSN  uint64
	EndLSN     uint64
	CommitTime time.Time
	Xid        uint32
	Gid        string
}

// RollbackPrepared rolls back a prepared transaction.
type RollbackPrepared struct {
	Flags          uint8
	PrepareEndLSN  uint64
	RollbackEndLSN uint64
	PrepareTime    time.Time
	RollbackTime   time.Time
	Xid            uint32
	Gid            string
}

// StreamPrepare prepares a streamed two-phase transaction.
type StreamPrepare struct {
	Flags       uint8
	PrepareLSN  uint64
	EndLSN      uint64
	PrepareTime time.Time
	Xid         uint32
	Gid         string
}

func (*Relation) pgoutputMessage()         {}
func (*Type) pgoutputMessage()             {}
func (*Begin) pgoutputMessage()            {}
func (*Commit) pgoutputMessage()           {}
func (*Origin) pgoutputMessage()           {}
func (*Insert) pgoutputMessage()           {}
func (*Update) pgoutputMessage()           {}
func (*Delete) pgoutputMessage()           {}
func (*Truncate) pgoutputMessage()         {}
func (*LogicalMessage) pgoutputMessage()   {}
func (*StreamStart) pgoutputMessage()      {}
func (*StreamStop) pgoutputMessage()       {}
func (*StreamCommit) pgoutputMessage()     {}
func (*StreamAbort) pgoutputMessage()      {}
func (*BeginPrepare) pgoutputMessage()     {}
func (*Prepare) pgoutputMessage()          {}
func (*CommitPrepared) pgoutputMessage()   {}
func (*RollbackPrepared) pgoutputMessage() {}
func (*StreamPrepare) pgoutputMessage()    {}

// ErrShort reports a truncated message.
var ErrShort = errors.New("pgoutput: truncated message")

// Decoder decodes a sequence of pgoutput messages from one slot, tracking
// the relation cache and whether the current segment belongs to a streamed
// transaction (which prefixes messages with the xid).
type Decoder struct {
	relations map[uint32]*Relation
	streamXid uint32
	inStream  bool
}

// NewDecoder returns an empty decoder.
func NewDecoder() *Decoder { return &Decoder{relations: map[uint32]*Relation{}} }

// Relation returns the cached relation with the given id.
func (d *Decoder) Relation(id uint32) (*Relation, bool) {
	r, ok := d.relations[id]
	return r, ok
}

// InStream reports whether the decoder is inside a streamed segment.
func (d *Decoder) InStream() bool { return d.inStream }

// Decode decodes one message (the payload of one XLogData frame).
func (d *Decoder) Decode(data []byte) (Message, error) {
	if len(data) == 0 {
		return nil, ErrShort
	}
	r := reader{buf: data[1:]}
	var xid uint32
	switch data[0] {
	case 'R', 'Y', 'I', 'U', 'D', 'T', 'M':
		if d.inStream {
			xid = r.uint32()
		}
	}
	var msg Message
	switch data[0] {
	case 'B':
		msg = &Begin{FinalLSN: r.uint64(), CommitTime: r.time(), Xid: r.uint32()}
	case 'C':
		msg = &Commit{Flags: r.uint8(), CommitLSN: r.uint64(), EndLSN: r.uint64(), CommitTime: r.time()}
	case 'O':
		msg = &Origin{CommitLSN: r.uint64(), Name: r.string()}
	case 'R':
		rel := &Relation{Xid: xid, ID: r.uint32(), Namespace: r.string(), Name: r.string(), ReplicaIdentity: r.uint8()}
		n := int(r.uint16())
		for i := 0; i < n && r.err == nil; i++ {
			rel.Columns = append(rel.Columns, Column{Key: r.uint8()&1 != 0, Name: r.string(), TypeOID: r.uint32(), TypeMod: int32(r.uint32())})
		}
		if r.err == nil {
			d.relations[rel.ID] = rel
		}
		msg = rel
	case 'Y':
		msg = &Type{Xid: xid, ID: r.uint32(), Namespace: r.string(), Name: r.string()}
	case 'I':
		ins := &Insert{Xid: xid, RelationID: r.uint32()}
		if k := r.uint8(); k != 'N' && r.err == nil {
			return nil, fmt.Errorf("pgoutput: insert tuple kind %q", k)
		}
		ins.New = r.tuple()
		msg = ins
	case 'U':
		up := &Update{Xid: xid, RelationID: r.uint32()}
		k := r.uint8()
		switch k {
		case 'K':
			t := r.tuple()
			up.Key = &t
			k = r.uint8()
		case 'O':
			t := r.tuple()
			up.Old = &t
			k = r.uint8()
		}
		if k != 'N' && r.err == nil {
			return nil, fmt.Errorf("pgoutput: update tuple kind %q", k)
		}
		up.New = r.tuple()
		msg = up
	case 'D':
		del := &Delete{Xid: xid, RelationID: r.uint32()}
		switch k := r.uint8(); k {
		case 'K':
			t := r.tuple()
			del.Key = &t
		case 'O':
			t := r.tuple()
			del.Old = &t
		default:
			if r.err == nil {
				return nil, fmt.Errorf("pgoutput: delete tuple kind %q", k)
			}
		}
		msg = del
	case 'T':
		tr := &Truncate{Xid: xid}
		n := int(r.uint32())
		opts := r.uint8()
		tr.Cascade = opts&1 != 0
		tr.RestartIdentity = opts&2 != 0
		for i := 0; i < n && r.err == nil; i++ {
			tr.RelationIDs = append(tr.RelationIDs, r.uint32())
		}
		msg = tr
	case 'M':
		lm := &LogicalMessage{Xid: xid, Transactional: r.uint8()&1 != 0, LSN: r.uint64(), Prefix: r.string()}
		lm.Content = r.bytes(int(r.uint32()))
		msg = lm
	case 'S':
		ss := &StreamStart{Xid: r.uint32(), FirstSegment: r.uint8() == 1}
		if r.err == nil {
			d.inStream = true
			d.streamXid = ss.Xid
		}
		msg = ss
	case 'E':
		d.inStream = false
		d.streamXid = 0
		msg = &StreamStop{}
	case 'c':
		msg = &StreamCommit{Xid: r.uint32(), Flags: r.uint8(), CommitLSN: r.uint64(), EndLSN: r.uint64(), CommitTime: r.time()}
	case 'A':
		sa := &StreamAbort{Xid: r.uint32(), SubXid: r.uint32()}
		if r.remaining() >= 16 {
			sa.AbortLSN = r.uint64()
			sa.AbortTime = r.time()
		}
		msg = sa
	case 'b':
		msg = &BeginPrepare{PrepareLSN: r.uint64(), EndLSN: r.uint64(), PrepareTime: r.time(), Xid: r.uint32(), Gid: r.string()}
	case 'P':
		msg = &Prepare{Flags: r.uint8(), PrepareLSN: r.uint64(), EndLSN: r.uint64(), PrepareTime: r.time(), Xid: r.uint32(), Gid: r.string()}
	case 'K':
		msg = &CommitPrepared{Flags: r.uint8(), CommitLSN: r.uint64(), EndLSN: r.uint64(), CommitTime: r.time(), Xid: r.uint32(), Gid: r.string()}
	case 'r':
		msg = &RollbackPrepared{Flags: r.uint8(), PrepareEndLSN: r.uint64(), RollbackEndLSN: r.uint64(), PrepareTime: r.time(), RollbackTime: r.time(), Xid: r.uint32(), Gid: r.string()}
	case 'p':
		msg = &StreamPrepare{Flags: r.uint8(), PrepareLSN: r.uint64(), EndLSN: r.uint64(), PrepareTime: r.time(), Xid: r.uint32(), Gid: r.string()}
	default:
		return nil, fmt.Errorf("pgoutput: unknown message type %q", data[0])
	}
	if r.err != nil {
		return nil, r.err
	}
	return msg, nil
}

var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// PGTime converts microseconds since the PostgreSQL epoch to time.Time.
func PGTime(micros int64) time.Time { return pgEpoch.Add(time.Duration(micros) * time.Microsecond) }

// PGMicros is the inverse of PGTime.
func PGMicros(t time.Time) int64 { return t.Sub(pgEpoch).Microseconds() }

type reader struct {
	buf []byte
	err error
}

func (r *reader) remaining() int { return len(r.buf) }

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || len(r.buf) < n {
		r.err = ErrShort
		return nil
	}
	b := r.buf[:n]
	r.buf = r.buf[n:]
	return b
}

func (r *reader) uint8() uint8 {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}

func (r *reader) uint16() uint16 {
	b := r.take(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}

func (r *reader) uint32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func (r *reader) uint64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (r *reader) time() time.Time {
	v := int64(r.uint64())
	if r.err != nil {
		return time.Time{}
	}
	return PGTime(v)
}

func (r *reader) string() string {
	if r.err != nil {
		return ""
	}
	for i, c := range r.buf {
		if c == 0 {
			s := string(r.buf[:i])
			r.buf = r.buf[i+1:]
			return s
		}
	}
	r.err = ErrShort
	return ""
}

func (r *reader) bytes(n int) []byte {
	b := r.take(n)
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func (r *reader) tuple() Tuple {
	n := int(r.uint16())
	var t Tuple
	for i := 0; i < n && r.err == nil; i++ {
		c := TupleColumn{Kind: r.uint8()}
		switch c.Kind {
		case ColumnNull, ColumnUnchanged:
		case ColumnText, ColumnBinary:
			c.Data = r.bytes(int(r.uint32()))
		default:
			if r.err == nil {
				r.err = fmt.Errorf("pgoutput: unknown tuple column kind %q", c.Kind)
			}
		}
		if r.err == nil {
			t.Columns = append(t.Columns, c)
		}
	}
	return t
}
