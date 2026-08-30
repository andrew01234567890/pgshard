package controller

import (
	"errors"
	"fmt"

	"github.com/andrew01234567890/pgshard/internal/pgoutput"
)

// The placement applier reads a pgoutput slot as
// pg_logical_slot_peek_binary_changes returns it and needs only row
// changes: relations, and the text tuples of inserts, updates and deletes.
// internal/pgoutput decodes the protocol; what follows adapts its
// message-per-type API to the applier's row-change loop and applies the
// applier's own policy -- text tuples only (the slot is read with
// binary=off), a tuple as wide as its relation, and no TRUNCATE.

// ChangeOp is the kind of a decoded row change.
type ChangeOp byte

// Row change kinds.
const (
	OpInsert ChangeOp = 'I'
	OpUpdate ChangeOp = 'U'
	OpDelete ChangeOp = 'D'
)

// Relation is a table the applier has seen announced on the slot.
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

// Change is one decoded row change of a relation. Old is set for every
// update and delete and New for every insert and update: the decoder
// underneath refuses a message that arrives without its tuple.
type Change struct {
	Op       ChangeOp
	Relation *Relation
	// Old is the replica identity, or the full old row when the table has
	// REPLICA IDENTITY FULL.
	Old *Tuple
	// New is the new row of an insert or update.
	New *Tuple
}

// Decoder turns the messages of one slot into row changes, keeping the
// relations announced on it across messages.
type Decoder struct {
	dec       *pgoutput.Decoder
	relations map[uint32]*Relation
}

// NewDecoder returns an empty decoder.
func NewDecoder() *Decoder {
	return &Decoder{dec: pgoutput.NewDecoder(), relations: map[uint32]*Relation{}}
}

// Decode parses one message. It returns a change for insert, update and
// delete messages, (nil, committed=true) for a commit, and (nil, false)
// for every other message.
func (d *Decoder) Decode(msg []byte) (change *Change, committed bool, err error) {
	m, err := d.dec.Decode(msg)
	if err != nil {
		return nil, false, err
	}
	switch m := m.(type) {
	case *pgoutput.Relation:
		rel := &Relation{ID: m.ID, Schema: m.Namespace, Name: m.Name}
		for _, c := range m.Columns {
			rel.Columns = append(rel.Columns, c.Name)
			rel.Key = append(rel.Key, c.Key)
		}
		d.relations[rel.ID] = rel
		return nil, false, nil
	case *pgoutput.Insert:
		return d.change(OpInsert, m.RelationID, nil, &m.New)
	case *pgoutput.Update:
		return d.change(OpUpdate, m.RelationID, firstTuple(m.Key, m.Old), &m.New)
	case *pgoutput.Delete:
		return d.change(OpDelete, m.RelationID, firstTuple(m.Key, m.Old), nil)
	case *pgoutput.Commit:
		return nil, true, nil
	case *pgoutput.Truncate:
		return nil, false, errors.New("pgoutput: TRUNCATE is not replicated by a placement workflow")
	}
	return nil, false, nil
}

func firstTuple(key, old *pgoutput.Tuple) *pgoutput.Tuple {
	if key != nil {
		return key
	}
	return old
}

func (d *Decoder) change(op ChangeOp, relID uint32, before, after *pgoutput.Tuple) (*Change, bool, error) {
	rel := d.relations[relID]
	if rel == nil {
		return nil, false, fmt.Errorf("pgoutput: %c message for unknown relation %d", op, relID)
	}
	c := &Change{Op: op, Relation: rel}
	var err error
	if before != nil {
		if c.Old, err = applierTuple(before, len(rel.Columns)); err != nil {
			return nil, false, err
		}
	}
	if after != nil {
		if c.New, err = applierTuple(after, len(rel.Columns)); err != nil {
			return nil, false, err
		}
	}
	return c, false, nil
}

// applierTuple keeps only what the applier can send on: text values, NULLs
// and unchanged TOAST markers, in a tuple as wide as the relation the
// applier is tracking.
func applierTuple(t *pgoutput.Tuple, ncols int) (*Tuple, error) {
	if len(t.Columns) != ncols {
		return nil, fmt.Errorf("pgoutput: tuple has %d columns, relation %d", len(t.Columns), ncols)
	}
	out := &Tuple{Values: make([]*string, ncols), Unchanged: make([]bool, ncols)}
	for i, c := range t.Columns {
		switch c.Kind {
		case pgoutput.ColumnNull:
		case pgoutput.ColumnUnchanged:
			out.Unchanged[i] = true
		case pgoutput.ColumnText:
			v := string(c.Data)
			out.Values[i] = &v
		case pgoutput.ColumnBinary:
			return nil, errors.New("pgoutput: binary tuple values are not supported")
		default:
			return nil, errors.New("pgoutput: unknown column kind")
		}
	}
	return out, nil
}
