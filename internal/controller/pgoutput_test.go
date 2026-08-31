package controller

import (
	"encoding/binary"
	"strings"
	"testing"
)

type msgBuilder struct{ b []byte }

func (m *msgBuilder) byte(v byte) *msgBuilder  { m.b = append(m.b, v); return m }
func (m *msgBuilder) u16(v uint16) *msgBuilder { m.b = binary.BigEndian.AppendUint16(m.b, v); return m }
func (m *msgBuilder) u32(v uint32) *msgBuilder { m.b = binary.BigEndian.AppendUint32(m.b, v); return m }
func (m *msgBuilder) u64(v uint64) *msgBuilder { m.b = binary.BigEndian.AppendUint64(m.b, v); return m }
func (m *msgBuilder) str(s string) *msgBuilder {
	m.b = append(m.b, s...)
	m.b = append(m.b, 0)
	return m
}
func (m *msgBuilder) text(s string) *msgBuilder {
	m.byte('t')
	m.u32(uint32(len(s)))
	m.b = append(m.b, s...)
	return m
}

func relationMsg(id uint32, cols ...string) []byte {
	m := (&msgBuilder{}).byte('R').u32(id).str("public").str("orders").byte('f').u16(uint16(len(cols)))
	for i, c := range cols {
		flag := byte(0)
		if i == 0 {
			flag = 1
		}
		m.byte(flag).str(c).u32(20).u32(0)
	}
	return m.b
}

func tuple(m *msgBuilder, vals ...*string) *msgBuilder {
	m.u16(uint16(len(vals)))
	for _, v := range vals {
		switch {
		case v == nil:
			m.byte('n')
		case *v == "<unchanged>":
			m.byte('u')
		default:
			m.text(*v)
		}
	}
	return m
}

func s(v string) *string { return &v }

func TestDecoderRelationInsertUpdateDelete(t *testing.T) {
	d := NewDecoder()
	if c, committed, err := d.Decode(relationMsg(7, "id", "tenant_id", "note")); c != nil || committed || err != nil {
		t.Fatalf("relation: %v %v %v", c, committed, err)
	}
	if c, committed, err := d.Decode((&msgBuilder{}).byte('B').u64(1).u64(2).u32(3).b); c != nil || committed || err != nil {
		t.Fatalf("begin: %v %v %v", c, committed, err)
	}
	ins := tuple((&msgBuilder{}).byte('I').u32(7).byte('N'), s("1"), s("10"), nil).b
	c, _, err := d.Decode(ins)
	if err != nil || c.Op != OpInsert || c.Relation.Name != "orders" || *c.New.Values[1] != "10" || c.New.Values[2] != nil {
		t.Fatalf("insert: %+v %v", c, err)
	}
	upd := tuple(tuple((&msgBuilder{}).byte('U').u32(7).byte('O'), s("1"), s("10"), s("old")).byte('N'), s("1"), s("11"), s("<unchanged>")).b
	c, _, err = d.Decode(upd)
	if err != nil || c.Op != OpUpdate || *c.Old.Values[2] != "old" || !c.New.Unchanged[2] || *c.New.Values[1] != "11" {
		t.Fatalf("update: %+v %v", c, err)
	}
	// Default replica identity sends the key as K, not the full old row as
	// O. The applier reads one old image either way.
	keyed := tuple(tuple((&msgBuilder{}).byte('U').u32(7).byte('K'), s("1"), nil, nil).byte('N'), s("1"), s("12"), s("note")).b
	c, _, err = d.Decode(keyed)
	if err != nil || c.Op != OpUpdate || c.Old == nil || *c.Old.Values[0] != "1" || *c.New.Values[1] != "12" {
		t.Fatalf("keyed update: %+v %v", c, err)
	}

	del := tuple((&msgBuilder{}).byte('D').u32(7).byte('K'), s("1"), nil, nil).b
	c, _, err = d.Decode(del)
	if err != nil || c.Op != OpDelete || c.Old == nil || *c.Old.Values[0] != "1" {
		t.Fatalf("delete: %+v %v", c, err)
	}
	if c, committed, err := d.Decode((&msgBuilder{}).byte('C').byte(0).u64(1).u64(2).u64(3).b); c != nil || !committed || err != nil {
		t.Fatalf("commit: %v %v %v", c, committed, err)
	}
	if !c.Relation.Key[0] || c.Relation.Key[1] {
		t.Fatalf("key flags: %v", c.Relation.Key)
	}
}

func TestDecoderRefusals(t *testing.T) {
	d := NewDecoder()
	cases := map[string][]byte{
		"empty":            {},
		"unknown relation": tuple((&msgBuilder{}).byte('I').u32(9).byte('N'), s("1")).b,
		"short relation":   (&msgBuilder{}).byte('R').u32(1).b,
		"truncate":         (&msgBuilder{}).byte('T').u32(1).byte(0).u32(7).b,
	}
	for name, msg := range cases {
		if _, _, err := d.Decode(msg); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, _, err := d.Decode(relationMsg(7, "id")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.Decode(tuple((&msgBuilder{}).byte('I').u32(7).byte('N'), s("1"), s("2")).b); err == nil || !strings.Contains(err.Error(), "columns") {
		t.Errorf("column count mismatch: %v", err)
	}
	if _, _, err := d.Decode((&msgBuilder{}).byte('I').u32(7).byte('N').u16(1).byte('b').u32(1).byte(0).b); err == nil || !strings.Contains(err.Error(), "binary") {
		t.Errorf("binary tuple: %v", err)
	}
	if _, _, err := d.Decode((&msgBuilder{}).byte('D').u32(7).byte('N').u16(1).text("1").b); err == nil {
		t.Error("delete without an old tuple accepted")
	}
	if _, _, err := d.Decode((&msgBuilder{}).byte('I').u32(7).byte('X').u16(1).text("1").b); err == nil {
		t.Error("unknown tuple kind accepted")
	}
	if c, committed, err := d.Decode((&msgBuilder{}).byte('Y').u32(1).str("public").str("mood").b); c != nil || committed || err != nil {
		t.Errorf("type message: %v %v %v", c, committed, err)
	}
}
