package controller

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/placement"
)

// TablePlacement is one side (from or to) of a placement workflow.
type TablePlacement struct {
	Placement string  `json:"placement"`
	ShardKey  *string `json:"shard_key"`
}

func (p TablePlacement) key() string {
	if p.ShardKey == nil {
		return ""
	}
	return *p.ShardKey
}

// placementRouter decides which serving shards hold a row under a
// placement: every shard for reference, the home shard for unsharded, the
// shard owning the hash of the key for sharded.
type placementRouter struct {
	placement TablePlacement
	home      int32
	ids       []int32
	ranges    placement.RangeSet
	// keyIndex is the position of the shard key in the row; keyType its SQL
	// type as format_type renders it.
	keyIndex int
	keyType  string
}

// Sources lists the shards that hold rows under this placement.
func (r *placementRouter) Sources() []int32 {
	switch r.placement.Placement {
	case "unsharded":
		return []int32{r.home}
	case "reference":
		return []int32{r.home}
	}
	return slices.Clone(r.ids)
}

// Holders lists every shard that owns a copy of the table under this
// placement.
func (r *placementRouter) Holders() []int32 {
	if r.placement.Placement == "unsharded" {
		return []int32{r.home}
	}
	return slices.Clone(r.ids)
}

// Route returns the shards a row belongs to.
func (r *placementRouter) Route(row []*string) ([]int32, error) {
	switch r.placement.Placement {
	case "unsharded":
		return []int32{r.home}, nil
	case "reference":
		return slices.Clone(r.ids), nil
	}
	if r.keyIndex < 0 || r.keyIndex >= len(row) {
		return nil, fmt.Errorf("shard key column %q not in row", r.placement.key())
	}
	v := row[r.keyIndex]
	if v == nil {
		return nil, fmt.Errorf("shard key %q is null", r.placement.key())
	}
	id, err := KeyspaceIDOfText(*v, r.keyType)
	if err != nil {
		return nil, err
	}
	i := r.ranges.Locate(id)
	if i < 0 || i >= len(r.ids) {
		return nil, fmt.Errorf("keyspace id %d outside every range", id)
	}
	return []int32{r.ids[i]}, nil
}

// KeyspaceIDOfText hashes the text form of a shard key value of SQL type
// typ the way the router hashes the bound value: integers as int8, character
// types as text, uuid over its bytes.
func KeyspaceIDOfText(v, typ string) (int64, error) {
	base := strings.ToLower(typ)
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	switch strings.TrimSpace(base) {
	case "bigint", "integer", "smallint", "int8", "int4", "int2":
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("shard key value %q is not an integer", v)
		}
		return placement.KeyspaceID(n)
	case "text", "character varying", "varchar", "name":
		return placement.KeyspaceID(v)
	case "character", "bpchar", "char":
		return 0, fmt.Errorf("shard key of type %s is not supported: blank-padded character equality does not match byte-wise hashing", typ)
	case "uuid":
		raw, err := hex.DecodeString(strings.ReplaceAll(v, "-", ""))
		if err != nil || len(raw) != 16 {
			return 0, fmt.Errorf("shard key value %q is not a uuid", v)
		}
		var u [16]byte
		copy(u[:], raw)
		return placement.KeyspaceID(u)
	}
	return 0, fmt.Errorf("shard key of type %s cannot be hashed", typ)
}

// QuoteLiteral renders v as an untyped SQL string literal; nil is NULL.
func QuoteLiteral(v *string) string {
	if v == nil {
		return "NULL"
	}
	return "'" + strings.ReplaceAll(strings.ReplaceAll(*v, "\\", "\\\\"), "'", "''") + "'"
}

func quoteLiteralE(v *string) string {
	if v == nil {
		return "NULL"
	}
	if strings.ContainsRune(*v, '\\') {
		return "E" + QuoteLiteral(v)
	}
	return QuoteLiteral(v)
}

// rowShape names the columns of the moved table in source order and the
// primary key columns the applier identifies rows by.
type rowShape struct {
	Schema   string
	Name     string
	Columns  []string
	Identity []string // attidentity per column: "a" always, "d" by default, "" none
	PK       []string
}

// identityAlways reports whether column index i is a GENERATED ALWAYS identity
// column, which cannot take an explicit value without OVERRIDING SYSTEM VALUE
// and cannot be updated to a non-DEFAULT value.
func (s rowShape) identityAlways(i int) bool {
	return i < len(s.Identity) && s.Identity[i] == "a"
}

// alwaysIdentityChanged reports whether an UPDATE gave a GENERATED ALWAYS
// identity column outside the primary key a new value -- what UPDATE t SET
// c = DEFAULT does. The upsert cannot carry it: PostgreSQL forbids such a
// column in ON CONFLICT DO UPDATE SET, so the statement would conflict on
// the unchanged key and leave the old value standing.
func (s rowShape) alwaysIdentityChanged(old, updated *Tuple) bool {
	if old == nil || updated == nil {
		return false
	}
	for i, c := range s.Columns {
		if !s.identityAlways(i) || slices.Contains(s.PK, c) {
			continue
		}
		if i >= len(old.Values) || i >= len(updated.Values) {
			continue
		}
		x, y := old.Values[i], updated.Values[i]
		if (x == nil) != (y == nil) || (x != nil && *x != *y) {
			return true
		}
	}
	return false
}

func (s rowShape) pkIndexes() []int {
	var out []int
	for _, k := range s.PK {
		out = append(out, slices.Index(s.Columns, k))
	}
	return out
}

func (s rowShape) qualified(name string) string {
	return QuoteIdent(s.Schema) + "." + QuoteIdent(name)
}

// UpsertSQL renders an INSERT ... ON CONFLICT (pk) DO UPDATE of rows into
// table; columns an update left unchanged (TOAST) are omitted, so a row with
// unchanged columns is rendered as its own statement.
func (s rowShape) UpsertSQL(table string, rows []*Tuple) []string {
	var out []string
	var plain []*Tuple
	for _, r := range rows {
		if slices.Contains(r.Unchanged, true) {
			out = append(out, s.upsert(table, []*Tuple{r}, r.Unchanged))
			continue
		}
		plain = append(plain, r)
	}
	if len(plain) > 0 {
		out = append(out, s.upsert(table, plain, nil))
	}
	return out
}

func (s rowShape) upsert(table string, rows []*Tuple, skip []bool) string {
	var cols []string
	var idx []int
	for i, c := range s.Columns {
		if skip != nil && skip[i] {
			continue
		}
		cols = append(cols, QuoteIdent(c))
		idx = append(idx, i)
	}
	override := false
	for _, i := range idx {
		if s.identityAlways(i) {
			override = true
			break
		}
	}
	var b strings.Builder
	b.WriteString("INSERT INTO " + s.qualified(table) + " (" + strings.Join(cols, ", ") + ") ")
	if override {
		// GENERATED ALWAYS identity columns reject an explicit value unless
		// the copy overrides the system-generated value.
		b.WriteString("OVERRIDING SYSTEM VALUE ")
	}
	b.WriteString("VALUES ")
	for ri, r := range rows {
		if ri > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j, i := range idx {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteLiteralE(r.Values[i]))
		}
		b.WriteString(")")
	}
	var pk []string
	for _, k := range s.PK {
		pk = append(pk, QuoteIdent(k))
	}
	pkSet := map[string]bool{}
	for _, k := range s.PK {
		pkSet[k] = true
	}
	var sets []string
	for j, i := range idx {
		// The conflict target never needs updating, and a GENERATED ALWAYS
		// identity column cannot be updated to a non-DEFAULT value at all.
		if pkSet[s.Columns[i]] || s.identityAlways(i) {
			continue
		}
		sets = append(sets, cols[j]+" = EXCLUDED."+cols[j])
	}
	if len(sets) == 0 {
		// Every non-key column is an identity/PK column: nothing to update on
		// conflict, so the copied row is already present.
		b.WriteString(" ON CONFLICT (" + strings.Join(pk, ", ") + ") DO NOTHING")
		return b.String()
	}
	b.WriteString(" ON CONFLICT (" + strings.Join(pk, ", ") + ") DO UPDATE SET ")
	b.WriteString(strings.Join(sets, ", "))
	return b.String()
}

// DeleteSQL renders a DELETE of one row by primary key.
func (s rowShape) DeleteSQL(table string, row *Tuple) string {
	var conds []string
	for _, i := range s.pkIndexes() {
		conds = append(conds, QuoteIdent(s.Columns[i])+" = "+quoteLiteralE(row.Values[i]))
	}
	return "DELETE FROM " + s.qualified(table) + " WHERE " + strings.Join(conds, " AND ")
}

// applyOp is one statement bound for one shard.
type applyOp struct {
	shard int32
	sql   string
}

// routeChange turns a decoded change into the statements the shadow tables
// need under the new placement: an update whose row moves between shards
// becomes a delete on the old shard and an insert on the new one. Old rows
// of an update identify the row by the key the message carried; without a
// full old row (a key change with a plain replica identity) the delete goes
// by the new row's key on every shard the old row may have lived on.
func routeChange(r *placementRouter, shape rowShape, table string, c *Change) ([]applyOp, error) {
	var ops []applyOp
	switch c.Op {
	case OpInsert:
		targets, err := r.Route(c.New.Values)
		if err != nil {
			return nil, err
		}
		for _, t := range targets {
			for _, sql := range shape.UpsertSQL(table, []*Tuple{c.New}) {
				ops = append(ops, applyOp{t, sql})
			}
		}
	case OpDelete:
		targets, err := r.Route(c.Old.Values)
		if err != nil {
			targets = r.Holders()
		}
		for _, t := range targets {
			ops = append(ops, applyOp{t, shape.DeleteSQL(table, c.Old)})
		}
	case OpUpdate:
		if c.Old != nil {
			for i, u := range c.New.Unchanged {
				if u {
					c.New.Values[i], c.New.Unchanged[i] = c.Old.Values[i], false
				}
			}
		}
		newTargets, err := r.Route(c.New.Values)
		if err != nil {
			return nil, err
		}
		old := c.Old
		if old == nil {
			old = c.New
		}
		oldTargets, err := r.Route(old.Values)
		if err != nil {
			oldTargets = r.Holders()
		}
		for _, t := range oldTargets {
			if !slices.Contains(newTargets, t) {
				ops = append(ops, applyOp{t, shape.DeleteSQL(table, old)})
			}
		}
		// A changed GENERATED ALWAYS identity is replayed the same way a
		// changed key is: remove the row the shadow holds and insert the
		// new one, which the insert may carry because it overrides the
		// system value.
		replace := !samePK(shape, old, c.New) || shape.alwaysIdentityChanged(c.Old, c.New)
		for _, t := range newTargets {
			if replace && slices.Contains(oldTargets, t) {
				ops = append(ops, applyOp{t, shape.DeleteSQL(table, old)})
			}
			for _, sql := range shape.UpsertSQL(table, []*Tuple{c.New}) {
				ops = append(ops, applyOp{t, sql})
			}
		}
	}
	return ops, nil
}

func samePK(shape rowShape, a, b *Tuple) bool {
	for _, i := range shape.pkIndexes() {
		x, y := a.Values[i], b.Values[i]
		if (x == nil) != (y == nil) || (x != nil && *x != *y) {
			return false
		}
	}
	return true
}
