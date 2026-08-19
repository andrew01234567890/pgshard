package plan

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"testing"
)

func be64(v int64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, uint64(v)); return b }
func be32(v int32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, uint32(v)); return b }
func be16(v int16) []byte { b := make([]byte, 2); binary.BigEndian.PutUint16(b, uint16(v)); return b }

func TestDecodeShardKey(t *testing.T) {
	cases := []struct {
		name   string
		oid    uint32
		hint   TypeHint
		format int16
		raw    []byte
		want   any
		err    error
		fails  bool
	}{
		{name: "int8 text", oid: oidInt8, raw: []byte("42"), want: int64(42)},
		{name: "int4 text", oid: oidInt4, raw: []byte(" 7 "), want: int64(7)},
		{name: "int2 text negative", oid: oidInt2, raw: []byte("-3"), want: int64(-3)},
		{name: "int8 text garbage", oid: oidInt8, raw: []byte("x"), fails: true},
		{name: "text", oid: oidText, raw: []byte("acme"), want: "acme"},
		{name: "varchar numeric string stays text", oid: oidVarchar, raw: []byte("123"), want: "123"},
		{name: "unknown text non-numeric", oid: 0, raw: []byte("acme"), want: "acme"},
		{name: "unknown text numeric is ambiguous", oid: 0, raw: []byte("123"), err: ErrAmbiguousKey},
		{name: "unknown oid 705 numeric is ambiguous", oid: oidUnknown, raw: []byte("123"), err: ErrAmbiguousKey},
		{name: "unknown text numeric with int hint", oid: 0, hint: HintInt, raw: []byte("123"), want: int64(123)},
		{name: "unknown text numeric with text hint", oid: 0, hint: HintText, raw: []byte("123"), want: "123"},
		{name: "declared type beats hint", oid: oidText, hint: HintInt, raw: []byte("123"), want: "123"},
		{name: "int8 binary", oid: oidInt8, format: 1, raw: be64(-99), want: int64(-99)},
		{name: "int4 binary", oid: oidInt4, format: 1, raw: be32(5), want: int64(5)},
		{name: "int2 binary", oid: oidInt2, format: 1, raw: be16(-2), want: int64(-2)},
		{name: "int8 binary wrong length", oid: oidInt8, format: 1, raw: be32(5), fails: true},
		{name: "unknown binary 8 bytes", oid: 0, format: 1, raw: be64(77), want: int64(77)},
		{name: "unknown binary 4 bytes", oid: 0, format: 1, raw: be32(77), want: int64(77)},
		{name: "unknown binary 2 bytes", oid: 0, format: 1, raw: be16(77), want: int64(77)},
		{name: "unknown binary other length", oid: 0, format: 1, raw: []byte("abc"), err: ErrAmbiguousKey},
		{name: "text binary", oid: oidText, format: 1, raw: []byte("acme"), want: "acme"},
		{name: "unknown binary with text hint", oid: 0, hint: HintText, format: 1, raw: []byte("12345678"), want: "12345678"},
		{name: "unsupported binary type", oid: 1700, format: 1, raw: be64(1), fails: true},
		{name: "unsupported text type", oid: 1700, raw: []byte("1.5"), fails: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := DecodeShardKey(c.oid, c.hint, c.format, c.raw)
			switch {
			case c.err != nil:
				if !errors.Is(err, c.err) {
					t.Fatalf("err = %v, want %v", err, c.err)
				}
			case c.fails:
				if err == nil {
					t.Fatalf("expected an error, got %v", got)
				}
			default:
				if err != nil || got != c.want {
					t.Fatalf("got %v (%T), %v; want %v (%T)", got, got, err, c.want, c.want)
				}
			}
		})
	}
}

func TestBindParamsFormatsAndNulls(t *testing.T) {
	b := BindParams{OIDs: []uint32{oidInt8, 0}, Formats: []int16{0, 1}, Values: [][]byte{[]byte("42"), be64(7)}}
	if v, err := b.ShardKey(1, HintNone); err != nil || v != int64(42) {
		t.Fatalf("$1 = %v, %v", v, err)
	}
	if v, err := b.ShardKey(2, HintNone); err != nil || v != int64(7) {
		t.Fatalf("$2 = %v, %v", v, err)
	}
	if _, err := b.ShardKey(3, HintNone); err == nil {
		t.Fatal("unbound parameter must fail")
	}
	if _, err := b.ShardKey(0, HintNone); err == nil {
		t.Fatal("$0 must fail")
	}
	all := BindParams{Formats: []int16{1}, Values: [][]byte{be64(1), be64(2)}}
	if v, err := all.ShardKey(2, HintNone); err != nil || v != int64(2) {
		t.Fatalf("single format applies to all: %v, %v", v, err)
	}
	null := BindParams{Values: [][]byte{nil}}
	if _, err := null.ShardKey(1, HintNone); err == nil {
		t.Fatal("NULL shard key must fail")
	}
}

func TestResolveUsesCastHints(t *testing.T) {
	snap := fixture(t)
	p := New()
	pl, err := p.Plan(context.Background(), session(snap), "select * from docs where slug = $1::text")
	if err != nil || !pl.Deferred {
		t.Fatalf("plan: %+v %v", pl, err)
	}
	got, err := pl.Resolve(BindParams{Values: [][]byte{[]byte("123")}})
	if err != nil {
		t.Fatal(err)
	}
	if w := shardOf(t, snap, "123"); len(got.Shards) != 1 || got.Shards[0] != w || got.ShardKeyValues[0] != "123" {
		t.Fatalf("resolved %+v, want shard %d of text 123", got, w)
	}
	pl, err = p.Plan(context.Background(), session(snap), "select * from docs where slug = $1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pl.Resolve(BindParams{Values: [][]byte{[]byte("123")}}); err == nil || !contains([]string{"0A000: parameter $1 cannot be a shard key: value is untyped and looks numeric: cast it to int8 or text"}, err.Error()) {
		t.Fatalf("ambiguous untyped parameter must be refused: %v", err)
	}
	pl, err = p.Plan(context.Background(), session(snap), "select * from orders where tenant_id = $1")
	if err != nil {
		t.Fatal(err)
	}
	got, err = pl.Resolve(BindParams{OIDs: []uint32{oidInt8}, Values: [][]byte{[]byte("42")}})
	if err != nil {
		t.Fatal(err)
	}
	if w := shardOf(t, snap, int64(42)); got.Shards[0] != w || got.Kind != EqualUnique || got.Deferred {
		t.Fatalf("resolved %+v, want shard %d", got, w)
	}
	again, err := got.Resolve(BindParams{})
	if err != nil || again.Shards[0] != got.Shards[0] {
		t.Fatalf("resolving a resolved plan must be a no-op: %+v %v", again, err)
	}
}

func TestPlanRefusesUnknownDatabaseDefaults(t *testing.T) {
	snap := fixture(t)
	db := snap.Databases[fixtureDB]
	db.DefaultPlacement = "sharded"
	snap.Databases[fixtureDB] = db
	_, err := New().Plan(context.Background(), session(snap), "select * from undeclared")
	if err == nil || !contains([]string{`0A000: table "undeclared" is not declared in the catalog and the database defaults to sharded placement`}, err.Error()) {
		t.Fatalf("got %v", err)
	}
	db.DefaultPlacement = "reference"
	snap.Databases[fixtureDB] = db
	pl, err := New().Plan(context.Background(), session(snap), "select * from undeclared")
	if err != nil || pl.Kind != Reference {
		t.Fatalf("reference default: %+v %v", pl, err)
	}
	pl, err = New().Plan(context.Background(), Session{Database: fixtureDB}, "select * from orders where tenant_id = 1")
	if err != nil || pl.Kind != Unsharded || pl.Generation != 0 {
		t.Fatalf("nil snapshot plans onto home: %+v %v", pl, err)
	}
}

func TestSearchPathAndReferenceSpread(t *testing.T) {
	snap := fixture(t)
	sess := session(snap)
	sess.SearchPath = []string{"audit", "public"}
	pl, err := New().Plan(context.Background(), sess, "select * from events where tenant_id = 1")
	if err != nil || pl.Kind != EqualUnique {
		t.Fatalf("search_path lookup: %+v %v", pl, err)
	}
	seen := map[int32]bool{}
	for id := uint64(0); id < 8; id++ {
		sess := session(snap)
		sess.ID = id
		pl, err := New().Plan(context.Background(), sess, "select * from regions")
		if err != nil || pl.Kind != Reference || len(pl.Shards) != 1 {
			t.Fatalf("reference read: %+v %v", pl, err)
		}
		seen[pl.Shards[0]] = true
	}
	if len(seen) != 4 {
		t.Fatalf("reference reads used shards %v, want all four", seen)
	}
}

func TestKindString(t *testing.T) {
	if Scatter.String() != "Scatter" || Kind(99).String() != "Kind(99)" {
		t.Fatal("Kind.String")
	}
}

func TestSearchPathClassification(t *testing.T) {
	snap := fixture(t)
	cases := []struct {
		sql  string
		want []string
		err  string
	}{
		{sql: "set search_path = audit, public", want: []string{"audit", "public"}},
		{sql: `set search_path to "Audit", 'x, y'`, want: []string{"Audit", "x", "y"}},
		{sql: "set search_path to default", want: nil},
		{sql: "reset search_path", want: nil},
		{sql: "reset all", want: nil},
		{sql: "set local search_path = audit", err: "SET LOCAL search_path"},
		{sql: "set search_path from current", err: "FROM CURRENT"},
	}
	for _, c := range cases {
		pl, err := New().Plan(context.Background(), session(snap), c.sql)
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Fatalf("%s: err = %v, want %q", c.sql, err, c.err)
			}
			continue
		}
		if err != nil || !pl.Class.SetGUC {
			t.Fatalf("%s: %+v %v", c.sql, pl, err)
		}
		if !slices.Equal(pl.Class.SearchPath, c.want) || (pl.Class.SearchPath == nil) != (c.want == nil) {
			t.Fatalf("%s: search path %q, want %q", c.sql, pl.Class.SearchPath, c.want)
		}
	}
}
