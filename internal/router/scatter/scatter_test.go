package scatter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

type sliceSource struct {
	rows [][][]byte
	i    int
	err  error
}

func (s *sliceSource) Next() ([][]byte, bool, error) {
	if s.i >= len(s.rows) {
		return nil, false, s.err
	}
	r := s.rows[s.i]
	s.i++
	return r, true, nil
}

func rows(vals ...string) [][][]byte {
	out := make([][][]byte, 0, len(vals))
	for _, v := range vals {
		out = append(out, row(v))
	}
	return out
}

func row(vals ...string) [][]byte {
	out := make([][]byte, len(vals))
	for i, v := range vals {
		if v != "NULL" {
			out[i] = []byte(v)
		}
	}
	return out
}

func sources(rs ...[][][]byte) []Source {
	out := make([]Source, len(rs))
	for i, r := range rs {
		out[i] = &sliceSource{rows: r}
	}
	return out
}

func textCols(oids ...uint32) []Column {
	out := make([]Column, len(oids))
	for i, o := range oids {
		out[i] = Column{TypeOID: o}
	}
	return out
}

func collect(t *testing.T, spec *plan.Merge, oids []uint32, srcs []Source) ([]string, int64, error) {
	t.Helper()
	var got []string
	n, err := Merge(spec, textCols(oids...), srcs, func(r [][]byte) error {
		parts := make([]string, len(r))
		for i, c := range r {
			if c == nil {
				parts[i] = "NULL"
			} else {
				parts[i] = string(c)
			}
		}
		got = append(got, strings.Join(parts, "|"))
		return nil
	})
	return got, n, err
}

func TestComparators(t *testing.T) {
	cases := []struct {
		oid  uint32
		a, b string
		want int
	}{
		{oidInt8, "9", "10", -1}, {oidInt4, "-3", "-3", 0}, {oidInt2, "5", "-7", 1},
		{oidFloat8, "1e3", "999.5", 1}, {oidFloat8, "NaN", "Infinity", 1}, {oidFloat8, "-Infinity", "-1e308", -1},
		{oidNumeric, "10.50", "10.5", 0}, {oidNumeric, "0.1", "0.10000000000000001", -1}, {oidNumeric, "NaN", "Infinity", 1}, {oidNumeric, "-Infinity", "-99999999999999999999", -1},
		{oidBool, "f", "t", -1}, {oidBool, "t", "t", 0},
		{oidDate, "2024-02-29", "2024-03-01", -1}, {oidDate, "0001-01-01 BC", "0001-01-01", -1}, {oidDate, "infinity", "9999-12-31", 1},
		{oidTimestamp, "2024-01-01 00:00:00.000001", "2024-01-01 00:00:00", 1},
		{oidTimestampTZ, "2024-01-01 01:00:00+01", "2024-01-01 00:00:00+00", 0}, {oidTimestampTZ, "2024-01-01 00:00:00.5+00", "2024-01-01 00:00:00-01", -1},
		{oidUUID, "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000010", -1}, {oidUUID, "ffffffff-ffff-ffff-ffff-ffffffffffff", "0fffffff-ffff-ffff-ffff-ffffffffffff", 1},
		{oidBytea, "\\x00ff", "\\x0100", -1},
	}
	for _, c := range cases {
		cmp, err := ComparatorFor(c.oid, FormatText, false)
		if err != nil {
			t.Fatalf("oid %d: %v", c.oid, err)
		}
		got, err := cmp([]byte(c.a), []byte(c.b))
		if err != nil || got != c.want {
			t.Errorf("oid %d: cmp(%q, %q) = %d, %v; want %d", c.oid, c.a, c.b, got, err, c.want)
		}
	}
	if _, err := ComparatorFor(oidText, FormatText, false); sqlstate(err) != pgwire.CodeFeatureNotSupported {
		t.Fatalf("text without COLLATE \"C\" must be refused, got %v", err)
	}
	cmp, err := ComparatorFor(oidVarchar, FormatText, true)
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := cmp([]byte("B"), []byte("a")); c >= 0 {
		t.Fatalf("C collation orders bytewise: got %d", c)
	}
	if _, err := ComparatorFor(3802, FormatText, false); sqlstate(err) != pgwire.CodeFeatureNotSupported {
		t.Fatalf("jsonb must be refused, got %v", err)
	}
	cmp, _ = ComparatorFor(oidInt4, FormatText, false)
	if _, err := cmp([]byte("x"), []byte("1")); err == nil {
		t.Fatal("garbage must be an error")
	}
}

func sqlstate(err error) string {
	var pe *pgwire.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestRowComparatorDirectionsAndNulls(t *testing.T) {
	oids := textCols(oidInt4, oidText)
	keyASC := []plan.SortKey{{Column: 0}}
	keyDESC := []plan.SortKey{{Column: 0, Desc: true, NullsFirst: true}}
	keyASCNullsFirst := []plan.SortKey{{Column: 0, NullsFirst: true}}
	for _, c := range []struct {
		keys []plan.SortKey
		a, b string
		want int
	}{
		{keyASC, "1", "2", -1}, {keyDESC, "1", "2", 1},
		{keyASC, "NULL", "2", 1}, {keyASC, "2", "NULL", -1}, {keyASC, "NULL", "NULL", 0},
		{keyDESC, "NULL", "2", -1}, {keyASCNullsFirst, "NULL", "2", -1},
	} {
		rc, err := NewRowComparator(c.keys, oids, 0)
		if err != nil {
			t.Fatal(err)
		}
		got, err := rc.Compare(row(c.a, "x"), row(c.b, "y"))
		if err != nil || got != c.want {
			t.Errorf("keys %+v: cmp(%s, %s) = %d, %v; want %d", c.keys, c.a, c.b, got, err, c.want)
		}
	}
	// Ties on the first key fall through to the second.
	rc, err := NewRowComparator([]plan.SortKey{{Column: 0}, {Column: 1, Desc: true, CCollation: true}}, oids, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := rc.Compare(row("1", "a"), row("1", "b")); got != 1 {
		t.Fatalf("second key DESC: got %d, want 1", got)
	}
	if _, err := NewRowComparator([]plan.SortKey{{Column: 1}}, oids, 0); sqlstate(err) != pgwire.CodeFeatureNotSupported {
		t.Fatalf("text key without C collation must be refused: %v", err)
	}
	if _, err := NewRowComparator([]plan.SortKey{{Column: 5}}, oids, 0); err == nil {
		t.Fatal("out-of-range key must be an error")
	}
}

func TestMergeOrderedStreamsAndTies(t *testing.T) {
	spec := &plan.Merge{OrderBy: []plan.SortKey{{Column: 0}}, Limit: -1, Offset: -1}
	got, n, err := collect(t, spec, []uint32{oidInt4, oidText}, sources(
		[][][]byte{row("1", "s0"), row("3", "s0"), row("NULL", "s0")},
		[][][]byte{row("2", "s1"), row("3", "s1")},
		[][][]byte{},
		[][][]byte{row("0", "s3"), row("3", "s3"), row("NULL", "s3")},
	))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"0|s3", "1|s0", "2|s1", "3|s0", "3|s1", "3|s3", "NULL|s0", "NULL|s3"}
	if strings.Join(got, ",") != strings.Join(want, ",") || n != 8 {
		t.Fatalf("merged %v (%d), want %v", got, n, want)
	}
	spec.OrderBy[0].Desc, spec.OrderBy[0].NullsFirst = true, true
	got, _, err = collect(t, spec, []uint32{oidInt4, oidText}, sources(
		[][][]byte{row("NULL", "s0"), row("3", "s0"), row("1", "s0")},
		[][][]byte{row("2", "s1")},
	))
	if err != nil {
		t.Fatal(err)
	}
	if want := "NULL|s0,3|s0,2|s1,1|s0"; strings.Join(got, ",") != want {
		t.Fatalf("desc merge %v, want %s", got, want)
	}
}

func TestMergeLimitOffsetAndHiddenColumns(t *testing.T) {
	spec := &plan.Merge{OrderBy: []plan.SortKey{{Column: 1}}, Hidden: 1, Limit: 3, Offset: 2}
	srcs := sources(
		[][][]byte{row("a", "1"), row("d", "4"), row("g", "7")},
		[][][]byte{row("b", "2"), row("e", "5")},
		[][][]byte{row("c", "3"), row("f", "6"), row("h", "8")},
	)
	got, n, err := collect(t, spec, []uint32{oidText, oidInt4}, srcs)
	if err != nil {
		t.Fatal(err)
	}
	if want := "c,d,e"; strings.Join(got, ",") != want || n != 3 {
		t.Fatalf("limit 3 offset 2 gave %v (%d), want %s", got, n, want)
	}
	// LIMIT 0 emits nothing; OFFSET past the end emits nothing.
	for _, spec := range []*plan.Merge{{Limit: 0, Offset: -1}, {Limit: -1, Offset: 100}, {Limit: 5, Offset: 100}} {
		got, n, err := collect(t, spec, []uint32{oidText, oidInt4}, sources([][][]byte{row("a", "1")}, [][][]byte{row("b", "2")}))
		if err != nil || len(got) != 0 || n != 0 {
			t.Fatalf("spec %+v gave %v (%d) %v", spec, got, n, err)
		}
	}
	// Concatenation keeps shard order and applies OFFSET/LIMIT across shards.
	spec = &plan.Merge{Limit: 2, Offset: 1}
	got, _, err = collect(t, spec, []uint32{oidText, oidInt4}, sources([][][]byte{row("a", "1"), row("b", "2")}, [][][]byte{row("c", "3")}))
	if err != nil || strings.Join(got, ",") != "b|2,c|3" {
		t.Fatalf("concat limit/offset gave %v %v", got, err)
	}
}

func TestMergeReportsSourceErrorsAndWidthMismatch(t *testing.T) {
	boom := errors.New("shard gone")
	spec := &plan.Merge{Limit: -1, Offset: -1}
	_, _, err := collect(t, spec, []uint32{oidInt4}, []Source{&sliceSource{rows: rows("1")}, &sliceSource{err: boom}})
	if !errors.Is(err, boom) {
		t.Fatalf("source error not surfaced: %v", err)
	}
	_, _, err = collect(t, spec, []uint32{oidInt4}, sources([][][]byte{row("1", "2")}))
	if err == nil {
		t.Fatal("a row wider than the description must be an error")
	}
}

func TestCombineAggregates(t *testing.T) {
	spec := &plan.Merge{Aggregates: []plan.AggFunc{plan.AggCount, plan.AggSum, plan.AggSum, plan.AggSum, plan.AggMin, plan.AggMax}, Limit: -1, Offset: -1}
	oids := []uint32{oidInt8, oidInt8, oidNumeric, oidFloat8, oidInt4, oidDate}
	got, n, err := collect(t, spec, oids, sources(
		[][][]byte{row("2", "10", "1.50", "0.5", "7", "2024-01-01")},
		[][][]byte{row("0", "NULL", "NULL", "NULL", "NULL", "NULL")},
		[][][]byte{row("3", "-4", "2.125", "1e20", "3", "2025-06-30")},
	))
	if err != nil {
		t.Fatal(err)
	}
	if want := "5|6|3.625|1e+20|3|2025-06-30"; len(got) != 1 || got[0] != want || n != 1 {
		t.Fatalf("combined %v (%d), want %s", got, n, want)
	}
	// All-NULL inputs stay NULL; count of nothing is 0.
	got, _, err = collect(t, &plan.Merge{Aggregates: []plan.AggFunc{plan.AggCount, plan.AggMax}, Limit: -1, Offset: -1}, []uint32{oidInt8, oidInt4},
		sources([][][]byte{row("0", "NULL")}, [][][]byte{row("0", "NULL")}))
	if err != nil || got[0] != "0|NULL" {
		t.Fatalf("all-NULL: %v %v", got, err)
	}
	// LIMIT/OFFSET apply to the single combined row.
	got, _, err = collect(t, &plan.Merge{Aggregates: []plan.AggFunc{plan.AggCount}, Limit: -1, Offset: 1}, []uint32{oidInt8}, sources([][][]byte{row("1")}))
	if err != nil || len(got) != 0 {
		t.Fatalf("offset past the aggregate row: %v %v", got, err)
	}
	// Text min/max and sums over unsupported types are refused.
	_, _, err = collect(t, &plan.Merge{Aggregates: []plan.AggFunc{plan.AggMax}, Limit: -1, Offset: -1}, []uint32{oidText}, sources([][][]byte{row("a")}))
	if sqlstate(err) != pgwire.CodeFeatureNotSupported {
		t.Fatalf("text max must be refused: %v", err)
	}
	_, _, err = collect(t, &plan.Merge{Aggregates: []plan.AggFunc{plan.AggSum}, Limit: -1, Offset: -1}, []uint32{1186}, sources([][][]byte{row("1 day")}))
	if sqlstate(err) != pgwire.CodeFeatureNotSupported {
		t.Fatalf("interval sum must be refused: %v", err)
	}
	// A shard returning no row or two rows is a protocol error.
	_, _, err = collect(t, &plan.Merge{Aggregates: []plan.AggFunc{plan.AggCount}, Limit: -1, Offset: -1}, []uint32{oidInt8}, sources([][][]byte{}))
	if err == nil {
		t.Fatal("missing aggregate row must be an error")
	}
	_, _, err = collect(t, &plan.Merge{Aggregates: []plan.AggFunc{plan.AggCount}, Limit: -1, Offset: -1}, []uint32{oidInt8}, sources(rows("1", "2")))
	if err == nil {
		t.Fatal("two aggregate rows must be an error")
	}
}

func TestNumericAndFloatFormatting(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"1", "1"}, {"0.5", "0.5"}, {"1e-5", "1e-05"}, {"123456789012345", "123456789012345"}, {"1e15", "1e+15"},
		{"-2.5e-7", "-2.5e-07"}, {"1e100", "1e+100"}, {"0.0001", "0.0001"}, {"NaN", "NaN"}, {"-Infinity", "-Infinity"},
	} {
		f, err := decodeFloatText([]byte(c.in))
		if err != nil {
			t.Fatal(err)
		}
		fl := f.f
		switch f.class {
		case classNaN:
			fl = math.NaN()
		case classPosInf:
			fl = math.Inf(1)
		case classNegInf:
			fl = math.Inf(-1)
		}
		if got := FormatFloat(fl); got != c.want {
			t.Errorf("FormatFloat(%s) = %s, want %s", c.in, got, c.want)
		}
	}
	if got := formatFloatBits(0.1, 32); got != "0.1" {
		t.Errorf("float4 0.1 formatted as %s", got)
	}
	acc := &numericSum{}
	for _, v := range []string{"1.5", "2.25", "-0.750"} {
		if err := acc.add([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := acc.result(); string(got) != "3.000" {
		t.Fatalf("numeric sum keeps the widest scale: %s", got)
	}
	acc = &numericSum{}
	_ = acc.add([]byte("Infinity"))
	_ = acc.add([]byte("-Infinity"))
	if got, _ := acc.result(); string(got) != "NaN" {
		t.Fatalf("Infinity + -Infinity = %s, want NaN", got)
	}
}

func binInt64(v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return b[:]
}

func binInt32(v int32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return b[:]
}

func binFloat64(f float64) []byte { return binInt64(int64(math.Float64bits(f))) }

func binNumeric(t *testing.T, s string) []byte {
	t.Helper()
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		t.Fatal(err)
	}
	out, err := typeMap.Encode(oidNumeric, FormatBinary, n, nil)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestBinaryFormatComparatorsAndAggregates(t *testing.T) {
	cases := []struct {
		oid  uint32
		a, b []byte
		want int
	}{
		{oidInt8, binInt64(-5), binInt64(3), -1},
		{oidInt4, binInt32(7), binInt32(7), 0},
		{oidInt2, []byte{0xff, 0xfe}, []byte{0x00, 0x01}, -1},
		{oidFloat8, binFloat64(2.5), binFloat64(-1e300), 1},
		{oidFloat8, binFloat64(math.NaN()), binFloat64(math.Inf(1)), 1},
		{oidNumeric, binNumeric(t, "10.50"), binNumeric(t, "10.5"), 0},
		{oidNumeric, binNumeric(t, "-0.001"), binNumeric(t, "0"), -1},
		{oidNumeric, binNumeric(t, "NaN"), binNumeric(t, "123456789012345678901234567890"), 1},
		{oidBool, []byte{0}, []byte{1}, -1},
		{oidDate, binInt32(-1), binInt32(0), -1},
		{oidDate, binInt32(math.MaxInt32), binInt32(1 << 30), 1},
		{oidTimestampTZ, binInt64(1_000_000), binInt64(999_999), 1},
		{oidTimestamp, binInt64(math.MinInt64), binInt64(math.MinInt64 + 1), -1},
		{oidUUID, []byte("0123456789abcdef"), []byte("0123456789abcdeg"), -1},
	}
	for _, c := range cases {
		cmp, err := ComparatorFor(c.oid, FormatBinary, false)
		if err != nil {
			t.Fatalf("oid %d: %v", c.oid, err)
		}
		got, err := cmp(c.a, c.b)
		if err != nil || got != c.want {
			t.Errorf("oid %d binary: got %d, %v; want %d", c.oid, got, err, c.want)
		}
	}
	// Text and binary encodings of the same timestamp agree.
	txt, _ := decodeTimestampText([]byte("2000-01-01 00:00:01.5+00"), true)
	if txt.i != 1_500_000 {
		t.Fatalf("timestamp text decodes to %d micros, want 1500000", txt.i)
	}
	d, _ := decodeDateText([]byte("2000-01-03"))
	if d.i != 2 {
		t.Fatalf("date text decodes to day %d, want 2", d.i)
	}
	// Aggregates in binary format produce binary results.
	spec := &plan.Merge{Aggregates: []plan.AggFunc{plan.AggCount, plan.AggSum, plan.AggSum, plan.AggMax}, Limit: -1, Offset: -1}
	cols := []Column{{oidInt8, FormatBinary}, {oidNumeric, FormatBinary}, {oidFloat8, FormatBinary}, {oidTimestampTZ, FormatBinary}}
	var got [][]byte
	_, err := Merge(spec, cols, sources(
		[][][]byte{{binInt64(2), binNumeric(t, "1.5"), binFloat64(0.25), binInt64(10)}},
		[][][]byte{{binInt64(3), binNumeric(t, "2.250"), binFloat64(0.5), binInt64(-4)}},
	), func(r [][]byte) error { got = r; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[0], binInt64(5)) || !bytes.Equal(got[2], binFloat64(0.75)) || !bytes.Equal(got[3], binInt64(10)) {
		t.Fatalf("binary aggregates: %x", got)
	}
	var n pgtype.Numeric
	if err := typeMap.Scan(oidNumeric, FormatBinary, got[1], &n); err != nil {
		t.Fatal(err)
	}
	if n.Int.String() != "3750" || n.Exp != -3 {
		t.Fatalf("binary numeric sum %s e%d, want 3750 e-3 (3.750)", n.Int, n.Exp)
	}
	// An int8 overflow of the combined count is 22003.
	_, err = Merge(&plan.Merge{Aggregates: []plan.AggFunc{plan.AggCount}, Limit: -1, Offset: -1}, []Column{{oidInt8, FormatText}},
		sources(rows("9223372036854775807"), rows("1")), func([][]byte) error { return nil })
	if sqlstate(err) != "22003" {
		t.Fatalf("overflow: %v", err)
	}
}

// TestMergeHiddenSortKeysFollowTheRealRowWidth: a hidden sort key was a
// position in the parsed select list, so a star -- whose width only the
// shard knows -- merged on an unrelated column and returned misordered
// rows, and with LIMIT the wrong ones.
func TestMergeHiddenSortKeysFollowTheRealRowWidth(t *testing.T) {
	// SELECT *, created_at AS __pgshard_sort_0 against a three-column
	// table: the key travels last, whatever the star expanded to.
	spec := &plan.Merge{OrderBy: []plan.SortKey{{Column: 0, FromHidden: true}}, Hidden: 1, Limit: 2, Offset: -1}
	got, n, err := collect(t, spec, []uint32{oidInt4, oidText, oidText, oidInt4}, sources(
		[][][]byte{row("1", "a", "x", "30"), row("4", "d", "x", "60")},
		[][][]byte{row("2", "b", "y", "10"), row("5", "e", "y", "50")},
		[][][]byte{row("3", "c", "z", "20"), row("6", "f", "z", "40")},
	))
	if err != nil {
		t.Fatal(err)
	}
	if want := "2|b|y,3|c|z"; strings.Join(got, ",") != want || n != 2 {
		t.Fatalf("merged %v (%d), want %s", got, n, want)
	}
}

// TestCompareKeysMatchesCompare: the merge heap compares decoded keys now
// rather than raw bytes, so the two have to order every pair the same way
// -- including NULLs, which are recorded rather than decoded, and DESC,
// which flips the comparison but not where a NULL sits.
func TestCompareKeysMatchesCompare(t *testing.T) {
	cols := []Column{{TypeOID: 23}, {TypeOID: 25}}
	rows := [][][]byte{
		{[]byte("1"), []byte("a")},
		{[]byte("2"), []byte("a")},
		{[]byte("1"), []byte("b")},
		{[]byte("1"), nil},
		{nil, []byte("a")},
		{nil, nil},
		{[]byte("-3"), []byte("")},
	}
	for _, desc := range []bool{false, true} {
		for _, nullsFirst := range []bool{false, true} {
			keys := []plan.SortKey{
				{Column: 0, Desc: desc, NullsFirst: nullsFirst, CCollation: true},
				{Column: 1, Desc: desc, NullsFirst: nullsFirst, CCollation: true},
			}
			rc, err := NewRowComparator(keys, cols, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range rows {
				for _, b := range rows {
					want, err := rc.Compare(a, b)
					if err != nil {
						t.Fatal(err)
					}
					ka, err := rc.Key(a)
					if err != nil {
						t.Fatal(err)
					}
					kb, err := rc.Key(b)
					if err != nil {
						t.Fatal(err)
					}
					got := rc.CompareKeys(ka, kb)
					if (got < 0) != (want < 0) || (got > 0) != (want > 0) {
						t.Fatalf("desc=%v nullsFirst=%v rows %v vs %v: keys say %d, bytes say %d", desc, nullsFirst, a, b, got, want)
					}
				}
			}
		}
	}
}

// BenchmarkOrderedMerge measures a merge over many shards ordered by a
// numeric column, which is the case where decoding costs most: arbitrary
// precision parsing, once per comparison before, once per row now.
func BenchmarkOrderedMerge(b *testing.B) {
	const shards, perShard = 64, 50
	cols := []Column{{TypeOID: 1700}}
	keys := []plan.SortKey{{Column: 0, CCollation: true}}
	spec := &plan.Merge{OrderBy: keys, Limit: -1}
	b.ReportAllocs()
	for b.Loop() {
		sources := make([]Source, shards)
		for s := range shards {
			rows := make([][][]byte, perShard)
			for i := range rows {
				rows[i] = [][]byte{[]byte(fmt.Sprintf("%d.%09d", s*perShard+i, i))}
			}
			sources[s] = &sliceSource{rows: rows}
		}
		n, err := Merge(spec, cols, sources, func([][]byte) error { return nil })
		if err != nil || n != shards*perShard {
			b.Fatalf("merged %d: %v", n, err)
		}
	}
}
