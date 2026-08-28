// Package scatter merges the per-shard result streams of a multi-shard
// read: k-way ordered merge over PostgreSQL text- or binary-format values,
// LIMIT/OFFSET after the merge, and combination of distributive aggregates.
package scatter

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// PostgreSQL type OIDs the merger understands.
const (
	oidBool        = 16
	oidBytea       = 17
	oidChar        = 18
	oidName        = 19
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidText        = 25
	oidOid         = 26
	oidFloat4      = 700
	oidFloat8      = 701
	oidBpchar      = 1042
	oidVarchar     = 1043
	oidDate        = 1082
	oidTimestamp   = 1114
	oidTimestampTZ = 1184
	oidNumeric     = 1700
	oidUUID        = 2950
)

// Wire formats of a column, as in RowDescription.
const (
	FormatText   = 0
	FormatBinary = 1
)

// class orders the special values PostgreSQL puts around the finite ones:
// -Infinity < finite < Infinity < NaN.
type class int8

const (
	classNegInf class = -1
	classFinite class = 0
	classPosInf class = 1
	classNaN    class = 2
)

// value is a decoded column value of one of the supported families.
type value struct {
	class class
	i     int64
	f     float64
	rat   *big.Rat
	scale int
	b     []byte
}

// family groups types that share a decoder and comparator.
type family int

const (
	famNone family = iota
	famInt
	famFloat
	famNumeric
	famBool
	famDate
	famTimestamp
	famUUID
	famBytes
	famTextCollated
)

func familyOf(oid uint32) family {
	switch oid {
	case oidInt2, oidInt4, oidInt8, oidOid:
		return famInt
	case oidFloat4, oidFloat8:
		return famFloat
	case oidNumeric:
		return famNumeric
	case oidBool:
		return famBool
	case oidDate:
		return famDate
	case oidTimestamp, oidTimestampTZ:
		return famTimestamp
	case oidUUID:
		return famUUID
	case oidBytea, oidChar:
		return famBytes
	case oidText, oidVarchar, oidBpchar, oidName:
		return famTextCollated
	}
	return famNone
}

// Comparator orders two non-NULL values of one column.
type Comparator func(a, b []byte) (int, error)

// ComparatorFor returns the comparator for a column of type oid in the given
// wire format; text types are ordered bytewise and need cCollation,
// everything else is decoded.
func ComparatorFor(oid uint32, format int16, cCollation bool) (Comparator, error) {
	fam := familyOf(oid)
	switch fam {
	case famNone:
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "multi-shard ORDER BY on a column of type oid %d is not available yet", oid)
		err.Hint = "order by an integer, numeric, float, boolean, date, timestamp, uuid, bytea or COLLATE \"C\" text column"
		return nil, err
	case famTextCollated:
		if !cCollation {
			err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "multi-shard ORDER BY on a text column needs an explicit COLLATE \"C\"")
			err.Hint = "the router orders text bytewise only; write ORDER BY col COLLATE \"C\", or filter on one shard key value"
			return nil, err
		}
	}
	dec := decoderFor(fam, oid, format)
	return func(a, b []byte) (int, error) {
		x, err := dec(a)
		if err != nil {
			return 0, err
		}
		y, err := dec(b)
		if err != nil {
			return 0, err
		}
		return compareValues(fam, x, y), nil
	}, nil
}

func compareValues(fam family, x, y value) int {
	if x.class != y.class {
		return cmp3(int(x.class), int(y.class))
	}
	if x.class != classFinite {
		return 0
	}
	switch fam {
	case famInt, famBool, famDate, famTimestamp:
		return cmp3(x.i, y.i)
	case famFloat:
		return cmp3(x.f, y.f)
	case famNumeric:
		return x.rat.Cmp(y.rat)
	}
	return bytes.Compare(x.b, y.b)
}

type decoder func([]byte) (value, error)

func decoderFor(fam family, oid uint32, format int16) decoder {
	bin := format == FormatBinary
	switch fam {
	case famInt:
		if bin {
			return decodeIntBinary
		}
		return decodeIntText
	case famFloat:
		if bin {
			return decodeFloatBinary
		}
		return decodeFloatText
	case famNumeric:
		if bin {
			return decodeNumericBinary
		}
		return decodeNumericText
	case famBool:
		if bin {
			return decodeBoolBinary
		}
		return decodeBoolText
	case famDate:
		if bin {
			return decodeDateBinary
		}
		return decodeDateText
	case famTimestamp:
		if bin {
			return decodeTimestampBinary
		}
		return func(v []byte) (value, error) { return decodeTimestampText(v, oid == oidTimestampTZ) }
	case famUUID:
		if bin {
			return decodeRaw
		}
		return decodeUUIDText
	case famBytes:
		if bin || oid != oidBytea {
			return decodeRaw
		}
		return decodeByteaText
	}
	return decodeRaw
}

func decodeRaw(v []byte) (value, error) { return value{b: v}, nil }

func decodeIntText(v []byte) (value, error) {
	i, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		return value{}, badValue("integer", v)
	}
	return value{i: i}, nil
}

func decodeIntBinary(v []byte) (value, error) {
	switch len(v) {
	case 2:
		return value{i: int64(int16(binary.BigEndian.Uint16(v)))}, nil
	case 4:
		return value{i: int64(int32(binary.BigEndian.Uint32(v)))}, nil
	case 8:
		return value{i: int64(binary.BigEndian.Uint64(v))}, nil
	}
	return value{}, badValue("binary integer", v)
}

func decodeFloatText(v []byte) (value, error) {
	s := string(v)
	switch s {
	case "NaN":
		return value{class: classNaN}, nil
	case "Infinity":
		return value{class: classPosInf}, nil
	case "-Infinity":
		return value{class: classNegInf}, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return value{}, badValue("float", v)
	}
	return floatValue(f), nil
}

func decodeFloatBinary(v []byte) (value, error) {
	switch len(v) {
	case 4:
		return floatValue(float64(math.Float32frombits(binary.BigEndian.Uint32(v)))), nil
	case 8:
		return floatValue(math.Float64frombits(binary.BigEndian.Uint64(v))), nil
	}
	return value{}, badValue("binary float", v)
}

func floatValue(f float64) value {
	switch {
	case math.IsNaN(f):
		return value{class: classNaN}
	case math.IsInf(f, 1):
		return value{class: classPosInf}
	case math.IsInf(f, -1):
		return value{class: classNegInf}
	}
	return value{f: f}
}

func decodeNumericText(v []byte) (value, error) {
	s := string(v)
	switch s {
	case "NaN":
		return value{class: classNaN}, nil
	case "Infinity":
		return value{class: classPosInf}, nil
	case "-Infinity":
		return value{class: classNegInf}, nil
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok || strings.ContainsAny(s, "eE/") {
		return value{}, badValue("numeric", v)
	}
	scale := 0
	if i := strings.IndexByte(s, '.'); i >= 0 {
		scale = len(s) - i - 1
	}
	return value{rat: r, scale: scale}, nil
}

var typeMap = pgtype.NewMap()

func decodeNumericBinary(v []byte) (value, error) {
	var n pgtype.Numeric
	if err := typeMap.Scan(oidNumeric, FormatBinary, v, &n); err != nil {
		return value{}, badValue("binary numeric", v)
	}
	switch {
	case n.NaN:
		return value{class: classNaN}, nil
	case n.InfinityModifier == pgtype.Infinity:
		return value{class: classPosInf}, nil
	case n.InfinityModifier == pgtype.NegativeInfinity:
		return value{class: classNegInf}, nil
	}
	r := new(big.Rat).SetInt(n.Int)
	scale := 0
	if n.Exp < 0 {
		scale = int(-n.Exp)
		r.Quo(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)))
	} else if n.Exp > 0 {
		r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil)))
	}
	return value{rat: r, scale: scale}, nil
}

func decodeBoolText(v []byte) (value, error) {
	switch string(v) {
	case "t":
		return value{i: 1}, nil
	case "f":
		return value{i: 0}, nil
	}
	return value{}, badValue("boolean", v)
}

func decodeBoolBinary(v []byte) (value, error) {
	if len(v) != 1 {
		return value{}, badValue("binary boolean", v)
	}
	if v[0] != 0 {
		return value{i: 1}, nil
	}
	return value{i: 0}, nil
}

const pgEpochUnix = 946684800

func decodeDateText(v []byte) (value, error) {
	switch string(v) {
	case "infinity":
		return value{class: classPosInf}, nil
	case "-infinity":
		return value{class: classNegInf}, nil
	}
	s, bc := splitBC(string(v))
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return value{}, badValue("date", v)
	}
	t = applyBC(t, bc)
	return value{i: (t.Unix() - pgEpochUnix) / 86400}, nil
}

func decodeDateBinary(v []byte) (value, error) {
	if len(v) != 4 {
		return value{}, badValue("binary date", v)
	}
	d := int32(binary.BigEndian.Uint32(v))
	switch d {
	case math.MaxInt32:
		return value{class: classPosInf}, nil
	case math.MinInt32:
		return value{class: classNegInf}, nil
	}
	return value{i: int64(d)}, nil
}

// decodeTimestampText reads PostgreSQL ISO output: "2006-01-02 15:04:05[.ffffff]"
// with a "+hh[:mm[:ss]]" zone for timestamptz, into microseconds since 2000.
func decodeTimestampText(v []byte, tz bool) (value, error) {
	switch string(v) {
	case "infinity":
		return value{class: classPosInf}, nil
	case "-infinity":
		return value{class: classNegInf}, nil
	}
	s, bc := splitBC(string(v))
	layouts := []string{"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"}
	if tz {
		layouts = []string{"2006-01-02 15:04:05.999999999-07:00:00", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999-07",
			"2006-01-02 15:04:05-07:00:00", "2006-01-02 15:04:05-07:00", "2006-01-02 15:04:05-07"}
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			t = applyBC(t, bc)
			return value{i: (t.Unix()-pgEpochUnix)*1e6 + int64(t.Nanosecond())/1e3}, nil
		}
	}
	return value{}, badValue("timestamp", v)
}

func decodeTimestampBinary(v []byte) (value, error) {
	if len(v) != 8 {
		return value{}, badValue("binary timestamp", v)
	}
	us := int64(binary.BigEndian.Uint64(v))
	switch us {
	case math.MaxInt64:
		return value{class: classPosInf}, nil
	case math.MinInt64:
		return value{class: classNegInf}, nil
	}
	return value{i: us}, nil
}

func splitBC(s string) (string, bool) {
	if strings.HasSuffix(s, " BC") {
		return strings.TrimSuffix(s, " BC"), true
	}
	return s, false
}

// applyBC maps "yyyy BC" onto the proleptic year 1-yyyy so ordering holds.
func applyBC(t time.Time, bc bool) time.Time {
	if !bc {
		return t
	}
	return t.AddDate(1-2*t.Year(), 0, 0)
}

func decodeUUIDText(v []byte) (value, error) {
	s := strings.ReplaceAll(string(v), "-", "")
	out, err := hex.DecodeString(s)
	if err != nil || len(out) != 16 {
		return value{}, badValue("uuid", v)
	}
	return value{b: out}, nil
}

func decodeByteaText(v []byte) (value, error) {
	if bytes.HasPrefix(v, []byte(`\x`)) {
		out, err := hex.DecodeString(string(v[2:]))
		if err != nil {
			return value{}, badValue("bytea", v)
		}
		return value{b: out}, nil
	}
	return value{b: v}, nil
}

func cmp3[T int | int64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func badValue(typ string, v []byte) error {
	return pgwire.Errorf(pgwire.CodeInternalError, "router: cannot read %s value %q from a shard", typ, string(v))
}

// Column describes one column of the shard rows.
type Column struct {
	TypeOID uint32
	Format  int16
}

// RowComparator orders whole rows by the sort keys.
type RowComparator struct {
	keys  []plan.SortKey
	comps []Comparator
}

// NewRowComparator resolves the comparators for keys against the columns of
// the shard rows. hidden is how many columns the router appended, which is
// what places a key the planner could only position relative to the end.
func NewRowComparator(keys []plan.SortKey, cols []Column, hidden int) (*RowComparator, error) {
	rc := &RowComparator{}
	for _, k := range keys {
		k.Column, k.FromHidden = k.Index(len(cols), hidden), false
		rc.keys = append(rc.keys, k)
		if k.Column < 0 || k.Column >= len(cols) {
			return nil, pgwire.Errorf(pgwire.CodeInternalError, "router: sort key column %d outside the %d-column shard row", k.Column, len(cols))
		}
		c, err := ComparatorFor(cols[k.Column].TypeOID, cols[k.Column].Format, k.CCollation)
		if err != nil {
			return nil, err
		}
		rc.comps = append(rc.comps, c)
	}
	return rc, nil
}

// Compare orders a before b (negative), after (positive) or equal (0);
// NULLs sort where the key says.
func (rc *RowComparator) Compare(a, b [][]byte) (int, error) {
	for i, k := range rc.keys {
		x, y := a[k.Column], b[k.Column]
		switch {
		case x == nil && y == nil:
			continue
		case x == nil:
			return nullOrder(k), nil
		case y == nil:
			return -nullOrder(k), nil
		}
		c, err := rc.comps[i](x, y)
		if err != nil {
			return 0, err
		}
		if c != 0 {
			if k.Desc {
				c = -c
			}
			return c, nil
		}
	}
	return 0, nil
}

func nullOrder(k plan.SortKey) int {
	if k.NullsFirst {
		return -1
	}
	return 1
}

// FormatFloat renders a float8 the way PostgreSQL's float8out does with the
// default extra_float_digits: shortest round-trip digits, exponent form
// outside [1e-4, 1e15).
func FormatFloat(f float64) string { return formatFloatBits(f, 64) }

// formatFloatBits is FormatFloat for float4 (bits 32) or float8 (bits 64).
func formatFloatBits(f float64, bits int) string {
	if bits == 32 {
		f = float64(float32(f))
	}
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case f == 0:
		if math.Signbit(f) {
			return "-0"
		}
		return "0"
	}
	mant, expStr, _ := strings.Cut(strconv.FormatFloat(f, 'e', -1, bits), "e")
	exp, _ := strconv.Atoi(expStr)
	if exp < -4 || exp >= 15 {
		sign := "+"
		if exp < 0 {
			sign, exp = "-", -exp
		}
		return fmt.Sprintf("%se%s%02d", mant, sign, exp)
	}
	return strconv.FormatFloat(f, 'f', -1, bits)
}

// formatNumeric renders a finite decimal with the given scale, or the
// special values, in PostgreSQL's text form.
func formatNumeric(v value) string {
	switch v.class {
	case classNaN:
		return "NaN"
	case classPosInf:
		return "Infinity"
	case classNegInf:
		return "-Infinity"
	}
	return v.rat.FloatString(v.scale)
}

// encodeNumericBinary renders v in the numeric binary wire format.
func encodeNumericBinary(v value) ([]byte, error) {
	n := pgtype.Numeric{Valid: true}
	switch v.class {
	case classNaN:
		n.NaN = true
	case classPosInf:
		n.InfinityModifier = pgtype.Infinity
	case classNegInf:
		n.InfinityModifier = pgtype.NegativeInfinity
	default:
		scaled := new(big.Rat).Mul(v.rat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(v.scale)), nil)))
		if !scaled.IsInt() {
			return nil, pgwire.Errorf(pgwire.CodeInternalError, "router: numeric %s does not fit scale %d", v.rat.String(), v.scale)
		}
		n.Int, n.Exp = new(big.Int).Set(scaled.Num()), int32(-v.scale)
	}
	return typeMap.Encode(oidNumeric, FormatBinary, n, nil)
}
