package plan

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/placement"
)

// PostgreSQL type OIDs the router understands as shard keys.
const (
	oidInt8    = 20
	oidInt2    = 21
	oidInt4    = 23
	oidText    = 25
	oidBpchar  = 1042
	oidVarchar = 1043
	oidUnknown = 705
	oidName    = 19
	oidUUID    = 2950
)

// BindParams adapts one Bind message to Params: values are decoded from
// the client's declared parameter types and formats.
type BindParams struct {
	// OIDs are the types declared at Parse; missing or 0 means unknown.
	OIDs    []uint32
	Formats []int16
	Values  [][]byte
}

// ShardKey implements Params.
func (b BindParams) ShardKey(n int32, hint TypeHint) (any, error) {
	i := int(n) - 1
	if i < 0 || i >= len(b.Values) {
		return nil, fmt.Errorf("parameter $%d was not bound", n)
	}
	raw := b.Values[i]
	if raw == nil {
		return nil, errors.New("shard key must not be NULL")
	}
	var oid uint32
	if i < len(b.OIDs) {
		oid = b.OIDs[i]
	}
	var format int16
	switch len(b.Formats) {
	case 0:
	case 1:
		format = b.Formats[0]
	default:
		if i < len(b.Formats) {
			format = b.Formats[i]
		}
	}
	return DecodeShardKey(oid, hint, format, raw)
}

// ErrAmbiguousKey reports an untyped value that could be an int8 or a text
// shard key.
var ErrAmbiguousKey = errors.New("value is untyped and looks numeric: cast it to int8 or text")

// DecodeShardKey turns one bound parameter into an int64 or string shard
// key.
//
// Two separate things happen, in PostgreSQL's own order. The wire value is
// decoded with the type the client DECLARED at Parse, and then the cast in
// the statement text -- hint -- is applied to the decoded value, because
// that is the expression PostgreSQL evaluates before it compares.
//
// The cast used to be read only as a decoding hint for an undeclared
// parameter, so `WHERE tenant_id = $1::int8` with a text parameter bound to
// '7' hashed the STRING "7" while PostgreSQL compared the INTEGER 7 -- a
// different shard, and the row silently missing.
//
// An undeclared text-format value that parses as an integer is still
// refused as ambiguous.
func DecodeShardKey(oid uint32, hint TypeHint, format int16, raw []byte) (any, error) {
	if oid == 0 || oid == oidUnknown {
		switch hint {
		case HintInt:
			oid = oidInt8
		case HintText:
			oid = oidText
		case HintUUID:
			oid = oidUUID
		}
		// The cast has typed it; there is nothing left for the cast to do.
		hint = HintNone
	}
	v, err := decodeParam(oid, format, raw)
	if err != nil {
		return nil, err
	}
	return applyCast(v, hint)
}

// applyCast evaluates the statement's cast over the decoded value, which is
// what PostgreSQL does before the comparison. A value the cast cannot
// produce is an error rather than a guess: PostgreSQL would fail the
// statement too, and routing it somewhere first helps nobody.
func applyCast(v any, hint TypeHint) (any, error) {
	switch hint {
	case HintInt:
		switch x := v.(type) {
		case int64:
			return x, nil
		case string:
			i, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parameter %q cast to an integer: %w", x, err)
			}
			return i, nil
		}
	case HintText:
		switch x := v.(type) {
		case string:
			return x, nil
		case int64:
			return strconv.FormatInt(x, 10), nil
		case [16]byte:
			return uuidText(x), nil
		}
	case HintUUID:
		switch x := v.(type) {
		case [16]byte:
			return x, nil
		case string:
			b, ok := placement.ParseUUID(x)
			if !ok {
				return nil, fmt.Errorf("parameter %q cast to a uuid is not a uuid", x)
			}
			return b, nil
		}
	default:
		return v, nil
	}
	return nil, fmt.Errorf("parameter of type %T cannot be cast to a shard key of the statement's type", v)
}

// uuidText is a uuid cast to text: PostgreSQL's canonical 8-4-4-4-12
// lowercase form, which is what the comparison then hashes.
func uuidText(b [16]byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for i, c := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, hex[c>>4], hex[c&0x0f])
	}
	return string(out)
}

func decodeParam(oid uint32, format int16, raw []byte) (any, error) {
	if format == 1 {
		switch oid {
		case oidInt8:
			if len(raw) != 8 {
				return nil, fmt.Errorf("int8 parameter has %d bytes", len(raw))
			}
			return int64(binary.BigEndian.Uint64(raw)), nil
		case oidInt4:
			if len(raw) != 4 {
				return nil, fmt.Errorf("int4 parameter has %d bytes", len(raw))
			}
			return int64(int32(binary.BigEndian.Uint32(raw))), nil
		case oidInt2:
			if len(raw) != 2 {
				return nil, fmt.Errorf("int2 parameter has %d bytes", len(raw))
			}
			return int64(int16(binary.BigEndian.Uint16(raw))), nil
		case 0, oidUnknown:
			// Undeclared binary integers: pgx and libpq encode Go/C
			// integers this way; a binary text value is not distinguishable
			// from an int8 of the same length, so only 2/4/8 bytes are read.
			switch len(raw) {
			case 8:
				return int64(binary.BigEndian.Uint64(raw)), nil
			case 4:
				return int64(int32(binary.BigEndian.Uint32(raw))), nil
			case 2:
				return int64(int16(binary.BigEndian.Uint16(raw))), nil
			}
			return nil, ErrAmbiguousKey
		case oidUUID:
			// The binary wire form of a uuid IS the sixteen bytes the hash
			// takes, so there is nothing to parse.
			if len(raw) != 16 {
				return nil, fmt.Errorf("uuid parameter has %d bytes", len(raw))
			}
			var b [16]byte
			copy(b[:], raw)
			return b, nil
		case oidText, oidVarchar, oidName:
			return string(raw), nil
		case oidBpchar:
			// The trailing spaces come off where the column's declared
			// length is known: this decodes the value, normaliseKey trims
			// it by type. A bpchar parameter is not itself padded -- the
			// padding is the column's -- but a client may send one that
			// is, and PostgreSQL would compare it equal either way.
			return string(raw), nil
		}
		return nil, fmt.Errorf("binary parameter of type oid %d is not a supported shard key", oid)
	}
	s := string(raw)
	switch oid {
	case oidInt8, oidInt4, oidInt2:
		i, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("integer parameter %q: %w", s, err)
		}
		return i, nil
	case oidUUID:
		b, ok := placement.ParseUUID(s)
		if !ok {
			return nil, fmt.Errorf("uuid parameter %q is not a uuid", s)
		}
		return b, nil
	case oidText, oidVarchar, oidName:
		return s, nil
	case oidBpchar:
		return s, nil
	case 0, oidUnknown:
		if _, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			return nil, ErrAmbiguousKey
		}
		return s, nil
	}
	return nil, fmt.Errorf("parameter of type oid %d is not a supported shard key", oid)
}
