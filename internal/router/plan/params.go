package plan

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
// key. The declared parameter type wins; a cast in the statement text
// (hint) types an undeclared parameter; an undeclared text-format value
// that parses as an integer is refused as ambiguous.
func DecodeShardKey(oid uint32, hint TypeHint, format int16, raw []byte) (any, error) {
	if oid == 0 || oid == oidUnknown {
		switch hint {
		case HintInt:
			oid = oidInt8
		case HintText:
			oid = oidText
		}
	}
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
		case oidText, oidVarchar, oidBpchar, oidName:
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
	case oidText, oidVarchar, oidBpchar, oidName:
		return s, nil
	case 0, oidUnknown:
		if _, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			return nil, ErrAmbiguousKey
		}
		return s, nil
	}
	return nil, fmt.Errorf("parameter of type oid %d is not a supported shard key", oid)
}
