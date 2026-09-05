package placement

// ParseUUID reads PostgreSQL's text representation of a uuid into the 16
// bytes uuid_hash_extended hashes.
//
// It is a port of string_to_uuid (src/backend/utils/adt/uuid.c), not a
// canonical-form parser, because the router has to accept exactly what the
// shard accepts. PostgreSQL is more permissive than the 8-4-4-4-12 form
// suggests: braces are optional, and a hyphen may follow ANY even number of
// bytes rather than only the four canonical positions. A stricter parser
// would refuse values PostgreSQL routes and stores, and a looser one would
// hash something PostgreSQL rejects.
func ParseUUID(s string) ([16]byte, bool) {
	var u [16]byte
	i := 0
	braces := false
	if i < len(s) && s[i] == '{' {
		i++
		braces = true
	}
	for b := 0; b < 16; b++ {
		if i+1 >= len(s) {
			return u, false
		}
		hi, ok := hexVal(s[i])
		if !ok {
			return u, false
		}
		lo, ok := hexVal(s[i+1])
		if !ok {
			return u, false
		}
		u[b] = hi<<4 | lo
		i += 2
		// The hyphen rule is PostgreSQL's: allowed after an odd byte index
		// and never after the last, so "0189d0f44c1a-7c2e9c1a2f1b3c4d5e6f"
		// parses and "0189-d0f4..." does not.
		if i < len(s) && s[i] == '-' && b%2 == 1 && b < 15 {
			i++
		}
	}
	if braces {
		if i >= len(s) || s[i] != '}' {
			return u, false
		}
		i++
	}
	if i != len(s) {
		return u, false
	}
	return u, true
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
