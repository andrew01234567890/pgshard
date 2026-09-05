package plan

import (
	"strconv"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/placement"
)

// normaliseKey applies the normalisation the shard key column's own type
// applies, so the router hashes the value the shard will store rather than
// the value the client sent.
//
// character varying(n) is the case that diverges: PostgreSQL does not
// reject an overlength value whose excess is only spaces, it silently drops
// the excess (SQL demands exactly that). It then stores and hashes 'abc'
// where the client sent 'abc   ', and a router hashing the untrimmed bytes
// sends the row to one shard and every later lookup of it to another.
//
// An overlength value whose excess is not all spaces is left alone: that
// statement fails on the shard with 22001 whatever shard it reaches, and
// inventing a truncation PostgreSQL would not do would only route it
// somewhere less obvious.
func normaliseKey(v any, columnType string) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	base, n, hasLimit := parseCharType(columnType)
	// A uuid column is hashed by uuid_hash_extended over its SIXTEEN RAW
	// BYTES -- which is what every row filter, copy and re-key the
	// controller builds uses. Left as a string it reached HashTextExtended
	// over the 36 characters instead, so the router placed and found rows at
	// one keyspace position while the copy moved them to another: after a
	// reshard the row existed and the router looked on a different shard.
	// Measured on four shards, three of four sample uuids landed elsewhere.
	//
	// A value that does not parse is left alone: PostgreSQL will reject the
	// statement for invalid uuid syntax, so where it routed first does not
	// matter.
	if base == "uuid" {
		if u, ok := placement.ParseUUID(s); ok {
			return u
		}
		return v
	}
	// character(n) is stored blank-padded and compares with its trailing
	// spaces ignored, and the ::text cast the row filter and the copy use
	// strips them. Trimming here is what makes the router hash the value
	// the shard stored rather than the bytes the client sent: without it
	// 'abc' and 'abc  ' are the same key to PostgreSQL and two different
	// shards to pgshard.
	switch base {
	case "character", "bpchar", "char":
		return strings.TrimRight(s, " ")
	}
	if !hasLimit || base != "character varying" {
		return v
	}
	r := []rune(s)
	if len(r) <= n {
		return v
	}
	if strings.Trim(string(r[n:]), " ") != "" {
		return v
	}
	return string(r[:n])
}

// parseCharType splits format_type output such as "character varying(8)"
// into its base name and length. hasLimit is false for a type with no
// length, which normalises nothing.
func parseCharType(t string) (base string, n int, hasLimit bool) {
	t = strings.TrimSpace(strings.ToLower(t))
	open := strings.IndexByte(t, '(')
	if open < 0 || !strings.HasSuffix(t, ")") {
		return t, 0, false
	}
	n, err := strconv.Atoi(t[open+1 : len(t)-1])
	if err != nil || n < 0 {
		return strings.TrimSpace(t[:open]), 0, false
	}
	return strings.TrimSpace(t[:open]), n, true
}

// NormaliseKeyForType is normaliseKey for a string, exported so the
// differential test can ask PostgreSQL whether the router and a shard
// agree about a key. The hazard this normalisation exists for is a
// disagreement with PostgreSQL, and a test that cannot reach both sides
// cannot see one.
func NormaliseKeyForType(v, columnType string) string {
	s, _ := normaliseKey(v, columnType).(string)
	return s
}
