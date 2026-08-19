// Package placement maps shard-key values to the int64 key space and the key
// space to shards. The hash is a bit-exact port of the PostgreSQL extended
// hash functions used for hash partitioning (src/common/hashfn.c and
// src/backend/access/hash/hashfunc.c), so a value hashes to the same key
// space position here and inside PostgreSQL.
package placement

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// PartitionSeed is PostgreSQL's HASH_PARTITION_SEED (0x7A5B22367996DCFD).
const PartitionSeed uint64 = 8816678312871386365

const (
	golden    = 0x9e3779b9
	pgHashMix = 3923095
)

func mix(a, b, c uint32) (uint32, uint32, uint32) {
	a -= c
	a ^= bits.RotateLeft32(c, 4)
	c += b
	b -= a
	b ^= bits.RotateLeft32(a, 6)
	a += c
	c -= b
	c ^= bits.RotateLeft32(b, 8)
	b += a
	a -= c
	a ^= bits.RotateLeft32(c, 16)
	c += b
	b -= a
	b ^= bits.RotateLeft32(a, 19)
	a += c
	c -= b
	c ^= bits.RotateLeft32(b, 4)
	b += a
	return a, b, c
}

func final(a, b, c uint32) (uint32, uint32) {
	c ^= b
	c -= bits.RotateLeft32(b, 14)
	a ^= c
	a -= bits.RotateLeft32(c, 11)
	b ^= a
	b -= bits.RotateLeft32(a, 25)
	c ^= b
	c -= bits.RotateLeft32(b, 16)
	a ^= c
	a -= bits.RotateLeft32(c, 4)
	b ^= a
	b -= bits.RotateLeft32(a, 14)
	c ^= b
	c -= bits.RotateLeft32(b, 24)
	return b, c
}

// HashBytesExtended is hash_bytes_extended: Jenkins lookup3 over k with a
// 64-bit seed, little-endian word order as on every platform pgshard runs on.
func HashBytesExtended(k []byte, seed uint64) uint64 {
	n := uint32(len(k))
	a := golden + n + pgHashMix
	b, c := a, a
	if seed != 0 {
		a += uint32(seed >> 32)
		b += uint32(seed)
		a, b, c = mix(a, b, c)
	}
	for len(k) >= 12 {
		a += binary.LittleEndian.Uint32(k[0:4])
		b += binary.LittleEndian.Uint32(k[4:8])
		c += binary.LittleEndian.Uint32(k[8:12])
		a, b, c = mix(a, b, c)
		k = k[12:]
	}
	switch len(k) {
	case 11:
		c += uint32(k[10]) << 24
		fallthrough
	case 10:
		c += uint32(k[9]) << 16
		fallthrough
	case 9:
		c += uint32(k[8]) << 8
		fallthrough
	case 8:
		b += uint32(k[7]) << 24
		fallthrough
	case 7:
		b += uint32(k[6]) << 16
		fallthrough
	case 6:
		b += uint32(k[5]) << 8
		fallthrough
	case 5:
		b += uint32(k[4])
		fallthrough
	case 4:
		a += uint32(k[3]) << 24
		fallthrough
	case 3:
		a += uint32(k[2]) << 16
		fallthrough
	case 2:
		a += uint32(k[1]) << 8
		fallthrough
	case 1:
		a += uint32(k[0])
	}
	b, c = final(a, b, c)
	return uint64(b)<<32 | uint64(c)
}

// HashUint32Extended is hash_bytes_uint32_extended.
func HashUint32Extended(k uint32, seed uint64) uint64 {
	a := uint32(golden + 4 + pgHashMix)
	b, c := a, a
	if seed != 0 {
		a += uint32(seed >> 32)
		b += uint32(seed)
		a, b, c = mix(a, b, c)
	}
	a += k
	b, c = final(a, b, c)
	return uint64(b)<<32 | uint64(c)
}

// HashInt8Extended is hashint8extended (also timestamp_hash_extended).
func HashInt8Extended(v int64, seed uint64) int64 {
	lo := uint32(v)
	hi := uint32(uint64(v) >> 32)
	if v >= 0 {
		lo ^= hi
	} else {
		lo ^= ^hi
	}
	return int64(HashUint32Extended(lo, seed))
}

// HashInt4Extended is hashint4extended.
func HashInt4Extended(v int32, seed uint64) int64 {
	return int64(HashUint32Extended(uint32(v), seed))
}

// HashInt2Extended is hashint2extended.
func HashInt2Extended(v int16, seed uint64) int64 {
	return int64(HashUint32Extended(uint32(int32(v)), seed))
}

// HashCharExtended is hashcharextended for the one-byte "char" type.
func HashCharExtended(v byte, seed uint64) int64 {
	return int64(HashUint32Extended(uint32(int32(int8(v))), seed))
}

// HashTextExtended is hashtextextended under a deterministic collation: the
// hash of the text's bytes.
func HashTextExtended(v string, seed uint64) int64 {
	return int64(HashBytesExtended([]byte(v), seed))
}

// HashUUIDExtended is uuid_hash_extended over the 16 raw bytes.
func HashUUIDExtended(v [16]byte, seed uint64) int64 {
	return int64(HashBytesExtended(v[:], seed))
}

// RawBytes is a shard-key value already reduced to the bytes PostgreSQL's
// hash_any_extended would see (bytea, or a text under a deterministic
// collation supplied by the caller).
type RawBytes []byte

// KeyspaceID hashes a shard-key value with PartitionSeed to its position in
// the int64 key space. Supported: int8/int16/int32/int64/int, string,
// [16]byte (uuid) and RawBytes. Numeric and float keys are refused: their
// PostgreSQL hashes depend on the value's internal representation.
func KeyspaceID(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return HashInt8Extended(x, PartitionSeed), nil
	case int:
		return HashInt8Extended(int64(x), PartitionSeed), nil
	case int32:
		return HashInt4Extended(x, PartitionSeed), nil
	case int16:
		return HashInt2Extended(x, PartitionSeed), nil
	case int8:
		return HashCharExtended(byte(x), PartitionSeed), nil
	case string:
		return HashTextExtended(x, PartitionSeed), nil
	case [16]byte:
		return HashUUIDExtended(x, PartitionSeed), nil
	case RawBytes:
		return int64(HashBytesExtended(x, PartitionSeed)), nil
	default:
		return 0, fmt.Errorf("placement: unsupported shard key type %T", v)
	}
}
