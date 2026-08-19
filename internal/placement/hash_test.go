package placement

import (
	"math"
	"testing"
)

func TestGoldensFromPostgres18(t *testing.T) {
	cases := []struct {
		v    int64
		seed uint64
		want int64
	}{
		{42, PartitionSeed, 7363975540656877951},
		{42, 0, 8010225493015854792},
		{-1, 0, -1888257769727981238},
	}
	for _, c := range cases {
		if got := HashInt8Extended(c.v, c.seed); got != c.want {
			t.Errorf("hashint8extended(%d,%d)=%d want %d", c.v, c.seed, got, c.want)
		}
	}
}

func TestKeyspaceIDTypes(t *testing.T) {
	want := HashInt8Extended(42, PartitionSeed)
	for _, v := range []any{int64(42), int(42)} {
		got, err := KeyspaceID(v)
		if err != nil || got != want {
			t.Errorf("KeyspaceID(%T %v)=%d,%v want %d", v, v, got, err, want)
		}
	}
	if got, _ := KeyspaceID(int32(42)); got != HashInt4Extended(42, PartitionSeed) {
		t.Errorf("int32 mismatch")
	}
	if got, _ := KeyspaceID(int16(-7)); got != HashInt2Extended(-7, PartitionSeed) {
		t.Errorf("int16 mismatch")
	}
	if got, _ := KeyspaceID(int8(-7)); got != HashCharExtended(0xf9, PartitionSeed) {
		t.Errorf("int8 mismatch")
	}
	if got, _ := KeyspaceID("hello"); got != HashTextExtended("hello", PartitionSeed) || got != int64(HashBytesExtended([]byte("hello"), PartitionSeed)) {
		t.Errorf("string mismatch")
	}
	var u [16]byte
	for i := range u {
		u[i] = byte(i * 17)
	}
	if got, _ := KeyspaceID(u); got != HashUUIDExtended(u, PartitionSeed) || got != int64(HashBytesExtended(u[:], PartitionSeed)) {
		t.Errorf("uuid mismatch")
	}
	if got, _ := KeyspaceID(RawBytes("hello")); got != HashTextExtended("hello", PartitionSeed) {
		t.Errorf("raw bytes mismatch")
	}
	if _, err := KeyspaceID(3.5); err == nil {
		t.Errorf("float accepted")
	}
	if _, err := KeyspaceID(nil); err == nil {
		t.Errorf("nil accepted")
	}
}

func TestInt8FoldMatchesInt4ForSmallValues(t *testing.T) {
	for _, v := range []int32{0, 1, -1, math.MaxInt32, math.MinInt32, 12345, -99999} {
		if HashInt8Extended(int64(v), PartitionSeed) != HashInt4Extended(v, PartitionSeed) {
			t.Errorf("int8/int4 hash differ for %d", v)
		}
	}
	if HashInt8Extended(1<<40, PartitionSeed) == HashInt4Extended(0, PartitionSeed) {
		t.Errorf("high half ignored")
	}
}

func TestSeedChangesResult(t *testing.T) {
	if HashBytesExtended([]byte("abc"), 0) == HashBytesExtended([]byte("abc"), 1) {
		t.Errorf("seed ignored")
	}
	if HashUint32Extended(7, 0) == HashUint32Extended(7, 1) {
		t.Errorf("seed ignored")
	}
}

func TestBytesTailLengths(t *testing.T) {
	seen := map[uint64]int{}
	for n := 0; n <= 25; n++ {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i + 1)
		}
		h := HashBytesExtended(b, PartitionSeed)
		if prev, ok := seen[h]; ok {
			t.Errorf("length %d collides with %d", n, prev)
		}
		seen[h] = n
		if n > 0 {
			b[n-1] ^= 0x80
			if HashBytesExtended(b, PartitionSeed) == h {
				t.Errorf("last byte of length %d ignored", n)
			}
		}
	}
}
