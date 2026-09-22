package plan

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// Every case was answered by PostgreSQL 18's int8 input function
// (pg_input_is_valid / ::int8) before it was written here; "" means the
// server refused it.
func TestIntegerKeysAreReadAsPostgreSQLReadsThem(t *testing.T) {
	cases := map[string]any{
		"16": int64(16), "0x10": int64(16), "0X1f": int64(31), "0o17": int64(15), "0O17": int64(15),
		"0b101": int64(5), "0B11": int64(3), "1_000": int64(1000), "0x_1": int64(1),
		"-0x10": int64(-16), "+0x10": int64(16), " 42 ": int64(42), "\t7\n": int64(7),
		"-9223372036854775808": int64(-9223372036854775808), "9223372036854775807": int64(9223372036854775807),
		"0x7fffffffffffffff": int64(9223372036854775807), "-0x8000000000000000": int64(-9223372036854775808),
		"0012": int64(12), "-0": int64(0), "0_1": int64(1), "0x1_2_3": int64(291), "0b1_0": int64(2), "0o1_7": int64(15),
		"1__0": "", "_1": "", "1_": "", "0x": "", "0b": "", "0o": "",
		"9223372036854775808": "", "-9223372036854775809": "", "0x8000000000000000": "",
		"0b2": "", "0o8": "", "0xg": "", "": "", "  ": "", "-": "", "+": "", "1e3": "", "1.0": "", "1 2": "",
	}
	for in, want := range cases {
		got, err := pgStrToInt64(in)
		if w, ok := want.(int64); ok {
			if err != nil || got != w {
				t.Fatalf("%q: %d %v, want %d", in, got, err, w)
			}
			continue
		}
		if err == nil {
			t.Fatalf("%q: read as %d, PostgreSQL refuses it", in, got)
		}
	}
}

// A key bound in text as 0x10 is the integer 16 on the shard, so it has to
// route where 16 does.
func TestAHexKeyRoutesWhereItsValueDoes(t *testing.T) {
	a, err := DecodeShardKey(oidInt8, HintNone, 0, []byte("0x10"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := DecodeShardKey(oidInt8, HintNone, 0, []byte("16"))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("0x10 decoded as %v, 16 as %v", a, b)
	}
}

// A literal key is read as the shard reads it too. The unknown literal
// '0x10' compared with an int8 key is 16 to PostgreSQL; hashing it as the
// string "0x10" sent the statement to a shard that does not hold the row.
func TestAnIntegerLiteralKeyRoutesWhereItsValueDoes(t *testing.T) {
	shardsOf := func(sql string) []int32 {
		t.Helper()
		p, err := New().Plan(context.Background(), session(fixture(t)), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return p.Shards
	}
	for sql, same := range map[string]string{
		"select * from orders where tenant_id = '0x10'::int8":  "select * from orders where tenant_id = 16",
		"select * from orders where tenant_id = '1_6'::bigint": "select * from orders where tenant_id = 16",
		"select * from orders where tenant_id = 0x10":          "select * from orders where tenant_id = 16",
		"select * from orders where tenant_id = 0x100000000":   "select * from orders where tenant_id = 4294967296",
	} {
		got, want := shardsOf(sql), shardsOf(same)
		if len(got) != 1 || !slices.Equal(got, want) {
			t.Fatalf("%s routes to %v, %s to %v", sql, got, same, want)
		}
	}
	for _, sql := range []string{
		"select * from orders where tenant_id = '0x10'",
		"select * from orders where tenant_id = '1_6'",
	} {
		_, err := New().Plan(context.Background(), session(fixture(t)), sql)
		if err == nil || !strings.Contains(err.Error(), "untyped and looks numeric") {
			t.Fatalf("%s: err = %v, want the numeric-literal refusal", sql, err)
		}
	}
}

func TestAnIntegerCastOfAParameterReadsPostgreSQLsSpellings(t *testing.T) {
	v, err := DecodeShardKey(oidText, HintInt, 0, []byte("0x10"))
	if err != nil || v != int64(16) {
		t.Fatalf("got %v %v, want 16", v, err)
	}
}

// An UNTYPED parameter spelled as any integer PostgreSQL reads -- 0x10 as
// much as 16 -- could be either kind of key, and is refused as ambiguous;
// declared as text it is a text key whatever it looks like.
func TestAnUntypedParameterSpelledAsAnIntegerIsAmbiguous(t *testing.T) {
	for _, raw := range []string{"16", "0x10", "1_000", "0b1"} {
		if _, err := DecodeShardKey(0, HintNone, 0, []byte(raw)); !errors.Is(err, ErrAmbiguousKey) {
			t.Fatalf("%q: err = %v, want ErrAmbiguousKey", raw, err)
		}
	}
	v, err := DecodeShardKey(oidText, HintNone, 0, []byte("0xabc"))
	if err != nil || v != "0xabc" {
		t.Fatalf("a text parameter 0xabc: %v %v", v, err)
	}
}
