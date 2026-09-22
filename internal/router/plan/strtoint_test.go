package plan

import "testing"

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
