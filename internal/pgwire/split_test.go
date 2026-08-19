package pgwire

import (
	"errors"
	"testing"
)

func TestCountStatements(t *testing.T) {
	cases := []struct {
		sql  string
		want int
	}{
		{"", 0},
		{"   \n\t", 0},
		{";", 0},
		{"; ;", 0},
		{"-- only a comment", 0},
		{"/* block */", 0},
		{"select 1", 1},
		{"select 1;", 1},
		{"select 1; ", 1},
		{"select 1; select 2", 2},
		{"select 1;select 2;", 2},
		{"select ';'", 1},
		{"select 'it''s; fine'", 1},
		{`select "a;b"`, 1},
		{`select E'a\';b'`, 1},
		{`select e'\\'; select 2`, 2},
		{"select $$a;b$$", 1},
		{"select $tag$ ; $x$ ; $tag$", 1},
		{"select $tag$;$TAG$;$tag$", 1},
		{"select $1; select $2", 2},
		{"select a$b$c; select 2", 2},
		{"select $ 1; select 2", 2},
		{"select 1 -- ; select 2", 1},
		{"select 1 -- ;\n select 2", 1},
		{"select 1 -- x\n; select 2", 2},
		{"select 1 /* ; */", 1},
		{"select 1 /* /* nested ; */ ; */", 1},
		{"select 1 /* /* nested */ */; select 2", 2},
		{"select $_a1$;$_a1$", 1},
		{"do $body$ begin perform 1; perform 2; end $body$", 1},
		{"do $body$ begin perform 1; end $body$; select 2", 2},
		{"select U&'d\\0061t;a'", 1},
		{"select 1;\n-- trailing", 1},
	}
	for _, c := range cases {
		got, err := countStatements(c.sql)
		if err != nil || got != c.want {
			t.Errorf("countStatements(%q) = %d, %v; want %d", c.sql, got, err, c.want)
		}
	}
}

func TestCountStatementsUnterminated(t *testing.T) {
	for _, sql := range []string{"select '", "select 'a;", `select "x`, "select $$;", "select $t$ ; $u$", "select /* ;", "select E'\\'", "select $$"} {
		if _, err := countStatements(sql); !errors.Is(err, errUnterminated) {
			t.Errorf("countStatements(%q) err = %v, want errUnterminated", sql, err)
		}
	}
	for _, sql := range []string{"select $", "select $1", "select $1$", "select $9x$ ; $9x$"} {
		if _, err := countStatements(sql); err != nil {
			t.Errorf("countStatements(%q) err = %v, want nil", sql, err)
		}
	}
}

func FuzzCountStatements(f *testing.F) {
	for _, s := range []string{"select 1; select 2", "select $$;$$", "/* */ 'x''y' \"z\"", "E'\\''"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		n, err := countStatements(sql)
		if err == nil && n < 0 {
			t.Fatal("negative count")
		}
		if err != nil && n != 0 {
			t.Fatal("count with error")
		}
	})
}

func FuzzDecodeStartupPacket(f *testing.F) {
	f.Add([]byte{0, 3, 0, 0, 'u', 's', 'e', 'r', 0, 'x', 0, 0})
	f.Add([]byte{0, 3, 0, 2, 0})
	f.Add([]byte{0x04, 0xd2, 0x16, 0x2f})
	f.Add([]byte{0x04, 0xd2, 0x16, 0x2e, 0, 0, 0, 1, 1, 2, 3, 4})
	f.Fuzz(func(t *testing.T, body []byte) {
		p, err := decodeStartupPacket(body)
		if err == nil && p == nil {
			t.Fatal("nil packet without error")
		}
	})
}
