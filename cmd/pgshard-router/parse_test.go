package main

import (
	"bytes"
	"testing"
)

func TestParseSubcommand(t *testing.T) {
	var out, errb bytes.Buffer
	code := runParse([]string{"select 1; insert into t values (1)"}, &out, &errb)
	want := "grammar: postgresql 18\nfingerprint: fc8cb463b01dc6df\nstatements: 2\nkinds: SelectStmt,InsertStmt\n"
	if code != 0 || out.String() != want || errb.String() != "" {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errb.String())
	}
}

func TestParseSubcommandSyntaxError(t *testing.T) {
	var out, errb bytes.Buffer
	code := runParse([]string{"selec 1"}, &out, &errb)
	want := "pgshard-router: parse: syntax error at or near \"selec\" (SQLSTATE 42601, position 1)\n"
	if code != 1 || out.String() != "" || errb.String() != want {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errb.String())
	}
}

func TestParseSubcommandUsage(t *testing.T) {
	var out, errb bytes.Buffer
	if code := runParse(nil, &out, &errb); code != 2 || errb.String() != "Usage: pgshard-router parse \"SQL\"\n" {
		t.Fatalf("code=%d err=%q", code, errb.String())
	}
}
