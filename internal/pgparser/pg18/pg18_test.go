package pg18

import (
	"errors"
	"testing"
)

func TestParseReturnsTypedTree(t *testing.T) {
	res, err := Parse("SELECT 1; INSERT INTO t VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stmts) != 2 {
		t.Fatalf("stmts = %d, want 2", len(res.Stmts))
	}
	if res.Stmts[0].Stmt.GetSelectStmt() == nil || res.Stmts[1].Stmt.GetInsertStmt() == nil {
		t.Fatalf("unexpected node types: %v", res.Stmts)
	}
}

func TestSyntaxErrorCarriesSQLStateAndPosition(t *testing.T) {
	_, err := Parse("SELECT 1 FROM")
	var pe *Error
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if pe.SQLState != "42601" || pe.Position != 14 || pe.Message != `syntax error at end of input` {
		t.Fatalf("got %+v", *pe)
	}
	_, err = Parse("SELECT * FORM t")
	if !errors.As(err, &pe) || pe.Position != 10 || pe.Message != `syntax error at or near "FORM"` {
		t.Fatalf("got %v", err)
	}
}

func TestScanTokens(t *testing.T) {
	res, err := Scan("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Tokens) != 2 || res.Tokens[0].Token.String() != "SELECT" || res.Tokens[1].Token.String() != "ICONST" {
		t.Fatalf("tokens = %v", res.Tokens)
	}
}

func TestDeparseRoundTrip(t *testing.T) {
	tree, err := Parse("select a,b from t where a=1")
	if err != nil {
		t.Fatal(err)
	}
	out, err := Deparse(tree)
	if err != nil || out != "SELECT a, b FROM t WHERE a = 1" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestFingerprintAndNormalize(t *testing.T) {
	a, err := Fingerprint("select 1")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Fingerprint("SELECT   2")
	if a != b || a != "50fde20626009aba" {
		t.Fatalf("fingerprints %q %q", a, b)
	}
	n, err := Normalize("select * from t where a = 1 and b = 'x'")
	if err != nil || n != "select * from t where a = $1 and b = $2" {
		t.Fatalf("n=%q err=%v", n, err)
	}
	if _, err := Fingerprint("selec"); err == nil {
		t.Fatal("want error")
	}
	if _, err := Normalize("selec"); err == nil {
		t.Fatal("want error")
	}
}
