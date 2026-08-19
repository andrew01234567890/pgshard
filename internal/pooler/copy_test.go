package pooler

import (
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func TestLastPKRoundTrip(t *testing.T) {
	for _, vals := range [][]string{{"7"}, {"3", "k'1"}, {"(0,12)"}, {""}} {
		enc := EncodeLastPK(vals)
		got, err := DecodeLastPK(enc)
		if err != nil || len(got) != len(vals) {
			t.Fatalf("%v -> %s -> %v %v", vals, enc, got, err)
		}
		for i := range vals {
			if got[i] != vals[i] {
				t.Fatalf("%v != %v", got, vals)
			}
		}
	}
	if got, err := DecodeLastPK(nil); err != nil || got != nil {
		t.Fatalf("empty: %v %v", got, err)
	}
	if _, err := DecodeLastPK([]byte(`{`)); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestKeysetSQL(t *testing.T) {
	rel := &pgshardv1.ChangeEvent_Relation{Columns: []*pgshardv1.ChangeEvent_Relation_Column{{Name: "b"}, {Name: "a"}, {Name: "x"}}}
	composite := copyTable{schema: "public", table: "pairs", relation: rel, keyNames: []string{`"a"`, `"b"`}, keyTypes: []string{"integer", "text"}}
	if got, want := keysetSQL(composite, false, 10), `SELECT * FROM "public"."pairs" ORDER BY "a", "b" LIMIT 10`; got != want {
		t.Fatalf("first page:\n%s\n%s", got, want)
	}
	if got, want := keysetSQL(composite, true, 10), `SELECT * FROM "public"."pairs" WHERE ("a", "b") > ($1::integer, $2::text) ORDER BY "a", "b" LIMIT 10`; got != want {
		t.Fatalf("next page:\n%s\n%s", got, want)
	}
	if idx := keyColumnIndexes(composite); len(idx) != 2 || idx[0] != 1 || idx[1] != 0 {
		t.Fatalf("key indexes: %v", idx)
	}
	ctid := copyTable{schema: "s", table: "no pk", relation: rel, byCtid: true, keyNames: []string{"ctid"}, keyTypes: []string{"tid"}}
	if got, want := keysetSQL(ctid, true, 5), `SELECT ctid::text, * FROM "s"."no pk" WHERE (ctid) > ($1::tid) ORDER BY ctid LIMIT 5`; got != want {
		t.Fatalf("ctid page:\n%s\n%s", got, want)
	}
	if idx := keyColumnIndexes(ctid); idx != nil {
		t.Fatalf("ctid key indexes: %v", idx)
	}
}
