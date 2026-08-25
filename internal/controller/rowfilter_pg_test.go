package controller

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/placement"
)

var rowFilterImages = []struct{ label, name string }{
	{"pg18", "ghcr.io/andrew01234567890/pgshard-postgres:18"},
	{"pg19", "ghcr.io/andrew01234567890/pgshard-postgres:19"},
}

// TestRowFilterMatchesPlacement inserts random int8, text and uuid keys and
// checks, for every target range of a 5-way split, that the publication row
// filter selects exactly the rows placement.KeyspaceID assigns to it.
func TestRowFilterMatchesPlacement(t *testing.T) {
	parallelPG(t)
	for _, img := range rowFilterImages {
		t.Run(img.label, func(t *testing.T) {
			conn := connect(t, startPostgresImage(t, img.name, nil))
			runRowFilterCheck(t, conn)
		})
	}
}

func runRowFilterCheck(t *testing.T, conn *pgx.Conn) {
	const n = 10000
	mustExec(t, conn, `CREATE TABLE k (i8 bigint, i4 integer, i2 smallint, tx text, vc varchar(40), u uuid)`)
	ints := make([]int64, n)
	texts := make([]string, n)
	uuids := make([][16]byte, n)
	for i := range n {
		var b [24]byte
		if _, err := rand.Read(b[:]); err != nil {
			t.Fatal(err)
		}
		ints[i] = int64(binary.LittleEndian.Uint64(b[:8]))
		texts[i] = fmt.Sprintf("key-%x", b[8:8+int(b[23]%12)])
		copy(uuids[i][:], b[8:24])
	}
	uuidStrs := make([]string, n)
	for i, u := range uuids {
		uuidStrs[i] = fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
	}
	mustExec(t, conn, `INSERT INTO k SELECT i8, (i8 % 2147483648)::int4, (i8 % 32768)::int2, tx, tx, u::uuid FROM unnest($1::int8[], $2::text[], $3::text[]) AS v(i8, tx, u)`, ints, texts, uuidStrs)
	ranges, err := placement.Split(5)
	if err != nil {
		t.Fatal(err)
	}
	type column struct {
		name, typ string
		key       func(i int) any
	}
	cols := []column{
		{"i8", "bigint", func(i int) any { return ints[i] }},
		{"i4", "integer", func(i int) any { return ints[i] % 2147483648 }},
		{"i2", "smallint", func(i int) any { return ints[i] % 32768 }},
		{"tx", "text", func(i int) any { return texts[i] }},
		{"vc", "character varying(40)", func(i int) any { return texts[i] }},
		{"u", "uuid", func(i int) any { return uuids[i] }},
	}
	for _, c := range cols {
		hash, err := KeyHashExpr(c.name, c.typ)
		if err != nil {
			t.Fatal(err)
		}
		total := 0
		for ri, r := range ranges {
			want := 0
			for i := range n {
				id, err := placement.KeyspaceID(c.key(i))
				if err != nil {
					t.Fatal(err)
				}
				if ranges.Locate(id) == ri {
					want++
				}
			}
			got := queryOne[int64](t, conn, "SELECT count(*) FROM k WHERE "+RangeFilter(hash, r))
			if int(got) != want {
				t.Errorf("%s range %d: filter selects %d rows, placement assigns %d", c.name, ri, got, want)
			}
			total += int(got)
		}
		if total != n {
			t.Errorf("%s: filters select %d rows in total, want %d", c.name, total, n)
		}
	}
	// The filters are accepted as publication row filters.
	mustExec(t, conn, `ALTER TABLE k REPLICA IDENTITY FULL`)
	h, _ := KeyHashExpr("u", "uuid")
	mustExec(t, conn, CreatePublicationSQL("p", []PublishedTable{{Schema: "public", Name: "k", Filter: RangeFilter(h, ranges[1])}}))
}
