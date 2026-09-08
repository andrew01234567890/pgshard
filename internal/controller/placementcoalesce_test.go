package controller

import (
	"strings"
	"testing"
)

func coalesceShape() rowShape {
	return rowShape{Schema: "public", Name: "t", Columns: []string{"id", "note"}, PK: []string{"id"}}
}

func plain(id, note string) *Tuple {
	return &Tuple{Values: []*string{s(id), s(note)}, Unchanged: []bool{false, false}}
}

// A round of inserts is one statement, not one per row. An operation
// otherwise carries a whole row and its literals, so a thousand-row
// round trip was a thousand parses and a thousand plans on the target.
func TestCoalesceJoinsARunOfUpsertsIntoOneStatement(t *testing.T) {
	shape := coalesceShape()
	var ops []applyOp
	for i := range 50 {
		ops = append(ops, applyOp{shard: 0, up: plain(itoa(int64(i)), "n")})
	}
	got := coalesce(shape, "t", ops)
	if len(got) != 1 {
		t.Fatalf("50 upserts rendered as %d statements, want 1", len(got))
	}
	if n := strings.Count(got[0], "), ("); n != 49 {
		t.Fatalf("the statement carries %d row separators, want 49: %.200s", n, got[0])
	}
}

// ON CONFLICT DO UPDATE refuses to affect the same row twice in one
// command (21000), so a row that changes twice inside one flush has to
// end the run. Getting this wrong turns a working catch-up into one that
// fails on exactly the workload it is there to keep up with.
func TestCoalesceEndsARunAtARepeatedKey(t *testing.T) {
	shape := coalesceShape()
	got := coalesce(shape, "t", []applyOp{
		{shard: 0, up: plain("1", "first")},
		{shard: 0, up: plain("2", "other")},
		{shard: 0, up: plain("1", "second")},
	})
	if len(got) != 2 {
		t.Fatalf("a repeated key rendered as %d statements, want 2: %q", len(got), got)
	}
	if !strings.Contains(got[0], "'first'") || !strings.Contains(got[0], "'other'") {
		t.Errorf("first statement lost a row: %s", got[0])
	}
	if !strings.Contains(got[1], "'second'") || strings.Contains(got[1], "'first'") {
		t.Errorf("second statement is not the repeat alone: %s", got[1])
	}
}

// Anything that is not a plain upsert ends the run, which is what keeps a
// delete after an upsert of the same row in that order.
func TestCoalesceKeepsOrderAroundADelete(t *testing.T) {
	shape := coalesceShape()
	got := coalesce(shape, "t", []applyOp{
		{shard: 0, up: plain("1", "a")},
		{shard: 0, sql: `DELETE FROM "public"."t" WHERE "id" = '1'`},
		{shard: 0, up: plain("1", "b")},
	})
	if len(got) != 3 {
		t.Fatalf("got %d statements, want 3: %q", len(got), got)
	}
	if !strings.HasPrefix(got[0], "INSERT") || !strings.HasPrefix(got[1], "DELETE") || !strings.HasPrefix(got[2], "INSERT") {
		t.Fatalf("the delete did not stay between the two upserts: %q", got)
	}
}

// A row with unchanged (TOAST) columns names only the columns it carries,
// so it cannot share a VALUES list with rows that name all of them.
func TestCoalesceLeavesAPartialRowAlone(t *testing.T) {
	shape := coalesceShape()
	partial := &Tuple{Values: []*string{s("2"), nil}, Unchanged: []bool{false, true}}
	ops := append(upsertOps(shape, "t", 0, partial), applyOp{shard: 0, up: plain("1", "a")})
	got := coalesce(shape, "t", ops)
	if len(got) != 2 {
		t.Fatalf("got %d statements, want 2: %q", len(got), got)
	}
	if strings.Contains(got[0], "'a'") {
		t.Fatalf("a partial row was merged with a whole one: %s", got[0])
	}
}

// A run is bounded, so one statement stays cancellable and its string
// stays small however long the flush is.
func TestCoalesceBoundsOneStatement(t *testing.T) {
	shape := coalesceShape()
	var ops []applyOp
	for i := range applyBatchOps + 1 {
		ops = append(ops, applyOp{shard: 0, up: plain(itoa(int64(i)), "n")})
	}
	if got := coalesce(shape, "t", ops); len(got) != 2 {
		t.Fatalf("%d upserts rendered as %d statements, want 2", len(ops), len(got))
	}
	wide := strings.Repeat("x", applyBatchBytes/4)
	ops = nil
	for i := range 5 {
		ops = append(ops, applyOp{shard: 0, up: plain(itoa(int64(i)), wide)})
	}
	if got := coalesce(shape, "t", ops); len(got) < 2 {
		t.Fatalf("five rows of %d bytes rendered as one statement", len(wide))
	}
}

// The byte bounds on the hold and on the open transaction have to count
// the rows an operation CARRIES, not only the ones it has already
// rendered. A plain upsert is carried as a tuple now, so an accounting
// that reads op.sql sees nothing at all for the ordinary case -- and both
// bounds exist to stop the controller holding a whole bulk load in memory.
func TestAnOperationsSizeCountsTheRowItCarries(t *testing.T) {
	shape := coalesceShape()
	big := strings.Repeat("x", 4096)
	carried := applyOp{shard: 0, up: plain("1", big)}
	if got := carried.bytes(); got < len(big) {
		t.Fatalf("a carried row of %d bytes was accounted as %d; the hold and the open-transaction bound both count this",
			len(big), got)
	}
	// A delete carries its row too, so it is the same hazard: an
	// accounting that only understood upserts would count a flush of
	// deletes as nothing.
	if got := (applyOp{shard: 0, del: plain(strings.Repeat("7", 200), "n")}).bytes(); got < 200 {
		t.Fatalf("a delete of a 200-byte key was accounted as %d bytes", got)
	}
	rendered := applyOp{shard: 0, sql: shape.DeleteSQL("t", plain("1", big))}
	if got := rendered.bytes(); got != len(rendered.sql) {
		t.Fatalf("a rendered statement of %d bytes was accounted as %d", len(rendered.sql), got)
	}
	// Escaping is what a literal actually costs in the statement, so a
	// value made entirely of quotes must not be accounted at half its
	// rendered size.
	quotes := strings.Repeat("'", 1000)
	if got := (applyOp{shard: 0, up: plain("2", quotes)}).bytes(); got < 2*len(quotes) {
		t.Fatalf("%d quotes render as %d bytes but were accounted as %d", len(quotes), 2*len(quotes), got)
	}
	// What the bounds need is that the accounting TRACKS the statement the
	// rows become: under-counting lets a hold grow past the bound that is
	// supposed to stop it. Measured over a run long enough that the fixed
	// INSERT header is noise, for each shape a value can take -- ordinary,
	// one that doubles on escaping, one that also earns the E prefix, and
	// a null.
	for name, v := range map[string]*string{
		"ordinary":  s(strings.Repeat("a", 100)),
		"quoted":    s(strings.Repeat("'", 100)),
		"backslash": s(strings.Repeat(`\`, 100)),
		"null":      nil,
	} {
		var rows []*Tuple
		accounted := 0
		for i := range 50 {
			row := &Tuple{Values: []*string{s(itoa(int64(i))), v}, Unchanged: []bool{false, false}}
			rows = append(rows, row)
			accounted += (applyOp{shard: 0, up: row}).bytes()
		}
		rendered := len(shape.UpsertSQL("t", rows)[0])
		if accounted < rendered*9/10 {
			t.Errorf("%s: 50 rows accounted as %d bytes render as %d; a bound counting these fires too late",
				name, accounted, rendered)
		}
	}
}

func del(id string) *Tuple {
	return &Tuple{Values: []*string{s(id), s("n")}, Unchanged: []bool{false, false}}
}

// A run of deletes is one statement too. They cannot share the upserts'
// statement, so the two kinds break each other's runs -- which is also
// what keeps a delete after an upsert of the same row in that order.
func TestCoalesceJoinsARunOfDeletes(t *testing.T) {
	shape := coalesceShape()
	var ops []applyOp
	for i := range 50 {
		ops = append(ops, applyOp{shard: 0, del: del(itoa(int64(i)))})
	}
	got := coalesce(shape, "t", ops)
	if len(got) != 1 {
		t.Fatalf("50 deletes rendered as %d statements, want 1: %q", len(got), got)
	}
	if !strings.HasPrefix(got[0], "DELETE") || !strings.Contains(got[0], `"id" IN (`) {
		t.Fatalf("not one IN list: %s", got[0])
	}
	if n := strings.Count(got[0], ", "); n != 49 {
		t.Fatalf("the statement carries %d separators, want 49", n)
	}
}

// A repeated key is fine in an IN list -- it deletes the row once -- so
// unlike the upsert side a delete run does not break on one. If it did,
// a workload that deletes and re-deletes would fall back to a statement
// each for no reason.
func TestCoalesceDoesNotBreakADeleteRunOnARepeatedKey(t *testing.T) {
	got := coalesce(coalesceShape(), "t", []applyOp{
		{shard: 0, del: del("1")}, {shard: 0, del: del("1")},
	})
	if len(got) != 1 {
		t.Fatalf("a repeated delete key split the run into %d statements: %q", len(got), got)
	}
}

func TestCoalesceKeepsUpsertsAndDeletesApartAndInOrder(t *testing.T) {
	got := coalesce(coalesceShape(), "t", []applyOp{
		{shard: 0, up: plain("1", "a")},
		{shard: 0, del: del("2")},
		{shard: 0, up: plain("3", "c")},
	})
	if len(got) != 3 {
		t.Fatalf("got %d statements, want 3: %q", len(got), got)
	}
	if !strings.HasPrefix(got[0], "INSERT") || !strings.HasPrefix(got[1], "DELETE") || !strings.HasPrefix(got[2], "INSERT") {
		t.Fatalf("the order was not kept: %q", got)
	}
}

func TestCoalesceBoundsADeleteStatement(t *testing.T) {
	var ops []applyOp
	for i := range applyBatchOps + 1 {
		ops = append(ops, applyOp{shard: 0, del: del(itoa(int64(i)))})
	}
	if got := coalesce(coalesceShape(), "t", ops); len(got) != 2 {
		t.Fatalf("%d deletes rendered as %d statements, want 2", len(ops), len(got))
	}
}

// One row keeps the equality form it has always had; several become a row
// constructor, which PostgreSQL compares element by element.
func TestDeleteManySQLShapes(t *testing.T) {
	single := rowShape{Schema: "public", Name: "t", Columns: []string{"id", "note"}, PK: []string{"id"}}
	if got, want := single.DeleteSQL("t", del("1")), `DELETE FROM "public"."t" WHERE "id" = '1'`; got != want {
		t.Errorf("one row:\n got %s\nwant %s", got, want)
	}
	if got, want := single.DeleteManySQL("t", []*Tuple{del("1"), del("2")}),
		`DELETE FROM "public"."t" WHERE "id" IN ('1', '2')`; got != want {
		t.Errorf("two rows:\n got %s\nwant %s", got, want)
	}
	comp := rowShape{Schema: "public", Name: "t", Columns: []string{"a", "b", "note"}, PK: []string{"a", "b"}}
	row := func(a, b string) *Tuple {
		return &Tuple{Values: []*string{s(a), s(b), s("n")}, Unchanged: []bool{false, false, false}}
	}
	if got, want := comp.DeleteSQL("t", row("1", "2")), `DELETE FROM "public"."t" WHERE "a" = '1' AND "b" = '2'`; got != want {
		t.Errorf("composite, one row:\n got %s\nwant %s", got, want)
	}
	if got, want := comp.DeleteManySQL("t", []*Tuple{row("1", "2"), row("3", "4")}),
		`DELETE FROM "public"."t" WHERE ("a", "b") IN (('1', '2'), ('3', '4'))`; got != want {
		t.Errorf("composite, two rows:\n got %s\nwant %s", got, want)
	}
}
