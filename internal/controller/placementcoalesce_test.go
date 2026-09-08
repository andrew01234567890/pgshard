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
}
