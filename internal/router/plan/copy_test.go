package plan

import (
	"context"
	"strings"
	"testing"
)

func copyPlan(t *testing.T, sql string) (Plan, error) {
	t.Helper()
	return New().Plan(context.Background(), session(fixture(t)), sql)
}

// COPY into a sharded table was refused outright, so the documented way to
// bulk load one was INSERT. The router can route the rows instead: it reads
// the shard key out of each and sends it where it belongs.
func TestCopyIntoAShardedTableIsPlanned(t *testing.T) {
	p, err := copyPlan(t, "copy orders (tenant_id, id, amount) from stdin")
	if err != nil {
		t.Fatalf("%v", err)
	}
	if p.Copy == nil {
		t.Fatal("no copy plan")
	}
	if p.Copy.KeyColumn != 0 || p.Copy.Columns != 3 {
		t.Fatalf("copy %+v, want the key at 0 of 3", *p.Copy)
	}
	if len(p.Shards) == 0 {
		t.Fatal("a sharded COPY reaches the serving shards")
	}
}

// The key is not always first, and the stream carries no column names, so
// the POSITION is what the router needs.
func TestCopyFindsTheKeyWhereverItIs(t *testing.T) {
	p, err := copyPlan(t, "copy orders (id, amount, tenant_id) from stdin")
	if err != nil {
		t.Fatal(err)
	}
	if p.Copy.KeyColumn != 2 {
		t.Fatalf("key column %d, want 2", p.Copy.KeyColumn)
	}
}

// A row whose key the router cannot read is a row it cannot place.
func TestCopyWithoutTheShardKeyIsRefused(t *testing.T) {
	_, err := copyPlan(t, "copy orders (id, amount) from stdin")
	if err == nil || !strings.Contains(err.Error(), `must include the shard key "tenant_id"`) {
		t.Fatalf("%v", err)
	}
}

// PostgreSQL takes the table's column order when the statement names none,
// and the router does not know it -- so it asks.
func TestCopyWithoutAColumnListIsRefused(t *testing.T) {
	_, err := copyPlan(t, "copy orders from stdin")
	if err == nil || !strings.Contains(err.Error(), "must name its columns") {
		t.Fatalf("%v", err)
	}
}

// The options a fan-out cannot carry are refused BY NAME, so the message
// says which one rather than that COPY is unavailable.
func TestCopyRefusesTheOptionsAFanOutCannotCarry(t *testing.T) {
	for _, c := range []struct{ sql, msg string }{
		{"copy orders (tenant_id, id) from stdin with (format csv)", "FORMAT csv"},
		{"copy orders (tenant_id, id) from stdin with (format binary)", "FORMAT binary"},
		{"copy orders (tenant_id, id) from stdin with (on_error ignore)", "ON_ERROR"},
		{"copy orders (tenant_id, id) from stdin with (log_verbosity verbose)", "LOG_VERBOSITY"},
		{"copy orders (tenant_id, id) from '/tmp/x'", "COPY FROM a file"},
		{"copy orders (tenant_id, id) from program 'cat /tmp/x'", "COPY FROM PROGRAM"},
		{"copy orders to stdout", "COPY TO from a sharded table"},
	} {
		_, err := copyPlan(t, c.sql)
		if err == nil || !strings.Contains(err.Error(), c.msg) {
			t.Errorf("%s: %v, want %q", c.sql, err, c.msg)
		}
	}
}

// FORMAT text is what COPY FROM STDIN uses by default, so saying so
// explicitly is the same statement.
func TestCopyAcceptsAnExplicitTextFormat(t *testing.T) {
	if _, err := copyPlan(t, "copy orders (tenant_id, id) from stdin with (format text)"); err != nil {
		t.Fatalf("%v", err)
	}
}

// Unsharded COPY is untouched, and a reference table is still refused: its
// rows belong on every shard, which is a fan-out of a different shape.
func TestCopyOnOtherPlacementsIsUnchanged(t *testing.T) {
	if _, err := copyPlan(t, "copy items from stdin"); err != nil {
		t.Errorf("unsharded COPY still works: %v", err)
	}
	_, err := copyPlan(t, "copy regions from stdin")
	if err == nil || !strings.Contains(err.Error(), "reference table") {
		t.Errorf("reference: %v", err)
	}
}
