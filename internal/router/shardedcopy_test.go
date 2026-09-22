package router

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

func newCopyHarness(t *testing.T) *shardedHarness {
	t.Helper()
	log := &fakeDecisionLog{rows: map[string]string{}, fail: map[string]error{}}
	h := newShardedHarnessWith(t, Config{Decisions: log})
	log.h = h
	next := *h.snap
	next.Tables = map[snapshot.TableKey]snapshot.Placement{}
	for k, p := range h.snap.Tables {
		if p.ShardKey == "tenant_id" {
			p.ShardKeyType = "bigint"
		}
		next.Tables[k] = p
	}
	h.snap = &next
	h.setSnap(&next)
	return h
}

func (h *shardedHarness) copiedRows(t *testing.T) map[int][]string {
	t.Helper()
	out := map[int][]string{}
	for i, p := range h.poolers {
		p.mu.Lock()
		for _, data := range p.copied {
			for _, line := range strings.Split(strings.TrimRight(data, "\n"), "\n") {
				if line != "" {
					out[i] = append(out[i], line)
				}
			}
		}
		p.mu.Unlock()
		sort.Strings(out[i])
	}
	return out
}

// Every row reaches the shard its key hashes to, and only that shard, and
// the tag counts the rows of the whole load.
func TestACopyIntoAShardedTableSendsEachRowToItsShard(t *testing.T) {
	h := newCopyHarness(t)
	conn := h.connect(t, h.dsn())
	var in strings.Builder
	want := map[int][]string{}
	for k := int64(1); k <= 200; k++ {
		line := fmt.Sprintf("%d\t%d", k, k*10)
		in.WriteString(line + "\n")
		sh := h.shardOf(t, k)
		want[sh] = append(want[sh], line)
	}
	tag, err := conn.PgConn().CopyFrom(context.Background(), strings.NewReader(in.String()), "copy orders (tenant_id, id) from stdin")
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 200 {
		t.Fatalf("tag %q, want COPY 200", tag)
	}
	got := h.copiedRows(t)
	for sh := range h.poolers {
		sort.Strings(want[sh])
		if strings.Join(got[sh], "|") != strings.Join(want[sh], "|") {
			t.Fatalf("shard %d got %d rows, want %d", sh, len(got[sh]), len(want[sh]))
		}
	}
	if !h.ranOn(0, "commit prepared") && !h.ranOn(1, "commit prepared") {
		t.Fatal("a load written to several shards did not commit with two-phase commit")
	}
}

// The key column is found by position, and a last row without its newline
// is a row.
func TestACopyReadsTheKeyWhereverItIsAndTakesAnUnterminatedLastRow(t *testing.T) {
	h := newCopyHarness(t)
	conn := h.connect(t, h.dsn())
	tag, err := conn.PgConn().CopyFrom(context.Background(), strings.NewReader("7\t3\n9\t5"), "copy orders (id, tenant_id) from stdin")
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 2 {
		t.Fatalf("tag %q", tag)
	}
	got := h.copiedRows(t)
	if !contains(got[h.shardOf(t, int64(3))], "7\t3") || !contains(got[h.shardOf(t, int64(5))], "9\t5") {
		t.Fatalf("rows landed %v", got)
	}
}

// A row the router cannot place fails the whole load, and nothing is
// committed on any shard.
func TestACopyRowThatCannotBePlacedFailsTheWholeLoad(t *testing.T) {
	for name, data := range map[string]string{
		"null key": "1\t1\n\\N\t2\n",
		"bad key":  "1\t1\nabc\t2\n",
		"short":    "1\t1\n2\n",
	} {
		t.Run(name, func(t *testing.T) {
			h := newCopyHarness(t)
			conn := h.connect(t, h.dsn())
			_, err := conn.PgConn().CopyFrom(context.Background(), strings.NewReader(data), "copy orders (tenant_id, id) from stdin")
			if err == nil {
				t.Fatal("the load succeeded")
			}
			for i := range h.poolers {
				if h.ranOn(i, "commit") {
					t.Fatalf("shard %d committed part of a failed load", i)
				}
			}
			if st := conn.PgConn().TxStatus(); st != 'I' {
				t.Fatalf("status %c after the failed load", st)
			}
		})
	}
}

// Inside the client's transaction the rows are the client's to commit.
func TestACopyInsideATransactionLeavesTheCommitToTheClient(t *testing.T) {
	h := newCopyHarness(t)
	conn := h.connect(t, h.dsn())
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("1\t1\n2\t2\n"), "copy orders (tenant_id, id) from stdin"); err != nil {
		t.Fatal(err)
	}
	if st := conn.PgConn().TxStatus(); st != 'T' {
		t.Fatalf("status %c, want the client's transaction still open", st)
	}
	for i := range h.poolers {
		if h.ranOn(i, "commit") {
			t.Fatalf("shard %d committed before the client did", i)
		}
	}
	if _, err := conn.Exec(ctx, "commit"); err != nil {
		t.Fatal(err)
	}
}

// The router counts what it sent and the shards what they stored; a load
// where the two differ lost rows, and must not be reported as done.
func TestACopyWhoseShardsStoredFewerRowsThanWereSentFails(t *testing.T) {
	h := newCopyHarness(t)
	sh := h.shardOf(t, int64(1))
	h.poolers[sh].mu.Lock()
	h.poolers[sh].copyShort = true
	h.poolers[sh].mu.Unlock()
	conn := h.connect(t, h.dsn())
	_, err := conn.PgConn().CopyFrom(context.Background(), strings.NewReader("1\t1\n"), "copy orders (tenant_id, id) from stdin")
	if err == nil || !strings.Contains(err.Error(), "the shards stored") {
		t.Fatalf("err = %v", err)
	}
	for i := range h.poolers {
		if h.ranOn(i, "commit") {
			t.Fatalf("shard %d committed a load that lost rows", i)
		}
	}
}

func TestACopyIntoAShardedTableOverTheExtendedProtocolIsRefused(t *testing.T) {
	h := newCopyHarness(t)
	conn := h.connect(t, h.dsn())
	rr := conn.PgConn().ExecParams(context.Background(), "copy orders (tenant_id, id) from stdin", nil, nil, nil, nil).Read()
	if rr.Err == nil || !strings.Contains(rr.Err.Error(), "only as a simple query") {
		t.Fatalf("err = %v", rr.Err)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
