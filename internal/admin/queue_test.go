package admin

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

type fakeQueueSource struct {
	entries []catalog.QueueEntry
	err     error
}

func (f fakeQueueSource) OperationQueue(context.Context) ([]catalog.QueueEntry, error) {
	return f.entries, f.err
}

// fakeQueueCatalog is a catalog source that also answers the queue.
type fakeQueueCatalog struct {
	fakeWorkflows
	queue fakeQueueSource
}

func (f fakeQueueCatalog) OperationQueue(ctx context.Context) ([]catalog.QueueEntry, error) {
	return f.queue.OperationQueue(ctx)
}

func ptr[T any](v T) *T { return &v }

func queueEntries(now time.Time) []catalog.QueueEntry {
	return []catalog.QueueEntry{
		{Position: 1, Kind: catalog.OperationReshard, ID: "00000000-0000-0000-0000-0000000000r1", Command: "reshard 2 to 4 shards",
			State: "running", Stage: ptr("copying"), Progress: ptr(0.28), ProgressBar: ptr("[#####---------------]  28%"),
			Detail: ptr("copying 12/40 tables"), CreatedAt: now.Add(-time.Hour), StartedAt: ptr(now.Add(-50 * time.Minute)), UpdatedAt: now},
		{Position: 2, Kind: catalog.OperationDDL, ID: "00000000-0000-0000-0000-0000000000d1", Database: ptr("app"),
			Command: "CREATE INDEX orders_note_idx", Statement: ptr("CREATE INDEX orders_note_idx ON orders (note)"), State: "waiting",
			WaitingFor: ptr("reshard 00000000-0000-0000-0000-0000000000r1 (in progress)"),
			Blockers:   []catalog.Blocker{{Kind: catalog.OperationReshard, ID: "00000000-0000-0000-0000-0000000000r1", Reason: catalog.BlockedByStarted}},
			Progress:   ptr(0.0), ProgressBar: ptr("[--------------------]   0%"), CreatedAt: now.Add(-5 * time.Minute), UpdatedAt: now},
	}
}

func TestQueuePageShowsWhatWaitsForWhat(t *testing.T) {
	now := time.Now()
	s, _ := newTestServer(t, fakeQueueCatalog{queue: fakeQueueSource{entries: queueEntries(now)}})
	rec := get(t, s, "/queue")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`data-live hx-get="/queue/panel"`, `aria-current="page">Queue`,
		"2 in the queue · 1 running · 1 waiting",
		"reshard 2 to 4 shards", "<td>1</td>", `aria-valuenow="28"`, `width="28"`, "28% · copying 12/40 tables",
		"CREATE INDEX orders_note_idx ON orders (note)", `class="state-waiting"`, "reshard 00000000 <span class=\"meta\">(in progress)</span>",
		`href="/migrations/00000000-0000-0000-0000-0000000000d1"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("page missing %q", want)
		}
	}
	if strings.Contains(body, " style=") {
		t.Error("the page uses an inline style, which the content security policy blocks")
	}
	if api := get(t, s, "/api/v1/queue").Body.String(); !strings.Contains(api, `"waiting_for": "reshard 00000000-0000-0000-0000-0000000000r1 (in progress)"`) {
		t.Errorf("API: %s", api)
	}
	if panel := get(t, s, "/queue/panel").Body.String(); !strings.Contains(panel, "reshard 2 to 4 shards") || strings.Contains(panel, "<nav") {
		t.Errorf("panel: %s", panel)
	}
}

func TestQueuePageSaysWhenThereIsNoQueue(t *testing.T) {
	s, _ := newTestServer(t, fakeWorkflows{})
	if body := get(t, s, "/queue").Body.String(); !strings.Contains(body, "no catalog connection is configured") {
		t.Errorf("without a catalog: %s", body)
	}
	s, _ = newTestServer(t, fakeQueueCatalog{queue: fakeQueueSource{err: &pgconn.PgError{Code: "42P01", Message: `relation "pgshard.operation_queue_detail" does not exist`}}})
	if body := get(t, s, "/queue").Body.String(); !strings.Contains(body, "has no operation queue yet") {
		t.Errorf("without the schema: %s", body)
	}
	s, _ = newTestServer(t, fakeQueueCatalog{queue: fakeQueueSource{err: errors.New("catalog unreachable")}})
	if body := get(t, s, "/queue").Body.String(); !strings.Contains(body, "catalog: catalog unreachable") {
		t.Errorf("with a broken catalog: %s", body)
	}
}

func TestQueuePageShowsTheFirstBlockerAndFoldsTheRest(t *testing.T) {
	now := time.Now()
	entries := queueEntries(now)
	last := entries[len(entries)-1]
	last.Position = 3
	last.ID = "00000000-0000-0000-0000-0000000000d2"
	last.Command = "ALTER TABLE orders ADD COLUMN shipped_at timestamptz"
	last.Statement = ptr(last.Command)
	last.Blockers = []catalog.Blocker{
		{Kind: catalog.OperationDDL, ID: "00000000-0000-0000-0000-0000000000d1", Reason: catalog.BlockedByEarlier},
		{Kind: catalog.OperationReshard, ID: "00000000-0000-0000-0000-0000000000r1", Reason: catalog.BlockedByStarted},
	}
	s, _ := newTestServer(t, fakeQueueCatalog{queue: fakeQueueSource{entries: append(entries, last)}})
	body := get(t, s, "/queue").Body.String()
	head := "<a href=\"/migrations/00000000-0000-0000-0000-0000000000d1\">DDL 00000000</a> <span class=\"meta\">(queued before it)</span></div><details><summary class=\"meta\">and 1 more</summary>"
	if !strings.Contains(body, head) {
		t.Errorf("the page does not fold the blockers after the first: %s", body)
	}
	if !strings.Contains(body, "reshard 00000000 <span class=\"meta\">(in progress)</span></div></details>") {
		t.Errorf("the folded blockers are missing: %s", body)
	}
}

func TestQueueSummaryAccountsForEveryEntry(t *testing.T) {
	now := time.Now()
	entries := queueEntries(now)
	paused := entries[0]
	paused.Position = 3
	paused.ID = "00000000-0000-0000-0000-0000000000r2"
	paused.State = "paused"
	s, _ := newTestServer(t, fakeQueueCatalog{queue: fakeQueueSource{entries: append(entries, paused)}})
	if body := get(t, s, "/queue").Body.String(); !strings.Contains(body, "3 in the queue · 1 running · 1 waiting · 1 paused") {
		t.Errorf("the summary drops the paused operation: %s", body)
	}
}
