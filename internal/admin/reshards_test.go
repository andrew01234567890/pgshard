package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

type fakeWorkflows struct {
	fakeCatalog
	workflows []WorkflowRecord
	err       error
}

func (f fakeWorkflows) Workflows(context.Context) ([]WorkflowRecord, error) {
	return f.workflows, f.err
}

const reshardWorkflowID = "0b7e6b6c-1c8e-4c0d-9f6e-1b8d4a0e2f11"

var (
	reshardT0     = time.Date(2026, 8, 20, 8, 0, 0, 0, time.UTC)
	reshardT1     = reshardT0.Add(10 * time.Minute)
	reshardFenced = reshardT0.Add(30 * time.Minute)
)

func reshardObjects() []client.Object {
	pause := metav1.Duration{Duration: 1200 * time.Millisecond}
	olderPause := metav1.Duration{Duration: 900 * time.Millisecond}
	objs := populated()
	cluster := objs[0].(*pgshardv1alpha1.PgShardCluster)
	cluster.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{Name: "demo-reshard-g2"}
	return append(objs,
		&pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "ns2"}},
		&pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-reshard-g2", Namespace: "ns1", CreationTimestamp: metav1.NewTime(reshardT0)},
			Spec: pgshardv1alpha1.PgShardReshardSpec{ClusterName: "demo", FromGeneration: 1, TargetGeneration: 2, TargetShardSet: "g2", TargetShards: 2,
				TargetRanges: []pgshardv1alpha1.ReshardRange{{ShardID: 1, RangeStart: 0, RangeEnd: 9223372036854775807}, {ShardID: 0, RangeStart: -9223372036854775808, RangeEnd: -1}}},
			Status: pgshardv1alpha1.PgShardReshardStatus{Phase: "Switching", WorkflowID: reshardWorkflowID, CutoverPause: &pause,
				Targets:    []pgshardv1alpha1.ReshardTargetStatus{{ShardID: 0, Group: "shard-0-g2", Ready: true, Primary: "demo-shard-0-g2-0"}, {ShardID: 1, Group: "shard-1-g2"}},
				JournalIDs: []string{"j-0001"}, Message: "switching: step verify",
				Conditions: []metav1.Condition{
					{Type: "TargetsReady", Status: "True", LastTransitionTime: metav1.NewTime(reshardT1), Message: "2/2 primaries"},
					{Type: "WritesSwitched", Status: "False", LastTransitionTime: metav1.NewTime(reshardT1)}}},
		},
		&pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-reshard-g3", Namespace: "ns1", CreationTimestamp: metav1.NewTime(reshardT1)},
			Spec:       pgshardv1alpha1.PgShardReshardSpec{ClusterName: "demo", FromGeneration: 2, TargetGeneration: 3, TargetShardSet: "g3", TargetShards: 4},
			Status:     pgshardv1alpha1.PgShardReshardStatus{Phase: "Failed", WorkflowID: "missing", Message: "copy failed: subscription slot lost", CutoverPause: &olderPause},
		},
		&pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: "other-reshard-g2", Namespace: "ns2"},
			Spec:       pgshardv1alpha1.PgShardReshardSpec{ClusterName: "other", FromGeneration: 1, TargetGeneration: 2, TargetShardSet: "g2", TargetShards: 2},
			Status:     pgshardv1alpha1.PgShardReshardStatus{Targets: []pgshardv1alpha1.ReshardTargetStatus{{ShardID: 0, Ready: true}, {ShardID: 1, Ready: true}}},
		},
	)
}

func reshardWorkflows() fakeWorkflows {
	status := map[string]any{
		"stage": "switching", "message": "workflow: step verify",
		"progress": map[string]any{"subscriptions": 4, "tables_total": 8, "tables_ready": 6, "lag_bytes": 2048, "paused": 1},
		"targets": map[string]any{
			"0": map[string]any{"subscriptions": 2, "tables_total": 4, "tables_ready": 4, "lag_bytes": 0},
			"1": map[string]any{"subscriptions": 2, "tables_total": 4, "tables_ready": 2, "lag_bytes": -1},
		},
		"cutover": map[string]any{
			"source_set": "default", "step": "verify", "attempts": 2, "fenced_at": reshardFenced, "pause_ms": 1500, "fence_ms": 2100,
			"aborts":      []string{"attempt 1: fence timeout"},
			"verify":      map[string]any{"tables": 8, "rows": 12345, "mismatches": []string{"app.orders on g2/1"}, "CheckedAt": reshardFenced.Add(time.Second)},
			"switched_at": reshardFenced.Add(3 * time.Second),
		},
	}
	spec := map[string]any{"shard_set": "g2", "generation": 2, "pause_before": "complete", "proceed": []string{"switchWrites"}, "retire_after_seconds": 3600}
	return fakeWorkflows{workflows: []WorkflowRecord{
		{ID: reshardWorkflowID, Kind: "reshard", State: "running", Spec: mustRaw(spec), Status: mustRaw(status), JournalIDs: []string{"j-workflow"}, CreatedAt: reshardT0, UpdatedAt: reshardFenced.Add(5 * time.Second)},
		{ID: "wf-placement", Kind: "table_placement", State: "running", CreatedAt: reshardT1, UpdatedAt: reshardT1,
			Spec:   mustRaw(map[string]any{"database": "app", "schema_name": "public", "table_name": "orders", "from": map[string]any{"placement": "reference"}, "to": map[string]any{"placement": "sharded", "shard_key": "customer_id"}}),
			Status: mustRaw(map[string]any{"stage": "copying", "message": "backfilling", "progress": map[string]any{"subscriptions": 1, "tables_total": 1, "tables_ready": 0, "lag_bytes": 512}})},
		{ID: "wf-upgrade", Kind: "upgrade", State: "pending", CreatedAt: reshardT1, UpdatedAt: reshardT1,
			Spec: mustRaw(map[string]any{"from": 18, "to": "19"}), Status: mustRaw(map[string]any{"stage": "planning", "message": "waiting for maintenance window"})},
		{ID: "wf-cancelled", Kind: "reshard", State: "cancelled", JournalIDs: []string{"j-cancelled"}, CreatedAt: reshardT0, UpdatedAt: reshardT1, Spec: mustRaw(map[string]any{}),
			Status: mustRaw(map[string]any{"stage": "cancelled", "reason": "shard set removed"})},
	}}
}

func mustRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func TestReshardsPageListsRecordsPlacementsAndUpgrades(t *testing.T) {
	s, _ := newTestServer(t, reshardWorkflows(), reshardObjects()...)
	rec := get(t, s, "/reshards")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`data-live hx-get="/reshards/panel"`, `aria-current="page">Reshards`,
		"ns1/demo", "active demo-reshard-g2", `href="/reshards/ns1/demo-reshard-g2"`,
		"g1 → g2 (2 shards, g2)", "Switching", "<td>switching</td>", "2026-08-20T08:00:00Z", "<td>1/2</td>",
		`aria-valuenow="75"`, "6/8 rels · lag 2.0 KiB · 1 paused", "shard 0: ", `aria-valuenow="100"`, "4/4 rels · lag 0 B", "2/4 rels · lag unknown",
		"<td>1.2s</td>", "<code>j-0001</code>", "switching: step verify", "<td>900ms</td>",
		`role="alert"`, "reshard demo-reshard-g3 failed: copy failed: subscription slot lost", "g2 → g3 (4 shards, g3)",
		"ns2/other", "g1 → g2 (2 shards, g2)", "Pending", "<td>2/2</td>",
		"Placement changes", "<td>app</td>", "public.orders", "reference → sharded(customer_id)", "<td>copying</td>", "0/1 rels · lag 512 B", "backfilling",
		"Major upgrades", "18 → 19", "planning", "waiting for maintenance window",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("page missing %q", want)
		}
	}
	for _, unwanted := range []string{"j-workflow", "workflow: step verify", "<td>1.5s</td>"} {
		if strings.Contains(body, unwanted) {
			t.Errorf("record fields must win over the workflow: found %q", unwanted)
		}
	}
	if !strings.Contains(get(t, s, "/reshards/panel").Body.String(), "Placement changes") {
		t.Error("panel fragment missing")
	}
}

func TestReshardsPageWithoutCatalog(t *testing.T) {
	s, _ := newTestServer(t, nil, reshardObjects()...)
	body := get(t, s, "/reshards").Body.String()
	for _, want := range []string{"No placement workflows (none recorded, or no catalog DSN configured)", "No major upgrade workflows recorded.", "demo-reshard-g2", "<td>—</td>"} {
		if !strings.Contains(body, want) {
			t.Errorf("page missing %q", want)
		}
	}
	if strings.Contains(body, "catalog:") {
		t.Error("no catalog must not report an error")
	}
	s, _ = newTestServer(t, fakeWorkflows{err: context.DeadlineExceeded}, reshardObjects()...)
	body = get(t, s, "/reshards").Body.String()
	if !strings.Contains(body, "catalog: context deadline exceeded") || !strings.Contains(body, "No placement workflows.") {
		t.Errorf("catalog error not shown: %s", body)
	}
	s, _ = newTestServer(t, fakeWorkflows{}, populated()...)
	if body := get(t, s, "/reshards").Body.String(); !strings.Contains(body, "No reshards.") {
		t.Error("empty cluster not shown")
	}
}

func TestReshardDetailShowsTimelineTargetsVerifyAndCutover(t *testing.T) {
	s, _ := newTestServer(t, reshardWorkflows(), reshardObjects()...)
	rec := get(t, s, "/reshards/ns1/demo-reshard-g2")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"demo · g1 → g2 (2 shards, g2)", "<dd>switching (workflow running)</dd>", "<code>" + reshardWorkflowID + "</code>",
		"2026-08-20T08:30:05Z", "<dd>1.2s</dd>",
		"<td>0</td><td>[-9223372036854775808, -1]</td><td>shard-0-g2</td>", `class="ok">true</td><td>demo-shard-0-g2-0</td>`, "<td>2</td>",
		"<td>1</td><td>[0, 9223372036854775807]</td><td>shard-1-g2</td>", `class="">false</td><td>—</td>`,
		"Stage timeline",
		"2026-08-20T08:00:00Z</time> · created", "2026-08-20T08:10:00Z</time> · TargetsReady · 2/2 primaries",
		"2026-08-20T08:30:00Z</time> · fenced", "2026-08-20T08:30:03Z</time> · switched", "2026-08-20T08:30:05Z</time> · switching · current",
		"<dd>default</dd>", "<dd>verify · attempt 2</dd>", "<dd>complete · proceed switchWrites </dd>",
		"<dd>1.5s (fence held 2.1s)</dd>", "2026-08-20T09:30:03Z", `class="bad">attempt 1: fence timeout`,
		"<dd>8</dd>", "<dd>12345</dd>", `class="bad">1</dd>`, "<li>app.orders on g2/1</li>",
		`href="/api/v1/reshards/ns1/demo-reshard-g2"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("detail missing %q", want)
		}
	}
	order := []string{"· created", "· TargetsReady", "· fenced", "· switched", "· current"}
	for i := 1; i < len(order); i++ {
		if strings.Index(body, order[i-1]) > strings.Index(body, order[i]) {
			t.Errorf("timeline not chronological: %s after %s", order[i-1], order[i])
		}
	}
}

func TestReshardDetailWithoutWorkflow(t *testing.T) {
	s, _ := newTestServer(t, reshardWorkflows(), reshardObjects()...)
	body := get(t, s, "/reshards/ns1/demo-reshard-g3").Body.String()
	for _, want := range []string{`role="alert"`, "reshard failed: copy failed: subscription slot lost", "<code>missing</code> · not in catalog", "No stage transitions recorded.", "Cutover not started.", "No targets recorded."} {
		if !strings.Contains(body, want) {
			t.Errorf("detail missing %q", want)
		}
	}
	s, _ = newTestServer(t, fakeWorkflows{err: context.DeadlineExceeded}, reshardObjects()...)
	if body := get(t, s, "/reshards/ns1/demo-reshard-g2").Body.String(); !strings.Contains(body, "catalog: context deadline exceeded") {
		t.Error("catalog error not shown on detail")
	}
}

func TestReshardNotFound(t *testing.T) {
	s, _ := newTestServer(t, reshardWorkflows(), reshardObjects()...)
	for _, path := range []string{"/reshards/ns1/nope", "/api/v1/reshards/ns1/nope", "/reshards/ns9/demo-reshard-g2"} {
		if rec := get(t, s, path); rec.Code != http.StatusNotFound {
			t.Errorf("%s: status %d", path, rec.Code)
		}
	}
}

func TestReshardsJSON(t *testing.T) {
	s, _ := newTestServer(t, reshardWorkflows(), reshardObjects()...)
	rec := get(t, s, "/api/v1/reshards")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
	var page ReshardsPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Clusters) != 2 || page.Clusters[0].Name != "demo" || page.Clusters[0].Active != "demo-reshard-g2" || len(page.Clusters[0].Reshards) != 2 {
		t.Fatalf("clusters: %+v", page.Clusters)
	}
	if page.Clusters[0].Reshards[0].Name != "demo-reshard-g3" || page.Clusters[0].Reshards[1].Name != "demo-reshard-g2" {
		t.Errorf("not newest first: %s, %s", page.Clusters[0].Reshards[0].Name, page.Clusters[0].Reshards[1].Name)
	}
	r := page.Clusters[0].Reshards[1]
	if !r.WorkflowFound || r.State != "running" || r.Stage != "switching" || r.CutoverPause != "1.2s" || r.JournalIDs[0] != "j-0001" || r.Message != "switching: step verify" || r.TargetsReady != 1 || len(r.JournalIDs) != 1 {
		t.Errorf("reshard: %+v", r)
	}
	if r.Progress == nil || r.Progress.Percent != 75 || !r.Progress.LagKnown || r.Progress.LagBytes != 2048 || r.Progress.Paused != 1 {
		t.Errorf("progress: %+v", r.Progress)
	}
	if len(r.Targets) != 2 || r.Targets[0].ShardID != 0 || r.Targets[1].ShardID != 1 || r.Targets[1].Progress == nil || r.Targets[1].Progress.LagKnown || r.Targets[1].Progress.Percent != 50 {
		t.Errorf("targets: %+v", r.Targets)
	}
	if r.Cutover == nil || r.Cutover.Verify == nil || r.Cutover.Verify.Rows != 12345 || r.Cutover.RetireAt == nil || !r.Cutover.RetireAt.Equal(reshardFenced.Add(3*time.Second+time.Hour)) {
		t.Errorf("cutover: %+v", r.Cutover)
	}
	if len(page.Placements) != 1 || page.Placements[0].Table != "public.orders" || page.Placements[0].To != "sharded(customer_id)" || page.Placements[0].Progress.Percent != 0 {
		t.Errorf("placements: %+v", page.Placements)
	}
	if len(page.Upgrades) != 1 || page.Upgrades[0].From != "18" || page.Upgrades[0].To != "19" || page.Upgrades[0].Stage != "planning" {
		t.Errorf("upgrades: %+v", page.Upgrades)
	}
	if page.Clusters[1].Reshards[0].Phase != "Pending" || page.Clusters[1].Reshards[0].StartedAt != nil {
		t.Errorf("defaults: %+v", page.Clusters[1].Reshards[0])
	}

	rec = get(t, s, "/api/v1/reshards/ns1/demo-reshard-g2")
	var one Reshard
	if err := json.Unmarshal(rec.Body.Bytes(), &one); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || one.Name != "demo-reshard-g2" || len(one.Timeline) != 5 || one.Timeline[0].Stage != "created" || one.Timeline[4].Stage != "switching" {
		t.Errorf("detail: %d %+v", rec.Code, one.Timeline)
	}
}

func TestReshardCancelledWorkflow(t *testing.T) {
	objs := reshardObjects()
	objs[len(objs)-2].(*pgshardv1alpha1.PgShardReshard).Status = pgshardv1alpha1.PgShardReshardStatus{Phase: "Cancelled", WorkflowID: "wf-cancelled"}
	s, _ := newTestServer(t, reshardWorkflows(), objs...)
	var one Reshard
	if err := json.Unmarshal(get(t, s, "/api/v1/reshards/ns1/demo-reshard-g3").Body.Bytes(), &one); err != nil {
		t.Fatal(err)
	}
	if one.CancelledAt == nil || !one.CancelledAt.Equal(reshardT1) || one.CancelReason != "shard set removed" || one.Cutover != nil || one.Progress != nil || one.CutoverPause != "" {
		t.Errorf("cancelled: %+v", one)
	}
	if len(one.JournalIDs) != 1 || one.JournalIDs[0] != "j-cancelled" || one.Message != "" {
		t.Errorf("cancelled: %+v", one)
	}
	if body := get(t, s, "/reshards/ns1/demo-reshard-g3").Body.String(); !strings.Contains(body, "<dd>2026-08-20T08:10:00Z · shard set removed</dd>") {
		t.Error("cancel timing not rendered")
	}
}

func TestReshardWorkflowWithBrokenStatus(t *testing.T) {
	src := reshardWorkflows()
	src.workflows[0].Status = json.RawMessage(`{"stage": 5}`)
	s, _ := newTestServer(t, src, reshardObjects()...)
	var one Reshard
	if err := json.Unmarshal(get(t, s, "/api/v1/reshards/ns1/demo-reshard-g2").Body.Bytes(), &one); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(one.Error, "workflow status:") || !one.WorkflowFound || one.Stage != "" {
		t.Errorf("broken status: %+v", one)
	}
}

func TestReshardCardsCountActiveAndLastPause(t *testing.T) {
	s, _ := newTestServer(t, nil, reshardObjects()...)
	body := get(t, s, "/").Body.String()
	for _, want := range []string{"Resharding in progress", `<div class="value">2</div>`, "Last cutover pause", `<div class="value">900ms</div>`, `href="/reshards/ns1/demo-reshard-g3"`, `href="/reshards">Reshards`} {
		if !strings.Contains(body, want) {
			t.Errorf("index missing %q", want)
		}
	}
	cards, err := BuildReshardCards(context.Background(), s.Client, "ns2")
	if err != nil {
		t.Fatal(err)
	}
	if cards.InProgress != 1 || cards.LastCutoverPause != "" || cards.LastCutover != "" {
		t.Errorf("ns2 cards: %+v", cards)
	}
	empty, _ := newTestServer(t, nil)
	if body := get(t, empty, "/").Body.String(); !strings.Contains(body, `<div class="value">0</div>`) || !strings.Contains(body, `<div class="value">—</div>`) {
		t.Error("empty cards")
	}
}

func TestPercent(t *testing.T) {
	for _, tc := range []struct{ done, total, want int }{{0, 0, 0}, {0, 10, 0}, {-1, 10, 0}, {3, 10, 30}, {1, 3, 33}, {10, 10, 100}, {12, 10, 100}, {5, 0, 0}} {
		if got := percent(tc.done, tc.total); got != tc.want {
			t.Errorf("percent(%d, %d) = %d, want %d", tc.done, tc.total, got, tc.want)
		}
	}
}

func TestReshardPagesEscapeUntrustedText(t *testing.T) {
	objs := reshardObjects()
	r := objs[len(objs)-2].(*pgshardv1alpha1.PgShardReshard)
	r.Status.Message = "<script>alert(1)</script>"
	r.Status.JournalIDs = []string{"<b>j</b>"}
	src := reshardWorkflows()
	src.workflows[1].Spec = mustRaw(map[string]any{"database": "<i>db</i>", "schema_name": "public", "table_name": "t", "from": map[string]any{"placement": "a"}, "to": map[string]any{"placement": "b"}})
	s, _ := newTestServer(t, src, objs...)
	for _, path := range []string{"/reshards", "/reshards/ns1/demo-reshard-g3"} {
		body := get(t, s, path).Body.String()
		if strings.Contains(body, "<script>alert") || strings.Contains(body, "<b>j</b>") || strings.Contains(body, "<i>db</i>") {
			t.Errorf("%s: unescaped content", path)
		}
		if !strings.Contains(body, "&lt;script&gt;") {
			t.Errorf("%s: message missing", path)
		}
	}
}
