package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

type fakeMigrations struct {
	fakeCatalog
	rows   []catalog.DDLMigration
	err    error
	counts catalog.MigrationCounts
	last   catalog.MigrationFilter
}

func (f *fakeMigrations) ListMigrations(_ context.Context, fl catalog.MigrationFilter) ([]catalog.DDLMigration, int, error) {
	f.last = fl
	if f.err != nil {
		return nil, 0, f.err
	}
	var match []catalog.DDLMigration
	for _, m := range f.rows {
		if (fl.Database == "" || m.Database == fl.Database) && (fl.State == "" || m.State == fl.State) {
			match = append(match, m)
		}
	}
	total := len(match)
	if fl.Offset > len(match) {
		fl.Offset = len(match)
	}
	match = match[fl.Offset:]
	if fl.Limit > 0 && len(match) > fl.Limit {
		match = match[:fl.Limit]
	}
	return match, total, nil
}

func (f *fakeMigrations) LoadMigration(_ context.Context, id string) (catalog.DDLMigration, error) {
	for _, m := range f.rows {
		if m.ID == id {
			return m, nil
		}
	}
	return catalog.DDLMigration{}, pgx.ErrNoRows
}

func (f *fakeMigrations) CountMigrations(context.Context) (catalog.MigrationCounts, error) {
	return f.counts, f.err
}

var t0 = time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)

func sampleMigrations() []catalog.DDLMigration {
	done := t0.Add(3 * time.Second)
	long := "CREATE TABLE very_long_table_name_for_truncation (" + strings.Repeat("c int, ", 20) + "id int)"
	return []catalog.DDLMigration{
		{ID: "m-degraded", Database: "app", Kind: "ALTER TABLE", Strategy: "direct", State: catalog.MigrationFailed, Statement: "ALTER TABLE t ADD COLUMN x int",
			CreatedAt: t0, FinishedAt: &done, Error: "shard 1: relation t does not exist",
			PerShard: map[string]catalog.ShardMigration{"0": {State: catalog.ShardApplied, Attempts: 1}, "1": {State: catalog.ShardFailed, Attempts: 3, Error: "relation t does not exist", SQLState: "42P01"}}},
		{ID: "m-multistep", Database: "app", Kind: "ALTER TABLE", Strategy: catalog.StrategyMultistep, State: catalog.MigrationRunning, Statement: long, CreatedAt: t0,
			Meta:     catalog.MigrationMeta{Steps: []catalog.MigrationStep{{SQL: "ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0) NOT VALID"}, {SQL: "ALTER TABLE t VALIDATE CONSTRAINT c"}}},
			PerShard: map[string]catalog.ShardMigration{"0": {State: catalog.ShardApplied, Step: 1}, "1": {State: catalog.ShardRetrying, Step: 1, Attempts: 2, Error: "lock timeout"}, "2": {State: catalog.ShardPending}}},
		{ID: "m-queued", Database: "other", Kind: "CREATE TABLE", Strategy: "direct", State: catalog.MigrationQueued, Statement: "CREATE TABLE z (id int)", CreatedAt: t0},
	}
}

func TestBuildMigrationRowProgressAndDegraded(t *testing.T) {
	ms := sampleMigrations()
	now := t0.Add(10 * time.Second)
	degraded := BuildMigrationRow(ms[0], now)
	if !degraded.Degraded || degraded.Progress != (ShardProgress{Applied: 1, Failed: 1, Total: 2}) || degraded.Duration != "3s" {
		t.Errorf("degraded row: %+v", degraded)
	}
	if len(degraded.Shards) != 2 || degraded.Shards[1].Error != "relation t does not exist" || degraded.Shards[1].Attempts != 3 {
		t.Errorf("shards: %+v", degraded.Shards)
	}
	if degraded.Progress.Percent(1) != 50 || degraded.Progress.FailedOffset() != 50 {
		t.Errorf("percent: %d %d", degraded.Progress.Percent(1), degraded.Progress.FailedOffset())
	}
	multi := BuildMigrationRow(ms[1], now)
	if multi.Degraded || multi.Progress != (ShardProgress{Applied: 1, Retrying: 1, Pending: 1, Total: 3}) || multi.CurrentStep != "2/2" || multi.Duration != "10s" {
		t.Errorf("multistep row: %+v", multi)
	}
	if !multi.Truncated || !strings.HasSuffix(multi.Short, "…") || len([]rune(multi.Short)) != statementPreview+1 {
		t.Errorf("truncation: %q", multi.Short)
	}
	if len(multi.Steps) != 2 || multi.Steps[0].Status != "partial" || multi.Steps[1].Status != "running" {
		t.Errorf("steps: %+v", multi.Steps)
	}
	queued := BuildMigrationRow(ms[2], now)
	if queued.Duration != "—" || queued.Progress.Total != 0 || queued.Degraded || queued.Steps != nil || queued.CurrentStep != "" {
		t.Errorf("queued row: %+v", queued)
	}
	allFailed := BuildMigrationRow(catalog.DDLMigration{State: catalog.MigrationFailed, CreatedAt: t0,
		PerShard: map[string]catalog.ShardMigration{"0": {State: catalog.ShardFailed}, "1": {State: catalog.ShardFailed}}}, now)
	if allFailed.Degraded {
		t.Error("all shards failed is not degraded")
	}
	failedStep := BuildMigrationRow(catalog.DDLMigration{State: catalog.MigrationFailed, CreatedAt: t0,
		Meta:     catalog.MigrationMeta{Steps: []catalog.MigrationStep{{SQL: "a"}, {SQL: "b"}, {SQL: "c"}}},
		PerShard: map[string]catalog.ShardMigration{"0": {State: catalog.ShardFailed, Step: 1}, "1": {State: catalog.ShardApplied}}}, now)
	if got := []string{failedStep.Steps[0].Status, failedStep.Steps[1].Status, failedStep.Steps[2].Status}; strings.Join(got, ",") != "done,failed,partial" || failedStep.CurrentStep != "" {
		t.Errorf("failed step statuses: %v current %q", got, failedStep.CurrentStep)
	}
	ordered := BuildMigrationRow(catalog.DDLMigration{CreatedAt: t0, PerShard: map[string]catalog.ShardMigration{"10": {State: catalog.ShardSkipped}, "2": {State: catalog.ShardRunning}}}, now)
	if ordered.Shards[0].Shard != "2" || ordered.Shards[1].Shard != "10" || ordered.Progress.Applied != 1 || ordered.Progress.Pending != 1 {
		t.Errorf("ordering/skipped: %+v", ordered)
	}
}

func TestMigrationsPageRendersFiltersAndPages(t *testing.T) {
	src := &fakeMigrations{rows: sampleMigrations(), counts: catalog.MigrationCounts{Running: 1, Queued: 1, Failed: 1}}
	s, _ := newTestServer(t, src, populated()...)
	rec := get(t, s, "/migrations")
	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, body)
	}
	for _, want := range []string{`href="/migrations/m-degraded"`, `href="/migrations/m-multistep"`, "m-queued", "DEGRADED", "step 2/2", "<details>", "3 migrations", `data-live`, `hx-get="/migrations/table?"`, "1 applied, 1 retrying, 0 failed, 1 pending of 3 shards", `<option value="failed">`, `aria-label="Sections"`} {
		if !strings.Contains(body, want) {
			t.Errorf("migrations page lacks %q", want)
		}
	}
	if strings.Contains(body, "style=") {
		t.Error("inline styles violate the CSP")
	}
	rec = get(t, s, "/migrations?database=other&state=queued")
	body = rec.Body.String()
	if src.last != (catalog.MigrationFilter{Database: "other", State: "queued", Limit: MigrationsPageSize}) {
		t.Errorf("filter passed to store: %+v", src.last)
	}
	if !strings.Contains(body, "m-queued") || strings.Contains(body, "m-degraded") || !strings.Contains(body, `<option value="queued" selected>`) || !strings.Contains(body, `value="other"`) {
		t.Errorf("filtered page: %s", body)
	}
	if rec := get(t, s, "/migrations?state=complete"); !strings.Contains(rec.Body.String(), "No migrations match") {
		t.Errorf("empty filter: %s", rec.Body)
	}
	if rec := get(t, s, "/api/v1/migrations?state=complete"); !strings.Contains(rec.Body.String(), `"pages": 1`) {
		t.Errorf("empty result must still be one page: %s", rec.Body)
	}
	if body := get(t, s, "/migrations").Body.String(); strings.Contains(body, ">previous<") || strings.Contains(body, ">next<") {
		t.Errorf("single page must have no paging links: %s", body)
	}
	many := &fakeMigrations{}
	for i := 0; i < MigrationsPageSize+1; i++ {
		many.rows = append(many.rows, catalog.DDLMigration{ID: "id-" + strings.Repeat("x", i), Database: "app", State: catalog.MigrationComplete, CreatedAt: t0})
	}
	paged, _ := newTestServer(t, many)
	rec = get(t, paged, "/migrations?database=app&page=2")
	body = rec.Body.String()
	if many.last.Offset != MigrationsPageSize || !strings.Contains(body, "page 2 of 2") || !strings.Contains(body, `href="/migrations?database=app">previous</a>`) || strings.Contains(body, ">next<") {
		t.Errorf("paging: offset %d body %s", many.last.Offset, body)
	}
	rec = get(t, paged, "/migrations/table?database=app")
	if strings.Contains(rec.Body.String(), "<html") || !strings.Contains(rec.Body.String(), `href="/migrations?database=app&amp;page=2">next</a>`) {
		t.Errorf("fragment: %s", rec.Body)
	}
	rec = get(t, s, "/api/v1/migrations")
	var page MigrationsPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil || page.Total != 3 || !page.Rows[0].Degraded {
		t.Errorf("JSON: %v %s", err, rec.Body)
	}
}

func TestMigrationDetailPage(t *testing.T) {
	src := &fakeMigrations{rows: sampleMigrations()}
	s, _ := newTestServer(t, src)
	rec := get(t, s, "/migrations/m-degraded")
	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	for _, want := range []string{`role="alert"`, "DEGRADED: the statement applied on 1 shard and failed on 1", "relation t does not exist", "<td>1</td><td class=\"state-failed\">failed</td><td>0</td><td>3</td>", "ALTER TABLE t ADD COLUMN x int", `hx-get="/migrations/m-degraded/detail"`} {
		if !strings.Contains(body, want) {
			t.Errorf("detail page lacks %q", want)
		}
	}
	rec = get(t, s, "/migrations/m-multistep")
	body = rec.Body.String()
	if strings.Contains(body, "DEGRADED") || !strings.Contains(body, `<li class="step-partial">`) || !strings.Contains(body, `<li class="step-running">`) || !strings.Contains(body, "lock timeout") {
		t.Errorf("multistep detail: %s", body)
	}
	if rec := get(t, s, "/migrations/m-multistep/detail"); strings.Contains(rec.Body.String(), "<html") || !strings.Contains(rec.Body.String(), "VALIDATE CONSTRAINT") {
		t.Errorf("detail fragment: %s", rec.Body)
	}
	if rec := get(t, s, "/migrations/nope"); rec.Code != http.StatusNotFound {
		t.Errorf("missing migration: %d", rec.Code)
	}
	if rec := get(t, s, "/api/v1/migrations/m-degraded"); !strings.Contains(rec.Body.String(), `"degraded": true`) {
		t.Errorf("detail JSON: %s", rec.Body)
	}
}

func TestMigrationsWithoutCatalogAndOnError(t *testing.T) {
	s, _ := newTestServer(t, nil)
	if rec := get(t, s, "/migrations"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "without a catalog DSN") {
		t.Errorf("no catalog: %d %s", rec.Code, rec.Body)
	}
	if rec := get(t, s, "/migrations/x"); rec.Code != http.StatusNotFound {
		t.Errorf("no catalog detail: %d", rec.Code)
	}
	broken, _ := newTestServer(t, &fakeMigrations{err: errors.New("connection refused")})
	if rec := get(t, broken, "/migrations"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "catalog: connection refused") {
		t.Errorf("store error: %d %s", rec.Code, rec.Body)
	}
}

func TestTopologyDDLCard(t *testing.T) {
	src := &fakeMigrations{counts: catalog.MigrationCounts{Running: 2, Queued: 3, Failed: 1}}
	s, _ := newTestServer(t, src, populated()...)
	body := get(t, s, "/clusters/ns1/demo").Body.String()
	if !strings.Contains(body, `<span class="state-running">2</span> running · <span class="state-queued">3</span> queued · <span class="state-failed">1</span> failed`) || !strings.Contains(body, `href="/migrations">DDL</a>`) {
		t.Errorf("DDL card: %s", body)
	}
	var top Topology
	if err := json.Unmarshal(get(t, s, "/api/v1/clusters/ns1/demo").Body.Bytes(), &top); err != nil || top.DDL == nil || top.DDL.Counts.Queued != 3 {
		t.Errorf("DDL in JSON: %v %+v", err, top.DDL)
	}
	plain, _ := newTestServer(t, fakeCatalog{}, populated()...)
	if body := get(t, plain, "/clusters/ns1/demo").Body.String(); strings.Contains(body, "ddl-card") {
		t.Error("DDL card shown without a migration source")
	}
	broken, _ := newTestServer(t, &fakeMigrations{err: errors.New("down")}, populated()...)
	if body := get(t, broken, "/clusters/ns1/demo/topology").Body.String(); !strings.Contains(body, "catalog: down") {
		t.Errorf("DDL card error: %s", body)
	}
}
