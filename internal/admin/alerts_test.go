package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/controller"
)

type fakeTwoPC struct {
	decisions []TwoPCRow
	paused    []WorkflowRow
	err       error
}

func (f fakeTwoPC) ListDecisions(context.Context) ([]TwoPCRow, error) {
	return f.decisions, f.err
}

func (f fakeTwoPC) ListPausedWorkflows(context.Context) ([]WorkflowRow, error) {
	return f.paused, f.err
}

func (fakeTwoPC) RestorePoints(context.Context) ([]controller.RestorePoint, error) { return nil, nil }
func (fakeTwoPC) ShardStatus(context.Context) ([]catalog.ShardStatus, error)       { return nil, nil }

func at(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestBuildTwoPCView(t *testing.T) {
	now := at("2026-08-24T12:00:00Z")
	decided := at("2026-08-24T11:59:00Z")
	src := fakeTwoPC{decisions: []TwoPCRow{
		{GID: "pgshard-1", State: "preparing", Participants: []int32{0, 1}, CreatedAt: now.Add(-10 * time.Minute)},
		{GID: "pgshard-2", State: "commit", Participants: []int32{1, 2}, CreatedAt: now.Add(-30 * time.Second), DecidedAt: &decided},
	}}
	v, err := BuildTwoPCView(context.Background(), src, now)
	if err != nil {
		t.Fatal(err)
	}
	if v.Count != 2 || v.InDoubt != 1 {
		t.Fatalf("Count=%d InDoubt=%d", v.Count, v.InDoubt)
	}
	if v.OldestAge != "10m0s" {
		t.Fatalf("OldestAge = %q", v.OldestAge)
	}
	if v.Entries[0].Decision != "undecided" || v.Entries[1].Decision != "commit" {
		t.Fatalf("decisions = %q, %q", v.Entries[0].Decision, v.Entries[1].Decision)
	}
	if v.Entries[1].AgeText != "30s" {
		t.Fatalf("AgeText = %q", v.Entries[1].AgeText)
	}
}

func TestBuildTwoPCViewWithoutSource(t *testing.T) {
	if _, err := BuildTwoPCView(context.Background(), nil, time.Now()); !errors.Is(err, ErrNoTwoPCSource) {
		t.Fatalf("err = %v", err)
	}
}

func TestDeriveAlerts(t *testing.T) {
	now := at("2026-08-24T12:00:00Z")
	stale := now.Add(-30 * time.Hour)
	fresh := now.Add(-time.Hour)
	cases := []struct {
		name string
		in   AlertInputs
		want []string
	}{
		{"quiet", AlertInputs{Now: now, BackupsKnown: true, LatestBackup: &fresh}, nil},
		{"in-doubt aged", AlertInputs{Now: now, Decisions: []TwoPCRow{
			{State: "preparing", CreatedAt: now.Add(-6 * time.Minute)}}}, []string{"TwoPCInDoubtAged"}},
		{"in-doubt young stays quiet", AlertInputs{Now: now, Decisions: []TwoPCRow{
			{State: "preparing", CreatedAt: now.Add(-time.Minute)}}}, nil},
		// A decided row is deleted when the resolver finishes it, so one
		// that outlives its decision is a transaction it cannot finish,
		// holding locks, WAL and a vacuum horizon while it waits.
		{"decided and unfinished", AlertInputs{Now: now, Decisions: []TwoPCRow{
			{GID: "g1", State: "commit", CreatedAt: now.Add(-2 * time.Hour), DecidedAt: ptrTime(now.Add(-time.Hour))}}},
			[]string{"TwoPCDecidedUnfinished"}},
		{"decided moments ago stays quiet", AlertInputs{Now: now, Decisions: []TwoPCRow{
			{GID: "g2", State: "commit", CreatedAt: now.Add(-time.Hour), DecidedAt: ptrTime(now.Add(-time.Second))}}}, nil},
		{"an aborted row waits the same way", AlertInputs{Now: now, Decisions: []TwoPCRow{
			{GID: "g3", State: "abort", CreatedAt: now.Add(-2 * time.Hour), DecidedAt: ptrTime(now.Add(-time.Hour))}}},
			[]string{"TwoPCDecidedUnfinished"}},
		{"slot lost", AlertInputs{Now: now, Streams: []StreamSummary{{Name: "s", LostSlots: 1}}}, []string{"StreamSlotLost"}},
		{"cutover pause", AlertInputs{Now: now, Paused: []WorkflowRow{
			{ID: "w1", Kind: "reshard", UpdatedAt: now.Add(-time.Hour)}}}, []string{"CutoverPauseExceeded"}},
		{"fresh pause stays quiet", AlertInputs{Now: now, Paused: []WorkflowRow{
			{ID: "w1", Kind: "reshard", UpdatedAt: now.Add(-time.Minute)}}}, nil},
		{"backup stale", AlertInputs{Now: now, BackupsKnown: true, LatestBackup: &stale}, []string{"BackupStale"}},
		{"backup missing", AlertInputs{Now: now, BackupsKnown: true}, []string{"BackupMissing"}},
		{"backups unknown stays quiet", AlertInputs{Now: now}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveAlerts(tc.in)
			var names []string
			for _, a := range got {
				names = append(names, a.Name)
			}
			if len(names) != len(tc.want) {
				t.Fatalf("alerts = %v, want %v", names, tc.want)
			}
			for i := range names {
				if names[i] != tc.want[i] {
					t.Fatalf("alerts = %v, want %v", names, tc.want)
				}
			}
		})
	}
}

func TestTwoPCHandlers(t *testing.T) {
	src := fakeTwoPC{decisions: []TwoPCRow{{GID: "pgshard-42", State: "preparing", Participants: []int32{0, 1}, CreatedAt: time.Now().Add(-time.Minute)}}}
	s, _ := newTestServer(t, src, populated()...)

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/twopc", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "pgshard-42") {
		t.Fatalf("GET /twopc = %d\n%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/twopc", nil))
	var v TwoPCView
	if err := json.NewDecoder(rec.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	if v.InDoubt != 1 || v.Entries[0].GID != "pgshard-42" {
		t.Fatalf("api view = %+v", v)
	}
}

func TestAlertsHandlers(t *testing.T) {
	src := fakeTwoPC{
		decisions: []TwoPCRow{{GID: "old", State: "preparing", CreatedAt: time.Now().Add(-time.Hour)}},
		paused:    []WorkflowRow{{ID: "w1", Kind: "reshard", State: "paused", UpdatedAt: time.Now().Add(-time.Hour)}},
	}
	s, _ := newTestServer(t, src, populated()...)

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/alerts", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "TwoPCInDoubtAged") {
		t.Fatalf("GET /alerts = %d\n%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/alerts", nil))
	var v AlertsView
	if err := json.NewDecoder(rec.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, a := range v.Alerts {
		names = append(names, a.Name)
	}
	want := []string{"TwoPCInDoubtAged", "CutoverPauseExceeded", "BackupMissing"}
	if len(names) != len(want) {
		t.Fatalf("alerts = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("alerts = %v, want %v", names, want)
		}
	}
}

func TestTwoPCWithoutCatalogIs503(t *testing.T) {
	s, _ := newTestServer(t, nil, populated()...)
	for _, path := range []string{"/twopc", "/alerts", "/api/v1/twopc", "/api/v1/alerts"} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("GET %s = %d, want 503", path, rec.Code)
		}
	}
}

func TestUnknownPanelIs404(t *testing.T) {
	s, _ := newTestServer(t, nil, populated()...)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/twopc/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /twopc/nope = %d, want 404", rec.Code)
	}
}

func TestAdminServesMetrics(t *testing.T) {
	s, _ := newTestServer(t, nil, populated()...)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `pgshard_build_info{process="admin"`) {
		t.Fatalf("GET /metrics = %d", rec.Code)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
