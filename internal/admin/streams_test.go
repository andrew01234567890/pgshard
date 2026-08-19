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
)

type fakeStreams struct {
	fakeCatalog
	streams []catalog.Stream
	status  []catalog.StreamStatus
	err     error
}

func (f fakeStreams) Streams(context.Context) ([]catalog.Stream, error) { return f.streams, f.err }

func (f fakeStreams) StreamStatus(_ context.Context, stream string) ([]catalog.StreamStatus, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []catalog.StreamStatus
	for _, r := range f.status {
		if stream == "" || r.Stream == stream {
			out = append(out, r)
		}
	}
	return out, nil
}

func streamFixture() fakeStreams {
	created := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	return fakeStreams{
		streams: []catalog.Stream{
			{Name: "orders", Database: "app", TwoPhase: true, State: catalog.StreamLost, CreatedAt: created},
			{Name: "audit", Database: "app", State: catalog.StreamActive, CreatedAt: created},
		},
		status: []catalog.StreamStatus{
			{Stream: "orders", ShardSet: "default", ShardID: 0, Slot: "pgshard_orders_shard0", WALStatus: "reserved", Active: true, RestartLSN: 0x100000010, ConfirmedFlushLSN: 0x100000020, RetainedBytes: 4096, Synced: true, Failover: true, UpdatedAt: created},
			{Stream: "orders", ShardSet: "default", ShardID: 1, Slot: "pgshard_orders_shard1", WALStatus: "lost", InvalidationReason: "wal_removed", RetainedBytes: 1 << 30, Failover: true, UpdatedAt: created},
			{Stream: "audit", ShardSet: "default", ShardID: 0, Slot: "pgshard_audit_shard0", WALStatus: "reserved", Active: true, Synced: true, Failover: true, UpdatedAt: created},
		},
	}
}

func TestStreamsListPage(t *testing.T) {
	s, _ := newTestServer(t, streamFixture(), populated()...)
	rec := get(t, s, "/streams")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{`<a href="/streams/orders">orders</a>`, `<a href="/streams/audit">audit</a>`, "1 / 1", "1.0 GiB", `role="alert"`, "2026-08-01 12:00"} {
		if !strings.Contains(body, want) {
			t.Errorf("list page lacks %q:\n%s", want, body)
		}
	}
	if strings.Index(body, "/streams/audit") > strings.Index(body, "/streams/orders") {
		t.Error("streams must be sorted by name")
	}
	req := httptest.NewRequest(http.MethodGet, "/streams", nil)
	req.Header.Set("HX-Request", "true")
	frag := httptest.NewRecorder()
	s.ServeHTTP(frag, req)
	if strings.Contains(frag.Body.String(), "<html") || !strings.Contains(frag.Body.String(), "<table") {
		t.Errorf("fragment must be the list only:\n%s", frag.Body)
	}
}

func TestStreamDetailPageShowsSlotsAndLostBanner(t *testing.T) {
	s, _ := newTestServer(t, streamFixture(), populated()...)
	rec := get(t, s, "/streams/orders")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{`<div role="alert" class="alert">LOST: 1 slot invalidated`, "<code>1/10</code>", "<code>1/20</code>", "wal_removed", "pgshard_orders_shard1", "default/1", `<td class="bad">lost</td>`, "2026-08-01 12:00:00", "4.0 KiB"} {
		if !strings.Contains(body, want) {
			t.Errorf("detail page lacks %q:\n%s", want, body)
		}
	}
	healthy := get(t, s, "/streams/audit")
	if strings.Contains(healthy.Body.String(), `role="alert"`) {
		t.Error("healthy stream must not show the LOST banner")
	}
}

func TestStreamDetailEscapesHTML(t *testing.T) {
	f := streamFixture()
	f.status[1].InvalidationReason = "<script>alert(1)</script>"
	s, _ := newTestServer(t, f, populated()...)
	body := get(t, s, "/streams/orders").Body.String()
	if strings.Contains(body, "<script>alert") || !strings.Contains(body, "&lt;script&gt;") {
		t.Errorf("invalidation reason must be escaped:\n%s", body)
	}
}

func TestStreamNotFoundAndUnavailable(t *testing.T) {
	s, _ := newTestServer(t, streamFixture(), populated()...)
	for _, path := range []string{"/streams/nope", "/api/v1/streams/nope"} {
		if rec := get(t, s, path); rec.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", path, rec.Code)
		}
	}
	noSrc, _ := newTestServer(t, nil, populated()...)
	for _, path := range []string{"/streams", "/streams/orders", "/api/v1/streams"} {
		if rec := get(t, noSrc, path); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s without a catalog: status %d, want 503", path, rec.Code)
		}
	}
	failing := streamFixture()
	failing.err = errors.New("boom")
	broken, _ := newTestServer(t, failing, populated()...)
	if rec := get(t, broken, "/streams"); rec.Code != http.StatusInternalServerError {
		t.Errorf("catalog failure: status %d", rec.Code)
	}
}

func TestStreamsJSON(t *testing.T) {
	s, _ := newTestServer(t, streamFixture(), populated()...)
	rec := get(t, s, "/api/v1/streams")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
	var ov StreamsOverview
	if err := json.Unmarshal(rec.Body.Bytes(), &ov); err != nil {
		t.Fatal(err)
	}
	if ov.Count != 2 || ov.Lost != 1 || len(ov.Streams) != 2 || ov.Streams[0].Name != "audit" {
		t.Fatalf("overview: %+v", ov)
	}
	orders := ov.Streams[1]
	if !orders.Lost || orders.LostSlots != 1 || orders.ActiveSlots != 1 || orders.InactiveSlots != 1 || orders.MaxRetainedBytes != 1<<30 || orders.Synced || !orders.TwoPhase || orders.Shards != 2 {
		t.Errorf("orders summary: %+v", orders)
	}
	if audit := ov.Streams[0]; audit.Lost || !audit.Synced || audit.ActiveSlots != 1 || audit.MaxRetainedBytes != 0 {
		t.Errorf("audit summary: %+v", audit)
	}

	rec = get(t, s, "/api/v1/streams/orders")
	var d StreamDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if d.Name != "orders" || len(d.Slots) != 2 || d.Slots[0].RestartLSN != "1/10" || d.Slots[0].ConfirmedFlushLSN != "1/20" || d.Slots[0].RetainedBytes != 4096 || !d.Slots[0].Synced || !d.Slots[1].Lost || d.Slots[1].InvalidationReason != "wal_removed" || !d.Slots[1].Failover {
		t.Errorf("detail: %+v", d)
	}
}

func TestIndexShowsStreamsCard(t *testing.T) {
	s, _ := newTestServer(t, streamFixture(), populated()...)
	body := get(t, s, "/").Body.String()
	if !strings.Contains(body, `<a href="/streams">Change streams</a>`) || !strings.Contains(body, "2 streams") || !strings.Contains(body, `<span class="bad">1 lost</span>`) {
		t.Errorf("index lacks the streams card:\n%s", body)
	}
	plain, _ := newTestServer(t, nil, populated()...)
	if strings.Contains(get(t, plain, "/").Body.String(), "Change streams") {
		t.Error("index without a catalog must not show the streams card")
	}
	failing := streamFixture()
	failing.err = errors.New("boom")
	broken, _ := newTestServer(t, failing, populated()...)
	if rec := get(t, broken, "/"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Streams unavailable: boom") {
		t.Errorf("index must still render when the catalog fails: %d %s", rec.Code, rec.Body)
	}
}

func TestFormatLSN(t *testing.T) {
	if got := FormatLSN(0); got != "0/0" {
		t.Fatal(got)
	}
	if got := FormatLSN(0x1A2B3C4D5E6F); got != "1A2B/3C4D5E6F" {
		t.Fatal(got)
	}
}
