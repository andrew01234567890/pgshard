package admin

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/controller"
	"github.com/andrew01234567890/pgshard/internal/operator"
)

func intp(i int) *int { return &i }

func populated() []client.Object {
	shard := 0
	return []client.Object{
		&pgshardv1alpha1.PgShardCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns1"},
			Spec:       pgshardv1alpha1.PgShardClusterSpec{PostgreSQL: pgshardv1alpha1.PostgreSQLSpec{Major: 18}, Shards: intp(1)},
			Status: pgshardv1alpha1.PgShardClusterStatus{
				ShardMapGeneration: 7,
				Conditions:         []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Ready", Message: "all good"}},
				Shards:             []pgshardv1alpha1.ShardStatus{{ID: 0, Primary: "demo-shard-0-0", Epoch: 3, RangeEnd: 100}},
			},
		},
		&pgshardv1alpha1.PgShardGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-shard-0", Namespace: "ns1", Labels: map[string]string{operator.LabelCluster: "demo"}},
			Spec:       pgshardv1alpha1.PgShardGroupSpec{ClusterRef: "demo", Kind: "shard", ShardID: &shard},
			Status: pgshardv1alpha1.PgShardGroupStatus{Primary: "demo-shard-0-0", Epoch: 3, Members: []pgshardv1alpha1.MemberStatus{
				{Name: "demo-shard-0-0", Role: "primary", Ready: true},
				{Name: "demo-shard-0-1", Role: "replica", Ready: false, ReplayLagBytes: 2048},
			}},
		},
		&pgshardv1alpha1.PgShardGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-catalog", Namespace: "ns1", Labels: map[string]string{operator.LabelCluster: "demo"}},
			Spec:       pgshardv1alpha1.PgShardGroupSpec{ClusterRef: "demo", Kind: "catalog"},
			Status:     pgshardv1alpha1.PgShardGroupStatus{Primary: "demo-catalog-0", Members: []pgshardv1alpha1.MemberStatus{{Name: "demo-catalog-0", Role: "primary", Ready: true}}},
		},
		&pgshardv1alpha1.PgShardGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "other-catalog", Namespace: "ns1", Labels: map[string]string{operator.LabelCluster: "other"}},
			Spec:       pgshardv1alpha1.PgShardGroupSpec{ClusterRef: "other", Kind: "catalog"},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-shard-0-1", Namespace: "ns1", Labels: map[string]string{operator.LabelCluster: "demo"}},
			Spec:       corev1.PodSpec{NodeName: "worker-2"},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		},
	}
}

func newTestServer(t *testing.T, src CatalogSource, objs ...client.Object) (*Server, *Notifier) {
	t.Helper()
	scheme, err := operator.NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	n := NewNotifier()
	s, err := NewServer(c, src, n, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.Tick = time.Hour
	return s, n
}

func get(t *testing.T, s http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestTopologyJSONReflectsGroupStatus(t *testing.T) {
	s, _ := newTestServer(t, nil, populated()...)
	rec := get(t, s, "/api/v1/clusters/ns1/demo")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Content-Security-Policy"); !strings.HasPrefix(got, "default-src 'none'") {
		t.Errorf("CSP header %q", got)
	}
	var top Topology
	if err := json.Unmarshal(rec.Body.Bytes(), &top); err != nil {
		t.Fatal(err)
	}
	if top.ShardMapGeneration != 7 || top.PostgresMajor != 18 {
		t.Errorf("header fields: %+v", top)
	}
	if len(top.Groups) != 2 || top.Groups[0].Kind != "catalog" || top.Groups[1].Name != "demo-shard-0" {
		t.Fatalf("groups: %+v", top.Groups)
	}
	shard := top.Groups[1]
	if shard.Primary != "demo-shard-0-0" || shard.Epoch != 3 || len(shard.Members) != 2 {
		t.Fatalf("shard group: %+v", shard)
	}
	replica := shard.Members[1]
	if replica.Role != "replica" || replica.Ready || replica.ReplayLagBytes != 2048 || replica.PodPhase != "Running" || replica.Node != "worker-2" || replica.Epoch != 3 {
		t.Errorf("replica member: %+v", replica)
	}
	if shard.Members[0].PodPhase != "" {
		t.Errorf("member without a pod must have an empty phase: %+v", shard.Members[0])
	}
	if len(top.Shards) != 1 || top.Shards[0].RangeEnd != 100 || top.Shards[0].Epoch != 3 {
		t.Errorf("shard map: %+v", top.Shards)
	}
	if len(top.Conditions) != 1 || top.Conditions[0].Status != "True" {
		t.Errorf("conditions: %+v", top.Conditions)
	}
	if top.Catalog != nil || top.CatalogError != "" {
		t.Errorf("catalog must be absent without a DSN: %+v", top)
	}
}

type fakeCatalog struct {
	rows   []catalog.ShardStatus
	err    error
	points []controller.RestorePoint
	rpErr  error
}

func (f fakeCatalog) RestorePoints(context.Context) ([]controller.RestorePoint, error) {
	return f.points, f.rpErr
}

func (f fakeCatalog) ShardStatus(context.Context) ([]catalog.ShardStatus, error) {
	return f.rows, f.err
}

func TestTopologyIncludesCatalogSnapshot(t *testing.T) {
	lag := int64(4096)
	src := fakeCatalog{rows: []catalog.ShardStatus{{ShardSet: "default", ShardID: 0, GroupName: "demo-shard-0", ServingState: "serving", PrimaryEpoch: 3, ReplayLagBytes: &lag}}}
	s, _ := newTestServer(t, src, populated()...)
	rec := get(t, s, "/api/v1/clusters/ns1/demo")
	var top Topology
	if err := json.Unmarshal(rec.Body.Bytes(), &top); err != nil {
		t.Fatal(err)
	}
	if len(top.Catalog) != 1 || top.Catalog[0].ServingState != "serving" || *top.Catalog[0].ReplayLagBytes != 4096 {
		t.Fatalf("catalog: %+v", top.Catalog)
	}
	page := get(t, s, "/clusters/ns1/demo")
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "serving") || !strings.Contains(page.Body.String(), "4.0 KiB") {
		t.Errorf("page: %d %s", page.Code, page.Body)
	}
	broken, _ := newTestServer(t, fakeCatalog{err: context.DeadlineExceeded}, populated()...)
	rec = get(t, broken, "/api/v1/clusters/ns1/demo")
	if err := json.Unmarshal(rec.Body.Bytes(), &top); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || top.CatalogError == "" {
		t.Errorf("catalog failure must be reported, not fatal: %d %+v", rec.Code, top)
	}
}

func TestPagesRenderEmptyAndPopulated(t *testing.T) {
	empty, _ := newTestServer(t, nil)
	if rec := get(t, empty, "/"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "No PgShardCluster") {
		t.Errorf("empty index: %d %s", rec.Code, rec.Body)
	}
	if rec := get(t, empty, "/clusters/ns1/missing"); rec.Code != http.StatusNotFound {
		t.Errorf("missing cluster: %d", rec.Code)
	}
	bare, _ := newTestServer(t, nil, &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "bare", Namespace: "ns2"}})
	if rec := get(t, bare, "/clusters/ns2/bare"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "No groups reported yet") {
		t.Errorf("bare cluster page: %d %s", rec.Code, rec.Body)
	}
	if rec := get(t, bare, "/api/v1/clusters/ns2/bare"); !strings.Contains(rec.Body.String(), `"groups": []`) {
		t.Errorf("bare cluster JSON must have empty arrays, not null: %s", rec.Body)
	}

	full, _ := newTestServer(t, nil, populated()...)
	rec := get(t, full, "/")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `href="/clusters/ns1/demo"`) {
		t.Errorf("index: %d %s", rec.Code, rec.Body)
	}
	rec = get(t, full, "/clusters/ns1/demo")
	body := rec.Body.String()
	for _, want := range []string{"demo", `id="shard-map-generation">7<`, "demo-shard-0-1", "worker-2", "2.0 KiB", "Running", "/static/htmx.min.js", "hx-trigger=\"topology\""} {
		if !strings.Contains(body, want) {
			t.Errorf("cluster page lacks %q", want)
		}
	}
	if rec.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Errorf("content type %q", rec.Header().Get("Content-Type"))
	}
	if rec := get(t, full, "/clusters/ns1/demo/topology"); rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "<html") || !strings.Contains(rec.Body.String(), "demo-shard-0-0") {
		t.Errorf("fragment: %d %s", rec.Code, rec.Body)
	}
	for _, path := range []string{"/static/htmx.min.js", "/static/app.js", "/static/style.css"} {
		if rec := get(t, full, path); rec.Code != http.StatusOK {
			t.Errorf("%s: %d", path, rec.Code)
		}
	}
	if rec := get(t, full, "/api/v1/clusters"); !strings.Contains(rec.Body.String(), `"name": "demo"`) {
		t.Errorf("clusters API: %s", rec.Body)
	}
}

func TestNamespaceScopesClusterList(t *testing.T) {
	scheme, _ := operator.NewScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns1"}},
		&pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns2"}},
	).Build()
	s, err := NewServer(c, nil, nil, "ns2", nil)
	if err != nil {
		t.Fatal(err)
	}
	rec := get(t, s, "/api/v1/clusters")
	var list []ClusterSummary
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != "b" {
		t.Errorf("list: %+v", list)
	}
}

func TestEventsStreamEmitsOnNotify(t *testing.T) {
	s, n := newTestServer(t, nil, populated()...)
	ts := httptest.NewServer(s)
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	r := bufio.NewReader(resp.Body)
	readEvent := func() string {
		var lines []string
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				t.Fatalf("read: %v (got %q)", err, lines)
			}
			line = strings.TrimRight(line, "\n")
			if line == "" {
				return strings.Join(lines, "\n")
			}
			lines = append(lines, line)
		}
	}
	first := readEvent()
	if !strings.Contains(first, "event: topology") || !strings.Contains(first, `"reason":"connected"`) {
		t.Fatalf("first event: %q", first)
	}
	got := make(chan string, 1)
	go func() { got <- readEvent() }()
	select {
	case ev := <-got:
		t.Fatalf("unexpected event before notify: %q", ev)
	case <-time.After(200 * time.Millisecond):
	}
	n.Notify()
	select {
	case ev := <-got:
		if !strings.Contains(ev, "id: 2") || !strings.Contains(ev, `"reason":"update"`) {
			t.Fatalf("update event: %q", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no event after Notify")
	}
}

func TestEventsStreamTicks(t *testing.T) {
	s, _ := newTestServer(t, nil)
	s.Tick = 50 * time.Millisecond
	ts := httptest.NewServer(s)
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, 4096)
	var seen string
	for !strings.Contains(seen, `"reason":"tick"`) {
		n, err := resp.Body.Read(buf)
		if err != nil {
			t.Fatalf("read: %v (seen %q)", err, seen)
		}
		seen += string(buf[:n])
	}
}

func TestNotifierCoalescesAndUnsubscribes(t *testing.T) {
	n := NewNotifier()
	ch, cancel := n.Subscribe()
	n.Notify()
	n.Notify()
	<-ch
	select {
	case <-ch:
		t.Fatal("ticks must coalesce")
	default:
	}
	cancel()
	n.Notify()
	select {
	case <-ch:
		t.Fatal("unsubscribed channel must not receive")
	default:
	}
}

func TestParseFlags(t *testing.T) {
	o, err := ParseFlags([]string{"--listen", ":9000", "--namespace", "x", "--catalog-dsn", "postgres://c/x", "--kubeconfig", "kc", "--token-file", "/etc/t"}, io.Discard)
	if err != nil || o.Listen != ":9000" || o.Namespace != "x" || o.CatalogDSN != "postgres://c/x" || o.Kubeconfig != "kc" || o.TokenFile != "/etc/t" {
		t.Fatalf("%+v %v", o, err)
	}
	o, err = ParseFlags([]string{"--insecure-no-auth"}, io.Discard)
	if err != nil || o.Listen != ":8081" || o.Namespace != "" || !o.InsecureNoAuth {
		t.Fatalf("defaults: %+v %v", o, err)
	}
	// Serving the cluster's operational detail to anyone who can reach it
	// is a choice, so it has to be made rather than fallen into.
	if _, err := ParseFlags(nil, io.Discard); err == nil {
		t.Fatal("neither a token nor the explicit no-auth flag must be rejected")
	}
	if _, err := ParseFlags([]string{"--token-file", "/etc/t", "--insecure-no-auth"}, io.Discard); err == nil {
		t.Fatal("a token and no-auth together must be rejected")
	}
	if _, err := ParseFlags([]string{"extra"}, io.Discard); err == nil {
		t.Fatal("positional argument must be rejected")
	}
	if _, err := ParseFlags([]string{"--nope"}, io.Discard); err == nil {
		t.Fatal("unknown flag must be rejected")
	}
}
