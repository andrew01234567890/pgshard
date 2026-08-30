package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// TestAdminRequiresItsCredential: everything the admin serves is
// operational detail about the cluster -- topology, backup and restore
// state, stream positions, two-phase commit identifiers, the text of DDL --
// so a reachable workload with no credential must see none of it. The
// liveness probe is the exception: requiring the token for it would make
// the credential a dependency of the pod staying up.
func TestAdminRequiresItsCredential(t *testing.T) {
	s, _ := newTestServer(t, nil, populated()...)
	s.Token = "sesame"

	for _, path := range []string{"/", "/api/v1/clusters", "/migrations", "/streams", "/twopc", "/alerts", "/metrics"} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s without a credential: %d, want 401", path, rec.Code)
		}
		if got := rec.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Basic") {
			t.Errorf("%s: WWW-Authenticate %q, want a Basic challenge so a browser can log in", path, got)
		}
	}

	// The event stream too, asked for with a context already done so that
	// a handler reached by mistake returns instead of streaming for ever.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil).WithContext(ctx))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("/events without a credential: %d, want 401", rec.Code)
	}

	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz: %d, want 200 without a credential", rec.Code)
	}

	for name, auth := range map[string]func(*http.Request){
		"bearer": func(r *http.Request) { r.Header.Set("Authorization", "Bearer sesame") },
		"basic":  func(r *http.Request) { r.SetBasicAuth("anyone", "sesame") },
	} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/clusters", nil)
		auth(req)
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: %d, want 200", name, rec.Code)
		}
	}

	for name, auth := range map[string]func(*http.Request){
		"wrong bearer":   func(r *http.Request) { r.Header.Set("Authorization", "Bearer open") },
		"wrong basic":    func(r *http.Request) { r.SetBasicAuth("anyone", "open") },
		"a prefix of it": func(r *http.Request) { r.Header.Set("Authorization", "Bearer sesam") },
	} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/clusters", nil)
		auth(req)
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: %d, want 401", name, rec.Code)
		}
	}
}

// TestAScopedAdminServesOneCluster: the operator deploys one admin per
// cluster and gives each its own credential, but the admin served every
// cluster in its namespace -- so whoever could read one cluster's admin
// could read all of them. --cluster is what makes that credential mean
// what its Secret implies.
func TestAScopedAdminServesOneCluster(t *testing.T) {
	objs := []client.Object{
		&pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "mine", Namespace: "ns1"}},
		&pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "theirs", Namespace: "ns1"}},
		&pgshardv1alpha1.PgShardBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "mine-full", Namespace: "ns1"},
			Spec:       pgshardv1alpha1.PgShardBackupSpec{ClusterName: "mine"},
		},
		&pgshardv1alpha1.PgShardBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "theirs-full", Namespace: "ns1"},
			Spec:       pgshardv1alpha1.PgShardBackupSpec{ClusterName: "theirs"},
		},
	}
	s, _ := newTestServer(t, nil, objs...)
	s.Namespace = "ns1"
	s.Cluster = "mine"

	// Nothing the admin serves may name the cluster it does not serve --
	// not a page, not a fragment, not an API response.
	for _, path := range []string{"/", "/api/v1/clusters", "/backups", "/api/v1/backups", "/backups/panel", "/upgrades", "/api/v1/upgrades", "/reshards", "/api/v1/reshards"} {
		rec := get(t, s, path)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d", path, rec.Code)
			continue
		}
		if body := rec.Body.String(); strings.Contains(body, "theirs") {
			t.Errorf("%s names a cluster this admin does not serve", path)
		}
	}

	// An object of another cluster reads as absent, not as forbidden: an
	// admin that serves one cluster has nothing to say about what else
	// exists.
	for _, path := range []string{"/clusters/ns1/theirs", "/clusters/ns1/theirs/topology", "/api/v1/clusters/ns1/theirs", "/backups/ns1/theirs-full"} {
		if rec := get(t, s, path); rec.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", path, rec.Code)
		}
	}

	// Its own cluster still works.
	for _, path := range []string{"/clusters/ns1/mine", "/api/v1/clusters/ns1/mine", "/backups/ns1/mine-full"} {
		if rec := get(t, s, path); rec.Code != http.StatusOK {
			t.Errorf("%s: status %d, want the cluster this admin serves", path, rec.Code)
		}
	}

	// An admin with no --cluster keeps serving the namespace, which is how
	// one run by hand across several clusters still works.
	unscoped, _ := newTestServer(t, nil, objs...)
	unscoped.Namespace = "ns1"
	if body := get(t, unscoped, "/api/v1/clusters").Body.String(); !strings.Contains(body, "theirs") {
		t.Errorf("an unscoped admin must still list every cluster: %s", body)
	}
}
