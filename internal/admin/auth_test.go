package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
