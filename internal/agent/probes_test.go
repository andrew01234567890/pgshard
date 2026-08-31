package agent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type fakeHealth struct {
	// roleErr makes reading the role fail, as a permission or I/O error
	// on PGDATA does.
	roleErr  error
	primary  bool
	started  error
	writes   error
	lag      int64
	lagErr   error
	lagCalls int
}

func (f *fakeHealth) Started(context.Context) error              { return f.started }
func (f *fakeHealth) IsPrimary() (bool, error)                   { return f.primary, f.roleErr }
func (f *fakeHealth) PrimaryAcceptsWrites(context.Context) error { return f.writes }
func (f *fakeHealth) ReplayLagBytes(context.Context) (int64, error) {
	f.lagCalls++
	return f.lag, f.lagErr
}

func get(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	return rr.Code
}

func TestStartz(t *testing.T) {
	h := &fakeHealth{}
	p := &Probes{Health: h}
	if get(t, p.Handler(), "/startz") != 200 {
		t.Fatal("expected 200")
	}
	h.started = errors.New("down")
	if get(t, p.Handler(), "/startz") != 500 {
		t.Fatal("expected 500")
	}
}

func TestReadyz(t *testing.T) {
	h := &fakeHealth{primary: true}
	p := &Probes{Health: h, MaxLagBytes: 100}
	if get(t, p.Handler(), "/readyz") != 200 {
		t.Fatal("primary should be ready")
	}
	h.writes = errors.New("in recovery")
	if get(t, p.Handler(), "/readyz") != 500 || h.lagCalls != 0 {
		t.Fatal("primary in recovery should not be ready and lag must not be consulted")
	}
	h.primary = false
	h.lag = 100
	if get(t, p.Handler(), "/readyz") != 200 {
		t.Fatal("standby at max lag should be ready")
	}
	h.lag = 101
	if get(t, p.Handler(), "/readyz") != 500 {
		t.Fatal("standby over lag should not be ready")
	}
	h.lag, h.lagErr = 0, errors.New("not streaming")
	if get(t, p.Handler(), "/readyz") != 500 {
		t.Fatal("non-streaming standby should not be ready")
	}
}

func TestLivezStandbyAlwaysOK(t *testing.T) {
	p := &Probes{Health: &fakeHealth{primary: false}, KubeReachable: func(context.Context) bool { return false }}
	if get(t, p.Handler(), "/livez") != 200 {
		t.Fatal("standby must be live")
	}
}

func TestLivezPrimaryKubeReachable(t *testing.T) {
	var fenced atomic.Int32
	p := &Probes{Health: &fakeHealth{primary: true}, KubeReachable: func(context.Context) bool { return true },
		Fenced: func() { fenced.Add(1) }}
	if get(t, p.Handler(), "/livez") != 200 || fenced.Load() != 0 {
		t.Fatal("primary with kube API should be live")
	}
}

func TestLivezPrimaryFallsBackToPeersAndSelfFences(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer up.Close()
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	defer down.Close()
	var fenced atomic.Int32
	p := &Probes{Health: &fakeHealth{primary: true}, KubeReachable: func(context.Context) bool { return false },
		Peers: []string{up.URL + "/failsafe"}, Fenced: func() { fenced.Add(1) }}
	if get(t, p.Handler(), "/livez") != 200 || fenced.Load() != 0 {
		t.Fatal("all peers reachable: primary should be live")
	}
	p.Peers = append(p.Peers, down.URL+"/failsafe")
	if get(t, p.Handler(), "/livez") != 200 {
		t.Fatal("one reachable peer keeps the primary live")
	}
	p.Peers = []string{down.URL + "/failsafe"}
	clock := time.Unix(0, 0)
	p.now = func() time.Time { return clock }
	if get(t, p.Handler(), "/livez") != 500 || fenced.Load() != 0 {
		t.Fatal("first isolated probe must fail but not fence")
	}
	clock = clock.Add(DefaultIsolationGrace - time.Second)
	get(t, p.Handler(), "/livez")
	if fenced.Load() != 0 {
		t.Fatal("isolation shorter than the grace must not fence")
	}
	p.Peers = []string{up.URL + "/failsafe"}
	get(t, p.Handler(), "/livez")
	p.Peers = []string{down.URL + "/failsafe"}
	clock = clock.Add(DefaultIsolationGrace)
	get(t, p.Handler(), "/livez")
	if fenced.Load() != 0 {
		t.Fatal("a recovered probe resets the isolation window")
	}
	clock = clock.Add(DefaultIsolationGrace)
	get(t, p.Handler(), "/livez")
	get(t, p.Handler(), "/livez")
	if fenced.Load() != 1 {
		t.Fatalf("fenced %d times, want exactly once after the grace", fenced.Load())
	}
	p2 := &Probes{Health: &fakeHealth{primary: true}, KubeReachable: func(context.Context) bool { return false }}
	if get(t, p2.Handler(), "/livez") != 500 {
		t.Fatal("no kube API and no peers: primary must not be live")
	}
	p3 := &Probes{Health: &fakeHealth{primary: true}, Peers: []string{down.URL}}
	if get(t, p3.Handler(), "/livez") != 500 {
		t.Fatal("no kube configured and unreachable peer: primary must not be live")
	}
	p4 := &Probes{Health: &fakeHealth{primary: true}, Peers: []string{"http://127.0.0.1:1/failsafe"}}
	if get(t, p4.Handler(), "/livez") != 500 {
		t.Fatal("connection refused peer must not be live")
	}
}

func TestFailsafeIsUnauthenticated200(t *testing.T) {
	p := &Probes{Health: &fakeHealth{}}
	if get(t, p.Handler(), "/failsafe") != 200 {
		t.Fatal("failsafe must return 200")
	}
}

// TestAProbeThatCannotReadTheRoleDoesNotGuess: the role comes from a file
// in PGDATA, and reading it can fail for reasons that are not "the file is
// absent". Answering those as "standby" made a primary that could not read
// its own data directory report ready and live -- readiness by ignorance,
// on the member that must be fenced fastest.
func TestAProbeThatCannotReadTheRoleDoesNotGuess(t *testing.T) {
	broken := errors.New("permission denied")
	p := &Probes{Health: &fakeHealth{primary: false, roleErr: broken}, MaxLagBytes: 1 << 20}
	ctx := context.Background()

	if err := p.Ready(ctx); err == nil {
		t.Fatal("readiness answered without being able to read the role")
	}
	// Live treats an unreadable role as a primary's: the checks it skips
	// for a standby are exactly the ones that fence an isolated primary.
	if err := p.Live(ctx); err == nil {
		t.Fatal("liveness skipped the primary checks for a member whose role it could not read")
	}
}
