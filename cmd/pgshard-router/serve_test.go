package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/router"
)

func TestServeUsageErrors(t *testing.T) {
	cases := [][]string{
		{"--bogus"},
		{"extra"},
		{"--tls-cert", "x", "--catalog-dsn", "postgres://x/y", "--insecure-dev"},
		{"--tls-key", "x", "--catalog-dsn", "postgres://x/y", "--insecure-dev"},
		{"--catalog-dsn", "postgres://x/y", "--tls-cert", "/nonexistent/c", "--tls-key", "/nonexistent/k", "--insecure-dev"},
		{"--insecure-dev"},
		{"--catalog-dsn", "postgres://x/y"},
		{"--catalog-dsn", "postgres://x/y", "--insecure-dev", "--pooler-tls-ca", "/x"},
		{"--catalog-dsn", "postgres://x/y", "--insecure-dev", "--pooler", "nonsense"},
		{"--catalog-dsn", "postgres://x/y", "--pooler-tls-cert", "/nonexistent/c", "--pooler-tls-key", "/nonexistent/k", "--pooler-tls-ca", "/nonexistent/ca"},
		{"--catalog-dsn", "not a dsn", "--insecure-dev"},
		{"--catalog-dsn", "postgres://x/y", "--insecure-dev", "--peer", "2=h:1"},
		{"--catalog-dsn", "postgres://x/y", "--insecure-dev", "--peer-service", "h:1"},
		{"--catalog-dsn", "postgres://x/y", "--insecure-dev", "--peer-cancel-listen", "127.0.0.1:0", "--peer", "zero=h:1"},
		{"--catalog-dsn", "postgres://x/y", "--insecure-dev", "--instance-id", "4294967296"},
	}
	for _, args := range cases {
		var out, errb bytes.Buffer
		if code := runServe(context.Background(), args, &out, &errb); code != 2 {
			t.Errorf("serve %v: code %d, stderr %q", args, code, errb.String())
		}
	}
	var out, errb bytes.Buffer
	if code := runServe(context.Background(), []string{"--help"}, &out, &errb); code != 0 || !strings.Contains(errb.String(), "-catalog-dsn") {
		t.Fatalf("--help: code %d, %q", code, errb.String())
	}
}

func TestServeNeedsReachableCatalog(t *testing.T) {
	var out, errb bytes.Buffer
	code := runServe(context.Background(), []string{"--catalog-dsn", "postgres://nobody@127.0.0.1:1/x?connect_timeout=1", "--insecure-dev"}, &out, &errb)
	if code != 1 || !strings.Contains(errb.String(), "catalog roles") {
		t.Fatalf("code %d stderr %q", code, errb.String())
	}
}

func TestPoolerFlags(t *testing.T) {
	p := poolerFlags{}
	for _, v := range []string{"0=127.0.0.1:1", "orders/3=h:2", "catalog/0=c:3"} {
		if err := p.Set(v); err != nil {
			t.Fatal(err)
		}
	}
	want := map[router.Shard]string{{Set: "default", ID: 0}: "127.0.0.1:1", {Set: "orders", ID: 3}: "h:2", {Set: "catalog", ID: 0}: "c:3"}
	for k, v := range want {
		if p[k] != v {
			t.Errorf("%v: got %q want %q", k, p[k], v)
		}
	}
	for _, bad := range []string{"", "x", "a=", "=b", "/1=h:1", "q/1=h:1=", "one=h:1"} {
		if err := (poolerFlags{}).Set(bad); err == nil && bad != "q/1=h:1=" {
			t.Errorf("%q accepted", bad)
		}
	}
	if !strings.Contains(p.String(), "orders") {
		t.Fatalf("String %q", p.String())
	}
}

func TestListenNetwork(t *testing.T) {
	cases := map[string]string{"0.0.0.0:5432": "tcp4", "127.0.0.1:1": "tcp4", "[::]:5432": "tcp", ":5432": "tcp", "localhost:5432": "tcp", "bad": "tcp"}
	for addr, want := range cases {
		if got := listenNetwork(addr); got != want {
			t.Errorf("%s: got %s want %s", addr, got, want)
		}
	}
}

func TestPeerFlags(t *testing.T) {
	p := peerFlags{}
	if err := p.Set("7=10.0.0.1:9000"); err != nil || p[7] != "10.0.0.1:9000" {
		t.Fatalf("%v %v", p, err)
	}
	for _, bad := range []string{"", "7", "7=", "=h:1", "0=h:1", "4294967296=h:1", "x=h:1"} {
		if err := (peerFlags{}).Set(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if !strings.Contains(p.String(), "10.0.0.1:9000") {
		t.Fatalf("String %q", p.String())
	}
}

// TestAnAuxiliaryListenerThatStopsIsReported: the router keeps accepting
// sessions whatever happens to its peer-cancel, VStream or health
// listener, so an exit nobody hears leaves it serving with a piece of
// itself missing -- cancellation delivered nowhere, a consumer's stream
// never reconnecting, a Service still sending traffic to a router that
// cannot say it is unwell.
func TestAnAuxiliaryListenerThatStopsIsReported(t *testing.T) {
	errc := make(chan error, 4)
	stopped := errors.New("listener closed")

	serveAux(context.Background(), errc, "vstream", func() error { return stopped })
	select {
	case err := <-errc:
		if !errors.Is(err, stopped) || !strings.Contains(err.Error(), "vstream") {
			t.Fatalf("reported %v, want the named listener's own error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an auxiliary listener stopped and nothing was reported")
	}

	// A listener that returns nil has still stopped serving.
	serveAux(context.Background(), errc, "health", func() error { return nil })
	select {
	case err := <-errc:
		if !strings.Contains(err.Error(), "health") {
			t.Fatalf("reported %v, want the health listener", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a listener that returned nil was taken for one still serving")
	}

	// An exit after the context ends is the shutdown, and says nothing.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	serveAux(ctx, errc, "peer cancels", func() error { return stopped })
	select {
	case err := <-errc:
		t.Fatalf("shutdown reported as a failure: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}
