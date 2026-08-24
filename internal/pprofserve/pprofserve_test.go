package pprofserve

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"testing"
	"time"
)

func TestServeExposesProfilesAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	addr, err := Serve(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if runtime.SetMutexProfileFraction(-1) != mutexProfileFraction {
		t.Errorf("mutex profile fraction not set while serving")
	}
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/mutex", "/debug/pprof/block", "/debug/pprof/allocs"} {
		resp, err := http.Get("http://" + addr + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: status %d", path, resp.StatusCode)
		}
	}
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.SetMutexProfileFraction(-1) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("mutex profiling still on after cancel")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServeBadAddr(t *testing.T) {
	if _, err := Serve(context.Background(), "127.0.0.1:notaport"); err == nil {
		t.Fatal("want error for bad address")
	}
}
