// Package pprofserve serves net/http/pprof on a dedicated listener for
// profiling runs. Mutex and block profiling are sampled only while a server
// is up; both are off in normal operation.
package pprofserve

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

const (
	mutexProfileFraction = 100
	blockProfileRateNs   = 10_000
)

// Serve listens on addr and serves /debug/pprof/ until ctx is cancelled.
// It returns once the listener is bound; the returned address is the bound
// one (useful with a :0 addr in tests).
func Serve(ctx context.Context, addr string) (string, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	runtime.SetMutexProfileFraction(mutexProfileFraction)
	runtime.SetBlockProfileRate(blockProfileRateNs)
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
		_ = hs.Close()
	}()
	go func() { _ = hs.Serve(l) }()
	return l.Addr().String(), nil
}
