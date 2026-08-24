// Package metrics defines the Prometheus metric sets of every pgshard
// process and the HTTP plumbing that exposes them.
package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/andrew01234567890/pgshard/internal/buildinfo"
)

// NewRegistry returns a registry preloaded with the Go runtime and process
// collectors and a pgshard_build_info gauge identifying process.
func NewRegistry(process string) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	info := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgshard_build_info",
		Help: "Build metadata; the value is always 1.",
	}, []string{"process", "version"})
	reg.MustRegister(info)
	info.WithLabelValues(process, buildinfo.Version).Set(1)
	return reg
}

// Handler serves reg in the Prometheus exposition format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// Serve exposes /metrics (and a trivial /healthz) for reg on addr until ctx
// ends. It returns nil after a clean shutdown.
func Serve(ctx context.Context, addr string, reg *prometheus.Registry) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler(reg))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		close(done)
	}()
	err = srv.Serve(l)
	<-done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
