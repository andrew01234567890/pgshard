package admin

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// htmx.min.js is htmx 2.0.8 (https://htmx.org), licensed under the
// Zero-Clause BSD license; see static/LICENSE.htmx.
//
//go:embed static/htmx.min.js static/LICENSE.htmx static/style.css static/app.js templates/*.html
var assets embed.FS

// RefreshInterval is the fallback SSE tick when no watch event arrives.
const RefreshInterval = 2 * time.Second

const contentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"

// Server is the admin HTTP handler.
type Server struct {
	Client    client.Reader
	Catalog   CatalogSource
	Notifier  *Notifier
	Namespace string
	Logger    *slog.Logger
	Tick      time.Duration

	tmpl *template.Template
	mux  *http.ServeMux
}

// NewServer wires the routes. Namespace scopes the clusters list; empty means all.
func NewServer(c client.Reader, catalogSrc CatalogSource, n *Notifier, namespace string, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if n == nil {
		n = NewNotifier()
	}
	tmpl, err := template.New("").Funcs(template.FuncMap{"bytes": humanBytes, "bytesp": humanBytesPtr}).ParseFS(assets, "templates/*.html")
	if err != nil {
		return nil, err
	}
	s := &Server{Client: c, Catalog: catalogSrc, Notifier: n, Namespace: namespace, Logger: logger, Tick: RefreshInterval, tmpl: tmpl}
	static, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(static)))
	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /clusters/{ns}/{name}", s.handleCluster)
	mux.HandleFunc("GET /clusters/{ns}/{name}/topology", s.handleTopologyFragment)
	mux.HandleFunc("GET /api/v1/clusters", s.handleAPIClusters)
	mux.HandleFunc("GET /api/v1/clusters/{ns}/{name}", s.handleAPICluster)
	mux.HandleFunc("GET /events", s.handleEvents)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	s.mux = mux
	return s, nil
}

// ServeHTTP adds security headers and request logging around the routes.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", contentSecurityPolicy)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	s.mux.ServeHTTP(rec, r)
	s.Logger.Info("request", "method", r.Method, "path", r.URL.Path, "status", rec.status, "duration", time.Since(start).String(), "remote", r.RemoteAddr)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	clusters, err := ListClusters(r.Context(), s.Client, s.Namespace)
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "index.html", map[string]any{"Clusters": clusters, "Namespace": s.Namespace})
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	t, err := BuildTopology(r.Context(), s.Client, s.Catalog, r.PathValue("ns"), r.PathValue("name"))
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "cluster.html", t)
}

func (s *Server) handleTopologyFragment(w http.ResponseWriter, r *http.Request) {
	t, err := BuildTopology(r.Context(), s.Client, s.Catalog, r.PathValue("ns"), r.PathValue("name"))
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "topology.html", t)
}

func (s *Server) handleAPIClusters(w http.ResponseWriter, r *http.Request) {
	clusters, err := ListClusters(r.Context(), s.Client, s.Namespace)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, clusters)
}

func (s *Server) handleAPICluster(w http.ResponseWriter, r *http.Request) {
	t, err := BuildTopology(r.Context(), s.Client, s.Catalog, r.PathValue("ns"), r.PathValue("name"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, t)
}

// handleEvents streams a "topology" SSE event whenever a watched object
// changes, and at least every Tick so clients recover from missed events.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	events, cancel := s.Notifier.Subscribe()
	defer cancel()
	ticker := time.NewTicker(s.Tick)
	defer ticker.Stop()
	seq := 0
	send := func(reason string) bool {
		seq++
		_, err := fmt.Fprintf(w, "id: %d\nevent: topology\ndata: {\"reason\":%q,\"at\":%q}\n\n", seq, reason, time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	if !send("connected") {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-events:
			if !send("update") {
				return
			}
			ticker.Reset(s.Tick)
		case <-ticker.C:
			if !send("tick") {
				return
			}
		}
	}
}

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	var buf strings.Builder
	if err := s.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprint(w, buf.String())
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	if apierrors.IsNotFound(err) {
		code = http.StatusNotFound
	} else if errors.Is(err, context.Canceled) {
		code = 499
	}
	s.Logger.Error("request failed", "err", err)
	http.Error(w, http.StatusText(code), code)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func humanBytesPtr(n *int64) string {
	if n == nil {
		return "\u2014"
	}
	return humanBytes(*n)
}
