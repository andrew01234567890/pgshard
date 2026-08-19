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

	"github.com/jackc/pgx/v5"
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
	Client  client.Reader
	Catalog CatalogSource
	// Migrations is set when Catalog also reads migrations; nil hides the panel.
	Migrations MigrationSource
	Notifier   *Notifier
	Namespace  string
	Logger     *slog.Logger
	Tick       time.Duration

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
	tmpl, err := template.New("").Funcs(template.FuncMap{"bytes": humanBytes, "bytesp": humanBytesPtr, "when": humanTime, "whenp": humanTimePtr}).ParseFS(assets, "templates/*.html")
	if err != nil {
		return nil, err
	}
	s := &Server{Client: c, Catalog: catalogSrc, Notifier: n, Namespace: namespace, Logger: logger, Tick: RefreshInterval, tmpl: tmpl}
	if ms, ok := catalogSrc.(MigrationSource); ok {
		s.Migrations = ms
	}
	static, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(static)))
	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /clusters/{ns}/{name}", s.handleCluster)
	mux.HandleFunc("GET /clusters/{ns}/{name}/topology", s.handleTopologyFragment)
	mux.HandleFunc("GET /backups", s.handleBackups)
	mux.HandleFunc("GET /backups/panel", s.handleBackupsFragment)
	mux.HandleFunc("GET /backups/{ns}/{name}", s.handleBackup)
	mux.HandleFunc("GET /restores/{ns}/{name}", s.handleRestore)
	mux.HandleFunc("GET /api/v1/backups", s.handleAPIBackups)
	mux.HandleFunc("GET /api/v1/restores", s.handleAPIRestores)
	mux.HandleFunc("GET /api/v1/restore-points", s.handleAPIRestorePoints)
	mux.HandleFunc("GET /migrations", s.handleMigrations)
	mux.HandleFunc("GET /migrations/table", s.handleMigrationsFragment)
	mux.HandleFunc("GET /migrations/{id}", s.handleMigration)
	mux.HandleFunc("GET /migrations/{id}/detail", s.handleMigrationFragment)
	mux.HandleFunc("GET /api/v1/migrations", s.handleAPIMigrations)
	mux.HandleFunc("GET /api/v1/migrations/{id}", s.handleAPIMigration)
	mux.HandleFunc("GET /streams", s.handleStreams)
	mux.HandleFunc("GET /streams/{name}", s.handleStream)
	mux.HandleFunc("GET /api/v1/streams", s.handleAPIStreams)
	mux.HandleFunc("GET /api/v1/streams/{name}", s.handleAPIStream)
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

func (s *Server) streamSource() StreamSource {
	src, _ := s.Catalog.(StreamSource)
	return src
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	clusters, err := ListClusters(r.Context(), s.Client, s.Namespace)
	if err != nil {
		s.fail(w, err)
		return
	}
	cards, err := BuildBackupCards(r.Context(), s.Client, s.Namespace, time.Now())
	if err != nil {
		s.fail(w, err)
		return
	}
	data := map[string]any{"Clusters": clusters, "Namespace": s.Namespace, "Cards": cards}
	if src := s.streamSource(); src != nil {
		overview, err := BuildStreamsOverview(r.Context(), src)
		if err != nil {
			data["StreamsError"] = err.Error()
		} else {
			data["Streams"] = overview
		}
	}
	s.render(w, "index.html", data)
}

func (s *Server) handleBackups(w http.ResponseWriter, r *http.Request) {
	page, err := BuildBackupsPage(r.Context(), s.Client, s.Catalog, s.Namespace)
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "backups.html", page)
}

func (s *Server) handleBackupsFragment(w http.ResponseWriter, r *http.Request) {
	page, err := BuildBackupsPage(r.Context(), s.Client, s.Catalog, s.Namespace)
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "backups_panel.html", page)
}

func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	b, err := GetBackup(r.Context(), s.Client, r.PathValue("ns"), r.PathValue("name"))
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "backup.html", b)
}

func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request) {
	rs, err := GetRestore(r.Context(), s.Client, r.PathValue("ns"), r.PathValue("name"))
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "restore.html", rs)
}

func (s *Server) handleAPIBackups(w http.ResponseWriter, r *http.Request) {
	list, err := ListBackups(r.Context(), s.Client, s.Namespace)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, list)
}

func (s *Server) handleAPIRestores(w http.ResponseWriter, r *http.Request) {
	list, err := ListRestores(r.Context(), s.Client, s.Namespace)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, list)
}

func (s *Server) handleAPIRestorePoints(w http.ResponseWriter, r *http.Request) {
	out := []RestorePoint{}
	if s.Catalog != nil {
		points, err := s.Catalog.RestorePoints(r.Context())
		if err != nil {
			s.fail(w, err)
			return
		}
		for _, rp := range points {
			out = append(out, convertRestorePoint(rp))
		}
	}
	writeJSON(w, out)
}

func (s *Server) topology(r *http.Request) (*Topology, error) {
	t, err := BuildTopology(r.Context(), s.Client, s.Catalog, r.PathValue("ns"), r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	t.DDL = s.ddlSummary(r.Context())
	return t, nil
}

func (s *Server) handleStreams(w http.ResponseWriter, r *http.Request) {
	overview, err := BuildStreamsOverview(r.Context(), s.streamSource())
	if err != nil {
		s.fail(w, err)
		return
	}
	if r.Header.Get("HX-Request") == "true" {
		s.render(w, "streams_list.html", overview)
		return
	}
	s.render(w, "streams.html", overview)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	d, err := BuildStreamDetail(r.Context(), s.streamSource(), r.PathValue("name"))
	if err != nil {
		s.fail(w, err)
		return
	}
	if r.Header.Get("HX-Request") == "true" {
		s.render(w, "stream_detail.html", d)
		return
	}
	s.render(w, "stream.html", d)
}

func (s *Server) handleAPIStreams(w http.ResponseWriter, r *http.Request) {
	overview, err := BuildStreamsOverview(r.Context(), s.streamSource())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, overview)
}

func (s *Server) handleAPIStream(w http.ResponseWriter, r *http.Request) {
	d, err := BuildStreamDetail(r.Context(), s.streamSource(), r.PathValue("name"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, d)
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	t, err := s.topology(r)
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "cluster.html", t)
}

func (s *Server) handleTopologyFragment(w http.ResponseWriter, r *http.Request) {
	t, err := s.topology(r)
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
	t, err := s.topology(r)
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
	switch {
	case apierrors.IsNotFound(err), errors.Is(err, pgx.ErrNoRows), errors.Is(err, ErrStreamNotFound):
		code = http.StatusNotFound
	case errors.Is(err, ErrNoStreamSource):
		code = http.StatusServiceUnavailable
	case errors.Is(err, context.Canceled):
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

func humanTime(t any) string {
	switch v := t.(type) {
	case time.Time:
		return v.UTC().Format("2006-01-02 15:04:05")
	case *time.Time:
		return humanTimePtr(v)
	}
	return "\u2014"
}

func humanTimePtr(t *time.Time) string {
	if t == nil {
		return "\u2014"
	}
	return humanTime(*t)
}
