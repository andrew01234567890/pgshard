package admin

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// QueueSource reads the head of the operation queue, and how many entries
// there are in all.
type QueueSource interface {
	OperationQueue(ctx context.Context) ([]catalog.QueueEntry, int, error)
}

// OperationQueue implements QueueSource.
func (p PgxCatalog) OperationQueue(ctx context.Context) ([]catalog.QueueEntry, int, error) {
	type page struct {
		entries []catalog.QueueEntry
		total   int
	}
	out, err := withConn(ctx, p, func(ctx context.Context, conn *pgx.Conn) (page, error) {
		entries, total, err := catalog.ListOperationQueue(ctx, conn, true)
		return page{entries, total}, err
	})
	return out.entries, out.total, err
}

// QueueView is the /queue page.
type QueueView struct {
	Entries []QueueRow `json:"entries"`
	// Waiting counts the entries that wait for another.
	Waiting int `json:"waiting"`
	Running int `json:"running"`
	// States counts every state present, in the order it first appears, so
	// that the summary accounts for all of the entries and not only the two
	// states with their own counter.
	States []QueueStateCount `json:"states"`
	// Total is how many operations are in the queue, which is more than
	// Entries holds once the queue is deeper than the page reads. Showing
	// the head of a long queue is the point; showing it as if it were the
	// whole queue is not.
	Total int    `json:"total"`
	Error string `json:"error,omitempty"`
	// Unavailable says why there is no queue to show: no catalog, or a
	// catalog not yet migrated to have one.
	Unavailable string    `json:"unavailable,omitempty"`
	ReadAt      time.Time `json:"read_at"`
}

// QueueStateCount is how many entries are in one state.
type QueueStateCount struct {
	State string `json:"state"`
	Count int    `json:"count"`
}

// QueueRow is one queue entry as the page shows it.
type QueueRow struct {
	catalog.QueueEntry
	KindLabel string `json:"kind_label"`
	// Percent is the progress bar's width, and HasProgress whether there is
	// one to draw.
	Percent     int         `json:"percent"`
	HasProgress bool        `json:"has_progress"`
	Short       string      `json:"-"`
	Truncated   bool        `json:"-"`
	Waits       []QueueLink `json:"-"`
	Since       string      `json:"-"`
}

// QueueLink names an operation an entry waits for, linked where the admin
// has a page for it.
type QueueLink struct {
	Label  string
	Href   string
	Reason string
}

var queueKindLabels = map[string]string{
	catalog.OperationDDL: "DDL", catalog.OperationReshard: "reshard", catalog.OperationUpgrade: "major upgrade", catalog.OperationPlacement: "table placement",
}

// BuildQueueView reads the queue and shapes it for the page.
func BuildQueueView(ctx context.Context, src QueueSource, now time.Time) QueueView {
	v := QueueView{ReadAt: now, Entries: []QueueRow{}}
	if src == nil {
		v.Unavailable = "no catalog connection is configured for the admin"
		return v
	}
	entries, total, err := src.OperationQueue(ctx)
	var pgErr *pgconn.PgError
	switch {
	case errors.As(err, &pgErr) && pgErr.Code == "42P01":
		v.Unavailable = "the catalog has no operation queue yet: it is migrated once every control-plane component runs a version that has one"
		return v
	case err != nil:
		v.Error = err.Error()
		return v
	}
	v.Total = total
	for _, e := range entries {
		row := QueueRow{QueueEntry: e, KindLabel: queueKindLabels[e.Kind]}
		if row.KindLabel == "" {
			row.KindLabel = e.Kind
		}
		if e.Progress != nil {
			row.HasProgress = true
			row.Percent = int(math.Floor(math.Max(0, math.Min(1, *e.Progress)) * 100))
		}
		text := e.Command
		if e.Statement != nil && *e.Statement != "" {
			text = *e.Statement
		}
		row.Short = text
		if len([]rune(text)) > statementPreview {
			row.Short, row.Truncated = string([]rune(text)[:statementPreview])+"…", true
		}
		for _, b := range e.Blockers {
			link := QueueLink{Label: queueKindLabels[b.Kind] + " " + shortID(b.ID), Reason: "queued before it"}
			if b.Reason == catalog.BlockedByStarted {
				link.Reason = "in progress"
			}
			if b.Kind == catalog.OperationDDL {
				link.Href = "/migrations/" + b.ID
			}
			row.Waits = append(row.Waits, link)
		}
		since := e.CreatedAt
		if e.StartedAt != nil {
			since = *e.StartedAt
		}
		row.Since = now.Sub(since).Round(time.Second).String()
		switch e.State {
		case "waiting":
			v.Waiting++
		case "running", "retiring":
			v.Running++
		}
		v.States = countState(v.States, e.State)
		v.Entries = append(v.Entries, row)
	}
	return v
}

func countState(counts []QueueStateCount, state string) []QueueStateCount {
	for i := range counts {
		if counts[i].State == state {
			counts[i].Count++
			return counts
		}
	}
	return append(counts, QueueStateCount{State: state, Count: 1})
}

func shortID(id string) string {
	if i := strings.IndexByte(id, '-'); i > 0 {
		return id[:i]
	}
	return id
}

func (s *Server) queueSource() QueueSource {
	src, _ := s.Catalog.(QueueSource)
	return src
}

func (s *Server) handleQueue(w http.ResponseWriter, r *http.Request) {
	s.render(w, "queue.html", BuildQueueView(r.Context(), s.queueSource(), time.Now()))
}

func (s *Server) handleQueueFragment(w http.ResponseWriter, r *http.Request) {
	s.render(w, "queue_panel.html", BuildQueueView(r.Context(), s.queueSource(), time.Now()))
}

func (s *Server) handleAPIQueue(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, BuildQueueView(r.Context(), s.queueSource(), time.Now()))
}
