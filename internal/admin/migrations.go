package admin

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// MigrationsPageSize is the number of rows per /migrations page.
const MigrationsPageSize = 25

// MigrationSource reads DDL migrations from the catalog database.
type MigrationSource interface {
	ListMigrations(ctx context.Context, f catalog.MigrationFilter) ([]catalog.DDLMigration, int, error)
	LoadMigration(ctx context.Context, id string) (catalog.DDLMigration, error)
	CountMigrations(ctx context.Context) (catalog.MigrationCounts, error)
}

// ShardProgress tallies the per-shard states of one migration.
type ShardProgress struct {
	Applied  int `json:"applied"`
	Retrying int `json:"retrying"`
	Failed   int `json:"failed"`
	Pending  int `json:"pending"`
	Total    int `json:"total"`
}

// Percent is the width of a progress segment, 0 when there are no shards.
func (p ShardProgress) Percent(n int) int {
	if p.Total == 0 {
		return 0
	}
	return n * 100 / p.Total
}

// FailedOffset is where the failed segment starts: after applied and retrying.
func (p ShardProgress) FailedOffset() int { return p.Percent(p.Applied + p.Retrying) }

// MigrationRow is one migration as the list and detail pages show it.
type MigrationRow struct {
	ID          string        `json:"id"`
	Database    string        `json:"database"`
	Kind        string        `json:"kind"`
	Strategy    string        `json:"strategy"`
	State       string        `json:"state"`
	Statement   string        `json:"statement"`
	Short       string        `json:"-"`
	Truncated   bool          `json:"-"`
	CreatedAt   time.Time     `json:"createdAt"`
	FinishedAt  *time.Time    `json:"finishedAt,omitempty"`
	Duration    string        `json:"duration"`
	Error       string        `json:"error,omitempty"`
	Progress    ShardProgress `json:"progress"`
	CurrentStep string        `json:"currentStep,omitempty"`
	Degraded    bool          `json:"degraded"`
	Shards      []ShardRow    `json:"shards"`
	Steps       []StepRow     `json:"steps,omitempty"`
}

// ShardRow is one shard's line of the detail page.
type ShardRow struct {
	Shard    string `json:"shard"`
	State    string `json:"state"`
	Step     int    `json:"step"`
	Attempts int    `json:"attempts"`
	Error    string `json:"error,omitempty"`
}

// StepRow is one step of a multistep migration with its overall status.
type StepRow struct {
	Index  int    `json:"index"`
	SQL    string `json:"sql"`
	Status string `json:"status"`
}

// MigrationsPage is the /migrations document.
type MigrationsPage struct {
	Rows     []MigrationRow `json:"rows"`
	Total    int            `json:"total"`
	Page     int            `json:"page"`
	Pages    int            `json:"pages"`
	Database string         `json:"database"`
	State    string         `json:"state"`
	Error    string         `json:"error,omitempty"`
	States   []string       `json:"-"`
}

// PrevURL is the previous page's link; empty on the first page.
func (p MigrationsPage) PrevURL() string { return p.pageURL(p.Page - 1) }

// NextURL is the next page's link; empty on the last page.
func (p MigrationsPage) NextURL() string { return p.pageURL(p.Page + 1) }

func (p MigrationsPage) pageURL(n int) string {
	if n < 1 || n > p.Pages {
		return ""
	}
	return "/migrations?" + p.query(n)
}

// FragmentURL is the address of the live table fragment.
func (p MigrationsPage) FragmentURL() string { return "/migrations/table?" + p.query(p.Page) }

func (p MigrationsPage) query(page int) string {
	q := url.Values{}
	if p.Database != "" {
		q.Set("database", p.Database)
	}
	if p.State != "" {
		q.Set("state", p.State)
	}
	if page > 1 {
		q.Set("page", strconv.Itoa(page))
	}
	return q.Encode()
}

var migrationStates = []string{catalog.MigrationQueued, catalog.MigrationRunning, catalog.MigrationComplete, catalog.MigrationFailed}

const statementPreview = 80

// passwordOption matches the PASSWORD '<verifier>' option of role
// statements so neither the page nor its JSON shows a verifier.
var passwordOption = regexp.MustCompile(`(?i)(PASSWORD\s+)'(?:[^']|'')*'`)

// RedactStatement hides password verifiers in a statement shown to users.
func RedactStatement(sql string) string {
	return passwordOption.ReplaceAllString(sql, "${1}'[redacted]'")
}

// BuildMigrationRow converts a catalog row into the UI's shape.
func BuildMigrationRow(m catalog.DDLMigration, now time.Time) MigrationRow {
	statement := RedactStatement(m.Statement)
	r := MigrationRow{ID: m.ID, Database: m.Database, Kind: m.Kind, Strategy: m.Strategy, State: m.State, Statement: statement,
		Short: statement, CreatedAt: m.CreatedAt, FinishedAt: m.FinishedAt, Error: RedactStatement(m.Error), Shards: []ShardRow{}}
	if len([]rune(statement)) > statementPreview {
		r.Short, r.Truncated = string([]rune(statement)[:statementPreview])+"…", true
	}
	end := now
	if m.FinishedAt != nil {
		end = *m.FinishedAt
	}
	if m.State == catalog.MigrationQueued {
		r.Duration = "—"
	} else {
		r.Duration = end.Sub(m.CreatedAt).Truncate(time.Millisecond).String()
	}
	keys := make([]string, 0, len(m.PerShard))
	for k := range m.PerShard {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return shardLess(keys[i], keys[j]) })
	for _, k := range keys {
		sm := m.PerShard[k]
		r.Shards = append(r.Shards, ShardRow{Shard: k, State: sm.State, Step: sm.Step, Attempts: sm.Attempts, Error: sm.Error})
		r.Progress.Total++
		switch sm.State {
		case catalog.ShardApplied, catalog.ShardSkipped:
			r.Progress.Applied++
		case catalog.ShardRetrying:
			r.Progress.Retrying++
		case catalog.ShardFailed:
			r.Progress.Failed++
		default:
			r.Progress.Pending++
		}
	}
	r.Degraded = r.Progress.Failed > 0 && r.Progress.Applied > 0
	if len(m.Meta.Steps) > 0 {
		r.Steps, r.CurrentStep = stepRows(m)
	}
	return r
}

func stepRows(m catalog.DDLMigration) ([]StepRow, string) {
	steps := make([]StepRow, 0, len(m.Meta.Steps))
	active := -1
	for i, st := range m.Meta.Steps {
		passed, failed, running := 0, false, false
		for _, sm := range m.PerShard {
			switch {
			case sm.State == catalog.ShardApplied || sm.State == catalog.ShardSkipped || sm.Step > i:
				passed++
			case sm.Step < i || sm.State == catalog.ShardPending:
			case sm.State == catalog.ShardFailed:
				failed = true
			default:
				running = true
			}
		}
		status := "pending"
		switch {
		case len(m.PerShard) > 0 && passed == len(m.PerShard):
			status = "done"
		case failed:
			status = "failed"
		case running:
			status = "running"
		case passed > 0:
			status = "partial"
		}
		if running && active < 0 {
			active = i
		}
		steps = append(steps, StepRow{Index: i + 1, SQL: st.SQL, Status: status})
	}
	current := ""
	if active >= 0 {
		current = strconv.Itoa(active+1) + "/" + strconv.Itoa(len(steps))
	}
	return steps, current
}

func shardLess(a, b string) bool {
	ai, aerr := strconv.Atoi(a)
	bi, berr := strconv.Atoi(b)
	if aerr == nil && berr == nil {
		return ai < bi
	}
	return a < b
}

func (s *Server) migrationsPage(r *http.Request) MigrationsPage {
	q := r.URL.Query()
	page := MigrationsPage{Database: q.Get("database"), State: q.Get("state"), Page: 1, Pages: 1, Rows: []MigrationRow{}, States: migrationStates}
	if n, err := strconv.Atoi(q.Get("page")); err == nil && n > 1 {
		page.Page = n
	}
	if s.Migrations == nil {
		page.Error = "the admin server was started without a catalog DSN"
		return page
	}
	rows, total, err := s.Migrations.ListMigrations(r.Context(), catalog.MigrationFilter{Database: page.Database, State: page.State, Limit: MigrationsPageSize, Offset: (page.Page - 1) * MigrationsPageSize})
	if err != nil {
		page.Error = err.Error()
		return page
	}
	page.Total = total
	page.Pages = (total + MigrationsPageSize - 1) / MigrationsPageSize
	if page.Pages < 1 {
		page.Pages = 1
	}
	now := time.Now()
	for _, m := range rows {
		page.Rows = append(page.Rows, BuildMigrationRow(m, now))
	}
	return page
}

func (s *Server) handleMigrations(w http.ResponseWriter, r *http.Request) {
	s.render(w, "migrations.html", s.migrationsPage(r))
}

func (s *Server) handleMigrationsFragment(w http.ResponseWriter, r *http.Request) {
	s.render(w, "migrations_table.html", s.migrationsPage(r))
}

func (s *Server) handleAPIMigrations(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.migrationsPage(r))
}

func (s *Server) loadMigration(w http.ResponseWriter, r *http.Request) (MigrationRow, bool) {
	if s.Migrations == nil {
		http.Error(w, "no catalog configured", http.StatusNotFound)
		return MigrationRow{}, false
	}
	m, err := s.Migrations.LoadMigration(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return MigrationRow{}, false
	}
	return BuildMigrationRow(m, time.Now()), true
}

func (s *Server) handleMigration(w http.ResponseWriter, r *http.Request) {
	if row, ok := s.loadMigration(w, r); ok {
		s.render(w, "migration.html", row)
	}
}

func (s *Server) handleMigrationFragment(w http.ResponseWriter, r *http.Request) {
	if row, ok := s.loadMigration(w, r); ok {
		s.render(w, "migration_detail.html", row)
	}
}

func (s *Server) handleAPIMigration(w http.ResponseWriter, r *http.Request) {
	if row, ok := s.loadMigration(w, r); ok {
		writeJSON(w, row)
	}
}

// DDLSummary is the topology page's DDL card.
type DDLSummary struct {
	Counts catalog.MigrationCounts `json:"counts"`
	Error  string                  `json:"error,omitempty"`
}

func (s *Server) ddlSummary(ctx context.Context) *DDLSummary {
	if s.Migrations == nil {
		return nil
	}
	c, err := s.Migrations.CountMigrations(ctx)
	out := &DDLSummary{Counts: c}
	if err != nil {
		out.Error = err.Error()
	}
	return out
}
