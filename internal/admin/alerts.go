package admin

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrNoTwoPCSource is returned when the admin runs without a catalog DSN.
var ErrNoTwoPCSource = errors.New("two-phase commit pages need --catalog-dsn")

// TwoPCRow is one row of pgshard.xact_decisions.
type TwoPCRow struct {
	GID          string     `json:"gid"`
	State        string     `json:"state"`
	Participants []int32    `json:"participants"`
	CreatedAt    time.Time  `json:"createdAt"`
	DecidedAt    *time.Time `json:"decidedAt,omitempty"`
}

// TwoPCSource reads the durable two-phase commit decision log. A
// CatalogSource that also implements it enables the /twopc and /alerts pages.
type TwoPCSource interface {
	ListDecisions(ctx context.Context) ([]TwoPCRow, error)
	ListPausedWorkflows(ctx context.Context) ([]WorkflowRow, error)
}

// WorkflowRow is one paused workflow.
type WorkflowRow struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	State     string    `json:"state"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// TwoPCEntry is one decision-log row prepared for display.
type TwoPCEntry struct {
	TwoPCRow
	Age      time.Duration `json:"-"`
	AgeText  string        `json:"age"`
	Decision string        `json:"decision"`
}

// TwoPCView is what the /twopc page renders.
type TwoPCView struct {
	Entries   []TwoPCEntry `json:"entries"`
	Count     int          `json:"count"`
	InDoubt   int          `json:"inDoubt"`
	OldestAge string       `json:"oldestAge,omitempty"`
}

// BuildTwoPCView loads and shapes the decision log; src nil returns
// ErrNoTwoPCSource.
func BuildTwoPCView(ctx context.Context, src TwoPCSource, now time.Time) (TwoPCView, error) {
	if src == nil {
		return TwoPCView{}, ErrNoTwoPCSource
	}
	rows, err := src.ListDecisions(ctx)
	if err != nil {
		return TwoPCView{}, err
	}
	v := TwoPCView{Entries: make([]TwoPCEntry, 0, len(rows)), Count: len(rows)}
	var oldest time.Duration
	for _, r := range rows {
		age := now.Sub(r.CreatedAt)
		decision := "undecided"
		if r.State != "preparing" {
			decision = r.State
		}
		if r.State == "preparing" {
			v.InDoubt++
			if age > oldest {
				oldest = age
			}
		}
		v.Entries = append(v.Entries, TwoPCEntry{TwoPCRow: r, Age: age, AgeText: shortDuration(age), Decision: decision})
	}
	if oldest > 0 {
		v.OldestAge = shortDuration(oldest)
	}
	return v, nil
}

// Alert is one firing condition on the /alerts page.
type Alert struct {
	Name     string `json:"name"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}

// AlertInputs is everything alert derivation reads; the handler assembles it
// from the catalog and the Kubernetes API.
type AlertInputs struct {
	Now       time.Time
	Decisions []TwoPCRow
	Streams   []StreamSummary
	Paused    []WorkflowRow
	// LatestBackup is when the newest completed backup finished; nil when
	// none exists or backups are not visible.
	LatestBackup *time.Time
	// BackupsKnown reports whether backup information was available at all;
	// false suppresses the freshness alert instead of firing it blindly.
	BackupsKnown bool
}

// Alert thresholds.
const (
	InDoubtAgeThreshold   = 5 * time.Minute
	PreparedCountWarning  = 100
	BackupStaleThreshold  = 26 * time.Hour
	CutoverPauseThreshold = 30 * time.Minute
)

// DeriveAlerts evaluates the documented alert conditions over in.
func DeriveAlerts(in AlertInputs) []Alert {
	var alerts []Alert
	var inDoubt int
	var oldest time.Duration
	for _, d := range in.Decisions {
		if d.State != "preparing" {
			continue
		}
		inDoubt++
		if age := in.Now.Sub(d.CreatedAt); age > oldest {
			oldest = age
		}
	}
	if inDoubt > 0 && oldest >= InDoubtAgeThreshold {
		alerts = append(alerts, Alert{Name: "TwoPCInDoubtAged", Severity: "critical",
			Detail: fmt.Sprintf("%d in-doubt two-phase commit(s); oldest %s", inDoubt, shortDuration(oldest))})
	}
	if n := len(in.Decisions); n >= PreparedCountWarning {
		alerts = append(alerts, Alert{Name: "PreparedTransactionsHigh", Severity: "warning",
			Detail: fmt.Sprintf("%d rows in the decision log", n)})
	}
	for _, s := range in.Streams {
		if s.Lost || s.LostSlots > 0 {
			alerts = append(alerts, Alert{Name: "StreamSlotLost", Severity: "critical",
				Detail: fmt.Sprintf("stream %s has %d lost slot(s)", s.Name, max(s.LostSlots, 1))})
		}
	}
	for _, w := range in.Paused {
		if in.Now.Sub(w.UpdatedAt) >= CutoverPauseThreshold {
			alerts = append(alerts, Alert{Name: "CutoverPauseExceeded", Severity: "warning",
				Detail: fmt.Sprintf("%s workflow %s paused for %s", w.Kind, w.ID, shortDuration(in.Now.Sub(w.UpdatedAt)))})
		}
	}
	if in.BackupsKnown {
		switch {
		case in.LatestBackup == nil:
			alerts = append(alerts, Alert{Name: "BackupMissing", Severity: "warning", Detail: "no completed backup exists"})
		case in.Now.Sub(*in.LatestBackup) >= BackupStaleThreshold:
			alerts = append(alerts, Alert{Name: "BackupStale", Severity: "critical",
				Detail: fmt.Sprintf("newest completed backup finished %s ago", shortDuration(in.Now.Sub(*in.LatestBackup)))})
		}
	}
	return alerts
}

// AlertsView is what the /alerts page renders.
type AlertsView struct {
	Alerts    []Alert `json:"alerts"`
	Firing    int     `json:"firing"`
	CheckedAt string  `json:"checkedAt"`
}

func shortDuration(d time.Duration) string {
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	case d >= time.Minute:
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
}
