package admin

import (
	"net/http"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func (s *Server) twoPCSource() TwoPCSource {
	src, _ := s.Catalog.(TwoPCSource)
	return src
}

func (s *Server) handleTwoPC(w http.ResponseWriter, r *http.Request) {
	v, err := BuildTwoPCView(r.Context(), s.twoPCSource(), time.Now())
	if err != nil {
		s.fail(w, err)
		return
	}
	if r.Header.Get("HX-Request") == "true" {
		s.render(w, "twopc_table.html", v)
		return
	}
	s.render(w, "twopc.html", v)
}

func (s *Server) handleAPITwoPC(w http.ResponseWriter, r *http.Request) {
	v, err := BuildTwoPCView(r.Context(), s.twoPCSource(), time.Now())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, v)
}

func (s *Server) buildAlerts(r *http.Request) (AlertsView, error) {
	src := s.twoPCSource()
	if src == nil {
		return AlertsView{}, ErrNoTwoPCSource
	}
	ctx := r.Context()
	now := time.Now()
	in := AlertInputs{Now: now}
	var err error
	if in.Decisions, err = src.ListDecisions(ctx); err != nil {
		return AlertsView{}, err
	}
	if in.Paused, err = src.ListPausedWorkflows(ctx); err != nil {
		return AlertsView{}, err
	}
	if ss := s.streamSource(); ss != nil {
		overview, err := BuildStreamsOverview(ctx, ss)
		if err != nil {
			return AlertsView{}, err
		}
		in.Streams = overview.Streams
	}
	if backups, err := ListBackups(ctx, s.Client, s.Namespace, s.Cluster); err == nil {
		in.BackupsKnown = true
		for _, b := range backups {
			if b.Phase == pgshardv1alpha1.BackupPhaseCompleted && b.CompletedAt != nil &&
				(in.LatestBackup == nil || b.CompletedAt.After(*in.LatestBackup)) {
				in.LatestBackup = b.CompletedAt
			}
		}
	}
	alerts := DeriveAlerts(in)
	return AlertsView{Alerts: alerts, Firing: len(alerts), CheckedAt: now.UTC().Format(time.RFC3339)}, nil
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	v, err := s.buildAlerts(r)
	if err != nil {
		s.fail(w, err)
		return
	}
	if r.Header.Get("HX-Request") == "true" {
		s.render(w, "alerts_table.html", v)
		return
	}
	s.render(w, "alerts.html", v)
}

func (s *Server) handleAPIAlerts(w http.ResponseWriter, r *http.Request) {
	v, err := s.buildAlerts(r)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, v)
}
