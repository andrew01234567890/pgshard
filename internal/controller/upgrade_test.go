package controller

import (
	"strings"
	"testing"
	"time"
)

func TestMissingExtensions(t *testing.T) {
	cases := []struct {
		name      string
		installed []string
		available []string
		want      string
	}{
		{"all present", []string{"plpgsql", "pgcrypto"}, []string{"pgcrypto", "plpgsql", "uuid-ossp"}, ""},
		{"one missing", []string{"plpgsql", "timescaledb"}, []string{"plpgsql"}, "timescaledb"},
		{"sorted and deduplicated", []string{"zzz", "aaa", "zzz"}, nil, "aaa,zzz"},
		{"nothing installed", nil, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := strings.Join(MissingExtensions(tc.installed, tc.available), ","); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestUpgradeRollbackWaitsForReverseThenCancels(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitched)
	h.wf.spec.Rollback = true
	if !h.pass(t) {
		t.Fatal("rollback request must advance the stage")
	}
	if h.wf.stage != StageRollingBack {
		t.Fatalf("stage %s", h.wf.stage)
	}
	h.pass(t)
	if h.wf.stage != StageRollingBack {
		t.Fatalf("rollback must hold while reverse replication is behind; stage %s", h.wf.stage)
	}
	h.ops.reverseCaughtUp = true
	h.pass(t)
	if h.wf.stage != StageRolledBack {
		t.Fatalf("stage %s", h.wf.stage)
	}
	if !strings.HasPrefix(h.store.finished, StateCancelled) || !strings.Contains(h.store.finished, "rolled back") {
		t.Fatalf("finished %q", h.store.finished)
	}
	if h.ops.calls[len(h.ops.calls)-1] != "complete" {
		t.Fatalf("replication objects not dropped: %v", h.ops.calls)
	}
}

func TestUpgradeRollbackIgnoredAfterCompletion(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitched)
	h.clock = h.clock.Add(25 * time.Hour)
	h.runUntil(t, StageCompleted)
	h.wf.spec.Rollback = true
	if h.pass(t) {
		t.Fatal("a completed workflow must not roll back")
	}
}
