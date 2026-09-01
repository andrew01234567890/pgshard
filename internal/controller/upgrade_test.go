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

// TestATargetOffersOnlyWhatEveryShardHas: availability is per
// installation, so an extension present on one target shard and not
// another would install on some shards and fail on the rest -- the same
// outcome as missing, found later and after the target groups are already
// provisioned.
func TestATargetOffersOnlyWhatEveryShardHas(t *testing.T) {
	for _, c := range []struct {
		name string
		a, b []string
		want []string
	}{
		{"identical", []string{"plpgsql", "pgcrypto"}, []string{"plpgsql", "pgcrypto"}, []string{"plpgsql", "pgcrypto"}},
		{"one shard is missing one", []string{"plpgsql", "postgis"}, []string{"plpgsql"}, []string{"plpgsql"}},
		{"nothing in common", []string{"postgis"}, []string{"pgcrypto"}, nil},
		{"a shard offering nothing offers nothing", []string{"plpgsql"}, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := intersect(c.a, c.b)
			if len(got) != len(c.want) {
				t.Fatalf("intersect(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("intersect(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
				}
			}
		})
	}
}
