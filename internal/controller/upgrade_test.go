package controller

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestUnsupportedExtensions(t *testing.T) {
	target := map[string]TargetExtension{
		"plpgsql":  {Default: "1.0"},
		"pgcrypto": {Default: "1.3"},
		// The shape the 18-to-19 upgrade actually has: a newer default,
		// with PostgreSQL declaring a path from the older version.
		"btree_gist": {Default: "1.9", ReachableFrom: map[string]bool{"1.8": true, "1.7": true}},
		// Available, newer, and PostgreSQL says nothing about how to get
		// from 1.9 to 2.0.
		"orphaned": {Default: "2.0", ReachableFrom: map[string]bool{"1.9.1": true}},
	}
	cases := []struct {
		name      string
		installed []InstalledExtension
		want      string
	}{
		{"same version passes", []InstalledExtension{{"plpgsql", "1.0"}}, ""},
		{"absent name is named", []InstalledExtension{{"timescaledb", "2.14"}}, "timescaledb is not available on the target major"},
		{
			"a newer default with an update path is the ordinary upgrade",
			[]InstalledExtension{{"btree_gist", "1.8"}},
			"",
		},
		{
			"a newer default with no path is refused, naming both versions",
			[]InstalledExtension{{"orphaned", "1.9"}},
			"orphaned is installed at 1.9 and the target major installs 2.0, with no update path between them",
		},
		{
			"a version the target cannot reach is refused even though the name is there",
			[]InstalledExtension{{"btree_gist", "1.1"}},
			"btree_gist is installed at 1.1 and the target major installs 1.9, with no update path between them",
		},
		{"sorted and deduplicated", []InstalledExtension{{"zzz", "1"}, {"aaa", "1"}, {"zzz", "1"}},
			"aaa is not available on the target major|zzz is not available on the target major"},
		{"nothing installed", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := strings.Join(UnsupportedExtensions(tc.installed, target), "|"); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

// A version string is never compared, because extversion is opaque and
// PostgreSQL does not order it. "1.11" sorts before "1.9" as a string and
// after it as anybody reading it would mean, and "2.1-beta" has no defined
// position at all -- which is why PostgreSQL has update paths instead. This
// pins that no ordering crept in: every one of these passes or fails on the
// declared path, not on how the two strings compare.
func TestExtensionVersionsAreNeverOrdered(t *testing.T) {
	target := map[string]TargetExtension{
		"ext": {Default: "1.9", ReachableFrom: map[string]bool{"1.11": true, "2.1-beta": true}},
	}
	for _, v := range []string{"1.11", "2.1-beta"} {
		if got := UnsupportedExtensions([]InstalledExtension{{"ext", v}}, target); len(got) > 0 {
			t.Errorf("%s has a declared path to 1.9 and must be accepted: %v", v, got)
		}
	}
	// And one with no path is refused however it compares.
	if got := UnsupportedExtensions([]InstalledExtension{{"ext", "1.10"}}, target); len(got) != 1 {
		t.Errorf("1.10 has no declared path and must be refused: %v", got)
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
	ext := func(def string, from ...string) TargetExtension {
		r := map[string]bool{}
		for _, v := range from {
			r[v] = true
		}
		return TargetExtension{Default: def, ReachableFrom: r}
	}
	for _, c := range []struct {
		name      string
		shards    []map[string]TargetExtension
		want      []string
		disagreed []string
	}{
		{"identical", []map[string]TargetExtension{
			{"plpgsql": ext("1.0"), "pgcrypto": ext("1.3")},
			{"plpgsql": ext("1.0"), "pgcrypto": ext("1.3")},
		}, []string{"pgcrypto", "plpgsql"}, nil},
		{"one shard is missing one", []map[string]TargetExtension{
			{"plpgsql": ext("1.0"), "postgis": ext("3.5")},
			{"plpgsql": ext("1.0")},
		}, []string{"plpgsql"}, nil},
		{"nothing in common", []map[string]TargetExtension{
			{"postgis": ext("3.5")}, {"pgcrypto": ext("1.3")},
		}, nil, nil},
		{"a shard offering nothing offers nothing", []map[string]TargetExtension{
			{"plpgsql": ext("1.0")}, {},
		}, nil, nil},
		// Worse than missing: the same restore would produce a different
		// schema per shard, so it gets its own answer rather than being
		// reported as absent.
		{"shards disagreeing on the default version", []map[string]TargetExtension{
			{"pg_stat_statements": ext("1.12")}, {"pg_stat_statements": ext("1.13")},
		}, nil, []string{"pg_stat_statements"}},
		// A version reachable on one shard and not another is the same
		// hazard as an extension present on one and not another.
		{"reachability is intersected too", []map[string]TargetExtension{
			{"btree_gist": ext("1.9", "1.8", "1.7")},
			{"btree_gist": ext("1.9", "1.8")},
		}, []string{"btree_gist"}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, disagreed := MergeTargetExtensions(c.shards)
			if names := sortedKeys(got); !slices.Equal(names, c.want) {
				t.Fatalf("common = %v, want %v", names, c.want)
			}
			if !slices.Equal(disagreed, c.disagreed) {
				t.Fatalf("disagreed = %v, want %v", disagreed, c.disagreed)
			}
			if c.name == "reachability is intersected too" && got["btree_gist"].ReachableFrom["1.7"] {
				t.Fatal("1.7 is reachable on one target shard only and must not be treated as reachable")
			}
		})
	}
}
