package catalog

import (
	"strings"
	"testing"
)

// TestOneStrategyPerMigration: the strategy column stores only direct and
// concurrent; multistep, rewrite and repack live in the metadata. Rebuilding
// them with three sequential assignments meant a row carrying two resolved
// to whichever was checked last -- silently running a different migration
// from the one that was planned, decided by the order of three ifs.
func TestOneStrategyPerMigration(t *testing.T) {
	for _, c := range []struct {
		name   string
		stored string
		meta   MigrationMeta
		want   string
	}{
		{"nothing in the metadata keeps the stored strategy", "direct", MigrationMeta{}, "direct"},
		{"concurrent is kept too", "concurrent", MigrationMeta{}, "concurrent"},
		{"steps mean multistep", "direct", MigrationMeta{Steps: []MigrationStep{{SQL: "select 1"}}}, StrategyMultistep},
		{"a rewrite means rewrite", "direct", MigrationMeta{Rewrite: &RewriteChange{}}, StrategyRewrite},
		{"repack means repack", "direct", MigrationMeta{Repack: true}, StrategyRepack},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := metaStrategy(c.stored, c.meta)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if got != c.want {
				t.Fatalf("strategy %q, want %q", got, c.want)
			}
		})
	}
}

// TestContradictoryMetadataIsNotResolved: two strategies at once is not a
// choice to be made by check order. No planner emits it, so the migration
// it describes is unknown -- and running the one that happened to be
// checked last is the worst of the available answers.
func TestContradictoryMetadataIsNotResolved(t *testing.T) {
	for _, c := range []struct {
		name string
		meta MigrationMeta
		want []string
	}{
		{"steps and rewrite", MigrationMeta{Steps: []MigrationStep{{SQL: "select 1"}}, Rewrite: &RewriteChange{}},
			[]string{StrategyMultistep, StrategyRewrite}},
		{"steps and repack", MigrationMeta{Steps: []MigrationStep{{SQL: "select 1"}}, Repack: true},
			[]string{StrategyMultistep, StrategyRepack}},
		{"rewrite and repack", MigrationMeta{Rewrite: &RewriteChange{}, Repack: true},
			[]string{StrategyRewrite, StrategyRepack}},
		{"all three", MigrationMeta{Steps: []MigrationStep{{SQL: "select 1"}}, Rewrite: &RewriteChange{}, Repack: true},
			[]string{StrategyMultistep, StrategyRewrite, StrategyRepack}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := metaStrategy("direct", c.meta)
			if err == nil {
				t.Fatalf("contradictory metadata resolved to %q instead of being refused", got)
			}
			// The message has to name what it found, or the operator is
			// left diffing JSON to learn which two.
			for _, w := range c.want {
				if !strings.Contains(err.Error(), w) {
					t.Fatalf("error %q does not name %q", err, w)
				}
			}
		})
	}
}
