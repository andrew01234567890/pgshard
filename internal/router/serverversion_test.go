package router

import (
	"fmt"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
)

// The version was compiled in, so it described the router that was built
// rather than the cluster that is serving. What a client is entitled to hear
// is the surface it will actually get.
func TestServerVersionIsTheSurfaceThatIsActuallyOffered(t *testing.T) {
	grammar := pgparser.Major
	for _, c := range []struct {
		name   string
		majors map[string]int
		want   int
	}{
		{"no snapshot yet, so only this router's own grammar is known", nil, grammar},
		{"a set the catalog never stamped says nothing", map[string]int{}, grammar},
		{"a cluster on the grammar's own major", map[string]int{"default": grammar}, grammar},
		{"mid-upgrade, the old set still serves and still has to be parseable",
			map[string]int{"default": grammar, "g2": grammar + 1}, grammar},
		{"a set older than the grammar holds the surface down to itself",
			map[string]int{"default": grammar - 1}, grammar - 1},
		{"every set past the grammar is still capped by what this router parses",
			map[string]int{"default": grammar + 1, "g2": grammar + 1}, grammar},
	} {
		t.Run(c.name, func(t *testing.T) {
			var s *snapshot.Snapshot
			if c.majors != nil {
				s = &snapshot.Snapshot{PGMajors: c.majors}
			}
			want := fmt.Sprintf("%d.0 (pgshard)", c.want)
			if got := ServerVersion(s); got != want {
				t.Fatalf("ServerVersion = %q, want %q", got, want)
			}
		})
	}
}
