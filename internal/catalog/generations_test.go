package catalog

import (
	"strings"
	"testing"
)

// TestErrNoShardMapGenerationSaysWhichTableAndWhy: the message is the
// deliverable. What routers actually logged was "cannot scan NULL into
// *int64", which names neither the table, nor that it is empty, nor that a
// catalog copy empties it -- three of them repeated that line for ten
// minutes during a catalog upgrade and it pointed at nothing.
func TestErrNoShardMapGenerationSaysWhichTableAndWhy(t *testing.T) {
	msg := ErrNoShardMapGeneration.Error()
	for _, want := range []string{"pgshard.shard_map_generation", "no row", "copy"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q does not say %q", msg, want)
		}
	}
	if strings.Contains(msg, "dest[0]") || strings.Contains(msg, "*int64") {
		t.Errorf("message %q still reads like a driver failure", msg)
	}
}
