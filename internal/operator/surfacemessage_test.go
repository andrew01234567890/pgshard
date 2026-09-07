package operator

import (
	"strconv"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgparser/grammar"
)

// The message an operator reads when the shards have moved past the routers
// has to be right about which side is which. It said the SQL surface follows
// a router build that parses the shards' major, which is the one thing that
// is not true in that state -- the whole point of the condition.
func TestTheSurfaceMessageNamesTheRouterGrammarNotTheShards(t *testing.T) {
	ahead := grammar.Major + 1
	msg := sqlSurfaceMessage(ahead)
	if !strings.Contains(msg, "a router build that parses "+strconv.Itoa(grammar.Major)) {
		t.Fatalf("message = %q, want the surface attributed to the grammar %d", msg, grammar.Major)
	}
	if strings.Contains(msg, "a router build that parses "+strconv.Itoa(ahead)) {
		t.Fatalf("message = %q, claims the routers parse the major only the shards run", msg)
	}
}
