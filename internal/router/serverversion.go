package router

import (
	"fmt"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
)

// ServerVersion is what a router reports as the server_version parameter:
// the PostgreSQL major whose SQL surface it actually offers.
//
// That is the lowest major among the live shard sets and this router's own
// grammar. The shards bound it because a statement no old-major group would
// accept must not be advertised as available while one still serves, and
// the grammar bounds it because a router cannot offer syntax it cannot
// parse -- a cluster fully on 19 read by a router built against the 18
// grammar has an 18 surface, and saying 19 would invite exactly the
// statements it refuses.
//
// The minor is 0 rather than a real one. Nothing here knows the minor the
// shards run, and the compiled-in "18.6" this replaced was a claim about a
// specific minor that was wrong the moment the shards were patched.
func ServerVersion(s *snapshot.Snapshot) string {
	var present []int
	if s != nil {
		present = s.ServingMajors()
	}
	return fmt.Sprintf("%d.0 (pgshard)", pgparser.EffectiveMajor(append(present, pgparser.Major)))
}
