package router

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// TestTheReshardDDLRefusalNamesAWayOut (PGS-876): a reshard that FAILS
// leaves its target set in provisioning, and Snapshot.Resharding() is true
// for exactly that state -- so the router goes on refusing every DDL
// statement cluster-wide until somebody clears the set. A failed reshard is
// the moment an operator most needs DDL, to repair or work around.
//
// The refusal's hint used to say only "retry once the reshard completes".
// For the failed case that is advice that can never come true, and the
// snapshot cannot tell a failed reshard from a running one, so the hint has
// to name both. The other half was that the two DDL paths disagreed: the
// home-DDL refusal carried no hint at all, so which path a statement took
// decided whether the client was told anything about recovering.
func TestTheReshardDDLRefusalNamesAWayOut(t *testing.T) {
	// BOTH paths, because they used to disagree and a shared helper is not
	// evidence that both call it: the fanned-out refusal carried the stale
	// hint and the home-DDL one carried none at all.
	for _, tc := range []struct {
		name  string
		local bool
	}{
		{"FannedOutDDL", false},
		{"HomeDDL", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newDDLHarness(t, &fakeQueue{noQueue: true})
			ctx := context.Background()
			conn := h.connect(t, h.dsn())

			// A set stuck in provisioning is what both a running and a
			// failed reshard look like to a router.
			s := *h.snap
			serving := make(map[snapshot.ShardKey]snapshot.Serving, len(s.Serving))
			for k, v := range s.Serving {
				serving[k] = v
			}
			k := snapshot.ShardKey{ShardSet: DefaultShardSet, ShardID: 0}
			stuck := serving[k]
			stuck.State = "provisioning"
			serving[k] = stuck
			s.Serving = serving
			if tc.local {
				dbs := make(map[string]catalog.Database, len(s.Databases))
				for n, d := range s.Databases {
					if n == "app" {
						d.LocalOnly = true
					}
					dbs[n] = d
				}
				s.Databases = dbs
			}
			h.setSnap(&s)

			_, err := conn.Exec(ctx, "create table hinted (id int primary key)")
			if err == nil {
				t.Fatal("DDL was accepted while a reshard held the shard map")
			}
			var pe *pgconn.PgError
			if !errors.As(err, &pe) {
				t.Fatalf("want a PostgreSQL error, got %T: %v", err, err)
			}
			t.Logf("refusal: %s\nhint: %s", pe.Message, pe.Hint)
			if pe.Code != "0A000" {
				t.Errorf("refusal code %s, want 0A000", pe.Code)
			}
			// The recovery an operator can actually perform. Naming
			// spec.shards is the point: it is the thing they revert, and
			// before this nothing in the refusal said so.
			for _, want := range []string{"FAILED", "spec.shards", "docs/resharding.md"} {
				if !strings.Contains(pe.Hint, want) {
					t.Errorf("the hint does not mention %q, so a failed reshard leaves the operator with advice that cannot come true:\n%s", want, pe.Hint)
				}
			}
		})
	}
}
