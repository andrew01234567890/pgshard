package router

import (
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

type controlPlane []string

func (c controlPlane) MayUseCatalog(role string) bool {
	return snapshot.NewRolesWithCatalogAccess(nil, c).MayUseCatalog(role)
}

// Every role pgshard knows is materialized on the catalog group with LOGIN
// and its verifier, so routing dbname=pgshard by name alone handed an
// ordinary application credential a session on the control-plane server.
// Its max_connections is the pooler's budget, so one tenant holding sessions
// open there takes the slots the routers and the poolers' LISTEN connections
// need, and the cluster stops seeing generation and epoch changes.
func TestOnlyControlPlaneRolesReachTheCatalogDatabase(t *testing.T) {
	// What the catalog server answers for pg_has_role: the bootstrap
	// superuser, an admin, a reader, and a role granted one of them.
	access := controlPlane{"postgres", "pgshard_admin", "pgshard_reader", "ops"}
	for _, c := range []struct {
		user    string
		refused bool
	}{
		{user: "postgres"},
		{user: "pgshard_admin"},
		{user: "pgshard_reader"},
		{user: "ops"},
		{user: "tenant", refused: true},
	} {
		r := newCatalogRouter(t, access)
		_, err := r.NewExecutor(catalogSession("pgshard", c.user))
		if !c.refused {
			if err != nil {
				t.Errorf("%s: %v", c.user, err)
			}
			continue
		}
		var pe *pgwire.Error
		if !errors.As(err, &pe) || pe.Code != pgwire.CodeInsufficientPrivilege || !strings.Contains(pe.Message, "control plane") {
			t.Errorf("%s: %v, want a 42501 naming the control plane", c.user, err)
		}
	}
}

// A router wired with no check cannot answer who is an admin, and a
// privilege it cannot confirm is not one it may grant. Ordinary databases
// are not gated either way.
func TestTheCatalogDatabaseIsRefusedWithoutACheckAndOtherDatabasesAreNot(t *testing.T) {
	r := newCatalogRouter(t, nil)
	r.cfg.CatalogAccess = nil
	if _, err := r.NewExecutor(catalogSession("pgshard", "pgshard_admin")); err == nil {
		t.Error("a router that cannot check access admitted a catalog session")
	}
	r = newCatalogRouter(t, controlPlane{"pgshard_admin"})
	if _, err := r.NewExecutor(catalogSession("app", "tenant")); err != nil {
		t.Errorf("an ordinary database was gated: %v", err)
	}
}

func newCatalogRouter(t *testing.T, access controlPlane) *Router {
	t.Helper()
	snap := &snapshot.Snapshot{Databases: map[string]catalog.Database{"app": {Name: "app"}}}
	r, err := New(Config{Snapshot: func() *snapshot.Snapshot { return snap },
		Poolers:       NewPoolers(nil, func() *snapshot.Snapshot { return snap }, nil),
		CatalogAccess: access})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func catalogSession(database, user string) pgwire.SessionInfo {
	return pgwire.SessionInfo{ID: 1, User: user, Database: database, Auth: &pgwire.AuthResult{SCRAM: &pgwire.SCRAMKeys{}}}
}
