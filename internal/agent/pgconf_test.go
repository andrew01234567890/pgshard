package agent

import (
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestRouterRoleMatchesTheCatalog: pg_hba admits the router's login role by
// name, and the catalog creates it by name. A rename in one place without
// the other locks every router out of its catalog.
func TestRouterRoleMatchesTheCatalog(t *testing.T) {
	if routerRole != catalog.RouterRole {
		t.Fatalf("pg_hba admits %q, the catalog creates %q", routerRole, catalog.RouterRole)
	}
	hba := RenderPgHBAConf(&Config{PodCIDR: "10.0.0.0/8"})
	var admitted bool
	for _, line := range strings.Split(hba, "\n") {
		f := strings.Fields(line)
		if len(f) == 5 && strings.HasPrefix(f[0], "host") && f[2] == catalog.RouterRole && f[4] == "scram-sha-256" {
			admitted = true
		}
	}
	if !admitted {
		t.Errorf("the router's role is not admitted over TCP, so it cannot reach the catalog:\n%s", hba)
	}
	// And an application role still is not.
	if !strings.Contains(hba, "reject") {
		t.Error("the reject line went missing")
	}
}
