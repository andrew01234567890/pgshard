package agent

import (
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestControlPlaneLoginRolesMatchTheCatalog: pg_hba admits each control-plane
// login role by name, and the catalog creates it by name. A role the catalog
// creates and pg_hba does not admit cannot reach the catalog at all -- the
// reject line below catches it -- and a rename in one place without the other
// does the same.
func TestControlPlaneLoginRolesMatchTheCatalog(t *testing.T) {
	for _, c := range []struct{ rendered, created string }{
		{routerRole, catalog.RouterRole},
		{controllerRole, catalog.ControllerRole},
	} {
		if c.rendered != c.created {
			t.Fatalf("pg_hba admits %q, the catalog creates %q", c.rendered, c.created)
		}
		hba := RenderPgHBAConf(&Config{PodCIDR: "10.0.0.0/8"})
		var admitted bool
		for _, line := range strings.Split(hba, "\n") {
			f := strings.Fields(line)
			if len(f) == 5 && strings.HasPrefix(f[0], "host") && f[2] == c.created && f[4] == "scram-sha-256" {
				admitted = true
			}
		}
		if !admitted {
			t.Errorf("%s is not admitted over TCP, so it cannot reach the catalog:\n%s", c.created, hba)
		}
	}
	// And an application role still is not.
	if !strings.Contains(RenderPgHBAConf(&Config{PodCIDR: "10.0.0.0/8"}), "reject") {
		t.Error("the reject line went missing")
	}
}

// TestPgHBAAdmitsWhoeverPrimaryConninfoNames: a standby streams as the user
// in primary_conninfo, and pg_rewind reaches the same source as the same
// user over an ordinary connection. pg_hba rejects every identity it does
// not list by name, so the two files have to agree -- and they are rendered
// by different functions from different fields, which is how they could
// stop agreeing without anything failing to compile.
//
// Both connection types, because they match different pg_hba lines: a
// replication line does not admit pg_rewind and an "all" line does not admit
// a walsender.
func TestPgHBAAdmitsWhoeverPrimaryConninfoNames(t *testing.T) {
	c := &Config{PodCIDR: "10.0.0.0/8", PrimaryConninfo: "host=src port=5432 user=" + replicationRole}
	var user string
	for _, kv := range strings.Fields(PrimaryConninfo(c)) {
		if v, ok := strings.CutPrefix(kv, "user="); ok {
			user = v
		}
	}
	if user == "" {
		t.Fatal("primary_conninfo names no user")
	}
	admitted := map[string]bool{}
	for _, line := range strings.Split(RenderPgHBAConf(c), "\n") {
		f := strings.Fields(line)
		if len(f) == 5 && strings.HasPrefix(f[0], "host") && f[2] == user && f[4] == "scram-sha-256" {
			admitted[f[1]] = true
		}
	}
	for _, db := range []string{"replication", "all"} {
		if !admitted[db] {
			t.Errorf("pg_hba does not admit %q for %s, so a standby cannot stream or rewind:\n%s",
				user, db, RenderPgHBAConf(c))
		}
	}
}
