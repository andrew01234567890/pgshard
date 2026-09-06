package operator

import (
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// The controller was the last component reaching the catalog as the
// superuser, which meant anything that could read its environment held
// direct write access to every shard and the catalog, bypassing the router.
// It now uses its own login role.
//
// PGPASSWORD stays the superuser's: the shard and subscription DSNs still
// need it, and libpq applies that variable to every connection that does not
// carry its own password -- so the catalog DSN has to carry one, from a file
// rather than argv, where /proc/<pid>/cmdline would expose it.
func TestTheControllerReachesTheCatalogAsItsOwnRole(t *testing.T) {
	c := newCluster("cred")
	dep := Renderer{}.ControllerDeployment(c)
	var args []string
	var mounted bool
	for _, ct := range dep.Spec.Template.Spec.Containers {
		args = append(args, ct.Args...)
		for _, m := range ct.VolumeMounts {
			if m.Name == controllerLoginVolume {
				mounted = true
			}
		}
		for _, e := range ct.Env {
			if e.Name == "PGPASSWORD" && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef.Name != SecretName(c.Name) {
				t.Errorf("PGPASSWORD is %s; the shard DSNs still need the superuser's",
					e.ValueFrom.SecretKeyRef.Name)
			}
		}
	}
	joined := strings.Join(args, " ")
	var catalogDSN string
	for _, a := range args {
		if v, ok := strings.CutPrefix(a, "--catalog-dsn="); ok {
			catalogDSN = v
		}
	}
	if catalogDSN == "" {
		t.Fatalf("no --catalog-dsn: %v", args)
	}
	if !strings.Contains(catalogDSN, "user="+catalog.ControllerRole) {
		t.Errorf("the catalog DSN is %q, want it to name %s", catalogDSN, catalog.ControllerRole)
	}
	if strings.Contains(catalogDSN, "user="+superuserName) {
		t.Errorf("the catalog DSN still names the superuser: %q", catalogDSN)
	}
	// Role and DCL work on the catalog group stays the superuser's, the
	// same as on every shard, and takes its password from PGPASSWORD.
	if got := argOf(dep.Spec.Template.Spec.Containers[0].Args, "--catalog-role-dsn="); got != CatalogDSN(c) {
		t.Errorf("catalog role DSN %q, want %q", got, CatalogDSN(c))
	}
	if !strings.Contains(joined, "--catalog-password-file="+controllerLoginDir) {
		t.Errorf("the catalog password does not come from a file: %v", args)
	}
	if strings.Contains(joined, "password=") {
		t.Error("a password is in argv, where /proc/<pid>/cmdline exposes it")
	}
	if !mounted {
		t.Error("the controller login Secret is not mounted")
	}

	// The shard DSNs are untouched: they do superuser work on the shards,
	// which is PGS-428 and not this change.
	if !strings.Contains(joined, "--shard-dsn-template=") || !strings.Contains(joined, "user="+superuserName) {
		t.Errorf("the shard DSN template no longer names the superuser: %v", args)
	}
}
