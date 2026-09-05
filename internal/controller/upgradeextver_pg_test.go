package controller

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// readInstalled and readAvailable run the precheck's own SQL, so this test
// measures the queries the controller uses rather than a paraphrase of them.
func readInstalled(t *testing.T, conn *pgx.Conn) []InstalledExtension {
	t.Helper()
	rows, err := conn.Query(context.Background(), installedExtensionsSQL)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[InstalledExtension])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func readAvailable(t *testing.T, conn *pgx.Conn) map[string]TargetExtension {
	t.Helper()
	rows, err := conn.Query(context.Background(), availableExtensionsSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]TargetExtension{}
	for rows.Next() {
		var name, def string
		var from []string
		if err := rows.Scan(&name, &def, &from); err != nil {
			t.Fatal(err)
		}
		reach := map[string]bool{}
		for _, v := range from {
			reach[v] = true
		}
		out[name] = TargetExtension{Default: def, ReachableFrom: reach}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestTheOrdinaryMajorUpgradePassesTheExtensionPrecheck is the reason this
// check could be written at all.
//
// The risk with a version check is not that it misses something; it is that
// it refuses upgrades that would have worked, which is worse than no check.
// Five contrib extensions ship a different default version on 19 than on 18,
// and a naive "the versions must match" would refuse every cluster using any
// of them for doing the ordinary thing.
//
// So this asserts the property that matters against the real pair of images:
// everything PostgreSQL 18 can install is carried by 19. It is a measurement,
// not a fixture -- if a future 19 drops an update path the check depends on,
// this fails and names it, which is exactly when somebody should look.
func TestTheOrdinaryMajorUpgradePassesTheExtensionPrecheck(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	src := connect(t, startPostgresImage(t, pgImage, nil))
	tgt := connect(t, startPostgresImage(t, pgImage19, nil))

	// Every extension 18 offers, installed. CREATE EXTENSION is the only
	// way to get a row in pg_extension, and an extension that cannot be
	// installed on the source cannot be on a source cluster either.
	var names []string
	rows, err := src.Query(ctx, `SELECT name FROM pg_available_extensions ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	names, err = pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	var installedNames []string
	for _, n := range names {
		if _, err := src.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS `+pgx.Identifier{n}.Sanitize()+` CASCADE`); err != nil {
			// Some contrib modules need a shared library preloaded or a
			// superuser-only setting; one that will not install here is not
			// one a source cluster has either.
			t.Logf("skipping %s: %v", n, err)
			continue
		}
		installedNames = append(installedNames, n)
	}
	if len(installedNames) < 20 {
		t.Fatalf("only %d extensions installed; the fixture is not measuring what it claims", len(installedNames))
	}

	installed := readInstalled(t, src)
	available := readAvailable(t, tgt)
	if bad := UnsupportedExtensions(installed, available); len(bad) > 0 {
		t.Fatalf("the ordinary 18-to-19 upgrade must not be refused, but the precheck says:\n  %v", bad)
	}

	// A control, so the pass above is not vacuous: a version the target
	// cannot reach IS refused, using the same real catalogue.
	differing := 0
	for _, e := range installed {
		if t, ok := available[e.Name]; ok && t.Default != e.Version {
			differing++
		}
	}
	if differing == 0 {
		t.Fatal("no extension has a different default on the target, so this test would pass without checking anything")
	}
	t.Logf("%d of %d installed extensions have a different default version on the target major", differing, len(installed))

	fake := []InstalledExtension{{Name: installed[0].Name, Version: "0.0-not-a-real-version"}}
	if bad := UnsupportedExtensions(fake, available); len(bad) != 1 {
		t.Fatalf("a version the target cannot reach must be refused, got %v", bad)
	}
}
