package router

import "testing"

// TestACatalogSessionAsksForThePhysicalDatabase: the catalog is a SCHEMA
// inside an ordinary database, so `dbname=pgshard` -- the name the guide
// documents, and the only way to edit the desired-state tables -- has no
// database of that name behind it. Sending the client's name to the pooler
// gets PostgreSQL's own 3D000 for a database that does not exist, which is
// what the documented catalog login did.
func TestACatalogSessionAsksForThePhysicalDatabase(t *testing.T) {
	r := &Router{cfg: Config{CatalogDatabase: "pgshard", CatalogPhysicalDatabase: "postgres"}}

	cat := &Executor{r: r, home: Shard{Set: CatalogShardSet}}
	cat.info.Database = "pgshard"
	if got := cat.backendDatabase(); got != "postgres" {
		t.Errorf("a catalog session opens a backend on %q; there is no database of that name", got)
	}

	// Every other session keeps the client's database: those are real
	// databases and the router must not rewrite them.
	app := &Executor{r: r, home: Shard{Set: "default"}}
	app.info.Database = "app"
	if got := app.backendDatabase(); got != "app" {
		t.Errorf("an application session opened a backend on %q, not the database it asked for", got)
	}
}
