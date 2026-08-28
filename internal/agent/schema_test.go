package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestMaterializeSchemaRunsDumpIntoPsql replaces pg_dump and psql in the
// supervisor's bin dir with scripts and checks the agent pipes the schema
// dump of the source into psql on the local database.
func TestMaterializeSchemaRunsDumpIntoPsql(t *testing.T) {
	in := newTestInstance(t)
	record := filepath.Join(t.TempDir(), "psql.txt")
	scripts := map[string]string{
		"pg_dump": "#!/bin/sh\necho \"DUMP $*\"\n",
		"psql":    fmt.Sprintf("#!/bin/sh\n{ echo \"ARGS $*\"; cat; } > %s\n", record),
	}
	for name, body := range scripts {
		if err := os.WriteFile(filepath.Join(in.sup.binDir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	srv := NewServer(in, in.epoch, nil, in.log, nil)
	resp, err := srv.MaterializeSchema(context.Background(), &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src dbname=app", Database: "app"})
	if err != nil || resp.GetError() != nil {
		t.Fatalf("MaterializeSchema: %v %v", err, resp.GetError())
	}
	got, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("ARGS -X -q -v ON_ERROR_STOP=1 --dbname=host=/tmp port=%d user=postgres dbname=app\nDUMP --schema-only --no-publications --no-subscriptions --dbname=host=src dbname=app\n", in.cfg.Port)
	if string(got) != want {
		t.Fatalf("psql saw:\n%s\nwant:\n%s", got, want)
	}

	if err := os.WriteFile(filepath.Join(in.sup.binDir, "psql"), []byte("#!/bin/sh\necho 'ERROR: relation exists' >&2\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	resp, err = srv.MaterializeSchema(context.Background(), &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src dbname=app", Database: "app"})
	if err != nil || resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "psql: ") || !strings.Contains(resp.GetError().GetMessage(), "relation exists") {
		t.Fatalf("psql failure must be reported: %v %v", err, resp.GetError())
	}
	for _, bad := range []string{"", "app db", "a'b", "app\ndb", "app\tdb"} {
		resp, err = srv.MaterializeSchema(context.Background(), &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src", Database: bad})
		if err != nil || resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "invalid database name") {
			t.Fatalf("database %q: %v %v", bad, err, resp.GetError())
		}
	}
}
