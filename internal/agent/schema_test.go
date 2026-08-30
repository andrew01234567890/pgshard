package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

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
	resp, err := srv.MaterializeSchema(context.Background(), &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src dbname=app", Database: "app", Epoch: proto.Uint64(in.epoch.Current())})
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
	resp, err = srv.MaterializeSchema(context.Background(), &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src dbname=app", Database: "app", Epoch: proto.Uint64(in.epoch.Current())})
	if err != nil || resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "psql: ") || !strings.Contains(resp.GetError().GetMessage(), "relation exists") {
		t.Fatalf("psql failure must be reported: %v %v", err, resp.GetError())
	}
	for _, bad := range []string{"", "app db", "a'b", "app\ndb", "app\tdb"} {
		resp, err = srv.MaterializeSchema(context.Background(), &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src", Database: bad, Epoch: proto.Uint64(in.epoch.Current())})
		if err != nil || resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "invalid database name") {
			t.Fatalf("database %q: %v %v", bad, err, resp.GetError())
		}
	}
}

// TestMaterializeSchemaProvesItIsTalkingToThePrimary: a schema copy can
// outlive a failover of its own target, so it must prove the member is
// still the one serving before it starts -- as every other mutating call
// does. It used to carry no epoch at all, and would happily write the
// schema onto a member that had stopped being the primary while it ran.
func TestMaterializeSchemaProvesItIsTalkingToThePrimary(t *testing.T) {
	in := newTestInstance(t)
	srv := NewServer(in, in.epoch, nil, in.log, nil)
	ctx := context.Background()

	// No epoch at all: refused rather than read as zero, because a caller
	// that does not name an epoch cannot be shown to mean this member.
	resp, err := srv.MaterializeSchema(ctx, &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src", Database: "app"})
	if err != nil || resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "no epoch") {
		t.Fatalf("a request with no epoch: %v %v", err, resp.GetError())
	}

	// An epoch that is not this member's: refused.
	resp, err = srv.MaterializeSchema(ctx, &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src", Database: "app", Epoch: proto.Uint64(in.epoch.Current() + 1)})
	if err != nil || resp.GetError() == nil {
		t.Fatalf("a request naming another epoch: %v %v", err, resp.GetError())
	}
	if resp.GetEpoch() != in.epoch.Current() {
		t.Fatalf("the response reported epoch %d, want the member's own %d", resp.GetEpoch(), in.epoch.Current())
	}
}
