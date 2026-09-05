package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	if _, err := srv.MaterializeSchema(context.Background(), &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src dbname=app", Database: "app", Epoch: proto.Uint64(in.epoch.Current())}); err != nil {
		t.Fatalf("MaterializeSchema: %v", err)
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
	_, err = srv.MaterializeSchema(context.Background(), &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src dbname=app", Database: "app", Epoch: proto.Uint64(in.epoch.Current())})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "psql: ") || !strings.Contains(err.Error(), "relation exists") {
		t.Fatalf("psql failure must fail the RPC: %v", err)
	}
	for _, bad := range []string{"", "app db", "a'b", "app\ndb", "app\tdb"} {
		_, err = srv.MaterializeSchema(context.Background(), &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src", Database: bad, Epoch: proto.Uint64(in.epoch.Current())})
		if err == nil || !strings.Contains(err.Error(), "invalid database name") {
			t.Fatalf("database %q: %v", bad, err)
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
	_, err := srv.MaterializeSchema(ctx, &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src", Database: "app"})
	if err == nil || !strings.Contains(err.Error(), "no epoch") {
		t.Fatalf("a request with no epoch: %v", err)
	}

	// A member whose epoch is not zero, so a refusal that carries NO epoch
	// cannot pass by reporting the zero value -- which is what the first
	// version of this assertion did.
	if err := in.epoch.Accept(7); err != nil {
		t.Fatal(err)
	}

	// An epoch that is not this member's: refused, with the code that says
	// the caller's view is what is stale.
	_, err = srv.MaterializeSchema(ctx, &pgshardv1.MaterializeSchemaRequest{SourceConninfo: "host=src", Database: "app", Epoch: proto.Uint64(in.epoch.Current() + 1)})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a request naming another epoch: %v", err)
	}
	// The member's own epoch travels with the refusal. It used to be a field
	// of the response, which a caller could only read by ignoring the status;
	// dropping it in the move would have cost a Status round trip to learn
	// what should have been sent.
	st, _ := status.FromError(err)
	var got uint64
	var carried bool
	for _, d := range st.Details() {
		if se, ok := d.(*pgshardv1.StaleEpoch); ok {
			got, carried = se.GetCurrent(), true
		}
	}
	if !carried || got != in.epoch.Current() {
		t.Fatalf("the refusal reported epoch %d (present=%v), want the member's own %d", got, carried, in.epoch.Current())
	}
}
