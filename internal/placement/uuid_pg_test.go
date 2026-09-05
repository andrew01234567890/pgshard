package placement

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestParseUUIDAcceptsWhatPostgreSQLAccepts.
//
// ParseUUID is a port of string_to_uuid, and the thing that matters is that
// it draws the SAME line: a spelling the router rejects but the shard
// accepts is a row the router will not route, and a spelling the router
// accepts but the shard rejects would be hashed from bytes no row has.
// PostgreSQL decides, not a canonical-form intuition -- it takes braces,
// omits hyphens, and allows a hyphen after any even number of BYTES rather
// than only the four familiar positions.
func TestParseUUIDAcceptsWhatPostgreSQLAccepts(t *testing.T) {
	for _, img := range pgImages {
		t.Run(img.label, func(t *testing.T) {
			conn := startPostgres(t, img.name)
			ctx := context.Background()
			for _, s := range []string{
				"0189d0f4-4c1a-7c2e-9c1a-2f1b3c4d5e6f",
				"0189D0F4-4C1A-7C2E-9C1A-2F1B3C4D5E6F",
				"0189d0f44c1a7c2e9c1a2f1b3c4d5e6f",
				"{0189d0f4-4c1a-7c2e-9c1a-2f1b3c4d5e6f}",
				"{0189d0f44c1a7c2e9c1a2f1b3c4d5e6f}",
				// A hyphen after an even number of bytes, which PostgreSQL
				// allows and the canonical form does not have.
				"0189d0f44c1a-7c2e9c1a2f1b3c4d5e6f",
				"0189-d0f4-4c1a-7c2e-9c1a-2f1b-3c4d-5e6f",
				// Rejected by both.
				"",
				"not-a-uuid",
				"0189d0f4-4c1a-7c2e-9c1a-2f1b3c4d5e6",
				"0189d0f4-4c1a-7c2e-9c1a-2f1b3c4d5e6ff",
				"{0189d0f4-4c1a-7c2e-9c1a-2f1b3c4d5e6f",
				"0189d0f4-4c1a-7c2e-9c1a-2f1b3c4d5e6f}",
				"0-189d0f44c1a7c2e9c1a2f1b3c4d5e6f",
				"0189d0f4-4c1a-7c2e-9c1a-2f1b3c4d5e6-f",
				"zz89d0f4-4c1a-7c2e-9c1a-2f1b3c4d5e6f",
			} {
				var pgOK bool
				if err := conn.QueryRow(ctx, `SELECT $1::uuid IS NOT NULL`, s).Scan(&pgOK); err != nil {
					pgOK = false
					// A failed cast poisons the session for the rest of it.
					if _, rerr := conn.Exec(ctx, `SELECT 1`); rerr != nil {
						t.Fatalf("connection unusable after %q: %v", s, rerr)
					}
				}
				_, ours := ParseUUID(s)
				if ours != pgOK {
					t.Errorf("%q: ParseUUID=%v, PostgreSQL accepts=%v", s, ours, pgOK)
				}
			}
		})
	}
}

// TestAUUIDKeyHashesWhereTheCopyPutsIt is the end-to-end property the hash
// comparison above does not reach.
//
// The controller moves a uuid-keyed row with uuid_hash_extended over its
// sixteen bytes. The router has to arrive at the SAME keyspace position from
// the text a client wrote, or it places and finds rows at one position while
// the copy moves them to another: after a reshard the row exists and the
// router looks on a different shard, with no error anywhere.
//
// Before this, the router hashed the 36 characters with hashtextextended.
// Three of four sample uuids landed on a different shard of four.
func TestAUUIDKeyHashesWhereTheCopyPutsIt(t *testing.T) {
	for _, img := range pgImages {
		t.Run(img.label, func(t *testing.T) {
			conn := startPostgres(t, img.name)
			ctx := context.Background()
			var texts []string
			rows, err := conn.Query(ctx, `SELECT gen_random_uuid()::text FROM generate_series(1, 200)`)
			if err != nil {
				t.Fatal(err)
			}
			if texts, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
				t.Fatal(err)
			}
			for _, s := range texts {
				b, ok := ParseUUID(s)
				if !ok {
					t.Fatalf("%q did not parse", s)
				}
				ours, err := KeyspaceID(b)
				if err != nil {
					t.Fatal(err)
				}
				// Exactly the expression copysql.go builds for a uuid key.
				var theirs int64
				if err := conn.QueryRow(ctx, `SELECT uuid_hash_extended($1::uuid, $2)`, s, int64(PartitionSeed)).Scan(&theirs); err != nil {
					t.Fatal(err)
				}
				if ours != theirs {
					t.Fatalf("%s: router %d, copy filter %d", s, ours, theirs)
				}
			}
		})
	}
}
