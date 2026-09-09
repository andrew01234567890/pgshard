package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestASubscriptionOfAnotherDatabaseIsNotMistakenForOurs.
//
// pg_subscription is a SHARED catalog: a subscription made in one database
// is visible from every other. Its unique index is per database, so the same
// NAME may exist twice -- which is exactly why the unqualified existence
// query could not tell them apart.
//
// The consequence was silent: the first database created
// pgshard_reshard_gN_tT_sS, the second saw that row, took its own
// subscription as already made, and copied none of its rows. A cluster with
// one database never noticed.
//
// This runs the query the code runs, against real PostgreSQL, from the
// second database.
func TestASubscriptionOfAnotherDatabaseIsNotMistakenForOurs(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgresWith(t, "-c wal_level=logical")
	conn := connect(t, dsn)

	for _, stmt := range []string{`CREATE DATABASE d1`, `CREATE DATABASE d2`} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	name := SubscriptionName(5, 1, 0)
	inDB := func(database string) string {
		t.Helper()
		ci, err := ConnInfo(dsn, database)
		if err != nil {
			t.Fatal(err)
		}
		return ci
	}

	d1 := connect(t, inDB("d1"))
	if _, err := d1.Exec(ctx, `CREATE PUBLICATION p FOR ALL TABLES`); err != nil {
		t.Fatal(err)
	}
	// connect=false so no slot is made: this is about catalogue visibility.
	if _, err := d1.Exec(ctx, `CREATE SUBSCRIPTION `+QuoteIdent(name)+
		` CONNECTION 'host=/tmp user=postgres dbname=d1' PUBLICATION p WITH (connect = false, slot_name = NONE)`); err != nil {
		t.Fatal(err)
	}

	d2 := connect(t, inDB("d2"))
	// The shared catalogue really does show it from the other database, or
	// the scoping below would be proving nothing.
	var unscoped int
	if err := d2.QueryRow(ctx, `SELECT count(*) FROM pg_subscription WHERE subname = $1`, name).Scan(&unscoped); err != nil {
		t.Fatal(err)
	}
	if unscoped != 1 {
		t.Fatalf("pg_subscription is meant to be shared; d2 sees %d rows for %s", unscoped, name)
	}

	var scoped int
	if err := d2.QueryRow(ctx, `SELECT count(*) FROM pg_subscription
		 WHERE subname = $1 AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())`,
		name).Scan(&scoped); err != nil {
		t.Fatal(err)
	}
	if scoped != 0 {
		t.Fatalf("d2 counted %d subscriptions of its own named %s; it has none", scoped, name)
	}
}

// TestDroppingSubscriptionsOnlyTouchesThisDatabase: the same shared-catalogue
// trap as above, at the other end of the workflow. dropSubscriptionsLike
// listed every database's identically named subscriptions, and a subscription
// can only be dropped from the database it belongs to -- so the second row
// failed 42704. Complete and Unwind retry every pass, so on a cluster with
// two user databases they never finished: the reverse subscriptions were
// never released and the retired shard set was never freed.
func TestDroppingSubscriptionsOnlyTouchesThisDatabase(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgresWith(t, "-c wal_level=logical")
	conn := connect(t, dsn)
	for _, stmt := range []string{`CREATE DATABASE d1`, `CREATE DATABASE d2`} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	inDB := func(database string) string {
		t.Helper()
		ci, err := ConnInfo(dsn, database)
		if err != nil {
			t.Fatal(err)
		}
		return ci
	}
	name := SubscriptionName(5, 1, 0)
	make := func(c *pgx.Conn, db string) {
		t.Helper()
		if _, err := c.Exec(ctx, `CREATE PUBLICATION p FOR ALL TABLES`); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, `CREATE SUBSCRIPTION `+QuoteIdent(name)+
			` CONNECTION 'host=/tmp user=postgres dbname=`+db+`' PUBLICATION p WITH (connect = false, slot_name = NONE)`); err != nil {
			t.Fatal(err)
		}
	}
	d1 := connect(t, inDB("d1"))
	make(d1, "d1")
	d2 := connect(t, inDB("d2"))
	make(d2, "d2")

	// The drop runs from d2, exactly as a Complete or Unwind pass does once
	// it has dialled that database.
	if err := dropSubscriptionsLike(ctx, pgxShardConn{d2}, strings.ReplaceAll(name, "_", "\\_")); err != nil {
		t.Fatalf("dropping d2's own subscriptions failed because it reached for d1's: %v", err)
	}

	count := func(c *pgx.Conn) int {
		t.Helper()
		var n int
		if err := c.QueryRow(ctx, `SELECT count(*) FROM pg_subscription WHERE subname = $1
			 AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())`, name).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count(d2); n != 0 {
		t.Fatalf("d2 still has %d subscriptions named %s", n, name)
	}
	if n := count(d1); n != 1 {
		t.Fatalf("d1 has %d subscriptions named %s: the drop reached into another database", n, name)
	}
}

// A replication slot is CLUSTER-WIDE even though the subscription is not, so
// two databases resharding at once must not ask a source for the same slot.
func TestASlotNameIsPerDatabase(t *testing.T) {
	seen := map[string]string{}
	for _, db := range []string{"app", "reports", "app2", ""} {
		for _, n := range []string{
			SlotName(5, 1, 0, db),
			ReverseSlotName(5, 0, 1, db),
		} {
			if len(n) > 63 {
				t.Errorf("%q is %d characters; PostgreSQL caps an identifier at 63", n, len(n))
			}
			if prev, ok := seen[n]; ok {
				t.Errorf("%q is the slot name of both %s and %s", n, prev, db)
			}
			seen[n] = db
		}
	}
	// The subscription name is unchanged, so an in-flight workflow's
	// existing subscriptions are still recognised after an upgrade and are
	// not created a second time.
	if !strings.HasPrefix(SlotName(5, 1, 0, "app"), SubscriptionName(5, 1, 0)+"_d") {
		t.Fatalf("the slot name must extend the subscription name, got %s", SlotName(5, 1, 0, "app"))
	}
}
