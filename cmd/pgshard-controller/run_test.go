package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/cli"
	"github.com/andrew01234567890/pgshard/internal/controller"
)

func TestRunUsageErrors(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{}, "--catalog-dsn is required"},
		{[]string{"--catalog-dsn", "postgres://x", "extra"}, "unexpected argument"},
		{[]string{"--catalog-dsn", "postgres://x"}, "--tls-cert, --tls-key and --tls-ca are required"},
		{[]string{"--catalog-dsn", "postgres://x", "--insecure-dev", "--tls-ca", "x"}, "cannot be combined"},
	}
	for _, c := range cases {
		var out, errb bytes.Buffer
		if code := runController(context.Background(), c.args, &out, &errb); code != cli.ExitUsage {
			t.Fatalf("%v: code %d, stderr %s", c.args, code, errb.String())
		}
		if !strings.Contains(errb.String(), c.want) {
			t.Fatalf("%v: stderr %q", c.args, errb.String())
		}
	}
}

func TestRunUnreachableCatalogIsNotReady(t *testing.T) {
	var out, errb bytes.Buffer
	code := runController(context.Background(), []string{"--catalog-dsn", "postgres://postgres@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1", "--listen", ""}, &out, &errb)
	if code != cli.ExitNotReady {
		t.Fatalf("code %d, stderr %s", code, errb.String())
	}
}

func TestShardConnInfo(t *testing.T) {
	ctx := context.Background()
	ref := controller.ShardRef{Set: "default", ID: 1}
	explicit := shardDSNFlag{ref: "postgres://postgres:p%27w@10.0.0.5:5433/postgres?sslmode=disable"}
	got, err := shardConnInfo(ctx, nil, explicit, "", ref, "app")
	if err != nil || got != "host='10.0.0.5' port=5433 user='postgres' dbname='app' password='p\\'w' sslmode=disable" {
		t.Fatalf("explicit DSN: %q %v", got, err)
	}
	if _, err := shardConnInfo(ctx, nil, nil, "", ref, "app"); err == nil {
		t.Fatal("no DSN and no template must fail")
	}
	got, err = controller.ConnInfo("host=h port=5432 user=u sslmode=disable", "db")
	if err != nil || got != "host='h' port=5432 user='u' dbname='db' sslmode=disable" {
		t.Fatalf("ConnInfo: %q %v", got, err)
	}
	if _, err := controller.ConnInfo("postgres://[bad", "db"); err == nil {
		t.Fatal("unparsable DSN must fail")
	}
}

// TestReshardSlotsFailOverByDefault: a reshard's subscription slots live on
// the source primary and a reshard outlives promotions. Copier.SlotFailover
// was threaded to both subscription paths and never set by the production
// constructor, so every reshard slot was created without failover = true --
// a failover mid-copy left the subscription pointing at a slot the new
// primary does not have.
//
// This checks only that the flag is registered and defaults on, not that
// its value reaches the Copier: runController builds the Copier inline.
// What stops the regression recurring is not this test but the field it
// sets -- SlotFailoverDisabled, whose zero value is the safe one, so a
// Copier built without it still asks for failover slots.
func TestReshardSlotsFailOverByDefault(t *testing.T) {
	var out, errb bytes.Buffer
	if code := runController(context.Background(), []string{"--help"}, &out, &errb); code == 0 {
		t.Log("help returned 0")
	}
	usage := out.String() + errb.String()
	if !strings.Contains(usage, "copy-slot-failover") {
		t.Fatalf("the reshard slot failover flag is not offered: %s", usage)
	}
	if !strings.Contains(usage, "(default true)") {
		t.Errorf("copy-slot-failover must default on; usage: %s", usage)
	}
}

// TestTheControllerTakesTheClustersAgentToken: the controller derived its
// agent credential from the catalog password alone, so anything holding
// the superuser password held one that unlocks Promote, Demote, Rewind and
// Reclone (PGS-428). The operator now mounts the cluster's own token and
// passes it here; the flag has to exist for that wiring to mean anything.
func TestTheControllerTakesTheClustersAgentToken(t *testing.T) {
	var out, errb bytes.Buffer
	runController(context.Background(), []string{"--help"}, &out, &errb)
	usage := out.String() + errb.String()
	if !strings.Contains(usage, "agent-token-file") {
		t.Fatalf("the controller offers no way to be given the cluster's agent token: %s", usage)
	}
}

// An unreadable token file fails at startup rather than at the first
// materialization: a controller that cannot present its credential should
// say so once, not once per reshard.
func TestAnUnreadableAgentTokenFileFailsAtStartup(t *testing.T) {
	var out, errb bytes.Buffer
	code := runController(context.Background(), []string{
		"--catalog-dsn", "postgres://nobody@127.0.0.1:1/none",
		"--agent-token-file", "/nonexistent/agent/token",
		"--listen", "",
	}, &out, &errb)
	if code == 0 {
		t.Fatal("a controller that cannot read its agent token must not start")
	}
	if !strings.Contains(errb.String()+out.String(), "agent token file") {
		t.Errorf("startup failure does not name the token file: %s", errb.String()+out.String())
	}
}

// TestTheCutoverFlagsTheDocsPromiseExist: docs/resharding.md tells an
// operator that a switch which has not reached the journal within
// --cutover-timeout is undone and retried, and that --cutover-attempts
// undone attempts fail the workflow. Neither flag existed, so both values
// were pinned at their defaults and the documented commands failed with
// "flag provided but not defined". The fence deadline matters more since
// it became what aborts a slow verify (PGS-605), so it has to be tunable.
func TestTheCutoverFlagsTheDocsPromiseExist(t *testing.T) {
	var out, errb bytes.Buffer
	runController(context.Background(), []string{"--help"}, &out, &errb)
	usage := out.String() + errb.String()
	for _, flag := range []string{"cutover-timeout", "cutover-attempts"} {
		if !strings.Contains(usage, flag) {
			t.Errorf("docs/resharding.md documents --%s and the controller does not offer it", flag)
		}
	}
	if !strings.Contains(usage, "1m0s") {
		t.Errorf("--cutover-timeout should default to the documented 60s; usage: %s", usage)
	}
}
