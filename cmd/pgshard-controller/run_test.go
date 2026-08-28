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
