package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/cli"
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
