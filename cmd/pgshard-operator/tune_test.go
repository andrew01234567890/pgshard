package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func TestTunePrintsConf(t *testing.T) {
	var out, errb bytes.Buffer
	code := tune([]string{"--cpu", "4", "--memory", "16Gi", "--profile", "oltp", "--storage", "ssd"}, &out, &errb)
	if code != 0 || errb.Len() != 0 {
		t.Fatalf("code=%d err=%s", code, errb.String())
	}
	for _, want := range []string{"shared_buffers = '4GB'", "wal_level = logical", "random_page_cost = 1.1", "\t# "} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
}

func TestTunePrintsJSON(t *testing.T) {
	var out, errb bytes.Buffer
	if code := tune([]string{"--cpu", "500m", "--memory", "2Gi", "--major", "19", "--json"}, &out, &errb); code != 0 {
		t.Fatalf("code=%d err=%s", code, errb.String())
	}
	var d []pgshardv1alpha1.DerivedSetting
	if err := json.Unmarshal(out.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	names := map[string]string{}
	for _, x := range d {
		names[x.Name] = x.Value
	}
	if names["io_max_workers"] != "3" || names["shared_buffers"] != "512MB" {
		t.Fatalf("got %v", names)
	}
}

func TestTuneErrors(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{}, 2},
		{[]string{"--cpu", "x", "--memory", "1Gi"}, 2},
		{[]string{"--cpu", "1", "--memory", "1Gi", "--disk", "big"}, 2},
		{[]string{"--cpu", "1", "--memory", "1Gi", "--max-backends", "5000"}, 1},
		{[]string{"--bogus"}, 2},
	} {
		var out, errb bytes.Buffer
		if code := tune(tc.args, &out, &errb); code != tc.code || errb.Len() == 0 {
			t.Errorf("%v: code=%d err=%q", tc.args, code, errb.String())
		}
	}
}
