package main

import (
	"flag"
	"testing"

	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
)

func TestCommandFlagsKeepAdmissionSafeByDefault(t *testing.T) {
	t.Parallel()
	flags := flag.NewFlagSet("pgshard-operator", flag.ContinueOnError)
	options := bindCommandFlags(flags)
	if !options.webhookEnabled || !options.leaderElection || !options.secureMetrics {
		t.Fatalf("unsafe command defaults = %#v", options)
	}
	if options.images != owned.DefaultImages() {
		t.Fatalf("default images = %#v", options.images)
	}
}

func TestCommandFlagsAllowExplicitCertificateFreeDevelopmentMode(t *testing.T) {
	t.Parallel()
	flags := flag.NewFlagSet("pgshard-operator", flag.ContinueOnError)
	options := bindCommandFlags(flags)
	if err := flags.Parse([]string{
		"--webhook-enabled=false",
		"--metrics-bind-address=0",
		"--orchestrator-image=pgshard/orchestrator:dev",
		"--pooler-image=pgshard/pooler:dev",
	}); err != nil {
		t.Fatal(err)
	}
	if options.webhookEnabled || options.metricsAddress != "0" {
		t.Fatalf("development options = %#v", options)
	}
	if options.images.Orchestrator != "pgshard/orchestrator:dev" || options.images.Pooler != "pgshard/pooler:dev" {
		t.Fatalf("development images = %#v", options.images)
	}
}
