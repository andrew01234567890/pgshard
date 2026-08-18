package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/andrew01234567890/pgshard/internal/cli"
	"github.com/andrew01234567890/pgshard/internal/pgtune"
)

func tune(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pgshard-operator tune", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cpu := fs.String("cpu", "", "CPU limit (Kubernetes quantity, e.g. 4 or 500m)")
	memory := fs.String("memory", "", "memory limit (Kubernetes quantity, e.g. 16Gi)")
	disk := fs.String("disk", "", "PGDATA volume size (Kubernetes quantity, optional)")
	profile := fs.String("profile", "oltp", "workload profile: oltp|mixed|analytics")
	storage := fs.String("storage", "unknown", "storage class hint: ssd|hdd|unknown")
	backends := fs.Int("max-backends", 100, "pooler server-connection budget across roles")
	slots := fs.Int("logical-slots", 0, "expected logical replication slots")
	replicas := fs.Int("replicas", 2, "streaming replicas per shard")
	major := fs.Int("major", 18, "PostgreSQL major version: 18|19")
	asJSON := fs.Bool("json", false, "print the DerivedSetting JSON list instead of conf text")
	if err := fs.Parse(args); err != nil {
		return cli.ExitUsage
	}
	if *cpu == "" || *memory == "" {
		fmt.Fprintln(stderr, "pgshard-operator tune: --cpu and --memory are required")
		return cli.ExitUsage
	}
	in := pgtune.Input{
		Major:        *major,
		Profile:      pgtune.Profile(*profile),
		Storage:      pgtune.Storage(*storage),
		MaxBackends:  *backends,
		LogicalSlots: *slots,
		Replicas:     *replicas,
	}
	var err error
	if in.CPUMillicores, err = quantity(*cpu, true); err != nil {
		fmt.Fprintf(stderr, "pgshard-operator tune: --cpu: %v\n", err)
		return cli.ExitUsage
	}
	if in.MemoryBytes, err = quantity(*memory, false); err != nil {
		fmt.Fprintf(stderr, "pgshard-operator tune: --memory: %v\n", err)
		return cli.ExitUsage
	}
	if *disk != "" {
		if in.DiskBytes, err = quantity(*disk, false); err != nil {
			fmt.Fprintf(stderr, "pgshard-operator tune: --disk: %v\n", err)
			return cli.ExitUsage
		}
	}
	settings, err := pgtune.Derive(in)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-operator tune: %v\n", err)
		return cli.ExitNotReady
	}
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(settings.Derived()); err != nil {
			fmt.Fprintf(stderr, "pgshard-operator tune: %v\n", err)
			return cli.ExitNotReady
		}
		return cli.ExitOK
	}
	fmt.Fprint(stdout, settings.Render())
	return cli.ExitOK
}

func quantity(s string, milli bool) (int64, error) {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, err
	}
	if milli {
		return q.MilliValue(), nil
	}
	return q.Value(), nil
}
