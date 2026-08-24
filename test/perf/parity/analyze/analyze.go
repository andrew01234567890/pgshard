package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

type Row struct {
	Frontend, Workload, Mode         string
	Clients                          int
	TPS, LatAvg, P99, CPUPerTxn, RSS float64
	Err                              bool
}

func Parse(r io.Reader) ([]Row, error) {
	cr := csv.NewReader(r)
	recs, err := cr.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(recs) < 1 {
		return nil, fmt.Errorf("empty csv")
	}
	idx := map[string]int{}
	for i, h := range recs[0] {
		idx[h] = i
	}
	for _, want := range []string{"frontend", "workload", "clients", "mode", "tps"} {
		if _, ok := idx[want]; !ok {
			return nil, fmt.Errorf("missing column %q", want)
		}
	}
	num := func(rec []string, col string) float64 {
		i, ok := idx[col]
		if !ok || i >= len(rec) {
			return 0
		}
		v, err := strconv.ParseFloat(rec[i], 64)
		if err != nil {
			return 0
		}
		return v
	}
	var rows []Row
	for _, rec := range recs[1:] {
		clients, _ := strconv.Atoi(rec[idx["clients"]])
		rows = append(rows, Row{
			Frontend:  rec[idx["frontend"]],
			Workload:  rec[idx["workload"]],
			Mode:      rec[idx["mode"]],
			Clients:   clients,
			TPS:       num(rec, "tps"),
			LatAvg:    num(rec, "lat_avg_ms"),
			P99:       num(rec, "p99_ms"),
			CPUPerTxn: num(rec, "fe_cpu_ms_per_txn"),
			RSS:       num(rec, "fe_rss_mib"),
			Err:       rec[idx["tps"]] == "ERROR",
		})
	}
	return rows, nil
}

type key struct {
	Workload, Mode string
	Clients        int
}

// Table renders one line per (workload, mode, clients, frontend) with the
// TPS delta versus the direct arm of the same scenario.
func Table(rows []Row) string {
	direct := map[key]float64{}
	scenarios := map[key][]Row{}
	for _, r := range rows {
		k := key{r.Workload, r.Mode, r.Clients}
		scenarios[k] = append(scenarios[k], r)
		if r.Frontend == "direct" && !r.Err {
			direct[k] = r.TPS
		}
	}
	keys := make([]key, 0, len(scenarios))
	for k := range scenarios {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.Workload != b.Workload {
			return a.Workload < b.Workload
		}
		if a.Mode != b.Mode {
			return a.Mode < b.Mode
		}
		return a.Clients < b.Clients
	})
	var b strings.Builder
	fmt.Fprintf(&b, "%-8s %-8s %7s  %-18s %12s %9s %9s %9s %10s %9s\n",
		"workload", "mode", "clients", "frontend", "tps", "vs-direct", "lat(ms)", "p99(ms)", "cpu-ms/tx", "rss(MiB)")
	for _, k := range keys {
		for _, r := range scenarios[k] {
			delta := "n/a"
			if d, ok := direct[k]; ok && d > 0 && !r.Err {
				delta = fmt.Sprintf("%+.1f%%", (r.TPS-d)/d*100)
			}
			tps := "ERROR"
			if !r.Err {
				tps = fmt.Sprintf("%.0f", r.TPS)
			}
			fmt.Fprintf(&b, "%-8s %-8s %7d  %-18s %12s %9s %9.2f %9.2f %10.3f %9.1f\n",
				k.Workload, k.Mode, k.Clients, r.Frontend, tps, delta, r.LatAvg, r.P99, r.CPUPerTxn, r.RSS)
		}
	}
	return b.String()
}
