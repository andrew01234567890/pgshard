package main

import (
	"strings"
	"testing"
)

const sample = `frontend,workload,clients,mode,duration_s,tps,lat_avg_ms,p50_ms,p99_ms,p999_ms,fe_cpu_pct,fe_cpu_ms_per_txn,fe_rss_mib,txns
direct,select,10,simple,3,10000,1.0,0.9,2.0,3.0,NA,NA,NA,30000
pgbouncer-txn,select,10,simple,3,8000,1.2,1.1,2.5,4.0,50,0.0625,12.5,24000
pgshard,select,10,simple,3,4000,2.5,2.3,5.0,8.0,120,0.3,90.0,12000
pgshard,tpcb,1000,prepared,3,ERROR,NA,NA,NA,NA,NA,NA,NA,NA
`

func TestParse(t *testing.T) {
	rows, err := Parse(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("rows = %d, want 4", len(rows))
	}
	if rows[0].Frontend != "direct" || rows[0].TPS != 10000 || rows[0].Clients != 10 {
		t.Fatalf("direct row parsed wrong: %+v", rows[0])
	}
	if rows[2].CPUPerTxn != 0.3 || rows[2].RSS != 90 {
		t.Fatalf("pgshard row parsed wrong: %+v", rows[2])
	}
	if !rows[3].Err {
		t.Fatalf("ERROR row not flagged: %+v", rows[3])
	}
}

func TestParseMissingColumn(t *testing.T) {
	if _, err := Parse(strings.NewReader("a,b\n1,2\n")); err == nil {
		t.Fatal("want error for missing columns")
	}
}

func TestTableDeltas(t *testing.T) {
	rows, err := Parse(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	table := Table(rows)
	for _, want := range []string{"+0.0%", "-20.0%", "-60.0%", "ERROR", "vs-direct"} {
		if !strings.Contains(table, want) {
			t.Errorf("table missing %q:\n%s", want, table)
		}
	}
	lines := strings.Split(strings.TrimSpace(table), "\n")
	if len(lines) != 5 {
		t.Fatalf("table lines = %d, want 5:\n%s", len(lines), table)
	}
}
