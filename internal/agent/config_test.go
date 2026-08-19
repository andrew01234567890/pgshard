package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testConfig() *Config {
	c := &Config{
		Cluster: "c1", Shard: "s0", Member: "s0-1", Role: RoleStandby,
		PGData: "/var/lib/postgresql/data", PasswordFile: "/etc/pgshard/pw",
		PrimaryConninfo: "host=s0-0 port=5432 user=postgres dbname=template1 application_name=wrong",
		PodCIDR:         "10.244.0.0/16",
		Postgres:        PostgresSettings{MaxPreparedTransactions: 200, SynchronousStandbyNames: "ANY 1 (s0-1, s0-2)"},
	}
	c.applyDefaults()
	return c
}

func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Fatalf("%s mismatch\n--- want\n%s--- got\n%s", name, want, got)
	}
}

func TestRenderPostgresqlConfStandbyGolden(t *testing.T) {
	golden(t, "postgresql.standby.conf", RenderPostgresqlConf(testConfig(), true))
}

func TestRenderPostgresqlConfPrimaryTLSGolden(t *testing.T) {
	c := testConfig()
	c.Role = RolePrimary
	c.TLS = TLSFiles{CertFile: "/certs/tls.crt", KeyFile: "/certs/tls.key", CAFile: "/certs/ca.crt"}
	golden(t, "postgresql.primary-tls.conf", RenderPostgresqlConf(c, false))
	golden(t, "pg_hba.tls.conf", RenderPgHBAConf(c))
}

func TestRenderPgHBAConfGolden(t *testing.T) {
	golden(t, "pg_hba.conf", RenderPgHBAConf(testConfig()))
}

func TestPrimaryConninfoForcesDbnameAndApplicationName(t *testing.T) {
	got := PrimaryConninfo(testConfig())
	want := "host=s0-0 port=5432 user=postgres dbname=postgres application_name=s0-1"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestRenderPostgresqlConfFixedSettings(t *testing.T) {
	out := RenderPostgresqlConf(testConfig(), false)
	for _, want := range []string{
		"wal_level = logical\n", "wal_log_hints = on\n", "restart_after_crash = off\n",
		"hot_standby_feedback = off\n", "track_commit_timestamp = on\n", "ssl = off\n",
		"max_prepared_transactions = 200\n", "wal_keep_size = '512MB'\n", "synchronous_standby_names = 'ANY 1 (s0-1, s0-2)'\n",
		"include_if_exists = 'pgshard.override.conf'\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Contains(out, "primary_conninfo") {
		t.Error("primary settings rendered for a primary")
	}
}

func TestQuoteEscapesSingleQuotes(t *testing.T) {
	if got := quote("it's"); got != "'it''s'" {
		t.Fatalf("got %s", got)
	}
}

func TestLoadConfigDefaultsAndValidation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	write := func(body string) {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"cluster":"c","shard":"s","member":"m","role":"primary","pgdata":"/d","passwordFile":"/p","lease":{"enabled":false},"shutdownTimeout":"7s"}`)
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.PodName != "m" || c.Port != 5432 || c.HTTPAddr != ":8080" || c.GRPCAddr != ":9090" ||
		time.Duration(c.ShutdownTimeout) != 7*time.Second || time.Duration(c.Lease.Duration) != 15*time.Second ||
		c.SlotName() != "pgshard_m" || (&Config{Member: "S0-1.x"}).SlotName() != "pgshard_s0_1_x" || c.LeaseName() != "c-s-primary" {
		t.Fatalf("defaults wrong: %+v", c)
	}
	cases := map[string]string{
		"standby without source": `{"cluster":"c","shard":"s","member":"m","role":"standby","pgdata":"/d","passwordFile":"/p"}`,
		"bad role":               `{"cluster":"c","shard":"s","member":"m","role":"leader","pgdata":"/d","passwordFile":"/p"}`,
		"lease without ns":       `{"cluster":"c","shard":"s","member":"m","role":"primary","pgdata":"/d","passwordFile":"/p","lease":{"enabled":true}}`,
		"half tls":               `{"cluster":"c","shard":"s","member":"m","role":"primary","pgdata":"/d","passwordFile":"/p","tls":{"certFile":"/c"}}`,
		"missing member":         `{"cluster":"c","shard":"s","role":"primary","pgdata":"/d","passwordFile":"/p"}`,
		"bad duration":           `{"cluster":"c","shard":"s","member":"m","role":"primary","pgdata":"/d","passwordFile":"/p","shutdownTimeout":"soon"}`,
	}
	for name, body := range cases {
		write(body)
		if _, err := LoadConfig(path); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestWriteConfigRendersFilesAndResetsAutoConf(t *testing.T) {
	c := testConfig()
	c.PGData = t.TempDir()
	if err := os.WriteFile(filepath.Join(c.PGData, autoConf), []byte("primary_conninfo = 'stale'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteConfig(c, true); err != nil {
		t.Fatal(err)
	}
	pg, _ := os.ReadFile(filepath.Join(c.PGData, postgresqlConf))
	hba, _ := os.ReadFile(filepath.Join(c.PGData, pgHBAConf))
	auto, _ := os.ReadFile(filepath.Join(c.PGData, autoConf))
	if string(pg) != RenderPostgresqlConf(c, true) || string(hba) != RenderPgHBAConf(c) {
		t.Fatal("rendered files differ from renderer output")
	}
	if strings.Contains(string(auto), "stale") {
		t.Fatalf("auto.conf not reset: %q", auto)
	}
}

func TestRestoreCommandRenderedOnlyWhenSet(t *testing.T) {
	c := testConfig()
	if strings.Contains(RenderPostgresqlConf(c, true), "restore_command") {
		t.Fatal("restore_command rendered without a value")
	}
	c.Postgres.RestoreCommand = "cp /archive/%f %p"
	if !strings.Contains(RenderPostgresqlConf(c, true), "restore_command = 'cp /archive/%f %p'\n") {
		t.Fatal("restore_command missing")
	}
}

func TestRenderPostgresqlConfAppendsUserParametersWithoutOverridingOwnedOnes(t *testing.T) {
	c := testConfig()
	c.Postgres.Parameters = map[string]string{"work_mem": "8MB", "port": "1", "wal_level": "minimal"}
	got := RenderPostgresqlConf(c, false)
	if !strings.Contains(got, "work_mem = '8MB'\n") {
		t.Fatalf("user parameter missing:\n%s", got)
	}
	if !strings.Contains(got, "port = 5432\n") || !strings.Contains(got, "wal_level = logical\n") {
		t.Fatalf("owned settings must win:\n%s", got)
	}
}

func TestWriteConfigCopiesTheOverrideFileIntoPGDATA(t *testing.T) {
	c := testConfig()
	c.PGData = t.TempDir()
	dir := t.TempDir()
	c.OverrideFile = filepath.Join(dir, "pgshard.override.conf")
	if err := WriteConfig(c, true); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(filepath.Join(c.PGData, overrideConf)); err != nil || len(body) != 0 {
		t.Fatalf("a missing override file writes an empty include target: %q %v", body, err)
	}
	if err := os.WriteFile(c.OverrideFile, []byte("shared_buffers = '1GB'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteConfig(c, true); err != nil {
		t.Fatal(err)
	}
	if body, _ := os.ReadFile(filepath.Join(c.PGData, overrideConf)); string(body) != "shared_buffers = '1GB'\n" {
		t.Fatalf("override not copied: %q", body)
	}
	c.OverrideFile = ""
	if err := os.Remove(filepath.Join(c.PGData, overrideConf)); err != nil {
		t.Fatal(err)
	}
	if err := WriteConfig(c, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(c.PGData, overrideConf)); !os.IsNotExist(err) {
		t.Fatal("no override file configured: PGDATA is left alone")
	}
}

func TestRefreshTakesOverParametersOverrideAndSettingsHash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "member.json")
	write := func(body string) {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"cluster":"c","shard":"s","member":"m","role":"primary","pgdata":"/d","passwordFile":"/p","port":5433,"postgres":{"parameters":{"work_mem":"4MB"}},"settingsHash":"a"}`)
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	write(`{"cluster":"c","shard":"s","member":"m","role":"standby","primaryConninfo":"host=x","pgdata":"/other","passwordFile":"/p","port":9999,"postgres":{"parameters":{"work_mem":"8MB"}},"overrideFile":"/etc/o.conf","settingsHash":"b"}`)
	if err := c.Refresh(); err != nil {
		t.Fatal(err)
	}
	if c.Postgres.Parameters["work_mem"] != "8MB" || c.OverrideFile != "/etc/o.conf" || c.SettingsHash != "b" {
		t.Fatalf("refresh must take the reloadable fields: %+v", c)
	}
	if c.Role != RolePrimary || c.PGData != "/d" || c.Port != 5433 {
		t.Fatalf("refresh must leave identity and paths alone: %+v", c)
	}
	write(`{not json`)
	if err := c.Refresh(); err == nil {
		t.Fatal("a broken file must fail the refresh, not blank the settings")
	}
	if (&Config{}).Refresh() != nil {
		t.Fatal("a config not loaded from a file has nothing to refresh")
	}
}
