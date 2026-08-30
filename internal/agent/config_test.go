package agent

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/agent/backup"
	"github.com/andrew01234567890/pgshard/internal/catalog"
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

func TestRenderPostgresqlConfBackupGolden(t *testing.T) {
	c := testConfig()
	c.Role = RolePrimary
	c.Backup = &backup.Settings{Stanza: "demo-s0-pg18", Repo: backup.Repo{Type: backup.TypePosix}}
	got := RenderPostgresqlConf(c, false)
	golden(t, "postgresql.primary-backup.conf", got)
	for _, want := range []string{
		"archive_mode = on\n",
		"archive_command = 'pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=demo-s0-pg18 archive-push %p'\n",
		"restore_command = 'pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=demo-s0-pg18 archive-get %f \"%p\"'\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q", want)
		}
	}
	c.Postgres.RestoreCommand = "custom"
	if !strings.Contains(RenderPostgresqlConf(c, false), "restore_command = 'custom'\n") {
		t.Error("explicit restore_command must win over the pgbackrest one")
	}
	c.NonServing = true
	if !strings.Contains(RenderPostgresqlConf(c, false), "archive_mode = off\n") {
		t.Error("a non-serving reshard target must not archive even with a backup policy")
	}
	c.NonServing = false
	c.Backup = nil
	if !strings.Contains(RenderPostgresqlConf(c, false), "archive_mode = off\n") {
		t.Error("archive_mode must be off without a backup policy")
	}
	for _, name := range []string{"archive_mode", "archive_command", "restore_command"} {
		if !slices.Contains(OwnedSettings(), name) {
			t.Errorf("%s must be agent-owned", name)
		}
	}
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

// TestLogicalWorkersExceedSubscriptions: every subscription holds an apply
// worker for its whole life and table sync draws from the same pool, so a
// pool no larger than the subscription count means no table ever finishes
// its initial copy. A reshard merge is where that bites -- each target
// subscribes to every source -- and it stalled at "5/8 tables ready, lag 0
// bytes" with no error until this was sized.
//
// These live on the agent rather than in pgtune deliberately: pgtune refuses
// to derive anything below a 512MB budget, so on a small cluster the tuned
// values never arrive and PostgreSQL's default of 4 would stand.
func TestLogicalWorkersExceedSubscriptions(t *testing.T) {
	c := testConfig()
	if c.Postgres.MaxLogicalReplicationWorkers <= c.Postgres.MaxReplicationSlots {
		t.Errorf("max_logical_replication_workers=%d is not above max_replication_slots=%d, so a group holding that many subscriptions has nothing left for table sync",
			c.Postgres.MaxLogicalReplicationWorkers, c.Postgres.MaxReplicationSlots)
	}
	if c.Postgres.MaxWorkerProcesses < c.Postgres.MaxLogicalReplicationWorkers {
		t.Errorf("max_worker_processes=%d is below max_logical_replication_workers=%d; PostgreSQL would silently start fewer",
			c.Postgres.MaxWorkerProcesses, c.Postgres.MaxLogicalReplicationWorkers)
	}
	if c.Postgres.MaxSyncWorkersPerSubscription < 1 {
		t.Error("max_sync_workers_per_subscription must allow at least one table sync")
	}
	conf := RenderPostgresqlConf(c, false)
	for _, want := range []string{"max_logical_replication_workers", "max_sync_workers_per_subscription", "max_worker_processes"} {
		if !strings.Contains(conf, want) {
			t.Errorf("%s is absent from postgresql.conf, so it would fall back to the PostgreSQL default", want)
		}
	}
}

// TestPgHBARefusesApplicationRolesOverTCP: application roles are
// materialised with their verifiers on every group, so a client that could
// reach a member could connect straight to a shard of its choosing and
// write to it -- past the router, and so past shard-key routing, the write
// fences a cutover raises, and the coordination that makes a multi-shard
// write atomic. SCRAM proved who the client was; nothing checked it had
// come the right way.
//
// TCP is the control plane's path: replicas, the controller and the router
// all arrive as the superuser, and the pooler an application talks through
// connects over the unix socket.
func TestPgHBARefusesApplicationRolesOverTCP(t *testing.T) {
	for _, tls := range []bool{false, true} {
		c := testConfig()
		if tls {
			c.TLS = TLSFiles{CertFile: "/certs/tls.crt", KeyFile: "/certs/tls.key", CAFile: "/certs/ca.crt"}
		}
		// The control plane's own roles, and nothing else. The superuser is
		// how the operator, the controller and a replica reach a member;
		// the router's catalog role is how the router reaches the catalog,
		// and it exists only where the catalog schema does, so on a shard
		// this line matches no role at all.
		controlPlane := map[string]bool{"postgres": true, catalog.RouterRole: true}
		var host, superuser, reject int
		for _, line := range strings.Split(RenderPgHBAConf(c), "\n") {
			f := strings.Fields(line)
			if len(f) < 4 || strings.HasPrefix(line, "#") || f[0] == "local" {
				continue
			}
			host++
			role, method := f[2], f[len(f)-1]
			switch {
			case method == "reject":
				reject++
			case controlPlane[role]:
				superuser++
			default:
				t.Errorf("tls=%v: %q lets role %q authenticate over TCP", tls, line, role)
			}
		}
		if superuser == 0 || reject == 0 || host != superuser+reject {
			t.Errorf("tls=%v: %d TCP lines, %d control plane, %d reject", tls, host, superuser, reject)
		}
		// An application role is still refused, which is the point of all
		// of this: it must reach a shard through the pooler and the router,
		// where shard-key routing, the write fences and the coordination of
		// a multi-shard write happen.
		if strings.Contains(RenderPgHBAConf(c), " app ") {
			t.Errorf("tls=%v: an application role reached the TCP rules", tls)
		}
		// The socket stays open to everything: that is how the pooler, and
		// so every application, actually reaches this instance.
		if !strings.Contains(RenderPgHBAConf(c), "local   all             all") {
			t.Errorf("tls=%v: local connections must still admit every role", tls)
		}
	}
}

// TestParameterNameCannotInjectASetting: the value of a parameter is
// quoted but its name is written as it stands, so a name carrying a
// newline used to write a second setting of its own -- and every guard on
// the CRD names a setting, so anything they refuse could be smuggled in
// behind a key they allow.
func TestParameterNameCannotInjectASetting(t *testing.T) {
	c := testConfig()
	c.Postgres.Parameters = map[string]string{
		"work_mem = '4MB'\nssl": "off",
		"fine_setting":          "1",
		"also.fine":             "2",
		"has space":             "3",
		"has=equals":            "4",
		"has'quote":             "5",
		"":                      "6",
	}
	conf := renderPostgresqlConf(c, false, false)

	if n := strings.Count(conf, "\nssl = "); n != 1 {
		t.Errorf("postgresql.conf has %d ssl lines, want the one pgshard writes:\n%s", n, conf)
	}
	if strings.Contains(conf, "ssl = 'off'") {
		t.Error("an injected key turned TLS off")
	}
	for _, want := range []string{"fine_setting = '1'", "also.fine = '2'"} {
		if !strings.Contains(conf, want) {
			t.Errorf("a legitimate parameter was dropped: %q missing", want)
		}
	}
	for _, bad := range []string{"has space", "has=equals", "has'quote"} {
		if strings.Contains(conf, bad) {
			t.Errorf("a key that is not a setting name reached the file: %q", bad)
		}
	}
	// Every line the file carries has to be one setting.
	for _, line := range strings.Split(conf, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.Count(line, " = ") != 1 {
			t.Errorf("line %q is not a single setting", line)
		}
	}
}
