package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/agent/backup"
)

func repoSettings() *backup.Settings {
	return &backup.Settings{Stanza: "new-s0-pg18", Repo: backup.Repo{Type: backup.TypePosix, Path: "/repo"}}
}

func tempRepoSettings(t *testing.T) *backup.Settings {
	t.Helper()
	dir := t.TempDir()
	s := repoSettings()
	s.ConfigPath = filepath.Join(dir, "pgbackrest.conf")
	s.SpoolPath = filepath.Join(dir, "spool")
	s.LogPath = filepath.Join(dir, "log")
	return s
}

func TestConfigValidateRestore(t *testing.T) {
	c := testConfig()
	c.Role = RolePrimary
	c.Restore = &backup.RestoreOptions{Stanza: "old-s0-pg18", Type: backup.TargetName, Target: "rp", BackupID: "x"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "restore requires backup") {
		t.Fatalf("restore without backup accepted: %v", err)
	}
	c.Backup = repoSettings()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Restore.Stanza = ""
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "restore.stanza") {
		t.Fatalf("missing stanza accepted: %v", err)
	}
	c.Restore = &backup.RestoreOptions{Stanza: "old", Type: backup.TargetStandby}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "standby") {
		t.Fatalf("standby target accepted as bootstrap: %v", err)
	}
	c.Restore = &backup.RestoreOptions{Stanza: "old", Type: backup.TargetName, Target: "rp"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "backup id") {
		t.Fatalf("name target without backup id accepted: %v", err)
	}
	c.Restore = nil
	c.RecloneFromRepo = true
	c.Backup = nil
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "recloneFromRepo") {
		t.Fatalf("recloneFromRepo without backup accepted: %v", err)
	}
}

func TestRecoveryConfRendersTargetAndSourceStanza(t *testing.T) {
	c := testConfig()
	c.Role = RolePrimary
	c.Backup = repoSettings()
	c.Restore = &backup.RestoreOptions{Stanza: "old-s0-pg18", Type: backup.TargetTime, Target: "2026-08-19 10:00:00+00", Exclusive: true, TargetTLI: 2}
	got := renderRecoveryConf(c)
	for _, want := range []string{
		"archive_mode = off\n",
		"restore_command = 'pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=old-s0-pg18 archive-get %f \"%p\"'\n",
		"recovery_target_time = '2026-08-19 10:00:00+00'\n",
		"recovery_target_inclusive = 'off'\n",
		"recovery_target_timeline = '2'\n",
		"recovery_target_action = 'promote'\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("recovery conf lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "archive_command") || strings.Contains(got, "primary_conninfo") {
		t.Fatalf("recovery conf must not archive or stream:\n%s", got)
	}
	normal := RenderPostgresqlConf(c, false)
	if !strings.Contains(normal, "archive_mode = on\n") || strings.Contains(normal, "recovery_target") || !strings.Contains(normal, "--stanza=new-s0-pg18 archive-get") {
		t.Fatalf("normal conf after recovery wrong:\n%s", normal)
	}
	for _, name := range backup.RecoverySettingNames {
		if !containsString(OwnedSettings(), name) {
			t.Errorf("%s not owned", name)
		}
	}
	c.PGData = t.TempDir()
	c.Backup = tempRepoSettings(t)
	if err := WriteRecoveryConfig(c); err != nil {
		t.Fatal(err)
	}
	conf, _ := os.ReadFile(c.Backup.ConfigPath)
	if !strings.Contains(string(conf), "[new-s0-pg18]") || !strings.Contains(string(conf), "[old-s0-pg18]") {
		t.Fatalf("pgbackrest.conf lacks the source stanza:\n%s", conf)
	}
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func TestBootstrapRestoresEmptyPrimaryFromRepo(t *testing.T) {
	in := newTestInstance(t)
	in.cfg.Role = RolePrimary
	in.cfg.Backup = tempRepoSettings(t)
	in.cfg.Restore = &backup.RestoreOptions{Stanza: "old-s0-pg18"}
	restored := 0
	in.restoreFn = func(context.Context) error {
		restored++
		return os.WriteFile(filepath.Join(in.cfg.PGData, "PG_VERSION"), []byte("18\n"), 0o600)
	}
	if err := in.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restored != 1 {
		t.Fatalf("restored %d times", restored)
	}
	if err := in.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restored != 1 {
		t.Fatal("restore ran again on a populated data directory")
	}
}

func TestBootstrapRestartsAnUnfinishedRestore(t *testing.T) {
	in := newTestInstance(t)
	in.cfg.Role = RolePrimary
	in.cfg.Backup = tempRepoSettings(t)
	in.cfg.Restore = &backup.RestoreOptions{Stanza: "old-s0-pg18"}
	_ = os.WriteFile(filepath.Join(in.cfg.PGData, "PG_VERSION"), []byte("18\n"), 0o600)
	_ = os.WriteFile(in.restorePendingPath(), []byte("x"), 0o600)
	restored := false
	in.restoreFn = func(context.Context) error {
		if _, err := os.Stat(filepath.Join(in.cfg.PGData, "PG_VERSION")); err == nil {
			t.Fatal("PGDATA was not cleared before restarting the restore")
		}
		restored = true
		return nil
	}
	if err := in.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !restored {
		t.Fatal("restore did not run")
	}
}

func TestBootstrapWithoutRestoreRunsInitdb(t *testing.T) {
	in := newTestInstance(t)
	in.cfg.Role = RolePrimary
	in.restoreFn = func(context.Context) error { t.Fatal("restore ran without restore settings"); return nil }
	err := in.Bootstrap(context.Background())
	if err == nil || !strings.Contains(err.Error(), "initdb") {
		t.Fatalf("expected initdb to be attempted, got %v", err)
	}
}

func TestRebuildPrefersRepoWhenConfigured(t *testing.T) {
	in := newTestInstance(t)
	in.cfg.Backup = tempRepoSettings(t)
	in.cfg.RecloneFromRepo = true
	var order []string
	in.rewindFn = func(context.Context, string) error { return errors.New("no rewind") }
	in.repoCloneFn = func(context.Context) error { order = append(order, "repo"); return nil }
	in.recloneFn = func(context.Context) error { order = append(order, "basebackup"); return nil }
	if err := in.Demote(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "repo" {
		t.Fatalf("order=%v", order)
	}
	order = nil
	in.repoCloneFn = func(context.Context) error { order = append(order, "repo"); return errors.New("no backup") }
	if err := in.Demote(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "repo,basebackup" {
		t.Fatalf("order=%v", order)
	}
	order = nil
	in.cfg.RecloneFromRepo = false
	if err := in.Demote(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "basebackup" {
		t.Fatalf("order=%v", order)
	}
}

func TestRecloneSelectsSource(t *testing.T) {
	in := newTestInstance(t)
	var used []string
	in.repoCloneFn = func(context.Context) error { used = append(used, "repo"); return nil }
	in.recloneFn = func(context.Context) error { used = append(used, "primary"); return nil }
	if err := in.Reclone(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if err := in.Reclone(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if strings.Join(used, ",") != "repo,primary" {
		t.Fatalf("used=%v", used)
	}
}
