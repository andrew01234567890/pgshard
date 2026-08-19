package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/agent/backup"
)

// Files rendered by the agent under PGDATA.
const (
	postgresqlConf = "postgresql.conf"
	pgHBAConf      = "pg_hba.conf"
	overrideConf   = "pgshard.override.conf"
	// slotsConf holds synchronized_standby_slots, rewritten at runtime by
	// SetSynchronizedStandbySlots and kept across config re-renders.
	slotsConf     = "pgshard.slots.conf"
	standbySignal = "standby.signal"
	autoConf      = "postgresql.auto.conf"
)

// RenderPostgresqlConf renders the fixed pgshard postgresql.conf. When
// standby is true the recovery settings pointing at PrimaryConninfo are
// included.
func RenderPostgresqlConf(c *Config, standby bool) string {
	return renderPostgresqlConf(c, standby, false)
}

// RenderRecoveryConf renders postgresql.conf for the archive recovery that
// follows a restore from c.Restore: WAL comes from the source stanza, the
// recovery target settings are set, and nothing is archived until the
// instance is a normal primary again.
func RenderRecoveryConf(c *Config) string { return renderPostgresqlConf(c, false, true) }

func renderPostgresqlConf(c *Config, standby, recovering bool) string {
	set := ownedSettings(c, standby, recovering)
	for k, v := range c.Postgres.Parameters {
		if _, owned := set[k]; !owned {
			set[k] = quote(v)
		}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("# Managed by pgshard-agent. Edits are overwritten.\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "%s = %s\n", k, set[k])
	}
	fmt.Fprintf(&b, "include_if_exists = %s\n", quote(overrideConf))
	fmt.Fprintf(&b, "include_if_exists = %s\n", quote(slotsConf))
	return b.String()
}

// OwnedSettings lists the postgresql.conf names the agent fixes itself; a
// value for one of them from any other source is ignored.
func OwnedSettings() []string {
	set := ownedSettings(&Config{TLS: TLSFiles{CertFile: "x", KeyFile: "x", CAFile: "x"}, Postgres: PostgresSettings{RestoreCommand: "x"}, Backup: &backup.Settings{}}, true, false)
	for _, k := range backup.RecoverySettingNames {
		set[k] = ""
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func ownedSettings(c *Config, standby, recovering bool) map[string]string {
	set := map[string]string{
		"listen_addresses":               quote("*"),
		"port":                           fmt.Sprint(c.Port),
		"wal_level":                      "logical",
		"wal_log_hints":                  "on",
		"restart_after_crash":            "off",
		"hot_standby":                    "on",
		"hot_standby_feedback":           onOff(standby),
		"wal_sender_timeout":             quote("5s"),
		"wal_receiver_timeout":           quote("5s"),
		"archive_timeout":                quote("5min"),
		"max_prepared_transactions":      fmt.Sprint(c.Postgres.MaxPreparedTransactions),
		"max_replication_slots":          fmt.Sprint(c.Postgres.MaxReplicationSlots),
		"max_wal_senders":                fmt.Sprint(c.Postgres.MaxWALSenders),
		"max_active_replication_origins": fmt.Sprint(c.Postgres.MaxActiveReplicationOrigins),
		"track_commit_timestamp":         "on",
		"password_encryption":            quote("scram-sha-256"),
		"synchronous_commit":             "on",
		"synchronous_standby_names":      quote(c.Postgres.SynchronousStandbyNames),
		"ssl":                            onOff(c.TLS.CertFile != ""),
		"unix_socket_directories":        quote("/tmp"),
		"wal_keep_size":                  quote(c.Postgres.WALKeepSize),
	}
	if c.TLS.CertFile != "" {
		set["ssl_cert_file"] = quote(c.TLS.CertFile)
		set["ssl_key_file"] = quote(c.TLS.KeyFile)
		if c.TLS.CAFile != "" {
			set["ssl_ca_file"] = quote(c.TLS.CAFile)
		}
	}
	set["archive_mode"] = onOff(c.Backup != nil)
	if c.Backup != nil {
		set["archive_command"] = quote(backup.ArchiveCommand(*c.Backup))
		set["restore_command"] = quote(backup.RestoreCommand(*c.Backup))
	}
	if c.Postgres.RestoreCommand != "" {
		set["restore_command"] = quote(c.Postgres.RestoreCommand)
	}
	if standby {
		set["primary_conninfo"] = quote(PrimaryConninfo(c))
		set["primary_slot_name"] = quote(c.SlotName())
		// Failover slots are synchronized to standbys; needs
		// hot_standby_feedback and a dbname in primary_conninfo (both set).
		set["sync_replication_slots"] = "on"
	}
	if recovering && c.Restore != nil && c.Backup != nil {
		set["archive_mode"] = "off"
		delete(set, "archive_command")
		set["restore_command"] = quote(backup.RestoreCommandFor(*c.Backup, c.Restore.Stanza))
		for k, v := range backup.RecoverySettings(*c.Restore) {
			set[k] = quote(v)
		}
	}
	return set
}

// PrimaryConninfo derives the standby's connection string from the
// configured source, forcing dbname and application_name.
func PrimaryConninfo(c *Config) string {
	parts := []string{}
	for _, kv := range strings.Fields(c.PrimaryConninfo) {
		k, _, _ := strings.Cut(kv, "=")
		if k == "dbname" || k == "application_name" {
			continue
		}
		parts = append(parts, kv)
	}
	parts = append(parts, "dbname=postgres", "application_name="+c.Member)
	return strings.Join(parts, " ")
}

// RenderPgHBAConf renders pg_hba.conf.
func RenderPgHBAConf(c *Config) string {
	host := "hostnossl"
	tlsLine := ""
	if c.TLS.CertFile != "" {
		host = "hostssl"
		tlsLine = "\n# TLS is preferred; plain TCP is rejected when certificates are configured.\n"
	}
	cidr := c.PodCIDR
	if cidr == "" {
		cidr = "samenet"
	}
	var b strings.Builder
	b.WriteString("# Managed by pgshard-agent. Edits are overwritten.\n")
	b.WriteString("local   all             all                                     scram-sha-256\n")
	b.WriteString("local   replication     all                                     scram-sha-256\n")
	b.WriteString(tlsLine)
	fmt.Fprintf(&b, "%-8s all             all             %-23s scram-sha-256\n", hostKeyword(host), cidr)
	fmt.Fprintf(&b, "%-8s replication     all             %-23s scram-sha-256\n", hostKeyword(host), cidr)
	if host == "hostssl" {
		fmt.Fprintf(&b, "%-8s all             all             %-23s reject\n", "hostnossl", cidr)
	}
	return b.String()
}

func hostKeyword(h string) string {
	if h == "hostnossl" {
		return "host"
	}
	return h
}

// WriteConfig writes postgresql.conf and pg_hba.conf into PGDATA and clears
// any settings a clone tool left in postgresql.auto.conf so the rendered
// file is authoritative.
func WriteConfig(c *Config, standby bool) error { return writeConfig(c, standby, false) }

// WriteRecoveryConfig writes the configuration for the recovery that follows
// a restore from the repository.
func WriteRecoveryConfig(c *Config) error { return writeConfig(c, false, true) }

func writeConfig(c *Config, standby, recovering bool) error {
	files := map[string]string{
		postgresqlConf: renderPostgresqlConf(c, standby, recovering),
		pgHBAConf:      RenderPgHBAConf(c),
		autoConf:       "# Managed by pgshard-agent; runtime ALTER SYSTEM is not supported.\n",
	}
	if c.OverrideFile != "" {
		body, err := os.ReadFile(c.OverrideFile)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		files[overrideConf] = string(body)
	}
	for name, body := range files {
		if err := writeFileSync(filepath.Join(c.PGData, name), []byte(body)); err != nil {
			return err
		}
	}
	if c.Backup != nil {
		var extra []string
		if c.Restore != nil && c.Restore.Stanza != c.Backup.Stanza {
			extra = append(extra, c.Restore.Stanza)
		}
		if err := backup.WriteConfig(*c.Backup, c.PGData, c.Port, extra...); err != nil {
			return fmt.Errorf("pgbackrest config: %w", err)
		}
	}
	return nil
}

func writeFileSync(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}
