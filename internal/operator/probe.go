package operator

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// PrimaryState is what the operator learned from one probe of a group primary.
type PrimaryState struct {
	// Streaming holds the application_name of every standby with a streaming
	// walsender.
	Streaming        map[string]bool
	SyncStandbyNames string
}

// StandbyState is what the operator learned from one probe of a member
// during failover.
type StandbyState struct {
	InRecovery bool
	// Streaming is true while a WAL receiver is still connected to a primary.
	Streaming bool
	// FlushLSN is pg_last_wal_receive_lsn(): the WAL durably received.
	FlushLSN uint64
}

// SettingState is one row of pg_settings the operator cares about.
type SettingState struct {
	Value string
	// Context is the GUC context: postmaster values need a restart, the
	// rest apply on reload.
	Context        string
	PendingRestart bool
}

// Prober talks SQL to group members. It is an interface so envtest can
// substitute a fake; the real one dials with pgx.
type Prober interface {
	Probe(ctx context.Context, dsn string) (PrimaryState, error)
	ProbeStandby(ctx context.Context, dsn string) (StandbyState, error)
	SetSyncStandbyNames(ctx context.Context, dsn, value string) error
	MigrateCatalog(ctx context.Context, dsn string) error
	// PublishShardStatus upserts pgshard.shard_status for one shard group;
	// it never lowers primary_epoch.
	PublishShardStatus(ctx context.Context, dsn string, shardID int, groupName string, epoch int64, endpoint string) error
	// EnsureSlots creates the missing physical slots in want on the primary
	// and drops an inactive slot named drop (the primary's own, inherited
	// from its time as a standby, which would otherwise pin WAL forever).
	EnsureSlots(ctx context.Context, dsn string, want []string, drop string) error
	// Settings reads pg_settings for names.
	Settings(ctx context.Context, dsn string, names []string) (map[string]SettingState, error)
}

// DSN builds the connection URL for a group's -rw Service.
func DSN(host, namespace, password string) string {
	return HostDSN(fmt.Sprintf("%s.%s.svc", host, namespace), password)
}

// HostDSN builds the connection URL for one host (a Service name or pod IP).
func HostDSN(host, password string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(superuserName, password),
		Host:     fmt.Sprintf("%s:%d", host, postgresPort),
		Path:     "/postgres",
		RawQuery: "sslmode=disable&connect_timeout=5",
	}
	return u.String()
}

// PgxProber is the production Prober.
type PgxProber struct{}

// Probe runs SELECT 1 and reads replication state on the primary.
func (PgxProber) Probe(ctx context.Context, dsn string) (PrimaryState, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return PrimaryState{}, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		return PrimaryState{}, fmt.Errorf("SELECT 1: %w", err)
	}
	var recovery bool
	if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&recovery); err != nil {
		return PrimaryState{}, err
	}
	if recovery {
		return PrimaryState{}, fmt.Errorf("primary service points at a standby")
	}
	st := PrimaryState{Streaming: map[string]bool{}}
	rows, err := conn.Query(ctx, "SELECT application_name FROM pg_stat_replication WHERE state = 'streaming'")
	if err != nil {
		return PrimaryState{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return PrimaryState{}, err
		}
		st.Streaming[name] = true
	}
	if err := rows.Err(); err != nil {
		return PrimaryState{}, err
	}
	if err := conn.QueryRow(ctx, "SHOW synchronous_standby_names").Scan(&st.SyncStandbyNames); err != nil {
		return PrimaryState{}, err
	}
	return st, nil
}

// ProbeStandby reads recovery and WAL receiver state from one member.
func (PgxProber) ProbeStandby(ctx context.Context, dsn string) (StandbyState, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return StandbyState{}, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var st StandbyState
	var lsn *int64
	err = conn.QueryRow(ctx, `SELECT pg_is_in_recovery(),
		EXISTS (SELECT 1 FROM pg_stat_wal_receiver WHERE status = 'streaming'),
		CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_flush_lsn() END - '0/0'::pg_lsn`).
		Scan(&st.InRecovery, &st.Streaming, &lsn)
	if err != nil {
		return StandbyState{}, err
	}
	if lsn != nil {
		st.FlushLSN = uint64(*lsn)
	}
	return st, nil
}

// PublishShardStatus upserts the shard's fence into pgshard.shard_status.
func (PgxProber) PublishShardStatus(ctx context.Context, dsn string, shardID int, groupName string, epoch int64, endpoint string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint)
		VALUES ($1, $2, $3, 'serving', $4, $5)
		ON CONFLICT (shard_set, shard_id) DO UPDATE
		SET group_name = EXCLUDED.group_name, primary_epoch = EXCLUDED.primary_epoch,
		    primary_endpoint = EXCLUDED.primary_endpoint, updated_at = now()
		WHERE pgshard.shard_status.primary_epoch <= EXCLUDED.primary_epoch`,
		shardSet, shardID, groupName, epoch, endpoint)
	return err
}

// EnsureSlots creates missing physical slots and drops the primary's own.
func (PgxProber) EnsureSlots(ctx context.Context, dsn string, want []string, drop string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, name := range want {
		if _, err := conn.Exec(ctx, `SELECT pg_create_physical_replication_slot($1, true)
			WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, name); err != nil {
			return fmt.Errorf("create slot %s: %w", name, err)
		}
	}
	if drop == "" {
		return nil
	}
	_, err = conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
		WHERE slot_name = $1 AND NOT active`, drop)
	return err
}

// Settings reads the named rows of pg_settings.
func (PgxProber) Settings(ctx context.Context, dsn string, names []string) (map[string]SettingState, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, "SELECT name, setting, context, pending_restart FROM pg_settings WHERE name = ANY($1)", names)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]SettingState{}
	for rows.Next() {
		var name string
		var st SettingState
		if err := rows.Scan(&name, &st.Value, &st.Context, &st.PendingRestart); err != nil {
			return nil, err
		}
		out[name] = st
	}
	return out, rows.Err()
}

// SetSyncStandbyNames applies synchronous_standby_names via ALTER SYSTEM and reloads.
func (PgxProber) SetSyncStandbyNames(ctx context.Context, dsn, value string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "ALTER SYSTEM SET synchronous_standby_names = "+quoteLiteral(value)); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "SELECT pg_reload_conf()")
	return err
}

// MigrateCatalog applies the embedded catalog migrations.
func (PgxProber) MigrateCatalog(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	return catalog.Migrate(ctx, conn)
}

func quoteLiteral(s string) string {
	var b []byte
	b = append(b, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			b = append(b, '\'')
		}
		b = append(b, s[i])
	}
	return string(append(b, '\''))
}
