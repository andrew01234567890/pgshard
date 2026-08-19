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

// Prober talks SQL to a group primary. It is an interface so envtest can
// substitute a fake; the real one dials with pgx.
type Prober interface {
	Probe(ctx context.Context, dsn string) (PrimaryState, error)
	SetSyncStandbyNames(ctx context.Context, dsn, value string) error
	MigrateCatalog(ctx context.Context, dsn string) error
}

// DSN builds the connection URL for a group's -rw Service.
func DSN(host, namespace, password string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(superuserName, password),
		Host:     fmt.Sprintf("%s.%s.svc:%d", host, namespace, postgresPort),
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
