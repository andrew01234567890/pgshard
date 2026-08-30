package agent

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// Started reports ok once the postmaster answers, including during crash
// recovery when it rejects connections with SQLSTATE 57P03.
func (in *Instance) Started(ctx context.Context) error {
	if !in.sup.Running() {
		return errors.New("postgres is not running")
	}
	conn, err := in.Connect(ctx)
	if err == nil {
		_ = conn.Close(ctx)
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "57P03" {
		return nil
	}
	return err
}

// IsPrimary is the inverse of IsStandby, and carries its error for the
// same reason: not knowing is not the same as being a primary.
func (in *Instance) IsPrimary() (bool, error) {
	standby, err := in.IsStandby()
	return !standby, err
}

// PrimaryAcceptsWrites checks the instance is out of recovery.
func (in *Instance) PrimaryAcceptsWrites(ctx context.Context) error {
	conn, err := in.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT NOT pg_is_in_recovery()").Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return errors.New("primary is in recovery")
	}
	return nil
}

// ReplayLagBytes returns the difference between the WAL received from the
// primary and what has been replayed; it fails when not streaming.
func (in *Instance) ReplayLagBytes(ctx context.Context) (int64, error) {
	conn, err := in.Connect(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var status *string
	var lag *int64
	err = conn.QueryRow(ctx, `SELECT r.status, pg_wal_lsn_diff(r.flushed_lsn, pg_last_wal_replay_lsn())::bigint
		FROM pg_stat_wal_receiver r`).Scan(&status, &lag)
	if err != nil {
		return 0, errors.New("wal receiver not running")
	}
	if status == nil || *status != "streaming" {
		return 0, errors.New("wal receiver not streaming")
	}
	if lag == nil {
		return 0, nil
	}
	return *lag, nil
}
