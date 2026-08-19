package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Instance drives one PostgreSQL data directory through its lifecycle.
type Instance struct {
	cfg   *Config
	sup   *Supervisor
	epoch *EpochStore
	log   *slog.Logger
	// pgctl and basebackup hooks are swappable for unit tests.
	rewindFn     func(ctx context.Context, source string) error
	recloneFn    func(ctx context.Context) error
	startFn      func(ctx context.Context) error
	slotFn       func(ctx context.Context, source string) error
	waitSourceFn func(ctx context.Context, source string) error
	// cloneRetry is the pause between bootstrap clone attempts.
	cloneRetry time.Duration
}

// NewInstance wires an Instance from its parts.
func NewInstance(cfg *Config, sup *Supervisor, epoch *EpochStore, log *slog.Logger) *Instance {
	in := &Instance{cfg: cfg, sup: sup, epoch: epoch, log: log, cloneRetry: 5 * time.Second}
	in.rewindFn = in.pgRewind
	in.recloneFn = in.baseBackup
	in.startFn = in.Start
	in.slotFn = in.ensureSlotOnSource
	in.waitSourceFn = in.waitSource
	sup.Env = append(sup.Env, "PGPASSFILE="+in.pgpassPath())
	return in
}

// IsEmpty reports whether PGDATA has no cluster in it.
func (in *Instance) IsEmpty() (bool, error) {
	_, err := os.Stat(filepath.Join(in.cfg.PGData, "PG_VERSION"))
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, os.ErrNotExist):
		return true, nil
	default:
		return false, err
	}
}

// IsStandby reports whether standby.signal is present.
func (in *Instance) IsStandby() bool {
	_, err := os.Stat(filepath.Join(in.cfg.PGData, standbySignal))
	return err == nil
}

// Bootstrap initialises an empty PGDATA according to the configured role
// and renders the configuration; it is a no-op on an existing cluster
// except for re-rendering configuration.
func (in *Instance) Bootstrap(ctx context.Context) error {
	if err := in.writePgpass(); err != nil {
		return err
	}
	empty, err := in.IsEmpty()
	if err != nil {
		return err
	}
	switch {
	case empty && in.cfg.Role == RolePrimary:
		if err := in.initdb(ctx); err != nil {
			return err
		}
	case empty && in.cfg.Role == RoleStandby:
		if err := in.retryClone(ctx); err != nil {
			return err
		}
	case !empty && in.cfg.Role == RoleStandby && !in.IsStandby():
		in.log.Info("data directory belongs to a former primary; rejoining as a standby")
		if err := in.waitSourceFn(ctx, in.cfg.PrimaryConninfo); err != nil {
			return err
		}
		if err := in.follow(ctx, in.cfg.PrimaryConninfo); err != nil {
			return err
		}
	}
	return WriteConfig(in.cfg, in.IsStandby())
}

func (in *Instance) initdb(ctx context.Context) error {
	in.log.Info("running initdb", "pgdata", in.cfg.PGData)
	cmd := in.sup.Command(ctx, "initdb", "-D", in.cfg.PGData, "--data-checksums",
		"--auth=scram-sha-256", "-U", "postgres", "--pwfile="+in.cfg.PasswordFile,
		"--encoding=UTF8", "--locale=C.UTF-8")
	_, err := in.sup.RunTracked(cmd)
	return err
}

// retryClone keeps cloning until it succeeds or ctx ends: at bootstrap the
// primary is usually still initialising when the standbys start.
func (in *Instance) retryClone(ctx context.Context) error {
	for {
		err := in.recloneFn(ctx)
		if err == nil {
			return nil
		}
		in.log.Warn("clone failed; retrying", "err", err, "in", in.cloneRetry)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last: %w)", ctx.Err(), err)
		case <-time.After(in.cloneRetry):
		}
	}
}

// baseBackup replaces PGDATA with a fresh clone of the primary.
func (in *Instance) baseBackup(ctx context.Context) error {
	if in.sup.Running() {
		return errors.New("cannot reclone while postgres is running")
	}
	in.log.Info("cloning from primary", "slot", in.cfg.SlotName())
	if err := clearDir(in.cfg.PGData); err != nil {
		return err
	}
	if err := in.ensureSlotOnSource(ctx, in.cfg.PrimaryConninfo); err != nil {
		return err
	}
	cmd := in.sup.Command(ctx, "pg_basebackup", "-D", in.cfg.PGData, "-d", PrimaryConninfo(in.cfg),
		"-X", "stream", "-c", "fast", "-R", "-S", in.cfg.SlotName(), "--no-password")
	if _, err := in.sup.RunTracked(cmd); err != nil {
		return err
	}
	return WriteConfig(in.cfg, true)
}

func (in *Instance) pgRewind(ctx context.Context, source string) error {
	args := []string{"--target-pgdata=" + in.cfg.PGData, "--source-server=" + source, "--no-ensure-shutdown"}
	if in.cfg.Postgres.RestoreCommand != "" {
		args = append(args, "--restore-target-wal")
	}
	cmd := in.sup.Command(ctx, "pg_rewind", args...)
	_, err := in.sup.RunTracked(cmd)
	return err
}

// pgpass lets libpq tools authenticate against the source without exposing
// the password on the command line.
func (in *Instance) pgpassPath() string { return filepath.Join(in.cfg.PGData, "..", ".pgshard-pgpass") }

func (in *Instance) writePgpass() error {
	pw, err := in.password()
	if err != nil {
		return err
	}
	return writeFileSync(in.pgpassPath(), []byte("*:*:*:postgres:"+pw+"\n"))
}

func (in *Instance) password() (string, error) {
	b, err := os.ReadFile(in.cfg.PasswordFile)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}

// Connect opens a superuser connection over the local socket.
func (in *Instance) Connect(ctx context.Context) (*pgx.Conn, error) {
	pw, err := in.password()
	if err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("host=/tmp port=%d user=postgres dbname=postgres password=%s connect_timeout=3", in.cfg.Port, pw)
	return pgx.Connect(ctx, dsn)
}

// Start launches postgres and waits until it accepts connections or ctx ends.
func (in *Instance) Start(ctx context.Context) error {
	if err := in.sup.Start(); err != nil {
		return err
	}
	return in.waitReady(ctx)
}

func (in *Instance) waitReady(ctx context.Context) error {
	for {
		if !in.sup.Running() {
			return errors.New("postgres exited during startup")
		}
		conn, err := in.Connect(ctx)
		if err == nil {
			_ = conn.Close(ctx)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgres not ready: %w (last: %w)", ctx.Err(), err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Promote turns a standby into the primary: waits for the WAL receiver to
// stop, runs pg_ctl promote, and checkpoints. The epoch must already have
// been accepted.
func (in *Instance) Promote(ctx context.Context) error {
	if !in.IsStandby() {
		return errors.New("not a standby")
	}
	if err := in.waitWALReceiverStopped(ctx); err != nil {
		return err
	}
	if _, err := in.sup.RunTracked(in.sup.Command(ctx, "pg_ctl", "promote", "-w", "-D", in.cfg.PGData)); err != nil {
		return err
	}
	for in.IsStandby() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("standby.signal still present: %w", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	if err := WriteConfig(in.cfg, false); err != nil {
		return err
	}
	if err := in.Reload(ctx); err != nil {
		return err
	}
	conn, err := in.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, "CHECKPOINT")
	return err
}

// waitWALReceiverStopped disconnects the standby from the old primary so no
// WAL arrives after the promotion decision.
func (in *Instance) waitWALReceiverStopped(ctx context.Context) error {
	conn, err := in.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "ALTER SYSTEM SET primary_conninfo = ''"); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		return err
	}
	for {
		var n int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_wal_receiver WHERE status <> 'stopped'").Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wal receiver still active: %w", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Demote turns this instance into a standby of source: fast shutdown,
// pg_rewind (falling back to a full reclone), stale slot removal, standby
// configuration and restart.
func (in *Instance) Demote(ctx context.Context, source string) error {
	if err := in.sup.Stop(ctx, ShutdownFast, time.Duration(in.cfg.ShutdownTimeout)); err != nil {
		return err
	}
	if source == "" {
		source = in.cfg.PrimaryConninfo
	}
	if err := in.follow(ctx, source); err != nil {
		return err
	}
	return in.startFn(ctx)
}

// follow turns a stopped former primary into a standby of source: pg_rewind
// (falling back to a full reclone), stale slot removal and standby
// configuration. postgres must not be running.
func (in *Instance) follow(ctx context.Context, source string) error {
	if err := in.rewindFn(ctx, source); err != nil {
		in.log.Warn("pg_rewind failed; recloning", "err", err)
		if err := in.recloneFn(ctx); err != nil {
			return fmt.Errorf("reclone after failed rewind: %w", err)
		}
	} else if err := in.slotFn(ctx, source); err != nil {
		return err
	}
	if err := in.dropStaleSlots(); err != nil {
		return err
	}
	if err := in.writeStandbySignal(); err != nil {
		return err
	}
	return WriteConfig(in.cfg, true)
}

// waitSource blocks until the source primary accepts connections, so a
// rejoin can pg_rewind instead of falling back to a full reclone while the
// -rw Service still has no endpoint.
func (in *Instance) waitSource(ctx context.Context, source string) error {
	pw, err := in.password()
	if err != nil {
		return err
	}
	for {
		conn, err := pgx.Connect(ctx, source+" password="+pw+" connect_timeout=5")
		if err == nil {
			_ = conn.Close(ctx)
			return nil
		}
		in.log.Warn("source primary not reachable yet; waiting", "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for source: %w (last: %w)", ctx.Err(), err)
		case <-time.After(in.cloneRetry):
		}
	}
}

// ensureSlotOnSource creates this member's physical slot on the source
// primary when it is missing, so streaming can start after a rewind.
func (in *Instance) ensureSlotOnSource(ctx context.Context, source string) error {
	pw, err := in.password()
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, source+" password="+pw+" connect_timeout=5")
	if err != nil {
		return fmt.Errorf("connect to source: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, `SELECT pg_create_physical_replication_slot($1, true)
		WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, in.cfg.SlotName())
	return err
}

// Reclone rebuilds PGDATA from the primary and starts it as a standby.
func (in *Instance) Reclone(ctx context.Context) error {
	if err := in.sup.Stop(ctx, ShutdownFast, time.Duration(in.cfg.ShutdownTimeout)); err != nil {
		return err
	}
	if err := in.recloneFn(ctx); err != nil {
		return err
	}
	return in.startFn(ctx)
}

// Rewind runs pg_rewind against source and restarts as a standby.
func (in *Instance) Rewind(ctx context.Context, source string) error {
	if err := in.sup.Stop(ctx, ShutdownFast, time.Duration(in.cfg.ShutdownTimeout)); err != nil {
		return err
	}
	if err := in.rewindFn(ctx, source); err != nil {
		return err
	}
	if err := in.slotFn(ctx, source); err != nil {
		return err
	}
	if err := in.writeStandbySignal(); err != nil {
		return err
	}
	if err := WriteConfig(in.cfg, true); err != nil {
		return err
	}
	return in.startFn(ctx)
}

// Restart stops with mode and starts postgres again.
func (in *Instance) Restart(ctx context.Context, mode ShutdownMode) error {
	if err := in.sup.Stop(ctx, mode, time.Duration(in.cfg.ShutdownTimeout)); err != nil {
		return err
	}
	return in.Start(ctx)
}

// Reload asks the postmaster to reread its configuration.
func (in *Instance) Reload(ctx context.Context) error {
	if err := in.cfg.Refresh(); err != nil {
		return err
	}
	if err := WriteConfig(in.cfg, in.IsStandby()); err != nil {
		return err
	}
	_, err := in.sup.RunTracked(in.sup.Command(ctx, "pg_ctl", "reload", "-D", in.cfg.PGData))
	return err
}

func (in *Instance) writeStandbySignal() error {
	return writeFileSync(filepath.Join(in.cfg.PGData, standbySignal), nil)
}

// dropStaleSlots removes replication slots inherited from the old timeline;
// postgres must be stopped.
func (in *Instance) dropStaleSlots() error {
	if in.sup.Running() {
		return errors.New("cannot drop slots while postgres is running")
	}
	dir := filepath.Join(in.cfg.PGData, "pg_replslot")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		in.log.Info("dropping stale replication slot", "slot", e.Name())
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return syncDir(dir)
}

func clearDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}
