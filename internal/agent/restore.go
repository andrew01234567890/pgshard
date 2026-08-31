package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/andrew01234567890/pgshard/internal/agent/backup"
)

const recoverySignal = "recovery.signal"

// restorePendingPath marks a restore from the repository that has not
// reached its target yet; it lives beside PGDATA so clearing PGDATA keeps it.
// removeIfPresent deletes path, treating only its absence as success.
func removeIfPresent(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (in *Instance) restorePendingPath() string {
	return filepath.Join(in.cfg.PGData, "..", ".pgshard-restore-pending")
}

// restorePending reports whether an earlier restore left its marker. Only
// the marker's absence is absence: reading PGDATA's parent can fail for
// reasons that are not "no restore was interrupted", and taking one for
// the other starts PostgreSQL on a half-restored directory.
func (in *Instance) restorePending() (bool, error) {
	_, err := os.Stat(in.restorePendingPath())
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	default:
		return false, err
	}
}

// restoreBootstrap builds a new primary from the source stanza: pgbackrest
// restore into the empty PGDATA, archive recovery to the target with WAL
// fetched from the source stanza, promotion, and a stop so the instance
// starts again as a normal primary. Nothing is archived before that point,
// so the source stanza is never written to.
func (in *Instance) restoreBootstrap(ctx context.Context) error {
	policy := in.cfg.BackupPolicy()
	if in.cfg.Restore == nil || policy == nil {
		return errors.New("restore needs restore and backup settings")
	}
	opts := *in.cfg.Restore
	opts.Delta = false
	in.log.Info("restoring from the repository", "options", opts.String(), "pgdata", in.cfg.PGData)
	if err := writeFileSync(in.restorePendingPath(), []byte(opts.String()+"\n")); err != nil {
		return err
	}
	if err := clearDir(in.cfg.PGData); err != nil {
		return err
	}
	if err := backup.WriteConfig(*policy, in.cfg.PGData, in.cfg.Port, opts.Stanza); err != nil {
		return fmt.Errorf("pgbackrest config: %w", err)
	}
	r, err := in.backupRunner()
	if err != nil {
		return err
	}
	if _, err := r.Restore(ctx, opts); err != nil {
		return err
	}
	// A signal file left behind decides how PostgreSQL starts, so a
	// failure to remove one is not something to start on top of.
	if err := removeIfPresent(filepath.Join(in.cfg.PGData, standbySignal)); err != nil {
		return err
	}
	if err := writeFileSync(filepath.Join(in.cfg.PGData, recoverySignal), nil); err != nil {
		return err
	}
	if err := WriteRecoveryConfig(in.cfg); err != nil {
		return err
	}
	if err := in.recoverToTarget(ctx); err != nil {
		return err
	}
	if err := WriteConfig(in.cfg, false); err != nil {
		return err
	}
	if err := os.Remove(in.restorePendingPath()); err != nil {
		return err
	}
	return syncDir(filepath.Dir(in.restorePendingPath()))
}

// recoverToTarget runs postgres until archive recovery has ended and the
// instance promoted, then stops it. PostgreSQL refuses to start when the
// WAL ends before the target, which surfaces here as an exit.
func (in *Instance) recoverToTarget(ctx context.Context) error {
	if err := in.startFn(ctx); err != nil {
		return fmt.Errorf("recovery: %w", err)
	}
	for {
		if !in.sup.Running() {
			return errors.New("postgres exited during recovery; the WAL may end before the target")
		}
		recovering, err := in.inRecovery(ctx)
		if err == nil && !recovering {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("recovery: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	tl, err := in.timeline(ctx)
	if err == nil {
		in.log.Info("recovery reached the target; promoted", "timeline", tl)
	}
	return in.sup.Stop(ctx, ShutdownFast, time.Duration(in.cfg.ShutdownTimeout))
}

func (in *Instance) inRecovery(ctx context.Context) (bool, error) {
	conn, err := in.Connect(ctx)
	if err != nil {
		return true, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var rec bool
	err = conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&rec)
	return rec, err
}

func (in *Instance) timeline(ctx context.Context) (int64, error) {
	conn, err := in.Connect(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var tl int64
	err = conn.QueryRow(ctx, "SELECT timeline_id FROM pg_control_checkpoint()").Scan(&tl)
	return tl, err
}

// repoClone rebuilds PGDATA as a standby from this member's own stanza with
// a delta restore, then points it at the primary. postgres must be stopped.
func (in *Instance) repoClone(ctx context.Context) error {
	if in.sup.Running() {
		return errors.New("cannot reclone while postgres is running")
	}
	r, err := in.backupRunner()
	if err != nil {
		return err
	}
	policy := in.cfg.BackupPolicy()
	if policy == nil {
		return ErrNoBackupPolicy
	}
	if err := os.MkdirAll(in.cfg.PGData, 0o700); err != nil {
		return err
	}
	if err := backup.WriteConfig(*policy, in.cfg.PGData, in.cfg.Port); err != nil {
		return fmt.Errorf("pgbackrest config: %w", err)
	}
	ok, err := r.HasCompletedBackup(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("the repository holds no backup for this stanza")
	}
	in.log.Info("recloning from the repository", "stanza", policy.Stanza, "slot", in.cfg.SlotName())
	if err := in.slotFn(ctx, in.cfg.PrimaryConninfo); err != nil {
		return err
	}
	if _, err := r.Restore(ctx, backup.RestoreOptions{Type: backup.TargetStandby, Delta: true}); err != nil {
		return err
	}
	if err := in.dropStaleSlots(); err != nil {
		return err
	}
	if err := removeIfPresent(filepath.Join(in.cfg.PGData, recoverySignal)); err != nil {
		return err
	}
	if err := in.writeStandbySignal(); err != nil {
		return err
	}
	return WriteConfig(in.cfg, true)
}
