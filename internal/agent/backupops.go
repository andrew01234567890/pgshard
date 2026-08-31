package agent

import (
	"context"
	"errors"
	"os/exec"
	"time"

	"github.com/andrew01234567890/pgshard/internal/agent/backup"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// ErrNoBackupPolicy is returned by backup RPCs when no policy is configured.
var ErrNoBackupPolicy = errors.New("no backup policy configured for this member")

// ErrBackupOnStandby is returned when a backup is requested from a standby.
var ErrBackupOnStandby = errors.New("backups run on the primary only")

// backupRunner builds the pgbackrest runner for the current settings.
func (in *Instance) backupRunner() (*backup.Runner, error) {
	if in.cfg.Backup == nil {
		return nil, ErrNoBackupPolicy
	}
	if in.newRunner != nil {
		return in.newRunner(*in.cfg.Backup), nil
	}
	r := backup.NewRunner(*in.cfg.Backup, in.log)
	r.Env = []string{"PGPASSFILE=" + in.pgpassPath()}
	r.Start = func(cmd *exec.Cmd) (func(), error) {
		if err := in.sup.StartTracked(cmd); err != nil {
			return nil, err
		}
		pid := cmd.Process.Pid
		return func() { in.sup.Untrack(pid) }, nil
	}
	return r, nil
}

// EnsureStanza creates or upgrades the pgbackrest stanza; a no-op without a
// policy or on a standby.
func (in *Instance) EnsureStanza(ctx context.Context) error {
	if in.cfg.Backup == nil {
		return nil
	}
	standby, err := in.IsStandby()
	if err != nil {
		return err
	}
	if standby {
		return nil
	}
	r, err := in.backupRunner()
	if err != nil {
		return err
	}
	return r.EnsureStanza(ctx)
}

// stanzaLockRetry is the pause after stanza-create lost the stanza lock to
// archive-push; bounded by stanzaLockRetries before falling back to the
// slow retry so a wedged lock file cannot spin the loop.
const (
	stanzaLockRetry   = time.Second
	stanzaLockRetries = 30
)

// startStanzaWorker runs ensureStanzaLoop as this instance's only stanza
// worker, ending any earlier one first.
//
// The loop retries a repository that is unreachable, which is the point of
// it, and an attempt can stay blocked for as long as pgBackRest takes to
// give up. Startup began one and every successful promotion began another,
// with no handle on either, so role cycling during repository trouble
// accumulated loops and the pgBackRest children they track -- and a
// demotion could not end the attempt that was still going.
func (in *Instance) startStanzaWorker(ctx context.Context, retry time.Duration) {
	in.stopStanzaWorker()
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	in.stanzaMu.Lock()
	in.stanzaCancel, in.stanzaDone = cancel, done
	in.stanzaMu.Unlock()
	go func() {
		defer close(done)
		in.ensureStanzaLoop(loopCtx, retry)
	}()
}

// stopStanzaWorker ends the stanza worker and waits for it, so the caller
// knows no pgBackRest attempt of the old term is still running.
func (in *Instance) stopStanzaWorker() {
	in.stanzaMu.Lock()
	cancel, done := in.stanzaCancel, in.stanzaDone
	in.stanzaCancel, in.stanzaDone = nil, nil
	in.stanzaMu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// ensureStanzaLoop retries EnsureStanza until it succeeds or ctx ends, so a
// repository that is unreachable at start does not keep the primary down.
func (in *Instance) ensureStanzaLoop(ctx context.Context, retry time.Duration) {
	busy := 0
	for {
		err := in.EnsureStanza(ctx)
		if err == nil {
			return
		}
		wait, fast := stanzaWait(err, retry, &busy)
		if fast {
			in.log.Info("pgbackrest stanza lock busy; retrying", "err", err, "in", wait, "attempt", busy)
		} else {
			in.log.Warn("pgbackrest stanza not ready; retrying", "err", err, "in", wait)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// stanzaWait picks the pause before the next EnsureStanza attempt: a lost
// stanza lock retries fast for up to stanzaLockRetries consecutive times,
// anything else (or a lock still busy after that) waits the slow retry.
func stanzaWait(err error, retry time.Duration, busy *int) (time.Duration, bool) {
	if backup.LockBusy(err) && *busy < stanzaLockRetries {
		*busy++
		return min(stanzaLockRetry, retry), true
	}
	*busy = 0
	return retry, false
}

// Backup takes a backup of the given type; the primary only.
func (in *Instance) Backup(ctx context.Context, t backup.Type) (backup.Result, error) {
	standby, err := in.IsStandby()
	if err != nil {
		return backup.Result{}, err
	}
	if standby {
		return backup.Result{}, ErrBackupOnStandby
	}
	r, err := in.backupRunner()
	if err != nil {
		return backup.Result{}, err
	}
	return r.Backup(ctx, t)
}

func backupType(t pgshardv1.BackupRequest_Type) (backup.Type, error) {
	switch t {
	case pgshardv1.BackupRequest_TYPE_FULL, pgshardv1.BackupRequest_TYPE_UNSPECIFIED:
		return backup.Full, nil
	case pgshardv1.BackupRequest_TYPE_DIFF:
		return backup.Diff, nil
	case pgshardv1.BackupRequest_TYPE_INCR:
		return backup.Incr, nil
	}
	return "", errors.New("unknown backup type")
}

func backupInfoProto(i backup.Info) *pgshardv1.BackupInfo {
	return &pgshardv1.BackupInfo{
		Label: i.Label, Type: i.Type, Prior: i.Prior,
		StartLsn: i.StartLSN, StopLsn: i.StopLSN,
		ArchiveStart: i.ArchiveStart, ArchiveStop: i.ArchiveStop,
		SizeBytes: i.SizeBytes, RepoSizeBytes: i.RepoBytes,
		StartedAt: i.StartedAt, FinishedAt: i.FinishedAt,
	}
}

// Backup runs pgbackrest backup on the primary at the current epoch.
func (s *Server) Backup(ctx context.Context, req *pgshardv1.BackupRequest) (*pgshardv1.BackupResponse, error) {
	resp := &pgshardv1.BackupResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	t, err := backupType(req.GetType())
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	res, err := s.inst.Backup(ctx, t)
	resp.Log = res.Log
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	resp.BackupRef = res.Info.Label
	resp.Info = backupInfoProto(res.Info)
	return resp, nil
}

// RestoreInfo reports the repository contents for this member's stanza.
func (s *Server) RestoreInfo(ctx context.Context, _ *pgshardv1.RestoreInfoRequest) (*pgshardv1.RestoreInfoResponse, error) {
	resp := &pgshardv1.RestoreInfoResponse{Epoch: s.epoch.Current()}
	r, err := s.inst.backupRunner()
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	st, err := r.Info(ctx)
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	resp.Stanza = st.Name
	resp.StatusCode = st.StatusCode
	resp.StatusMessage = st.StatusMessage
	resp.ArchiveMin = st.ArchiveMin
	resp.ArchiveMax = st.ArchiveMax
	for _, b := range st.Backups {
		resp.Backups = append(resp.Backups, backupInfoProto(b))
	}
	return resp, nil
}

// Expire applies retention at the current epoch.
func (s *Server) Expire(ctx context.Context, req *pgshardv1.ExpireRequest) (*pgshardv1.ExpireResponse, error) {
	resp := &pgshardv1.ExpireResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	r, err := s.inst.backupRunner()
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	resp.Log, err = r.Expire(ctx)
	resp.Error = pgErr(err)
	return resp, nil
}

// Verify checks the repository.
func (s *Server) Verify(ctx context.Context, _ *pgshardv1.VerifyRequest) (*pgshardv1.VerifyResponse, error) {
	resp := &pgshardv1.VerifyResponse{Epoch: s.epoch.Current()}
	r, err := s.inst.backupRunner()
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	resp.Log, err = r.Verify(ctx)
	resp.Error = pgErr(err)
	return resp, nil
}
