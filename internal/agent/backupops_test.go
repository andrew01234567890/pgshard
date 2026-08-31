package agent

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/agent/backup"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

type fakePgbackrest struct {
	mu    sync.Mutex
	calls []string
	fail  map[string]error
	info  string
}

func (f *fakePgbackrest) exec(_ context.Context, args []string, onLine func(string)) error {
	cmd := args[len(args)-1]
	f.mu.Lock()
	f.calls = append(f.calls, strings.Join(args, " "))
	f.mu.Unlock()
	if cmd == "info" {
		onLine(f.info)
	} else {
		onLine("P00 INFO: " + cmd + " command end")
	}
	return f.fail[cmd]
}

func (f *fakePgbackrest) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newBackupServer(t *testing.T, standby bool) (*Server, *fakePgbackrest) {
	t.Helper()
	in := newTestInstance(t)
	in.cfg.Role = RolePrimary
	if standby {
		if err := in.writeStandbySignal(); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.ReadFile(filepath.Join("backup", "testdata", "info.json"))
	if err != nil {
		t.Fatal(err)
	}
	f := &fakePgbackrest{info: string(info), fail: map[string]error{}}
	in.cfg.Backup = &backup.Settings{Stanza: "t-catalog-pg18", Repo: backup.Repo{Type: backup.TypePosix}}
	in.newRunner = func(s backup.Settings) *backup.Runner {
		r := backup.NewRunner(s, in.log)
		r.Exec = f.exec
		return r
	}
	return NewServer(in, in.epoch, nil, in.log, func(error) {}), f
}

func TestBackupRPCRunsOnPrimaryAndReportsInfo(t *testing.T) {
	s, f := newBackupServer(t, false)
	resp, err := s.Backup(context.Background(), &pgshardv1.BackupRequest{Type: pgshardv1.BackupRequest_TYPE_INCR})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetError() != nil {
		t.Fatalf("error: %v", resp.GetError())
	}
	if resp.GetBackupRef() != "20260819-030047F_20260819-030048I" || resp.GetInfo().GetType() != "incr" || resp.GetInfo().GetStopLsn() != 0x4000050 || resp.GetInfo().GetPrior() != "20260819-030047F" {
		t.Errorf("resp %v", resp)
	}
	if len(resp.GetLog()) != 1 {
		t.Errorf("log %v", resp.GetLog())
	}
	if len(f.calls) != 2 || !strings.HasSuffix(f.calls[0], "--type=incr backup") || !strings.HasSuffix(f.calls[1], "--output=json info") {
		t.Errorf("calls %v", f.calls)
	}
	if !strings.Contains(f.calls[0], "--stanza=t-catalog-pg18") {
		t.Errorf("stanza missing: %v", f.calls[0])
	}
}

func TestBackupRPCDefaultTypeIsFullAndFencesEpoch(t *testing.T) {
	s, f := newBackupServer(t, false)
	if err := s.epoch.Accept(3); err != nil {
		t.Fatal(err)
	}
	resp, _ := s.Backup(context.Background(), &pgshardv1.BackupRequest{Epoch: 2})
	if resp.GetError().GetSqlstate() != "55000" || len(f.calls) != 0 {
		t.Fatalf("stale epoch must be rejected before running: %v %v", resp.GetError(), f.calls)
	}
	resp, _ = s.Backup(context.Background(), &pgshardv1.BackupRequest{Epoch: 3})
	if resp.GetError() != nil || !strings.HasSuffix(f.calls[0], "--type=full backup") {
		t.Fatalf("full: %v %v", resp.GetError(), f.calls)
	}
}

func TestBackupRPCRefusesStandbyAndMissingPolicy(t *testing.T) {
	s, f := newBackupServer(t, true)
	resp, _ := s.Backup(context.Background(), &pgshardv1.BackupRequest{})
	if resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "primary only") || len(f.calls) != 0 {
		t.Fatalf("standby: %v %v", resp.GetError(), f.calls)
	}
	s, f = newBackupServer(t, false)
	s.inst.cfg.Backup = nil
	resp, _ = s.Backup(context.Background(), &pgshardv1.BackupRequest{})
	if resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "no backup policy") || len(f.calls) != 0 {
		t.Fatalf("no policy: %v %v", resp.GetError(), f.calls)
	}
	info, _ := s.RestoreInfo(context.Background(), nil)
	if info.GetError() == nil {
		t.Fatal("restore info without policy must fail")
	}
}

func TestBackupRPCReportsPgbackrestFailure(t *testing.T) {
	s, f := newBackupServer(t, false)
	f.fail["backup"] = errors.New("exit status 57")
	resp, _ := s.Backup(context.Background(), &pgshardv1.BackupRequest{})
	if resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "exit status 57") || resp.GetBackupRef() != "" {
		t.Fatalf("resp %v", resp)
	}
	if len(resp.GetLog()) != 1 {
		t.Errorf("log tail must be returned on failure: %v", resp.GetLog())
	}
}

func TestRestoreInfoExpireVerify(t *testing.T) {
	s, f := newBackupServer(t, false)
	info, _ := s.RestoreInfo(context.Background(), nil)
	if info.GetError() != nil || info.GetStanza() != "t-catalog-pg18" || info.GetArchiveMax() != "000000010000000000000004" || len(info.GetBackups()) != 2 || info.GetStatusMessage() != "ok" {
		t.Fatalf("info %v", info)
	}
	exp, _ := s.Expire(context.Background(), &pgshardv1.ExpireRequest{})
	if exp.GetError() != nil || len(exp.GetLog()) != 1 || !strings.HasSuffix(f.calls[len(f.calls)-1], " expire") {
		t.Fatalf("expire %v %v", exp, f.calls)
	}
	f.fail["verify"] = errors.New("exit status 1")
	ver, _ := s.Verify(context.Background(), nil)
	if ver.GetError() == nil || !strings.HasSuffix(f.calls[len(f.calls)-1], " verify") {
		t.Fatalf("verify %v %v", ver, f.calls)
	}
	if err := s.epoch.Accept(1); err != nil {
		t.Fatal(err)
	}
	if exp, _ := s.Expire(context.Background(), &pgshardv1.ExpireRequest{Epoch: 0}); exp.GetError().GetSqlstate() != "55000" {
		t.Fatalf("expire must be fenced: %v", exp)
	}
}

func TestEnsureStanzaLoopRetriesAndSkipsStandby(t *testing.T) {
	s, f := newBackupServer(t, false)
	f.fail["stanza-create"] = errors.New("exit 28")
	f.fail["stanza-upgrade"] = errors.New("exit 28")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.inst.ensureStanzaLoop(ctx, time.Millisecond); close(done) }()
	deadline := time.After(2 * time.Second)
	for f.count() < 4 {
		select {
		case <-deadline:
			t.Fatalf("no retries: %v", f.calls)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	<-done
	s2, f2 := newBackupServer(t, true)
	if err := s2.inst.EnsureStanza(context.Background()); err != nil || len(f2.calls) != 0 {
		t.Fatalf("standby must not touch the stanza: %v %v", err, f2.calls)
	}
}

func lockBusyError(t *testing.T) error {
	t.Helper()
	err := exec.Command("sh", "-c", "exit 50").Run()
	if !backup.LockBusy(err) {
		t.Fatalf("exit 50 must read as a busy lock: %v", err)
	}
	if other := exec.Command("sh", "-c", "exit 28").Run(); backup.LockBusy(other) || backup.LockBusy(errors.New("exit status 50")) {
		t.Fatal("only exit code 50 is a busy lock")
	}
	return err
}

func TestEnsureStanzaLoopRetriesFastWhileTheLockIsBusy(t *testing.T) {
	s, f := newBackupServer(t, false)
	busy := lockBusyError(t)
	f.fail["stanza-create"] = busy
	f.fail["stanza-upgrade"] = busy
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.inst.ensureStanzaLoop(ctx, time.Hour); close(done) }()
	deadline := time.After(5 * time.Second)
	for f.count() < 4 {
		select {
		case <-deadline:
			t.Fatalf("busy lock must retry well before the slow retry: %v", f.calls)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	<-done
}

func TestStanzaWaitBoundsTheFastRetries(t *testing.T) {
	busy := lockBusyError(t)
	n := 0
	for i := 0; i < stanzaLockRetries; i++ {
		if wait, fast := stanzaWait(busy, time.Hour, &n); wait != stanzaLockRetry || !fast {
			t.Fatalf("attempt %d: wait %v fast %v", i, wait, fast)
		}
	}
	if wait, fast := stanzaWait(busy, time.Hour, &n); wait != time.Hour || fast || n != 0 {
		t.Fatalf("after %d busy attempts: wait %v fast %v n %d", stanzaLockRetries, wait, fast, n)
	}
	if wait, fast := stanzaWait(busy, time.Hour, &n); wait != stanzaLockRetry || !fast || n != 1 {
		t.Fatalf("budget must reset: wait %v fast %v n %d", wait, fast, n)
	}
	if wait, fast := stanzaWait(errors.New("exit status 28"), time.Hour, &n); wait != time.Hour || fast || n != 0 {
		t.Fatalf("other errors take the slow path: wait %v fast %v n %d", wait, fast, n)
	}
	if wait, _ := stanzaWait(busy, time.Millisecond, &n); wait != time.Millisecond {
		t.Fatalf("fast retry never exceeds the slow one: %v", wait)
	}
}

// TestOneStanzaWorkerPerPrimaryTerm: the stanza loop retries a repository
// that is unreachable, and an attempt can stay blocked for as long as
// pgBackRest takes to give up. Startup began one and every successful
// promotion began another, with no handle on either, so role cycling during
// repository trouble accumulated loops and the pgBackRest children they
// track -- and a demotion could not end the attempt still running.
func TestOneStanzaWorkerPerPrimaryTerm(t *testing.T) {
	in := newTestInstance(t)
	dir := t.TempDir()
	in.cfg.Role = RolePrimary
	in.cfg.Backup = &backup.Settings{Stanza: "t-catalog-pg18", Repo: backup.Repo{Type: backup.TypePosix},
		ConfigPath: filepath.Join(dir, "pgbackrest.conf"), SpoolPath: filepath.Join(dir, "spool"), LogPath: filepath.Join(dir, "log")}

	// Each attempt blocks until its context ends, so a live worker holds
	// exactly one: the count in flight is the number of workers.
	var running atomic.Int64
	in.newRunner = func(s backup.Settings) *backup.Runner {
		r := backup.NewRunner(s, in.log)
		r.Exec = func(ctx context.Context, _ []string, _ func(string)) error {
			// A repository that never answers, which is what the loop is
			// for and what used to outlive the term that started it.
			running.Add(1)
			defer running.Add(-1)
			<-ctx.Done()
			return ctx.Err()
		}
		return r
	}

	in.rewindFn = func(context.Context, string) error { return nil }
	ctx := context.Background()
	in.startStanzaWorker(ctx, time.Millisecond)
	awaitCount(t, &running, 1)

	// A second term does not add a second worker: the first one is ended,
	// and the attempt in flight is the new one.
	in.startStanzaWorker(ctx, time.Millisecond)
	awaitCount(t, &running, 1)
	time.Sleep(100 * time.Millisecond)
	if n := running.Load(); n != 1 {
		t.Fatalf("%d stanza attempts in flight after a second term, want 1", n)
	}

	// Demotion ends the term, and waits for it.
	if err := in.Demote(ctx, "host=x"); err != nil {
		t.Fatal(err)
	}
	if n := running.Load(); n != 0 {
		t.Fatalf("%d stanza attempts still running after demotion", n)
	}
}

// awaitCount waits for c to settle at want.
func awaitCount(t *testing.T, c *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for c.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("in flight = %d, want %d", c.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestASecondBackupWaitsRatherThanFailingOnTheLock: pgBackRest takes a
// per-stanza lock and refuses a second operation outright, so a backup that
// arrived while another was finishing failed with "unable to acquire lock on
// file ... Resource temporarily unavailable" and the record carrying it went
// to Failed. Seen in CI on a pull request that touched neither backups nor
// the agent: a scheduled incremental started while a full was still running.
func TestASecondBackupWaitsRatherThanFailingOnTheLock(t *testing.T) {
	in := newTestInstance(t)
	dir := t.TempDir()
	in.cfg.Role = RolePrimary
	in.cfg.Backup = &backup.Settings{Stanza: "t-catalog-pg18", Repo: backup.Repo{Type: backup.TypePosix},
		ConfigPath: filepath.Join(dir, "pgbackrest.conf"), SpoolPath: filepath.Join(dir, "spool"), LogPath: filepath.Join(dir, "log")}

	info, err := os.ReadFile(filepath.Join("backup", "testdata", "info.json"))
	if err != nil {
		t.Fatal(err)
	}
	f := &fakePgbackrest{info: string(info), fail: map[string]error{}}

	var concurrent, peak atomic.Int64
	release := make(chan struct{})
	in.newRunner = func(s backup.Settings) *backup.Runner {
		r := backup.NewRunner(s, in.log)
		r.Exec = func(ctx context.Context, args []string, onLine func(string)) error {
			if args[len(args)-1] != "backup" {
				return f.exec(ctx, args, onLine)
			}
			n := concurrent.Add(1)
			defer concurrent.Add(-1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			select {
			case <-release:
				return f.exec(ctx, args, onLine)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return r
	}

	ctx := context.Background()
	done := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := in.Backup(ctx, backup.Incr)
			done <- err
		}()
	}
	// Both are in flight as far as the agent is concerned; only one may be
	// inside pgbackrest.
	time.Sleep(100 * time.Millisecond)
	if n := peak.Load(); n != 1 {
		t.Fatalf("%d pgbackrest operations ran at once; the second would meet the stanza lock", n)
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("backup: %v", err)
		}
	}
	if n := peak.Load(); n != 1 {
		t.Fatalf("peak concurrency %d, want 1", n)
	}
}

// TestABackupThatGivesUpWaitingSaysWhatItWasWaitingFor: the wait is bounded
// by the caller's own context, and what it reports is the reason -- not
// pgBackRest's exit status 50, which named a lock file and nothing else.
func TestABackupThatGivesUpWaitingSaysWhatItWasWaitingFor(t *testing.T) {
	in := newTestInstance(t)
	dir := t.TempDir()
	in.cfg.Role = RolePrimary
	in.cfg.Backup = &backup.Settings{Stanza: "t-catalog-pg18", Repo: backup.Repo{Type: backup.TypePosix},
		ConfigPath: filepath.Join(dir, "pgbackrest.conf"), SpoolPath: filepath.Join(dir, "spool"), LogPath: filepath.Join(dir, "log")}
	held := make(chan struct{})
	in.newRunner = func(s backup.Settings) *backup.Runner {
		r := backup.NewRunner(s, in.log)
		r.Exec = func(ctx context.Context, _ []string, _ func(string)) error {
			close(held)
			<-ctx.Done()
			return ctx.Err()
		}
		return r
	}
	first, stopFirst := context.WithCancel(context.Background())
	go func() { _, _ = in.Backup(first, backup.Full) }()
	<-held

	second, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := in.Backup(second, backup.Incr)
	if err == nil || !strings.Contains(err.Error(), "another operation is still running on this stanza") {
		t.Fatalf("the second backup reported: %v", err)
	}
	stopFirst()
}
