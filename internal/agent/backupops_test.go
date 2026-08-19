package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
