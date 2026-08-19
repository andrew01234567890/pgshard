package agent

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestInstance(t *testing.T) *Instance {
	t.Helper()
	c := testConfig()
	c.PGData = filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(filepath.Join(c.PGData, "pg_replslot", "old_slot"), 0o700); err != nil {
		t.Fatal(err)
	}
	c.PasswordFile = filepath.Join(t.TempDir(), "pw")
	_ = os.WriteFile(c.PasswordFile, []byte("secret\n"), 0o600)
	log := slog.New(slog.DiscardHandler)
	sup := NewSupervisor(t.TempDir(), c.PGData, log)
	ep, err := OpenEpochStore(c.PGData)
	if err != nil {
		t.Fatal(err)
	}
	in := NewInstance(c, sup, ep, log)
	in.startFn = func(context.Context) error { return nil }
	in.slotFn = func(context.Context, string) error { return nil }
	in.waitSourceFn = func(context.Context, string) error { return nil }
	return in
}

func TestDemoteFallsBackToRecloneWhenRewindFails(t *testing.T) {
	in := newTestInstance(t)
	var rewound, recloned []string
	in.rewindFn = func(_ context.Context, src string) error {
		rewound = append(rewound, src)
		return errors.New("no common ancestor")
	}
	in.recloneFn = func(context.Context) error { recloned = append(recloned, "x"); return nil }
	if err := in.Demote(context.Background(), "host=new"); err != nil {
		t.Fatal(err)
	}
	if len(rewound) != 1 || rewound[0] != "host=new" || len(recloned) != 1 {
		t.Fatalf("rewound=%v recloned=%v", rewound, recloned)
	}
	if !in.IsStandby() {
		t.Fatal("standby.signal missing after demote")
	}
	if entries, _ := os.ReadDir(filepath.Join(in.cfg.PGData, "pg_replslot")); len(entries) != 0 {
		t.Fatalf("stale slots not dropped: %v", entries)
	}
	pg, _ := os.ReadFile(filepath.Join(in.cfg.PGData, postgresqlConf))
	if string(pg) != RenderPostgresqlConf(in.cfg, true) {
		t.Fatal("standby config not rendered")
	}
}

func TestDemoteUsesConfiguredSourceAndSkipsRecloneOnSuccess(t *testing.T) {
	in := newTestInstance(t)
	var src, slotSrc string
	recloned := false
	in.rewindFn = func(_ context.Context, s string) error { src = s; return nil }
	in.recloneFn = func(context.Context) error { recloned = true; return nil }
	in.slotFn = func(_ context.Context, s string) error { slotSrc = s; return nil }
	if err := in.Demote(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if src != in.cfg.PrimaryConninfo || recloned || slotSrc != src {
		t.Fatalf("src=%q recloned=%v slotSrc=%q", src, recloned, slotSrc)
	}
}

func TestDemoteReportsRecloneFailure(t *testing.T) {
	in := newTestInstance(t)
	in.rewindFn = func(context.Context, string) error { return errors.New("rewind boom") }
	in.recloneFn = func(context.Context) error { return errors.New("clone boom") }
	err := in.Demote(context.Background(), "")
	if err == nil || err.Error() != "reclone after failed rewind: clone boom" {
		t.Fatalf("err=%v", err)
	}
}

func TestPromoteRefusesOnPrimary(t *testing.T) {
	in := newTestInstance(t)
	if err := in.Promote(context.Background()); err == nil {
		t.Fatal("promote on a primary must fail")
	}
}

func TestBootstrapNoopOnExistingClusterRendersConfig(t *testing.T) {
	in := newTestInstance(t)
	_ = os.WriteFile(filepath.Join(in.cfg.PGData, "PG_VERSION"), []byte("18\n"), 0o600)
	_ = os.WriteFile(filepath.Join(in.cfg.PGData, standbySignal), nil, 0o600)
	if err := in.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	pg, _ := os.ReadFile(filepath.Join(in.cfg.PGData, postgresqlConf))
	if string(pg) != RenderPostgresqlConf(in.cfg, true) {
		t.Fatal("expected standby config")
	}
	pgpass, err := os.ReadFile(in.pgpassPath())
	if err != nil || string(pgpass) != "*:*:*:postgres:secret\n" {
		t.Fatalf("pgpass=%q err=%v", pgpass, err)
	}
}

func TestBootstrapRejoinsFormerPrimaryAsStandby(t *testing.T) {
	in := newTestInstance(t)
	in.cfg.Role = RoleStandby
	_ = os.WriteFile(filepath.Join(in.cfg.PGData, "PG_VERSION"), []byte("18\n"), 0o600)
	var rewound, waited string
	in.waitSourceFn = func(_ context.Context, src string) error {
		if rewound != "" {
			t.Fatal("must wait for the source before rewinding")
		}
		waited = src
		return nil
	}
	in.rewindFn = func(_ context.Context, src string) error { rewound = src; return nil }
	in.recloneFn = func(context.Context) error { t.Fatal("reclone must not run when rewind succeeds"); return nil }
	if err := in.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rewound != in.cfg.PrimaryConninfo || waited != rewound {
		t.Fatalf("rewound against %q waited %q", rewound, waited)
	}
	if !in.IsStandby() {
		t.Fatal("standby.signal missing after rejoin")
	}
	if entries, _ := os.ReadDir(filepath.Join(in.cfg.PGData, "pg_replslot")); len(entries) != 0 {
		t.Fatalf("stale slots not dropped: %v", entries)
	}
	pg, _ := os.ReadFile(filepath.Join(in.cfg.PGData, postgresqlConf))
	if string(pg) != RenderPostgresqlConf(in.cfg, true) {
		t.Fatal("standby config not rendered")
	}
}

func TestBootstrapKeepsExistingPrimaryWhenRolePrimary(t *testing.T) {
	in := newTestInstance(t)
	in.cfg.Role = RolePrimary
	_ = os.WriteFile(filepath.Join(in.cfg.PGData, "PG_VERSION"), []byte("18\n"), 0o600)
	in.rewindFn = func(context.Context, string) error { t.Fatal("rewind must not run"); return nil }
	if err := in.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if in.IsStandby() {
		t.Fatal("primary must stay a primary")
	}
}

func TestBootstrapRetriesCloneUntilPrimaryAnswers(t *testing.T) {
	in := newTestInstance(t)
	in.cfg.Role = RoleStandby
	in.cloneRetry = time.Millisecond
	attempts := 0
	in.recloneFn = func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("connection refused")
		}
		return os.WriteFile(filepath.Join(in.cfg.PGData, standbySignal), nil, 0o600)
	}
	if err := in.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d", attempts)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	in.recloneFn = func(context.Context) error { return errors.New("still refused") }
	_ = os.Remove(filepath.Join(in.cfg.PGData, "PG_VERSION"))
	if err := in.Bootstrap(ctx); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled bootstrap must stop retrying: %v", err)
	}
}
