package agent

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
	if standby, err := in.IsStandby(); err != nil || !standby {
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

// TestARejoinRetriedAfterItsRewindResumesAfterIt: pg_rewind succeeds and a
// step after it fails -- here the slot on the source, which is a network
// call. The retry used to start from the top and point pg_rewind at a
// target it had already rewound, whose control file is no longer a clean
// shutdown; any failure there fell back to a full reclone, turning seconds
// of rewind into a copy of the whole volume.
func TestARejoinRetriedAfterItsRewindResumesAfterIt(t *testing.T) {
	in := newTestInstance(t)
	var rewinds, reclones, slots int
	in.rewindFn = func(context.Context, string) error { rewinds++; return nil }
	in.recloneFn = func(context.Context) error { reclones++; return nil }
	in.slotFn = func(context.Context, string) error {
		slots++
		if slots == 1 {
			return errors.New("source unreachable")
		}
		return nil
	}

	if err := in.Demote(context.Background(), "host=new"); err == nil {
		t.Fatal("the first attempt was meant to fail after the rewind")
	}
	if err := in.Demote(context.Background(), "host=new"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if rewinds != 1 {
		t.Errorf("pg_rewind ran %d times; the retry repeated a rewind that had already completed", rewinds)
	}
	if reclones != 0 {
		t.Errorf("recloned %d times after a rewind that succeeded", reclones)
	}
	if slots != 2 {
		t.Errorf("slot step ran %d times, want the failed attempt and the retry", slots)
	}
	if standby, err := in.IsStandby(); err != nil || !standby {
		t.Fatal("standby.signal missing after the resumed rejoin")
	}
	// Started, so the marker is spent: a later term's rejoin needs its own
	// rewind.
	if rewound, err := in.rewoundFor("host=new"); err != nil || rewound {
		t.Fatalf("the rewind marker outlived the start it was for: rewound=%v err=%v", rewound, err)
	}
}

// TestARewindMarkerForAnotherSourceIsNotUsed: a rewind makes a target
// consistent with ONE source. A retry pointed at a different primary --
// failover moved on while the rejoin was failing -- has to rewind again.
func TestARewindMarkerForAnotherSourceIsNotUsed(t *testing.T) {
	in := newTestInstance(t)
	if err := in.markRewound("host=old"); err != nil {
		t.Fatal(err)
	}
	var rewoundAgainst []string
	in.rewindFn = func(_ context.Context, src string) error { rewoundAgainst = append(rewoundAgainst, src); return nil }
	in.recloneFn = func(context.Context) error { t.Fatal("recloned"); return nil }
	if err := in.Demote(context.Background(), "host=new"); err != nil {
		t.Fatal(err)
	}
	if len(rewoundAgainst) != 1 || rewoundAgainst[0] != "host=new" {
		t.Fatalf("rewound against %v; a marker for host=old must not stand in for a rewind against host=new", rewoundAgainst)
	}
}

// TestAFailedStartKeepsTheRewindMarker: the marker is spent only once
// postgres has come up on the rewound directory. A start that fails leaves
// nothing run on it, so the retry must still skip the rewind.
func TestAFailedStartKeepsTheRewindMarker(t *testing.T) {
	in := newTestInstance(t)
	in.rewindFn = func(context.Context, string) error { return nil }
	starts := 0
	in.startFn = func(context.Context) error {
		starts++
		if starts == 1 {
			return errors.New("postgres exited during startup")
		}
		return nil
	}
	if err := in.Demote(context.Background(), "host=new"); err == nil {
		t.Fatal("the first start was meant to fail")
	}
	if rewound, err := in.rewoundFor("host=new"); err != nil || !rewound {
		t.Fatalf("a failed start cleared the rewind marker: rewound=%v err=%v", rewound, err)
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
	if standby, err := in.IsStandby(); err != nil || !standby {
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
	if standby, err := in.IsStandby(); err != nil || standby {
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

func TestPromotionPendingMarkerLifecycle(t *testing.T) {
	in := newTestInstance(t)
	if in.PromotionPending() {
		t.Fatal("fresh instance must not report a pending promotion")
	}
	if err := in.setPromotionPending(); err != nil {
		t.Fatal(err)
	}
	if !in.PromotionPending() {
		t.Fatal("marker set but not reported")
	}
	// Clearing is idempotent: a second clear on an absent marker is a no-op.
	for i := 0; i < 2; i++ {
		if err := in.clearPromotionPending(); err != nil {
			t.Fatalf("clear %d: %v", i, err)
		}
	}
	if in.PromotionPending() {
		t.Fatal("marker still reported after clear")
	}
}

// TestACloneDoesNotEmptyPGDATABeforeItHasReachedTheSource: a rewind that
// failed because the primary was unreachable falls through to a full clone,
// and the clone emptied PGDATA before it had contacted anything. The clone
// then failed for the same reason, leaving the member with no data at all
// where waiting would have cost only time.
func TestACloneDoesNotEmptyPGDATABeforeItHasReachedTheSource(t *testing.T) {
	in := newTestInstance(t)
	// A source that nothing is listening on: the address is unroutable, so
	// the connection attempt fails rather than hanging for its timeout.
	in.cfg.PrimaryConninfo = "host=127.0.0.1 port=1 user=postgres dbname=postgres connect_timeout=1"
	marker := filepath.Join(in.cfg.PGData, "PG_VERSION")
	if err := os.WriteFile(marker, []byte("18\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := in.baseBackup(context.Background()); err == nil {
		t.Fatal("a clone from an unreachable source must fail")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("PGDATA was emptied before the source was reached: %v", err)
	}
}

// The password a standby streams with lands in the pgpass file, because
// pg_basebackup and pg_rewind take it from there: a --dbname carrying a
// password would put it in argv, which /proc exposes to anything on the
// node. The superuser's entry stays -- pgbackrest and the schema copy still
// reach the local server as postgres.
func TestPgpassCarriesTheReplicationPasswordToo(t *testing.T) {
	in := newTestInstance(t)
	pwFile := filepath.Join(t.TempDir(), "replication")
	// A colon and a backslash: .pgpass ends its fields on the first and
	// escapes with the second, so an unescaped password reads as the wrong
	// field and silently authenticates nothing.
	if err := os.WriteFile(pwFile, []byte(`a:b\c`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	in.cfg.ReplicationPasswordFile = pwFile
	if err := in.writePgpass(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(in.pgpassPath())
	if err != nil {
		t.Fatal(err)
	}
	want := "*:*:*:postgres:secret\n" + `*:*:*:pgshard_replication:a\:b\\c` + "\n"
	if string(got) != want {
		t.Fatalf("pgpass = %q, want %q", got, want)
	}

	// An unreadable or empty file is a startup failure, not a member that
	// silently streams as nobody.
	in.cfg.ReplicationPasswordFile = filepath.Join(t.TempDir(), "absent")
	if err := in.writePgpass(); err == nil {
		t.Error("a missing replication password file was accepted")
	}
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	in.cfg.ReplicationPasswordFile = empty
	if err := in.writePgpass(); err == nil {
		t.Error("an empty replication password file was accepted")
	}
}

// The credential sent to the source must belong to the role the conninfo
// names, and it must never be part of the connection STRING: pgx puts that
// whole string into a parse error, and its redaction of password='...'
// stops at the first quote inside the value. A member reaches its primary with primary_conninfo for three
// different things -- waiting for it to come up, creating this member's
// slot on it, and cloning -- and each builds its own connection string.
// Splicing the superuser's password onto a conninfo that says
// user=pgshard_replication authenticates nothing, and it fails at the
// moment a member is trying to rejoin, which is the worst moment to find
// out.
func TestTheSourcePasswordBelongsToTheRoleTheConninfoNames(t *testing.T) {
	in := newTestInstance(t)
	pwFile := filepath.Join(t.TempDir(), "replication")
	if err := os.WriteFile(pwFile, []byte("streamer\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	in.cfg.ReplicationPasswordFile = pwFile

	for _, c := range []struct{ source, want string }{
		{"host=src port=5432 user=" + replicationRole, "streamer"},
		{"host=src port=5432 user=" + superuserRole, "secret"},
		// No user is the libpq default, which in a member's environment is
		// the superuser.
		{"host=src port=5432", "secret"},
	} {
		cfg, err := in.sourceConfig(c.source)
		if err != nil {
			t.Fatalf("%s: %v", c.source, err)
		}
		if cfg.Password != c.want {
			t.Errorf("%s: password %q, want %q", c.source, cfg.Password, c.want)
		}
		if strings.Contains(cfg.ConnString(), c.want) {
			t.Errorf("%s: the password is in the connection string %q, which pgx puts into a parse error",
				c.source, cfg.ConnString())
		}
	}

	// A member whose operator predates the replication Secret still has
	// only the superuser's password, and must keep working.
	in.cfg.ReplicationPasswordFile = ""
	cfg, err := in.sourceConfig("host=src user=" + replicationRole)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Password != "secret" {
		t.Errorf("without a replication Secret the password is %q, want the superuser's", cfg.Password)
	}
}

// A conninfo with no dbname does not mean "the default database": libpq and
// pgx leave it to the server, and the server uses THE USER NAME. So the
// moment primary_conninfo stopped saying user=postgres, every ordinary
// connection to a source started asking for a database called
// pgshard_replication, which does not exist -- a clone that never
// bootstraps, a rejoin that never rewinds, a slot that is never created.
// It cost nothing for as long as the two names happened to coincide.
func TestASourceConnectionNamesADatabaseThatExists(t *testing.T) {
	in := newTestInstance(t)
	for _, c := range []struct{ source, want string }{
		{"host=src user=" + replicationRole, "postgres"},
		{"host=src user=" + superuserRole, "postgres"},
		{"host=src user=" + replicationRole + " dbname=app", "app"},
	} {
		cfg, err := in.sourceConfig(c.source)
		if err != nil {
			t.Fatalf("%s: %v", c.source, err)
		}
		if cfg.Database != c.want {
			t.Errorf("%s: database %q, want %q", c.source, cfg.Database, c.want)
		}
	}
	// pg_rewind takes a string rather than a config and libpq defaults it
	// the same way, so that path needs the same treatment.
	if got := withDatabase("host=src user=" + replicationRole); !strings.Contains(got, "dbname=postgres") {
		t.Errorf("pg_rewind source = %q, want a dbname", got)
	}
	if got := withDatabase("host=src dbname=app"); strings.Contains(got, "dbname=postgres") {
		t.Errorf("an explicit dbname was overridden: %q", got)
	}
}

// PGS-736. pg_rewind must be allowed to fix an unclean shutdown, or an
// unplanned failover can never rewind.
//
// --no-ensure-shutdown skips the single-user recovery pg_rewind runs on a
// target that crashed, and pg_rewind then refuses any target whose control
// file is not DB_SHUTDOWNED or DB_SHUTDOWNED_IN_RECOVERY. Fencing deletes
// the old primary's Pod with podFenceGrace -- ten seconds -- while the
// agent's own stop budget is three times ShutdownTimeout, so the SIGKILL
// lands mid-shutdown and the target crashed. The flag therefore turned
// "rewind, falling back to a full reclone" into "always reclone", and on a
// large shard that runs into the startup probe.
//
// The argument list is the whole of the decision, which is why it is
// asserted directly: nothing else in the agent can tell these two
// behaviours apart, because pg_rewind is what differs.
func TestRewindMayFixAnUncleanShutdown(t *testing.T) {
	in := newTestInstance(t)
	for _, arg := range in.rewindArgs("host=new") {
		if arg == "--no-ensure-shutdown" {
			t.Fatal("--no-ensure-shutdown makes pg_rewind refuse a crashed target, so an unplanned failover always recloses instead of rewinding")
		}
	}
	if !hasPrefixArg(in.rewindArgs("host=new"), "--source-server=") {
		t.Errorf("args = %v, want the source server", in.rewindArgs("host=new"))
	}
}

// --restore-target-wal is passed only when a restore_command exists to
// fetch the WAL with: pg_rewind refuses the flag without one.
func TestRewindAsksForArchivedWALOnlyWhenItCanFetchIt(t *testing.T) {
	in := newTestInstance(t)
	if hasPrefixArg(in.rewindArgs("host=new"), "--restore-target-wal") {
		t.Error("no restore_command is configured, so there is nothing to restore the WAL with")
	}
	in.cfg.Postgres.RestoreCommand = "cp /archive/%f %p"
	if !hasPrefixArg(in.rewindArgs("host=new"), "--restore-target-wal") {
		t.Error("a restore_command is configured and the WAL the rewind needs may only be in the archive")
	}
}

// hasPrefixArg reports whether any arg starts with prefix.
func hasPrefixArg(args []string, prefix string) bool {
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			return true
		}
	}
	return false
}
