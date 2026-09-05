package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// fakeBarrierStore is the catalog side: the fence, the decision log
// counters and the recorded restore points, with a journal of every step.
type fakeBarrierStore struct {
	mu        sync.Mutex
	fenced    bool
	fencedAt  time.Time
	preparing int
	// clearPreparingAfter counts down on each read and zeroes preparing
	// when it reaches zero, so a test can say "in flight for the first N
	// polls" rather than "in flight for N milliseconds" and race the
	// poller on a loaded machine.
	clearPreparingAfter int
	watermark           int64
	recorded            []RestorePoint
	owner               string
	reserved            []string
	failed              map[string]string
	journal             *[]string
	fail                map[string]error
	now                 func() time.Time
}

func (s *fakeBarrierStore) log(step string) {
	*s.journal = append(*s.journal, step)
}

func (s *fakeBarrierStore) Lock(context.Context) (func(), error) { return func() {}, nil }

func (s *fakeBarrierStore) Fence(_ context.Context, active bool, reason, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail["fence"]; err != nil && active {
		return err
	}
	if err := s.fail["release"]; err != nil && !active {
		return err
	}
	s.fenced = active
	if active {
		s.owner = owner
		s.fencedAt = s.now()
		s.log("fence " + reason)
	} else {
		s.log("release")
	}
	return nil
}

func (s *fakeBarrierStore) FencedAt(context.Context) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.fenced {
		return time.Time{}, errors.New("not fenced")
	}
	return s.fencedAt, nil
}

func (s *fakeBarrierStore) PreparingCount(context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail["preparing"]; err != nil {
		return 0, err
	}
	s.log(fmt.Sprintf("preparing=%d", s.preparing))
	n := s.preparing
	if s.clearPreparingAfter > 0 {
		s.clearPreparingAfter--
		if s.clearPreparingAfter == 0 {
			s.preparing = 0
		}
	}
	return n, nil
}

func (s *fakeBarrierStore) DecisionWatermark(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watermark, nil
}

func (s *fakeBarrierStore) Exists(_ context.Context, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.recorded {
		if r.Name == name {
			return true, nil
		}
	}
	for _, n := range s.reserved {
		if n == name {
			return true, nil
		}
	}
	return false, nil
}

// Reserve claims the name; a second reservation of a claimed name fails the
// way the unique constraint does in PostgreSQL.
func (s *fakeBarrierStore) Reserve(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail["reserve"]; err != nil {
		return err
	}
	for _, n := range s.reserved {
		if n == name {
			return errors.New("duplicate key value violates unique constraint")
		}
	}
	s.reserved = append(s.reserved, name)
	s.log("reserve " + name)
	return nil
}

func (s *fakeBarrierStore) Fail(_ context.Context, name, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed == nil {
		s.failed = map[string]string{}
	}
	s.failed[name] = reason
	s.log("fail " + name)
	return nil
}

func (s *fakeBarrierStore) Record(_ context.Context, rp RestorePoint) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail["record"]; err != nil {
		return "", err
	}
	if !s.fenced {
		return "", errors.New("recorded after the fence was released")
	}
	rp.ID = fmt.Sprintf("id-%d", len(s.recorded)+1)
	s.recorded = append(s.recorded, rp)
	s.log("record " + rp.Name)
	return rp.ID, nil
}

func (s *fakeBarrierStore) ShardMapGeneration(context.Context) (int64, error) { return 42, nil }

func (s *fakeBarrierStore) FenceRaised(context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fenced, nil
}

func (s *fakeBarrierStore) FenceOwnedBy(_ context.Context, owner string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fenced && s.owner == owner, nil
}

// fakeGroups is the catalog and two shards with their prepared transaction
// counts and archives.
type fakeGroups struct {
	mu       sync.Mutex
	prepared map[string]int
	// preparedGIDs names the transactions a group holds; nil means they are
	// generated from prepared, so a test that only cares about the count
	// does not have to name them.
	preparedGIDs map[string][]string
	// clearPreparedAfter counts down per group on each read, so a test can
	// hold a shard in flight for a number of polls rather than a duration.
	clearPreparedAfter map[string]int
	archived           map[string]string
	points             map[string][]string
	journal            *[]string
	fail               map[string]error
	// archiveAfter is how many ArchivedThrough polls a group answers with
	// an old segment before the restore point's segment shows up.
	archiveAfter map[string]int
	polls        map[string]int
	store        *fakeBarrierStore
	// onRestorePoint runs on every CreateRestorePoint, before the point.
	onRestorePoint func()
	// paused records which groups currently refuse writes, and writers how
	// many write transactions each still has in flight.
	paused  map[string]bool
	writers map[string]int
	subs    map[string]int
}

func (g *fakeGroups) List(context.Context) ([]GroupRef, error) {
	if err := g.fail["list"]; err != nil {
		return nil, err
	}
	return []GroupRef{{Name: CatalogGroup}, {Name: "shard0", Set: "default", ID: 0}, {Name: "shard1", Set: "default", ID: 1}}, nil
}

func (g *fakeGroups) PreparedGIDs(_ context.Context, ref GroupRef) ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.fail["prepared:"+ref.Name]; err != nil {
		return nil, err
	}
	*g.journal = append(*g.journal, fmt.Sprintf("prepared %s=%d", ref.Name, g.prepared[ref.Name]))
	n := g.prepared[ref.Name]
	if g.clearPreparedAfter[ref.Name] > 0 {
		g.clearPreparedAfter[ref.Name]--
		if g.clearPreparedAfter[ref.Name] == 0 {
			g.prepared[ref.Name] = 0
		}
	}
	if names := g.preparedGIDs[ref.Name]; names != nil {
		return names, nil
	}
	gids := make([]string, n)
	for i := range gids {
		gids[i] = fmt.Sprintf("pgshard-%s-%d", ref.Name, i)
	}
	return gids, nil
}

func (g *fakeGroups) CreateRestorePoint(_ context.Context, ref GroupRef, name string) (RestorePointResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.fail["restorepoint:"+ref.Name]; err != nil {
		return RestorePointResult{}, err
	}
	if g.store != nil && !g.store.fenced {
		return RestorePointResult{}, errors.New("restore point created without the fence")
	}
	if g.onRestorePoint != nil {
		g.onRestorePoint()
	}
	g.points[ref.Name] = append(g.points[ref.Name], name)
	n := len(g.points[ref.Name])
	*g.journal = append(*g.journal, fmt.Sprintf("restorepoint %s %s", ref.Name, name))
	return RestorePointResult{LSN: uint64(1000*n + int(ref.ID)), Timeline: 1 + int64(ref.ID), WALSegment: fmt.Sprintf("00000001000000000000000%d", n)}, nil
}

func (g *fakeGroups) SubscriptionCount(_ context.Context, ref GroupRef) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.fail["subs:"+ref.Name]; err != nil {
		return 0, err
	}
	return g.subs[ref.Name], nil
}

func (g *fakeGroups) PauseEffective(_ context.Context, ref GroupRef) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.fail["effective:"+ref.Name]; err != nil {
		return false, err
	}
	return g.paused[ref.Name], nil
}

func (g *fakeGroups) PauseWrites(_ context.Context, ref GroupRef, pause bool) (time.Time, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := "pause"
	if !pause {
		key = "resume"
	}
	if err := g.fail[key+":"+ref.Name]; err != nil {
		return time.Time{}, err
	}
	g.paused[ref.Name] = pause
	*g.journal = append(*g.journal, key+" "+ref.Name)
	return time.Unix(0, 0), nil
}

func (g *fakeGroups) WritersSince(_ context.Context, ref GroupRef, _ time.Time) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.fail["writers:"+ref.Name]; err != nil {
		return 0, err
	}
	n := g.writers[ref.Name]
	*g.journal = append(*g.journal, fmt.Sprintf("writers %s=%d", ref.Name, n))
	// A paused group drains: each poll finishes one writer.
	if n > 0 {
		g.writers[ref.Name] = n - 1
	}
	return n, nil
}

func (g *fakeGroups) ArchivedThrough(_ context.Context, ref GroupRef) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.fail["archive:"+ref.Name]; err != nil {
		return "", err
	}
	g.polls[ref.Name]++
	if g.polls[ref.Name] <= g.archiveAfter[ref.Name] {
		*g.journal = append(*g.journal, "archive-wait "+ref.Name)
		return "000000010000000000000000", nil
	}
	*g.journal = append(*g.journal, "archived "+ref.Name)
	return g.archived[ref.Name], nil
}

type barrierFixture struct {
	store   *fakeBarrierStore
	groups  *fakeGroups
	journal []string
	clock   time.Time
	b       *Barrier
}

func newBarrierFixture() *barrierFixture {
	f := &barrierFixture{clock: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)}
	now := func() time.Time { f.clock = f.clock.Add(time.Millisecond); return f.clock }
	f.store = &fakeBarrierStore{journal: &f.journal, fail: map[string]error{}, now: now}
	f.groups = &fakeGroups{prepared: map[string]int{}, clearPreparedAfter: map[string]int{}, archived: map[string]string{}, points: map[string][]string{},
		journal: &f.journal, fail: map[string]error{}, archiveAfter: map[string]int{}, polls: map[string]int{}, store: f.store,
		paused: map[string]bool{}, writers: map[string]int{}, subs: map[string]int{}}
	for _, g := range []string{CatalogGroup, "shard0", "shard1"} {
		f.groups.archived[g] = "000000010000000000000009"
	}
	// FenceSettle is the one real-time wait in a barrier; these tests drive
	// a fake clock, so they turn it off and TestABarrierLetsTheFenceReach
	// TheRouters covers it on its own.
	f.b = &Barrier{Store: f.store, Groups: f.groups, Now: now, Poll: time.Millisecond, DrainTimeout: 50 * time.Millisecond, ArchiveTimeout: 50 * time.Millisecond, FenceSettle: -1}
	return f
}

func TestBarrierHappyPathStepOrder(t *testing.T) {
	f := newBarrierFixture()
	f.groups.archiveAfter["shard1"] = 2
	rp, err := f.b.Run(context.Background(), "nightly-1")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"reserve nightly-1",
		"fence barrier nightly-1",
		"pause shard0",
		"pause shard1",
		"preparing=0",
		"prepared catalog=0",
		"prepared shard0=0",
		"prepared shard1=0",
		"writers shard0=0",
		"writers shard1=0",
		"preparing=0",
		"prepared catalog=0",
		"prepared shard0=0",
		"prepared shard1=0",
		"writers shard0=0",
		"writers shard1=0",
		"restorepoint catalog pgshard-nightly-1",
		"restorepoint shard0 pgshard-nightly-1",
		"restorepoint shard1 pgshard-nightly-1",
		"archived catalog",
		"archived shard0",
		"archive-wait shard1",
		"archive-wait shard1",
		"archived shard1",
		"writers shard0=0",
		"writers shard1=0",
		"record nightly-1",
		"resume shard0",
		"resume shard1",
		"release",
	}
	if strings.Join(f.journal, "\n") != strings.Join(want, "\n") {
		t.Fatalf("journal:\n%s\nwant:\n%s", strings.Join(f.journal, "\n"), strings.Join(want, "\n"))
	}
	if rp.ID != "id-1" || !rp.Certified || rp.ShardMapGeneration != 42 || len(rp.Groups) != 3 || rp.CreatedAt.IsZero() {
		t.Fatalf("restore point %+v", rp)
	}
	if rp.Groups[2] != (GroupRestorePoint{Group: "shard1", LSN: 1001, Timeline: 2, WALSegment: "000000010000000000000001"}) {
		t.Fatalf("shard1 point %+v", rp.Groups[2])
	}
	if f.store.fenced {
		t.Fatal("fence left raised")
	}
	if _, err := f.b.Run(context.Background(), "nightly-1"); !errors.Is(err, ErrBarrierExists) {
		t.Fatalf("duplicate name: %v", err)
	}
	if _, err := f.b.Run(context.Background(), "Bad Name"); !errors.Is(err, ErrBarrierName) {
		t.Fatalf("bad name: %v", err)
	}
	if len(f.journal) != len(want) {
		t.Fatalf("refused runs touched the cluster: %v", f.journal[len(want):])
	}
}

func TestBarrierDrainWaitsForInFlightTransactions(t *testing.T) {
	f := newBarrierFixture()
	// In flight for exactly one poll each, cleared by the read itself. A
	// goroutine clearing these on a timer raced the poller: on a loaded
	// machine the first poll could land after the timer, so the journal
	// never recorded the in-flight state the assertions look for.
	f.store.preparing, f.store.clearPreparingAfter = 1, 1
	f.groups.prepared["shard0"], f.groups.clearPreparedAfter["shard0"] = 2, 1
	f.b.DrainTimeout = 2 * time.Second
	if _, err := f.b.Run(context.Background(), "b"); err != nil {
		t.Fatalf("drain: %v\n%s", err, strings.Join(f.journal, "\n"))
	}
	j := strings.Join(f.journal, "\n")
	if !strings.Contains(j, "preparing=1") || !strings.Contains(j, "prepared shard0=2") || strings.Index(j, "prepared shard0=0") > strings.Index(j, "restorepoint catalog") {
		t.Fatalf("journal:\n%s", j)
	}
	if !strings.Contains(j, "preparing=1\npreparing=") {
		t.Fatalf("groups were polled while decision rows were preparing:\n%s", j)
	}
}

func TestBarrierFailuresReleaseTheFence(t *testing.T) {
	cases := []struct {
		name string
		arm  func(f *barrierFixture)
		want string
	}{
		{"drain timeout", func(f *barrierFixture) { f.store.preparing = 1 }, "drain: still in flight after 50ms: 1 decision row(s) preparing"},
		{"prepared on a shard", func(f *barrierFixture) { f.groups.prepared["shard1"] = 3 }, "3 prepared transaction(s) on shard1"},
		{"prepared count error", func(f *barrierFixture) { f.groups.fail["prepared:shard0"] = errors.New("shard0 down") }, "drain: shard0: shard0 down"},
		// The resolver finishes only what pgshard coordinated, so a
		// transaction somebody else prepared blocks every barrier until a
		// human ends it. Naming it is the difference between a drain an
		// operator can act on and one that just says "1".
		{"a foreign prepared transaction is named", func(f *barrierFixture) {
			f.groups.prepared["shard1"] = 1
			f.groups.preparedGIDs = map[string][]string{"shard1": {"someone-elses-2pc"}}
		}, "1 prepared transaction(s) on shard1: someone-elses-2pc"},
		{"restore point error", func(f *barrierFixture) { f.groups.fail["restorepoint:shard1"] = errors.New("read only") }, "restore point on shard1: read only"},
		{"archive error", func(f *barrierFixture) { f.groups.fail["archive:catalog"] = errors.New("archive_mode is off") }, "archive of catalog: archive_mode is off"},
		{"archive timeout", func(f *barrierFixture) { f.groups.archiveAfter["shard0"] = 1 << 20 }, "archive of shard0: 000000010000000000000001 not archived after 50ms"},
		{"late transactions", func(f *barrierFixture) {
			var once sync.Once
			f.groups.onRestorePoint = func() {
				once.Do(func() { f.store.mu.Lock(); f.store.watermark += 2; f.store.mu.Unlock() })
			}
		}, "2 two-phase transaction(s) started while the fence was up"},
		{"record error", func(f *barrierFixture) { f.store.fail["record"] = errors.New("disk full") }, "record: disk full"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newBarrierFixture()
			c.arm(f)
			_, err := f.b.Run(context.Background(), "b")
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err %v, want %q", err, c.want)
			}
			if f.store.fenced {
				t.Fatal("fence left raised")
			}
			if len(f.store.recorded) != 0 {
				t.Fatal("restore point recorded despite the failure")
			}
			// The name is claimed before anything is touched and the failed
			// attempt is recorded, with the fence released in between.
			if f.journal[0] != "reserve b" || f.journal[len(f.journal)-1] != "fail b" {
				t.Fatalf("journal %v", f.journal)
			}
			if !slices.Contains(f.journal, "fence barrier b") || !slices.Contains(f.journal, "release") {
				t.Fatalf("journal %v", f.journal)
			}
			// A retry of the same name is refused: the physical WAL restore
			// point of that name may already exist on a touched group.
			if _, err := f.b.Run(context.Background(), "b"); !errors.Is(err, ErrBarrierExists) {
				t.Fatalf("retry of a burnt name: %v", err)
			}
		})
	}
}

func TestBarrierNeverFencesWhenItCannotStart(t *testing.T) {
	f := newBarrierFixture()
	f.groups.fail["list"] = errors.New("catalog unreachable")
	if _, err := f.b.Run(context.Background(), "b"); err == nil || !strings.Contains(err.Error(), "groups: catalog unreachable") {
		t.Fatalf("err %v", err)
	}
	if slices.Contains(f.journal, "fence barrier b") {
		t.Fatalf("journal %v", f.journal)
	}
	f = newBarrierFixture()
	f.store.fail["fence"] = errors.New("read only catalog")
	if _, err := f.b.Run(context.Background(), "b"); err == nil || !strings.Contains(err.Error(), "fence: read only catalog") {
		t.Fatalf("err %v", err)
	}
	if slices.Contains(f.journal, "restorepoint") {
		t.Fatalf("journal %v", f.journal)
	}
}

func TestBarrierReportsAFenceThatWouldNotRelease(t *testing.T) {
	f := newBarrierFixture()
	f.store.fail["release"] = errors.New("catalog gone")
	_, err := f.b.Run(context.Background(), "b")
	if err == nil || !strings.Contains(err.Error(), "release fence: catalog gone") {
		t.Fatalf("err %v", err)
	}
	if len(f.store.recorded) != 1 {
		t.Fatal("the restore point itself was fine and must be recorded")
	}
}

func TestBarrierCancellationReleasesTheFence(t *testing.T) {
	f := newBarrierFixture()
	f.store.preparing = 1
	f.b.DrainTimeout = time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	_, err := f.b.Run(ctx, "b")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	if f.store.fenced {
		t.Fatal("fence left raised after cancellation")
	}
}

// TestBarrierToleratesDecisionsSettledDuringDrain: two-phase commits that
// began before every router observed the fence and finished during the
// drain must not invalidate the restore points.
func TestBarrierToleratesDecisionsSettledDuringDrain(t *testing.T) {
	f := newBarrierFixture()
	f.store.preparing = 1
	go func() {
		time.Sleep(10 * time.Millisecond)
		f.store.mu.Lock()
		f.store.watermark += 5
		f.store.preparing = 0
		f.store.mu.Unlock()
	}()
	f.b.DrainTimeout = 2 * time.Second
	if _, err := f.b.Run(context.Background(), "settled"); err != nil {
		t.Fatalf("decisions that settled during the drain must not fail the barrier: %v\n%s", err, strings.Join(f.journal, "\n"))
	}
}

// TestBarrierDrainsOrdinaryWriters: writes that started before the pause must
// finish before any restore point is taken, otherwise a write could commit
// between two groups' points.
func TestBarrierDrainsOrdinaryWriters(t *testing.T) {
	f := newBarrierFixture()
	f.groups.writers["shard1"] = 2
	if _, err := f.b.Run(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	j := strings.Join(f.journal, "\n")
	first := strings.Index(j, "restorepoint")
	if last := strings.LastIndex(j[:first], "writers shard1="); last < 0 {
		t.Fatalf("writers were never drained before the points:\n%s", j)
	}
	// Every writer poll happens before the first restore point.
	if strings.Contains(j[first:], "writers shard1=2") || strings.Contains(j[first:], "writers shard1=1") {
		t.Fatalf("restore points taken while a group still had writers:\n%s", j)
	}
}

// TestBarrierWriterDrainTimesOut: a writer that never finishes fails the
// barrier rather than certifying an inconsistent point.
func TestBarrierWriterDrainTimesOut(t *testing.T) {
	f := newBarrierFixture()
	f.groups.writers["shard0"] = 1 << 20
	_, err := f.b.Run(context.Background(), "b")
	if err == nil || !strings.Contains(err.Error(), "shard0 has") {
		t.Fatalf("err %v, want a writer drain timeout", err)
	}
	if f.store.fenced {
		t.Fatal("fence left raised")
	}
	if f.groups.paused["shard0"] || f.groups.paused["shard1"] {
		t.Fatal("groups left paused after a failed barrier")
	}
}

// TestBarrierAbortsWhenAGroupCannotBePaused: certifying while one group still
// accepts writes is the very inconsistency the pause prevents.
func TestBarrierAbortsWhenAGroupCannotBePaused(t *testing.T) {
	f := newBarrierFixture()
	f.groups.fail["pause:shard1"] = errors.New("permission denied")
	_, err := f.b.Run(context.Background(), "b")
	if err == nil || !strings.Contains(err.Error(), "pause writes on shard1") {
		t.Fatalf("err %v", err)
	}
	if len(f.groups.points) != 0 {
		t.Fatalf("restore points taken despite the pause failure: %v", f.groups.points)
	}
	if f.groups.paused["shard0"] {
		t.Fatal("the already-paused group was not resumed")
	}
	if f.store.fenced {
		t.Fatal("fence left raised")
	}
}

// TestBarrierRecoveryLiftsAStrandedFence: a barrier whose controller died
// leaves the cluster fenced and paused; the recovery pass lifts both.
func TestBarrierRecoveryLiftsAStrandedPause(t *testing.T) {
	f := newBarrierFixture()
	f.store.fenced = true
	f.store.fencedAt = f.clock
	f.groups.paused["shard0"], f.groups.paused["shard1"] = true, true

	// A fence younger than the longest possible run may belong to a barrier
	// that is still going, so recovery leaves it alone.
	if err := f.b.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !f.groups.paused["shard0"] {
		t.Fatal("recovery resumed a pause that could still belong to a live barrier")
	}

	// Once no run can still be in flight, the pause is lifted.
	f.clock = f.clock.Add(2 * f.b.maxRunTime())
	if err := f.b.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.groups.paused["shard0"] || f.groups.paused["shard1"] {
		t.Fatalf("groups left paused: %v", f.groups.paused)
	}
	// The fence is never cleared by recovery: a barrier restore raises it
	// deliberately and must stay fenced until its reconciliation finishes.
	if !f.store.fenced {
		t.Fatal("recovery cleared a fence it does not own")
	}
	f.store.fenced = false
	// With no fence raised it is a no-op.
	before := len(f.journal)
	if err := f.b.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.journal) != before {
		t.Fatalf("recovery touched a healthy cluster: %v", f.journal[before:])
	}
}

// TestBarrierRefusesWhileACopyIsApplying: logical replication apply workers
// write outside both the pause and the drain, so a group receiving a copy
// cannot be part of a certified point.
func TestBarrierRefusesWhileACopyIsApplying(t *testing.T) {
	f := newBarrierFixture()
	f.groups.subs["shard1"] = 1
	_, err := f.b.Run(context.Background(), "b")
	if err == nil || !strings.Contains(err.Error(), "subscription") {
		t.Fatalf("err %v, want a refusal naming the subscription", err)
	}
	if len(f.groups.points) != 0 {
		t.Fatalf("restore points taken while a copy was applying: %v", f.groups.points)
	}
	if f.groups.paused["shard0"] {
		t.Fatal("a group was left paused after the refusal")
	}
	if f.store.fenced {
		t.Fatal("fence left raised")
	}
}

// TestBarrierRefusesToCertifyAfterAPauseIsLost: a primary that restarts, is
// promoted, or is resumed underneath the run stops refusing writes, so the
// window is no longer provably quiet and the point must not be certified.
func TestBarrierRefusesToCertifyAfterAPauseIsLost(t *testing.T) {
	f := newBarrierFixture()
	// Resume shard1 underneath the run, just before the points are taken.
	f.groups.onRestorePoint = func() { f.groups.paused["shard1"] = false }
	_, err := f.b.Run(context.Background(), "b")
	if err == nil || !strings.Contains(err.Error(), "stopped refusing writes") {
		t.Fatalf("err %v, want a refusal to certify", err)
	}
	if len(f.store.recorded) != 0 {
		t.Fatal("a point was certified after the pause was lost")
	}
}

// TestBarrierKeepsTheFenceRaisedWhenAResumeFails: the fence is the durable
// trace that shards are still paused, so it must stay up for recovery to
// retry rather than stranding them silently.
func TestBarrierKeepsTheFenceRaisedWhenAResumeFails(t *testing.T) {
	f := newBarrierFixture()
	f.groups.fail["resume:shard1"] = errors.New("connection refused")
	_, err := f.b.Run(context.Background(), "b")
	if err == nil || !strings.Contains(err.Error(), "resume writes on shard1") {
		t.Fatalf("err %v", err)
	}
	if !f.store.fenced {
		t.Fatal("fence cleared even though a shard is still paused; recovery can never repair it")
	}
	if !f.groups.paused["shard1"] {
		t.Fatal("test setup: shard1 should still be paused")
	}
	// Recovery retries the resume once no run can be in flight.
	f.groups.fail["resume:shard1"] = nil
	f.clock = f.clock.Add(2 * f.b.maxRunTime())
	if err := f.b.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.groups.paused["shard1"] {
		t.Fatal("recovery did not resume the stranded shard")
	}
}

// TestArchivedSegmentIgnoresWhatIsNotASegment: pg_stat_archiver reports
// the last file archived, and that is not always a WAL segment. Comparing
// a timeline history file to a segment name as plain strings certified a
// restore point whose segment had never reached the archive -- a history
// file for timeline 2 sorts above every timeline-1 segment, because the
// two diverge at the timeline digit.
func TestArchivedSegmentIgnoresWhatIsNotASegment(t *testing.T) {
	const want = "000000010000000000000009"
	for _, tc := range []struct {
		name, last, seg string
		fails           bool
	}{
		{name: "the segment itself", last: want, seg: want},
		{name: "a later segment", last: "00000001000000000000000A", seg: "00000001000000000000000A"},
		{name: "an earlier segment", last: "000000010000000000000008", seg: "000000010000000000000008"},
		{name: "a backup label names its segment", last: want + ".00000028.backup", seg: want},
		{name: "a history file names none", last: "00000002.history"},
		{name: "nothing archived yet", last: ""},
		{name: "a segment from another timeline", last: "000000020000000000000009", fails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seg, err := archivedSegment(tc.last, want)
			if tc.fails {
				if err == nil {
					t.Fatalf("timeline change must fail the barrier, got %q", seg)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if seg != tc.seg {
				t.Errorf("archivedSegment(%q) = %q, want %q", tc.last, seg, tc.seg)
			}
			if certified := seg != "" && seg >= want; certified != (tc.seg >= want && tc.seg != "") {
				t.Errorf("certification from %q = %v", tc.last, certified)
			}
		})
	}
}

// TestBarrierDoesNotCertifyOnAHistoryFile: a group that changed timeline
// while the barrier was between the restore point and the archive wait
// reports a history file as the last thing archived. It sorts above every
// segment of the older timeline, so the wait used to end immediately and
// the point was recorded certified with its segment still unarchived --
// a restore aimed at it then stops short of the point on that group.
func TestBarrierDoesNotCertifyOnAHistoryFile(t *testing.T) {
	f := newBarrierFixture()
	f.groups.archived["shard1"] = "00000002.history"
	_, err := f.b.Run(context.Background(), "nightly-1")
	if err == nil {
		t.Fatal("a history file certified the barrier")
	}
	if !strings.Contains(err.Error(), "not archived after") || !strings.Contains(err.Error(), "shard1") {
		t.Errorf("error must name the group and the segment it waited for: %v", err)
	}
	if f.store.fenced {
		t.Error("fence left raised")
	}
	if len(f.store.recorded) > 0 {
		t.Error("an uncertified restore point was recorded")
	}
}

// TestBarrierFailsWhenTheTimelineMovedUnderIt: once the group archives a
// segment of the new timeline, the barrier can say what happened rather
// than wait out its timeout -- a promotion since the restore point
// invalidates the point whatever reached the archive.
func TestBarrierFailsWhenTheTimelineMovedUnderIt(t *testing.T) {
	f := newBarrierFixture()
	f.groups.archived["shard1"] = "000000020000000000000009"
	_, err := f.b.Run(context.Background(), "nightly-1")
	if err == nil {
		t.Fatal("a timeline change certified the barrier")
	}
	if !strings.Contains(err.Error(), "timeline changed") {
		t.Errorf("error must name the timeline change: %v", err)
	}
	if f.store.fenced {
		t.Error("fence left raised")
	}
}

// TestABarrierLetsTheFenceReachTheRoutersBeforePausing: the pause is two
// things raised in sequence, and the routers only see the first. A barrier
// that pauses the groups the instant it raises the fence can begin and end
// before a router notices, and every write forwarded meanwhile is refused
// by PostgreSQL with 25006 instead of being buffered.
func TestABarrierLetsTheFenceReachTheRoutersBeforePausing(t *testing.T) {
	f := newBarrierFixture()
	f.b.FenceSettle = 40 * time.Millisecond
	start := time.Now()
	if _, err := f.b.Run(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < f.b.FenceSettle {
		t.Fatalf("the barrier paused the groups after %s, before the fence could reach a router", took)
	}
	// The fence is up first and the groups are paused after it.
	fence := slices.IndexFunc(f.journal, func(e string) bool { return strings.HasPrefix(e, "fence barrier") })
	pause := slices.Index(f.journal, "pause shard0")
	if fence < 0 || pause < 0 || fence > pause {
		t.Fatalf("fence at %d, pause at %d: %v", fence, pause, f.journal)
	}
}

// TestACancelledSettleStillReleasesTheFence: the wait is the first thing
// after the fence goes up, so a barrier cancelled during it is the easiest
// way to leave a cluster fenced with nothing running to lift it.
func TestACancelledSettleStillReleasesTheFence(t *testing.T) {
	f := newBarrierFixture()
	f.b.FenceSettle = time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	if _, err := f.b.Run(ctx, "b"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	if f.store.fenced {
		t.Fatal("fence left raised after a cancelled settle")
	}
}

// TestAFailedBarrierIsAFailedRPC: a barrier that did not happen has to
// arrive as a failed call. Embedded in a successful response it counted as
// OK to every gRPC retry policy, interceptor and metric in the path, and
// only a caller that also read the body could tell the difference -- the
// two-channel problem, inside a single RPC, since the same method already
// reported its name and busy failures as statuses.
func TestAFailedBarrierIsAFailedRPC(t *testing.T) {
	f := newBarrierFixture()
	f.groups.fail["restorepoint:shard1"] = errors.New("read only")
	srv := &Server{Barrier: f.b}

	resp, err := srv.CreateBarrier(context.Background(), &pgshardv1.CreateBarrierRequest{Name: "b1"})
	if err == nil {
		t.Fatalf("a barrier that failed returned a successful RPC: %v", resp)
	}
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("code %v, want Internal: a run that failed partway is not known to be retryable", got)
	}
	if !strings.Contains(err.Error(), "read only") {
		t.Fatalf("the status dropped what went wrong: %v", err)
	}
	if resp != nil {
		t.Fatalf("a failed call must carry no response body: %v", resp)
	}
}
