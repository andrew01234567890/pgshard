package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBarrierStore is the catalog side: the fence, the decision log
// counters and the recorded restore points, with a journal of every step.
type fakeBarrierStore struct {
	mu        sync.Mutex
	fenced    bool
	fencedAt  time.Time
	preparing int
	watermark int64
	recorded  []RestorePoint
	journal   *[]string
	fail      map[string]error
	now       func() time.Time
}

func (s *fakeBarrierStore) log(step string) {
	*s.journal = append(*s.journal, step)
}

func (s *fakeBarrierStore) Lock(context.Context) (func(), error) { return func() {}, nil }

func (s *fakeBarrierStore) Fence(_ context.Context, active bool, reason, _ string) error {
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
	return s.preparing, nil
}

func (s *fakeBarrierStore) DecisionWatermark(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watermark, nil
}

func (s *fakeBarrierStore) Exists(_ context.Context, name string) (bool, error) {
	for _, r := range s.recorded {
		if r.Name == name {
			return true, nil
		}
	}
	return false, nil
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

// fakeGroups is the catalog and two shards with their prepared transaction
// counts and archives.
type fakeGroups struct {
	mu       sync.Mutex
	prepared map[string]int
	archived map[string]string
	points   map[string][]string
	journal  *[]string
	fail     map[string]error
	// archiveAfter is how many ArchivedThrough polls a group answers with
	// an old segment before the restore point's segment shows up.
	archiveAfter map[string]int
	polls        map[string]int
	store        *fakeBarrierStore
	// onRestorePoint runs on every CreateRestorePoint, before the point.
	onRestorePoint func()
}

func (g *fakeGroups) List(context.Context) ([]GroupRef, error) {
	if err := g.fail["list"]; err != nil {
		return nil, err
	}
	return []GroupRef{{Name: CatalogGroup}, {Name: "shard0", Set: "default", ID: 0}, {Name: "shard1", Set: "default", ID: 1}}, nil
}

func (g *fakeGroups) PreparedCount(_ context.Context, ref GroupRef) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.fail["prepared:"+ref.Name]; err != nil {
		return 0, err
	}
	*g.journal = append(*g.journal, fmt.Sprintf("prepared %s=%d", ref.Name, g.prepared[ref.Name]))
	return g.prepared[ref.Name], nil
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
	f.groups = &fakeGroups{prepared: map[string]int{}, archived: map[string]string{}, points: map[string][]string{},
		journal: &f.journal, fail: map[string]error{}, archiveAfter: map[string]int{}, polls: map[string]int{}, store: f.store}
	for _, g := range []string{CatalogGroup, "shard0", "shard1"} {
		f.groups.archived[g] = "000000010000000000000009"
	}
	f.b = &Barrier{Store: f.store, Groups: f.groups, Now: now, Poll: time.Millisecond, DrainTimeout: 50 * time.Millisecond, ArchiveTimeout: 50 * time.Millisecond}
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
		"fence barrier nightly-1",
		"preparing=0", "prepared catalog=0", "prepared shard0=0", "prepared shard1=0",
		"preparing=0", "prepared catalog=0", "prepared shard0=0", "prepared shard1=0",
		"restorepoint catalog pgshard-nightly-1", "restorepoint shard0 pgshard-nightly-1", "restorepoint shard1 pgshard-nightly-1",
		"archived catalog", "archived shard0", "archive-wait shard1", "archive-wait shard1", "archived shard1",
		"record nightly-1", "release",
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
	f.store.preparing = 1
	f.groups.prepared["shard0"] = 2
	go func() {
		time.Sleep(10 * time.Millisecond)
		f.store.mu.Lock()
		f.store.preparing = 0
		f.store.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		f.groups.mu.Lock()
		f.groups.prepared["shard0"] = 0
		f.groups.mu.Unlock()
	}()
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
			if f.journal[0] != "fence barrier b" || f.journal[len(f.journal)-1] != "release" {
				t.Fatalf("journal %v", f.journal)
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
	if len(f.journal) != 0 {
		t.Fatalf("journal %v", f.journal)
	}
	f = newBarrierFixture()
	f.store.fail["fence"] = errors.New("read only catalog")
	if _, err := f.b.Run(context.Background(), "b"); err == nil || !strings.Contains(err.Error(), "fence: read only catalog") {
		t.Fatalf("err %v", err)
	}
	if len(f.journal) != 0 {
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
