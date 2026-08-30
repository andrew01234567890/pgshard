package vstream

import (
	"math/rand/v2"
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// slowestOtherByScan is what commitFloors replaced: the slowest commit
// timestamp among the shards other than self, found by walking them all.
// It is kept here as the reference the summary is checked against, since
// this is an algorithm swap rather than a rewrite of one expression.
func (m *merger) slowestOtherByScan(self router.Shard) (int64, bool) {
	var floor int64
	known := false
	for _, sh := range m.shards {
		if sh == self {
			continue
		}
		ts, ok := m.lastCommit[sh]
		if !ok {
			continue
		}
		if !known || ts < floor {
			floor, known = ts, true
		}
	}
	return floor, known
}

// TestCommitFloorsMatchesTheScanItReplaced: aligning skew asked for the
// slowest shard other than this one, once per candidate, by walking every
// shard -- so it cost the square of the shard count on every unit. The
// summary has to answer identically for every shape of input, including
// ties and shards with no commit recorded at all.
func TestCommitFloorsMatchesTheScanItReplaced(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, shards := range []int{0, 1, 2, 3, 8, 64} {
		for trial := range 200 {
			m := &merger{lastCommit: map[router.Shard]int64{}}
			for i := range shards {
				sh := router.Shard{Set: "default", ID: int32(i)}
				m.shards = append(m.shards, sh)
				switch trial % 4 {
				case 0: // every shard has a commit, all distinct
					m.lastCommit[sh] = int64(rng.IntN(1000))
				case 1: // ties are common
					m.lastCommit[sh] = int64(rng.IntN(3))
				case 2: // some shards have none
					if rng.IntN(2) == 0 {
						m.lastCommit[sh] = int64(rng.IntN(100))
					}
				case 3: // exactly one shard has one
					if i == 0 {
						m.lastCommit[sh] = 42
					}
				}
			}
			floors := m.commitFloors()
			for _, sh := range m.shards {
				wantTS, wantOK := m.slowestOtherByScan(sh)
				gotTS, gotOK := floors.without(sh)
				if gotOK != wantOK || (wantOK && gotTS != wantTS) {
					t.Fatalf("shards=%d trial=%d shard=%v: got (%d,%v), scan says (%d,%v); commits %v",
						shards, trial, sh, gotTS, gotOK, wantTS, wantOK, m.lastCommit)
				}
			}
		}
	}
}

// BenchmarkSlowestOther compares the two ways of answering "the slowest
// shard other than this one" for every shard of a pass, which is what
// aligning skew does on each unit it emits.
func BenchmarkSlowestOther(b *testing.B) {
	for _, shards := range []int{8, 128} {
		m := &merger{lastCommit: map[router.Shard]int64{}}
		for i := range shards {
			sh := router.Shard{Set: "default", ID: int32(i)}
			m.shards = append(m.shards, sh)
			m.lastCommit[sh] = int64(i)
		}
		b.Run("scan/"+itoa(shards), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, sh := range m.shards {
					if _, ok := m.slowestOtherByScan(sh); !ok {
						b.Fatal("no floor")
					}
				}
			}
		})
		b.Run("floors/"+itoa(shards), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				floors := m.commitFloors()
				for _, sh := range m.shards {
					if _, ok := floors.without(sh); !ok {
						b.Fatal("no floor")
					}
				}
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestVectorReportsThePositionItIsAskedFor: the position message is built
// once and updated in place, so the risk is a stale field rather than a
// wrong calculation -- a shard whose LSN moved but whose entry was not
// refreshed, or an entry left behind from a shard that has since gone
// quiet.
func TestVectorReportsThePositionItIsAskedFor(t *testing.T) {
	shards := []router.Shard{{Set: "default", ID: 0}, {Set: "default", ID: 1}, {Set: "default", ID: 2}}
	m := &merger{shards: shards, generation: 7, position: map[router.Shard]uint64{}, copying: map[router.Shard]*pgshardv1.VCopyState{}}

	// Nothing yet.
	if v := m.vector(); len(v.Shards) != 0 || v.ShardMapGeneration != 7 {
		t.Fatalf("empty position: %+v", v)
	}

	// Each shard appears once its LSN moves, and carries the LSN it has.
	want := map[uint32]uint64{}
	for i, sh := range shards {
		m.position[sh] = uint64(100 + i)
		want[uint32(sh.ID)] = uint64(100 + i)
		got := map[uint32]uint64{}
		for _, e := range m.vector().Shards {
			got[e.Shard.ShardId] = e.Lsn
		}
		if len(got) != len(want) {
			t.Fatalf("after %d shards moved: %v", i+1, got)
		}
		for id, lsn := range want {
			if got[id] != lsn {
				t.Errorf("shard %d reported %d, want %d", id, got[id], lsn)
			}
		}
	}

	// A later move is reflected, and the entry is the same object reused.
	first := m.vector().Shards[0]
	m.position[shards[0]] = 999
	again := m.vector().Shards[0]
	if again.Lsn != 999 {
		t.Errorf("a moved shard reported %d", again.Lsn)
	}
	if first != again {
		t.Error("the entry was reallocated rather than reused, so the point of this is gone")
	}

	// The generation is re-read, not frozen at the first call.
	m.generation = 9
	if v := m.vector(); v.ShardMapGeneration != 9 {
		t.Errorf("generation %d, want the current 9", v.ShardMapGeneration)
	}
}

func BenchmarkVector(b *testing.B) {
	var shards []router.Shard
	m := &merger{generation: 7, position: map[router.Shard]uint64{}, copying: map[router.Shard]*pgshardv1.VCopyState{}}
	for i := range 128 {
		sh := router.Shard{Set: "default", ID: int32(i)}
		shards = append(shards, sh)
		m.position[sh] = uint64(i + 1)
	}
	m.shards = shards
	b.ReportAllocs()
	for b.Loop() {
		if len(m.vector().Shards) != 128 {
			b.Fatal("short vector")
		}
	}
}
