package vstream

import (
	"math/rand/v2"
	"testing"

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
