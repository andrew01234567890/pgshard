package router

import (
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestAReferenceReadStaysOnTheShardTheTransactionIsOn.
//
// referenceTarget chose by session id and ignored the shard the transaction
// was already pinned to. A reference table is on EVERY shard, so that choice
// changes nothing about the answer and forces a shard switch for no gain:
// under REPEATABLE READ or SERIALIZABLE it is refused outright with "cannot
// span shards", and under READ COMMITTED it is a needless park, Reserve,
// SET LOCAL lock_timeout and a hidden-writer probe at COMMIT.
func TestAReferenceReadStaysOnTheShardTheTransactionIsOn(t *testing.T) {
	h := newShardedHarness(t)
	ids := h.snap.ShardIDs(DefaultShardSet)
	if len(ids) < 2 {
		t.Skip("this needs more than one shard to have somewhere else to go")
	}

	newSess := func(id uint64) *Executor {
		return newExecutor(h.r, pgwire.SessionInfo{ID: id, Database: "app", User: "app",
			Auth: &pgwire.AuthResult{SCRAM: &pgwire.SCRAMKeys{}}}, Shard{Set: DefaultShardSet, ID: ids[0]})
	}

	// Find a session whose spread choice is NOT the shard we will pin it
	// to, or the assertion below could pass by coincidence.
	var e *Executor
	pin := Shard{Set: DefaultShardSet, ID: ids[1]}
	for id := uint64(1); id < 64; id++ {
		c := newSess(id)
		if c.referenceTarget() != pin {
			e = c
			break
		}
	}
	if e == nil {
		t.Skip("every session id spreads to the pinned shard here")
	}
	spread := e.referenceTarget()

	// Outside a transaction the spread is unchanged: this must not turn
	// every reference read into a home-shard read.
	e.shard = pin
	if got := e.referenceTarget(); got != spread {
		t.Fatalf("outside a transaction a reference read moved from %v to %v; the spread is gone", spread, got)
	}

	// Inside one, it follows the transaction.
	e.tx = pgwire.TxInBlock
	if got := e.referenceTarget(); got != pin {
		t.Fatalf("a reference read inside a transaction went to %v, not the pinned shard %v: that is a shard switch for the same answer", got, pin)
	}

	// A shard that is not in this set is not followed.
	e.shard = Shard{Set: "other-set", ID: ids[1]}
	if got := e.referenceTarget(); got != spread {
		t.Fatalf("a session pinned to another set followed it to %v", got)
	}
}
