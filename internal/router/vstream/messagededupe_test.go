package vstream

import (
	"testing"

	"github.com/andrew01234567890/pgshard/internal/router"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestANonTransactionalMessageIsNotReplayedOnReconnect.
//
// A non-transactional message is emitted immediately rather than at a
// commit, and it used to arrive with no position at all. The reader's
// dedupe tests position, so every failover or pooler blip replayed every
// such message since the slot's confirmed LSN -- and consumers using them
// as DDL markers acted on the same marker again.
//
// The producer stamps every ChangeEvent with its WAL position, so the unit
// carries it and the ordinary dedupe applies.
func TestANonTransactionalMessageIsNotReplayedOnReconnect(t *testing.T) {
	msg := func(lsn uint64, body string) *pgshardv1.ChangeEvent {
		return &pgshardv1.ChangeEvent{Lsn: lsn, Event: &pgshardv1.ChangeEvent_Message_{
			Message: &pgshardv1.ChangeEvent_Message{Prefix: "p", Content: []byte(body)}}}
	}
	a := assembler{shard: router.Shard{Set: router.DefaultShardSet, ID: 0}, relations: map[uint32]*relMeta{}, streamed: map[uint32]*unit{}}

	u, err := a.add(msg(500, "marker"))
	if err != nil {
		t.Fatal(err)
	}
	if u == nil || !u.position || u.endLSN != 500 {
		t.Fatalf("a message must carry its WAL position, or the reader cannot tell a replay from a new one: %+v", u)
	}

	// A message the producer did not stamp keeps today's behaviour --
	// delivered, possibly twice. Treating an absent LSN as zero would make
	// it compare "already delivered" and drop it, and silently dropping a
	// DDL marker is worse than delivering one twice.
	u, err = a.add(msg(0, "unstamped"))
	if err != nil {
		t.Fatal(err)
	}
	if u == nil || u.position {
		t.Fatalf("an unstamped message must not claim a position: %+v", u)
	}
}
