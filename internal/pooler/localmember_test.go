package pooler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

type localMember struct{ inRecovery, known bool }

func (l localMember) InRecovery() (bool, bool) { return l.inRecovery, l.known }

// The epoch a pooler fences with is the catalog's for its shard, and it is
// the same value the router stamps requests with: both sides of that
// comparison come from one row, so it catches a router behind the catalog
// and never a pooler standing in front of a member that is no longer the
// primary. A shard fails over, the router reloads to the new epoch and sends
// on the stream it still holds to the OLD primary's pooler, and that pooler
// -- reading the same catalog -- agrees on the epoch and admits the write.
// The member's own recovery state is the fact the catalog cannot supply.
func TestAPoolerInFrontOfADemotedPrimaryRefuses(t *testing.T) {
	shard := snapshot.ShardKey{ShardSet: "default", ShardID: 1}
	s := &SnapshotSource{Watcher: &snapshot.Watcher{}, Shard: shard,
		Base:  View{Generation: 1, Epoch: 1, Serving: true, Role: pgshardv1.HealthStatus_ROLE_PRIMARY},
		Local: localMember{inRecovery: true, known: true}}
	s.Watcher.SetForTest(&snapshot.Snapshot{LoadedAt: time.Now(), ShardMapGeneration: 9,
		Serving: map[snapshot.ShardKey]snapshot.Serving{shard: {Epoch: 4}}})
	v := s.View()
	if !v.Standby {
		t.Fatal("a pooler whose own server is in recovery does not know it")
	}
	if v.Role != pgshardv1.HealthStatus_ROLE_STANDBY {
		t.Errorf("Health reports role %v for a server in recovery", v.Role)
	}
	// The epoch the request carries is the one the catalog now holds, so
	// the epoch fence admits it. Only the member check refuses.
	if e := fence(v, &pgshardv1.Generation{ShardMapGeneration: 9, PrimaryEpoch: 4}); e != nil {
		t.Fatalf("the catalog epoch matches on both sides, so the epoch fence cannot be what refuses: %v", e)
	}
	e := member(v)
	if e == nil {
		t.Fatal("a pooler in front of a demoted primary admitted the request")
	}
	if e.Sqlstate != fenceSQLState || !strings.Contains(e.Message, "in recovery") {
		t.Fatalf("refusal %v must be a %s naming the member's recovery state", e, fenceSQLState)
	}
}

// The refusal is on the request path, not only in the view: both the
// streaming path and Reserve have to stop before a backend is dialled.
func TestADemotedPrimaryIsRefusedOnBothRequestPaths(t *testing.T) {
	view := View{Generation: 7, Epoch: 3, Serving: true, Standby: true}
	s := NewServer(Config{Source: notServing{view}})
	res, err := s.Reserve(context.Background(), &pgshardv1.ReserveRequest{SessionId: "r",
		Generation: &pgshardv1.Generation{ShardMapGeneration: 7, PrimaryEpoch: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetError() == nil || !strings.Contains(res.GetError().GetMessage(), "in recovery") {
		t.Fatalf("Reserve on a demoted member: %v", res.GetError())
	}

	h := startHarness(t, PoolConfig{})
	h.src.Set(View{Generation: 7, Epoch: 3, Role: pgshardv1.HealthStatus_ROLE_PRIMARY, Standby: true})
	stream, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e := firstError(roundTrip(t, stream, queryReq("s-demoted", "select 1", gen(7, 3), identity("alice"))))
	if e == nil || !strings.Contains(e.Message, "in recovery") {
		t.Fatalf("Execute on a demoted member: %v", e)
	}
	_ = stream.CloseSend()
	if n := h.pg.dials.Load(); n != 0 {
		t.Fatalf("a demoted member dialled PostgreSQL %d times", n)
	}
}

// A probe that has not answered yet, or that cannot reach its server, has
// learned nothing. Refusing on that would take a shard out on a socket
// hiccup, and the requests it would refuse fail on their own anyway.
func TestAnUnansweredProbeChangesNothing(t *testing.T) {
	shard := snapshot.ShardKey{ShardSet: "default", ShardID: 1}
	base := View{Generation: 1, Epoch: 1, Serving: true, Role: pgshardv1.HealthStatus_ROLE_PRIMARY}
	for _, local := range []LocalMember{nil, localMember{}} {
		s := &SnapshotSource{Watcher: &snapshot.Watcher{}, Shard: shard, Base: base, Local: local}
		s.Watcher.SetForTest(&snapshot.Snapshot{LoadedAt: time.Now(), ShardMapGeneration: 9,
			Serving: map[snapshot.ShardKey]snapshot.Serving{shard: {Epoch: 4}}})
		v := s.View()
		if v.Standby || member(v) != nil {
			t.Fatalf("an unanswered probe fenced the shard: %+v", v)
		}
		if v.Role != pgshardv1.HealthStatus_ROLE_PRIMARY {
			t.Errorf("an unanswered probe changed the reported role to %v", v.Role)
		}
	}
}

// A primary answers as one, and the reported role follows the server rather
// than the configured Base -- which was hardcoded to ROLE_PRIMARY for every
// pooler, standbys included.
func TestAPrimaryAnswersAsOne(t *testing.T) {
	shard := snapshot.ShardKey{ShardSet: "default", ShardID: 1}
	s := &SnapshotSource{Watcher: &snapshot.Watcher{}, Shard: shard,
		Base:  View{Generation: 1, Epoch: 1, Serving: true},
		Local: localMember{inRecovery: false, known: true}}
	s.Watcher.SetForTest(&snapshot.Snapshot{LoadedAt: time.Now(), ShardMapGeneration: 9,
		Serving: map[snapshot.ShardKey]snapshot.Serving{shard: {Epoch: 4}}})
	v := s.View()
	if v.Standby || member(v) != nil {
		t.Fatalf("a primary was fenced: %+v", v)
	}
	if v.Role != pgshardv1.HealthStatus_ROLE_PRIMARY {
		t.Errorf("a primary reports role %v", v.Role)
	}
}
