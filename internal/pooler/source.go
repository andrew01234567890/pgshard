package pooler

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// View is the pooler's current belief about its shard, used to fence
// requests and to answer Health.
type View struct {
	Generation uint64
	Epoch      uint64
	Role       pgshardv1.HealthStatus_Role
	// LagBytes is the member's replay lag, nil when nothing measured it.
	// Nothing does today: the value belongs to the agent, which measures
	// this member's streaming lag, and the pooler has no client for it.
	// Publishing a zero in the meantime would assert "caught up" to
	// anything that gates on lag.
	LagBytes *uint64
	Serving  bool
	// Migrating is set while a reshard cutover fences the shard's ranges:
	// new PREPARE TRANSACTIONs are refused so the sources drain.
	Migrating bool
}

// Source supplies the View. The agent/operator will drive it later; today it
// is static configuration or a catalog snapshot watcher.
type Source interface {
	View() View
}

// StaticSource is a Source that can be updated atomically.
type StaticSource struct {
	v atomic.Pointer[View]
}

// NewStaticSource returns a StaticSource holding v.
func NewStaticSource(v View) *StaticSource {
	s := &StaticSource{}
	s.Set(v)
	return s
}

// Set replaces the view. A static view is always serving: it is configured
// rather than refreshed, so there is nothing about it that can go stale.
// Serving is what a SnapshotSource clears when its catalog view stops being
// refreshed, and leaving the zero value of the field to mean "refuse
// everything" would make every View literal a trap.
func (s *StaticSource) Set(v View) {
	v.Serving = true
	s.v.Store(&v)
}

// View implements Source.
func (s *StaticSource) View() View { return *s.v.Load() }

// SnapshotSource derives generation and epoch for one shard from a catalog
// snapshot watcher; role/lag come from Base.
type SnapshotSource struct {
	Watcher *snapshot.Watcher
	Shard   snapshot.ShardKey
	Base    View
}

// View implements Source. Before the first snapshot it reports Base.
func (s *SnapshotSource) View() View {
	v := s.Base
	snap := s.Watcher.Current()
	if snap == nil {
		return v
	}
	if snap.Stale(time.Now()) {
		// Generation and epoch are the fence the pooler enforces for the
		// router; enforcing them from a view that has stopped being
		// refreshed is enforcing the wrong thing, so stop serving instead.
		v.Serving = false
		return v
	}
	v.Generation = uint64(snap.ShardMapGeneration)
	if sv, ok := snap.Serving[s.Shard]; ok {
		v.Epoch = uint64(sv.Epoch)
		v.Migrating = sv.Migrating
	}
	return v
}

// migratingSQLState is cannot_connect_now: the request must be retried once
// the cutover released the fence.
const migratingSQLState = "57P03"

// fenceMigrating refuses a new PREPARE TRANSACTION on a migrating shard; every
// other statement (reads, the commit or rollback of an already prepared
// transaction, the writes of a transaction the router let finish) passes.
//
// Both protocols are inspected. The router runs its own PREPARE TRANSACTION as
// a simple query, and refuses a client's when it plans the Parse, so the
// extended path carries nothing today -- but the pooler is the fence that
// makes a stateless router safe, and a fence that holds only while the router
// is correct is not one.
func fenceMigrating(v View, req *pgshardv1.ExecuteRequest) *pgshardv1.Error {
	if !v.Migrating {
		return nil
	}
	var sql string
	switch m := req.Message.(type) {
	case *pgshardv1.ExecuteRequest_SimpleQuery:
		sql = m.SimpleQuery.GetSql()
	case *pgshardv1.ExecuteRequest_Parse:
		sql = m.Parse.GetSql()
	}
	if !isPrepareTransaction(sql) {
		return nil
	}
	return &pgshardv1.Error{Sqlstate: migratingSQLState, Message: "shard is migrating: new prepared transactions are refused",
		Hint: "retry the transaction once the reshard cutover published the new shard map"}
}

func isPrepareTransaction(sql string) bool {
	s := strings.TrimLeft(sql, " \t\r\n(")
	if len(s) < len("PREPARE TRANSACTION") {
		return false
	}
	return strings.EqualFold(s[:len("PREPARE TRANSACTION")], "PREPARE TRANSACTION")
}

// SQLSTATE 55000 (object_not_in_prerequisite_state) marks fencing refusals.
const fenceSQLState = "55000"

// serving refuses everything while the pooler's own view of the catalog has
// stopped being refreshed. The generation and epoch in a stale view are the
// last ones it read, and enforcing a fence from those is enforcing the wrong
// thing -- a router that has moved on is admitted, and one that has not is
// refused with a message blaming the router. View() already stops serving in
// that case; nothing read it.
func serving(v View) *pgshardv1.Error {
	if v.Serving {
		return nil
	}
	return &pgshardv1.Error{Sqlstate: fenceSQLState,
		Message: "this pooler's catalog view is stale, so it cannot say which shard map or epoch it serves",
		Hint:    "the pooler cannot reach the catalog; the request is not wrong and can be retried",
		Reason:  pgshardv1.Reason_REASON_STALE_GENERATION}
}

// fence checks a request's generation against the view; nil means admitted.
func fence(v View, g *pgshardv1.Generation) *pgshardv1.Error {
	if g == nil {
		return &pgshardv1.Error{Sqlstate: fenceSQLState, Message: "missing routing generation",
			Reason: pgshardv1.Reason_REASON_STALE_GENERATION}
	}
	if g.ShardMapGeneration != v.Generation {
		return &pgshardv1.Error{Sqlstate: fenceSQLState, Message: "stale routing generation",
			Detail: detailf("request %d, pooler %d", g.ShardMapGeneration, v.Generation),
			Reason: pgshardv1.Reason_REASON_STALE_GENERATION}
	}
	if g.PrimaryEpoch != v.Epoch {
		return &pgshardv1.Error{Sqlstate: fenceSQLState, Message: "stale primary epoch",
			Detail: detailf("request %d, pooler %d", g.PrimaryEpoch, v.Epoch),
			Reason: pgshardv1.Reason_REASON_STALE_GENERATION}
	}
	return nil
}
