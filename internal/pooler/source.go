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
	LagBytes   uint64
	Serving    bool
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

// Set replaces the view.
func (s *StaticSource) Set(v View) { s.v.Store(&v) }

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
func fenceMigrating(v View, req *pgshardv1.ExecuteRequest) *pgshardv1.Error {
	if !v.Migrating {
		return nil
	}
	q, ok := req.Message.(*pgshardv1.ExecuteRequest_SimpleQuery)
	if !ok || !isPrepareTransaction(q.SimpleQuery.Sql) {
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

// fence checks a request's generation against the view; nil means admitted.
func fence(v View, g *pgshardv1.Generation) *pgshardv1.Error {
	if g == nil {
		return &pgshardv1.Error{Sqlstate: fenceSQLState, Message: "missing routing generation"}
	}
	if g.ShardMapGeneration != v.Generation {
		return &pgshardv1.Error{Sqlstate: fenceSQLState, Message: "stale routing generation",
			Detail: detailf("request %d, pooler %d", g.ShardMapGeneration, v.Generation)}
	}
	if g.PrimaryEpoch != v.Epoch {
		return &pgshardv1.Error{Sqlstate: fenceSQLState, Message: "stale primary epoch",
			Detail: detailf("request %d, pooler %d", g.PrimaryEpoch, v.Epoch)}
	}
	return nil
}
