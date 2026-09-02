package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/andrew01234567890/pgshard/internal/buildinfo"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// Server implements pgshard.v1.Agent over an Instance.
type Server struct {
	pgshardv1.UnimplementedAgentServer

	inst  *Instance
	epoch *EpochStore
	lease *Lease
	log   *slog.Logger
	// fatal stops the whole agent; used when the lease is lost.
	fatal func(error)

	mu        sync.Mutex
	holdStop  context.CancelFunc
	opTimeout time.Duration
	// bgCtx bounds background work started by RPCs (stanza creation after a
	// promotion); it is the agent's run context.
	bgCtx context.Context
}

// stanzaRetry is the pause between attempts to create the pgbackrest stanza.
const stanzaRetry = 30 * time.Second

// NewServer wires the RPC surface. lease may be nil when leasing is disabled.
func NewServer(inst *Instance, epoch *EpochStore, lease *Lease, log *slog.Logger, fatal func(error)) *Server {
	return &Server{inst: inst, epoch: epoch, lease: lease, log: log, fatal: fatal, opTimeout: 10 * time.Minute, bgCtx: context.Background()}
}

func pgErr(err error) *pgshardv1.Error {
	if err == nil {
		return nil
	}
	code := "XX000"
	if errors.Is(err, ErrStaleEpoch) {
		code = "55000"
	}
	return &pgshardv1.Error{Sqlstate: code, Message: err.Error()}
}

// fence accepts a strictly greater epoch (role changes) or returns the
// stale-epoch error to embed.
func (s *Server) fence(epoch uint64) error {
	return s.epoch.Accept(epoch)
}

// fenceCurrent accepts only the current epoch (same-term operations) and
// returns the context that term's work must run under: it ends when a later
// epoch is accepted, so an operation admitted by one controller does not go
// on running for the one that replaced it. The release must be called when
// the operation finishes.
func (s *Server) fenceCurrent(ctx context.Context, epoch uint64) (context.Context, func(), error) {
	return s.epoch.Term(ctx, epoch)
}

// Status is read-only.
func (s *Server) Status(ctx context.Context, _ *pgshardv1.StatusRequest) (*pgshardv1.StatusResponse, error) {
	resp := &pgshardv1.StatusResponse{Epoch: s.epoch.Current(), Role: pgshardv1.StatusResponse_ROLE_PRIMARY,
		PromotionPending: s.inst.PromotionPending(), Build: buildinfo.String()}
	// A role that cannot be read is reported as an error rather than as
	// primary: the operator promotes and fences on this answer.
	switch standby, err := s.inst.IsStandby(); {
	case err != nil:
		resp.Error = pgErr(err)
		return resp, nil
	case standby:
		resp.Role = pgshardv1.StatusResponse_ROLE_STANDBY
	}
	conn, err := s.inst.Connect(ctx)
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	defer func() { _ = conn.Close(ctx) }()
	resp.Running = true
	var lsn uint64
	var tl int64
	q := `SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END - '0/0'::pg_lsn,
	             coalesce((SELECT received_tli FROM pg_stat_wal_receiver), (pg_control_checkpoint()).timeline_id)`
	if err := conn.QueryRow(ctx, q).Scan(&lsn, &tl); err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	resp.Lsn = lsn
	resp.Timeline = uint32(tl)
	return resp, nil
}

// Promote fences, acquires the lease, starts renewing, then promotes. The
// renewal loop starts immediately after the lease is acquired, before the
// fallible promote steps, so a failure after pg_ctl promote can never leave a
// writable primary whose lease is not being renewed (which would let a second
// member be promoted after it expires — split brain). Instance.Promote is
// idempotent, so the operator can retry to finish the post-promotion setup.
func (s *Server) Promote(ctx context.Context, req *pgshardv1.PromoteRequest) (*pgshardv1.PromoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &pgshardv1.PromoteResponse{Epoch: s.epoch.Current()}
	if err := s.fence(req.GetEpoch()); err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	resp.Epoch = req.GetEpoch()
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	// Acquire the lease only when no hold is renewing it yet: a re-promote of
	// a node that is already primary (to finish a failed post-promotion
	// setup) must not race its own hold loop with a second writer, or a
	// spurious update conflict could read as a lost lease and self-fence a
	// healthy primary.
	if s.lease != nil && s.holdStop == nil {
		if err := s.lease.Acquire(ctx); err != nil {
			resp.Error = pgErr(fmt.Errorf("acquire lease: %w", err))
			return resp, nil
		}
	}
	// Renew from here on: if a promote step below fails after pg_ctl promote,
	// the database is already writable and the hold keeps the lease alive so
	// no other member is promoted.
	s.startHold()
	// Instance.Promote is idempotent; a transient failure in the post-
	// promotion setup (reload, checkpoint) is retried here so the setup
	// completes and the operator sees a clean success. A persistent failure
	// still returns an error, but the hold above keeps the lease so there is
	// no split brain while it is resolved.
	if err := s.promoteWithRetry(ctx); err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	s.inst.startStanzaWorker(s.bgCtx, stanzaRetry)
	st, _ := s.Status(ctx, nil)
	resp.Timeline = st.GetTimeline()
	return resp, nil
}

// promoteRetries bounds how many times Promote re-runs the idempotent
// post-promotion setup before returning an error to the operator.
const promoteRetries = 3

func (s *Server) promoteWithRetry(ctx context.Context) error {
	var err error
	for attempt := 0; attempt < promoteRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
			s.log.Warn("retrying promotion setup", "attempt", attempt, "err", err)
		}
		if err = s.inst.Promote(ctx); err == nil {
			return nil
		}
	}
	return err
}

// startHold renews the primary lease until releaseLease ends it. The
// renewal deliberately runs on context.Background rather than the agent's
// lifecycle context: shutdown cancels that context first and only then
// stops PostgreSQL, with a budget of three times shutdownTimeout against a
// lease an eighth as long, so a hold bound to it would stop renewing while
// this member was still a writable primary and let another one promote
// under it. Ending the hold is releaseLease's job, once postgres is down.
func (s *Server) startHold() {
	if s.lease == nil || s.holdStop != nil {
		return
	}
	hctx, cancel := context.WithCancel(context.Background())
	s.holdStop = cancel
	go func() {
		if err := s.lease.Hold(hctx); err != nil {
			s.log.Error("primary lease lost; fencing", "err", err)
			s.fatal(fmt.Errorf("primary lease lost: %w", err))
		}
	}()
}

// releaseLease hands the lease back once postgres has stopped, so a
// successor need not wait for expiry.
func (s *Server) releaseLease(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopHold(ctx)
}

func (s *Server) stopHold(ctx context.Context) {
	if s.holdStop != nil {
		s.holdStop()
		s.holdStop = nil
	}
	if s.lease != nil {
		if err := s.lease.Release(ctx); err != nil {
			s.log.Warn("lease release failed", "err", err)
		}
	}
}

// Demote fences, releases the lease and follows the current lease holder.
func (s *Server) Demote(ctx context.Context, req *pgshardv1.DemoteRequest) (*pgshardv1.DemoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &pgshardv1.DemoteResponse{Epoch: s.epoch.Current()}
	if err := s.fence(req.GetEpoch()); err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	resp.Epoch = req.GetEpoch()
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	// Stop PostgreSQL before releasing the lease: releasing while the node is
	// still a writable primary would let a successor acquire the lease and
	// promote, producing two writable primaries. The hold keeps renewing
	// until the database is down.
	if err := s.inst.Demote(ctx, ""); err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	s.stopHold(ctx)
	return resp, nil
}

// Every mutating RPC below fences on the epoch before it touches
// PostgreSQL, so a controller that has been superseded cannot act on a
// member that has moved on without it. Status and ListSlots do not fence,
// because they read.

// Rewind runs pg_rewind against req.Source, which is how a demoted primary
// rejoins without a full copy.
func (s *Server) Rewind(ctx context.Context, req *pgshardv1.RewindRequest) (*pgshardv1.RewindResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &pgshardv1.RewindResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	s.stopHold(ctx)
	if err := s.inst.Rewind(ctx, req.GetSource()); err != nil {
		resp.Error = pgErr(err)
	}
	return resp, nil
}

// Reclone rebuilds the data directory from the primary (pg_basebackup) or
// from the member's backup stanza (pgbackrest delta restore as a standby).
func (s *Server) Reclone(ctx context.Context, req *pgshardv1.RecloneRequest) (*pgshardv1.RecloneResponse, error) {
	fromRepo := req.GetSourceKind() == pgshardv1.RecloneRequest_SOURCE_KIND_BACKUP
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &pgshardv1.RecloneResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	s.stopHold(ctx)
	if err := s.inst.Reclone(ctx, fromRepo); err != nil {
		resp.Error = pgErr(err)
	}
	return resp, nil
}

// Reload re-reads the configuration file; PostgreSQL keeps running, so a
// setting that needs a restart is not applied by this.
func (s *Server) Reload(ctx context.Context, req *pgshardv1.ReloadRequest) (*pgshardv1.ReloadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &pgshardv1.ReloadResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	if err := s.inst.Reload(ctx); err != nil {
		resp.Error = pgErr(err)
	}
	resp.SettingsHash = s.inst.cfg.SettingsHash
	return resp, nil
}

// Restart stops PostgreSQL in the requested shutdown mode and starts it
// again, which is what a setting Reload cannot apply needs.
func (s *Server) Restart(ctx context.Context, req *pgshardv1.RestartRequest) (*pgshardv1.RestartResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &pgshardv1.RestartResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	mode := ShutdownFast
	switch req.GetMode() {
	case pgshardv1.RestartRequest_MODE_SMART:
		mode = ShutdownSmart
	case pgshardv1.RestartRequest_MODE_IMMEDIATE:
		mode = ShutdownImmediate
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	if err := s.inst.Restart(ctx, mode); err != nil {
		resp.Error = pgErr(err)
	}
	return resp, nil
}

// CreateRestorePoint records a named point a PITR can recover to. It is
// the per-group half of a cluster-consistent barrier.
func (s *Server) CreateRestorePoint(ctx context.Context, req *pgshardv1.CreateRestorePointRequest) (*pgshardv1.CreateRestorePointResponse, error) {
	resp := &pgshardv1.CreateRestorePointResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	err = s.withConn(ctx, func(q querier) error {
		return q.QueryRow(ctx, "SELECT pg_create_restore_point($1) - '0/0'::pg_lsn", req.GetName()).Scan(&resp.Lsn)
	})
	resp.Error = pgErr(err)
	return resp, nil
}

// CreateSlot creates a physical or logical replication slot. A slot
// retains WAL until it is dropped or invalidated, so one created and
// forgotten fills the disk.
func (s *Server) CreateSlot(ctx context.Context, req *pgshardv1.CreateSlotRequest) (*pgshardv1.CreateSlotResponse, error) {
	resp := &pgshardv1.CreateSlotResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	err = s.withConn(ctx, func(q querier) error {
		var lsn *uint64
		var err error
		switch req.GetKind() {
		case pgshardv1.SlotKind_SLOT_KIND_PHYSICAL:
			err = q.QueryRow(ctx, "SELECT lsn - '0/0'::pg_lsn FROM pg_create_physical_replication_slot($1, true)", req.GetName()).Scan(&lsn)
		case pgshardv1.SlotKind_SLOT_KIND_LOGICAL:
			err = q.QueryRow(ctx, "SELECT lsn - '0/0'::pg_lsn FROM pg_create_logical_replication_slot($1, $2, false, false, $3)",
				req.GetName(), req.GetPlugin(), req.GetFailover()).Scan(&lsn)
		default:
			return errors.New("slot kind must be physical or logical")
		}
		if err != nil {
			return err
		}
		if lsn != nil {
			resp.Lsn = *lsn
		}
		return nil
	})
	resp.Error = pgErr(err)
	return resp, nil
}

// DropSlot removes a slot and the WAL it was retaining.
func (s *Server) DropSlot(ctx context.Context, req *pgshardv1.DropSlotRequest) (*pgshardv1.DropSlotResponse, error) {
	resp := &pgshardv1.DropSlotResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	err = s.withConn(ctx, func(q querier) error {
		_, err := q.Exec(ctx, "SELECT pg_drop_replication_slot($1)", req.GetName())
		return err
	})
	resp.Error = pgErr(err)
	return resp, nil
}

// ListSlots is read-only.
func (s *Server) ListSlots(ctx context.Context, _ *pgshardv1.ListSlotsRequest) (*pgshardv1.ListSlotsResponse, error) {
	resp := &pgshardv1.ListSlotsResponse{Epoch: s.epoch.Current()}
	err := s.withConn(ctx, func(q querier) error {
		rows, err := q.Query(ctx, `SELECT slot_name, slot_type, coalesce(plugin, ''), failover, active,
			coalesce(restart_lsn - '0/0'::pg_lsn, 0), coalesce(confirmed_flush_lsn - '0/0'::pg_lsn, 0),
			coalesce(wal_status, ''), coalesce(invalidation_reason, ''), synced, temporary, two_phase, coalesce(database, '')
			FROM pg_replication_slots ORDER BY slot_name`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var sl pgshardv1.Slot
			var kind string
			if err := rows.Scan(&sl.Name, &kind, &sl.Plugin, &sl.Failover, &sl.Active, &sl.RestartLsn, &sl.ConfirmedFlushLsn,
				&sl.WalStatus, &sl.InvalidationReason, &sl.Synced, &sl.Temporary, &sl.TwoPhase, &sl.Database); err != nil {
				return err
			}
			sl.Kind = pgshardv1.SlotKind_SLOT_KIND_PHYSICAL
			if strings.EqualFold(kind, "logical") {
				sl.Kind = pgshardv1.SlotKind_SLOT_KIND_LOGICAL
			}
			resp.Slots = append(resp.Slots, &sl)
		}
		return rows.Err()
	})
	resp.Error = pgErr(err)
	return resp, nil
}
