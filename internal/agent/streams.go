package agent

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// StreamPublication is the FOR ALL TABLES publication change streams decode.
const StreamPublication = "pgshard_all"

// CreateStreamSlot fences, ensures the publication exists in the stream's
// database and creates the failover-enabled pgoutput slot there. An existing
// slot is reused when its two_phase setting matches the request.
func (s *Server) CreateStreamSlot(ctx context.Context, req *pgshardv1.CreateStreamSlotRequest) (*pgshardv1.CreateStreamSlotResponse, error) {
	resp := &pgshardv1.CreateStreamSlotResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		return nil, s.rpcErr(err)
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	if !catalog.ValidStreamName(req.GetStream()) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid stream name %q", req.GetStream())
	}
	if req.GetDatabase() == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}
	resp.Slot = catalog.StreamSlotName(req.GetStream(), s.inst.cfg.Shard)
	err = s.withDB(ctx, req.GetDatabase(), func(q querier) error {
		if err := ensurePublication(ctx, q); err != nil {
			return err
		}
		var lsn *uint64
		err := q.QueryRow(ctx, `SELECT lsn - '0/0'::pg_lsn FROM pg_create_logical_replication_slot($1, 'pgoutput', false, $2, true)`,
			resp.Slot, req.GetTwoPhase()).Scan(&lsn)
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "42710" {
			return existingStreamSlot(ctx, q, resp.Slot, req.GetTwoPhase(), &resp.Lsn)
		}
		if err != nil {
			return err
		}
		if lsn != nil {
			resp.Lsn = *lsn
		}
		return nil
	})
	if err != nil {
		return nil, s.rpcErr(err)
	}
	return resp, nil
}

func existingStreamSlot(ctx context.Context, q querier, slot string, twoPhase bool, lsn *uint64) error {
	var have bool
	err := q.QueryRow(ctx, `SELECT two_phase, coalesce(confirmed_flush_lsn - '0/0'::pg_lsn, 0) FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&have, lsn)
	if err != nil {
		return err
	}
	if have != twoPhase {
		return fmt.Errorf("slot %s exists with two_phase=%t, requested %t", slot, have, twoPhase)
	}
	return nil
}

func ensurePublication(ctx context.Context, q querier) error {
	var n int
	if err := q.QueryRow(ctx, `SELECT count(*) FROM pg_publication WHERE pubname = $1`, StreamPublication).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := q.Exec(ctx, "CREATE PUBLICATION "+StreamPublication+" FOR ALL TABLES")
	return err
}

// DropStreamSlot fences and drops the stream's slot; a missing slot is not
// an error.
func (s *Server) DropStreamSlot(ctx context.Context, req *pgshardv1.DropStreamSlotRequest) (*pgshardv1.DropStreamSlotResponse, error) {
	resp := &pgshardv1.DropStreamSlotResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		return nil, s.rpcErr(err)
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	if !catalog.ValidStreamName(req.GetStream()) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid stream name %q", req.GetStream())
	}
	slot := catalog.StreamSlotName(req.GetStream(), s.inst.cfg.Shard)
	err = s.withConn(ctx, func(q querier) error {
		_, err := q.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, slot)
		return err
	})
	if err != nil {
		return nil, s.rpcErr(err)
	}
	return resp, nil
}

// SetSynchronizedStandbySlots fences and sets synchronized_standby_slots to
// the requested physical slots that exist and are active. A listed slot
// that is missing or inactive would stall every failover-slot walsender,
// so such entries are dropped rather than applied.
func (s *Server) SetSynchronizedStandbySlots(ctx context.Context, req *pgshardv1.SetSynchronizedStandbySlotsRequest) (*pgshardv1.SetSynchronizedStandbySlotsResponse, error) {
	resp := &pgshardv1.SetSynchronizedStandbySlotsResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		return nil, s.rpcErr(err)
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	err = s.withConn(ctx, func(q querier) error {
		rows, err := q.Query(ctx, `SELECT slot_name FROM pg_replication_slots
			WHERE slot_type = 'physical' AND active AND slot_name = ANY($1) ORDER BY slot_name`, req.GetSlots())
		if err != nil {
			return err
		}
		applied, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		resp.Applied = applied
		var current string
		if err := q.QueryRow(ctx, "SHOW synchronized_standby_slots").Scan(&current); err != nil {
			return err
		}
		want := strings.Join(applied, ", ")
		if current == want {
			return nil
		}
		if err := writeFileSync(filepath.Join(s.inst.cfg.PGData, slotsConf), []byte("synchronized_standby_slots = "+quote(want)+"\n")); err != nil {
			return err
		}
		_, err = q.Exec(ctx, "SELECT pg_reload_conf()")
		return err
	})
	if err != nil {
		return nil, s.rpcErr(err)
	}
	return resp, nil
}

func (s *Server) withDB(ctx context.Context, database string, fn func(querier) error) error {
	conn, err := s.inst.ConnectDB(ctx, database)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	return fn(conn)
}
