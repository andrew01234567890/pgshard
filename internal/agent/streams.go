package agent

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// StreamPublication is the FOR ALL TABLES publication change streams decode.
const StreamPublication = "pgshard_all"

// CreateStreamSlot fences, ensures the publication exists in the stream's
// database and creates the failover-enabled pgoutput slot there.
func (s *Server) CreateStreamSlot(ctx context.Context, req *pgshardv1.CreateStreamSlotRequest) (*pgshardv1.CreateStreamSlotResponse, error) {
	resp := &pgshardv1.CreateStreamSlotResponse{Epoch: s.epoch.Current()}
	if err := s.fenceCurrent(req.GetEpoch()); err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	resp.Epoch = req.GetEpoch()
	if !catalog.ValidStreamName(req.GetStream()) {
		resp.Error = &pgshardv1.Error{Sqlstate: "22023", Message: fmt.Sprintf("invalid stream name %q", req.GetStream())}
		return resp, nil
	}
	if req.GetDatabase() == "" {
		resp.Error = &pgshardv1.Error{Sqlstate: "22023", Message: "database is required"}
		return resp, nil
	}
	resp.Slot = catalog.StreamSlotName(req.GetStream(), s.inst.cfg.Shard)
	err := s.withDB(ctx, req.GetDatabase(), func(q querier) error {
		if err := ensurePublication(ctx, q); err != nil {
			return err
		}
		var lsn *uint64
		err := q.QueryRow(ctx, `SELECT lsn - '0/0'::pg_lsn FROM pg_create_logical_replication_slot($1, 'pgoutput', false, $2, true)`,
			resp.Slot, req.GetTwoPhase()).Scan(&lsn)
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
	if err := s.fenceCurrent(req.GetEpoch()); err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	resp.Epoch = req.GetEpoch()
	if !catalog.ValidStreamName(req.GetStream()) {
		resp.Error = &pgshardv1.Error{Sqlstate: "22023", Message: fmt.Sprintf("invalid stream name %q", req.GetStream())}
		return resp, nil
	}
	slot := catalog.StreamSlotName(req.GetStream(), s.inst.cfg.Shard)
	err := s.withConn(ctx, func(q querier) error {
		_, err := q.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, slot)
		return err
	})
	resp.Error = pgErr(err)
	return resp, nil
}

// SetSynchronizedStandbySlots fences and sets synchronized_standby_slots to
// the requested physical slots that exist and are active. A listed slot
// that is missing or inactive would stall every failover-slot walsender,
// so such entries are dropped rather than applied.
func (s *Server) SetSynchronizedStandbySlots(ctx context.Context, req *pgshardv1.SetSynchronizedStandbySlotsRequest) (*pgshardv1.SetSynchronizedStandbySlotsResponse, error) {
	resp := &pgshardv1.SetSynchronizedStandbySlotsResponse{Epoch: s.epoch.Current()}
	if err := s.fenceCurrent(req.GetEpoch()); err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	resp.Epoch = req.GetEpoch()
	err := s.withConn(ctx, func(q querier) error {
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
	resp.Error = pgErr(err)
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
