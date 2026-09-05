package agent

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/twopc"
)

// catalogDatabase is the database the pgshard catalog schema lives in on
// the catalog group.
const catalogDatabase = "postgres"

// ListTransactionDecisions reads pgshard.xact_decisions of the catalog
// database. Read-only.
func (s *Server) ListTransactionDecisions(ctx context.Context, _ *pgshardv1.ListTransactionDecisionsRequest) (*pgshardv1.ListTransactionDecisionsResponse, error) {
	resp := &pgshardv1.ListTransactionDecisionsResponse{Epoch: s.epoch.Current()}
	conn, err := s.inst.ConnectDB(ctx, catalogDatabase)
	if err != nil {
		return nil, s.rpcErr(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT gid, state, participants, participant_xids FROM pgshard.xact_decisions ORDER BY created_at, gid`)
	if err != nil {
		return nil, s.rpcErr(err)
	}
	stored, err := pgx.CollectRows(rows, pgx.RowToStructByPos[decisionRow])
	if err != nil {
		return nil, s.rpcErr(err)
	}
	for _, r := range stored {
		resp.Decisions = append(resp.Decisions, twopc.DecisionToProto(r.decision()))
	}
	return resp, nil
}

// decisionRow is one xact_decisions row as the catalog stores it: the
// participants and their transaction ids in two arrays paired by position.
// This is the only place that pairing is read, so nothing downstream can
// mis-associate a transaction id with a shard.
type decisionRow struct {
	GID          string
	State        string
	Participants []int32
	XIDs         []string
}

func (r decisionRow) decision() twopc.Decision {
	d := twopc.Decision{GID: r.GID, State: twopc.State(r.State)}
	// Arrays of different lengths cannot be paired, and the writer sets
	// both in one statement, so a mismatch means the row is not what it
	// claims to be. Dropping every id is the safe reading: a commit the
	// participant does not hold becomes unverifiable and fails the
	// restore, rather than being decided against the wrong transaction.
	paired := len(r.Participants) == len(r.XIDs)
	for i, shard := range r.Participants {
		p := twopc.Participation{Shard: shard}
		if paired {
			p.XID = r.XIDs[i]
		}
		d.Participants = append(d.Participants, p)
	}
	return d
}

// ListPreparedTransactions lists the pgshard-coordinated prepared
// transactions of this instance. Read-only.
func (s *Server) ListPreparedTransactions(ctx context.Context, _ *pgshardv1.ListPreparedTransactionsRequest) (*pgshardv1.ListPreparedTransactionsResponse, error) {
	resp := &pgshardv1.ListPreparedTransactionsResponse{Epoch: s.epoch.Current()}
	prepared, err := (&instanceParticipant{inst: s.inst}).Prepared(ctx)
	if err != nil {
		return nil, s.rpcErr(err)
	}
	for _, gid := range slices.Sorted(maps.Keys(prepared)) {
		resp.Prepared = append(resp.Prepared, &pgshardv1.PreparedTransaction{Gid: gid, Database: prepared[gid]})
	}
	return resp, nil
}

// ReconcilePreparedTransactions fences and applies the decision log to
// this instance's prepared transactions.
func (s *Server) ReconcilePreparedTransactions(ctx context.Context, req *pgshardv1.ReconcilePreparedTransactionsRequest) (*pgshardv1.ReconcilePreparedTransactionsResponse, error) {
	resp := &pgshardv1.ReconcilePreparedTransactionsResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		return nil, s.rpcErr(err)
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	decisions := make([]twopc.Decision, 0, len(req.GetDecisions()))
	for _, d := range req.GetDecisions() {
		decisions = append(decisions, twopc.DecisionFromProto(d))
	}
	out, err := twopc.Reconcile(ctx, &instanceParticipant{inst: s.inst}, req.GetShardId(), decisions)
	resp.Committed, resp.RolledBack = uint32(out.Committed), uint32(out.RolledBack)
	resp.Contradictions, resp.Unverifiable, resp.Unreadable = out.Contradictions, out.Unverifiable, out.Unreadable
	if err != nil {
		return nil, s.rpcErr(err)
	}
	return resp, nil
}

// SetWriteFence fences and raises or releases the write fence in the
// catalog database.
func (s *Server) SetWriteFence(ctx context.Context, req *pgshardv1.SetWriteFenceRequest) (*pgshardv1.SetWriteFenceResponse, error) {
	resp := &pgshardv1.SetWriteFenceResponse{Epoch: s.epoch.Current()}
	ctx, endTerm, err := s.fenceCurrent(ctx, req.GetEpoch())
	if err != nil {
		return nil, s.rpcErr(err)
	}
	defer endTerm()
	resp.Epoch = req.GetEpoch()
	conn, err := s.inst.ConnectDB(ctx, catalogDatabase)
	if err != nil {
		return nil, s.rpcErr(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	return nil, s.rpcErr(catalog.SetWriteFence(ctx, conn, req.GetActive(), req.GetReason()))
}

// instanceParticipant finishes prepared transactions from the database
// each was prepared in.
type instanceParticipant struct {
	inst *Instance
}

func (p *instanceParticipant) with(ctx context.Context, database string, fn func(twopc.Conn) error) error {
	conn, err := p.inst.ConnectDB(ctx, database)
	if err != nil {
		return fmt.Errorf("database %s: %w", database, err)
	}
	defer func() { _ = conn.Close(ctx) }()
	return fn(pgxTwopcConn{conn})
}

func (p *instanceParticipant) Prepared(ctx context.Context) (map[string]string, error) {
	var out map[string]string
	err := p.with(ctx, "postgres", func(c twopc.Conn) (err error) {
		out, err = twopc.ListPrepared(ctx, c)
		return err
	})
	return out, err
}

func (p *instanceParticipant) Finish(ctx context.Context, database, gid string, commit bool) error {
	return p.with(ctx, database, func(c twopc.Conn) error { return twopc.Finish(ctx, c, gid, commit) })
}

func (p *instanceParticipant) XactStatus(ctx context.Context, xid string) (twopc.XactStatus, error) {
	var out twopc.XactStatus
	err := p.with(ctx, "postgres", func(c twopc.Conn) (err error) {
		out, err = twopc.QueryXactStatus(ctx, c, xid)
		return err
	})
	return out, err
}

type pgxTwopcConn struct{ *pgx.Conn }

func (c pgxTwopcConn) Exec(ctx context.Context, sql string, args ...any) (twopc.Tag, error) {
	return c.Conn.Exec(ctx, sql, args...)
}
