package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// StreamPublication is the FOR ALL TABLES publication change streams decode.
const StreamPublication = "pgshard_all"

// StreamAdmin creates and drops change streams: the catalog row plus one
// failover-enabled pgoutput slot per shard, created through a superuser
// connection to each shard's primary in the stream's database.
type StreamAdmin struct {
	Pool   *pgxpool.Pool
	Shards DatabaseDialer
}

// StreamSlot is the slot of a stream on one shard.
type StreamSlot struct {
	Shard ShardRef
	Slot  string
	LSN   uint64
}

// Create registers the stream and creates its slots; it is idempotent for
// slots that already exist. The stream becomes active once every shard has
// its slot; a failure leaves it creating with the slots made so far.
func (a *StreamAdmin) Create(ctx context.Context, name, database string, twoPhase bool, shardSet string) ([]StreamSlot, error) {
	if shardSet == "" {
		shardSet = "default"
	}
	if database == "" {
		return nil, errors.New("database is required")
	}
	if err := catalog.CreateStream(ctx, a.Pool, catalog.Stream{Name: name, Database: database, TwoPhase: twoPhase}); err != nil {
		return nil, err
	}
	shards, err := (&Resolver{Pool: a.Pool}).listShards(ctx, shardSet)
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("shard set %q has no shards", shardSet)
	}
	groups, err := (&StreamMonitor{Pool: a.Pool}).groupNames(ctx)
	if err != nil {
		return nil, err
	}
	var out []StreamSlot
	for _, sh := range shards {
		slot := catalog.StreamSlotName(name, groups[sh])
		lsn, err := a.createSlot(ctx, sh, database, slot, twoPhase)
		if err != nil {
			return out, fmt.Errorf("shard %s/%d: %w", sh.Set, sh.ID, err)
		}
		out = append(out, StreamSlot{Shard: sh, Slot: slot, LSN: lsn})
	}
	if err := catalog.SetStreamState(ctx, a.Pool, name, catalog.StreamActive); err != nil {
		return out, err
	}
	return out, nil
}

func (a *StreamAdmin) createSlot(ctx context.Context, sh ShardRef, database, slot string, twoPhase bool) (uint64, error) {
	conn, err := a.Shards.DialDatabase(ctx, sh.Set, sh.ID, database)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := ensurePublication(ctx, conn); err != nil {
		return 0, err
	}
	var lsn int64
	err = scanOne(ctx, conn, `SELECT lsn - '0/0'::pg_lsn FROM pg_create_logical_replication_slot($1, 'pgoutput', false, $2, true)`, &lsn, slot, twoPhase)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42710" {
		return queryExisting(ctx, conn, slot)
	}
	return uint64(lsn), err
}

func queryExisting(ctx context.Context, conn ShardConn, slot string) (uint64, error) {
	var lsn int64
	err := scanOne(ctx, conn, `SELECT coalesce(confirmed_flush_lsn - '0/0'::pg_lsn, 0) FROM pg_replication_slots WHERE slot_name = $1`, &lsn, slot)
	return uint64(lsn), err
}

func ensurePublication(ctx context.Context, conn ShardConn) error {
	var n int64
	if err := scanOne(ctx, conn, `SELECT count(*) FROM pg_publication WHERE pubname = $1`, &n, StreamPublication); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := conn.Exec(ctx, "CREATE PUBLICATION "+StreamPublication+" FOR ALL TABLES")
	return err
}

func scanOne(ctx context.Context, conn ShardConn, sql string, dst any, args ...any) error {
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if rows.Err() != nil {
			return rows.Err()
		}
		return errors.New("no row")
	}
	if err := rows.Scan(dst); err != nil {
		return err
	}
	rows.Close()
	return rows.Err()
}

// Drop drops the stream's slot on every shard (missing slots are fine) and
// deletes the catalog rows. An unreachable shard keeps the stream.
func (a *StreamAdmin) Drop(ctx context.Context, name string) error {
	if !catalog.ValidStreamName(name) {
		return fmt.Errorf("invalid stream name %q", name)
	}
	shards, err := (&Resolver{Pool: a.Pool}).listShards(ctx, "")
	if err != nil {
		return err
	}
	groups, err := (&StreamMonitor{Pool: a.Pool}).groupNames(ctx)
	if err != nil {
		return err
	}
	for _, sh := range shards {
		slot := catalog.StreamSlotName(name, groups[sh])
		if err := a.dropSlot(ctx, sh, slot); err != nil {
			return fmt.Errorf("shard %s/%d: %w", sh.Set, sh.ID, err)
		}
	}
	return catalog.DeleteStream(ctx, a.Pool, name)
}

func (a *StreamAdmin) dropSlot(ctx context.Context, sh ShardRef, slot string) error {
	conn, err := a.Shards.DialDatabase(ctx, sh.Set, sh.ID, "")
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, slot)
	return err
}

// CreateStream implements Controller.CreateStream.
func (s *Server) CreateStream(ctx context.Context, req *pgshardv1.CreateStreamRequest) (*pgshardv1.CreateStreamResponse, error) {
	if s.Streams == nil {
		return nil, status.Error(codes.Unimplemented, "the controller has no shard access configured for streams")
	}
	slots, err := s.Streams.Create(ctx, req.GetStream(), req.GetDatabase(), req.GetTwoPhase(), req.GetShardSet())
	resp := &pgshardv1.CreateStreamResponse{}
	for _, sl := range slots {
		resp.Slots = append(resp.Slots, &pgshardv1.CreateStreamResponse_Slot{Shard: &pgshardv1.ShardRef{ShardSet: sl.Shard.Set, ShardId: uint32(sl.Shard.ID)}, Slot: sl.Slot, Lsn: sl.LSN})
	}
	if err != nil {
		resp.Error = &pgshardv1.Error{Message: err.Error()}
	}
	return resp, nil
}

// DropStream implements Controller.DropStream.
func (s *Server) DropStream(ctx context.Context, req *pgshardv1.DropStreamRequest) (*pgshardv1.DropStreamResponse, error) {
	if s.Streams == nil {
		return nil, status.Error(codes.Unimplemented, "the controller has no shard access configured for streams")
	}
	resp := &pgshardv1.DropStreamResponse{}
	if err := s.Streams.Drop(ctx, req.GetStream()); err != nil {
		resp.Error = &pgshardv1.Error{Message: err.Error()}
	}
	return resp, nil
}
