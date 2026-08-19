package admin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// StreamSource reads change streams and their per-shard slot status from
// the catalog database. A CatalogSource that also implements it enables the
// streams pages.
type StreamSource interface {
	Streams(ctx context.Context) ([]catalog.Stream, error)
	StreamStatus(ctx context.Context, stream string) ([]catalog.StreamStatus, error)
}

// ErrStreamNotFound is returned by BuildStreamDetail for an unknown stream.
var ErrStreamNotFound = errors.New("stream not found")

// ErrNoStreamSource is returned when the admin runs without a catalog DSN.
var ErrNoStreamSource = errors.New("streams need --catalog-dsn")

// StreamSummary is one row of the streams list.
type StreamSummary struct {
	Name             string    `json:"name"`
	Database         string    `json:"database"`
	TwoPhase         bool      `json:"twoPhase"`
	State            string    `json:"state"`
	CreatedAt        time.Time `json:"createdAt"`
	Shards           int       `json:"shards"`
	ActiveSlots      int       `json:"activeSlots"`
	InactiveSlots    int       `json:"inactiveSlots"`
	LostSlots        int       `json:"lostSlots"`
	MaxRetainedBytes int64     `json:"maxRetainedBytes"`
	Synced           bool      `json:"synced"`
	Lost             bool      `json:"lost"`
}

// StreamsOverview is the streams list and the counts the overview card shows.
type StreamsOverview struct {
	Streams []StreamSummary `json:"streams"`
	Count   int             `json:"count"`
	Lost    int             `json:"lost"`
}

// SlotRow is one shard's slot in the stream detail.
type SlotRow struct {
	ShardSet           string    `json:"shardSet"`
	ShardID            int32     `json:"shardId"`
	Slot               string    `json:"slot"`
	Active             bool      `json:"active"`
	RestartLSN         string    `json:"restartLsn"`
	ConfirmedFlushLSN  string    `json:"confirmedFlushLsn"`
	RetainedBytes      int64     `json:"retainedBytes"`
	WALStatus          string    `json:"walStatus"`
	InvalidationReason string    `json:"invalidationReason"`
	Synced             bool      `json:"synced"`
	Failover           bool      `json:"failover"`
	UpdatedAt          time.Time `json:"updatedAt"`
	Lost               bool      `json:"lost"`
}

// StreamDetail is one stream with its per-shard slots.
type StreamDetail struct {
	StreamSummary
	Slots []SlotRow `json:"slots"`
}

// FormatLSN renders a WAL position as PostgreSQL does (X/Y).
func FormatLSN(lsn uint64) string { return fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn)) }

func summarize(st catalog.Stream, rows []catalog.StreamStatus) StreamSummary {
	s := StreamSummary{Name: st.Name, Database: st.Database, TwoPhase: st.TwoPhase, State: st.State, CreatedAt: st.CreatedAt,
		Lost: st.State == catalog.StreamLost, Synced: len(rows) > 0}
	for _, r := range rows {
		s.Shards++
		if r.Active {
			s.ActiveSlots++
		} else {
			s.InactiveSlots++
		}
		if r.WALStatus == "lost" {
			s.LostSlots++
			s.Lost = true
		}
		if r.RetainedBytes > s.MaxRetainedBytes {
			s.MaxRetainedBytes = r.RetainedBytes
		}
		if !r.Synced {
			s.Synced = false
		}
	}
	return s
}

// BuildStreamsOverview lists every stream with its slot summary.
func BuildStreamsOverview(ctx context.Context, src StreamSource) (*StreamsOverview, error) {
	if src == nil {
		return nil, ErrNoStreamSource
	}
	streams, err := src.Streams(ctx)
	if err != nil {
		return nil, err
	}
	status, err := src.StreamStatus(ctx, "")
	if err != nil {
		return nil, err
	}
	byStream := map[string][]catalog.StreamStatus{}
	for _, r := range status {
		byStream[r.Stream] = append(byStream[r.Stream], r)
	}
	out := &StreamsOverview{Streams: []StreamSummary{}}
	for _, st := range streams {
		s := summarize(st, byStream[st.Name])
		out.Streams = append(out.Streams, s)
		out.Count++
		if s.Lost {
			out.Lost++
		}
	}
	sort.Slice(out.Streams, func(i, j int) bool { return out.Streams[i].Name < out.Streams[j].Name })
	return out, nil
}

// BuildStreamDetail returns one stream with its per-shard slot rows.
func BuildStreamDetail(ctx context.Context, src StreamSource, name string) (*StreamDetail, error) {
	if src == nil {
		return nil, ErrNoStreamSource
	}
	streams, err := src.Streams(ctx)
	if err != nil {
		return nil, err
	}
	var found *catalog.Stream
	for i := range streams {
		if streams[i].Name == name {
			found = &streams[i]
			break
		}
	}
	if found == nil {
		return nil, fmt.Errorf("%w: %s", ErrStreamNotFound, name)
	}
	rows, err := src.StreamStatus(ctx, name)
	if err != nil {
		return nil, err
	}
	d := &StreamDetail{StreamSummary: summarize(*found, rows), Slots: []SlotRow{}}
	for _, r := range rows {
		d.Slots = append(d.Slots, SlotRow{ShardSet: r.ShardSet, ShardID: r.ShardID, Slot: r.Slot, Active: r.Active,
			RestartLSN: FormatLSN(r.RestartLSN), ConfirmedFlushLSN: FormatLSN(r.ConfirmedFlushLSN), RetainedBytes: r.RetainedBytes,
			WALStatus: r.WALStatus, InvalidationReason: r.InvalidationReason, Synced: r.Synced, Failover: r.Failover,
			UpdatedAt: r.UpdatedAt, Lost: r.WALStatus == "lost"})
	}
	return d, nil
}

// Streams implements StreamSource.
func (p PgxCatalog) Streams(ctx context.Context) ([]catalog.Stream, error) {
	return withConn(ctx, p, func(ctx context.Context, conn *pgx.Conn) ([]catalog.Stream, error) {
		return catalog.ListStreams(ctx, conn)
	})
}

// StreamStatus implements StreamSource.
func (p PgxCatalog) StreamStatus(ctx context.Context, stream string) ([]catalog.StreamStatus, error) {
	return withConn(ctx, p, func(ctx context.Context, conn *pgx.Conn) ([]catalog.StreamStatus, error) {
		return catalog.ListStreamStatus(ctx, conn, stream)
	})
}
