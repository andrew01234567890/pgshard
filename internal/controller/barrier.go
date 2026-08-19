package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// RestorePointPrefix starts the WAL restore point name of every barrier.
const RestorePointPrefix = "pgshard-"

// CatalogGroup is the name the barrier records the catalog group under.
const CatalogGroup = "catalog"

// Barrier timing defaults.
const (
	DefaultDrainTimeout   = 30 * time.Second
	DefaultArchiveTimeout = 2 * time.Minute
	DefaultBarrierPoll    = 200 * time.Millisecond
)

var barrierName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// GroupRef is one group taking part in a barrier: the catalog or one shard.
type GroupRef struct {
	Name string
	// Set and ID locate a shard group; both are empty for the catalog.
	Set string
	ID  int32
}

// Catalog reports whether g is the catalog group.
func (g GroupRef) Catalog() bool { return g.Set == "" }

// RestorePointResult is where a group's restore point landed.
type RestorePointResult struct {
	LSN        uint64
	Timeline   int64
	WALSegment string
}

// BarrierGroups runs the per-group steps of a barrier.
type BarrierGroups interface {
	// List returns every group: the catalog first, then the shards.
	List(ctx context.Context) ([]GroupRef, error)
	// PreparedCount counts router-coordinated prepared transactions on g.
	PreparedCount(ctx context.Context, g GroupRef) (int, error)
	// CreateRestorePoint creates the named restore point on g's primary and
	// forces the WAL segment holding it out to the archive.
	CreateRestorePoint(ctx context.Context, g GroupRef, name string) (RestorePointResult, error)
	// ArchivedThrough reports the newest WAL segment g archived. A group
	// that does not archive WAL returns an error.
	ArchivedThrough(ctx context.Context, g GroupRef) (string, error)
}

// BarrierStore is the catalog side of a barrier.
type BarrierStore interface {
	// Fence raises or releases the cluster write fence.
	Fence(ctx context.Context, active bool, reason string) error
	// FencedAt returns when the current fence was raised.
	FencedAt(ctx context.Context) (time.Time, error)
	// DecisionWatermark returns a value that grows with every decision row
	// ever inserted, whether or not the row still exists.
	DecisionWatermark(ctx context.Context) (int64, error)
	// PreparingCount counts decision rows still preparing.
	PreparingCount(ctx context.Context) (int, error)
	// Exists reports whether a restore point of that name is recorded.
	Exists(ctx context.Context, name string) (bool, error)
	// Record inserts the restore point and returns its id.
	Record(ctx context.Context, rp RestorePoint) (string, error)
	// ShardMapGeneration reads the current shard map generation.
	ShardMapGeneration(ctx context.Context) (int64, error)
}

// GroupRestorePoint is the recorded restore point of one group.
type GroupRestorePoint struct {
	Group      string `json:"group"`
	LSN        uint64 `json:"lsn"`
	Timeline   int64  `json:"timeline"`
	WALSegment string `json:"wal_segment"`
}

// RestorePoint is one row of pgshard.restore_points.
type RestorePoint struct {
	ID                 string
	Name               string
	ShardMapGeneration int64
	Certified          bool
	Groups             []GroupRestorePoint
	CreatedAt          time.Time
}

// RestorePointName is the WAL restore point name of a barrier.
func RestorePointName(barrier string) string { return RestorePointPrefix + barrier }

// Barrier creates certified restore points: writes are paused, two-phase
// commits drained, a named restore point created on every group and
// archived, then the pause is lifted and the point recorded. Any failure
// lifts the pause and records nothing.
type Barrier struct {
	Store  BarrierStore
	Groups BarrierGroups
	// Resolver, when set, runs between drain polls so preparing rows of dead
	// routers are aborted instead of blocking the drain.
	Resolver *Resolver
	Logger   *slog.Logger

	DrainTimeout   time.Duration
	ArchiveTimeout time.Duration
	Poll           time.Duration
	Now            func() time.Time
}

func (b *Barrier) logger() *slog.Logger {
	if b.Logger == nil {
		return slog.Default()
	}
	return b.Logger
}

func (b *Barrier) now() time.Time {
	if b.Now != nil {
		return b.Now()
	}
	return time.Now()
}

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// ErrBarrierExists is returned for a name that is already recorded.
var ErrBarrierExists = errors.New("barrier: a restore point of that name exists")

// ErrBarrierName is returned for a name that is not a DNS-style label.
var ErrBarrierName = errors.New("barrier: name must be 1-63 lowercase letters, digits and hyphens")

// Run creates the barrier named name.
func (b *Barrier) Run(ctx context.Context, name string) (RestorePoint, error) {
	if !barrierName.MatchString(name) {
		return RestorePoint{}, ErrBarrierName
	}
	if exists, err := b.Store.Exists(ctx, name); err != nil {
		return RestorePoint{}, fmt.Errorf("barrier %s: %w", name, err)
	} else if exists {
		return RestorePoint{}, fmt.Errorf("barrier %s: %w", name, ErrBarrierExists)
	}
	groups, err := b.Groups.List(ctx)
	if err != nil {
		return RestorePoint{}, fmt.Errorf("barrier %s: groups: %w", name, err)
	}
	if len(groups) == 0 {
		return RestorePoint{}, fmt.Errorf("barrier %s: no groups", name)
	}
	if err := b.Store.Fence(ctx, true, "barrier "+name); err != nil {
		return RestorePoint{}, fmt.Errorf("barrier %s: fence: %w", name, err)
	}
	b.logger().Info("barrier: write fence raised", "barrier", name)
	rp, err := b.fenced(ctx, name, groups)
	// The fence is released even when ctx is done.
	release, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if rerr := b.Store.Fence(release, false, ""); rerr != nil {
		b.logger().Error("barrier: releasing the write fence failed; routers keep refusing writes", "barrier", name, "err", rerr)
		if err == nil {
			err = fmt.Errorf("barrier %s: release fence: %w", name, rerr)
		}
	} else {
		b.logger().Info("barrier: write fence released", "barrier", name)
	}
	if err != nil {
		return RestorePoint{}, err
	}
	return rp, nil
}

// fenced runs the steps between raising and releasing the fence.
func (b *Barrier) fenced(ctx context.Context, name string, groups []GroupRef) (RestorePoint, error) {
	if _, err := b.Store.FencedAt(ctx); err != nil {
		return RestorePoint{}, fmt.Errorf("barrier %s: %w", name, err)
	}
	// Decision rows are deleted once a transaction finishes, so a row count
	// cannot prove nothing started under the fence; the watermark can.
	before, err := b.Store.DecisionWatermark(ctx)
	if err != nil {
		return RestorePoint{}, fmt.Errorf("barrier %s: %w", name, err)
	}
	if err := b.drain(ctx, name, groups); err != nil {
		return RestorePoint{}, err
	}
	rp := RestorePoint{Name: name, Certified: true}
	for _, g := range groups {
		res, err := b.Groups.CreateRestorePoint(ctx, g, RestorePointName(name))
		if err != nil {
			return RestorePoint{}, fmt.Errorf("barrier %s: restore point on %s: %w", name, g.Name, err)
		}
		rp.Groups = append(rp.Groups, GroupRestorePoint{Group: g.Name, LSN: res.LSN, Timeline: res.Timeline, WALSegment: res.WALSegment})
	}
	if err := b.awaitArchived(ctx, name, groups, rp.Groups); err != nil {
		return RestorePoint{}, err
	}
	after, err := b.Store.DecisionWatermark(ctx)
	if err != nil {
		return RestorePoint{}, fmt.Errorf("barrier %s: %w", name, err)
	}
	if n := after - before; n > 0 {
		return RestorePoint{}, fmt.Errorf("barrier %s: %d two-phase transaction(s) started while the fence was up; the restore points are not consistent, retry", name, n)
	}
	if rp.ShardMapGeneration, err = b.Store.ShardMapGeneration(ctx); err != nil {
		return RestorePoint{}, fmt.Errorf("barrier %s: shard map generation: %w", name, err)
	}
	rp.CreatedAt = b.now()
	if rp.ID, err = b.Store.Record(ctx, rp); err != nil {
		return RestorePoint{}, fmt.Errorf("barrier %s: record: %w", name, err)
	}
	b.logger().Info("barrier: certified restore point recorded", "barrier", name, "groups", len(rp.Groups))
	return rp, nil
}

// drain waits until no decision row is preparing and no group holds a
// router-coordinated prepared transaction.
func (b *Barrier) drain(ctx context.Context, name string, groups []GroupRef) error {
	deadline := b.now().Add(orDefault(b.DrainTimeout, DefaultDrainTimeout))
	for {
		if b.Resolver != nil {
			if _, err := b.Resolver.Resolve(ctx, ""); err != nil {
				b.logger().Warn("barrier: resolver pass failed", "barrier", name, "err", err)
			}
		}
		pending, err := b.inFlight(ctx, groups)
		if err != nil {
			return fmt.Errorf("barrier %s: drain: %w", name, err)
		}
		if pending == "" {
			return nil
		}
		if !b.now().Before(deadline) {
			return fmt.Errorf("barrier %s: drain: still in flight after %s: %s", name, orDefault(b.DrainTimeout, DefaultDrainTimeout), pending)
		}
		if err := b.sleep(ctx); err != nil {
			return fmt.Errorf("barrier %s: drain: %w", name, err)
		}
	}
}

// inFlight describes what still blocks the drain, "" when nothing does.
func (b *Barrier) inFlight(ctx context.Context, groups []GroupRef) (string, error) {
	n, err := b.Store.PreparingCount(ctx)
	if err != nil {
		return "", err
	}
	if n > 0 {
		return fmt.Sprintf("%d decision row(s) preparing", n), nil
	}
	for _, g := range groups {
		c, err := b.Groups.PreparedCount(ctx, g)
		if err != nil {
			return "", fmt.Errorf("%s: %w", g.Name, err)
		}
		if c > 0 {
			return fmt.Sprintf("%d prepared transaction(s) on %s", c, g.Name), nil
		}
	}
	return "", nil
}

// awaitArchived waits until every group's archive holds the segment of its
// restore point.
func (b *Barrier) awaitArchived(ctx context.Context, name string, groups []GroupRef, points []GroupRestorePoint) error {
	deadline := b.now().Add(orDefault(b.ArchiveTimeout, DefaultArchiveTimeout))
	for i, g := range groups {
		for {
			archived, err := b.Groups.ArchivedThrough(ctx, g)
			if err != nil {
				return fmt.Errorf("barrier %s: archive of %s: %w", name, g.Name, err)
			}
			if archived >= points[i].WALSegment {
				break
			}
			if !b.now().Before(deadline) {
				return fmt.Errorf("barrier %s: archive of %s: %s not archived after %s (last archived %q)", name, g.Name, points[i].WALSegment, orDefault(b.ArchiveTimeout, DefaultArchiveTimeout), archived)
			}
			if err := b.sleep(ctx); err != nil {
				return fmt.Errorf("barrier %s: archive of %s: %w", name, g.Name, err)
			}
		}
	}
	return nil
}

func (b *Barrier) sleep(ctx context.Context) error {
	t := time.NewTimer(orDefault(b.Poll, DefaultBarrierPoll))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// PGBarrierStore is the BarrierStore over the catalog.
type PGBarrierStore struct {
	Pool *pgxpool.Pool
}

// Fence implements BarrierStore.
func (s *PGBarrierStore) Fence(ctx context.Context, active bool, reason string) error {
	return catalog.SetWriteFence(ctx, s.Pool, active, reason)
}

// FencedAt implements BarrierStore.
func (s *PGBarrierStore) FencedAt(ctx context.Context) (time.Time, error) {
	f, err := catalog.ReadWriteFence(ctx, s.Pool)
	if err != nil {
		return time.Time{}, err
	}
	if !f.Active || f.FencedAt == nil {
		return time.Time{}, errors.New("write fence is not raised")
	}
	return *f.FencedAt, nil
}

// PreparingCount implements BarrierStore.
func (s *PGBarrierStore) PreparingCount(ctx context.Context) (int, error) {
	var n int
	err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM pgshard.xact_decisions WHERE state = 'preparing'`).Scan(&n)
	return n, err
}

// DecisionWatermark implements BarrierStore.
func (s *PGBarrierStore) DecisionWatermark(ctx context.Context) (int64, error) {
	var n int64
	err := s.Pool.QueryRow(ctx, `SELECT coalesce(pg_sequence_last_value('pgshard.xact_decisions_seq_seq'), 0)`).Scan(&n)
	return n, err
}

// Exists implements BarrierStore.
func (s *PGBarrierStore) Exists(ctx context.Context, name string) (bool, error) {
	var exists bool
	err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pgshard.restore_points WHERE name = $1)`, name).Scan(&exists)
	return exists, err
}

// Record implements BarrierStore.
func (s *PGBarrierStore) Record(ctx context.Context, rp RestorePoint) (string, error) {
	perGroup := map[string]GroupRestorePoint{}
	for _, g := range rp.Groups {
		perGroup[g.Group] = g
	}
	body, err := json.Marshal(perGroup)
	if err != nil {
		return "", err
	}
	var id string
	err = s.Pool.QueryRow(ctx, `INSERT INTO pgshard.restore_points (id, name, shard_map_generation, per_group, certified, created_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, $5) RETURNING id::text`, rp.Name, rp.ShardMapGeneration, body, rp.Certified, rp.CreatedAt).Scan(&id)
	return id, err
}

// ShardMapGeneration implements BarrierStore.
func (s *PGBarrierStore) ShardMapGeneration(ctx context.Context) (int64, error) {
	gen, _, err := catalog.Generations(ctx, s.Pool)
	return gen, err
}

// ListRestorePoints returns recorded restore points, newest first.
func ListRestorePoints(ctx context.Context, q catalog.Querier, certifiedOnly bool) ([]RestorePoint, error) {
	rows, err := q.Query(ctx, `SELECT id::text, name, shard_map_generation, per_group, certified, created_at
		FROM pgshard.restore_points WHERE NOT $1 OR certified ORDER BY created_at DESC, name`, certifiedOnly)
	if err != nil {
		return nil, err
	}
	type row struct {
		ID         string
		Name       string
		Generation int64
		PerGroup   []byte
		Certified  bool
		CreatedAt  time.Time
	}
	list, err := pgx.CollectRows(rows, pgx.RowToStructByPos[row])
	if err != nil {
		return nil, err
	}
	out := make([]RestorePoint, 0, len(list))
	for _, r := range list {
		rp := RestorePoint{ID: r.ID, Name: r.Name, ShardMapGeneration: r.Generation, Certified: r.Certified, CreatedAt: r.CreatedAt}
		var perGroup map[string]GroupRestorePoint
		if err := json.Unmarshal(r.PerGroup, &perGroup); err != nil {
			return nil, fmt.Errorf("restore point %s: %w", r.Name, err)
		}
		for name, g := range perGroup {
			g.Group = name
			rp.Groups = append(rp.Groups, g)
		}
		sort.Slice(rp.Groups, func(i, j int) bool { return rp.Groups[i].Group < rp.Groups[j].Group })
		out = append(out, rp)
	}
	return out, nil
}

// SQLBarrierGroups runs the group steps over SQL: the catalog through the
// pool, the shards through the resolver's dialer.
type SQLBarrierGroups struct {
	Pool   *pgxpool.Pool
	Shards ShardDialer
}

// List implements BarrierGroups.
func (s *SQLBarrierGroups) List(ctx context.Context) ([]GroupRef, error) {
	rows, err := s.Pool.Query(ctx, `SELECT group_name, shard_set, shard_id FROM pgshard.shard_status ORDER BY shard_set, shard_id`)
	if err != nil {
		return nil, err
	}
	shards, err := pgx.CollectRows(rows, pgx.RowToStructByPos[GroupRef])
	if err != nil {
		return nil, err
	}
	return append([]GroupRef{{Name: CatalogGroup}}, shards...), nil
}

type groupConn interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (s *SQLBarrierGroups) with(ctx context.Context, g GroupRef, fn func(groupConn) error) error {
	if g.Catalog() {
		return fn(s.Pool)
	}
	if s.Shards == nil {
		return fmt.Errorf("no shard access configured")
	}
	conn, err := s.Shards.Dial(ctx, g.Set, g.ID)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	return fn(conn)
}

func scalar[T any](ctx context.Context, c groupConn, sql string, args ...any) (T, error) {
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		var zero T
		return zero, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowTo[T])
}

// PreparedCount implements BarrierGroups.
func (s *SQLBarrierGroups) PreparedCount(ctx context.Context, g GroupRef) (int, error) {
	var n int
	err := s.with(ctx, g, func(c groupConn) (err error) {
		n, err = scalar[int](ctx, c, `SELECT count(*)::int FROM pg_prepared_xacts WHERE gid LIKE $1`, GIDPrefix+"%")
		return err
	})
	return n, err
}

// CreateRestorePoint implements BarrierGroups.
func (s *SQLBarrierGroups) CreateRestorePoint(ctx context.Context, g GroupRef, name string) (RestorePointResult, error) {
	var res RestorePointResult
	err := s.with(ctx, g, func(c groupConn) error {
		rows, err := c.Query(ctx, `SELECT (lsn - '0/0'::pg_lsn)::bigint, pg_walfile_name(lsn), timeline_id::bigint
			FROM pg_create_restore_point($1) AS lsn, pg_control_checkpoint()`, name)
		if err != nil {
			return err
		}
		row, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[struct {
			LSN      int64
			Segment  string
			Timeline int64
		}])
		if err != nil {
			return err
		}
		res = RestorePointResult{LSN: uint64(row.LSN), Timeline: row.Timeline, WALSegment: row.Segment}
		_, err = scalar[string](ctx, c, `SELECT pg_switch_wal()::text`)
		return err
	})
	return res, err
}

// ArchivedThrough implements BarrierGroups.
func (s *SQLBarrierGroups) ArchivedThrough(ctx context.Context, g GroupRef) (string, error) {
	var last string
	err := s.with(ctx, g, func(c groupConn) error {
		rows, err := c.Query(ctx, `SELECT coalesce(last_archived_wal, ''), current_setting('archive_mode') FROM pg_stat_archiver`)
		if err != nil {
			return err
		}
		row, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[struct {
			Last string
			Mode string
		}])
		if err != nil {
			return err
		}
		if row.Mode == "off" {
			return errors.New("archive_mode is off; the restore point cannot be certified")
		}
		last = row.Last
		return nil
	})
	return last, err
}
