package snapshot

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// Beginner starts transactions; *pgx.Conn and *pgxpool.Pool satisfy it.
type Beginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// Load reads one Snapshot inside a single REPEATABLE READ transaction so
// every table is seen at the same point in time.
func Load(ctx context.Context, db Beginner) (*Snapshot, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	s := &Snapshot{
		ShardSets: map[string][]Range{},
		Serving:   map[ShardKey]Serving{},
		Databases: map[string]catalog.Database{},
		Tables:    map[TableKey]Placement{},
		Sequences: map[string]bool{},
	}
	if s.ShardMapGeneration, s.DesiredGeneration, err = catalog.Generations(ctx, tx); err != nil {
		return nil, fmt.Errorf("snapshot: generations: %w", err)
	}
	ranges, err := catalog.ListAllShardRanges(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: shard ranges: %w", err)
	}
	for _, r := range ranges {
		s.ShardSets[r.ShardSet] = append(s.ShardSets[r.ShardSet], rangeFromCatalog(r))
	}
	statuses, err := catalog.ListAllShardStatus(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: shard status: %w", err)
	}
	for _, st := range statuses {
		sv := Serving{Epoch: st.PrimaryEpoch, State: st.ServingState}
		if st.PrimaryEndpoint != nil {
			sv.PrimaryEndpoint = *st.PrimaryEndpoint
		}
		s.Serving[ShardKey{st.ShardSet, st.ShardID}] = sv
	}
	dbs, err := catalog.ListDatabases(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: databases: %w", err)
	}
	for _, d := range dbs {
		s.Databases[d.Name] = d
	}
	tables, err := catalog.ListAllTables(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: tables: %w", err)
	}
	tableStatus, err := catalog.ListAllTableStatus(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: table status: %w", err)
	}
	effective := map[TableKey]catalog.TableStatus{}
	for _, ts := range tableStatus {
		effective[TableKey{ts.Database, ts.SchemaName, ts.TableName}] = ts
	}
	for _, t := range tables {
		key := TableKey{t.Database, t.SchemaName, t.TableName}
		if ts, ok := effective[key]; ok && ts.EffectivePlacement != nil {
			p := Placement{Placement: *ts.EffectivePlacement, Generation: ts.EffectiveGeneration}
			if ts.EffectiveShardKey != nil {
				p.ShardKey = *ts.EffectiveShardKey
			}
			p.SequenceColumns = t.SequenceColumns
			s.Tables[key] = p
			continue
		}
		if t.Placement == "unsharded" {
			s.Tables[key] = Placement{Placement: t.Placement, Generation: t.DesiredGeneration}
		}
	}
	fence, err := catalog.ReadWriteFence(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: write fence: %w", err)
	}
	s.WriteFence = fence.Active
	names, err := catalog.ListSequenceNames(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: sequences: %w", err)
	}
	for _, n := range names {
		s.Sequences[n] = true
	}
	return s, tx.Commit(ctx)
}

// LoadRoles reads role verifiers. The connection must be allowed to read
// pgshard.roles.verifier (pgshard_system or pgshard_admin).
func LoadRoles(ctx context.Context, q catalog.Querier) (*Roles, error) {
	rows, err := q.Query(ctx, `SELECT rolname, coalesce(verifier, '') FROM pgshard.roles`)
	if err != nil {
		return nil, fmt.Errorf("snapshot: roles: %w", err)
	}
	defer rows.Close()
	r := &Roles{verifiers: map[string]string{}}
	for rows.Next() {
		var name, verifier string
		if err := rows.Scan(&name, &verifier); err != nil {
			return nil, err
		}
		r.verifiers[name] = verifier
	}
	return r, rows.Err()
}
