package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// Beginner starts transactions; *pgx.Conn and *pgxpool.Pool satisfy it.
type Beginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// LoadServing reads only what a pooler enforces: the shard-map generation
// and the serving row of every shard. It is a fraction of Load -- no
// ranges, databases, tables, table status, rewrites, roles or sequences --
// and the difference matters because there is a pooler per shard member,
// each holding a LISTEN connection and reloading on every notification as
// well as on a timer. A cluster's catalog read load otherwise grows as
// pooler count times catalog size, and adding shards does both at once.
//
// The result is marked Partial. It must not be used to plan: its Tables
// and Databases are empty because they were never read, which is
// indistinguishable from a cluster that has none.
func LoadServing(ctx context.Context, db Beginner) (*Snapshot, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	s := &Snapshot{
		LoadedAt:        time.Now(),
		Partial:         true,
		ShardSets:       map[string][]Range{},
		Serving:         map[ShardKey]Serving{},
		Databases:       map[string]catalog.Database{},
		Tables:          map[TableKey]Placement{},
		Views:           map[TableKey]View{},
		Sequences:       map[string]bool{},
		ScalarFunctions: map[FunctionKey]bool{},
	}
	if s.ShardMapGeneration, s.DesiredGeneration, err = catalog.Generations(ctx, tx); err != nil {
		return nil, fmt.Errorf("snapshot: generations: %w", err)
	}
	statuses, err := catalog.ListAllShardStatus(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: shard status: %w", err)
	}
	for _, st := range statuses {
		sv := Serving{Epoch: st.PrimaryEpoch, State: st.ServingState, Migrating: st.Migrating}
		if st.PrimaryEndpoint != nil {
			sv.PrimaryEndpoint = *st.PrimaryEndpoint
		}
		s.Serving[ShardKey{st.ShardSet, st.ShardID}] = sv
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("snapshot: commit: %w", err)
	}
	return s, nil
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
		LoadedAt:        time.Now(),
		ShardSets:       map[string][]Range{},
		PGMajors:        map[string]int{},
		Serving:         map[ShardKey]Serving{},
		Databases:       map[string]catalog.Database{},
		Tables:          map[TableKey]Placement{},
		Views:           map[TableKey]View{},
		Sequences:       map[string]bool{},
		ScalarFunctions: map[FunctionKey]bool{},
	}
	// Refuse a catalog migrated by a NEWER binary than this one, here rather
	// than only in Migrate. CheckCompatible existed for exactly this and had
	// one caller -- Migrate -- which only the operator's probe and the
	// router's dev bootstrap reach. So a router, pooler or controller still
	// on the previous release read a catalog the operator had already
	// migrated and failed somewhere deep in a query with a raw "column does
	// not exist", naming neither the version gap nor the component that
	// opened it.
	//
	// That window is not rare: the operator migrates the catalog and THEN
	// rolls the components, so every upgrade has one.
	//
	// Refusing here is safe. A failed load leaves the watcher's current
	// snapshot in place, so a component keeps serving what it already had
	// rather than crash-looping through the roll that would fix it -- and
	// if it can never reload, MaxAge fails it closed rather than letting it
	// serve a stale view for ever.
	if err := catalog.CheckCompatible(ctx, tx, nil); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
	if s.ShardMapGeneration, s.DesiredGeneration, err = catalog.Generations(ctx, tx); err != nil {
		return nil, fmt.Errorf("snapshot: generations: %w", err)
	}
	if s.ServingSet, err = catalog.ServingShardSet(ctx, tx); err != nil {
		return nil, fmt.Errorf("snapshot: serving shard set: %w", err)
	}
	ranges, err := catalog.ListAllShardRanges(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: shard ranges: %w", err)
	}
	for _, r := range ranges {
		s.ShardSets[r.ShardSet] = append(s.ShardSets[r.ShardSet], rangeFromCatalog(r))
	}
	sets, err := catalog.ListShardSets(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: shard sets: %w", err)
	}
	for _, set := range sets {
		// A retired set still has rows for as long as its groups are kept.
		// Counting its major would hold the cluster's SQL surface down to
		// the version it was upgraded away from.
		//
		// A set that is only proposed or provisioning does count, and that
		// is safe rather than lucky: a reshard stamps the pending set with
		// the serving set's own major, and an upgrade only starts when the
		// spec asks for a higher one, so nothing writes a pending major
		// below what already serves. A downgrade path would break that, and
		// would have to narrow this to the serving set.
		if set.State == catalog.ShardSetRetired || set.PGMajor == nil {
			continue
		}
		s.PGMajors[set.Name] = *set.PGMajor
	}
	statuses, err := catalog.ListAllShardStatus(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: shard status: %w", err)
	}
	for _, st := range statuses {
		sv := Serving{Epoch: st.PrimaryEpoch, State: st.ServingState, Migrating: st.Migrating}
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
			p := Placement{Placement: *ts.EffectivePlacement, Generation: ts.EffectiveGeneration, Migrating: ts.Migrating}
			if ts.EffectiveShardKey != nil {
				p.ShardKey = *ts.EffectiveShardKey
			}
			p.SequenceColumns = t.SequenceColumns
			p.ReferenceChecked = ts.ReferenceCheckedGeneration != nil && *ts.ReferenceCheckedGeneration == ts.EffectiveGeneration
			p.ReferenceHazards = ts.ReferenceHazards
			p.ShardKeyChecked = shardKeyChecked(ts)
			p.ShardKeyError = shardKeyError(ts)
			p.ShardKeyType = shardKeyType(ts)
			s.Tables[key] = p
			continue
		}
		if t.Placement == "unsharded" {
			s.Tables[key] = Placement{Placement: t.Placement, Generation: t.DesiredGeneration}
		}
	}
	if err := loadViews(ctx, tx, s); err != nil {
		return nil, fmt.Errorf("snapshot: views: %w", err)
	}
	// A table that is a reference table only because its database defaults
	// to reference placement has no row in pgshard.tables, so the loop
	// above never sees it. The inspection pass records what it found on a
	// status row, and that row is the only record pgshard has that the
	// table exists at all -- so it is what routers plan from.
	for key, ts := range effective {
		if _, ok := s.Tables[key]; ok || ts.EffectivePlacement == nil {
			continue
		}
		p := Placement{Placement: *ts.EffectivePlacement, Generation: ts.EffectiveGeneration, Migrating: ts.Migrating}
		if ts.EffectiveShardKey != nil {
			p.ShardKey = *ts.EffectiveShardKey
		}
		p.ReferenceChecked = ts.ReferenceCheckedGeneration != nil && *ts.ReferenceCheckedGeneration == ts.EffectiveGeneration
		p.ReferenceHazards = ts.ReferenceHazards
		p.ShardKeyChecked = shardKeyChecked(ts)
		p.ShardKeyError = shardKeyError(ts)
		p.ShardKeyType = shardKeyType(ts)
		s.Tables[key] = p
	}
	rewrites, err := catalog.PendingRewrites(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: rewrites: %w", err)
	}
	for _, rw := range rewrites {
		schema := rw.Rewrite.Schema
		if schema == "" {
			schema = "public"
		}
		key := TableKey{rw.Database, schema, rw.Rewrite.Table}
		p, ok := s.Tables[key]
		if !ok {
			continue
		}
		p.HiddenColumns = append(p.HiddenColumns, rw.Rewrite.HiddenColumn(rw.ID))
		if len(rw.Rewrite.Columns) > 0 {
			p.VisibleColumns = rw.Rewrite.Columns
		}
		s.Tables[key] = p
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
	funcs, err := catalog.ListDeclaredFunctions(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: functions: %w", err)
	}
	// Two passes, because one aggregate row anywhere in the database
	// withdraws the name however many scalar rows it has: the router
	// matches by name alone, so an ambiguous name is not a name it can
	// decide.
	aggregates := map[FunctionKey]bool{}
	for _, f := range funcs {
		if f.Aggregate() {
			aggregates[FunctionKey{Database: f.Database, Name: f.Name}] = true
		}
	}
	for _, f := range funcs {
		k := FunctionKey{Database: f.Database, Name: f.Name}
		if !f.Aggregate() && !aggregates[k] {
			s.ScalarFunctions[k] = true
		}
	}
	s.index()
	return s, tx.Commit(ctx)
}

// LoadRoles reads role verifiers and login gates. The connection must be
// allowed to read pgshard.roles.verifier (pgshard_system or pgshard_admin).
func LoadRoles(ctx context.Context, q catalog.Querier) (*Roles, error) {
	rows, err := q.Query(ctx, `SELECT rolname, coalesce(verifier, ''), login, valid_until, connection_limit FROM pgshard.roles`)
	if err != nil {
		return nil, fmt.Errorf("snapshot: roles: %w", err)
	}
	defer rows.Close()
	r := &Roles{verifiers: map[string]RoleCred{}, catalogAccess: map[string]bool{}, adminAccess: map[string]bool{}}
	for rows.Next() {
		var name string
		var cred RoleCred
		if err := rows.Scan(&name, &cred.Verifier, &cred.CanLogin, &cred.ValidUntil, &cred.ConnectionLimit); err != nil {
			return nil, err
		}
		r.verifiers[name] = cred
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Who may open a session on the catalog database is asked of the
	// catalog server rather than read from pgshard.role_members, because
	// the server's answer is the one that matters and it is the only
	// complete one: pg_has_role is transitive, it is true for a superuser,
	// and the bootstrap credential a new cluster is first used with is a
	// superuser that no GRANT in the desired state ever mentions. The join
	// keeps a role queued in the catalog but not yet materialized on this
	// group from erroring the whole load.
	access, err := q.Query(ctx, `SELECT r.rolname FROM pgshard.roles r JOIN pg_roles pr ON pr.rolname = r.rolname
		WHERE pg_has_role(pr.oid, 'pgshard_admin'::regrole, 'USAGE') OR pg_has_role(pr.oid, 'pgshard_reader'::regrole, 'USAGE')`)
	if err != nil {
		return nil, fmt.Errorf("snapshot: catalog access: %w", err)
	}
	defer access.Close()
	for access.Next() {
		var name string
		if err := access.Scan(&name); err != nil {
			return nil, err
		}
		r.catalogAccess[name] = true
	}
	// Who may ACT on the cluster, asked the same way and for the same
	// reasons, but of pgshard_admin alone: a reader may look at the
	// control plane, and only an administrator may pin a session to one
	// shard or anything else that reaches past the router's routing.
	admins, err := q.Query(ctx, `SELECT r.rolname FROM pgshard.roles r JOIN pg_roles pr ON pr.rolname = r.rolname
		WHERE pg_has_role(pr.oid, 'pgshard_admin'::regrole, 'USAGE')`)
	if err != nil {
		return nil, fmt.Errorf("snapshot: admin access: %w", err)
	}
	defer admins.Close()
	for admins.Next() {
		var name string
		if err := admins.Scan(&name); err != nil {
			return nil, err
		}
		r.adminAccess[name] = true
	}
	return r, access.Err()
}

// shardKeyChecked reports that a verdict was recorded for the generation
// now in force. The controller publishes the verdict on its own pass, after
// the reconciler has already made the table effective, so a sharded table
// is routable-looking before anything has asked the shards what its key
// column really is.
func shardKeyChecked(ts catalog.TableStatus) bool {
	return ts.ShardKeyCheckedGeneration != nil && *ts.ShardKeyCheckedGeneration == ts.EffectiveGeneration
}

// shardKeyError reports the controller's verdict on a table's shard key,
// but only for the generation now in force: a verdict recorded against an
// older generation was about a key the table no longer has.
func shardKeyError(ts catalog.TableStatus) string {
	if ts.ShardKeyError == nil || ts.ShardKeyCheckedGeneration == nil || *ts.ShardKeyCheckedGeneration != ts.EffectiveGeneration {
		return ""
	}
	return *ts.ShardKeyError
}

// shardKeyType reports the key column's recorded type, under the same
// generation rule as shardKeyError: a type recorded against an older
// generation described a column the table no longer keys on, and
// normalising by it would be worse than not normalising at all.
func shardKeyType(ts catalog.TableStatus) string {
	if ts.ShardKeyType == nil || ts.ShardKeyCheckedGeneration == nil || *ts.ShardKeyCheckedGeneration != ts.EffectiveGeneration {
		return ""
	}
	return *ts.ShardKeyType
}

// loadViews reads pgshard.views. A view with no entry is an undeclared
// relation to the planner, and an undeclared relation falls to the database
// default placement -- which for a view over a sharded table is one shard's
// rows and no error. Opaque views are loaded too, precisely so the planner
// can tell "a view it cannot route" from "a table it has never heard of".
func loadViews(ctx context.Context, q catalog.Querier, s *Snapshot) error {
	rows, err := q.Query(ctx, `SELECT database, schema_name, view_name, base_schema, base_name, shape, columns::text FROM pgshard.views`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var db, schema, name, baseSchema, baseName, shape, cols string
		if err := rows.Scan(&db, &schema, &name, &baseSchema, &baseName, &shape, &cols); err != nil {
			return err
		}
		v := View{Simple: shape == catalog.ViewSimple}
		if v.Simple {
			v.Base = TableKey{Database: db, SchemaName: baseSchema, TableName: baseName}
			if err := json.Unmarshal([]byte(cols), &v.Columns); err != nil {
				// A column map that cannot be read is not a map to route by.
				v.Simple, v.Columns = false, nil
			}
		}
		s.Views[TableKey{Database: db, SchemaName: schema, TableName: name}] = v
	}
	return rows.Err()
}
