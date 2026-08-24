package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// DefaultCutoverLockTimeout bounds each SHARE lock of the sweep.
const DefaultCutoverLockTimeout = 5 * time.Second

// JournalSchema and JournalTable name the resharding journal in every user
// database: the row every source writes at the point of no return.
const (
	JournalSchema = "pgshard_journal"
	JournalTable  = "resharding_journal"
)

// ReversePublicationName names the publication on a target that carries the
// sharded rows of one source's ranges back to it.
func ReversePublicationName(generation int64, source int32) string {
	return fmt.Sprintf("pgshard_reshard_g%d_rev_s%d", generation, source)
}

// ReverseHomePublicationName names the publication on the home target that
// carries reference and unsharded tables back to the home source.
func ReverseHomePublicationName(generation int64) string {
	return fmt.Sprintf("pgshard_reshard_g%d_rev_home", generation)
}

// ReverseSubscriptionName names the subscription (and slot on the target) of
// one (source, target) pair of the reverse direction.
func ReverseSubscriptionName(generation int64, source, target int32) string {
	return fmt.Sprintf("pgshard_reshard_g%d_rev_s%d_t%d", generation, source, target)
}

// pgCutover implements cutoverOps against the catalog and the shards.
type pgCutover struct {
	c         *Copier
	wf        *copyWorkflow
	srcSet    string
	srcIDs    []int32
	srcRanges placement.RangeSet
	dbs       []dbPlan
}

// driveCutover builds the PostgreSQL ops of one workflow and advances it.
func (c *Copier) driveCutover(ctx context.Context, wf *copyWorkflow) (bool, error) {
	if wf.sourceSet() == "" {
		set, _, err := c.sources(ctx)
		if err != nil {
			return false, err
		}
		wf.cutover.SourceSet = set
	}
	ops, err := c.pgCutover(ctx, wf)
	if err != nil {
		return false, err
	}
	return c.cutover(ctx, wf, ops)
}

func (c *Copier) pgCutover(ctx context.Context, wf *copyWorkflow) (*pgCutover, error) {
	ranges, err := catalog.ListShardRanges(ctx, c.Pool, wf.sourceSet())
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("source shard set %s has no ranges", wf.sourceSet())
	}
	ops := &pgCutover{c: c, wf: wf, srcSet: wf.sourceSet(), srcRanges: catalog.RangeSet(ranges)}
	for _, r := range ranges {
		ops.srcIDs = append(ops.srcIDs, r.ShardID)
	}
	if ops.dbs, err = c.databases(ctx); err != nil {
		return nil, err
	}
	return ops, nil
}

func (o *pgCutover) lockTimeout() time.Duration {
	if o.c.LockTimeout > 0 {
		return o.c.LockTimeout
	}
	return DefaultCutoverLockTimeout
}

func (o *pgCutover) forwardPattern(t int32) string {
	return fmt.Sprintf("pgshard\\_reshard\\_g%d\\_t%d\\_%%", o.wf.gen, t)
}

func (o *pgCutover) reversePattern(s int32) string {
	return fmt.Sprintf("pgshard\\_reshard\\_g%d\\_rev\\_s%d\\_%%", o.wf.gen, s)
}

// GateOpen: every table ready, lag under threshold, no paused subscription
// and an apply worker alive behind every forward subscription.
func (o *pgCutover) GateOpen(ctx context.Context) (bool, string, error) {
	progress, err := o.c.observe(ctx, o.wf, o.srcSet, o.srcIDs, o.dbs)
	if err != nil {
		return false, "", err
	}
	o.wf.copy.Progress = progress
	if !progress.CaughtUp(o.c.lagBytes()) {
		return false, fmt.Sprintf("%d/%d tables ready, lag %d bytes", progress.TablesReady, progress.TablesTotal, progress.LagBytes), nil
	}
	if progress.Paused > 0 {
		return false, fmt.Sprintf("%d subscriptions paused by the throttle", progress.Paused), nil
	}
	var stalled []string
	for _, db := range o.dbs {
		for _, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return false, "", err
			}
			rows, err := conn.Query(ctx, `SELECT s.subname FROM pg_subscription s
				LEFT JOIN pg_stat_subscription st ON st.subid = s.oid AND st.relid IS NULL
				WHERE s.subname LIKE $1 AND (NOT s.subenabled OR st.pid IS NULL) ORDER BY 1`, o.forwardPattern(t))
			if err == nil {
				var names []string
				names, err = pgx.CollectRows(rows, pgx.RowTo[string])
				for _, n := range names {
					stalled = append(stalled, fmt.Sprintf("%s/%d %s", db.name, t, n))
				}
			}
			_ = conn.Close(ctx)
			if err != nil {
				return false, "", err
			}
		}
	}
	if len(stalled) > 0 {
		return false, "subscriptions without an apply worker: " + strings.Join(stalled, ", "), nil
	}
	return true, "", nil
}

// Fence marks every source shard migrating and takes the DDL lock of every
// database.
func (o *pgCutover) Fence(ctx context.Context) error {
	tx, err := o.c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `UPDATE pgshard.shard_status SET migrating = true, updated_at = now() WHERE shard_set = $1 AND NOT migrating`, o.srcSet); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO pgshard.workflow_locks (kind, key, workflow_id)
		SELECT 'ddl', name, $1::uuid FROM pgshard.databases ON CONFLICT (kind, key) DO NOTHING`, o.wf.id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Drain runs the resolver against every source and lists what remains.
func (o *pgCutover) Drain(ctx context.Context) ([]string, error) {
	var remaining []string
	for _, s := range o.srcIDs {
		gids, err := o.c.preparedOn(ctx, o.srcSet, s)
		if err != nil {
			return nil, err
		}
		for _, g := range gids {
			remaining = append(remaining, fmt.Sprintf("%s/%d:%s", o.srcSet, s, g))
		}
	}
	return remaining, nil
}

// Sweep takes a SHARE lock on every user table of every source database in
// one short transaction per database: it returns once no write is in
// flight, or errRetry when one outlasts the lock timeout.
func (o *pgCutover) Sweep(ctx context.Context) error {
	for _, db := range o.dbs {
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = o.sweepOne(ctx, conn)
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("sweep of %s on %s/%d: %w", db.name, o.srcSet, s, err)
			}
		}
	}
	return nil
}

func (o *pgCutover) sweepOne(ctx context.Context, conn ShardConn) error {
	tables, err := listTables(ctx, conn)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, QuoteIdent(t.schema)+"."+QuoteIdent(t.name))
	}
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", o.lockTimeout().Milliseconds()))
	if err == nil {
		_, err = conn.Exec(ctx, "LOCK TABLE "+strings.Join(names, ", ")+" IN SHARE MODE")
	}
	if err != nil {
		_, _ = conn.Exec(ctx, "ROLLBACK")
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			return retryf("lock timeout: a write is still in flight")
		}
		return err
	}
	_, err = conn.Exec(ctx, "COMMIT")
	return err
}

// Positions reads pg_current_wal_lsn() of every source.
func (o *pgCutover) Positions(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	for _, s := range o.srcIDs {
		lsn, err := o.currentLSN(ctx, o.srcSet, s)
		if err != nil {
			return nil, err
		}
		out[fmt.Sprint(s)] = lsn
	}
	return out, nil
}

func (o *pgCutover) currentLSN(ctx context.Context, set string, id int32) (int64, error) {
	conn, err := o.c.Shards.Dial(ctx, set, id)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint`)
	if err != nil {
		return 0, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowTo[int64])
}

// CaughtUp: the apply worker of every forward subscription reported a
// position at or past its source's.
func (o *pgCutover) CaughtUp(ctx context.Context, positions map[string]int64) (bool, string, error) {
	var behind []string
	for _, db := range o.dbs {
		for _, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return false, "", err
			}
			rows, err := conn.Query(ctx, `SELECT s.subname, coalesce((st.latest_end_lsn - '0/0'::pg_lsn)::bigint, -1)
				FROM pg_subscription s LEFT JOIN pg_stat_subscription st ON st.subid = s.oid AND st.relid IS NULL
				WHERE s.subname LIKE $1 ORDER BY 1`, o.forwardPattern(t))
			if err == nil {
				err = func() error {
					defer rows.Close()
					for rows.Next() {
						var name string
						var applied int64
						if err := rows.Scan(&name, &applied); err != nil {
							return err
						}
						want := positions[fmt.Sprint(sourceOf(name, o.wf.gen, t, o.srcIDs))]
						if applied < want {
							behind = append(behind, fmt.Sprintf("%s/%d %s at %d of %d", db.name, t, name, applied, want))
						}
					}
					return rows.Err()
				}()
			}
			_ = conn.Close(ctx)
			if err != nil {
				return false, "", err
			}
		}
	}
	if len(behind) > 0 {
		return false, "subscriptions behind the source position: " + strings.Join(behind, ", "), nil
	}
	return true, "", nil
}

// rowDigest is count(*) and the sum of per-row text hashes of one table
// slice.
type rowDigest struct {
	Rows int64
	Hash int64
}

func (d rowDigest) add(o rowDigest) rowDigest {
	return rowDigest{Rows: d.Rows + o.Rows, Hash: d.Hash + o.Hash}
}

func digest(ctx context.Context, conn ShardConn, schema, name, filter string) (rowDigest, error) {
	sql := fmt.Sprintf(`SELECT count(*), coalesce(sum(hashtext(t::text)::bigint), 0) FROM %s.%s t`, QuoteIdent(schema), QuoteIdent(name))
	if filter != "" {
		sql += " WHERE (" + filter + ")"
	}
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return rowDigest{}, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[rowDigest])
}

// Verify compares, per table and target, what the sources predict for the
// target's ranges with what the target holds. It runs under the fence
// after the targets caught up, so both sides are still.
func (o *pgCutover) Verify(ctx context.Context) (VerifyReport, error) {
	report := VerifyReport{CheckedAt: o.c.now()}
	for _, db := range o.dbs {
		expected := map[string]map[int32]rowDigest{}
		hashes := map[string]string{}
		add := func(table string, t int32, d rowDigest) {
			if expected[table] == nil {
				expected[table] = map[int32]rowDigest{}
			}
			expected[table][t] = expected[table][t].add(d)
		}
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return report, err
			}
			err = func() error {
				for _, tb := range db.sharded {
					key := tb.SchemaName + "." + tb.TableName
					if hashes[key] == "" {
						st, err := describeTable(ctx, conn, tb.SchemaName, tb.TableName, *tb.ShardKey)
						if errors.Is(err, pgx.ErrNoRows) {
							continue
						}
						if err != nil {
							return err
						}
						if hashes[key], err = KeyHashExpr(*tb.ShardKey, st.keyType); err != nil {
							return err
						}
					}
					for i, t := range o.wf.ids {
						d, err := digest(ctx, conn, tb.SchemaName, tb.TableName, RangeFilter(hashes[key], o.wf.ranges[i]))
						if err != nil {
							return err
						}
						add(key, t, d)
					}
				}
				if s != db.home {
					return nil
				}
				shardedNames := map[string]bool{}
				for _, tb := range db.sharded {
					shardedNames[tb.SchemaName+"."+tb.TableName] = true
				}
				refNames := map[string]bool{}
				for _, tb := range db.reference {
					refNames[tb.SchemaName+"."+tb.TableName] = true
				}
				others, err := listTables(ctx, conn)
				if err != nil {
					return err
				}
				homeTarget := o.wf.ids[HomeTarget(o.wf.ranges)]
				for _, ot := range others {
					key := ot.schema + "." + ot.name
					if shardedNames[key] || ot.schema == JournalSchema {
						continue
					}
					d, err := digest(ctx, conn, ot.schema, ot.name, "")
					if err != nil {
						return err
					}
					if refNames[key] {
						for _, t := range o.wf.ids {
							add(key, t, d)
						}
					} else {
						add(key, homeTarget, d)
					}
				}
				return nil
			}()
			_ = conn.Close(ctx)
			if err != nil {
				return report, fmt.Errorf("verify %s on %s/%d: %w", db.name, o.srcSet, s, err)
			}
		}
		for _, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return report, err
			}
			err = func() error {
				for _, key := range sortedKeys(expected) {
					want, ok := expected[key][t]
					if !ok {
						continue
					}
					schema, name, _ := strings.Cut(key, ".")
					got, err := digest(ctx, conn, schema, name, "")
					if err != nil {
						return err
					}
					report.Tables++
					report.Rows += got.Rows
					if got != want {
						report.Mismatches = append(report.Mismatches, fmt.Sprintf("%s.%s on %s/%d: %d rows hash %d, sources predict %d rows hash %d",
							db.name, key, o.wf.set, t, got.Rows, got.Hash, want.Rows, want.Hash))
					}
				}
				return nil
			}()
			_ = conn.Close(ctx)
			if err != nil {
				return report, fmt.Errorf("verify %s on %s/%d: %w", db.name, o.wf.set, t, err)
			}
		}
	}
	return report, nil
}

// Reverse creates, per database, a publication per source on every target
// (the source's ranges) plus the home publication on the home target, and a
// disabled subscription per (source, target) pair on every source.
func (o *pgCutover) Reverse(ctx context.Context) error {
	homeTarget := o.wf.ids[HomeTarget(o.wf.ranges)]
	for _, db := range o.dbs {
		for _, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return err
			}
			err = o.reversePublishOn(ctx, conn, db, t == homeTarget)
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("reverse publications of %s on %s/%d: %w", db.name, o.wf.set, t, err)
			}
		}
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = o.reverseSubscribeOn(ctx, conn, db, s, homeTarget)
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("reverse subscriptions of %s on %s/%d: %w", db.name, o.srcSet, s, err)
			}
		}
	}
	return nil
}

func (o *pgCutover) reversePublishOn(ctx context.Context, conn ShardConn, db dbPlan, home bool) error {
	existing, err := existingPublications(ctx, conn, o.wf.gen)
	if err != nil {
		return err
	}
	want := map[string][]PublishedTable{}
	var sharded []struct {
		table catalog.Table
		hash  string
	}
	for _, tb := range db.sharded {
		st, err := describeTable(ctx, conn, tb.SchemaName, tb.TableName, *tb.ShardKey)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		hash, err := KeyHashExpr(*tb.ShardKey, st.keyType)
		if err != nil {
			return err
		}
		if !st.replIdentOK {
			if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY FULL", QuoteIdent(tb.SchemaName), QuoteIdent(tb.TableName))); err != nil {
				return err
			}
		}
		sharded = append(sharded, struct {
			table catalog.Table
			hash  string
		}{tb, hash})
	}
	for i, s := range o.srcIDs {
		var tables []PublishedTable
		for _, sp := range sharded {
			tables = append(tables, PublishedTable{Schema: sp.table.SchemaName, Name: sp.table.TableName, Filter: RangeFilter(sp.hash, o.srcRanges[i])})
		}
		want[ReversePublicationName(o.wf.gen, s)] = tables
	}
	if home {
		shardedNames := map[string]bool{}
		for _, tb := range db.sharded {
			shardedNames[tb.SchemaName+"."+tb.TableName] = true
		}
		others, err := listTables(ctx, conn)
		if err != nil {
			return err
		}
		var tables []PublishedTable
		for _, ot := range others {
			if shardedNames[ot.schema+"."+ot.name] || ot.schema == JournalSchema {
				continue
			}
			if !ot.replIdentOK {
				if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY FULL", QuoteIdent(ot.schema), QuoteIdent(ot.name))); err != nil {
					return err
				}
			}
			tables = append(tables, PublishedTable{Schema: ot.schema, Name: ot.name})
		}
		want[ReverseHomePublicationName(o.wf.gen)] = tables
	}
	for _, name := range sortedKeys(want) {
		if existing[name] {
			continue
		}
		if _, err := conn.Exec(ctx, CreatePublicationSQL(name, want[name])); err != nil {
			return err
		}
	}
	return nil
}

func (o *pgCutover) reverseSubscribeOn(ctx context.Context, conn ShardConn, db dbPlan, s, homeTarget int32) error {
	rows, err := conn.Query(ctx, `SELECT subname FROM pg_subscription WHERE subname LIKE $1`, o.reversePattern(s))
	if err != nil {
		return err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for _, n := range names {
		existing[n] = true
	}
	for _, t := range o.wf.ids {
		name := ReverseSubscriptionName(o.wf.gen, s, t)
		if existing[name] {
			continue
		}
		pubs := []string{ReversePublicationName(o.wf.gen, s)}
		if s == db.home && t == homeTarget {
			pubs = append(pubs, ReverseHomePublicationName(o.wf.gen))
		}
		conninfo, err := o.c.SourceConnInfo(ctx, ShardRef{Set: o.wf.set, ID: t}, db.name)
		if err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, createSubscriptionTimeout)
		_, err = conn.Exec(cctx, CreateReverseSubscriptionSQL(name, conninfo, pubs, SubscriptionOptions{Slot: name, Failover: o.c.SlotFailover}))
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateReverseSubscriptionSQL renders the reverse subscription: no initial
// copy, a slot created now, disabled until the flip, and only rows the
// publisher originated (origin = none) so forwarded rows never loop back.
func CreateReverseSubscriptionSQL(name, conninfo string, publications []string, opts SubscriptionOptions) string {
	pubs := make([]string, 0, len(publications))
	for _, p := range publications {
		pubs = append(pubs, QuoteIdent(p))
	}
	with := []string{"copy_data = false", "create_slot = true", "enabled = false", "streaming = parallel", "two_phase = false", "origin = none",
		"slot_name = " + quoteLiteral(opts.Slot)}
	if opts.Failover {
		with = append(with, "failover = true")
	}
	return fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (%s)",
		QuoteIdent(name), quoteLiteral(conninfo), strings.Join(pubs, ", "), strings.Join(with, ", "))
}

// journalDDL creates the journal table of a user database.
var journalDDL = []string{
	"CREATE SCHEMA IF NOT EXISTS " + JournalSchema,
	fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		id uuid NOT NULL, generation bigint NOT NULL, source_shard integer NOT NULL,
		participants integer[] NOT NULL, targets jsonb NOT NULL DEFAULT '{}'::jsonb,
		created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (id, source_shard))`, JournalSchema, JournalTable),
}

// Journal writes the journal row into every user database of every source
// and into the catalog. Its id is allocated before the first attempt, so a
// repeated step finds its rows in place.
func (o *pgCutover) Journal(ctx context.Context, id string) error {
	targets := map[string]int64{}
	for _, t := range o.wf.ids {
		lsn, err := o.currentLSN(ctx, o.wf.set, t)
		if err != nil {
			return err
		}
		targets[fmt.Sprint(t)] = lsn
	}
	for _, db := range o.dbs {
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = func() error {
				for _, ddl := range journalDDL {
					if _, err := conn.Exec(ctx, ddl); err != nil {
						return err
					}
				}
				_, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.%s (id, generation, source_shard, participants, targets)
					VALUES ($1::uuid, $2, $3, $4, $5::jsonb) ON CONFLICT (id, source_shard) DO NOTHING`, JournalSchema, JournalTable),
					id, o.wf.gen, s, o.srcIDs, mustJSON(targets))
				return err
			}()
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("journal of %s on %s/%d: %w", db.name, o.srcSet, s, err)
			}
		}
	}
	tx, err := o.c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO pgshard.resharding_journal (id, generation, shard_set, participants, targets)
		VALUES ($1::uuid, $2, $3, $4, $5::jsonb) ON CONFLICT (id) DO NOTHING`, id, o.wf.gen, o.wf.set, o.srcIDs, mustJSON(targets)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE pgshard.workflows SET journal_ids = array_append(array_remove(journal_ids, $2), $2) WHERE id = $1::uuid`, o.wf.id, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Flip publishes the new map in one catalog transaction: targets serving,
// sources retired, the target set serving and published, every database's
// home shard moved to the home target, and the shard-map generation bumped
// so poolers refuse stale routing. A flip that already happened is skipped.
func (o *pgCutover) Flip(ctx context.Context, _ string) error {
	tx, err := o.c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM pgshard.shard_sets WHERE shard_set = $1 FOR UPDATE`, o.wf.set).Scan(&state); err != nil {
		return err
	}
	if state == catalog.ShardSetServing {
		return nil
	}
	homeTarget := o.wf.ids[HomeTarget(o.wf.ranges)]
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE pgshard.shard_status SET serving_state = $2, migrating = false, updated_at = now() WHERE shard_set = $1`, []any{o.wf.set, ServingServing}},
		{`UPDATE pgshard.shard_status SET serving_state = $2, updated_at = now() WHERE shard_set = $1`, []any{o.srcSet, ServingRetired}},
		{`UPDATE pgshard.shard_sets SET state = $2, updated_at = now() WHERE shard_set = $1`, []any{o.wf.set, catalog.ShardSetServing}},
		{`UPDATE pgshard.shard_sets SET state = $2, updated_at = now() WHERE shard_set = $1`, []any{o.srcSet, catalog.ShardSetRetired}},
		{`INSERT INTO pgshard.serving (shard_set, generation)
			SELECT $1, coalesce(max(desired_generation), 0) FROM pgshard.shard_ranges WHERE shard_set = $1
			ON CONFLICT (shard_set) DO UPDATE SET generation = EXCLUDED.generation, published_at = now()`, []any{o.wf.set}},
		{`UPDATE pgshard.databases SET home_shard = $1 WHERE home_shard <> $1`, []any{homeTarget}},
		{`UPDATE pgshard.shard_map_generation SET generation = generation + 1, updated_at = now()`, nil},
	} {
		if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// Swap freezes the forward direction (disable; dropped on complete) and
// starts the reverse one.
func (o *pgCutover) Swap(ctx context.Context) error {
	for _, db := range o.dbs {
		for _, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return err
			}
			err = alterSubscriptions(ctx, conn, o.forwardPattern(t), true, "DISABLE")
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = alterSubscriptions(ctx, conn, o.reversePattern(s), false, "ENABLE")
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// alterSubscriptions applies verb to the subscriptions matching pattern
// whose enabled flag equals enabled.
func alterSubscriptions(ctx context.Context, conn ShardConn, pattern string, enabled bool, verb string) error {
	rows, err := conn.Query(ctx, `SELECT subname FROM pg_subscription WHERE subname LIKE $1 AND subenabled = $2 ORDER BY 1`, pattern, enabled)
	if err != nil {
		return err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, n := range names {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s %s", QuoteIdent(n), verb)); err != nil {
			return err
		}
	}
	return nil
}

// Release drops the range fence and the DDL locks.
func (o *pgCutover) Release(ctx context.Context) error {
	tx, err := o.c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `UPDATE pgshard.shard_status SET migrating = false, updated_at = now() WHERE shard_set = $1 AND migrating`, o.srcSet); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, o.wf.id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Complete drops the reverse subscriptions on the sources, the reverse
// slots and publications on the targets, the frozen forward subscriptions
// on the targets and the forward slots and publications on the sources, and
// unpublishes the retired set. Sources that are already gone are skipped.
func (o *pgCutover) Complete(ctx context.Context) error {
	for _, db := range o.dbs {
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				o.c.logger().Info("reshard complete: source unreachable, skipping its reverse subscriptions", "workflow", o.wf.id, "source", s, "err", err)
				continue
			}
			err = dropSubscriptionsLike(ctx, conn, o.reversePattern(s))
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
		for _, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return err
			}
			err = dropSubscriptionsLike(ctx, conn, o.forwardPattern(t))
			if err == nil {
				err = dropPublications(ctx, conn, o.wf.gen)
			}
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	for _, t := range o.wf.ids {
		conn, err := o.c.Shards.Dial(ctx, o.wf.set, t)
		if err != nil {
			return err
		}
		err = dropSlots(ctx, conn, o.wf.gen)
		_ = conn.Close(ctx)
		if err != nil {
			return err
		}
	}
	for _, s := range o.srcIDs {
		conn, err := o.c.Shards.Dial(ctx, o.srcSet, s)
		if err != nil {
			o.c.logger().Info("reshard complete: source unreachable, skipping its slots and publications", "workflow", o.wf.id, "source", s, "err", err)
			continue
		}
		err = dropSlots(ctx, conn, o.wf.gen)
		_ = conn.Close(ctx)
		if err != nil {
			return err
		}
		for _, db := range o.dbs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = dropPublications(ctx, conn, o.wf.gen)
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	_, err := o.c.Pool.Exec(ctx, `DELETE FROM pgshard.serving WHERE shard_set = $1`, o.srcSet)
	return err
}
