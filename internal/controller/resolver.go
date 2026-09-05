package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/metrics"
)

// GIDPrefix starts every transaction identifier the router coordinates.
const GIDPrefix = "pgshard-"

// DefaultPreparingTimeout is how long a preparing decision row may go
// without a coordinator heartbeat before the resolver decides abort for
// it: the router that owned it is presumed dead. Live coordinators beat
// far more often than this, so only a dead one ages out.
const DefaultPreparingTimeout = 10 * time.Second

// ShardConn is a connection to one shard's primary with the privileges to
// see and finish prepared transactions.
type ShardConn interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (CommandTag, error)
	Close(ctx context.Context) error
}

// CommandTag is what Exec reports. It is named rather than pgconn.CommandTag
// so a ShardConn can be implemented without pgconn, and exported because an
// unexported return type cannot be named at all from another package: a
// would-be implementation could not write the method signature, whatever it
// was willing to depend on.
type CommandTag interface{ RowsAffected() int64 }

// ShardDialer opens a connection to a shard's primary.
type ShardDialer interface {
	Dial(ctx context.Context, shardSet string, shardID int32) (ShardConn, error)
}

// ShardDBDialer opens a connection to one database of a shard's primary.
type ShardDBDialer interface {
	ShardDialer
	DialDatabase(ctx context.Context, shardSet string, shardID int32, database string) (ShardConn, error)
}

// ShardRef names one shard.
type ShardRef struct {
	Set string
	ID  int32
}

// Resolver finishes in-doubt two-phase commits from the decision log and
// rolls back prepared transactions no decision row claims. Every step is
// idempotent and safe to run concurrently with a coordinating router.
type Resolver struct {
	Pool *pgxpool.Pool
	// Shards must be database-aware: PostgreSQL finishes a prepared
	// transaction only from the database it was prepared in, so the type is
	// the requirement rather than a capability discovered at run time. A
	// decorator that implemented only ShardDialer used to satisfy the field
	// and silently lose that, finishing against the DSN's default database.
	Shards ShardDBDialer
	Logger *slog.Logger
	// PreparingTimeout overrides DefaultPreparingTimeout. It is held to a
	// floor of several coordinator heartbeats; see preparingTimeout.
	PreparingTimeout time.Duration
	// HeartbeatInterval is how often coordinators are expected to mark a
	// preparing row alive; zero means catalog.DecisionHeartbeatInterval.
	// Lowering it lowers the floor under PreparingTimeout with it, which
	// is what a test that wants both short needs.
	HeartbeatInterval time.Duration
	// SweepInterval is how often the whole topology is searched for
	// prepared transactions no decision row names. That sweep is a safety
	// net for a coordinator that died between preparing and recording, so
	// it does not have to run as often as the resolver looks at the
	// decisions it does know about. Zero means DefaultSweepInterval.
	SweepInterval time.Duration
	// lastSweep is when the whole topology was last searched.
	lastSweep time.Time
	// warnRaised keeps the raised-timeout warning to one line per process.
	warnRaised sync.Once
	// Now overrides the clock in tests.
	Now func() time.Time
	// Metrics counts resolved transactions; nil disables it.
	Metrics *metrics.Controller
}

// Outcome counts one resolution pass.
type Outcome struct {
	Committed  int
	RolledBack int
	Unresolved int
}

type decision struct {
	GID          string
	State        string
	Participants []int32
	// AgeSeconds is how long the row has gone without a sign of its
	// coordinator. The routers stamp heartbeat_at with the catalog's
	// clock, so the age is measured there too: a controller whose clock
	// runs fast must not be able to call a live coordinator dead.
	AgeSeconds float64
}

func (d decision) age() time.Duration {
	return time.Duration(d.AgeSeconds * float64(time.Second))
}

// holder is one place a prepared transaction sits: a shard's primary and
// the database it was prepared in.
type holder struct {
	Shard    ShardRef
	Database string
}

// Resolve runs one pass; shardSet "" means every shard set.
func (r *Resolver) Resolve(ctx context.Context, shardSet string) (Outcome, error) {
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}
	var out Outcome
	rows, err := r.Pool.Query(ctx, `SELECT gid, state, participants, extract(epoch from clock_timestamp() - greatest(created_at, heartbeat_at))::float8 FROM pgshard.xact_decisions ORDER BY created_at`)
	if err != nil {
		return out, fmt.Errorf("resolver: decisions: %w", err)
	}
	decisions, err := pgx.CollectRows(rows, pgx.RowToStructByPos[decision])
	if err != nil {
		return out, fmt.Errorf("resolver: decisions: %w", err)
	}
	// A healthy cluster deletes each decision row as the 2PC completes, so
	// at rest there is nothing to resolve -- and searching every shard for
	// prepared transactions anyway costs a dial and a query per shard, per
	// pass, forever. With nothing in doubt the only reason to look is the
	// orphan sweep, which is a safety net for a coordinator that died
	// between preparing and recording, and does not need the resolver's
	// cadence. When there is something in doubt the search is unchanged:
	// a decision may only be deleted once the whole topology was seen, and
	// making that wait for a sweep would put a minute between a commit and
	// its resolution.
	if len(decisions) == 0 && !r.sweepDue() {
		return out, nil
	}
	shards, err := r.listShards(ctx, shardSet)
	if err != nil {
		return out, err
	}
	holders, scanErrs := r.scanPrepared(ctx, shards)
	if shardSet == "" {
		r.lastSweep = r.now()
	}
	// A decision may only be deleted once every shard of the whole current
	// topology was searched: a participant can sit on a group its shard id
	// no longer maps to after a reshard.
	complete := shardSet == "" && len(scanErrs) == 0
	for _, d := range decisions {
		if err := r.resolveDecision(ctx, d, holders, complete, &out); err != nil {
			out.Unresolved++
			logger.Warn("resolver: transaction left in doubt", "gid", d.GID, "state", d.State, "err", err)
		}
	}
	for sh, err := range scanErrs {
		out.Unresolved++
		logger.Warn("resolver: prepared-transaction scan failed", "shard", fmt.Sprintf("%s/%d", sh.Set, sh.ID), "err", err)
	}
	if err := r.sweepOrphans(ctx, holders, &out); err != nil {
		out.Unresolved++
		logger.Warn("resolver: orphan sweep failed", "err", err)
	}
	return out, nil
}

// DefaultSweepInterval is how often the whole topology is searched for
// prepared transactions no decision row names.
const DefaultSweepInterval = time.Minute

func (r *Resolver) sweepDue() bool {
	every := r.SweepInterval
	if every == 0 {
		every = DefaultSweepInterval
	}
	return !r.now().Before(r.lastSweep.Add(every))
}

// scanPrepared searches every shard's pg_prepared_xacts for
// router-coordinated gids and returns where each is held, with the scan
// errors per unreachable shard.
func (r *Resolver) scanPrepared(ctx context.Context, shards []ShardRef) (map[string][]holder, map[ShardRef]error) {
	// Serially, a pass costs one round trip per shard in sequence, so at a
	// few hundred shards it no longer fits in the interval it runs on and
	// the resolver never idles. The shards are independent, so the pass
	// takes as long as the slowest one rather than the sum.
	type result struct {
		shard ShardRef
		gids  map[string]string
		err   error
	}
	results := make([]result, len(shards))
	var wg sync.WaitGroup
	sem := make(chan struct{}, scanConcurrency)
	for i, sh := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			gids, err := r.listPrepared(ctx, sh)
			results[i] = result{shard: sh, gids: gids, err: err}
		}()
	}
	wg.Wait()

	holders := map[string][]holder{}
	scanErrs := map[ShardRef]error{}
	// Collected in topology order, so a gid held on several shards lists
	// them the same way on every pass.
	for _, res := range results {
		if res.err != nil {
			scanErrs[res.shard] = res.err
			continue
		}
		for gid, db := range res.gids {
			holders[gid] = append(holders[gid], holder{Shard: res.shard, Database: db})
		}
	}
	return holders, scanErrs
}

// scanConcurrency bounds the prepared-transaction scan: enough to stop the
// pass being a sum of round trips, few enough not to open a connection to
// every shard of a large topology at once.
const scanConcurrency = 16

// listPrepared reads sh's pg_prepared_xacts: gid to database. The view is
// cluster-wide, so one connection sees every database's entries.
func (r *Resolver) listPrepared(ctx context.Context, sh ShardRef) (map[string]string, error) {
	conn, err := r.Shards.Dial(ctx, sh.Set, sh.ID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT gid, database FROM pg_prepared_xacts WHERE gid LIKE $1 ORDER BY prepared`, GIDPrefix+"%")
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for rows.Next() {
		var gid, db string
		if err := rows.Scan(&gid, &db); err != nil {
			return nil, err
		}
		out[gid] = db
	}
	return out, rows.Err()
}

func (r *Resolver) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// preparingTimeout never returns less than the coordinator heartbeat can
// survive. The interval and this timeout are one invariant across two
// processes, and a timeout set within a beat or two of the interval aborts
// live coordinators whose beat was merely late -- safe, but a transaction
// the client was told nothing had gone wrong with.
func (r *Resolver) preparingTimeout() time.Duration {
	beat := r.HeartbeatInterval
	if beat <= 0 {
		beat = catalog.DecisionHeartbeatInterval
	}
	want := r.PreparingTimeout
	if want <= 0 {
		want = DefaultPreparingTimeout
	}
	floor := catalog.MinPreparingBeats * beat
	if want >= floor {
		return want
	}
	// Silently running to a different timeout than the one configured is
	// how an operator ends up debugging the wrong number.
	r.warnRaised.Do(func() {
		logger := r.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("resolver: preparing timeout raised to span the coordinator heartbeat",
			"configured", want, "using", floor, "heartbeat", beat, "beats", catalog.MinPreparingBeats)
	})
	return floor
}

// resolveDecision finishes one decision row. A preparing row older than the
// timeout belongs to a dead router that never decided: abort is safe
// because commit was never recorded. Commit rows are committed on every
// shard that holds the prepared transaction — searched across the whole
// topology, never trusted to the recorded participant list, which a
// reshard can leave pointing at groups that no longer hold it. The row is
// deleted only once every shard was searched and none still holds the gid.
func (r *Resolver) resolveDecision(ctx context.Context, d decision, holders map[string][]holder, complete bool, out *Outcome) error {
	if d.State == "preparing" {
		if d.age() < r.preparingTimeout() {
			return nil
		}
		// The staleness check re-runs inside the UPDATE, against the
		// catalog's clock at that moment: a coordinator heartbeat landing
		// after the scan makes it match zero rows instead of aborting a
		// live transaction.
		tag, err := r.Pool.Exec(ctx, `UPDATE pgshard.xact_decisions SET state = 'abort', decided_at = now() WHERE gid = $1 AND state = 'preparing' AND greatest(created_at, heartbeat_at) <= clock_timestamp() - make_interval(secs => $2::float8)`, d.GID, r.preparingTimeout().Seconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			if err := r.Pool.QueryRow(ctx, `SELECT state FROM pgshard.xact_decisions WHERE gid = $1`, d.GID).Scan(&d.State); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil
				}
				return err
			}
			if d.State == "preparing" {
				return nil
			}
		} else {
			d.State = "abort"
		}
	}
	for len(holders[d.GID]) > 0 {
		h := holders[d.GID][0]
		// The coordinator can finish a participant between the scan
		// snapshot and this call, and killing it does not recall the
		// COMMIT PREPARED already in flight. sweepOrphans already reads a
		// gone prepared transaction as resolved; on this path it was
		// reported in doubt for ever, because nothing else ever clears a
		// decision whose participant has left.
		if err := r.finishOn(ctx, h, d.State == "commit", d.GID); err != nil && !isGonePreparedXact(err) {
			return err
		}
		holders[d.GID] = holders[d.GID][1:]
	}
	delete(holders, d.GID)
	if !complete {
		return errors.New("not every shard could be searched for the prepared transaction: keeping the decision")
	}
	if _, err := r.Pool.Exec(ctx, `DELETE FROM pgshard.xact_decisions WHERE gid = $1 AND state = $2`, d.GID, d.State); err != nil {
		return err
	}
	if d.State == "commit" {
		out.Committed++
	} else {
		out.RolledBack++
	}
	return nil
}

// finishOn commits or rolls back gid where h holds it. PostgreSQL only
// finishes a prepared transaction from the database it was prepared in, so
// the connection targets h's database, not the DSN's default one.
func (r *Resolver) finishOn(ctx context.Context, h holder, commit bool, gid string) error {
	verb := "ROLLBACK PREPARED"
	if commit {
		verb = "COMMIT PREPARED"
	}
	// Shard ids repeat across shard sets, and PostgreSQL will only finish a
	// prepared transaction from the database it was prepared in, so an
	// error naming neither leaves an operator to search every set and
	// database for the participant that would not budge.
	where := fmt.Sprintf("%s on %s/%d database %q", verb, h.Shard.Set, h.Shard.ID, h.Database)
	conn, err := r.Shards.DialDatabase(ctx, h.Shard.Set, h.Shard.ID, h.Database)
	if err != nil {
		return fmt.Errorf("%s: %w", where, err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, verb+" "+quoteLiteral(gid)); err != nil {
		return fmt.Errorf("%s: %w", where, err)
	}
	return nil
}

// sweepOrphans finishes the remaining prepared transactions no decision
// pass handled: by the decision row's state when one exists (checked at
// sweep time, so a commit-decided gid is never rolled back), rolled back
// when no row claims the gid — the coordinator writes the row before any
// participant prepares, so a rowless prepared transaction is an orphan.
func (r *Resolver) sweepOrphans(ctx context.Context, holders map[string][]holder, out *Outcome) error {
	gids := make([]string, 0, len(holders))
	for gid := range holders {
		gids = append(gids, gid)
	}
	slices.Sort(gids)
	var errs []error
	for _, gid := range gids {
		var state string
		err := r.Pool.QueryRow(ctx, `SELECT state FROM pgshard.xact_decisions WHERE gid = $1`, gid).Scan(&state)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			state = "abort"
		case err != nil:
			errs = append(errs, fmt.Errorf("%s: %w", gid, err))
			continue
		}
		if state == "preparing" {
			continue
		}
		for _, h := range holders[gid] {
			err := r.finishOn(ctx, h, state == "commit", gid)
			switch {
			// The live coordinator can finish gid and delete its row between
			// the scan snapshot and this sweep: a gone prepared transaction
			// is already resolved, not a failure of this pass.
			case isGonePreparedXact(err):
				continue
			case err != nil:
				errs = append(errs, fmt.Errorf("%s on %s/%d: %w", gid, h.Shard.Set, h.Shard.ID, err))
				continue
			}
			if state == "commit" {
				out.Committed++
			} else {
				out.RolledBack++
			}
		}
	}
	return errors.Join(errs...)
}

func isGonePreparedXact(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UndefinedObject {
		return true
	}
	return strings.Contains(err.Error(), "does not exist")
}

// listShards returns every shard row, whatever its serving state. That is
// the invariant the resolver rests on: a retired shard can still hold a
// prepared transaction, and a decision row can only be deleted once every
// holder has finished it, so a scan that skipped non-serving shards could
// delete a decision while its transaction was still prepared -- and a
// prepared transaction nobody will ever finish pins WAL and blocks slot
// creation for as long as the shard exists.
//
// Retirement leaves the row (serving_state = 'retired'); only dropping the
// set removes it, by which point the cutover has drained the sources
// through the resolver. So the filter here is deliberately on nothing but
// the set name.
func (r *Resolver) listShards(ctx context.Context, shardSet string) ([]ShardRef, error) {
	rows, err := r.Pool.Query(ctx, `SELECT shard_set, shard_id FROM pgshard.shard_status WHERE ($1 = '' OR shard_set = $1) ORDER BY shard_set, shard_id`, shardSet)
	if err != nil {
		return nil, fmt.Errorf("resolver: shards: %w", err)
	}
	refs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[ShardRef])
	if err != nil {
		return nil, fmt.Errorf("resolver: shards: %w", err)
	}
	return refs, nil
}

// Run resolves in-doubt transactions on every tick while this replica is
// the leader. Only the leader may: a pass commits and rolls back prepared
// transactions on every group.
func (r *Resolver) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	runLoop(ctx, interval, leader, func() *slog.Logger { return r.Logger }, "resolver", func(ctx context.Context) {
		out, err := r.Resolve(ctx, "")
		if err != nil && r.Logger != nil {
			r.Logger.Warn("resolver pass failed", "err", err)
		}
		if r.Metrics != nil {
			r.Metrics.ResolvedTxns.WithLabelValues("committed").Add(float64(out.Committed))
			r.Metrics.ResolvedTxns.WithLabelValues("rolled_back").Add(float64(out.RolledBack))
			r.Metrics.ResolverUnresolved.Set(float64(out.Unresolved))
		}
	})
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// PgxShardDialer opens pgx connections from a DSN per shard: explicit
// entries first, then Template with {set}, {id} and {group} (the
// shard_status group name) substituted.
type PgxShardDialer struct {
	Pool     *pgxpool.Pool
	DSNs     map[ShardRef]string
	Template string

	// groups caches the group name a template DSN needs. A copy pass dials
	// each source and target of each database several times every few
	// seconds, and each template dial asked the catalog for a name that
	// almost never changes -- so the catalog's read load grew with the
	// square of the topology for a lookup that is nearly constant.
	//
	// "Almost never" is why this is a cache and not a field read once: the
	// operator's upsert does set group_name, so a rebuilt or renamed group
	// can change it. A dial that fails forgets the entry and tries again
	// with a fresh lookup, so a stale name costs one failed connection
	// rather than a wedged workflow.
	groupMu sync.Mutex
	groups  map[ShardRef]string
}

func (d *PgxShardDialer) cachedGroup(ref ShardRef) (string, bool) {
	d.groupMu.Lock()
	defer d.groupMu.Unlock()
	g, ok := d.groups[ref]
	return g, ok
}

func (d *PgxShardDialer) rememberGroup(ref ShardRef, group string) {
	d.groupMu.Lock()
	defer d.groupMu.Unlock()
	if d.groups == nil {
		d.groups = map[ShardRef]string{}
	}
	d.groups[ref] = group
}

// forgetGroup drops a cached name after a dial that used it failed.
func (d *PgxShardDialer) forgetGroup(ref ShardRef) {
	d.groupMu.Lock()
	defer d.groupMu.Unlock()
	delete(d.groups, ref)
}

// Dial implements ShardDialer.
func (d *PgxShardDialer) Dial(ctx context.Context, shardSet string, shardID int32) (ShardConn, error) {
	return d.DialDatabase(ctx, shardSet, shardID, "")
}

// dsn returns the DSN of one shard, and whether it came from the group
// cache -- a caller that then fails to connect uses that to decide whether
// the name is worth forgetting.
func (d *PgxShardDialer) dsn(ctx context.Context, shardSet string, shardID int32) (dsn string, cached bool, err error) {
	ref := ShardRef{Set: shardSet, ID: shardID}
	if dsn, ok := d.DSNs[ref]; ok {
		return dsn, false, nil
	}
	if d.Template == "" {
		return "", false, fmt.Errorf("no DSN for shard %s/%d", shardSet, shardID)
	}
	if group, ok := d.cachedGroup(ref); ok {
		return ExpandShardTemplate(d.Template, shardSet, shardID, group), true, nil
	}
	group, err := GroupName(ctx, d.Pool, shardSet, shardID)
	if err != nil {
		return "", false, err
	}
	d.rememberGroup(ref, group)
	return ExpandShardTemplate(d.Template, shardSet, shardID, group), false, nil
}

// GroupName reads the shard_status group name of one shard.
func GroupName(ctx context.Context, pool *pgxpool.Pool, shardSet string, shardID int32) (string, error) {
	var group string
	if err := pool.QueryRow(ctx, `SELECT group_name FROM pgshard.shard_status WHERE shard_set = $1 AND shard_id = $2`, shardSet, shardID).Scan(&group); err != nil {
		return "", fmt.Errorf("shard %s/%d: %w", shardSet, shardID, err)
	}
	return group, nil
}

// ExpandShardTemplate substitutes {set}, {id} and {group} in a DSN
// template. The database is not substituted: it is a name a CREATEDB user
// chooses, so it is set through ConnInfo, which cannot let it become
// anything but a database.
func ExpandShardTemplate(template, shardSet string, shardID int32, group string) string {
	return strings.NewReplacer("{set}", shardSet, "{id}", fmt.Sprint(shardID), "{group}", group).Replace(template)
}

// ConnInfo renders dsn with its database replaced by database, as a libpq
// keyword/value string. The name is checked before it is used and every
// value is quoted, so a database name cannot become further keywords:
// PostgreSQL accepts any quoted identifier as a database name, and libpq
// separates keywords on whitespace, so an unquoted name is an injection of
// host, sslmode and the rest into a string that carries a shard superuser
// credential.
func ConnInfo(dsn, database string) (string, error) {
	if err := catalog.CheckDatabaseName(database); err != nil {
		return "", err
	}
	// Parsed only to reject a DSN that is not one; the RESULT is built from
	// the original text.
	//
	// It used to be rebuilt from the parsed fields -- host, port, user,
	// password, dbname -- which silently dropped every other option. A DSN
	// configured "sslmode=verify-full sslrootcert=/ca.crt" produced a
	// conninfo with no sslmode at all, so the subscription fell back to
	// libpq's default and connected without verifying anything, over a
	// string that carries a shard superuser credential. sslcert/sslkey went
	// the same way, so a certificate-authenticated subscription could not
	// connect at all. A tls.Config cannot be turned back into sslrootcert,
	// which is why reconstruction cannot be made safe and the text is kept.
	if _, err := pgx.ParseConfig(dsn); err != nil {
		return "", err
	}
	return withDatabase(dsn, database)
}

// withDatabase replaces the database in a DSN and changes nothing else.
//
// libpq accepts two forms and they need different treatment: a URL, whose
// database is the path, and a keyword/value string, where the LAST
// occurrence of a keyword wins -- so appending is a replacement, and one
// that cannot disturb a value it does not understand.
func withDatabase(dsn, database string) (string, error) {
	trimmed := strings.TrimSpace(dsn)
	if strings.HasPrefix(trimmed, "postgres://") || strings.HasPrefix(trimmed, "postgresql://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return "", err
		}
		u.Path = "/" + database
		// A dbname in the query string would override the path.
		q := u.Query()
		if q.Has("dbname") {
			q.Set("dbname", database)
			u.RawQuery = q.Encode()
		}
		return u.String(), nil
	}
	return trimmed + " dbname=" + quoteConnValue(database), nil
}

func quoteConnValue(v string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(v) + "'"
}

type pgxShardConn struct{ *pgx.Conn }

func (c pgxShardConn) Exec(ctx context.Context, sql string, args ...any) (CommandTag, error) {
	return c.Conn.Exec(ctx, sql, args...)
}
