package router

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// DDLAsyncGUC is the session setting that makes DDL return as soon as its
// migration is queued.
const DDLAsyncGUC = "pgshard.ddl_async"

// DDLDedupGUC is the session setting that, turned off, makes a statement
// always queue a new migration rather than attach to an identical one.
const DDLDedupGUC = "pgshard.ddl_dedup"

// MigrationQueue hands DDL to the controller's applier and reports how it
// ended.
type MigrationQueue interface {
	// Enqueue queues m, or attaches to an identical migration that can
	// stand for it when m carries a dedup key.
	Enqueue(ctx context.Context, m catalog.DDLMigration) (catalog.EnqueueResult, error)
	// Wait blocks until the migration is complete or failed. waiting, when
	// not nil, is told what the migration waits for whenever that changes.
	Wait(ctx context.Context, id string, waiting func([]catalog.Blocker)) (catalog.DDLMigration, error)
	// HomeDDLBlockers lists what DDL run directly on database's home shard
	// would overlap; queued is false for a catalog without the operation
	// queue, where the caller keeps its own check.
	HomeDDLBlockers(ctx context.Context, database string) (blockers []catalog.Blocker, queued bool, err error)
	// QueueSchema reports whether the catalog has the operation queue at
	// all. A router newer than its catalog keeps the refusals it had
	// before rather than queueing into something that is not there.
	QueueSchema(ctx context.Context) (bool, error)
}

// PGMigrationQueue queues migrations in pgshard.migrations of the catalog
// and polls the row until the applier finishes it.
type PGMigrationQueue struct {
	Pool *pgxpool.Pool
	// Poll is the wait between state reads; default 200ms.
	Poll time.Duration
	// MaxWait bounds how long Wait tolerates no sign of a controller: no
	// heartbeat for that long on a catalog with the operation queue, or no
	// observable progress on one without. Default DefaultMigrationMaxWait.
	MaxWait time.Duration
	// RetryWindow is how long after an identical migration completed a
	// statement sent again attaches to it; default catalog.DefaultRetryWindow.
	RetryWindow time.Duration
	// CatalogOutage bounds how long Wait keeps reading through catalog
	// errors; default DefaultCatalogOutage.
	CatalogOutage time.Duration
	// BlockersEvery is how often a waiting session asks what its migration
	// waits for; default blockersEvery.
	BlockersEvery time.Duration

	// Tests override the catalog reads.
	load     func(ctx context.Context, id string) (catalog.DDLMigration, error)
	blockers func(ctx context.Context, id string) ([]catalog.Blocker, error)
	beat     func(ctx context.Context) (age time.Duration, found bool, err error)
	queue    func(ctx context.Context) (bool, error)

	// QueueProbeEvery is how long an answer of "this catalog has no
	// operation queue" is trusted; default queueProbeEvery.
	QueueProbeEvery time.Duration

	mu           sync.Mutex
	queuePresent bool
	queueChecked time.Time
	queueErr     error
	// probing is closed by the session running the probe, and is what the
	// others wait on instead of probing too.
	probing chan struct{}
}

// queueProbeEvery is how long the absence of the operation queue is
// trusted. Its presence is permanent and never re-read.
const queueProbeEvery = 30 * time.Second

// queueProbeErrorEvery is how long a failed probe is remembered. Short: a
// catalog that has come back must not wait out the whole window, and one
// probe per second for the whole router is not a storm.
const queueProbeErrorEvery = time.Second

// queueProbeTimeout bounds the probe itself.
const queueProbeTimeout = 10 * time.Second

// DefaultMigrationMaxWait is how long Wait tolerates no sign of a
// controller before giving up: a flat overall deadline would abort a
// migration that simply waits its turn behind a long reshard.
const DefaultMigrationMaxWait = 10 * time.Minute

// DefaultCatalogOutage is how long a waiting statement reads through
// catalog errors -- a primary switchover, a catalog upgrade's cutover --
// before it gives up.
const DefaultCatalogOutage = 5 * time.Minute

// blockersEvery is how often Wait asks what a queued migration waits for.
const blockersEvery = 2 * time.Second

// errNoApplier reports a migration no controller has been seen driving.
type errNoApplier struct {
	id, state string
	quiet     time.Duration
}

func (e errNoApplier) Error() string {
	return fmt.Sprintf("no pgshard controller has been seen applying migrations for %s; migration %s stays %s", e.quiet.Round(time.Second), e.id, e.state)
}

// migrationProgress fingerprints the durable state of m: Wait resets its
// inactivity deadline whenever this changes.
func migrationProgress(m catalog.DDLMigration) string {
	keys := make([]string, 0, len(m.PerShard))
	for k := range m.PerShard {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(m.State)
	for _, k := range keys {
		s := m.PerShard[k]
		fmt.Fprintf(&b, "|%s=%s/%d/%d", k, s.State, s.Attempts, s.Step)
	}
	return b.String()
}

// queued reports whether the catalog has the operation queue; once it has,
// it always will.
//
// One PGMigrationQueue serves every session of a router, so this answer is
// shared and the probe behind it is taken once: the lock is never held
// across the catalog read, callers that arrive while a probe is in flight
// wait for that one rather than issuing their own, and a probe that fails
// is remembered too. The probe used to run under the mutex, and a failing
// one cached nothing -- so against a catalog that was reachable but slow,
// every 200ms poll of every waiting migration and every statement on a
// local database re-issued it and queued behind the last, turning an outage
// that should degrade in parallel into a convoy. sync.Mutex is not
// cancellable, so a session parked there could not be cut short by its own
// deadline, its statement_timeout or a client's cancel.
func (q *PGMigrationQueue) queued(ctx context.Context) (bool, error) {
	probe := q.queue
	if probe == nil {
		if q.Pool == nil {
			return false, nil
		}
		probe = func(ctx context.Context) (bool, error) { return catalog.QueueSchema(ctx, q.Pool) }
	}
	every := q.QueueProbeEvery
	if every <= 0 {
		every = queueProbeEvery
	}
	for {
		q.mu.Lock()
		// A failure is trusted for much less time than an answer: it
		// collapses a storm of retries without making a catalog that has
		// come back wait out the window. Sessions arriving together share
		// one probe whatever the outcome.
		window := every
		if q.queueErr != nil {
			window = min(every, queueProbeErrorEvery)
		}
		if q.queuePresent || (!q.queueChecked.IsZero() && time.Since(q.queueChecked) < window) {
			present, err := q.queuePresent, q.queueErr
			q.mu.Unlock()
			return present, err
		}
		if inFlight := q.probing; inFlight != nil {
			q.mu.Unlock()
			select {
			case <-inFlight:
				continue
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		done := make(chan struct{})
		q.probing = done
		q.mu.Unlock()
		// The probe is a fact about the catalog, not about this session,
		// so it is not cut short by this caller going away -- and it is
		// bounded, because the session context that used to carry it is
		// unbounded unless the operator set --max-query-duration.
		pctx, stop := context.WithTimeout(context.WithoutCancel(ctx), queueProbeTimeout)
		present, err := probe(pctx)
		stop()
		q.mu.Lock()
		q.queuePresent, q.queueErr, q.queueChecked = present, err, time.Now()
		q.probing = nil
		q.mu.Unlock()
		close(done)
		return present, err
	}
}

// QueueSchema implements MigrationQueue.
func (q *PGMigrationQueue) QueueSchema(ctx context.Context) (bool, error) { return q.queued(ctx) }

// Enqueue implements MigrationQueue.
func (q *PGMigrationQueue) Enqueue(ctx context.Context, m catalog.DDLMigration) (catalog.EnqueueResult, error) {
	queued, err := q.queued(ctx)
	if err != nil {
		return catalog.EnqueueResult{}, err
	}
	if !queued {
		m.DedupKey = ""
		id, err := catalog.EnqueueMigration(ctx, q.Pool, m)
		return catalog.EnqueueResult{ID: id, State: catalog.MigrationQueued}, err
	}
	window := q.RetryWindow
	if window <= 0 {
		window = catalog.DefaultRetryWindow
	}
	return catalog.EnqueueMigrationOnce(ctx, q.Pool, m, window)
}

// HomeDDLBlockers implements MigrationQueue.
func (q *PGMigrationQueue) HomeDDLBlockers(ctx context.Context, database string) ([]catalog.Blocker, bool, error) {
	queued, err := q.queued(ctx)
	if err != nil || !queued {
		return nil, false, err
	}
	blockers, err := catalog.HomeDDLBlockers(ctx, q.Pool, database)
	return blockers, true, err
}

// Wait implements MigrationQueue.
func (q *PGMigrationQueue) Wait(ctx context.Context, id string, waiting func([]catalog.Blocker)) (catalog.DDLMigration, error) {
	poll := q.Poll
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}
	maxWait := q.MaxWait
	if maxWait <= 0 {
		maxWait = DefaultMigrationMaxWait
	}
	outage := q.CatalogOutage
	if outage <= 0 {
		outage = DefaultCatalogOutage
	}
	load, blockers, beat := q.load, q.blockers, q.beat
	if load == nil {
		load = func(ctx context.Context, id string) (catalog.DDLMigration, error) {
			return catalog.LoadMigration(ctx, q.Pool, id)
		}
	}
	if blockers == nil {
		blockers = func(ctx context.Context, id string) ([]catalog.Blocker, error) {
			return catalog.OperationBlockers(ctx, q.Pool, catalog.OperationDDL, id)
		}
	}
	if beat == nil {
		beat = func(ctx context.Context) (time.Duration, bool, error) {
			// The applier's PASS, not its liveness goroutine: the
			// goroutine beats on its own timer and keeps beating through a
			// pass that never returns, so waiting on it meant waiting for
			// ever on a wedged controller (PGS-907).
			return catalog.ApplierProgressAge(ctx, q.Pool)
		}
	}
	every := q.BlockersEvery
	if every <= 0 {
		every = blockersEvery
	}
	deadline := time.Now().Add(maxWait)
	last, lastBlockers := "", ""
	var failingSince, blockersAt time.Time
	var m catalog.DDLMigration
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		err := func() error {
			var err error
			if m, err = load(ctx, id); err != nil {
				return err
			}
			if m.State == catalog.MigrationComplete || m.State == catalog.MigrationFailed {
				return nil
			}
			if cur := migrationProgress(m); cur != last {
				last, deadline = cur, time.Now().Add(maxWait)
			}
			queued, err := q.queued(ctx)
			if err != nil || !queued {
				return err
			}
			age, found, err := beat(ctx)
			if err != nil {
				return err
			}
			if found && age < maxWait {
				deadline = time.Now().Add(maxWait - age)
			}
			if waiting != nil && m.State == catalog.MigrationQueued && time.Since(blockersAt) >= every {
				blockersAt = time.Now()
				bs, err := blockers(ctx, id)
				if err != nil {
					return err
				}
				if key := fmt.Sprint(bs); key != lastBlockers {
					lastBlockers = key
					waiting(bs)
				}
			}
			return nil
		}()
		switch {
		case err != nil && ctx.Err() == nil:
			if failingSince.IsZero() {
				failingSince = time.Now()
			}
			if time.Since(failingSince) > outage {
				return m, err
			}
			deadline = time.Now().Add(maxWait)
		case err != nil:
			return m, ctx.Err()
		default:
			failingSince = time.Time{}
			if m.State == catalog.MigrationComplete || m.State == catalog.MigrationFailed {
				return m, nil
			}
			if time.Now().After(deadline) {
				return m, errNoApplier{id: id, state: m.State, quiet: maxWait}
			}
		}
		select {
		case <-ctx.Done():
			return m, ctx.Err()
		case <-t.C:
		}
	}
}

// ddlAsync reports whether the session set pgshard.ddl_async.
func (e *Executor) ddlAsync() bool {
	on := false
	for _, list := range [][]gucEntry{e.gucs, e.staged} {
		for _, g := range list {
			switch g.name {
			case "":
				on = false
			case DDLAsyncGUC:
				on = gucOn(g.value)
			}
		}
	}
	return on
}

func gucOn(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "on", "true", "yes", "1":
		return true
	}
	return false
}

// runMigration queues the DDL of pl and answers the client when the
// applier has finished it (or at once under pgshard.ddl_async).
func (e *Executor) runMigration(ctx context.Context, pl plan.Plan, w pgwire.ResultWriter) error {
	m := pl.Migration
	if e.tx == pgwire.TxIdle {
		if err := e.queueMigration(ctx, m, w); err != nil {
			return err
		}
		return w.CommandComplete(m.Kind)
	}
	if err := e.refuseDDLInTransaction(m); err != nil {
		return err
	}
	if err := w.Notice(&pgproto3.NoticeResponse{Severity: "NOTICE", SeverityUnlocalized: "NOTICE", Code: "00000",
		Message: fmt.Sprintf("%s is applied on its own, not as part of this transaction", m.Kind),
		Hint:    fmt.Sprintf("database %q runs DDL transactions sequentially: a ROLLBACK does not undo it", e.info.Database)}); err != nil {
		return err
	}
	// The transaction has run nothing but its prelude, so its backend does
	// not wait out the migration inside it: idle_in_transaction_session_timeout
	// would end it, and a barrier's drain would wait for it. The prelude
	// opens it again before the statement is answered.
	err := e.releaseUntouchedTxn(ctx)
	if err == nil {
		err = e.queueMigration(ctx, m, w)
	}
	if err == nil {
		if rerr := e.replayPrelude(ctx); rerr != nil {
			// The DDL is in; a client told only that the connection failed
			// would send it again and be told it already exists.
			perr := pgwire.Errorf(codeConnectionFailure, "%s was applied, but opening the transaction again afterwards failed: %v", m.Kind, rerr)
			perr.Hint = "the DDL stays applied; end this transaction and do not send it again"
			err = perr
		}
	}
	if err != nil {
		// PostgreSQL fails the transaction a statement errors in. The
		// transaction holds nothing on a shard yet, so its backend goes and
		// the router answers for the failed transaction until it ends.
		e.dropStream()
		e.failTxn()
		return err
	}
	e.txnRanDDL = true
	return w.CommandComplete(m.Kind)
}

// refuseDDLInTransaction refuses DDL inside a transaction, unless the
// database runs DDL transactions sequentially and the transaction has run
// nothing on a shard.
func (e *Executor) refuseDDLInTransaction(m *plan.Migration) error {
	if e.tx == pgwire.TxFailed {
		return pgwire.Errorf("25P02", "current transaction is aborted, commands ignored until end of transaction block")
	}
	sequential := e.ddlRunsSequentially()
	if sequential && !e.txnTouched && !e.multiShardTxn() {
		return nil
	}
	if sequential {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%s is not available in a transaction that has already run a statement on a shard: DDL here is applied on its own, so it cannot commit together with that statement", m.Kind)
		err.Hint = "run the DDL in a transaction of its own"
		return err
	}
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%s inside a transaction block is not available through the router: DDL fans out to every shard and cannot be rolled back with the transaction", m.Kind)
	err.Hint = "run DDL outside BEGIN/COMMIT; each shard applies it in its own transaction"
	if e.implicitTx {
		// The client sent no BEGIN, so a hint about its own is a hint
		// about something it did not do. What it sent is a batch, and
		// the transaction is the one this router opened around it.
		err = pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%s is not available inside a multi-statement simple query: it fans out to every shard and cannot be rolled back with the transaction the batch runs in", m.Kind)
		err.Hint = "send the DDL as its own query, not as one statement of a semicolon-separated batch"
	}
	return err
}

// refuseShardStatementAfterDDL refuses a statement that would run on a
// shard in a transaction that has already applied DDL on its own: the two
// could not commit or roll back together.
func (e *Executor) refuseShardStatementAfterDDL(pl plan.Plan) error {
	if !e.txnRanDDL || pl.Kind == plan.MigrationKind || pl.Kind == plan.SessionLocal || pl.Class.Txn != plan.TxnNone {
		return nil
	}
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "a statement that runs on a shard is not available after DDL in the same transaction: the DDL was applied on its own, so the two cannot commit together")
	err.Hint = "commit the transaction that ran the DDL, and run this statement in another"
	return err
}

// ddlRunsSequentially reports whether the session's database runs a
// transaction's DDL statement by statement (pgshard.databases.ddl_transactions).
func (e *Executor) ddlRunsSequentially() bool {
	if e.catalogSession() {
		return false
	}
	snap := e.r.cfg.Snapshot()
	return snap != nil && snap.Databases[e.info.Database].DDLTransactions == catalog.DDLTransactionsSequential
}

// queueMigration queues m and waits for the applier to finish it (or not,
// under pgshard.ddl_async); the caller answers the statement.
func (e *Executor) queueMigration(ctx context.Context, m *plan.Migration, w pgwire.ResultWriter) error {
	if e.r.cfg.Migrations == nil {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "DDL is not available: the router has no migration queue")
		err.Hint = "start the router with a catalog connection that may write pgshard.migrations"
		return err
	}
	// The LIVE home shard, deliberately, and not the statement's snapshot.
	// A migration is applied in the future, so the home shard that matters
	// is the one at apply time: catalog.SaveQueuedMigrationProgress rewrites
	// this field when the migration starts and refuses queued -> running
	// while it differs from pgshard.databases, precisely because "one queued
	// before a cutover and held through it was recorded with the retired
	// set's home shard". Pinning it to plan time is the wrong direction.
	req := catalog.DDLMigration{Database: e.info.Database, Statement: m.Statement, Kind: m.Kind, Strategy: m.Strategy, Scope: m.Scope,
		HomeShard: e.Home().ID, Meta: catalog.MigrationMeta{
			SearchPath:    e.recordedSearchPath(),
			Object:        catalog.MigrationObject{Kind: m.Object.Kind, Schema: m.Object.Schema, Name: m.Object.Name, Table: m.Object.Table, Expect: m.Object.Expect},
			Target:        m.Target,
			RunAs:         e.info.User,
			Role:          m.Role,
			RoleOp:        m.RoleOp,
			Verifier:      m.Verifier,
			ClearVerifier: m.ClearVerifier,
			Roles:         m.Roles,
			View:          m.View,
			Database:      m.Database, DatabaseOp: m.DatabaseOp, Steps: migrationSteps(m.Steps), Rewrite: m.Rewrite}}
	if !e.ddlDedupOff() {
		req.DedupKey = catalog.MigrationDedupKey(req, normalizedStatement(m.Statement))
	}
	queued, err := e.r.cfg.Migrations.Enqueue(ctx, req)
	if err != nil {
		return pgwire.Errorf(codeConnectionFailure, "queueing the migration in the catalog failed: %v", err)
	}
	id := queued.ID
	hint := "SELECT state, per_shard FROM pgshard.migrations_public WHERE id = '" + id + "'"
	if queued.Attached {
		msg := fmt.Sprintf("an identical migration %s is already %s; waiting for it instead of queueing the statement again", id, queued.State)
		if queued.State == catalog.MigrationComplete {
			msg = fmt.Sprintf("an identical migration %s has just completed; not running the statement again", id)
		}
		if err := notice(w, msg, hint); err != nil {
			return err
		}
	}
	if e.ddlAsync() {
		if !queued.Attached {
			if err := notice(w, fmt.Sprintf("migration %s queued; the statement is applied in the background", id), hint); err != nil {
				return err
			}
		}
		// The caller sends the command tag, synchronously or not. Sending
		// it here too put two CommandComplete frames on the wire for one
		// statement, which PostgreSQL never does and which desynchronises
		// a client that pairs one tag with one Execute.
		return nil
	}
	waitCtx, stop := ctx, context.CancelFunc(func() {})
	if d := e.statementTimeout(); d > 0 {
		waitCtx, stop = context.WithTimeoutCause(ctx, d, errStatementTimeout)
	}
	defer stop()
	done, err := e.r.cfg.Migrations.Wait(waitCtx, id, func(blockers []catalog.Blocker) {
		if len(blockers) > 0 {
			_ = notice(w, fmt.Sprintf("migration %s waits for %s", id, describeBlockers(blockers)), hint)
		}
	})
	if err != nil {
		continues := fmt.Sprintf("Migration %s continues in the background.", id)
		// What the enqueue actually stored, not what this router asked
		// for: a catalog with no operation queue has nowhere to keep a
		// key, and promising the client its retry will wait is how the
		// retry builds the index a second time.
		if queued.Deduplicated {
			continues += " Running the same statement again waits for it while it is queued or running."
		}
		var quiet errNoApplier
		switch {
		case errors.Is(context.Cause(waitCtx), errStatementTimeout) && ctx.Err() == nil:
			e := pgwire.Errorf(pgwire.CodeQueryCanceled, "canceling statement due to statement timeout")
			e.Detail, e.Hint = continues, hint
			return e
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			e := pgwire.Errorf(pgwire.CodeQueryCanceled, "canceling statement due to user request")
			e.Detail, e.Hint = continues, hint
			return e
		case errors.As(err, &quiet):
			e := pgwire.Errorf(codeObjectNotInPrerequisiteState, "%v", quiet)
			e.Detail, e.Hint = continues, "check that a pgshard controller is running and leading"
			return e
		}
		return pgwire.Errorf(codeConnectionFailure, "waiting for migration %s: %v", id, err)
	}
	if done.State == catalog.MigrationFailed {
		return migrationError(done)
	}
	if err := e.refreshAfterMigration(ctx); err != nil {
		if err := w.Notice(&pgproto3.NoticeResponse{Severity: "WARNING", SeverityUnlocalized: "WARNING", Code: "01000",
			Message: fmt.Sprintf("migration %s is applied, but this router could not reload the catalog: %v", id, err),
			Hint:    "the next statement may be planned without what the migration changed until the router's catalog snapshot reloads"}); err != nil {
			return err
		}
	}
	return nil
}

// errStatementTimeout is the cause of a DDL wait the session's
// statement_timeout ended.
var errStatementTimeout = errors.New("statement timeout")

// codeObjectNotInPrerequisiteState is SQLSTATE 55000.
const codeObjectNotInPrerequisiteState = "55000"

// notice sends a NOTICE and pushes it to the client now: a statement waiting
// in the queue may not answer for hours, and a notice buffered until then
// says nothing.
func notice(w pgwire.ResultWriter, message, hint string) error {
	if err := w.Notice(&pgproto3.NoticeResponse{Severity: "NOTICE", SeverityUnlocalized: "NOTICE", Code: "00000", Message: message, Hint: hint}); err != nil {
		return err
	}
	if f, ok := w.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

// describeBlockers names what a migration waits for without the statement
// text of anyone else's migration.
func describeBlockers(blockers []catalog.Blocker) string {
	names := make([]string, 0, len(blockers))
	for _, b := range blockers {
		what := map[string]string{catalog.OperationDDL: "migration", catalog.OperationReshard: "reshard",
			catalog.OperationUpgrade: "major upgrade", catalog.OperationPlacement: "table placement"}[b.Kind]
		if what == "" {
			what = b.Kind
		}
		when := "in progress"
		if b.Reason == catalog.BlockedByEarlier {
			when = "queued before it"
		}
		names = append(names, fmt.Sprintf("%s %s (%s)", what, b.ID, when))
	}
	return strings.Join(names, ", ")
}

// normalizedStatement is sql as the parser renders it, so a statement sent
// again with other whitespace, letter case or comments is recognised; sql
// itself when it does not parse back.
func normalizedStatement(sql string) string {
	res, err := pgparser.Parse(sql)
	if err != nil {
		return sql
	}
	out, err := pgparser.Deparse(res.Tree)
	if err != nil {
		return sql
	}
	return out
}

// ddlDedupOff reports whether the session turned pgshard.ddl_dedup off.
func (e *Executor) ddlDedupOff() bool {
	off := false
	for _, list := range [][]gucEntry{e.gucs, e.staged} {
		for _, g := range list {
			switch g.name {
			case "":
				off = false
			case DDLDedupGUC:
				off = g.value != "" && !gucOn(g.value)
			}
		}
	}
	return off
}

// statementTimeout is the session's statement_timeout: the startup value,
// then every settled and staged SET and RESET in order.
func (e *Executor) statementTimeout() time.Duration {
	startup := e.info.Params["statement_timeout"]
	if v, ok := startupOption(e.info.Params["options"], "statement_timeout"); ok {
		startup = v
	}
	value := startup
	for _, list := range [][]gucEntry{e.gucs, e.staged} {
		for _, g := range list {
			switch g.name {
			case "":
				value = startup
			case "statement_timeout":
				// RESET, and SET TO DEFAULT, carry no value and put the
				// session back to what it started with.
				value = g.value
				if value == "" {
					value = startup
				}
			}
		}
	}
	return parseTimeGUC(value)
}

// parseTimeGUC reads a time setting whose base unit is milliseconds, as
// PostgreSQL does: a bare number is milliseconds, and us, ms, s, min, h and d
// name their unit. Anything else is no timeout.
func parseTimeGUC(v string) time.Duration {
	v = strings.TrimSpace(v)
	i := strings.IndexFunc(v, func(r rune) bool { return (r < '0' || r > '9') && r != '.' })
	num, unit := v, ""
	if i >= 0 {
		num, unit = v[:i], strings.TrimSpace(v[i:])
	}
	n, err := strconv.ParseFloat(num, 64)
	if err != nil || n <= 0 {
		return 0
	}
	scale := map[string]time.Duration{"": time.Millisecond, "us": time.Microsecond, "ms": time.Millisecond, "s": time.Second,
		"min": time.Minute, "h": time.Hour, "d": 24 * time.Hour}[unit]
	if scale == 0 {
		return 0
	}
	return time.Duration(n * float64(scale))
}

// checkFanoutDDL refuses a fanned-out migration while a reshard copies, on
// a catalog that has no operation queue to wait in.
//
// With the queue, the migration is enqueued and waits its turn, which is
// the point of all this. Without it, the applier holds the migration for
// the whole copy while the router waits for progress that cannot come, and
// after ten minutes tells the client no controller has been seen applying
// migrations -- when one is running and leading, and a reshard is simply
// holding the migration. A router newer than its catalog is an ordinary
// moment in a rollout, and this is the refusal it used to give in
// milliseconds.
func (e *Executor) checkFanoutDDL(ctx context.Context) error {
	if e.r.cfg.Migrations == nil {
		return nil
	}
	queued, err := e.r.cfg.Migrations.QueueSchema(ctx)
	if err != nil {
		return pgwire.Errorf(codeConnectionFailure, "reading the operation queue: %v", err)
	}
	if queued {
		return nil
	}
	if snap := e.r.cfg.Snapshot(); snap != nil && snap.Resharding() {
		return reshardingDDLRefusal()
	}
	return nil
}

// reshardingDDLRefusal is the refusal both DDL paths give while a reshard
// holds the shard map.
//
// The hint names the FAILED case as well as the running one, because the
// snapshot cannot tell them apart: Resharding() is true while any set is in
// provisioning (snapshot.go), and a reshard that failed leaves its set
// exactly there. So "retry once the reshard completes" -- which was the
// whole hint -- is advice that can never come true for the case an operator
// is most likely to be in when they hit this, and a failed reshard is
// precisely when they need DDL to repair or work around (PGS-876).
func reshardingDDLRefusal() *pgwire.Error {
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "DDL is not available while a reshard is active: the copy replicates rows only, and a schema change on the serving shards would break the new shards' apply")
	err.Hint = "retry once the reshard completes; if it has FAILED it holds the shard map until it is cleared, which reverting spec.shards to the serving count does (or deleting the set's rows for a catalog-sourced one) -- see docs/resharding.md. A catalog migrated to the operation queue queues the statement instead of refusing it"
	return err
}

// checkHomeDDL refuses DDL a local database runs directly on its home shard
// while something it would overlap is unfinished: it cannot wait its turn in
// the operation queue the way a migration does.
func (e *Executor) checkHomeDDL(ctx context.Context) error {
	var blockers []catalog.Blocker
	queued := false
	if e.r.cfg.Migrations != nil {
		var err error
		if blockers, queued, err = e.r.cfg.Migrations.HomeDDLBlockers(ctx, e.info.Database); err != nil {
			return pgwire.Errorf(codeConnectionFailure, "reading the operation queue: %v", err)
		}
	}
	if !queued {
		if snap := e.r.cfg.Snapshot(); snap != nil && snap.Resharding() {
			// The same refusal as checkFanoutDDL, hint included: this one
			// carried none at all, so which of the two paths a statement
			// took decided whether the client was told anything about
			// recovering.
			return reshardingDDLRefusal()
		}
		return nil
	}
	if len(blockers) == 0 {
		return nil
	}
	err := pgwire.Errorf(codeObjectNotInPrerequisiteState, "schema changes on local database %s are not available while %s is unfinished", e.info.Database, describeBlockers(blockers))
	err.Detail = "DDL on a local database runs on its home shard at once and cannot wait its turn behind a reshard, upgrade or table placement."
	err.Hint = "retry once it completes; a shorter resharding.retireOldGroupsAfter shortens a reshard's wait"
	return err
}

// migrationRefreshTimeout bounds how long an applied migration waits for
// the router's snapshot to catch up before it is answered anyway.
const migrationRefreshTimeout = 5 * time.Second

// refreshAfterMigration reloads the snapshot before an applied migration is
// answered. The watcher reloads on its own after a migration notifies, but
// behind a notification budget every per-shard step of a migration draws
// on, so the reload can land after the client's next statement has been
// planned -- without a view the migration recorded, which is then read from
// one shard with no error (PGS-871).
func (e *Executor) refreshAfterMigration(ctx context.Context) error {
	if e.r.cfg.RefreshSnapshot == nil {
		return nil
	}
	// Not cut short by the client: the migration is already applied, and the
	// bound is short.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), migrationRefreshTimeout)
	defer cancel()
	return e.r.cfg.RefreshSnapshot(ctx)
}

// migrationError reports a failed migration with the shard that failed
// first and the shards left applied (DEGRADED: the schema differs across
// shards until the statement is fixed and re-run).
func migrationError(m catalog.DDLMigration) error {
	var failed, applied []string
	code, msg := pgwire.CodeInternalError, m.Error
	for id, s := range m.PerShard {
		switch s.State {
		case catalog.ShardFailed:
			failed = append(failed, id)
			if s.SQLState != "" && (code == pgwire.CodeInternalError || id < failed[0]) {
				code, msg = s.SQLState, s.Error
			}
		case catalog.ShardApplied:
			applied = append(applied, id)
		}
	}
	sort.Strings(failed)
	sort.Strings(applied)
	if msg == "" {
		msg = "migration failed"
	}
	err := pgwire.Errorf(code, "%s", msg)
	err.Detail = fmt.Sprintf("migration %s failed on shard %s", m.ID, strings.Join(failed, ", "))
	if len(applied) > 0 {
		err.Detail += fmt.Sprintf("; applied on shard %s (schema is DEGRADED until the statement succeeds everywhere)", strings.Join(applied, ", "))
	}
	err.Hint = "fix the cause and run the statement again; shards where it already applied are skipped for idempotent forms (IF NOT EXISTS / IF EXISTS)"
	return err
}

// migrationBatch answers an extended-protocol batch whose statements are
// migrations: Describe reports no parameters and no rows, and each Execute
// runs the migration its own portal was bound to. ddl is Executor.batchDDL.
func (e *Executor) migrationBatch(ctx context.Context, batch []*pgshardv1.ExecuteRequest, ddl []*plan.Plan, w pgwire.ResultWriter) (bool, error) {
	var ddlSeen, other bool
	for _, pl := range ddl {
		ddlSeen = ddlSeen || pl != nil
		other = other || pl == nil
	}
	if !ddlSeen {
		return false, nil
	}
	if other {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "DDL must be the only statement of its batch")
		err.Hint = "send a Sync before and after it"
		return true, err
	}
	next := 0
	for _, req := range batch {
		switch r := req.Message.(type) {
		case *pgshardv1.ExecuteRequest_Parse, *pgshardv1.ExecuteRequest_Bind:
			next++
			if err := e.answerStagedCompletions(w, []*pgshardv1.ExecuteRequest{req}); err != nil {
				return true, err
			}
		case *pgshardv1.ExecuteRequest_Describe:
			if r.Describe.Kind == pgshardv1.Describe_KIND_STATEMENT {
				if err := w.ParameterDescription(nil); err != nil {
					return true, err
				}
			}
			if err := w.NoData(); err != nil {
				return true, err
			}
		case *pgshardv1.ExecuteRequest_Execute:
			pl := ddl[next]
			next++
			if err := e.runMigration(ctx, *pl, w); err != nil {
				return true, err
			}
		}
	}
	return true, nil
}

func migrationSteps(steps []plan.Step) []catalog.MigrationStep {
	if len(steps) == 0 {
		return nil
	}
	out := make([]catalog.MigrationStep, len(steps))
	for i, s := range steps {
		out[i] = catalog.MigrationStep{SQL: s.SQL, Concurrent: s.Concurrent, Index: s.Index, OnFail: s.OnFail,
			Skip: catalog.MigrationCheck{Kind: s.Skip.Kind, Schema: s.Skip.Schema, Table: s.Skip.Table, Name: s.Skip.Name, NameSchema: s.Skip.NameSchema}}
	}
	return out
}
