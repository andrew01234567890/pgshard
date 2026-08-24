package router

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// DDLAsyncGUC is the session setting that makes DDL return as soon as its
// migration is queued.
const DDLAsyncGUC = "pgshard.ddl_async"

// MigrationQueue hands DDL to the controller's applier and reports how it
// ended.
type MigrationQueue interface {
	Enqueue(ctx context.Context, m catalog.DDLMigration) (id string, err error)
	// Wait blocks until the migration is complete or failed.
	Wait(ctx context.Context, id string) (catalog.DDLMigration, error)
}

// PGMigrationQueue queues migrations in pgshard.migrations of the catalog
// and polls the row until the applier finishes it.
type PGMigrationQueue struct {
	Pool *pgxpool.Pool
	// Poll is the wait between state reads; default 200ms.
	Poll time.Duration
	// MaxWait bounds how long Wait tolerates a migration making no
	// observable progress, so a deployment without a running applier does
	// not block the client forever; any state change resets it. Default
	// DefaultMigrationMaxWait.
	MaxWait time.Duration
	// load overrides the catalog read in tests.
	load func(ctx context.Context, id string) (catalog.DDLMigration, error)
}

// DefaultMigrationMaxWait is how long Wait tolerates a migration whose
// state does not change before giving up: a flat overall deadline would
// abort a legitimately-progressing rewrite or CREATE INDEX CONCURRENTLY
// that simply takes longer.
const DefaultMigrationMaxWait = 10 * time.Minute

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

// Enqueue implements MigrationQueue.
func (q *PGMigrationQueue) Enqueue(ctx context.Context, m catalog.DDLMigration) (string, error) {
	return catalog.EnqueueMigration(ctx, q.Pool, m)
}

// Wait implements MigrationQueue.
func (q *PGMigrationQueue) Wait(ctx context.Context, id string) (catalog.DDLMigration, error) {
	poll := q.Poll
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}
	maxWait := q.MaxWait
	if maxWait <= 0 {
		maxWait = DefaultMigrationMaxWait
	}
	deadline := time.Now().Add(maxWait)
	last := ""
	t := time.NewTicker(poll)
	defer t.Stop()
	load := q.load
	if load == nil {
		load = func(ctx context.Context, id string) (catalog.DDLMigration, error) {
			return catalog.LoadMigration(ctx, q.Pool, id)
		}
	}
	for {
		m, err := load(ctx, id)
		if err != nil {
			return m, err
		}
		if m.State == catalog.MigrationComplete || m.State == catalog.MigrationFailed {
			return m, nil
		}
		if cur := migrationProgress(m); cur != last {
			last = cur
			deadline = time.Now().Add(maxWait)
		} else if time.Now().After(deadline) {
			return m, fmt.Errorf("migration %s is still %s with no progress observed for %s; it continues in the background: is a pgshard controller running the DDL applier?", id, m.State, maxWait)
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
	if e.tx != pgwire.TxIdle {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%s inside a transaction block is not available through the router: DDL fans out to every shard and cannot be rolled back with the transaction", m.Kind)
		err.Hint = "run DDL outside BEGIN/COMMIT; each shard applies it in its own transaction"
		return err
	}
	if e.r.cfg.Migrations == nil {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "DDL is not available: the router has no migration queue")
		err.Hint = "start the router with a catalog connection that may write pgshard.migrations"
		return err
	}
	req := catalog.DDLMigration{Database: e.info.Database, Statement: m.Statement, Kind: m.Kind, Strategy: m.Strategy, Scope: m.Scope,
		HomeShard: e.home.ID, Meta: catalog.MigrationMeta{
			Object:   catalog.MigrationObject{Kind: m.Object.Kind, Schema: m.Object.Schema, Name: m.Object.Name, Expect: m.Object.Expect},
			RunAs:    e.info.User,
			Role:     m.Role,
			RoleOp:   m.RoleOp,
			Verifier: m.Verifier,
			Roles:    m.Roles,
			Database: m.Database, DatabaseOp: m.DatabaseOp, Steps: migrationSteps(m.Steps), Rewrite: m.Rewrite}}
	id, err := e.r.cfg.Migrations.Enqueue(ctx, req)
	if err != nil {
		return pgwire.Errorf(codeConnectionFailure, "queueing the migration in the catalog failed: %v", err)
	}
	if e.ddlAsync() {
		if err := w.Notice(&pgproto3.NoticeResponse{Severity: "NOTICE", SeverityUnlocalized: "NOTICE", Code: "00000",
			Message: fmt.Sprintf("migration %s queued; the statement is applied in the background", id),
			Hint:    "SELECT state, per_shard FROM pgshard.migrations WHERE id = '" + id + "'"}); err != nil {
			return err
		}
		return w.CommandComplete(m.Kind)
	}
	done, err := e.r.cfg.Migrations.Wait(ctx, id)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			e := pgwire.Errorf("57014", "canceling statement due to user request; migration %s continues in the background", id)
			e.Hint = "SELECT state, per_shard FROM pgshard.migrations WHERE id = '" + id + "'"
			return e
		}
		return pgwire.Errorf(codeConnectionFailure, "waiting for migration %s: %v", id, err)
	}
	if done.State == catalog.MigrationFailed {
		return migrationError(done)
	}
	return w.CommandComplete(m.Kind)
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

// migrationBatch answers an extended-protocol batch whose statement is a
// migration: Describe reports no parameters and no rows, Execute runs it.
func (e *Executor) migrationBatch(ctx context.Context, batch []*pgshardv1.ExecuteRequest, parsed []string, w pgwire.ResultWriter) (bool, error) {
	var mig *prepared
	other := false
	stmtOf := func(portal string) string { return e.portals[portal] }
	for _, req := range batch {
		var stmt string
		switch r := req.Message.(type) {
		case *pgshardv1.ExecuteRequest_Parse:
			if len(parsed) == 0 {
				continue
			}
			stmt, parsed = parsed[0], parsed[1:]
		case *pgshardv1.ExecuteRequest_Bind:
			stmt = stmtOf(r.Bind.Portal)
		case *pgshardv1.ExecuteRequest_Execute:
			stmt = stmtOf(r.Execute.Portal)
		default:
			continue
		}
		st, ok := e.stmts[stmt]
		if ok && st.plan.Kind == plan.MigrationKind {
			mig = &st
		} else {
			other = true
		}
	}
	if mig == nil {
		return false, nil
	}
	if other {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "DDL must be the only statement of its batch")
		err.Hint = "send a Sync before and after it"
		return true, err
	}
	for _, req := range batch {
		switch r := req.Message.(type) {
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
			if err := e.runMigration(ctx, mig.plan, w); err != nil {
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
