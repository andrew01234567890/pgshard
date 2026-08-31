// Package twopc holds the decision table that finishes prepared transactions
// against the router's decision log after a restore, and the SQL that
// applies it on one participant.
package twopc

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// GIDPrefix starts every transaction identifier the router coordinates.
const GIDPrefix = "pgshard-"

// State is what the decision log records for a transaction. The empty
// state is the absence of a row, which is presumed abort; a state this
// build does not recognise is not the same thing.
type State string

// Decision states of the log.
const (
	StateNone      State = ""
	StatePreparing State = "preparing"
	StateCommit    State = "commit"
	StateAbort     State = "abort"
)

// Known reports whether s is a state this build can act on.
func (s State) Known() bool {
	switch s {
	case StateNone, StatePreparing, StateCommit, StateAbort:
		return true
	}
	return false
}

// Action is what to do with one gid on one participant.
type Action int

// Actions of the decision table.
const (
	// Nothing: the participant already finished the transaction as decided.
	Nothing Action = iota
	// Commit runs COMMIT PREPARED.
	Commit
	// Rollback runs ROLLBACK PREPARED.
	Rollback
	// Contradiction: the log says commit but the participant neither holds
	// the transaction prepared nor committed it. The restore is inconsistent.
	Contradiction
	// Unverifiable: the log says commit, the participant does not hold the
	// transaction prepared and its transaction id no longer resolves
	// (frozen past the clog horizon, unrecorded or in the future). Treated
	// like a contradiction, reported apart.
	Unverifiable
	// Unreadable: the log records a state this build does not recognise --
	// a newer coordinator's, or a corrupted row. The transaction is left
	// prepared and reported. Reading it as an abort is how a distributed
	// transaction that committed everywhere else loses one participant.
	Unreadable
)

var actionNames = [...]string{"nothing", "commit", "rollback", "contradiction", "unverifiable", "unreadable"}

func (a Action) String() string {
	if int(a) < len(actionNames) {
		return actionNames[a]
	}
	return fmt.Sprintf("Action(%d)", int(a))
}

// XactStatus is pg_xact_status of the participant's transaction id;
// StatusUnavailable when the id is unknown or no longer resolvable.
type XactStatus string

// Statuses pg_xact_status reports, plus StatusUnavailable for a NULL answer
// (xid frozen past the clog horizon), a future or an unrecorded id.
const (
	StatusCommitted   XactStatus = "committed"
	StatusAborted     XactStatus = "aborted"
	StatusInProgress  XactStatus = "in progress"
	StatusUnavailable XactStatus = ""
)

// Decide is the decision table. state is the log's decision (StateNone
// when the log has no row for the gid), prepared whether the participant
// still holds the transaction, status the participant's view of the
// transaction id when it is not prepared.
func Decide(state State, prepared bool, status XactStatus) Action {
	if !state.Known() {
		return Unreadable
	}
	switch {
	case prepared && state == StateCommit:
		return Commit
	case prepared:
		return Rollback
	case state == StateCommit && status == StatusCommitted:
		return Nothing
	case state == StateCommit && status == StatusUnavailable:
		return Unverifiable
	case state == StateCommit:
		return Contradiction
	}
	return Nothing
}

// Participation is one shard's part of a transaction, carrying its own
// transaction id rather than a position in a second array.
type Participation struct {
	Shard int32
	// XID is the transaction id on that shard, empty when the router
	// recorded none.
	XID string
}

// Decision is one row of the decision log.
type Decision struct {
	GID          string
	State        State
	Participants []Participation
}

// XID returns the transaction id recorded for shard, if any.
func (d Decision) XID(shard int32) string {
	for _, p := range d.Participants {
		if p.Shard == shard {
			return p.XID
		}
	}
	return ""
}

// Involves reports whether shard is a participant.
func (d Decision) Involves(shard int32) bool {
	for _, p := range d.Participants {
		if p.Shard == shard {
			return true
		}
	}
	return false
}

// Participant is one shard as the reconciler sees it.
type Participant interface {
	// Prepared lists the router-coordinated prepared transactions with the
	// database each was prepared in.
	Prepared(ctx context.Context) (map[string]string, error)
	// Finish commits or rolls back a prepared transaction; database is the
	// one it was prepared in, since PostgreSQL only finishes a prepared
	// transaction from that database.
	Finish(ctx context.Context, database, gid string, commit bool) error
	// XactStatus reports what became of a transaction id ("" when unknown).
	XactStatus(ctx context.Context, xid string) (XactStatus, error)
}

// Conn is a superuser connection to the participant.
type Conn interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (Tag, error)
}

// Tag is the part of pgconn.CommandTag the reconciler needs.
type Tag interface{ RowsAffected() int64 }

// ConnParticipant is a Participant over one connection: it finishes
// prepared transactions of the database the connection is in.
type ConnParticipant struct{ Conn Conn }

// Prepared implements Participant.
func (p ConnParticipant) Prepared(ctx context.Context) (map[string]string, error) {
	return ListPrepared(ctx, p.Conn)
}

// Finish implements Participant.
func (p ConnParticipant) Finish(ctx context.Context, _, gid string, commit bool) error {
	return Finish(ctx, p.Conn, gid, commit)
}

// XactStatus implements Participant.
func (p ConnParticipant) XactStatus(ctx context.Context, xid string) (XactStatus, error) {
	return QueryXactStatus(ctx, p.Conn, xid)
}

// ListPrepared reads pg_prepared_xacts for router-coordinated gids.
func ListPrepared(ctx context.Context, conn Conn) (map[string]string, error) {
	rows, err := conn.Query(ctx, `SELECT gid, database FROM pg_prepared_xacts WHERE gid LIKE $1 ORDER BY prepared`, GIDPrefix+"%")
	if err != nil {
		return nil, fmt.Errorf("prepared transactions: %w", err)
	}
	out := map[string]string{}
	for rows.Next() {
		var gid, db string
		if err := rows.Scan(&gid, &db); err != nil {
			return nil, fmt.Errorf("prepared transactions: %w", err)
		}
		out[gid] = db
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("prepared transactions: %w", err)
	}
	return out, nil
}

// Finish runs COMMIT PREPARED or ROLLBACK PREPARED on conn.
func Finish(ctx context.Context, conn Conn, gid string, commit bool) error {
	verb := "ROLLBACK PREPARED"
	if commit {
		verb = "COMMIT PREPARED"
	}
	if _, err := conn.Exec(ctx, verb+" "+quoteLiteral(gid)); err != nil {
		return fmt.Errorf("%s %s: %w", strings.ToLower(verb), gid, err)
	}
	return nil
}

// Outcome counts one reconciliation on one participant.
type Outcome struct {
	Committed  int
	RolledBack int
	// Contradictions lists the gids the participant cannot honour.
	Contradictions []string
	// Unverifiable lists commit-decided gids whose outcome the participant
	// can neither confirm nor deny (see Unverifiable).
	Unverifiable []string
	// Unreadable lists gids whose recorded state this build does not
	// recognise; they are left prepared (see Unreadable).
	Unreadable []string
}

// Reconcile applies the decision log to participant shard p. Every
// router-coordinated prepared transaction on it is finished: by the log's
// decision when there is one, rolled back otherwise -- except one whose
// state this build cannot read, which is left prepared. Commit-decided
// transactions the participant does not hold prepared must be committed
// (by their recorded transaction id); anything else is a contradiction. It
// never returns early on a contradiction, so the outcome is complete.
func Reconcile(ctx context.Context, p Participant, shard int32, decisions []Decision) (Outcome, error) {
	var out Outcome
	prepared, err := p.Prepared(ctx)
	if err != nil {
		return out, err
	}
	seen := map[string]bool{}
	for _, d := range decisions {
		if !d.Involves(shard) {
			continue
		}
		seen[d.GID] = true
		db, held := prepared[d.GID]
		var status XactStatus
		if !held && d.State == StateCommit {
			if status, err = p.XactStatus(ctx, d.XID(shard)); err != nil {
				return out, err
			}
		}
		if err := apply(ctx, p, db, d.GID, Decide(d.State, held, status), &out); err != nil {
			return out, err
		}
	}
	gids := slices.Sorted(maps.Keys(prepared))
	for _, g := range gids {
		if seen[g] {
			continue
		}
		if err := apply(ctx, p, prepared[g], g, Decide(StateNone, true, ""), &out); err != nil {
			return out, err
		}
	}
	return out, nil
}

func apply(ctx context.Context, p Participant, db, gid string, a Action, out *Outcome) error {
	switch a {
	case Commit:
		if err := p.Finish(ctx, db, gid, true); err != nil {
			return err
		}
		out.Committed++
	case Rollback:
		if err := p.Finish(ctx, db, gid, false); err != nil {
			return err
		}
		out.RolledBack++
	case Contradiction:
		out.Contradictions = append(out.Contradictions, gid)
	case Unverifiable:
		out.Unverifiable = append(out.Unverifiable, gid)
	case Unreadable:
		out.Unreadable = append(out.Unreadable, gid)
	}
	return nil
}

// QueryXactStatus asks conn what became of xid; an unknown, future or
// frozen id yields StatusUnavailable.
func QueryXactStatus(ctx context.Context, conn Conn, xid string) (XactStatus, error) {
	if xid == "" {
		return StatusUnavailable, nil
	}
	rows, err := conn.Query(ctx, `SELECT coalesce(pg_xact_status($1::xid8), '')`, xid)
	var s string
	if err == nil {
		s, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	}
	switch {
	case isFutureXID(err):
		return StatusUnavailable, nil
	case err != nil:
		return "", fmt.Errorf("pg_xact_status(%s): %w", xid, err)
	}
	return XactStatus(s), nil
}

func isFutureXID(err error) bool {
	return err != nil && strings.Contains(err.Error(), "is in the future")
}

// Contradictions collects the contradictions and unverifiable outcomes of
// several participants as one error, nil when there are none.
func Contradictions(byShard map[int32]Outcome) error {
	shards := slices.Sorted(maps.Keys(byShard))
	var parts []string
	for _, shard := range shards {
		for _, gid := range byShard[shard].Contradictions {
			parts = append(parts, fmt.Sprintf("shard %d: %s is decided commit but is neither prepared nor committed", shard, gid))
		}
		for _, gid := range byShard[shard].Unverifiable {
			parts = append(parts, fmt.Sprintf("shard %d: %s is decided commit and not prepared, and its transaction id's status is unavailable (frozen, unrecorded or in the future): the commit cannot be verified", shard, gid))
		}
		for _, gid := range byShard[shard].Unreadable {
			parts = append(parts, fmt.Sprintf("shard %d: %s records a decision this build does not recognise, so it is left prepared", shard, gid))
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return errors.New("two-phase reconciliation contradictions: " + strings.Join(parts, "; "))
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
