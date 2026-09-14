package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Cutover stages of a reshard workflow, after the copy caught up.
const (
	// StageAwaitingSwitch holds until the switch gate opens: lag under
	// threshold, every table ready, no stalled subscription, and no
	// pauseBefore=switchWrites without a proceed.
	StageAwaitingSwitch = "awaiting_switch_writes"
	// StageSwitching runs the write switch steps; status.cutover.step
	// records the one in flight so a restart resumes there.
	StageSwitching = "switching"
	// StageSwitched is the new map serving, the old groups kept for
	// retireOldGroupsAfter with reverse replication flowing.
	StageSwitched = "switched"
	// StageRollingBack undoes a switched run: the serving map returns to
	// the sources once reverse replication caught up.
	StageRollingBack = "rolling_back"
	// StageRolledBack ends a rolled-back workflow (state cancelled).
	StageRolledBack = "rolled_back"
	// StageCompleting drops the replication objects of the run.
	StageCompleting = "completing"
	// StageCompleted ends the workflow (state completed).
	StageCompleted = "completed"
	// StageFailed ends the workflow (state failed).
	StageFailed = "failed"
)

// Switch steps, in order. Every step is idempotent against the catalogs of
// the shards and the catalog database; a crash after a step and before its
// record only repeats it.
const (
	StepFence     = "fence"
	StepDrain     = "drain"
	StepSweep     = "sweep"
	StepPositions = "positions"
	StepCatchUp   = "catch_up"
	StepVerify    = "verify"
	StepSequences = "sequences"
	StepReverse   = "reverse"
	StepQuiesce   = "quiesce"
	StepJournal   = "journal"
	StepFlip      = "flip"
	StepSwap      = "swap_replication"
	StepRelease   = "release"
)

var switchSteps = []string{StepFence, StepDrain, StepSweep, StepPositions, StepCatchUp, StepVerify, StepReverse, StepQuiesce, StepSequences, StepJournal, StepFlip, StepSwap, StepRelease}

// Pause points of spec.resharding.pauseBefore, mirrored into the workflow
// spec by the operator together with the proceed list.
const (
	PauseSwitchWrites = "switchWrites"
	PauseComplete     = "complete"
)

// Cutover limits.
const (
	// DefaultCutoverTimeout bounds the fence: a switch that has not reached
	// the journal by then is undone and tried again.
	DefaultCutoverTimeout = 60 * time.Second
	// DefaultCutoverAttempts is how many undone switches fail the workflow.
	DefaultCutoverAttempts = 3
	// DefaultRetireAfter is how long the old groups stay after the switch.
	DefaultRetireAfter = 24 * time.Hour
)

// cutoverState is the cutover record under workflows.status->'cutover'.
type cutoverState struct {
	SourceSet  string           `json:"source_set,omitempty"`
	Step       string           `json:"step,omitempty"`
	Attempts   int              `json:"attempts,omitempty"`
	FencedAt   *time.Time       `json:"fenced_at,omitempty"`
	Positions  map[string]int64 `json:"positions,omitempty"`
	JournalID  string           `json:"journal_id,omitempty"`
	Verify     *VerifyReport    `json:"verify,omitempty"`
	FlippedAt  *time.Time       `json:"flipped_at,omitempty"`
	ReleasedAt *time.Time       `json:"released_at,omitempty"`
	// PauseMS is the router-visible write pause: fence raised to new map
	// published. FenceMS is fence raised to fence released.
	PauseMS    int64      `json:"pause_ms,omitempty"`
	FenceMS    int64      `json:"fence_ms,omitempty"`
	SwitchedAt *time.Time `json:"switched_at,omitempty"`
	// DisablingAt records that THIS run is the one turning the forward
	// subscriptions off, so a resume can tell its own half-finished swap
	// from a subscription an operator disabled by hand. Saved before the
	// disable, so a crash between the two leaves the catch-up check
	// running rather than skipped.
	DisablingAt *time.Time `json:"disabling_at,omitempty"`
	Gate        string     `json:"gate,omitempty"`
	// Pause names the configured pause holding the workflow
	// (PauseSwitchWrites or PauseComplete) and PausedAt when it began.
	// A configured pause leaves the workflow running -- only an operator
	// pauses the workflow itself -- so its top-level state says nothing
	// about it, and every pass rewrites updated_at, so the age of one
	// cannot be read from the row either.
	Pause    string     `json:"pause,omitempty"`
	PausedAt *time.Time `json:"paused_at,omitempty"`
	// StepSince is when the current step was entered. After the journal
	// every error is retried without a timeout or attempt limit, and each
	// pass refreshes updated_at, so a step that has failed for hours is
	// indistinguishable from a healthy running workflow without this.
	StepSince *time.Time `json:"step_since,omitempty"`
	// StepRetries counts the passes the current step has failed. Before
	// the journal a step gives up; after it there is nothing to do but
	// retry, so this and StepSince are the only measure of a step that is
	// getting nowhere.
	StepRetries int `json:"step_retries,omitempty"`
	// Schema fingerprints both sets at the switch, keyed set/shard/database.
	// Logical replication carries no DDL, so a rollback has to prove the
	// sources were not left structurally behind while they were idle.
	Schema map[string]string `json:"schema,omitempty"`
	Aborts []string          `json:"aborts,omitempty"`
}

// stampStep records when the current step was entered.
func (s *cutoverState) stampStep(now time.Time) {
	t := now
	s.StepSince, s.StepRetries = &t, 0
}

// stalled reports whether the current step has been going nowhere long
// enough to be worth an operator's attention rather than a line in a log
// nobody reads.
func (s *cutoverState) stalled(now time.Time) bool {
	return s.StepSince != nil && now.Sub(*s.StepSince) >= stalledAfter
}

// stalledFor renders how long the current step has been retrying, once that
// is long enough to be worth saying. A post-journal step has no timeout and
// no attempt limit, so the age is the only signal that it is not progressing.
func (s *cutoverState) stalledFor(now time.Time) string {
	if s.StepSince == nil {
		return ""
	}
	d := now.Sub(*s.StepSince)
	if d < stalledAfter {
		return ""
	}
	return fmt.Sprintf(" (step %s has not advanced for %s)", s.Step, d.Round(time.Second))
}

// stalledAfter is how long a single cutover step may retry before its age is
// reported in the status.
const stalledAfter = 2 * time.Minute

// cutoverSpec is what the operator mirrors into the workflow spec.
type cutoverSpec struct {
	SourceSet          string   `json:"source_set"`
	PauseBefore        string   `json:"pause_before"`
	Proceed            []string `json:"proceed"`
	RetireAfterSeconds int64    `json:"retire_after_seconds"`
	// Rollback asks a switched run to return serving to the sources while
	// the retirement window keeps them current over reverse replication.
	Rollback bool `json:"rollback"`
}

func (s cutoverSpec) paused(point string) bool {
	return s.PauseBefore == point && !slices.Contains(s.Proceed, point)
}

func (s cutoverSpec) retireAfter() time.Duration {
	if s.RetireAfterSeconds > 0 {
		return time.Duration(s.RetireAfterSeconds) * time.Second
	}
	return DefaultRetireAfter
}

// VerifyReport is the VDiff-lite result: per table and target, the row
// count and row-hash sum the sources predict against what the target holds.
type VerifyReport struct {
	Tables     int      `json:"tables"`
	Rows       int64    `json:"rows"`
	Mismatches []string `json:"mismatches,omitempty"`
	// Tagged with the name it already had. Without a tag this field
	// serialised as "CheckedAt" among snake_case siblings, and the admin
	// had to mirror that accident to read it -- an odd-looking tag with
	// nothing to say why. Rows exist carrying the key, so it is kept and
	// made deliberate rather than changed; renaming it belongs with the
	// shared workflow model in PGS-331, which is also what would stop the
	// next field doing this.
	CheckedAt time.Time `json:"CheckedAt"`
}

// cutoverOps are the side effects of the switch, one per step. The
// PostgreSQL implementation lives in cutoverpg.go; tests drive the state
// machine with fakes.
type cutoverOps interface {
	// GateOpen reports whether the copy is ready to switch; the string
	// explains a closed gate.
	GateOpen(ctx context.Context) (bool, string, error)
	// Fence raises the range fence on the sources.
	Fence(ctx context.Context) error
	// Drain resolves in-doubt prepared transactions on the sources and
	// lists the ones that remain.
	Drain(ctx context.Context) ([]string, error)
	// Sweep takes and releases a SHARE lock on every sharded table of
	// every source so no write is in flight; errRetry means a lock timed
	// out.
	Sweep(ctx context.Context) error
	// Positions reads pg_current_wal_lsn() of every source.
	Positions(ctx context.Context) (map[string]int64, error)
	// CaughtUp reports whether every forward subscription passed the
	// source positions.
	CaughtUp(ctx context.Context, positions map[string]int64) (bool, string, error)
	// Verify compares sources and targets.
	Verify(ctx context.Context) (VerifyReport, error)
	// Sequences carries every user-database sequence position from the
	// sources to the targets inside the fence.
	Sequences(ctx context.Context) error
	// Reverse creates the reverse publications and disabled subscriptions.
	Reverse(ctx context.Context) error
	// SchemaFingerprints hashes every database on both sets, keyed
	// set/shard/database.
	SchemaFingerprints(ctx context.Context) (map[string]string, error)
	// Journal writes the journal rows (idempotent by id).
	Journal(ctx context.Context, id string) error
	// Flip publishes the new map in one catalog transaction.
	Flip(ctx context.Context, journalID string) error
	// Swap disables forward and enables reverse replication.
	// PauseSources stops the sources accepting new writing transactions,
	// and lets them again; lifting touches only this workflow's claim.
	// From quiesce to the swap disabling forward replication nothing may
	// commit on a source: before the flip such a write lands on a target
	// that may hold a newer one, and after DisableForward it is left on a
	// source nothing replicates from any more.
	//
	// The pause alone does not buy that, which is what DrainSources is
	// for: default_transaction_read_only is read when a transaction
	// STARTS, so one already open commits straight through it.
	PauseSources(ctx context.Context, pause bool) error
	// SourcesPaused reports whether every source still refuses new writing
	// transactions under this workflow's claim.
	SourcesPaused(ctx context.Context) (bool, error)
	// DrainSources waits for the writing transactions that were already
	// open when the pause went up. Until it returns, "paused" means only
	// that no NEW writer can begin.
	DrainSources(ctx context.Context) error
	// DisableForward stops the forward subscriptions; EnableReverse starts
	// the reverse ones. They were one step, which meant the sources had to
	// be writable again before the reverse apply could run and could not
	// stay paused across the disable.
	DisableForward(ctx context.Context) error
	EnableReverse(ctx context.Context) error
	// ForwardDisabled reports whether any forward subscription has already
	// been disabled, which is how a resume tells that it is re-entering
	// StepSwap past its point of no return rather than starting it.
	ForwardDisabled(ctx context.Context) (bool, error)
	// Release drops the range fence.
	Release(ctx context.Context) error
	// Complete drops every replication object of the run.
	Complete(ctx context.Context) error
	// Rollback returns serving to the sources: fence the targets, wait for
	// reverse replication (errRetry while behind), carry the sequences
	// back and flip the serving map to the source set. When it succeeds the
	// targets are left paused, and nothing lifts that: a router still
	// holding the snapshot from before the flip back can commit on a
	// target, and the only thing that would carry the row to the source is
	// the reverse subscription Complete drops. Complete then turns the
	// pause into the retired set's permanent one. The forward path keeps
	// its sources paused until DisableForward for the same reason.
	Rollback(ctx context.Context) error
	// DropJournal removes the journal rows this run wrote on its sources.
	DropJournal(ctx context.Context, id string) error
}

// sourceRetiredError marks a switch that can never proceed, as against one
// that cannot proceed yet: its source set is no longer serving, and nothing
// returns a retired set to serving. Retrying such a step forever holds the
// run's replication slots, and with them the sources' WAL.
type sourceRetiredError struct{ err error }

func (e *sourceRetiredError) Error() string { return e.err.Error() }
func (e *sourceRetiredError) Unwrap() error { return e.err }

func sourceRetired(format string, args ...any) error {
	return &sourceRetiredError{fmt.Errorf(format, args...)}
}

func isSourceRetired(err error) bool {
	var e *sourceRetiredError
	return errors.As(err, &e)
}

func isFatal(err error) bool {
	var f *fatalError
	return errors.As(err, &f)
}

// errRetry marks a step that should run again next pass.
var errRetry = errors.New("retry next pass")

func retryf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errRetry, fmt.Sprintf(format, args...))
}

func (c *Copier) cutoverTimeout() time.Duration {
	if c.CutoverTimeout > 0 {
		return c.CutoverTimeout
	}
	return DefaultCutoverTimeout
}

func (c *Copier) cutoverAttempts() int {
	if c.CutoverAttempts > 0 {
		return c.CutoverAttempts
	}
	return DefaultCutoverAttempts
}

// cutover advances one workflow through the cutover stages by one pass.
// It reports whether the stage changed.
func (c *Copier) cutover(ctx context.Context, wf *copyWorkflow, ops cutoverOps) (bool, error) {
	switch wf.stage {
	case StageCatchUpDone:
		if wf.copy.Paused {
			// Throttle pauses by disabling the subscriptions, and it runs
			// only for the copy stages. awaiting_switch_writes is a cutover
			// stage, so advancing while paused is a one-way door: nothing
			// re-evaluates the watermark, nothing re-enables what was
			// disabled, and the switch gate then rejects the very
			// subscriptions the throttle turned off. Holding here instead
			// leaves the workflow where the throttle can still see it, and
			// a transient lag spike resolves itself.
			return false, c.saveCutover(ctx, wf, "caught up, but the source standby lag is over the watermark; holding before the switch gate")
		}
		wf.stage = StageAwaitingSwitch
		return true, c.saveCutover(ctx, wf, "copy caught up; waiting for the switch gate")
	case StageAwaitingSwitch:
		return c.gate(ctx, wf, ops)
	case StageSwitching:
		return c.switchWrites(ctx, wf, ops)
	case StageSwitched:
		if wf.spec.Rollback {
			wf.stage = StageRollingBack
			return true, c.saveCutover(ctx, wf, "rollback requested: returning serving to "+wf.sourceSet())
		}
		return c.retire(ctx, wf)
	case StageRollingBack:
		return c.rollback(ctx, wf, ops)
	case StageCompleting:
		if err := ops.Complete(ctx); err != nil {
			return false, err
		}
		wf.stage = StageCompleted
		return true, c.finishCutover(ctx, wf, StateCompleted, "reshard completed: reverse replication dropped, old shard set retired")
	}
	return false, nil
}

func (c *Copier) gate(ctx context.Context, wf *copyWorkflow, ops cutoverOps) (bool, error) {
	open, why, err := ops.GateOpen(ctx)
	if err != nil {
		return false, err
	}
	if !open {
		wf.cutover.Gate = why
		return false, c.saveCutover(ctx, wf, "switch gate closed: "+why)
	}
	if wf.spec.paused(PauseSwitchWrites) {
		wf.cutover.Gate = "paused before switchWrites"
		c.holdAt(wf, PauseSwitchWrites)
		return false, c.saveCutover(ctx, wf, "paused before switchWrites: waiting for proceed")
	}
	wf.cutover.Gate = ""
	c.released(wf)
	wf.cutover.Step = StepFence
	wf.stage = StageSwitching
	return true, c.saveCutover(ctx, wf, "switch gate open: switching writes")
}

// rollback undoes a switched run: once reverse replication caught up the
// serving map returns to the sources, every replication object of the run
// is dropped and the workflow ends cancelled. The target set stays retired
// for the operator to tear down.
func (c *Copier) rollback(ctx context.Context, wf *copyWorkflow, ops cutoverOps) (bool, error) {
	if err := ops.Rollback(ctx); err != nil {
		if errors.Is(err, errRetry) {
			return false, c.saveCutover(ctx, wf, "rolling back: "+err.Error())
		}
		return false, err
	}
	if err := ops.Complete(ctx); err != nil {
		return false, err
	}
	wf.stage = StageRolledBack
	return true, c.finishCutover(ctx, wf, StateCancelled, "rolled back: serving returned to "+wf.sourceSet())
}

// switchWrites runs the switch steps from the recorded one. Before the
// journal a step that cannot finish within the cutover timeout undoes the
// fence and returns to the gate; after it every error is retried.
func (c *Copier) switchWrites(ctx context.Context, wf *copyWorkflow, ops cutoverOps) (bool, error) {
	if wf.cutover.Step == "" {
		wf.cutover.Step = StepFence
		wf.cutover.stampStep(c.now())
	}
	for {
		step := wf.cutover.Step
		if step == "" {
			return true, nil
		}
		waiting, err := c.runStep(ctx, wf, ops, step)
		if err != nil {
			if isSourceRetired(err) {
				return c.abandonSwitch(ctx, wf, ops, err.Error())
			}
			if !beforeJournal(step) {
				// Nothing undoes a step past the journal, so the only
				// honest report is how long it has been failing and how
				// often. Saving it here also means a stalled switch reads
				// as one in the catalog, not as a workflow updated a
				// moment ago.
				wf.cutover.StepRetries++
				msg := fmt.Sprintf("switching: step %s failed %d time(s): %v", step, wf.cutover.StepRetries, err)
				if serr := c.saveCutover(ctx, wf, msg+wf.cutover.stalledFor(c.now())); serr != nil {
					return false, serr
				}
				return false, err
			}
			if !errors.Is(err, errRetry) {
				// A fence must never outlive the switch that raised it --
				// unless the switch is no longer ours, in which case the
				// replica that owns it now is behind that fence.
				if isFatal(err) {
					if oerr := holdClaim(ctx, c.Pool, wf.id, wf.owner); oerr != nil {
						return false, oerr
					}
					if perr := ops.PauseSources(ctx, false); perr != nil {
						return false, perr
					}
					if rerr := ops.Release(ctx); rerr != nil {
						return false, rerr
					}
					return false, err
				}
				if c.mayAbort(wf) && c.now().Sub(*wf.cutover.FencedAt) > c.cutoverTimeout() {
					return c.abortSwitch(ctx, wf, ops, fmt.Sprintf("step %s did not finish within %s: %s", step, c.cutoverTimeout(), err))
				}
				return false, err
			}
			waiting = true
		}
		if waiting {
			if c.mayAbort(wf) && beforeJournal(step) && c.now().Sub(*wf.cutover.FencedAt) > c.cutoverTimeout() {
				// The step knows what it is still waiting for; without it an
				// abort says only that the deadline passed, which does not
				// distinguish replication that is still catching up from a
				// subscription whose slot has gone.
				why := fmt.Sprintf("step %s did not finish within %s", step, c.cutoverTimeout())
				if err != nil {
					why += ": " + err.Error()
				}
				return c.abortSwitch(ctx, wf, ops, why)
			}
			msg := fmt.Sprintf("switching: waiting at step %s", step)
			if err != nil {
				msg += ": " + strings.TrimPrefix(err.Error(), errRetry.Error()+": ")
			}
			msg += wf.cutover.stalledFor(c.now())
			return false, c.saveCutover(ctx, wf, msg)
		}
		// The deadline was only ever consulted when a step failed or
		// reported waiting, so a step that SUCCEEDED slowly held the fence
		// for as long as it liked. verify is the one that matters: it
		// scans every table with writes already fenced, so on a real table
		// the hold grows with the data and nothing here stopped it.
		if c.mayAbort(wf) && beforeJournal(step) && c.now().Sub(*wf.cutover.FencedAt) > c.cutoverTimeout() {
			return c.abortSwitch(ctx, wf, ops, fmt.Sprintf("step %s finished but the fence had been held %s, over the %s limit",
				step, c.now().Sub(*wf.cutover.FencedAt).Round(time.Millisecond), c.cutoverTimeout()))
		}
		next := nextStep(step)
		wf.cutover.Step = next
		wf.cutover.stampStep(c.now())
		if next == "" {
			now := c.now()
			wf.cutover.SwitchedAt = &now
			wf.stage = StageSwitched
			return true, c.saveCutover(ctx, wf, fmt.Sprintf("writes switched to %s: pause %dms, fence %dms; old groups retire after %s",
				wf.set, wf.cutover.PauseMS, wf.cutover.FenceMS, wf.spec.retireAfter()))
		}
		if err := c.saveCutover(ctx, wf, "switching: step "+step+" done"); err != nil {
			return false, err
		}
	}
}

// runStep runs one step; waiting means the step must run again next pass.
func (c *Copier) runStep(ctx context.Context, wf *copyWorkflow, ops cutoverOps, step string) (waiting bool, err error) {
	switch step {
	case StepFence:
		if err := ops.Fence(ctx); err != nil {
			return false, err
		}
		if wf.cutover.FencedAt == nil {
			now := c.now()
			wf.cutover.FencedAt = &now
		}
	case StepDrain:
		remaining, err := ops.Drain(ctx)
		if err != nil {
			return false, err
		}
		if len(remaining) > 0 {
			return true, retryf("prepared transactions %v", remaining)
		}
	case StepSweep:
		if err := ops.Sweep(ctx); err != nil {
			return false, err
		}
	case StepPositions:
		pos, err := ops.Positions(ctx)
		if err != nil {
			return false, err
		}
		wf.cutover.Positions = pos
	case StepCatchUp:
		// Every forward subscription has reached the recorded positions,
		// and that is the whole step.
		//
		// It used to demand, on top of that, that re-reading the sources
		// gave back the SAME positions. But those are pg_current_wal_lsn,
		// which a checkpoint or an autovacuum moves with no user write
		// behind it -- measured on an idle PostgreSQL 18 with no writes at
		// all -- so the step was a coin flip against background WAL, and
		// it lost about one cutover in eleven. StepFlip learned the same
		// thing about the same question and stopped asking it.
		//
		// Not asking is what makes the step terminate: the recorded
		// positions are a FIXED boundary, so a source that keeps writing
		// cannot run away from the targets. And nothing is lost by it. A
		// write landing after the positions were read is still carried by
		// the forward subscriptions, which stay enabled until StepSwap --
		// and StepQuiesce pauses the sources, drains the writers already
		// open, re-reads the positions and re-checks them before the
		// journal. That is where a straggler is caught.
		ok, why, err := ops.CaughtUp(ctx, wf.cutover.Positions)
		if err != nil {
			return false, err
		}
		if !ok {
			return true, retryf("%s", why)
		}
	case StepVerify:
		report, err := ops.Verify(ctx)
		if err != nil {
			return false, err
		}
		previous := wf.cutover.Verify
		wf.cutover.Verify = &report
		if len(report.Mismatches) > 0 {
			// A source that moved while the digests were being taken makes
			// the target look ahead of it: the source is read first, and a
			// write landing between that read and the target's is already
			// applied by the time the target is read. Seen in CI as a
			// target holding exactly one batch more than the sources
			// predicted. That is a race in the measurement, not a target
			// that disagrees with its source, and it must not abandon a
			// switch.
			//
			// Neither of the two questions about POSITIONS tells those
			// apart. "Did they move" is answered yes by background WAL
			// with nothing behind it, so every mismatch read as a race and
			// a real one could never be reported. "Have the targets caught
			// up to where the sources are now" is answered yes in exactly
			// the racing case -- the write is already applied, which is
			// why the target looked ahead -- so it calls the race real.
			//
			// Ask the digests again instead. A race is gone or different
			// next pass, because the numbers it produced described one
			// instant; a target that genuinely disagrees produces the same
			// count, sum and xor every time.
			if previous == nil || !slices.Equal(previous.Mismatches, report.Mismatches) {
				return true, retryf("digests disagree, asking again to tell a moving source from a real disagreement (%s)",
					strings.Join(report.Mismatches, "; "))
			}
			return false, fatal("verification failed, the same digests twice: %s", strings.Join(report.Mismatches, "; "))
		}
	case StepQuiesce:
		// The sources stop taking writing transactions here, before the
		// journal, and stay stopped until StepSwap has disabled forward
		// replication. The fence lets a transaction that was already open
		// go on writing, and nothing else stops a router or pooler that has
		// not reloaded; a write like that committing on a source after the
		// flip is carried to a target that is already serving, over
		// whatever the target committed meanwhile -- an older write over a
		// newer one, or a duplicate key that stalls the apply (PGS-750). On
		// one PostgreSQL the target's statement would have waited for it.
		//
		// Before the journal, so a transaction that will not end cannot
		// hold the fence for good: waiting here is bounded by the cutover
		// timeout, which undoes the switch and lifts the pause. The pause
		// is lifted whenever the step has to wait, too, so a quiet moment
		// between attempts lets new writers through rather than holding
		// them behind a drain that may not converge. Not right after the fence, either: an
		// attempt that paused milliseconds after it failed client
		// transfers with 25006 (PGS-784), most likely from routers that had
		// not yet seen the fence; by now it has stood through verify.
		if waiting, err := quiesceSources(ctx, ops); waiting || err != nil {
			return waiting, err
		}
	case StepSequences:
		if wf.cutover.Schema == nil {
			// A switch saved at this step by a controller that ran reverse
			// after the sequences, resumed here: reverse never ran, and
			// going on would complete a switch with no reverse replication
			// and so no rollback. Back to it, and quiesce with it.
			wf.cutover.Step = StepReverse
			return true, retryf("reverse replication was never created; running reverse and quiesce first")
		}
		if err := ops.Sequences(ctx); err != nil {
			return false, err
		}
	case StepReverse:
		if err := ops.Reverse(ctx); err != nil {
			return false, err
		}
		// Taken here, inside the fence and while the DDL locks still
		// stand, so both sets are quiet and the hashes describe the
		// structure a rollback would be returning to.
		fps, err := ops.SchemaFingerprints(ctx)
		if err != nil {
			return false, err
		}
		wf.cutover.Schema = fps
	case StepJournal:
		if wf.cutover.JournalID == "" {
			id, err := c.store().NewJournalID(ctx)
			if err != nil {
				return false, err
			}
			wf.cutover.JournalID = id
			if err := c.saveCutover(ctx, wf, "switching: journal id allocated"); err != nil {
				return false, err
			}
		}
		if err := ops.Journal(ctx, wf.cutover.JournalID); err != nil {
			return false, err
		}
	case StepFlip:
		// The pause quiesce raised has to have stood since, or the drain it
		// did proves nothing about now. It does not when a controller
		// upgraded mid-switch resumes past a quiesce it never ran, or when
		// something lifted the pause by hand. Then this is quiesce again,
		// after the journal: it cannot abort, and it waits for as long as a
		// transaction begun before the pause stays open -- but it lifts the
		// pause while it waits, which is safe because the targets do not
		// serve yet, so each attempt measures from a pause of its own.
		stood, err := ops.SourcesPaused(ctx)
		if err != nil {
			return false, err
		}
		if !stood {
			if waiting, err := quiesceSources(ctx, ops); waiting || err != nil {
				return waiting, err
			}
		} else {
			// The pause stood, so the transactions left are read-only ones
			// begun under it. Only one that has written -- a client that
			// overrode the pause for its own transaction -- is waited for.
			if err := ops.DrainSources(ctx); err != nil {
				return true, retryf("%s", err)
			}
		}
		pos, err := ops.Positions(ctx)
		if err != nil {
			return false, err
		}
		ok, why, err := ops.CaughtUp(ctx, pos)
		if err != nil {
			return false, err
		}
		if !ok {
			return true, retryf("%s", why)
		}
		if err := ops.Flip(ctx, wf.cutover.JournalID); err != nil {
			return false, err
		}
		if wf.cutover.FlippedAt == nil {
			now := c.now()
			wf.cutover.FlippedAt = &now
			if wf.cutover.FencedAt != nil {
				wf.cutover.PauseMS = now.Sub(*wf.cutover.FencedAt).Milliseconds()
			}
		}
	case StepSwap:
		// The forward subscriptions stay enabled until here; before they
		// are disabled the targets must have applied everything the (now
		// fenced and retired) sources wrote, or a last write acked on a
		// source would be dropped.
		//
		// Checking and then disabling is not enough on its own: the check
		// speaks for the position it sampled, and a router that has not
		// yet reloaded its snapshot can still commit to a source in the
		// gap. So the sources stop accepting writing transactions first,
		// and only start again once the forward subscriptions are off --
		// which is also why enabling the reverse ones is a separate step,
		// since their apply workers need the sources writable.
		// Everything up to DisableForward establishes that the targets
		// have all of the sources' writes. Once the forward subscriptions
		// are off that cannot be re-established: their slots'
		// confirmed_flush_lsn stops advancing while the sources' WAL keeps
		// moving for reasons no fence stops, so CaughtUp compares a
		// standing position against a moving one and reads as behind for
		// good. Re-running the whole step after a crash in the middle of
		// it retries that comparison forever and never reaches
		// EnableReverse, leaving the targets serving with no reverse
		// replication -- the rollback path -- while the workflow reports
		// itself as merely retrying.
		//
		// So the resume skips that one pair and NOTHING else. The pause and
		// the drain are re-run: a crash can land anywhere after the flip
		// raised the pause, and re-raising one that stood is free. The
		// sequence carry is re-run for the same reason, and repeating it is
		// safe because Apply takes the greater.
		//
		// Two conditions, not one. ForwardDisabled alone cannot tell our
		// own disable from an operator disabling a subscription by hand,
		// and skipping the check for that would finalize the run with
		// changes still unapplied: a loud stall turned into a silent loss.
		// The marker says we are the ones who did it.
		disabled, err := ops.ForwardDisabled(ctx)
		if err != nil {
			return false, err
		}
		resuming := disabled && wf.cutover.DisablingAt != nil
		// Raised again only when it did not stand since quiesce: raising a
		// standing pause measures from now, and the drain would then wait
		// for every read begun under it.
		stood, err := ops.SourcesPaused(ctx)
		if err != nil {
			return false, err
		}
		if !stood {
			if err := ops.PauseSources(ctx, true); err != nil {
				return false, err
			}
		}
		// The pause stops transactions that BEGIN after it. One that began
		// before commits straight through it, because
		// default_transaction_read_only is read at transaction start --
		// and the router lets a transaction opened before the fence carry
		// on writing, deliberately. Such a write lands after the positions
		// below are sampled and after the forward subscriptions are gone,
		// on a source that is about to be retired, and nothing carries it
		// anywhere: an acknowledged commit is lost when the source goes.
		//
		// A drain timing out is a long transaction, not a broken cutover,
		// so the step retries -- with the pause up, as StepFlip leaves it:
		// the sources are retired, and a source made writable between
		// attempts is one a stale router can commit on while its forward
		// subscription is being disabled.
		if err := ops.DrainSources(ctx); err != nil {
			if !stood && !resuming {
				// A pause this pass raised and could not drain is not one
				// the next pass may trust as having stood: lifted, it is
				// raised and measured afresh. Forward replication is still
				// whole, so what lands meanwhile is carried.
				return true, errors.Join(retryf("%s", err), ops.PauseSources(ctx, false))
			}
			return true, retryf("%s", err)
		}
		if !resuming {
			pos, err := ops.Positions(ctx)
			if err != nil {
				return false, err
			}
			ok, why, err := ops.CaughtUp(ctx, pos)
			if err != nil {
				return false, err
			}
			if !ok {
				return true, retryf("%s", why)
			}
		}
		// Sequence positions are not replicated. The flip carried them with
		// the sources paused; carrying them again here costs nothing, since
		// Apply takes the greater, and covers a pause that did not stand
		// between the two -- a crash, or one lifted by hand.
		if err := ops.Sequences(ctx); err != nil {
			return false, err
		}
		if wf.cutover.DisablingAt == nil {
			now := c.now()
			wf.cutover.DisablingAt = &now
			if err := c.saveCutover(ctx, wf, "switching: disabling forward replication"); err != nil {
				return false, err
			}
		}
		if err := ops.DisableForward(ctx); err != nil {
			return false, err
		}
		if err := ops.PauseSources(ctx, false); err != nil {
			return false, err
		}
		if err := ops.EnableReverse(ctx); err != nil {
			return false, err
		}
	case StepRelease:
		if err := ops.Release(ctx); err != nil {
			return false, err
		}
		if wf.cutover.ReleasedAt == nil {
			now := c.now()
			wf.cutover.ReleasedAt = &now
			if wf.cutover.FencedAt != nil {
				wf.cutover.FenceMS = now.Sub(*wf.cutover.FencedAt).Milliseconds()
			}
		}
	default:
		return false, fatal("unknown switch step %q", step)
	}
	return false, nil
}

// mayAbort reports whether the switch can still be undone. The journal is
// the point of no return: once its rows are on the sources, every consumer
// of the change stream has been told the cutover happened, and nothing
// retracts that. So the question is answered from the durable fact that a
// journal id was allocated, not from the step cursor, which a controller
// upgraded mid-switch may read against a different step order. Past that
// point every error is retried instead.
func (c *Copier) mayAbort(wf *copyWorkflow) bool {
	return wf.cutover.FencedAt != nil && wf.cutover.JournalID == ""
}

// abandonSwitch ends a switch that can never finish because its source set
// was retired underneath it, releasing everything the run holds.
//
// The journal is the point of no return because its rows tell every change
// stream consumer that the cutover happened, and nothing retracts that.
// That reasoning assumes the cutover then happens. Here it cannot: another
// workflow already flipped and retired these sources, so this run's journal
// rows point consumers at a target set that will never serve. Leaving them
// sends a consumer that has not read them yet to a dead end, so they are
// removed with the rest of what the run holds -- and a consumer that
// repositioned on them before that has to resume from the set now serving,
// which is what it would have done had these rows never existed.
//
// What is not released here is the target set: it holds a copy no one is
// serving, and tearing it down is the operator's decision, not a step in
// unwinding the switch.
func (c *Copier) abandonSwitch(ctx context.Context, wf *copyWorkflow, ops cutoverOps, reason string) (bool, error) {
	if err := ops.DropJournal(ctx, wf.cutover.JournalID); err != nil {
		return false, err
	}
	if err := ops.Release(ctx); err != nil {
		return false, err
	}
	if err := ops.Complete(ctx); err != nil {
		return false, err
	}
	wf.stage = StageFailed
	return true, c.finishCutover(ctx, wf, StateFailed, "abandoned: "+reason)
}

// abortSwitch undoes the fence before the journal and returns to the gate,
// failing the workflow once the attempts are used up.
// quiesceSources pauses the sources, waits out the writers and prepared
// transactions begun before the pause, and confirms the targets have applied
// everything the sources hold. Whenever it has to wait it lifts the pause
// again: the targets do not serve yet, so the forward subscriptions carry
// anything written meanwhile, and the next attempt measures from its own
// pause. It reports waiting, or an error, with the pause lifted.
func quiesceSources(ctx context.Context, ops cutoverOps) (bool, error) {
	if err := ops.PauseSources(ctx, true); err != nil {
		return false, errors.Join(err, ops.PauseSources(ctx, false))
	}
	lift := func(waiting bool, err error) (bool, error) {
		return waiting, errors.Join(err, ops.PauseSources(ctx, false))
	}
	if err := ops.DrainSources(ctx); err != nil {
		return lift(true, retryf("%s", err))
	}
	// A prepared transaction has no backend for the drain to see, and
	// COMMIT PREPARED runs in a read-only transaction.
	prepared, err := ops.Drain(ctx)
	if err != nil {
		return lift(false, err)
	}
	if len(prepared) > 0 {
		return lift(true, retryf("prepared transactions %v", prepared))
	}
	pos, err := ops.Positions(ctx)
	if err != nil {
		return lift(false, err)
	}
	ok, why, err := ops.CaughtUp(ctx, pos)
	if err != nil {
		return lift(false, err)
	}
	if !ok {
		return lift(true, retryf("%s", why))
	}
	return false, nil
}

func (c *Copier) abortSwitch(ctx context.Context, wf *copyWorkflow, ops cutoverOps, reason string) (bool, error) {
	if err := holdClaim(ctx, c.Pool, wf.id, wf.owner); err != nil {
		return false, err
	}
	// A switch undone after quiesce leaves its pause behind otherwise, on
	// sources that still serve. Only this workflow's claim is lifted, and
	// before the fence: routers that see the fence drop send writes at once.
	if err := ops.PauseSources(ctx, false); err != nil {
		return false, err
	}
	if err := ops.Release(ctx); err != nil {
		return false, err
	}
	wf.cutover.Attempts++
	wf.cutover.Aborts = append(wf.cutover.Aborts, fmt.Sprintf("%s: %s", c.now().Format(time.RFC3339), reason))
	wf.cutover.Step, wf.cutover.FencedAt, wf.cutover.Positions = "", nil, nil
	if wf.cutover.Attempts >= c.cutoverAttempts() {
		return false, fatal("switch aborted %d times, last: %s", wf.cutover.Attempts, reason)
	}
	wf.stage = StageAwaitingSwitch
	return true, c.saveCutover(ctx, wf, "switch undone ("+reason+"); back at the gate")
}

// retire holds the switched workflow until the retirement window passed and
// the complete pause (if any) was released.
func (c *Copier) retire(ctx context.Context, wf *copyWorkflow) (bool, error) {
	if wf.cutover.SwitchedAt != nil {
		if remaining := wf.cutover.SwitchedAt.Add(wf.spec.retireAfter()).Sub(c.now()); remaining > 0 {
			return false, c.saveCutover(ctx, wf, fmt.Sprintf("switched: old groups retire in %s", remaining.Round(time.Second)))
		}
	}
	if wf.spec.paused(PauseComplete) {
		c.holdAt(wf, PauseComplete)
		return false, c.saveCutover(ctx, wf, "paused before complete: waiting for proceed")
	}
	c.released(wf)
	wf.stage = StageCompleting
	return true, c.saveCutover(ctx, wf, "completing: dropping reverse replication")
}

// holdAt records which configured pause is holding the workflow, and since
// when. The timestamp is written once: a pause that keeps being observed is
// the same pause, and refreshing it would report every one as new.
func (c *Copier) holdAt(wf *copyWorkflow, point string) {
	if wf.cutover.Pause == point {
		return
	}
	at := c.now()
	wf.cutover.Pause, wf.cutover.PausedAt = point, &at
}

// released clears the pause once the workflow moves past it.
func (c *Copier) released(wf *copyWorkflow) {
	wf.cutover.Pause, wf.cutover.PausedAt = "", nil
}

func beforeJournal(step string) bool {
	return slices.Index(switchSteps, step) < slices.Index(switchSteps, StepJournal)
}

func nextStep(step string) string {
	i := slices.Index(switchSteps, step)
	if i < 0 || i+1 >= len(switchSteps) {
		return ""
	}
	return switchSteps[i+1]
}

// cutoverStore persists the cutover record; the catalog database in
// production, memory in tests.
type cutoverStore interface {
	Save(ctx context.Context, wf *copyWorkflow, message string) error
	Finish(ctx context.Context, wf *copyWorkflow, state, message string) error
	NewJournalID(ctx context.Context) (string, error)
}

type poolCutoverStore struct{ c *Copier }

func (s poolCutoverStore) Save(ctx context.Context, wf *copyWorkflow, message string) error {
	patch := map[string]any{"stage": wf.stage, "cutover": wf.cutover, "message": message}
	return ownedExec(ctx, s.c.Pool, wf.owner,
		`UPDATE pgshard.workflows SET status = status || $2::jsonb, updated_at = now()
		 WHERE id = $1::uuid AND ($3::text IS NULL OR (owner = $3 AND state = $4))`,
		wf.id, mustJSON(patch), nullIfEmpty(wf.owner), wf.fence)
}

func (s poolCutoverStore) Finish(ctx context.Context, wf *copyWorkflow, state, message string) error {
	if err := ownedExec(ctx, s.c.Pool, wf.owner,
		`UPDATE pgshard.workflows SET state = $2, status = status || $3::jsonb, updated_at = now()
		 WHERE id = $1::uuid AND ($4::text IS NULL OR (owner = $4 AND state = $5))`,
		wf.id, state, mustJSON(map[string]any{"stage": wf.stage, "cutover": wf.cutover, "message": message}), nullIfEmpty(wf.owner), wf.fence); err != nil {
		return err
	}
	wf.fence = state
	return nil
}

func (s poolCutoverStore) NewJournalID(ctx context.Context) (string, error) {
	var id string
	err := s.c.Pool.QueryRow(ctx, `SELECT gen_random_uuid()::text`).Scan(&id)
	return id, err
}

func (c *Copier) store() cutoverStore {
	if c.cutoverStore != nil {
		return c.cutoverStore
	}
	return poolCutoverStore{c}
}

func (c *Copier) saveCutover(ctx context.Context, wf *copyWorkflow, message string) error {
	return c.store().Save(ctx, wf, message)
}

func (c *Copier) finishCutover(ctx context.Context, wf *copyWorkflow, state, message string) error {
	return c.store().Finish(ctx, wf, state, message)
}
