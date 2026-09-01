package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
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
	StepJournal   = "journal"
	StepFlip      = "flip"
	StepSwap      = "swap_replication"
	StepRelease   = "release"
)

var switchSteps = []string{StepFence, StepDrain, StepSweep, StepPositions, StepCatchUp, StepVerify, StepSequences, StepReverse, StepJournal, StepFlip, StepSwap, StepRelease}

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
	// maxSeqRecarries bounds how often a moving source sends the flip back
	// to re-carry sequences before it proceeds anyway.
	maxSeqRecarries = 2
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
	// Recarries counts how many times the flip has sent the switch back to
	// re-carry sequences because the sources moved.
	Recarries int `json:"recarries,omitempty"`
	// PauseMS is the router-visible write pause: fence raised to new map
	// published. FenceMS is fence raised to fence released.
	PauseMS    int64      `json:"pause_ms,omitempty"`
	FenceMS    int64      `json:"fence_ms,omitempty"`
	SwitchedAt *time.Time `json:"switched_at,omitempty"`
	Gate       string     `json:"gate,omitempty"`
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
	// and lets them again. The swap's last catch-up check and the
	// disabling of the forward subscriptions have to happen with nothing
	// able to arrive in between, or a write acknowledged in that gap is
	// left on a source nothing replicates from any more.
	PauseSources(ctx context.Context, pause bool) error
	// DisableForward stops the forward subscriptions; EnableReverse starts
	// the reverse ones. They were one step, which meant the sources had to
	// be writable again before the reverse apply could run and could not
	// stay paused across the disable.
	DisableForward(ctx context.Context) error
	EnableReverse(ctx context.Context) error
	// Release drops the range fence.
	Release(ctx context.Context) error
	// Complete drops every replication object of the run.
	Complete(ctx context.Context) error
	// Rollback returns serving to the sources: fence the targets, wait for
	// reverse replication (errRetry while behind), carry the sequences
	// back and flip the serving map to the source set.
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
		ok, why, err := ops.CaughtUp(ctx, wf.cutover.Positions)
		if err != nil {
			return false, err
		}
		if !ok {
			return true, retryf("%s", why)
		}
		// A router that had not seen the fence yet may have written after
		// the positions were read; the switch only proceeds once the
		// sources stood still through a whole catch-up.
		pos, err := ops.Positions(ctx)
		if err != nil {
			return false, err
		}
		if !maps.Equal(pos, wf.cutover.Positions) {
			wf.cutover.Positions = pos
			return true, retryf("sources advanced past the recorded positions")
		}
	case StepVerify:
		report, err := ops.Verify(ctx)
		if err != nil {
			return false, err
		}
		wf.cutover.Verify = &report
		if len(report.Mismatches) > 0 {
			// A source that moved while the digests were being taken makes
			// the target look ahead of it: the source is read first, and a
			// write landing between that read and the target's is already
			// applied by the time the target is read. Seen in CI as a
			// target holding exactly one batch more than the sources
			// predicted. That is a race in the measurement, not a target
			// that disagrees with its source, and it must not abandon a
			// switch -- the positions say which it was.
			pos, perr := ops.Positions(ctx)
			if perr != nil {
				return false, perr
			}
			if !maps.Equal(pos, wf.cutover.Positions) {
				wf.cutover.Positions = pos
				return true, retryf("sources advanced while verifying (%s)", strings.Join(report.Mismatches, "; "))
			}
			return false, fatal("verification failed: %s", strings.Join(report.Mismatches, "; "))
		}
	case StepSequences:
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
		// Last check before the flip: a router that missed the fence may
		// have written (or called nextval) after the recorded positions;
		// the flip only happens once the targets applied everything the
		// sources hold and the sources stood still through the check.
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
		if !maps.Equal(pos, wf.cutover.Positions) && wf.cutover.Recarries < maxSeqRecarries {
			// The movement may carry sequence advances the earlier
			// StepSequences did not see, so the switch jumps back there
			// and re-carries them before another flip attempt.
			//
			// Bounded, because a source is never obliged to stand still:
			// a checkpoint or an autovacuum moves pg_current_wal_lsn with
			// no user write behind it, and after the journal there is no
			// timeout to end the wait. What the flip needs is that the
			// targets hold everything the sources hold, which CaughtUp
			// has just established for these very positions. Sequence
			// positions are the part CaughtUp cannot speak for, and the
			// headroom collectSequences adds is finite, so the swap
			// carries them again with the sources paused rather than
			// leaving this bound to be what keeps them right.
			wf.cutover.Positions = pos
			wf.cutover.Recarries++
			wf.cutover.Step = StepSequences
			if err := c.saveCutover(ctx, wf, "switching: sources advanced before the flip; re-carrying sequences"); err != nil {
				return false, err
			}
			return true, retryf("sources advanced past the recorded positions before the flip")
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
		if err := ops.PauseSources(ctx, true); err != nil {
			return false, err
		}
		pos, err := ops.Positions(ctx)
		if err != nil {
			return false, errors.Join(err, ops.PauseSources(ctx, false))
		}
		ok, why, err := ops.CaughtUp(ctx, pos)
		if err != nil {
			return false, errors.Join(err, ops.PauseSources(ctx, false))
		}
		if !ok {
			// Left writable between attempts: a workflow that stops here
			// must not leave the sources refusing writes for good.
			return true, errors.Join(retryf("%s", why), ops.PauseSources(ctx, false))
		}
		// Sequence positions are not replicated. Between the carry before
		// the flip and here, a router that had not yet reloaded could
		// still have called nextval on a source: the row it wrote reaches
		// the targets, the sequence position does not, and the targets
		// hand the same value out again. The carry runs a second time
		// with the sources paused, so nothing can consume a value after
		// it -- which is what makes the flip's bounded re-carry a
		// liveness measure rather than the thing safety rests on.
		if err := ops.Sequences(ctx); err != nil {
			return false, errors.Join(err, ops.PauseSources(ctx, false))
		}
		if err := ops.DisableForward(ctx); err != nil {
			return false, errors.Join(err, ops.PauseSources(ctx, false))
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
// journal id was allocated, not from the step cursor -- StepFlip rewinds
// the cursor back to StepSequences when the sources advanced before the
// flip, and a cursor that can move backwards across the boundary cannot
// stand for a write that cannot be taken back. Past that point every error
// is retried instead.
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
func (c *Copier) abortSwitch(ctx context.Context, wf *copyWorkflow, ops cutoverOps, reason string) (bool, error) {
	if err := holdClaim(ctx, c.Pool, wf.id, wf.owner); err != nil {
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
