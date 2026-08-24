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
	Gate       string     `json:"gate,omitempty"`
	Aborts     []string   `json:"aborts,omitempty"`
}

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
	CheckedAt  time.Time
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
	// Journal writes the journal rows (idempotent by id).
	Journal(ctx context.Context, id string) error
	// Flip publishes the new map in one catalog transaction.
	Flip(ctx context.Context, journalID string) error
	// Swap disables forward and enables reverse replication.
	Swap(ctx context.Context) error
	// Release drops the range fence.
	Release(ctx context.Context) error
	// Complete drops every replication object of the run.
	Complete(ctx context.Context) error
	// Rollback returns serving to the sources: fence the targets, wait for
	// reverse replication (errRetry while behind), carry the sequences
	// back and flip the serving map to the source set.
	Rollback(ctx context.Context) error
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
		return false, c.saveCutover(ctx, wf, "paused before switchWrites: waiting for proceed")
	}
	wf.cutover.Gate = ""
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
	}
	for {
		step := wf.cutover.Step
		if step == "" {
			return true, nil
		}
		waiting, err := c.runStep(ctx, wf, ops, step)
		if err != nil {
			if !beforeJournal(step) {
				return false, err
			}
			if !errors.Is(err, errRetry) {
				// A fence must never outlive the switch that raised it.
				if isFatal(err) {
					if rerr := ops.Release(ctx); rerr != nil {
						return false, rerr
					}
					return false, err
				}
				if wf.cutover.FencedAt != nil && c.now().Sub(*wf.cutover.FencedAt) > c.cutoverTimeout() {
					return c.abortSwitch(ctx, wf, ops, fmt.Sprintf("step %s did not finish within %s: %s", step, c.cutoverTimeout(), err))
				}
				return false, err
			}
			waiting = true
		}
		if waiting {
			if beforeJournal(step) && wf.cutover.FencedAt != nil && c.now().Sub(*wf.cutover.FencedAt) > c.cutoverTimeout() {
				return c.abortSwitch(ctx, wf, ops, fmt.Sprintf("step %s did not finish within %s", step, c.cutoverTimeout()))
			}
			msg := fmt.Sprintf("switching: waiting at step %s", step)
			if err != nil {
				msg += ": " + strings.TrimPrefix(err.Error(), errRetry.Error()+": ")
			}
			return false, c.saveCutover(ctx, wf, msg)
		}
		next := nextStep(step)
		wf.cutover.Step = next
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
		if !maps.Equal(pos, wf.cutover.Positions) {
			wf.cutover.Positions = pos
			if err := c.saveCutover(ctx, wf, "switching: sources advanced before the flip; positions re-recorded"); err != nil {
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
		if err := ops.Swap(ctx); err != nil {
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

// abortSwitch undoes the fence before the journal and returns to the gate,
// failing the workflow once the attempts are used up.
func (c *Copier) abortSwitch(ctx context.Context, wf *copyWorkflow, ops cutoverOps, reason string) (bool, error) {
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
		return false, c.saveCutover(ctx, wf, "paused before complete: waiting for proceed")
	}
	wf.stage = StageCompleting
	return true, c.saveCutover(ctx, wf, "completing: dropping reverse replication")
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
	_, err := s.c.Pool.Exec(ctx, `UPDATE pgshard.workflows SET status = status || $2::jsonb, updated_at = now() WHERE id = $1::uuid`, wf.id, mustJSON(patch))
	return err
}

func (s poolCutoverStore) Finish(ctx context.Context, wf *copyWorkflow, state, message string) error {
	_, err := s.c.Pool.Exec(ctx, `UPDATE pgshard.workflows SET state = $2, status = status || $3::jsonb, updated_at = now() WHERE id = $1::uuid`,
		wf.id, state, mustJSON(map[string]any{"stage": wf.stage, "cutover": wf.cutover, "message": message}))
	return err
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
