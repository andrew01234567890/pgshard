package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

type cutoverMemStore struct {
	saves    []string
	finished string
	ids      int
}

func (m *cutoverMemStore) Save(_ context.Context, _ *copyWorkflow, message string) error {
	m.saves = append(m.saves, message)
	return nil
}

func (m *cutoverMemStore) Finish(_ context.Context, _ *copyWorkflow, state, message string) error {
	m.finished = state + ": " + message
	return nil
}

func (m *cutoverMemStore) NewJournalID(context.Context) (string, error) {
	m.ids++
	return fmt.Sprintf("journal-%d", m.ids), nil
}

type fakeOps struct {
	fingerprints    map[string]string
	calls           []string
	reverseCaughtUp bool
	gateOpen        bool
	gateWhy         string
	drain           []string
	sweepErr        error
	caughtUp        bool
	verify          VerifyReport
	verifyAdvance   int64
	fail            map[string]error
	fenced          bool
	journaled       map[string]int
	lsn             int64
	advance         int64
	// forwardDisabled is set by DisableForward and read by ForwardDisabled,
	// standing in for pg_subscription.subenabled on the targets.
	forwardDisabled bool
	// crashAfterDisable arms a failure on the unpause that follows
	// DisableForward, which is the one instant where the run has passed
	// the point of no return and not yet recorded anything.
	crashAfterDisable bool
	// partialDisable makes DisableForward disable some subscriptions and
	// THEN fail, which is what walking databases and targets one at a time
	// does when the second one is unreachable. The step's error path then
	// unpauses the sources with forward replication already half off.
	partialDisable bool
	// outsideDisable turns a forward subscription off at the flip, standing
	// in for an operator doing it by hand: the run itself never recorded
	// that it was disabling anything.
	outsideDisable bool
	// backgroundAdvance moves the source's WAL position for reasons no
	// fence can stop -- a checkpoint, an autovacuum, a standby snapshot.
	// Measured on an idle PostgreSQL 18 with no writes at all, so a fake
	// whose fenced source stands perfectly still is not a model of one.
	backgroundAdvance int64
	paused            bool
	pauses            int
	// caughtUpUntil, when set, makes CaughtUp report behind from that call
	// onwards, so a test can park the run at a chosen check.
	caughtUpUntil int
	caughtUpCalls int
	// openWriters are transactions that were already writing when the
	// pause went up. The pause cannot stop them --
	// default_transaction_read_only is read at transaction start -- so
	// they keep the source advancing until DrainSources waits them out.
	openWriters int
	// lostWrite records one of them committing after the positions were
	// sampled and after forward replication was disabled: an acknowledged
	// write on a source that is about to be retired, replicated nowhere.
	lostWrite bool
	// writeAfterFlip records one of them still open when the targets began
	// serving: its commit reaches a target that may already hold a newer
	// write to the same row (PGS-750).
	writeAfterFlip bool
	// preparedWhilePaused is how many more times Drain finds a prepared
	// transaction once the sources are paused: one prepared by a writer
	// the pause let finish, which no backend drain can see.
	preparedWhilePaused int
	// pauseLifted stands in for the pause being lifted behind the run's
	// back -- by hand, or by a controller that resumed past a quiesce it
	// never ran -- while the fake still believes it raised it.
	pauseLifted bool
	// disableDone is DisableForward having finished; unpausedHalfDisabled
	// records the sources made writable while it had only got part way.
	disableDone          bool
	unpausedHalfDisabled bool
	drains               int
	// targetsPaused stands in for the pause Rollback raises on the targets.
	// completeWhileTargetsWritable records Complete dropping the reverse
	// subscriptions with the targets taking writes: a stale router's commit
	// on a target in that window reaches nothing that would carry it back.
	// Complete then makes a rolled-back run's retired targets permanently
	// read-only, which the fake models by leaving targetsPaused set.
	targetsPaused                bool
	completeWhileTargetsWritable bool
}

func newFakeOps() *fakeOps {
	return &fakeOps{gateOpen: true, caughtUp: true, fail: map[string]error{}, journaled: map[string]int{}}
}

func (f *fakeOps) step(name string) error {
	f.calls = append(f.calls, name)
	if err := f.fail[name]; err != nil {
		delete(f.fail, name)
		return err
	}
	return nil
}

func (f *fakeOps) GateOpen(context.Context) (bool, string, error) {
	return f.gateOpen, f.gateWhy, f.step("gate")
}
func (f *fakeOps) Fence(context.Context) error { f.fenced = true; return f.step(StepFence) }
func (f *fakeOps) Drain(context.Context) ([]string, error) {
	if f.paused && f.preparedWhilePaused > 0 {
		f.preparedWhilePaused--
		return []string{"default/0:pgshard-late"}, f.step(StepDrain)
	}
	return f.drain, f.step(StepDrain)
}
func (f *fakeOps) Sweep(context.Context) error {
	if err := f.step(StepSweep); err != nil {
		return err
	}
	return f.sweepErr
}
func (f *fakeOps) Positions(context.Context) (map[string]int64, error) {
	if !f.paused {
		f.lsn += f.advance
	}
	// Background WAL moves the source's position without leaving anything
	// to apply: it decodes to nothing, so the walsender carries every slot
	// over it on a keepalive.
	f.lsn += f.backgroundAdvance
	return map[string]int64{"0": f.lsn}, f.step(StepPositions)
}

// DrainSources ends the writers the pause could not.
func (f *fakeOps) DrainSources(context.Context) error {
	if err := f.step("drain_sources"); err != nil {
		return err
	}
	f.drains++
	f.openWriters = 0
	return nil
}
func (f *fakeOps) CaughtUp(_ context.Context, pos map[string]int64) (bool, string, error) {
	// A slot may be at or past the asked-for position -- it advances over
	// WAL that decodes to nothing -- so only a position from the future is
	// a caller mistake.
	if pos["0"] > f.lsn {
		return false, "", fmt.Errorf("caught-up check against a position %d ahead of the source at %d", pos["0"], f.lsn)
	}
	f.caughtUpCalls++
	if f.caughtUpUntil > 0 && f.caughtUpCalls > f.caughtUpUntil {
		return false, "lagging", f.step(StepCatchUp)
	}
	return f.caughtUp, "lagging", f.step(StepCatchUp)
}
func (f *fakeOps) Verify(context.Context) (VerifyReport, error) {
	// A source that moves while the digests are being taken: the source is
	// read first and the target second, so a write in between is already
	// applied on the target when it is read -- which is why the target
	// looks AHEAD, and why asking whether the targets have caught up says
	// yes about a race.
	//
	// The numbers such a mismatch reports describe one instant, so they are
	// different every time it is asked. A target that genuinely disagrees
	// reports the same count, sum and xor every time, and that is the only
	// thing telling the two apart.
	f.lsn += f.verifyAdvance
	report := f.verify
	if f.verifyAdvance > 0 && len(report.Mismatches) > 0 {
		report.Mismatches = []string{fmt.Sprintf("%s (at %d)", report.Mismatches[0], f.lsn)}
	}
	return report, f.step(StepVerify)
}
func (f *fakeOps) Sequences(context.Context) error {
	return f.step(StepSequences)
}
func (f *fakeOps) Reverse(context.Context) error { return f.step(StepReverse) }
func (f *fakeOps) SchemaFingerprints(context.Context) (map[string]string, error) {
	if f.fingerprints == nil {
		return map[string]string{"default/0/app": "before"}, nil
	}
	return f.fingerprints, nil
}
func (f *fakeOps) Journal(_ context.Context, id string) error {
	f.journaled[id]++
	return f.step(StepJournal)
}
func (f *fakeOps) Flip(context.Context, string) error {
	if f.openWriters > 0 {
		f.writeAfterFlip = true
	}
	if f.outsideDisable {
		f.forwardDisabled = true
		f.caughtUp = false
	}
	return f.step(StepFlip)
}
func (f *fakeOps) DisableForward(context.Context) error {
	// A writer still open at this point commits with the forward
	// subscriptions already gone, so its row stays on a source nothing
	// replicates from.
	if f.openWriters > 0 {
		f.lostWrite = true
	}
	if f.partialDisable {
		// Half disabled is already past the point CaughtUp can speak for.
		f.forwardDisabled = true
		f.caughtUp = false
		f.partialDisable = false
		return errors.New("target 2 unreachable")
	}
	if err := f.step(StepSwap); err != nil {
		return err
	}
	// Disabling is what freezes the forward slots, so from here the fake
	// can no longer be caught up: exactly the real coordinate, where
	// confirmed_flush_lsn stands still and the source's WAL does not.
	f.forwardDisabled = true
	f.caughtUp = false
	f.disableDone = true
	if f.crashAfterDisable {
		f.crashAfterDisable = false
		f.fail["pause_sources"] = errors.New("controller crashed after disabling forward replication")
	}
	return nil
}

func (f *fakeOps) ForwardDisabled(context.Context) (bool, error) {
	return f.forwardDisabled, f.step("forward_disabled")
}
func (f *fakeOps) EnableReverse(context.Context) error { return f.step("enable_reverse") }

// PauseSources records the pause and, while paused, stops the fake source
// advancing: that is what a write pause buys, and a test that keeps the
// source moving through it is not testing the pause.
func (f *fakeOps) PauseSources(_ context.Context, pause bool) error {
	if err := f.step("pause_sources"); err != nil {
		return err
	}
	if pause {
		f.pauseLifted = false
	}
	if !pause && f.forwardDisabled && !f.disableDone {
		f.unpausedHalfDisabled = true
	}
	f.paused = pause
	if pause {
		f.pauses++
	}
	return nil
}
func (f *fakeOps) SourcesPaused(context.Context) (bool, error) {
	return f.paused && !f.pauseLifted, nil
}
func (f *fakeOps) DropJournal(_ context.Context, id string) error {
	delete(f.journaled, id)
	return f.step("drop_journal")
}
func (f *fakeOps) Release(context.Context) error { f.fenced = false; return f.step(StepRelease) }
func (f *fakeOps) Complete(context.Context) error {
	if err := f.step("complete"); err != nil {
		return err
	}
	if f.wasRolledBack() {
		if !f.targetsPaused {
			f.completeWhileTargetsWritable = true
		}
		// The tail of the real Complete: the targets are the retired set
		// now, and their pause becomes the permanent, unclaimed one.
		f.targetsPaused = true
	}
	return nil
}

// Rollback mirrors pgCutover.Rollback's pause handling: raised first,
// lifted again on every return that stops short of the flip back, and left
// standing by one that gets there.
func (f *fakeOps) Rollback(context.Context) error {
	if err := f.step("rollback"); err != nil {
		return err
	}
	f.targetsPaused = true
	if !f.reverseCaughtUp {
		f.targetsPaused = false
		return retryf("reverse replication behind")
	}
	return nil
}

func (f *fakeOps) wasRolledBack() bool {
	return slices.Contains(f.calls, "rollback")
}

type cutoverHarness struct {
	c     *Copier
	wf    *copyWorkflow
	ops   *fakeOps
	store *cutoverMemStore
	clock time.Time
}

func newCutoverHarness(t *testing.T) *cutoverHarness {
	t.Helper()
	h := &cutoverHarness{ops: newFakeOps(), store: &cutoverMemStore{}, clock: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	h.c = &Copier{cutoverStore: h.store, Now: func() time.Time { return h.clock }}
	h.wf = &copyWorkflow{id: "wf", stage: StageCatchUpDone, set: "g2", gen: 2, spec: cutoverSpec{SourceSet: "default", RetireAfterSeconds: 3600}}
	return h
}

func (h *cutoverHarness) pass(t *testing.T) bool {
	t.Helper()
	advanced, err := h.c.cutover(context.Background(), h.wf, h.ops)
	if err != nil {
		t.Fatalf("stage %s step %s: %v", h.wf.stage, h.wf.cutover.Step, err)
	}
	return advanced
}

// waitAt drives passes until the run has saved a wait at step, which is how
// a step before the journal reports one.
func (h *cutoverHarness) waitAt(t *testing.T, step string) string {
	t.Helper()
	for i := 0; i < 50; i++ {
		if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err != nil {
			t.Fatalf("pass: %v", err)
		}
		if last := h.store.saves[len(h.store.saves)-1]; h.wf.cutover.Step == step && strings.HasPrefix(last, "switching: waiting at step "+step) {
			return last
		}
	}
	t.Fatalf("never waited at %s; at %s", step, h.wf.cutover.Step)
	return ""
}

// parkAt drives passes until the run reports an error at step.
func (h *cutoverHarness) parkAt(t *testing.T, step string) error {
	t.Helper()
	var err error
	for i := 0; h.wf.cutover.Step != step || err == nil; i++ {
		if i > 50 {
			t.Fatalf("never parked at %s; at %s (%v)", step, h.wf.cutover.Step, err)
		}
		_, err = h.c.cutover(context.Background(), h.wf, h.ops)
	}
	return err
}

func (h *cutoverHarness) runUntil(t *testing.T, stage string) {
	t.Helper()
	for i := 0; i < 50; i++ {
		if h.wf.stage == stage {
			return
		}
		h.pass(t)
	}
	t.Fatalf("never reached %s; at %s/%s", stage, h.wf.stage, h.wf.cutover.Step)
}

func TestCutoverHappyPath(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitched)
	want := []string{"gate", StepFence, StepDrain, StepSweep, StepPositions, StepCatchUp, StepVerify, StepReverse,
		"pause_sources", "drain_sources", StepDrain, StepPositions, StepCatchUp, StepSequences, StepJournal,
		"drain_sources", StepPositions, StepCatchUp, StepFlip,
		"forward_disabled", "drain_sources", StepPositions, StepCatchUp, StepSequences, StepSwap, "pause_sources", "enable_reverse", StepRelease}
	if got := strings.Join(h.ops.calls, ","); got != strings.Join(want, ",") {
		t.Fatalf("calls %s", got)
	}
	if h.ops.fenced {
		t.Fatal("fence left raised")
	}
	if h.wf.cutover.JournalID == "" || h.ops.journaled[h.wf.cutover.JournalID] != 1 {
		t.Fatalf("journal %+v", h.ops.journaled)
	}
	if h.wf.cutover.SwitchedAt == nil || h.wf.cutover.ReleasedAt == nil || h.wf.cutover.FlippedAt == nil {
		t.Fatalf("timestamps missing: %+v", h.wf.cutover)
	}
	if h.pass(t) {
		t.Fatal("retirement window must hold the workflow")
	}
	h.clock = h.clock.Add(2 * time.Hour)
	h.runUntil(t, StageCompleted)
	if h.ops.calls[len(h.ops.calls)-1] != "complete" || !strings.HasPrefix(h.store.finished, StateCompleted) {
		t.Fatalf("complete: calls %v finished %q", h.ops.calls, h.store.finished)
	}
}

// TestCutoverConvergesWhileBackgroundWALMoves is PGS-784. A checkpoint, an
// autovacuum or a standby snapshot moves pg_current_wal_lsn with no user
// write behind it -- measured on an idle PostgreSQL 18 with no writes at
// all -- and no fence can stop any of them. catch_up used to demand the
// position be identical across a catch-up, which made it a coin flip
// against that background: it lost about one CI cutover in eleven with
// "sources advanced past the recorded positions", and a real upgrade sat
// retrying it indefinitely.
func TestCutoverConvergesWhileBackgroundWALMoves(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.backgroundAdvance = 7
	h.runUntil(t, StageSwitching)
	h.runUntil(t, StageSwitched)
	for _, a := range h.wf.cutover.Aborts {
		t.Errorf("background WAL must not abort a switch: %s", a)
	}
	if h.ops.lsn == 0 {
		t.Fatal("the fixture stopped exercising the property: the source never moved")
	}
}

// TestCutoverConvergesWhileTheSourcesKeepWriting: the recorded positions are
// a FIXED boundary, so a source that keeps writing cannot run away from the
// targets. Making the sources stand still first is not an option before the
// journal -- default_transaction_read_only there fails the writes of the
// clients the fence deliberately let through, seen as 25006 in
// TestReshardCutoverUnderLoad. StepSwap is where the sources are stopped,
// after the flip, and it re-reads and re-checks the positions there.
func TestCutoverConvergesWhileTheSourcesKeepWriting(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.advance = 1000
	h.runUntil(t, StageSwitching)
	h.runUntil(t, StageSwitched)
	for _, a := range h.wf.cutover.Aborts {
		t.Errorf("a writing source must not abort a switch: %s", a)
	}
	if h.wf.cutover.Positions["0"] > h.ops.lsn {
		t.Fatalf("positions %v ahead of the source at %d", h.wf.cutover.Positions, h.ops.lsn)
	}
}

// TestAbortSaysWhatItWasWaitingFor guards the abort message. The catch-up step
// distinguishes replication that is still applying from a subscription whose
// slot has gone, and an abort that reports only that the deadline passed throws
// that away -- which is exactly what a CI failure needs.
func TestAbortSaysWhatItWasWaitingFor(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.caughtUp = false
	h.runUntil(t, StageSwitching)
	for h.wf.cutover.Step != StepCatchUp {
		h.pass(t)
	}
	h.clock = h.clock.Add(2 * DefaultCutoverTimeout)
	h.pass(t)

	if len(h.wf.cutover.Aborts) == 0 {
		t.Fatal("the switch must abort once the fence outlives the timeout")
	}
	last := h.wf.cutover.Aborts[len(h.wf.cutover.Aborts)-1]
	if !strings.Contains(last, StepCatchUp) {
		t.Fatalf("abort %q must name the step", last)
	}
	if !strings.Contains(last, "lagging") {
		t.Fatalf("abort %q must carry what the step was waiting for", last)
	}
}

func TestCutoverPauseMeasuresFenceToFlip(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.caughtUp = false
	h.runUntil(t, StageSwitching)
	for h.wf.cutover.Step != StepCatchUp || h.wf.cutover.Step == "" {
		h.pass(t)
	}
	h.clock = h.clock.Add(700 * time.Millisecond)
	h.ops.caughtUp = true
	h.runUntil(t, StageSwitched)
	if h.wf.cutover.PauseMS != 700 || h.wf.cutover.FenceMS != 700 {
		t.Fatalf("pause %d fence %d", h.wf.cutover.PauseMS, h.wf.cutover.FenceMS)
	}
}

func TestCutoverGateAndPauseBefore(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.gateOpen, h.ops.gateWhy = false, "lag 5 bytes"
	h.runUntil(t, StageAwaitingSwitch)
	if h.pass(t) || h.wf.cutover.Gate != "lag 5 bytes" {
		t.Fatalf("closed gate must hold: %+v", h.wf.cutover)
	}
	h.ops.gateOpen = true
	h.wf.spec.PauseBefore = PauseSwitchWrites
	if h.pass(t) || h.wf.cutover.Gate != "paused before switchWrites" {
		t.Fatalf("pause must hold: %+v", h.wf.cutover)
	}
	h.wf.spec.Proceed = []string{PauseSwitchWrites}
	if !h.pass(t) || h.wf.stage != StageSwitching {
		t.Fatalf("proceed must open the gate: %s", h.wf.stage)
	}
	h.runUntil(t, StageSwitched)
	h.clock = h.clock.Add(2 * time.Hour)
	h.wf.spec.PauseBefore = PauseComplete
	if h.pass(t) || h.wf.stage != StageSwitched {
		t.Fatal("pause before complete must hold")
	}
	h.wf.spec.Proceed = []string{PauseComplete}
	h.runUntil(t, StageCompleted)
}

func TestCutoverVerifyMismatchAbortsBeforeJournal(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.verify = VerifyReport{Mismatches: []string{"app.orders/0: count 10 vs 9"}}
	h.runUntil(t, StageSwitching)
	// The first mismatch is asked again rather than believed: only the same
	// digests twice are a disagreement rather than a measurement race.
	var err error
	for i := 0; i < 5 && !isFatal(err); i++ {
		_, err = h.c.cutover(context.Background(), h.wf, h.ops)
	}
	if !isFatal(err) || !strings.Contains(err.Error(), "verification failed") {
		t.Fatalf("err %v", err)
	}
	if h.ops.fenced {
		t.Fatal("fence must be released on a fatal abort")
	}
	if len(h.ops.journaled) != 0 {
		t.Fatal("journal must not be written")
	}
}

// TestCutoverVerifyRetriesWhileTheSourceMoves: verify reads the sources
// first and the targets second, so a write landing between the two reads is
// already applied on the target and makes it look ahead of its source. CI
// saw exactly that -- a target holding one batch more than the sources
// predicted. That is a race in the measurement, not a target that disagrees
// with its source, and it must not abandon the switch. What tells the two
// apart is asking the digests again: a race reported one instant and reports
// different numbers next time, while a real disagreement repeats exactly.
func TestCutoverVerifyRetriesWhileTheSourceMoves(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.verify = VerifyReport{Mismatches: []string{"app.ledger on g2/1: 1740 rows, sources predict 1730"}}
	h.ops.verifyAdvance = 10
	h.runUntil(t, StageSwitching)

	// Several passes: each reaches verify, sees the source has moved, and
	// starts again rather than giving up.
	for range 3 {
		if _, err := h.c.cutover(context.Background(), h.wf, h.ops); isFatal(err) {
			t.Fatalf("a mismatch against a moving source must not abandon the switch: %v", err)
		}
	}
	if len(h.ops.journaled) != 0 {
		t.Fatal("journal must not be written")
	}

	// Standing still, the same mismatch is what it says it is.
	h.ops.verifyAdvance = 0
	var err error
	for range 6 {
		if _, err = h.c.cutover(context.Background(), h.wf, h.ops); isFatal(err) {
			break
		}
	}
	if !isFatal(err) || !strings.Contains(err.Error(), "verification failed") {
		t.Fatalf("a mismatch against a still source must abort: %v", err)
	}
}

func TestCutoverTimeoutUndoesFenceAndFailsAfterAttempts(t *testing.T) {
	h := newCutoverHarness(t)
	h.c.CutoverTimeout, h.c.CutoverAttempts = time.Second, 2
	h.ops.drain = []string{"gid-1"}
	h.runUntil(t, StageSwitching)
	h.pass(t)
	if h.wf.cutover.Step != StepDrain || !h.ops.fenced {
		t.Fatalf("drain must wait fenced: %+v", h.wf.cutover)
	}
	h.clock = h.clock.Add(2 * time.Second)
	if !h.pass(t) || h.wf.stage != StageAwaitingSwitch || h.ops.fenced || h.wf.cutover.Attempts != 1 || h.wf.cutover.FencedAt != nil {
		t.Fatalf("timeout must undo: %s fenced=%t %+v", h.wf.stage, h.ops.fenced, h.wf.cutover)
	}
	h.runUntil(t, StageSwitching)
	h.pass(t)
	h.clock = h.clock.Add(2 * time.Second)
	_, err := h.c.cutover(context.Background(), h.wf, h.ops)
	if !isFatal(err) || h.ops.fenced {
		t.Fatalf("second abort must fail the workflow: %v fenced=%t", err, h.ops.fenced)
	}
}

func TestCutoverAfterJournalRetriesForever(t *testing.T) {
	h := newCutoverHarness(t)
	h.c.CutoverTimeout = time.Second
	h.ops.fail[StepFlip] = errors.New("catalog down")
	h.runUntil(t, StageSwitching)
	_, err := h.c.cutover(context.Background(), h.wf, h.ops)
	if err == nil || isFatal(err) || h.wf.cutover.Step != StepFlip {
		t.Fatalf("flip error must be retried, not aborted: %v step %s", err, h.wf.cutover.Step)
	}
	h.clock = h.clock.Add(time.Hour)
	h.runUntil(t, StageSwitched)
	if h.wf.cutover.Attempts != 0 || h.ops.journaled[h.wf.cutover.JournalID] != 1 {
		t.Fatalf("no undo after the journal: %+v", h.wf.cutover)
	}
}

// opOf is the fake operation a switch step is seen by: quiesce is the one
// step that is not an operation of its own.
func opOf(step string) string {
	if step == StepQuiesce {
		return "pause_sources"
	}
	return step
}

func TestCutoverCrashAtEveryStepResumesIdempotently(t *testing.T) {
	for _, crashAt := range switchSteps {
		t.Run(crashAt, func(t *testing.T) {
			h := newCutoverHarness(t)
			h.runUntil(t, StageSwitching)
			crashed := errors.New("crash")
			h.ops.fail[opOf(crashAt)] = crashed
			_, err := h.c.cutover(context.Background(), h.wf, h.ops)
			if !errors.Is(err, crashed) {
				t.Fatalf("err %v", err)
			}
			if h.wf.cutover.Step != crashAt {
				t.Fatalf("step after crash %s", h.wf.cutover.Step)
			}
			// A restart reloads the persisted record: the step itself, its
			// timestamps and the journal id survive; the ops fake keeps the
			// shard-side state.
			resumed := *h.wf
			h.wf = &resumed
			h.runUntil(t, StageSwitched)
			if n := strings.Count(strings.Join(h.ops.calls, ","), StepJournal); n > 2 {
				t.Fatalf("journal called %d times", n)
			}
			if len(h.ops.journaled) != 1 {
				t.Fatalf("journal ids %v", h.ops.journaled)
			}
			if h.ops.fenced {
				t.Fatal("fence left raised")
			}
			for _, step := range switchSteps {
				if !strings.Contains(strings.Join(h.ops.calls, ","), opOf(step)) {
					t.Fatalf("step %s never ran", step)
				}
			}
		})
	}
}

func TestRetryWaitsWithoutTimeoutAfterJournal(t *testing.T) {
	if beforeJournal(StepFlip) || !beforeJournal(StepVerify) || beforeJournal(StepJournal) {
		t.Fatal("beforeJournal boundary")
	}
	if nextStep(StepRelease) != "" || nextStep("bogus") != "" || nextStep(StepFence) != StepDrain {
		t.Fatal("nextStep")
	}
}

func TestCutoverSpecDefaults(t *testing.T) {
	var s cutoverSpec
	if s.retireAfter() != DefaultRetireAfter || s.paused(PauseComplete) {
		t.Fatal("defaults")
	}
	s = cutoverSpec{PauseBefore: PauseComplete, RetireAfterSeconds: 5}
	if !s.paused(PauseComplete) || s.paused(PauseSwitchWrites) || s.retireAfter() != 5*time.Second {
		t.Fatal("spec")
	}
}

func TestCutoverSwapWaitsUntilTargetsCaughtUp(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	boom := errors.New("boom")
	h.ops.fail[StepSwap] = boom
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); !errors.Is(err, boom) {
		t.Fatalf("err %v", err)
	}
	if n := strings.Count(strings.Join(h.ops.calls, ","), StepSwap); n != 1 {
		t.Fatalf("swap called %d times", n)
	}
	h.ops.caughtUp = false
	for i := 0; i < 3; i++ {
		_, err := h.c.cutover(context.Background(), h.wf, h.ops)
		if err == nil || isFatal(err) {
			t.Fatalf("pass %d: err %v", i, err)
		}
		if n := strings.Count(strings.Join(h.ops.calls, ","), StepSwap); n != 1 {
			t.Fatalf("pass %d: swap ran again while the targets lag (%d calls)", i, n)
		}
	}
	h.ops.caughtUp = true
	h.runUntil(t, StageSwitched)
}

func TestCutoverErroringStepBeforeJournalHitsTimeout(t *testing.T) {
	h := newCutoverHarness(t)
	h.c.CutoverTimeout, h.c.CutoverAttempts = time.Second, 2
	h.ops.sweepErr = errors.New("shard unreachable")
	h.runUntil(t, StageSwitching)
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err == nil {
		t.Fatal("sweep error must surface")
	}
	if !h.ops.fenced || h.wf.cutover.Step != StepSweep {
		t.Fatalf("step %s fenced=%t", h.wf.cutover.Step, h.ops.fenced)
	}
	h.clock = h.clock.Add(2 * time.Second)
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err != nil {
		t.Fatalf("timeout must abort, not error: %v", err)
	}
	if h.wf.stage != StageAwaitingSwitch || h.ops.fenced || h.wf.cutover.Attempts != 1 {
		t.Fatalf("erroring step must undo the fence after the timeout: %s fenced=%t %+v", h.wf.stage, h.ops.fenced, h.wf.cutover)
	}
}

// TestCutoverSucceedingStepStillHitsTheFenceDeadline: the deadline was only
// consulted when a step failed or reported waiting, so a step that SUCCEEDED
// slowly held the fence for as long as it took. verify is the case that
// matters -- it scans every table with writes already fenced, so on a real
// table the hold grows with the data and nothing stopped it.
func TestCutoverSucceedingStepStillHitsTheFenceDeadline(t *testing.T) {
	h := newCutoverHarness(t)
	h.c.CutoverTimeout, h.c.CutoverAttempts = time.Second, 2
	h.ops.fail = map[string]error{StepVerify: retryf("still comparing")}
	h.runUntil(t, StageSwitching)
	h.pass(t)
	if h.wf.cutover.Step != StepVerify || !h.ops.fenced {
		t.Fatalf("expected to be parked at verify behind the fence; step=%s fenced=%t", h.wf.cutover.Step, h.ops.fenced)
	}
	// The scan finishes -- successfully -- but only after the fence has been
	// held past its limit.
	h.clock = h.clock.Add(2 * time.Second)
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err != nil {
		t.Fatalf("an over-deadline fence must abort, not error: %v", err)
	}
	if h.wf.stage != StageAwaitingSwitch || h.ops.fenced {
		t.Fatalf("a step that succeeded past the deadline must undo the fence: stage=%s fenced=%t", h.wf.stage, h.ops.fenced)
	}
	if h.wf.cutover.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", h.wf.cutover.Attempts)
	}
}

// TestStalledStepReportsItsAge: after the journal every error is retried
// with no timeout and no attempt limit, and every pass refreshes updated_at.
// A step that has been failing for hours therefore looks exactly like a
// healthy running workflow -- which is the state a real upgrade sat in,
// retrying "sources advanced past the recorded positions" indefinitely.
func TestStalledStepReportsItsAge(t *testing.T) {
	base := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	st := &cutoverState{Step: StepSequences}
	st.stampStep(base)

	// Briefly retrying is normal and says nothing.
	if got := st.stalledFor(base.Add(10 * time.Second)); got != "" {
		t.Fatalf("a step retrying for 10s reported %q; that is ordinary", got)
	}
	// Past the threshold the age is the signal.
	got := st.stalledFor(base.Add(37 * time.Minute))
	if !strings.Contains(got, StepSequences) || !strings.Contains(got, "37m0s") {
		t.Fatalf("a step stuck for 37 minutes reported %q; it must name the step and the age", got)
	}
	// Advancing resets it: the age is per step, not per workflow.
	st.Step = StepFlip
	st.stampStep(base.Add(37 * time.Minute))
	if got := st.stalledFor(base.Add(37*time.Minute + time.Second)); got != "" {
		t.Fatalf("a freshly entered step reported %q", got)
	}
}

func TestStepAgeIsAbsentBeforeAnyStep(t *testing.T) {
	st := &cutoverState{}
	if got := st.stalledFor(time.Now()); got != "" {
		t.Fatalf("unstamped state reported %q", got)
	}
}

// TestCaughtUpIsNeverVacuouslyTrue: the check asked whether every forward
// slot had confirmed a flush at or past its source's position, and
// answered "yes" whenever there was nothing to ask -- no sources, no
// targets, or a source whose position the map did not carry, where the
// lookup yields zero and every slot is at or past zero. Each of those
// lets a switch proceed onto a target that never received the rows, which
// is how an upgrade completed with a quarter of the acknowledged writes on
// the group it retired.
func TestCaughtUpIsNeverVacuouslyTrue(t *testing.T) {
	for _, c := range []struct {
		name              string
		srcIDs, targetIDs []int32
		positions         map[string]int64
		want              string
	}{
		{"no sources", nil, []int32{0}, map[string]int64{"0": 100}, "no source shards"},
		{"no targets", []int32{0}, nil, map[string]int64{"0": 100}, "no target shards"},
		{"position missing", []int32{0}, []int32{1}, map[string]int64{}, "0 has no recorded source position"},
		{"position for another shard", []int32{0}, []int32{1}, map[string]int64{"7": 100}, "0 has no recorded source position"},
		{"position zero", []int32{0}, []int32{1}, map[string]int64{"0": 0}, "0 has no recorded source position"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := nothingToCompare("default", c.srcIDs, c.targetIDs, c.positions)
			if len(got) == 0 {
				t.Fatalf("comparing nothing must not read as caught up")
			}
			if !strings.Contains(strings.Join(got, "; "), c.want) {
				t.Fatalf("refusals %v, want one mentioning %q", got, c.want)
			}
		})
	}
	// A question with content is left to the slot comparison.
	if got := nothingToCompare("default", []int32{0, 1}, []int32{2}, map[string]int64{"0": 10, "1": 20}); len(got) != 0 {
		t.Fatalf("a complete question must not be refused: %v", got)
	}
}

// TestSwapPausesTheSourcesAroundTheLastCheck: the swap sampled the source
// positions, checked the targets had applied them, and only then disabled
// the forward subscriptions. A router that had not yet reloaded its
// snapshot could commit to a source in that gap, and the write was
// acknowledged on a group whose replication was about to be turned off.
// The check speaks for the position it sampled and nothing else, so the
// sources have to be unable to accept a write between the two.
func TestSwapPausesTheSourcesAroundTheLastCheck(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitched)
	calls := strings.Join(h.ops.calls, ",")
	swap := strings.Index(calls, StepSwap)
	pause := strings.LastIndex(calls[:swap], "pause_sources")
	if swap < 0 || pause < 0 {
		t.Fatalf("calls %s", calls)
	}
	// Nothing between the pause and the disable may sample or decide
	// anything the pause was taken to make safe -- and the disable itself
	// must land inside it.
	between := calls[pause:swap]
	if !strings.Contains(between, StepPositions) || !strings.Contains(between, StepCatchUp) {
		t.Fatalf("the last check must happen inside the pause: %s", between)
	}
	if h.ops.paused {
		t.Fatal("the sources must be writable again once the forward subscriptions are off")
	}
	// The reverse subscriptions apply to the sources, so they start after
	// the pause is lifted.
	if strings.Index(calls, "enable_reverse") < strings.LastIndex(calls, "pause_sources") {
		t.Fatalf("reverse replication must start after the pause is lifted: %s", calls)
	}
}

// TestSwapWaitsOutTheWritersThePauseCannotStop.
//
// default_transaction_read_only is read when a transaction STARTS, so the
// pause stops new writers and nothing else: a transaction already open
// commits straight through it. The router lets one that opened before the
// fence carry on writing, deliberately, so this is the ordinary case and not
// a race.
//
// Such a write lands after the positions were sampled and after the forward
// subscriptions are disabled -- an acknowledged commit on a source that is
// about to be retired, which nothing replicates and nothing carries back.
// The rollback path waited for these from the start; the forward swap did
// not.
func TestSwapWaitsOutTheWritersThePauseCannotStop(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.openWriters = 1
	h.runUntil(t, StageSwitched)

	if h.ops.drains == 0 {
		t.Fatal("the swap must wait for the writers that were open when the pause went up")
	}
	if h.ops.lostWrite {
		t.Fatal("a writer open across the swap committed on a source whose forward replication was already off")
	}
	calls := strings.Join(h.ops.calls, ",")
	swap := strings.Index(calls, StepSwap)
	pause := strings.LastIndex(calls[:swap], "pause_sources")
	drain := strings.Index(calls[pause:swap], "drain_sources")
	pos := strings.Index(calls[pause:swap], StepPositions)
	if drain < 0 || pos < 0 || drain > pos {
		// Draining after the sample proves nothing: the sample would
		// already be missing the write.
		t.Fatalf("the drain must happen inside the pause and before the positions are sampled: %s", calls[pause:swap])
	}
}

// TestQuiesceWaitsOutTheWritersThePauseCannotStop (PGS-750): a writer the
// fence let carry on must be finished before the journal, and so before the
// targets serve. Committing after the flip, its row is carried onto a target
// that may already hold a newer write to it.
func TestQuiesceWaitsOutTheWritersThePauseCannotStop(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.openWriters = 1
	h.runUntil(t, StageSwitched)
	if h.ops.writeAfterFlip {
		t.Fatal("a writer open before the pause was still open when the targets began serving")
	}
	calls := strings.Join(h.ops.calls, ",")
	window := calls[strings.Index(calls, StepReverse):strings.Index(calls, StepJournal)]
	order := []string{"pause_sources", "drain_sources", StepDrain, StepPositions, StepCatchUp}
	at := 0
	for _, c := range order {
		next := strings.Index(window[at:], c)
		if next < 0 {
			t.Fatalf("between reverse and the journal, want %v in order: %s", order, window)
		}
		at += next + len(c)
	}
}

// TestQuiesceWaitsForATransactionPreparedUnderThePause: a prepared
// transaction has no backend for the writer drain to see, and COMMIT
// PREPARED runs in a read-only transaction, so quiesce asks for prepared
// transactions again once the sources are paused.
func TestQuiesceWaitsForATransactionPreparedUnderThePause(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	h.ops.preparedWhilePaused = 1
	if msg := h.waitAt(t, StepQuiesce); !strings.Contains(msg, "pgshard-late") {
		t.Fatalf("quiesce waited with %q, want the prepared transaction named", msg)
	}
	if h.wf.cutover.JournalID != "" {
		t.Fatal("the journal was written with a prepared transaction on a source")
	}
	h.runUntil(t, StageSwitched)
}

// TestASlowWriterUndoesTheSwitchInsteadOfHoldingTheFence: a transaction that
// will not end cannot hold the write fence for good. Quiesce lifts its pause
// whenever it has to wait, and past the cutover timeout the switch is
// undone -- fence released, sources writable -- as any step before the
// journal is.
func TestASlowWriterUndoesTheSwitchInsteadOfHoldingTheFence(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	h.ops.fail["drain_sources"] = errors.New("1 write transactions on default still open after 30s")
	if msg := h.waitAt(t, StepQuiesce); !strings.Contains(msg, "still open") {
		t.Fatalf("quiesce waited with %q, want the open writers named", msg)
	}
	if h.ops.paused {
		t.Fatal("quiesce kept the pause while it waited")
	}
	h.clock = h.clock.Add(2 * DefaultCutoverTimeout)
	h.ops.fail["drain_sources"] = errors.New("1 write transactions on default still open after 30s")
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err != nil {
		t.Fatalf("the undo reported %v", err)
	}
	if h.wf.stage != StageAwaitingSwitch || len(h.wf.cutover.Aborts) != 1 {
		t.Fatalf("stage %s aborts %v, want the switch undone once", h.wf.stage, h.wf.cutover.Aborts)
	}
	if h.ops.fenced || h.ops.paused {
		t.Fatalf("after the undo: fenced %v, paused %v", h.ops.fenced, h.ops.paused)
	}
	h.runUntil(t, StageSwitched)
}

// TestAnUndoAfterQuiesceLiftsItsPause: quiesce succeeded and a later step
// before the journal fails past the cutover timeout. The switch is undone
// with the pause standing, on sources that still serve; the undo has to
// lift it, or they refuse writes with nothing left to give them back.
func TestAnUndoAfterQuiesceLiftsItsPause(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	h.ops.fail[StepSequences] = errors.New("target unreachable")
	if err := h.parkAt(t, StepSequences); err == nil {
		t.Fatal("the failing step must surface its error")
	}
	if !h.ops.paused {
		t.Fatal("quiesce's pause did not stand into the next step, so this does not test the undo")
	}
	h.clock = h.clock.Add(2 * DefaultCutoverTimeout)
	h.ops.fail[StepSequences] = errors.New("target unreachable")
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err != nil {
		t.Fatalf("the undo reported %v", err)
	}
	if h.wf.stage != StageAwaitingSwitch || len(h.wf.cutover.Aborts) != 1 {
		t.Fatalf("stage %s aborts %v, want the switch undone once", h.wf.stage, h.wf.cutover.Aborts)
	}
	if h.ops.fenced || h.ops.paused {
		t.Fatalf("after the undo: fenced %v, paused %v", h.ops.fenced, h.ops.paused)
	}
	// Pause first: routers that see the fence drop send writes at once.
	if indexOfLast(t, h.ops.calls, "pause_sources") > indexOfLast(t, h.ops.calls, StepRelease) {
		t.Fatalf("the fence was released before the pause was lifted: %s", strings.Join(h.ops.calls, ","))
	}
}

// TestASwitchSavedBeforeReverseRanDoesNotSkipIt: a controller that ran
// reverse after the sequence carry may have saved a switch at "sequences".
// Resumed under the order that runs reverse first, going on would complete
// the switch with no reverse replication, and so no rollback.
func TestASwitchSavedBeforeReverseRanDoesNotSkipIt(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	h.ops.fail[StepVerify] = errors.New("boom")
	if err := h.parkAt(t, StepVerify); err == nil {
		t.Fatal("the verify step must surface its error")
	}
	h.wf.cutover.Step, h.wf.cutover.Schema = StepSequences, nil
	before := len(h.ops.calls)
	h.runUntil(t, StageSwitched)
	calls := strings.Join(h.ops.calls[before:], ",")
	journal := strings.Index(calls, StepJournal)
	if journal < 0 || !strings.Contains(calls[:journal], StepReverse) || !strings.Contains(calls[:journal], "pause_sources") {
		t.Fatalf("a switch resumed at the sequences before reverse ran must run reverse and quiesce before the journal: %s", calls)
	}
	if h.wf.cutover.Schema == nil {
		t.Fatal("no schema fingerprints were taken, so a rollback could not check drift")
	}
}

// TestASwapThatRaisedAPauseItCouldNotDrainLiftsIt: when the pause did not
// stand into the swap, the swap raises its own; if that one cannot drain,
// the next pass must not take it for a pause that stood and skip the wait.
func TestASwapThatRaisedAPauseItCouldNotDrainLiftsIt(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	h.ops.fail[StepSwap] = errors.New("boom")
	if err := h.parkAt(t, StepSwap); err == nil {
		t.Fatal("the swap step must surface its error")
	}
	h.ops.pauseLifted, h.ops.paused = true, false
	h.ops.openWriters = 1
	h.ops.fail["drain_sources"] = errors.New("1 write transactions on default still open after 30s")
	if err := h.parkAt(t, StepSwap); !errors.Is(err, errRetry) {
		t.Fatalf("a swap that could not drain gave up with %v, want a retry", err)
	}
	if h.ops.paused {
		t.Fatal("a pause the swap raised and could not drain was left standing for the next pass to trust")
	}
	h.runUntil(t, StageSwitched)
	if h.ops.lostWrite {
		t.Fatal("a writer was still open when forward replication went away")
	}
}

// TestAFlipWhosePauseDidNotStandQuiescesAgain: the drain at quiesce speaks
// for a pause that has stood since. One lifted in between -- by hand, or by a
// controller that resumed past a quiesce it never ran -- is raised and
// drained again before the targets serve.
func TestAFlipWhosePauseDidNotStandQuiescesAgain(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	h.ops.fail[StepJournal] = errors.New("boom")
	if err := h.parkAt(t, StepJournal); err == nil {
		t.Fatal("the journal step must surface its error")
	}
	h.ops.pauseLifted = true
	h.ops.openWriters = 1
	before := len(h.ops.calls)
	h.runUntil(t, StageSwitched)
	calls := strings.Join(h.ops.calls[before:], ",")
	flip := strings.Index(calls, StepFlip)
	if flip < 0 || !strings.Contains(calls[:flip], "pause_sources,drain_sources") {
		t.Fatalf("a flip whose pause did not stand must pause and drain again first: %s", calls)
	}
	if h.ops.writeAfterFlip {
		t.Fatal("a writer begun while the pause was down was still open at the flip")
	}
}

// TestSwapKeepsTheSourcesPausedWhenItCannotFinish: the sources are retired
// once the flip is published, and one made writable between attempts is one
// a router that has not reloaded can commit on while its forward
// subscription is being disabled. A run that stops here keeps them paused;
// WritePauseSweep lifts the claimed pause once the workflow ends.
func TestSwapKeepsTheSourcesPausedWhenItCannotFinish(t *testing.T) {
	h := newCutoverHarness(t)
	// The catch-up step, quiesce and the flip ask first; the swap's own
	// check is the fourth, and that is the one this parks on.
	h.ops.caughtUpUntil = 3
	h.runUntil(t, StageSwitching)
	var err error
	for i := 0; h.wf.cutover.Step != StepSwap || err == nil; i++ {
		if i > 50 {
			t.Fatalf("never parked at %s; at %s (%v)", StepSwap, h.wf.cutover.Step, err)
		}
		_, err = h.c.cutover(context.Background(), h.wf, h.ops)
	}
	if !errors.Is(err, errRetry) {
		t.Fatalf("swap gave up with %v, want a retry", err)
	}
	if !h.ops.paused {
		t.Fatal("the swap lifted the pause on the retired sources while it could not finish")
	}
}

// TestSwapCarriesSequencesInsideThePause: sequence positions are not
// replicated. Between the carry before the flip and the swap, a router
// that had not reloaded could still call nextval on a source: the row it
// writes reaches the targets, the sequence position does not, and the
// targets hand the same value out again. The carry has to run once more
// where nothing can consume a value after it.
func TestSwapCarriesSequencesInsideThePause(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitched)
	calls := strings.Join(h.ops.calls, ",")
	swap := strings.Index(calls, StepSwap)
	pause := strings.LastIndex(calls[:swap], "pause_sources")
	if swap < 0 || pause < 0 {
		t.Fatalf("calls %s", calls)
	}
	if !strings.Contains(calls[pause:swap], StepSequences) {
		t.Fatalf("the sequences must be carried again inside the pause: %s", calls[pause:swap])
	}
	if got := strings.Count(calls, StepSequences); got < 2 {
		t.Fatalf("sequences carried %d times, want the pre-flip carry and the one at the swap", got)
	}
}

// TestNoAbortOnceTheJournalIsWritten: the journal is the point of no
// return -- its rows tell every consumer of the change stream that the
// cutover happened, and nothing retracts them. A step after it that fails
// with the fence long past its timeout is retried, never undone.
func TestNoAbortOnceTheJournalIsWritten(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	h.ops.fail[StepFlip] = errors.New("catalog unreachable")
	h.clock = h.clock.Add(4 * DefaultCutoverTimeout)
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err == nil {
		t.Fatal("the failing step must surface its error")
	}
	if h.wf.cutover.JournalID == "" || h.wf.cutover.Step != StepFlip {
		t.Fatalf("step %s journal %q: the run did not fail past the journal, so this does not test what it says", h.wf.cutover.Step, h.wf.cutover.JournalID)
	}
	if len(h.wf.cutover.Aborts) != 0 {
		t.Fatalf("the switch was undone after the journal: %v", h.wf.cutover.Aborts)
	}
	if !h.ops.fenced {
		t.Error("the fence was released after the journal")
	}

	// It recovers by retrying, not by going back to the gate.
	h.runUntil(t, StageSwitched)
}

// TestASwitchWhoseSourceWasRetiredEndsInsteadOfRetrying: after the journal
// every error is retried, because the journal is the point of no return.
// A source set another workflow already retired is the exception -- the
// flip can never publish on top of it -- and retrying held the run's
// slots, and the sources' WAL with them, for ever.
func TestASwitchWhoseSourceWasRetiredEndsInsteadOfRetrying(t *testing.T) {
	h := newCutoverHarness(t)
	h.c.CutoverTimeout = time.Second
	h.ops.fail[StepFlip] = sourceRetired("default no longer serves; another workflow published [g3]")
	h.runUntil(t, StageSwitching)

	advanced, err := h.c.cutover(context.Background(), h.wf, h.ops)
	if err != nil {
		t.Fatalf("an abandoned switch ends the workflow, it does not error the pass: %v", err)
	}
	if !advanced || h.wf.stage != StageFailed {
		t.Fatalf("stage %s advanced %v, want the workflow finished", h.wf.stage, advanced)
	}
	if !strings.Contains(h.store.finished, StateFailed) || !strings.Contains(h.store.finished, "no longer serves") {
		t.Fatalf("the operator must see why it ended: %q", h.store.finished)
	}
	if h.ops.fenced {
		t.Fatal("the fence outlived the switch that raised it")
	}
	for _, want := range []string{"drop_journal", "complete"} {
		if !slices.Contains(h.ops.calls, want) {
			t.Fatalf("%s never ran, so the run's replication objects are still there: %v", want, h.ops.calls)
		}
	}
	if len(h.ops.journaled) != 0 {
		t.Fatalf("journal rows point consumers at a set that will never serve: %v", h.ops.journaled)
	}
}

// TestAConfiguredPauseIsRecordedWithItsOwnClock: pauseBefore holds a
// workflow that stays running, and every pass rewrites updated_at, so
// nothing on the row said which pause was holding it or for how long.
func TestAConfiguredPauseIsRecordedWithItsOwnClock(t *testing.T) {
	h := newCutoverHarness(t)
	h.wf.spec.PauseBefore = PauseSwitchWrites
	h.runUntil(t, StageAwaitingSwitch)
	h.pass(t)
	if h.wf.cutover.Pause != PauseSwitchWrites || h.wf.cutover.PausedAt == nil {
		t.Fatalf("the pause holding the workflow is not recorded: %+v", h.wf.cutover)
	}
	began := *h.wf.cutover.PausedAt

	h.clock = h.clock.Add(time.Hour)
	h.pass(t)
	if h.wf.cutover.PausedAt == nil || !h.wf.cutover.PausedAt.Equal(began) {
		t.Fatalf("observing the same pause again restarted its clock: %v, began %v", h.wf.cutover.PausedAt, began)
	}
	if h.wf.stage != StageAwaitingSwitch {
		t.Fatalf("stage %s: a pause must hold the workflow at the gate", h.wf.stage)
	}

	h.wf.spec.Proceed = []string{PauseSwitchWrites}
	h.pass(t)
	if h.wf.cutover.Pause != "" || h.wf.cutover.PausedAt != nil {
		t.Fatalf("a workflow let through still reports a pause: %+v", h.wf.cutover)
	}
}

// TestAStalledPostJournalStepSaysSoInTheCatalog: after the journal a step
// is retried without a timeout or an attempt limit, and the pass that fails
// used to save nothing, so the workflow read as recently updated and
// perfectly healthy while writes stayed fenced.
func TestAStalledPostJournalStepSaysSoInTheCatalog(t *testing.T) {
	h := newCutoverHarness(t)
	h.c.CutoverTimeout = time.Second
	h.runUntil(t, StageSwitching)
	h.ops.fail[StepFlip] = errors.New("catalog down")

	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err == nil {
		t.Fatal("a failing flip must still report the error")
	}
	if h.wf.cutover.StepRetries != 1 {
		t.Fatalf("retries = %d, want the failed pass counted", h.wf.cutover.StepRetries)
	}
	last := h.store.saves[len(h.store.saves)-1]
	if !strings.Contains(last, "step flip failed 1 time(s)") || !strings.Contains(last, "catalog down") {
		t.Fatalf("a failed post-journal pass must say so in the status: %q", last)
	}
	if h.wf.cutover.stalled(h.clock) {
		t.Fatal("a step that just failed once is not stalled yet")
	}

	h.clock = h.clock.Add(stalledAfter)
	h.ops.fail[StepFlip] = errors.New("catalog down")
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err == nil {
		t.Fatal("a failing flip must still report the error")
	}
	if !h.wf.cutover.stalled(h.clock) {
		t.Fatal("a step failing for the whole stall window is stalled")
	}
	last = h.store.saves[len(h.store.saves)-1]
	if !strings.Contains(last, "has not advanced for") || !strings.Contains(last, "step flip failed 2 time(s)") {
		t.Fatalf("a stalled step must report its age and its retries: %q", last)
	}

	// Advancing resets both: the next step's age is its own.
	h.clock = h.clock.Add(time.Minute)
	h.runUntil(t, StageSwitched)
	if h.wf.cutover.StepRetries != 0 {
		t.Fatalf("retries = %d after the step advanced", h.wf.cutover.StepRetries)
	}
}

// TestTheVerifyReportKeepsItsStoredKey: CheckedAt had no JSON tag, so it
// serialised under its Go field name among snake_case siblings and the
// admin had to mirror that accident to read it. Rows exist carrying the
// key, so the tag declares what is already stored rather than changing
// it -- and this test is what would fail if someone tidied the casing
// without migrating them.
func TestTheVerifyReportKeepsItsStoredKey(t *testing.T) {
	when := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	b, err := json.Marshal(VerifyReport{Tables: 2, Rows: 64, CheckedAt: when})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["CheckedAt"]; !ok {
		t.Fatalf("stored keys %v: rows exist with CheckedAt, and the admin reads that name", keysOf(raw))
	}
	// And it still reads back, which a renamed key would not from an old row.
	var back VerifyReport
	if err := json.Unmarshal([]byte(`{"tables":2,"rows":64,"CheckedAt":"2026-09-01T12:00:00Z"}`), &back); err != nil {
		t.Fatal(err)
	}
	if !back.CheckedAt.Equal(when) {
		t.Fatalf("CheckedAt read back as %v, want %v", back.CheckedAt, when)
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestAPausedCopyDoesNotEnterCutover: the throttle pauses by DISABLING the
// subscriptions, and it runs only for the copy stages.
// awaiting_switch_writes is a cutover stage, so advancing while paused is a
// one-way door -- nothing re-evaluates the watermark, nothing re-enables
// what was disabled, and the switch gate then rejects the very
// subscriptions the throttle turned off. A transient replica-lag spike at
// the catch-up boundary would strand an otherwise healthy reshard until
// someone repaired catalog state by hand.
func TestAPausedCopyDoesNotEnterCutover(t *testing.T) {
	h := newCutoverHarness(t)
	h.wf.copy.Paused = true

	if h.pass(t) {
		t.Fatal("a paused copy must not advance out of catch_up_done")
	}
	if h.wf.stage != StageCatchUpDone {
		t.Fatalf("stage %s, want the workflow held where the throttle can still see it", h.wf.stage)
	}
	if len(h.store.saves) == 0 || !strings.Contains(h.store.saves[len(h.store.saves)-1], "watermark") {
		t.Errorf("the hold must say why: %v", h.store.saves)
	}

	// Once the lag recovers the throttle clears the pause, and the next
	// pass proceeds exactly as it would have.
	h.wf.copy.Paused = false
	if !h.pass(t) {
		t.Fatal("an unpaused copy must advance")
	}
	if h.wf.stage != StageAwaitingSwitch {
		t.Fatalf("stage %s, want %s", h.wf.stage, StageAwaitingSwitch)
	}
}

// TestVerifyIsBoundedByWhatIsLeftOfTheFence: the digests are full scans and
// they run with writes already fenced, so the scan is the write outage.
// Bounding the pass by the fence's remaining budget turns an oversized table
// into a cancelled scan and an aborted switch, rather than an outage that
// lasts as long as the scan does.
func TestVerifyIsBoundedByWhatIsLeftOfTheFence(t *testing.T) {
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	now := base
	c := &Copier{CutoverTimeout: 60 * time.Second, Now: func() time.Time { return now }}

	t.Run("no fence yet is not bounded", func(t *testing.T) {
		o := &pgCutover{c: c, wf: &copyWorkflow{}}
		if _, ok := o.fenceRemaining(); ok {
			t.Fatal("nothing is fenced, so there is no fence to bound")
		}
	})

	t.Run("past the journal is not bounded", func(t *testing.T) {
		fenced := base
		o := &pgCutover{c: c, wf: &copyWorkflow{cutover: cutoverState{FencedAt: &fenced, JournalID: "j"}}}
		if _, ok := o.fenceRemaining(); ok {
			t.Fatal("past the journal there is no going back, so no deadline to enforce")
		}
	})

	t.Run("the budget is what the fence has left", func(t *testing.T) {
		fenced := base
		o := &pgCutover{c: c, wf: &copyWorkflow{cutover: cutoverState{FencedAt: &fenced}}}
		now = base.Add(20 * time.Second)
		got, ok := o.fenceRemaining()
		if !ok || got != 40*time.Second {
			t.Fatalf("remaining = %v ok=%t, want 40s of a 60s fence held for 20s", got, ok)
		}
	})

	t.Run("already over budget still runs, and briefly", func(t *testing.T) {
		fenced := base
		o := &pgCutover{c: c, wf: &copyWorkflow{cutover: cutoverState{FencedAt: &fenced}}}
		now = base.Add(90 * time.Second)
		got, ok := o.fenceRemaining()
		if !ok || got <= 0 {
			t.Fatalf("remaining = %v ok=%t: an over-budget fence must still send the query, so the "+
				"step fails on the deadline check rather than on a context that was dead already", got, ok)
		}
	})
}

// TestCutoverResumesAfterForwardReplicationIsDisabled covers the one instant
// in StepSwap where the step has passed its point of no return and recorded
// nothing: between DisableForward and the unpause that follows it.
//
// Re-running the step from the top cannot work there. Disabling the forward
// subscriptions freezes their slots' confirmed_flush_lsn while the sources'
// WAL keeps moving on its own, so CaughtUp compares a standing position
// against a moving one and reads as behind for good. The step would retry
// that comparison forever and never reach EnableReverse -- leaving the
// targets serving with no reverse replication, which is to say with the
// rollback path gone, while the workflow reported itself as retrying.
func TestCutoverResumesAfterForwardReplicationIsDisabled(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.crashAfterDisable = true
	// The controller's own loop records a failed step and comes back, so a
	// pass that errors is survivable here in the way the harness's pass is
	// not: the crash is the point of the test, not its outcome.
	var crashes int
	for i := 0; i < 50 && h.wf.stage != StageSwitched; i++ {
		if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err != nil {
			crashes++
		}
	}
	if h.wf.stage != StageSwitched {
		t.Fatalf("never switched; at %s/%s after %d crash(es): %s", h.wf.stage, h.wf.cutover.Step, crashes, strings.Join(h.ops.calls, ","))
	}
	if crashes != 1 {
		t.Fatalf("want exactly the one injected crash, got %d", crashes)
	}

	if !h.ops.forwardDisabled {
		t.Fatal("forward replication was never disabled, so the test never reached the window it is about")
	}
	if h.ops.caughtUp {
		t.Fatal("the fake stayed caught up after the disable, so the resume was never asked the hard question")
	}
	if h.ops.paused {
		t.Fatal("the sources were left paused")
	}
	var enables int
	for _, c := range h.ops.calls {
		if c == "enable_reverse" {
			enables++
		}
	}
	if enables != 1 {
		t.Fatalf("reverse replication enabled %d times, want exactly 1: %s", enables, strings.Join(h.ops.calls, ","))
	}
	// The resume must not re-ask the question the disable made unanswerable.
	after := h.ops.calls[indexOfLast(t, h.ops.calls, "forward_disabled"):]
	for _, c := range after {
		if c == StepCatchUp {
			t.Fatalf("resume re-ran the catch-up check it can never satisfy: %s", strings.Join(after, ","))
		}
	}
}

func indexOfLast(t *testing.T, calls []string, name string) int {
	t.Helper()
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i] == name {
			return i
		}
	}
	// Returning 0 here would widen the caller's slice to everything and
	// quietly turn a missing call into a passing assertion.
	t.Fatalf("%q was never called: %s", name, strings.Join(calls, ","))
	return -1
}

// TestCutoverPartialDisableResumesUnderTheStandingPause is the other way
// into the same window.
//
// DisableForward walks databases and targets one at a time, and a later one
// can fail with forward replication already half off. The sources must not
// be writable at any point from there until it is all off: a stale router's
// commit in that window is acknowledged into a slot Complete is about to
// drop. The step's error path used to unpause them and rely on the resume
// raising a fresh pause; it now keeps the pause, and the resume works under
// it.
func TestCutoverPartialDisableResumesUnderTheStandingPause(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.partialDisable = true
	var errs int
	for i := 0; i < 50 && h.wf.stage != StageSwitched; i++ {
		if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err != nil {
			errs++
		}
	}
	if h.wf.stage != StageSwitched {
		t.Fatalf("never switched; at %s/%s: %s", h.wf.stage, h.wf.cutover.Step, strings.Join(h.ops.calls, ","))
	}
	if errs != 1 {
		t.Fatalf("want the one injected failure, got %d", errs)
	}
	if h.ops.unpausedHalfDisabled {
		t.Fatalf("the sources were made writable with forward replication half disabled: %s", strings.Join(h.ops.calls, ","))
	}
	after := h.ops.calls[indexOfLast(t, h.ops.calls, "forward_disabled"):]
	for _, want := range []string{"drain_sources", StepSequences} {
		if !slices.Contains(after, want) {
			t.Fatalf("resume skipped %s: %s", want, strings.Join(after, ","))
		}
	}
	// The pair it may skip, and must: the disabled half has frozen.
	if slices.Contains(after, StepCatchUp) {
		t.Fatalf("resume re-ran the catch-up check it can never satisfy: %s", strings.Join(after, ","))
	}
	if h.ops.lostWrite {
		t.Fatal("a writer was still open when forward replication went away")
	}
	if h.ops.paused {
		t.Fatal("the sources were left paused")
	}
}

// TestCutoverDoesNotSkipTheCheckForSomeoneElsesDisable pins the second half
// of the resume condition.
//
// A subscription an operator disabled by hand looks exactly like one this
// run disabled, and treating it as a resume would finalize the cutover with
// that subscription's changes never applied -- a silent loss where the old
// behaviour was a visible stall. Only the run that recorded that it was
// disabling may skip the catch-up check.
func TestCutoverDoesNotSkipTheCheckForSomeoneElsesDisable(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.outsideDisable = true
	// The stall shows up as a retryable error every pass, which is the
	// point: it is loud, and it is what this test is protecting.
	for i := 0; i < 20 && h.wf.stage != StageSwitched; i++ {
		_, _ = h.c.cutover(context.Background(), h.wf, h.ops)
	}
	if h.wf.stage == StageSwitched {
		t.Fatalf("the switch finished over a subscription nobody checked: %s", strings.Join(h.ops.calls, ","))
	}
	if h.wf.cutover.DisablingAt != nil {
		t.Fatal("the run recorded a disable it never made")
	}
	if !slices.Contains(h.ops.calls[indexOfLast(t, h.ops.calls, "forward_disabled"):], StepCatchUp) {
		t.Fatalf("the catch-up check was skipped for a disable this run did not make: %s", strings.Join(h.ops.calls, ","))
	}
}
