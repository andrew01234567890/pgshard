package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	fail            map[string]error
	fenced          bool
	journaled       map[string]int
	lsn             int64
	advance         int64
	paused          bool
	pauses          int
	// caughtUpUntil, when set, makes CaughtUp report behind from that call
	// onwards, so a test can park the run at a chosen check.
	caughtUpUntil int
	caughtUpCalls int
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
	return map[string]int64{"0": f.lsn}, f.step(StepPositions)
}
func (f *fakeOps) CaughtUp(_ context.Context, pos map[string]int64) (bool, string, error) {
	if pos["0"] != f.lsn {
		return false, "", fmt.Errorf("caught-up check against stale position %d, current %d", pos["0"], f.lsn)
	}
	f.caughtUpCalls++
	if f.caughtUpUntil > 0 && f.caughtUpCalls > f.caughtUpUntil {
		return false, "lagging", f.step(StepCatchUp)
	}
	return f.caughtUp, "lagging", f.step(StepCatchUp)
}
func (f *fakeOps) Verify(context.Context) (VerifyReport, error) { return f.verify, f.step(StepVerify) }
func (f *fakeOps) Sequences(context.Context) error              { return f.step(StepSequences) }
func (f *fakeOps) Reverse(context.Context) error                { return f.step(StepReverse) }
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
func (f *fakeOps) Flip(context.Context, string) error   { return f.step(StepFlip) }
func (f *fakeOps) DisableForward(context.Context) error { return f.step(StepSwap) }
func (f *fakeOps) EnableReverse(context.Context) error  { return f.step("enable_reverse") }

// PauseSources records the pause and, while paused, stops the fake source
// advancing: that is what a write pause buys, and a test that keeps the
// source moving through it is not testing the pause.
func (f *fakeOps) PauseSources(_ context.Context, pause bool) error {
	if err := f.step("pause_sources"); err != nil {
		return err
	}
	f.paused = pause
	if pause {
		f.pauses++
	}
	return nil
}
func (f *fakeOps) DropJournal(_ context.Context, id string) error {
	delete(f.journaled, id)
	return f.step("drop_journal")
}
func (f *fakeOps) Release(context.Context) error  { f.fenced = false; return f.step(StepRelease) }
func (f *fakeOps) Complete(context.Context) error { return f.step("complete") }
func (f *fakeOps) Rollback(context.Context) error {
	if err := f.step("rollback"); err != nil {
		return err
	}
	if !f.reverseCaughtUp {
		return retryf("reverse replication behind")
	}
	return nil
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
	want := []string{"gate", StepFence, StepDrain, StepSweep, StepPositions, StepCatchUp, StepPositions, StepVerify, StepSequences, StepReverse, StepJournal,
		StepPositions, StepCatchUp, StepFlip,
		"pause_sources", StepPositions, StepCatchUp, StepSequences, StepSwap, "pause_sources", "enable_reverse", StepRelease}
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

func TestCutoverWaitsUntilSourcesStandStill(t *testing.T) {
	h := newCutoverHarness(t)
	h.ops.advance = 10
	h.runUntil(t, StageSwitching)
	for i := 0; i < 3; i++ {
		h.pass(t)
		if h.wf.cutover.Step != StepCatchUp {
			t.Fatalf("pass %d: step %s, want catch_up while the sources move", i, h.wf.cutover.Step)
		}
	}
	h.ops.advance = 0
	h.runUntil(t, StageSwitched)
	if h.wf.cutover.Positions["0"] != h.ops.lsn {
		t.Fatalf("positions %v, source at %d", h.wf.cutover.Positions, h.ops.lsn)
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
	_, err := h.c.cutover(context.Background(), h.wf, h.ops)
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

func TestCutoverCrashAtEveryStepResumesIdempotently(t *testing.T) {
	for _, crashAt := range switchSteps {
		t.Run(crashAt, func(t *testing.T) {
			h := newCutoverHarness(t)
			h.runUntil(t, StageSwitching)
			crashed := errors.New("crash")
			h.ops.fail[crashAt] = crashed
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
				if !strings.Contains(strings.Join(h.ops.calls, ","), step) {
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

// TestCutoverFlipRecarriesSequencesThenFlipsAnyway: movement at the flip
// sends the switch back to re-carry sequences, but only so many times. A
// source is never obliged to stand still -- a checkpoint or an autovacuum
// moves pg_current_wal_lsn with no user write behind it -- and after the
// journal there is no timeout to end the wait, so requiring stillness is
// requiring something the source may never do. A real 18-to-19 upgrade sat
// in exactly this state for the whole 30-minute e2e budget, reporting
// "sources advanced past the recorded positions before the flip".
func TestCutoverFlipRecarriesSequencesThenFlipsAnyway(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	boom := errors.New("boom")
	h.ops.fail[StepJournal] = boom
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); !errors.Is(err, boom) {
		t.Fatalf("err %v", err)
	}
	// The source never stops moving, not even between the journal and the
	// flip.
	h.ops.advance = 10
	for i := range maxSeqRecarries {
		_, err := h.c.cutover(context.Background(), h.wf, h.ops)
		if err == nil || isFatal(err) {
			t.Fatalf("pass %d: err %v", i, err)
		}
		if h.wf.cutover.Step != StepSequences || strings.Contains(strings.Join(h.ops.calls, ","), StepFlip) {
			t.Fatalf("pass %d: movement at the flip must jump back to the sequence carry (step %s, calls %v)", i, h.wf.cutover.Step, h.ops.calls)
		}
	}
	sequenceRuns := strings.Count(strings.Join(h.ops.calls, ","), StepSequences)
	h.runUntil(t, StageSwitched)
	if got := strings.Count(strings.Join(h.ops.calls, ","), StepSequences); got <= sequenceRuns {
		t.Fatalf("sequences must be re-carried after the sources moved: %d runs before settling, %d after", sequenceRuns, got)
	}
	if h.wf.cutover.Recarries != maxSeqRecarries {
		t.Fatalf("re-carries %d, want the bound %d", h.wf.cutover.Recarries, maxSeqRecarries)
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

// TestSwapLeavesTheSourcesWritableWhenItCannotFinish: a workflow that stops
// at the swap must not leave the sources refusing writes for good.
func TestSwapLeavesTheSourcesWritableWhenItCannotFinish(t *testing.T) {
	h := newCutoverHarness(t)
	// The catch-up step and the flip ask first; the swap's own check is the
	// third, and that is the one this parks on.
	h.ops.caughtUpUntil = 2
	h.runUntil(t, StageSwitching)
	var err error
	for i := 0; h.wf.cutover.Step != StepSwap || err == nil; i++ {
		if i > 50 {
			t.Fatalf("never parked at %s; at %s (%v)", StepSwap, h.wf.cutover.Step, err)
		}
		_, err = h.c.cutover(context.Background(), h.wf, h.ops)
	}
	// After the journal a step that cannot finish is reported rather than
	// silently retried, which is what parks the run here.
	if !errors.Is(err, errRetry) {
		t.Fatalf("swap gave up with %v, want a retry", err)
	}
	if h.ops.paused {
		t.Fatal("the sources were left refusing writes after the swap gave up")
	}
	if h.ops.pauses == 0 {
		t.Fatal("the swap must have paused them in the first place")
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
// cutover happened, and nothing retracts them. The abort was gated on the
// step cursor, and StepFlip rewinds that cursor back to StepSequences when
// the sources moved before the flip, so an error on the way forward again
// found itself "before the journal" with the fence long past its timeout
// and undid a switch the sources had already announced.
func TestNoAbortOnceTheJournalIsWritten(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	// Park the run at the journal, then let it through with the sources
	// moving so the flip rewinds to the sequence carry.
	h.ops.fail[StepJournal] = errors.New("boom")
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err == nil {
		t.Fatal("the journal step must surface its error")
	}
	delete(h.ops.fail, StepJournal)
	h.ops.advance = 10
	// The rewind reports itself as a retry, which pass would treat as
	// fatal, so the pass is driven directly here.
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err == nil || isFatal(err) {
		t.Fatalf("movement at the flip must ask for a retry, got %v", err)
	}
	if h.wf.cutover.Step != StepSequences {
		t.Fatalf("step %s, want the rewind to the sequence carry", h.wf.cutover.Step)
	}
	if h.wf.cutover.JournalID == "" {
		t.Fatal("the journal was never written, so this does not test what it says")
	}

	// A step now fails with the fence well past its timeout: the old gate
	// would have aborted here, because the cursor is before the journal.
	h.ops.advance = 0
	h.ops.fail[StepSequences] = errors.New("target unreachable")
	h.clock = h.clock.Add(4 * DefaultCutoverTimeout)
	aborts := len(h.wf.cutover.Aborts)
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err == nil {
		t.Fatal("the failing step must surface its error")
	}
	if len(h.wf.cutover.Aborts) != aborts {
		t.Fatalf("the switch was undone after the journal: %v", h.wf.cutover.Aborts)
	}
	if !h.ops.fenced {
		t.Error("the fence was released after the journal")
	}

	// It recovers by retrying, not by going back to the gate.
	delete(h.ops.fail, StepSequences)
	h.runUntil(t, StageSwitched)
}

// TestJournalRefreshesItsTargetsAfterARewind: the journal's targets are
// where a consumer repositions to. A flip that rewinds to re-carry the
// sequences comes back through the journal, and the row must describe the
// attempt that actually flipped -- leaving the first attempt's positions
// starts a consumer before the cutover it is repositioning to.
func TestJournalRefreshesItsTargetsAfterARewind(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	h.ops.fail[StepJournal] = errors.New("boom")
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err == nil {
		t.Fatal("the journal step must surface its error")
	}
	delete(h.ops.fail, StepJournal)
	h.ops.advance = 10
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); err == nil || isFatal(err) {
		t.Fatalf("movement at the flip must ask for a retry, got %v", err)
	}
	first := h.ops.journaled[h.wf.cutover.JournalID]
	if first == 0 {
		t.Fatal("the journal was never written")
	}
	h.ops.advance = 0
	h.runUntil(t, StageSwitched)
	if again := h.ops.journaled[h.wf.cutover.JournalID]; again <= first {
		t.Errorf("the journal was written %d times and not again after the rewind", again)
	}
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
