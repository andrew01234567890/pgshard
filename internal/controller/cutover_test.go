package controller

import (
	"context"
	"errors"
	"fmt"
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
	calls     []string
	gateOpen  bool
	gateWhy   string
	drain     []string
	sweepErr  error
	caughtUp  bool
	verify    VerifyReport
	fail      map[string]error
	fenced    bool
	journaled map[string]int
	lsn       int64
	advance   int64
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
	f.lsn += f.advance
	return map[string]int64{"0": f.lsn}, f.step(StepPositions)
}
func (f *fakeOps) CaughtUp(_ context.Context, pos map[string]int64) (bool, string, error) {
	if pos["0"] != f.lsn {
		return false, "", fmt.Errorf("caught-up check against stale position %d, current %d", pos["0"], f.lsn)
	}
	return f.caughtUp, "lagging", f.step(StepCatchUp)
}
func (f *fakeOps) Verify(context.Context) (VerifyReport, error) { return f.verify, f.step(StepVerify) }
func (f *fakeOps) Reverse(context.Context) error                { return f.step(StepReverse) }
func (f *fakeOps) Journal(_ context.Context, id string) error {
	f.journaled[id]++
	return f.step(StepJournal)
}
func (f *fakeOps) Flip(context.Context, string) error { return f.step(StepFlip) }
func (f *fakeOps) Swap(context.Context) error         { return f.step(StepSwap) }
func (f *fakeOps) Release(context.Context) error      { f.fenced = false; return f.step(StepRelease) }
func (f *fakeOps) Complete(context.Context) error     { return f.step("complete") }

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
	want := []string{"gate", StepFence, StepDrain, StepSweep, StepPositions, StepCatchUp, StepPositions, StepVerify, StepReverse, StepJournal,
		StepPositions, StepCatchUp, StepFlip, StepPositions, StepCatchUp, StepSwap, StepRelease}
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

func TestCutoverFlipWaitsForLateSourceWrites(t *testing.T) {
	h := newCutoverHarness(t)
	h.runUntil(t, StageSwitching)
	boom := errors.New("boom")
	h.ops.fail[StepJournal] = boom
	if _, err := h.c.cutover(context.Background(), h.wf, h.ops); !errors.Is(err, boom) {
		t.Fatalf("err %v", err)
	}
	h.ops.advance = 10
	for i := 0; i < 3; i++ {
		_, err := h.c.cutover(context.Background(), h.wf, h.ops)
		if err == nil || isFatal(err) {
			t.Fatalf("pass %d: err %v", i, err)
		}
		if h.wf.cutover.Step != StepFlip || strings.Contains(strings.Join(h.ops.calls, ","), StepFlip) {
			t.Fatalf("pass %d: flip must not run while the sources move (step %s, calls %v)", i, h.wf.cutover.Step, h.ops.calls)
		}
	}
	h.ops.advance = 0
	h.runUntil(t, StageSwitched)
	if h.wf.cutover.Positions["0"] != h.ops.lsn {
		t.Fatalf("positions %v, source at %d", h.wf.cutover.Positions, h.ops.lsn)
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
