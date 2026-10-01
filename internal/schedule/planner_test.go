package schedule

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type harness struct {
	store    journal.Store
	recorder *journal.Recorder
	gates    *gate.State
	signals  *signal.Store
	queue    *admission.Queue
	cycles   *cycle.Coordinator
	planner  *Planner
	now      time.Time
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	clock := func() time.Time { return time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC) }
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, clock)
	gates := gate.NewState("chamber-1")
	commands := gate.NewCommands()
	gateController := gate.NewController(gates, commands, recorder, clock)
	signals := signal.NewStore()
	queue := admission.NewQueue()
	controller := admission.NewController(queue, gates, signals, clock)
	admissionService := admission.NewService(controller)
	hydraulicsController := hydraulics.NewController(commands, recorder, hydraulics.NewRamp(time.Millisecond, clock), clock)
	coordinator := cycle.NewCoordinator(gateController, hydraulicsController, recorder, clock)
	planner := NewPlanner(admissionService, coordinator, recorder, clock)
	return &harness{
		store: store, recorder: recorder, gates: gates, signals: signals,
		queue: queue, cycles: coordinator, planner: planner, now: clock(),
	}
}

func (h *harness) vessels(n int) []model.Vessel {
	vessels := make([]model.Vessel, n)
	for i := range vessels {
		vessel, err := model.NewVessel("vessel", "CALL", 42)
		if err != nil {
			panic(err)
		}
		vessels[i] = vessel
	}
	return vessels
}

func (h *harness) eventCount(t *testing.T) int {
	t.Helper()
	events, err := h.store.Events(context.Background(), "chamber-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	return len(events)
}

func (h *harness) unseal() {
	if err := h.gates.Observe("chamber-1", model.DirectionUpstream, model.GateOpen, h.now); err != nil {
		panic(err)
	}
}

func TestSubmitRejectedWhenChamberBusyLeavesNoTrace(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	first, firstCycle, err := h.planner.Submit(ctx, "chamber-1", model.DirectionUpstream, h.vessels(2))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}

	_, _, err = h.planner.Submit(ctx, "chamber-1", model.DirectionDownstream, h.vessels(2))
	if err == nil {
		t.Fatal("second submit must conflict with the active transit")
	}
	if !errors.Is(err, cycle.ErrChamberBusy) {
		t.Fatalf("want ErrChamberBusy, got %v", err)
	}

	// Only the first plan and its queue entries remain visible.
	if active := h.planner.Active("chamber-1"); len(active) != 1 || active[0].ID != first.ID {
		t.Fatalf("active plans = %v, want only first plan", active)
	}
	if entries := h.planner.Queue("chamber-1"); len(entries) != 2 {
		t.Fatalf("queue len = %d, want 2 (rejected plan leaked entries)", len(entries))
	}
	current, ok := h.cycles.Current("chamber-1")
	if !ok || current.ID != firstCycle.ID {
		t.Fatal("current cycle must remain the first transit")
	}
	if states := h.signals.List(); len(states) != 1 {
		t.Fatalf("signal states = %d, want 1 (rejected submit granted a signal)", len(states))
	}
	// Exactly the first submit's three success events were journaled.
	if count := h.eventCount(t); count != 3 {
		t.Fatalf("journaled events = %d, want 3 (rejected submit left success events)", count)
	}
}

func TestConcurrentSubmitExactlyOneWinner(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const contenders = 50
	start := make(chan struct{})
	var wg sync.WaitGroup
	successes := make(chan model.ConvoyPlan, contenders)
	errCount := int32(0)
	var counterMu sync.Mutex
	wg.Add(contenders)
	for i := 0; i < contenders; i++ {
		go func() {
			defer wg.Done()
			<-start
			plan, _, err := h.planner.Submit(ctx, "chamber-1", model.DirectionUpstream, h.vessels(2))
			if err != nil {
				counterMu.Lock()
				errCount++
				counterMu.Unlock()
				return
			}
			successes <- plan
		}()
	}
	close(start)
	wg.Wait()
	close(successes)

	var winners []model.ConvoyPlan
	for plan := range successes {
		winners = append(winners, plan)
	}
	if len(winners) != 1 {
		t.Fatalf("successful submits = %d, want exactly 1", len(winners))
	}
	if int(errCount) != contenders-1 {
		t.Fatalf("rejected submits = %d, want %d", errCount, contenders-1)
	}
	if active := h.planner.Active("chamber-1"); len(active) != 1 {
		t.Fatalf("active plans = %d, want 1", len(active))
	}
	if entries := h.planner.Queue("chamber-1"); len(entries) != 2 {
		t.Fatalf("queue len = %d, want 2", len(entries))
	}
	if states := h.signals.List(); len(states) != 1 {
		t.Fatalf("signal states = %d, want 1", len(states))
	}
	if count := h.eventCount(t); count != 3 {
		t.Fatalf("journaled events = %d, want 3", count)
	}
}

func TestSubmitRejectedWhenGatesUnsealedLeavesNoTrace(t *testing.T) {
	h := newHarness(t)
	h.unseal()
	ctx := context.Background()

	_, _, err := h.planner.Submit(ctx, "chamber-1", model.DirectionUpstream, h.vessels(2))
	if err == nil {
		t.Fatal("submit with open gates must be rejected")
	}

	if active := h.planner.Active("chamber-1"); len(active) != 0 {
		t.Fatalf("active plans = %d, want 0", len(active))
	}
	if entries := h.planner.Queue("chamber-1"); len(entries) != 0 {
		t.Fatalf("queue len = %d, want 0", len(entries))
	}
	if _, ok := h.cycles.Current("chamber-1"); ok {
		t.Fatal("rejected submit must roll the staged cycle back")
	}
	if states := h.signals.List(); len(states) != 0 {
		t.Fatalf("signal states = %d, want 0", len(states))
	}
	if count := h.eventCount(t); count != 0 {
		t.Fatalf("journaled events = %d, want 0", count)
	}
}

func TestRejectedSubmitDoesNotBurnGeneration(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	first, firstCycle, err := h.planner.Submit(ctx, "chamber-1", model.DirectionUpstream, h.vessels(2))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if firstCycle.Generation != 1 {
		t.Fatalf("first generation = %d, want 1", firstCycle.Generation)
	}
	if _, _, err := h.planner.Submit(ctx, "chamber-1", model.DirectionUpstream, h.vessels(2)); err == nil {
		t.Fatal("second submit must conflict")
	}
	// Terminate the first transit and clear its queue, as cancellation would.
	if _, err := h.cycles.Cancel(ctx, "chamber-1", "operator cancellation"); err != nil {
		t.Fatal(err)
	}
	h.planner.admission.Abort("chamber-1", first.ID)

	_, nextCycle, err := h.planner.Submit(ctx, "chamber-1", model.DirectionUpstream, h.vessels(2))
	if err != nil {
		t.Fatalf("submit after terminal cycle: %v", err)
	}
	if nextCycle.Generation != 2 {
		t.Fatalf("generation after rejected submit = %d, want 2 (rollback did not restore the counter)", nextCycle.Generation)
	}
}

type failBatchStore struct {
	journal.Store
	fail bool
}

func (f *failBatchStore) AppendBatch(ctx context.Context, events []model.Event) ([]model.Event, error) {
	if f.fail {
		return nil, errors.New("journal disk unavailable")
	}
	return f.Store.AppendBatch(ctx, events)
}

func TestSubmitJournalFailureRollsEverythingBack(t *testing.T) {
	clock := func() time.Time { return time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC) }
	store := &failBatchStore{Store: journal.NewMemoryStore(), fail: true}
	recorder := journal.NewRecorder(store, clock)
	gates := gate.NewState("chamber-1")
	commands := gate.NewCommands()
	gateController := gate.NewController(gates, commands, recorder, clock)
	signals := signal.NewStore()
	queue := admission.NewQueue()
	controller := admission.NewController(queue, gates, signals, clock)
	admissionService := admission.NewService(controller)
	hydraulicsController := hydraulics.NewController(commands, recorder, hydraulics.NewRamp(time.Millisecond, clock), clock)
	coordinator := cycle.NewCoordinator(gateController, hydraulicsController, recorder, clock)
	planner := NewPlanner(admissionService, coordinator, recorder, clock)

	vessel, err := model.NewVessel("vessel", "CALL", 42)
	if err != nil {
		t.Fatal(err)
	}
	vessels := []model.Vessel{vessel}
	_, _, err = planner.Submit(context.Background(), "chamber-1", model.DirectionUpstream, vessels)
	if err == nil {
		t.Fatal("submit must fail when the journal cannot persist events")
	}
	if active := planner.Active("chamber-1"); len(active) != 0 {
		t.Fatalf("active plans = %d, want 0", len(active))
	}
	if entries := queue.List("chamber-1"); len(entries) != 0 {
		t.Fatalf("queue len = %d, want 0", len(entries))
	}
	if _, ok := coordinator.Current("chamber-1"); ok {
		t.Fatal("cycle must be rolled back after journal failure")
	}
	if states := signals.List(); len(states) != 0 {
		t.Fatalf("signal states = %d, want 0 (granted signal must be revoked)", len(states))
	}

	// Once the journal recovers, a submit succeeds at generation 1: the failed
	// attempt left neither durable nor in-memory residue.
	store.fail = false
	plan, cycleValue, err := planner.Submit(context.Background(), "chamber-1", model.DirectionUpstream, vessels)
	if err != nil {
		t.Fatalf("submit after recovery: %v", err)
	}
	if plan.ID == uuid.Nil || cycleValue.Generation != 1 {
		t.Fatalf("post-recovery submit malformed: generation=%d", cycleValue.Generation)
	}
}
