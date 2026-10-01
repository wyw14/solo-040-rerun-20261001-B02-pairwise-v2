package schedule_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/schedule"
	"github.com/wyw14/cry-96/internal/signal"

	"github.com/google/uuid"
)

const chamberID = "chamber-1"

type graph struct {
	store     *journal.MemoryStore
	recorder  *journal.Recorder
	gates     *gate.State
	signals   *signal.Store
	queue     *admission.Queue
	admission *admission.Service
	cycles    *cycle.Coordinator
	planner   *schedule.Planner
	now       func() time.Time
}

func newGraph() *graph {
	now := time.Now
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, now)
	gates := gate.NewState(chamberID) // both gates start closed => sealed
	signals := signal.NewStore()
	q := admission.NewQueue()
	controller := admission.NewController(q, gates, signals, now)
	admissionService := admission.NewService(controller, now)
	cycles := cycle.NewCoordinator(nil, nil, recorder, now)
	planner := schedule.NewPlanner(cycles, admissionService, recorder, now)
	return &graph{
		store: store, recorder: recorder, gates: gates, signals: signals,
		queue: q, admission: admissionService, cycles: cycles, planner: planner, now: now,
	}
}

func vessels(n int) []model.Vessel {
	result := make([]model.Vessel, 0, n)
	for index := 0; index < n; index++ {
		vessel, err := model.NewVessel("vessel", "CALL", 100)
		if err != nil {
			panic(err)
		}
		result = append(result, vessel)
	}
	return result
}

// A successful submit leaves plan, queue, signal and exactly one batch of
// success events, and starts the cycle.
func TestSubmitSuccessIsAtomic(t *testing.T) {
	g := newGraph()
	ctx := context.Background()

	plan, transit, err := g.planner.Submit(ctx, chamberID, model.DirectionUpstream, vessels(2))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if transit.PlanID != plan.ID {
		t.Fatalf("cycle plan mismatch")
	}
	if got := g.planner.Active(chamberID); len(got) != 1 || got[0].ID != plan.ID {
		t.Fatalf("active plans = %v", got)
	}
	if entries := g.queue.List(chamberID); len(entries) != 2 {
		t.Fatalf("queue entries = %d, want 2", len(entries))
	}
	events, err := g.store.Events(ctx, chamberID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3 (plan, cycle, signal)", len(events))
	}
	kinds := []model.EventType{model.EventPlanCreated, model.EventCycleCreated, model.EventSignalChanged}
	for index, kind := range kinds {
		if events[index].Type != kind || events[index].Sequence != uint64(index+1) {
			t.Fatalf("event %d = %s seq %d", index, events[index].Type, events[index].Sequence)
		}
	}
}

// Unsealed gates must reject the request without a plan, queue entry or any
// misleading success event.
func TestSubmitUnsealedLeavesNoTrace(t *testing.T) {
	g := newGraph()
	ctx := context.Background()
	if err := g.gates.Observe(chamberID, model.DirectionUpstream, model.GateOpen, g.now()); err != nil {
		t.Fatal(err)
	}

	_, _, err := g.planner.Submit(ctx, chamberID, model.DirectionUpstream, vessels(1))
	var rejected *admission.ErrNotAdmitted
	if !errors.As(err, &rejected) {
		t.Fatalf("err = %v, want ErrNotAdmitted", err)
	}
	if plans := g.planner.Active(chamberID); len(plans) != 0 {
		t.Fatalf("plans left behind: %d", len(plans))
	}
	if entries := g.queue.List(chamberID); len(entries) != 0 {
		t.Fatalf("queue entries left behind: %d", len(entries))
	}
	events, _ := g.store.Events(ctx, chamberID, 0)
	if len(events) != 0 {
		t.Fatalf("journal events left behind: %d", len(events))
	}
	if states := g.signals.List(); len(states) != 0 {
		t.Fatalf("field signals left behind: %d", len(states))
	}
	if _, ok := g.cycles.Current(chamberID); ok {
		t.Fatalf("active cycle left behind")
	}
}

// Once a transit is running, concurrent submissions serialize behind the
// chamber boundary: exactly one succeeds and every loser leaves nothing.
func TestConcurrentSubmissionsAgainstActiveCycle(t *testing.T) {
	g := newGraph()
	ctx := context.Background()

	original, _, err := g.planner.Submit(ctx, chamberID, model.DirectionUpstream, vessels(1))
	if err != nil {
		t.Fatalf("seed submit: %v", err)
	}

	const contenders = 16
	var wg sync.WaitGroup
	results := make([]error, contenders)
	start := make(chan struct{})
	wg.Add(contenders)
	for index := 0; index < contenders; index++ {
		go func(index int) {
			defer wg.Done()
			<-start
			_, _, results[index] = g.planner.Submit(ctx, chamberID, model.DirectionDownstream, vessels(1))
		}(index)
	}
	close(start)
	wg.Wait()

	for index, result := range results {
		if !errors.Is(result, cycle.ErrChamberActive) {
			t.Fatalf("contender %d err = %v, want ErrChamberActive", index, result)
		}
	}
	if plans := g.planner.Active(chamberID); len(plans) != 1 || plans[0].ID != original.ID {
		t.Fatalf("active plans = %v, want only the original", plans)
	}
	if entries := g.queue.List(chamberID); len(entries) != 1 || entries[0].PlanID != original.ID {
		t.Fatalf("queue = %v, want only the original plan's vessel", entries)
	}
	events, _ := g.store.Events(ctx, chamberID, 0)
	if len(events) != 3 {
		t.Fatalf("journal events = %d, want only the original batch of 3", len(events))
	}
}

// Even racing from an empty chamber, only one of many submissions wins.
func TestConcurrentColdStartSingleWinner(t *testing.T) {
	g := newGraph()
	ctx := context.Background()

	const contenders = 16
	var wg sync.WaitGroup
	results := make([]error, contenders)
	planIDs := make(map[uuid.UUID]int)
	var mu sync.Mutex
	start := make(chan struct{})
	wg.Add(contenders)
	for index := 0; index < contenders; index++ {
		go func(index int) {
			defer wg.Done()
			<-start
			plan, _, err := g.planner.Submit(ctx, chamberID, model.DirectionUpstream, vessels(1))
			results[index] = err
			if err == nil {
				mu.Lock()
				planIDs[plan.ID]++
				mu.Unlock()
			}
		}(index)
	}
	close(start)
	wg.Wait()

	successes := 0
	for _, result := range results {
		if result == nil {
			successes++
		} else if !errors.Is(result, cycle.ErrChamberActive) {
			t.Fatalf("unexpected err %v", result)
		}
	}
	if successes != 1 {
		t.Fatalf("successes = %d, want exactly 1", successes)
	}
	if len(planIDs) != 1 {
		t.Fatalf("distinct winning plans = %d", len(planIDs))
	}
	events, _ := g.store.Events(ctx, chamberID, 0)
	if len(events) != 3 {
		t.Fatalf("journal events = %d, want one batch of 3", len(events))
	}
}

// failingBatchStore fails AppendBatch once, forcing rollback after admission.
type failingBatchStore struct {
	*journal.MemoryStore
	failuresLeft int
}

func (s *failingBatchStore) AppendBatch(ctx context.Context, events []model.Event) ([]model.Event, error) {
	if s.failuresLeft > 0 {
		s.failuresLeft--
		return nil, errors.New("journal unavailable")
	}
	return s.MemoryStore.AppendBatch(ctx, events)
}

// If the success batch cannot be persisted, every staged effect is unwound.
func TestSubmitJournalFailureRollsEverythingBack(t *testing.T) {
	now := time.Now
	store := &failingBatchStore{MemoryStore: journal.NewMemoryStore(), failuresLeft: 1}
	recorder := journal.NewRecorder(store, now)
	gates := gate.NewState(chamberID)
	signals := signal.NewStore()
	q := admission.NewQueue()
	controller := admission.NewController(q, gates, signals, now)
	admissionService := admission.NewService(controller, now)
	cycles := cycle.NewCoordinator(nil, nil, recorder, now)
	planner := schedule.NewPlanner(cycles, admissionService, recorder, now)

	_, _, err := planner.Submit(context.Background(), chamberID, model.DirectionUpstream, vessels(1))
	if err == nil {
		t.Fatal("expected journal failure")
	}
	if plans := planner.Active(chamberID); len(plans) != 0 {
		t.Fatalf("plans left behind: %d", len(plans))
	}
	if entries := q.List(chamberID); len(entries) != 0 {
		t.Fatalf("queue entries left behind: %d", len(entries))
	}
	if states := signals.List(); len(states) != 0 {
		t.Fatalf("field signals left behind: %d", len(states))
	}
	events, _ := store.Events(context.Background(), chamberID, 0)
	if len(events) != 0 {
		t.Fatalf("journal events left behind: %d", len(events))
	}
	if _, ok := cycles.Current(chamberID); ok {
		t.Fatalf("active cycle left behind")
	}

	// After recovery the chamber must accept a fresh submission.
	if _, _, err := planner.Submit(context.Background(), chamberID, model.DirectionUpstream, vessels(1)); err != nil {
		t.Fatalf("retry submit: %v", err)
	}
}
