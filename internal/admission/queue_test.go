package admission

import (
	"testing"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func testVessels(n int) []model.Vessel {
	vessels := make([]model.Vessel, n)
	for i := range vessels {
		vessels[i] = model.Vessel{ID: uuid.New(), Name: "v", CallSign: "c", LengthM: 10}
	}
	return vessels
}

func TestQueueReservationHiddenUntilCommit(t *testing.T) {
	queue := NewQueue()
	chamberID := "chamber-1"
	planID := uuid.New()
	vessels := testVessels(2)

	if err := queue.Reserve(chamberID, planID, vessels); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// Pending entries are invisible to external listing...
	if entries := queue.List(chamberID); len(entries) != 0 {
		t.Fatalf("pending reservation leaked into List: %d entries", len(entries))
	}
	// ...but FIFO admission can see the head while the submit is in flight.
	head, ok := queue.Peek(chamberID)
	if !ok || head.PlanID != planID {
		t.Fatalf("Peek should see reservation head, got ok=%v plan=%s", ok, head.PlanID)
	}
	if !queue.MarkReleased(chamberID, vessels[0].ID) {
		t.Fatal("MarkReleased should mark the pending head vessel")
	}

	queue.Commit(planID)
	entries := queue.List(chamberID)
	if len(entries) != 2 {
		t.Fatalf("committed queue should expose 2 entries, got %d", len(entries))
	}
	if !entries[0].Released {
		t.Fatal("released flag should survive commit")
	}
}

func TestQueueAbortDropsPendingReservation(t *testing.T) {
	queue := NewQueue()
	chamberID := "chamber-1"
	planID := uuid.New()
	if err := queue.Reserve(chamberID, planID, testVessels(2)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	queue.RemovePlan(chamberID, planID)

	if entries := queue.List(chamberID); len(entries) != 0 {
		t.Fatalf("abort left %d visible entries", len(entries))
	}
	if _, ok := queue.Peek(chamberID); ok {
		t.Fatal("abort left entries in FIFO")
	}
	// A second reservation for the same plan is allowed after abort.
	if err := queue.Reserve(chamberID, planID, testVessels(1)); err != nil {
		t.Fatalf("reserve after abort: %v", err)
	}
}

func TestQueueFIFOAcrossReservations(t *testing.T) {
	queue := NewQueue()
	chamberID := "chamber-1"
	first := uuid.New()
	second := uuid.New()
	if err := queue.Reserve(chamberID, first, testVessels(1)); err != nil {
		t.Fatal(err)
	}
	if err := queue.Reserve(chamberID, second, testVessels(1)); err != nil {
		t.Fatal(err)
	}
	head, _ := queue.Peek(chamberID)
	if head.PlanID != first {
		t.Fatalf("FIFO head = %s, want %s", head.PlanID, first)
	}
	queue.Commit(first)
	queue.RemovePlan(chamberID, first)
	head, _ = queue.Peek(chamberID)
	if head.PlanID != second {
		t.Fatalf("after removing first, head = %s, want %s", head.PlanID, second)
	}
}
