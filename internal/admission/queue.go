package admission

import (
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

type QueueEntry struct {
	Vessel   model.Vessel `json:"vessel"`
	PlanID   uuid.UUID    `json:"plan_id"`
	Released bool         `json:"released"`
}

// Queue preserves FIFO order while supporting convoy cancellation recovery.
//
// A reservation starts uncommitted: its entries are owned by the in-flight
// submit and stay invisible to Peek/List until Commit marks them live. Abort
// discards them, so a submit rejected before its commit boundary never leaves
// a queue entry behind.
type Queue struct {
	mu      sync.RWMutex
	entries map[string][]QueueEntry
	pending map[uuid.UUID]struct{}
}

func NewQueue() *Queue {
	return &Queue{entries: make(map[string][]QueueEntry), pending: make(map[uuid.UUID]struct{})}
}

// Reserve appends the plan's vessels as uncommitted entries.
func (q *Queue) Reserve(chamberID string, planID uuid.UUID, vessels []model.Vessel) error {
	if chamberID == "" || planID == uuid.Nil || len(vessels) == 0 {
		return errors.New("queue reservation requires chamber, plan and vessels")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, exists := q.pending[planID]; exists {
		return errors.New("queue reservation already exists for plan")
	}
	q.pending[planID] = struct{}{}
	for _, vessel := range vessels {
		q.entries[chamberID] = append(q.entries[chamberID], QueueEntry{Vessel: vessel, PlanID: planID})
	}
	return nil
}

// Commit makes a reservation's entries visible.
func (q *Queue) Commit(planID uuid.UUID) {
	q.mu.Lock()
	delete(q.pending, planID)
	q.mu.Unlock()
}

// Peek returns the first entry in FIFO order, including an uncommitted
// reservation. It serves admission decisions made inside an in-flight submit:
// only the plan whose own reservation sits at the head may claim the chamber,
// so concurrent submits naturally serialise into one winner. External readers
// use List, which keeps reservations hidden until commit.
func (q *Queue) Peek(chamberID string) (QueueEntry, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	entries := q.entries[chamberID]
	if len(entries) == 0 {
		return QueueEntry{}, false
	}
	return entries[0], true
}

// MarkReleased marks the head vessel released. The vessel still belongs to an
// uncommitted reservation during submit, so pending entries are eligible; the
// released flag becomes visible when the reservation commits.
func (q *Queue) MarkReleased(chamberID string, vesselID uuid.UUID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for index := range q.entries[chamberID] {
		entry := &q.entries[chamberID][index]
		if entry.Vessel.ID == vesselID {
			entry.Released = true
			return true
		}
	}
	return false
}

func (q *Queue) RemovePlan(chamberID string, planID uuid.UUID) []QueueEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Removing a live plan also clears any lingering reservation marker.
	delete(q.pending, planID)
	entries := q.entries[chamberID]
	kept := entries[:0]
	var removed []QueueEntry
	for _, entry := range entries {
		if entry.PlanID == planID {
			removed = append(removed, entry)
		} else {
			kept = append(kept, entry)
		}
	}
	q.entries[chamberID] = append([]QueueEntry(nil), kept...)
	return removed
}

func (q *Queue) Restore(chamberID string, entries []QueueEntry) {
	q.mu.Lock()
	current := q.entries[chamberID]
	restored := make([]QueueEntry, 0, len(entries)+len(current))
	for _, entry := range entries {
		entry.Released = false
		restored = append(restored, entry)
	}
	q.entries[chamberID] = append(restored, current...)
	q.mu.Unlock()
}

// List returns only committed entries; reservations remain invisible.
func (q *Queue) List(chamberID string) []QueueEntry {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var result []QueueEntry
	for _, entry := range q.entries[chamberID] {
		if _, pending := q.pending[entry.PlanID]; pending {
			continue
		}
		result = append(result, entry)
	}
	return result
}
