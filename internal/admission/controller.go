package admission

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type Decision struct {
	Allowed  bool      `json:"allowed"`
	Reason   string    `json:"reason"`
	VesselID uuid.UUID `json:"vessel_id,omitempty"`
}

// Controller grants admission only from observed sealed gate positions.
type Controller struct {
	queue   *Queue
	gates   *gate.State
	signals *signal.Store
	now     func() time.Time
}

func NewController(queue *Queue, gates *gate.State, signals *signal.Store, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	return &Controller{queue: queue, gates: gates, signals: signals, now: now}
}

// Preflight decides whether the cycle may start without mutating any field
// state. It is evaluated inside the submit commit boundary, so a rejection
// leaves no signal or queue evidence behind.
func (c *Controller) Preflight(cycle model.TransitCycle) Decision {
	entry, ok := c.queue.Peek(cycle.ChamberID)
	if !ok {
		return Decision{Reason: "queue empty"}
	}
	if entry.PlanID != cycle.PlanID {
		return Decision{VesselID: entry.Vessel.ID, Reason: "head vessel belongs to another plan"}
	}
	if !c.gates.Sealed(cycle.ChamberID) {
		return Decision{VesselID: entry.Vessel.ID, Reason: "chamber gates are not sealed"}
	}
	return Decision{Allowed: true, VesselID: entry.Vessel.ID, Reason: "sealed chamber ready"}
}

// Grant commits the proceed aspect for a vessel that already passed Preflight.
// A failure removes any aspect it may have written so callers never observe a
// half-applied admission.
func (c *Controller) Grant(cycle model.TransitCycle, vesselID uuid.UUID) (model.SignalState, error) {
	state := c.signals.Set(vesselID, cycle.ID, cycle.Generation, model.SignalProceed, c.now())
	if state.Aspect != model.SignalProceed || !c.queue.MarkReleased(cycle.ChamberID, vesselID) {
		c.signals.Remove(vesselID)
		return model.SignalState{}, errors.New("failed to commit admission decision")
	}
	return state, nil
}

// Revoke removes an aspect granted within a submit that is rolling back.
func (c *Controller) Revoke(vesselID uuid.UUID) { c.signals.Remove(vesselID) }

func (c *Controller) OwnerAllowed(vesselID uuid.UUID, owner string, token uint64, lease model.Lease, now time.Time) bool {
	return lease.VesselID == vesselID && lease.Owner == owner &&
		lease.FencingToken == token && lease.Active(now)
}

func (c *Controller) Queue() *Queue { return c.queue }
