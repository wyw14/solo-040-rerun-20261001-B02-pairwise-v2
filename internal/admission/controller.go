package admission

import (
	"context"
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

// ErrNotAdmitted reports that a submission could not be admitted and leaves no
// state behind; the reason is operator-facing.
type ErrNotAdmitted struct{ Reason string }

func (e *ErrNotAdmitted) Error() string { return e.Reason }

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

// Commit admits a freshly submitted plan while the chamber submit boundary is
// held. The gates check, field signal and queue release settle together with
// the caller's journaled event; on failure nothing is marked admitted. It must
// not take any lock held across the surrounding boundary. It returns the
// applied proceed state so the caller can journal it, and rolls the signal
// back itself if the queue cannot be marked.
func (c *Controller) Commit(ctx context.Context, plan model.ConvoyPlan, cycle model.TransitCycle) (Decision, model.SignalState, *model.SignalState, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, model.SignalState{}, nil, err
	}
	entry, ok := c.queue.Peek(cycle.ChamberID)
	if !ok {
		return Decision{}, model.SignalState{}, nil, &ErrNotAdmitted{Reason: "queue empty"}
	}
	if entry.PlanID != cycle.PlanID {
		return Decision{}, model.SignalState{}, nil, &ErrNotAdmitted{Reason: "head vessel belongs to another plan"}
	}
	if !c.gates.Sealed(cycle.ChamberID) {
		return Decision{}, model.SignalState{}, nil, &ErrNotAdmitted{Reason: "chamber gates are not sealed"}
	}
	var prior *model.SignalState
	if previous, hadPrevious := c.signals.Get(entry.Vessel.ID); hadPrevious {
		prior = &previous
	}
	state := c.signals.Set(entry.Vessel.ID, cycle.ID, cycle.Generation, model.SignalProceed, c.now())
	if state.Aspect != model.SignalProceed || !c.queue.MarkReleased(cycle.ChamberID, entry.Vessel.ID) {
		c.RestoreSignal(entry.Vessel.ID, prior)
		return Decision{}, model.SignalState{}, nil, errors.New("failed to commit admission decision")
	}
	return Decision{Allowed: true, VesselID: entry.Vessel.ID, Reason: "sealed chamber ready"}, state, prior, nil
}

// RestoreSignal reinstates the signal a vessel held before a submission, or
// removes it when the vessel had none.
func (c *Controller) RestoreSignal(vesselID uuid.UUID, previous *model.SignalState) {
	if previous != nil {
		c.signals.Set(previous.VesselID, previous.CycleID, previous.Generation, previous.Aspect, c.now())
		return
	}
	c.signals.Remove(vesselID)
}

func (c *Controller) OwnerAllowed(vesselID uuid.UUID, owner string, token uint64, lease model.Lease, now time.Time) bool {
	return lease.VesselID == vesselID && lease.Owner == owner &&
		lease.FencingToken == token && lease.Active(now)
}

func (c *Controller) Queue() *Queue { return c.queue }
