package cycle

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Coordinator is the authoritative owner of active transit generations.
type Coordinator struct {
	mu          sync.RWMutex
	current     map[string]model.TransitCycle
	generations map[string]uint64
	gates       *gate.Controller
	hydraulics  *hydraulics.Controller
	recorder    *journal.Recorder
	now         func() time.Time
}

func NewCoordinator(gates *gate.Controller, hydraulicsController *hydraulics.Controller, recorder *journal.Recorder, now func() time.Time) *Coordinator {
	if now == nil {
		now = time.Now
	}
	return &Coordinator{
		current: make(map[string]model.TransitCycle), generations: make(map[string]uint64),
		gates: gates, hydraulics: hydraulicsController, recorder: recorder, now: now,
	}
}

// ErrChamberBusy signals that a non-terminal transit already owns a chamber.
// It is a pure state rejection: no generation was bumped and no cycle stored.
var ErrChamberBusy = errors.New("chamber already has an active transit")

// Claim reserves the chamber for a new transit generation and runs commit with
// the reservation in place. The planner uses commit to perform the rest of the
// submit boundary. If commit fails, the reservation — current cycle and
// generation bump — is rolled back, leaving the chamber exactly as it was.
func (c *Coordinator) Claim(ctx context.Context, plan model.ConvoyPlan, commit func(model.TransitCycle) error) (model.TransitCycle, error) {
	if err := ctx.Err(); err != nil {
		return model.TransitCycle{}, err
	}
	c.mu.Lock()
	if active, ok := c.current[plan.ChamberID]; ok && !active.Stage.Terminal() {
		c.mu.Unlock()
		return model.TransitCycle{}, ErrChamberBusy
	}
	previousCycle, hadPrevious := c.current[plan.ChamberID]
	previousGeneration := c.generations[plan.ChamberID]
	c.generations[plan.ChamberID] = previousGeneration + 1
	cycleValue, err := model.NewTransitCycle(plan.ChamberID, plan.ID, plan.Direction, c.generations[plan.ChamberID], c.now())
	if err != nil {
		c.generations[plan.ChamberID] = previousGeneration
		c.mu.Unlock()
		return model.TransitCycle{}, err
	}
	c.current[plan.ChamberID] = cycleValue
	c.mu.Unlock()

	if err := commit(cycleValue); err != nil {
		c.mu.Lock()
		if current, ok := c.current[plan.ChamberID]; ok && current.ID == cycleValue.ID {
			if hadPrevious {
				c.current[plan.ChamberID] = previousCycle
			} else {
				delete(c.current, plan.ChamberID)
			}
			c.generations[plan.ChamberID] = previousGeneration
		}
		c.mu.Unlock()
		return model.TransitCycle{}, err
	}
	return cycleValue, nil
}

func (c *Coordinator) Current(chamberID string) (model.TransitCycle, bool) {
	c.mu.RLock()
	cycleValue, ok := c.current[chamberID]
	c.mu.RUnlock()
	return cycleValue.Clone(), ok
}

func (c *Coordinator) Cancel(ctx context.Context, chamberID, reason string) (model.TransitCycle, error) {
	c.mu.Lock()
	cycleValue, ok := c.current[chamberID]
	if !ok || cycleValue.Stage.Terminal() {
		c.mu.Unlock()
		return model.TransitCycle{}, errors.New("no cancellable transit")
	}
	if err := cycleValue.Advance(terminalStage(reason), reason, c.now()); err != nil {
		c.mu.Unlock()
		return model.TransitCycle{}, err
	}
	c.current[chamberID] = cycleValue
	c.mu.Unlock()
	_, err := c.recorder.Record(ctx, chamberID, model.EventCycleAdvanced, cycleValue.ID, cycleValue.Generation, cycleValue)
	return cycleValue, err
}

func (c *Coordinator) ApplyGateAck(ctx context.Context, ack model.DeviceAck) error {
	command, err := c.gates.Confirm(ctx, ack)
	if err != nil {
		return err
	}
	c.mu.Lock()
	cycleValue, ok := c.current[ack.ChamberID]
	if !ok || cycleValue.Stage.Terminal() || !cycleValue.IdentityMatches(command.CycleID, command.Generation) {
		c.mu.Unlock()
		return errors.New("device acknowledgement belongs to an inactive transit generation")
	}
	next, err := nextAfterGate(cycleValue.Stage)
	if err == nil {
		err = cycleValue.Advance(next, "matching gate command settled", c.now())
	}
	if err == nil {
		c.current[ack.ChamberID] = cycleValue
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if next == model.StageLeveling {
		if err := c.hydraulics.BeginLeveling(cycleValue); err != nil {
			return err
		}
	}
	_, err = c.recorder.Record(ctx, ack.ChamberID, model.EventCycleAdvanced, cycleValue.ID, cycleValue.Generation, cycleValue)
	return err
}

func (c *Coordinator) ApplySignalHandoff(ctx context.Context, cycleID uuid.UUID, generation uint64) error {
	c.mu.Lock()
	var chamberID string
	var cycleValue model.TransitCycle
	for id, candidate := range c.current {
		if candidate.IdentityMatches(cycleID, generation) {
			chamberID, cycleValue = id, candidate
			break
		}
	}
	if chamberID == "" {
		c.mu.Unlock()
		return errors.New("signal handoff transit is not current")
	}
	if err := applySignalTransition(&cycleValue); err != nil {
		c.mu.Unlock()
		return err
	}
	cycleValue.UpdatedAt = c.now().UTC()
	c.current[chamberID] = cycleValue
	c.mu.Unlock()
	_, err := c.recorder.Record(ctx, chamberID, model.EventCycleAdvanced, cycleValue.ID, generation, cycleValue)
	return err
}

func (c *Coordinator) Restore(cycleValue model.TransitCycle) {
	c.mu.Lock()
	c.current[cycleValue.ChamberID] = cycleValue
	if c.generations[cycleValue.ChamberID] < cycleValue.Generation {
		c.generations[cycleValue.ChamberID] = cycleValue.Generation
	}
	c.mu.Unlock()
}
