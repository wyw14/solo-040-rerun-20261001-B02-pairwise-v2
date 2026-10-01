package schedule

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Planner owns convoy plans and their admission queue membership.
type Planner struct {
	mu        sync.RWMutex
	plans     map[uuid.UUID]model.ConvoyPlan
	cycles    *cycle.Coordinator
	admission *admission.Service
	recorder  *journal.Recorder
	now       func() time.Time
}

func NewPlanner(cycles *cycle.Coordinator, admissionService *admission.Service, recorder *journal.Recorder, now func() time.Time) *Planner {
	if now == nil {
		now = time.Now
	}
	return &Planner{
		plans: make(map[uuid.UUID]model.ConvoyPlan), cycles: cycles,
		admission: admissionService, recorder: recorder, now: now,
	}
}

// Submit creates a plan and starts its transit as one boundary. The plan,
// queue entries and every "success" event are staged together and either all
// become visible (chamber free, admission safe, journal persisted) or none do.
// Concurrent submissions for the same chamber are serialized and at most one
// succeeds.
func (p *Planner) Submit(ctx context.Context, chamberID string, direction model.Direction, vessels []model.Vessel) (model.ConvoyPlan, model.TransitCycle, error) {
	plan, err := model.NewConvoyPlan(chamberID, direction, vessels, p.now())
	if err != nil {
		return model.ConvoyPlan{}, model.TransitCycle{}, err
	}

	var (
		staged         bool
		signaledVessel uuid.UUID
		previousSignal *model.SignalState
	)

	cycleValue, err := p.cycles.Submit(
		ctx,
		plan,
		func(proposed model.TransitCycle) error {
			// Stage the plan and its waiting queue while the chamber boundary
			// is held: nothing is visible until this callback returns nil.
			if err := p.admission.BeginSubmission(plan); err != nil {
				return err
			}
			staged = true
			decision, state, prior, err := p.admission.CommitSubmission(ctx, plan, proposed)
			if err != nil {
				return err
			}
			signaledVessel = decision.VesselID
			previousSignal = prior
			events := []journal.EventSpec{
				{Kind: model.EventPlanCreated, CycleID: plan.ID, Payload: plan},
				{Kind: model.EventCycleCreated, CycleID: proposed.ID, Generation: proposed.Generation, Payload: proposed},
				{Kind: model.EventSignalChanged, CycleID: proposed.ID, Generation: proposed.Generation, Payload: state},
			}
			if _, err := p.recorder.RecordBatch(ctx, chamberID, events); err != nil {
				return err
			}
			p.mu.Lock()
			p.plans[plan.ID] = plan
			p.mu.Unlock()
			return nil
		},
		func(_ model.TransitCycle, _ error) {
			// Runs under the chamber lock; unwind only what the callback staged.
			if staged {
				p.admission.RollbackSubmission(plan, signaledVessel, previousSignal)
			}
			p.mu.Lock()
			delete(p.plans, plan.ID)
			p.mu.Unlock()
		},
	)
	if err != nil {
		return model.ConvoyPlan{}, model.TransitCycle{}, err
	}
	return plan.Clone(), cycleValue, nil
}

func (p *Planner) Get(planID uuid.UUID) (model.ConvoyPlan, bool) {
	p.mu.RLock()
	plan, ok := p.plans[planID]
	p.mu.RUnlock()
	return plan.Clone(), ok
}

func (p *Planner) replace(plan model.ConvoyPlan) {
	p.mu.Lock()
	p.plans[plan.ID] = plan.Clone()
	p.mu.Unlock()
}

func (p *Planner) Active(chamberID string) []model.ConvoyPlan {
	p.mu.RLock()
	var result []model.ConvoyPlan
	for _, plan := range p.plans {
		if plan.ChamberID == chamberID && !plan.Cancelled {
			result = append(result, plan.Clone())
		}
	}
	p.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func (p *Planner) require(planID uuid.UUID) (model.ConvoyPlan, error) {
	plan, ok := p.Get(planID)
	if !ok {
		return model.ConvoyPlan{}, errors.New("convoy plan not found")
	}
	return plan, nil
}
