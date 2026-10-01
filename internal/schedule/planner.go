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

// Planner owns convoy plans and their admission queue membership, and runs the
// single atomic submit boundary that creates a plan, reserves its chamber and
// admits the head vessel.
type Planner struct {
	mu        sync.RWMutex
	plans     map[uuid.UUID]model.ConvoyPlan
	admission *admission.Service
	cycles    *cycle.Coordinator
	recorder  *journal.Recorder
	now       func() time.Time
}

func NewPlanner(admissionService *admission.Service, cycles *cycle.Coordinator, recorder *journal.Recorder, now func() time.Time) *Planner {
	if now == nil {
		now = time.Now
	}
	return &Planner{
		plans: make(map[uuid.UUID]model.ConvoyPlan), admission: admissionService,
		cycles: cycles, recorder: recorder, now: now,
	}
}

// Submit is the atomic plan-creation / enqueue / cycle-start boundary.
//
// It either commits every effect together and returns the plan and cycle, or
// it rejects after rolling every effect back: a failed submit stores no plan,
// leaves no waiting-queue entry, grants no field signal and records no success
// event. Concurrent submits for one chamber serialise on the cycle
// coordinator, so exactly one non-terminal transit can own a chamber.
func (p *Planner) Submit(ctx context.Context, chamberID string, direction model.Direction, vessels []model.Vessel) (model.ConvoyPlan, model.TransitCycle, error) {
	plan, err := model.NewConvoyPlan(chamberID, direction, vessels, p.now())
	if err != nil {
		return model.ConvoyPlan{}, model.TransitCycle{}, err
	}

	// Stage the vessels as an invisible reservation first. Nothing can observe
	// them until the chamber is claimed and the boundary commits.
	if err := p.admission.Reserve(plan); err != nil {
		return model.ConvoyPlan{}, model.TransitCycle{}, err
	}

	var grantedVessel uuid.UUID
	signalGranted := false
	published := false

	cycleValue, claimErr := p.cycles.Claim(ctx, plan, func(claimed model.TransitCycle) error {
		// Pure decision: a rejection here mutates nothing, so a busy chamber or
		// unsealed gate leaves no trace of the submit.
		decision := p.admission.Preflight(claimed)
		if !decision.Allowed {
			return errors.New(decision.Reason)
		}
		// Apply the field effect in memory; it is journaled atomically below.
		signalState, err := p.admission.Grant(claimed, decision.VesselID)
		if err != nil {
			return err
		}
		grantedVessel = decision.VesselID
		signalGranted = true
		// Single durable boundary: cycle, signal and plan events commit
		// together or not at all, so a journal failure cannot orphan an event
		// for a submit whose in-memory effects we are about to roll back.
		if _, err := p.recorder.RecordBatch(ctx, chamberID, []journal.EventSpec{
			{Kind: model.EventCycleCreated, CycleID: claimed.ID, Generation: claimed.Generation, Payload: claimed},
			{Kind: model.EventSignalChanged, CycleID: claimed.ID, Generation: claimed.Generation, Payload: signalState},
			{Kind: model.EventPlanCreated, CycleID: plan.ID, Payload: plan},
		}); err != nil {
			return err
		}
		// Visibility switch with no I/O afterwards: plan and committed queue
		// entries appear together, and Claim can no longer fail past here.
		p.mu.Lock()
		p.plans[plan.ID] = plan
		p.admission.Commit(plan.ID)
		p.mu.Unlock()
		published = true
		return nil
	})

	if claimErr != nil {
		p.rollback(plan, signalGranted, grantedVessel, published)
		return model.ConvoyPlan{}, model.TransitCycle{}, claimErr
	}
	return plan.Clone(), cycleValue.Clone(), nil
}

// rollback undoes every side effect a submit may have produced before failing.
// The cycle reservation is already rolled back by cycle.Coordinator.Claim.
func (p *Planner) rollback(plan model.ConvoyPlan, signalGranted bool, vesselID uuid.UUID, published bool) {
	if signalGranted {
		p.admission.Revoke(vesselID)
	}
	if published {
		p.mu.Lock()
		delete(p.plans, plan.ID)
		p.mu.Unlock()
	}
	// RemovePlan clears both an uncommitted reservation and committed entries.
	p.admission.Abort(plan.ChamberID, plan.ID)
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
	defer p.mu.RUnlock()
	result := make([]model.ConvoyPlan, 0)
	for _, plan := range p.plans {
		if plan.ChamberID == chamberID && !plan.Cancelled {
			result = append(result, plan.Clone())
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

// ActiveWithQueue returns the active plans and the live waiting queue under one
// read boundary, so the plan list can never disagree with queue membership.
func (p *Planner) ActiveWithQueue(chamberID string) ([]model.ConvoyPlan, []admission.QueueEntry) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]model.ConvoyPlan, 0)
	for _, plan := range p.plans {
		if plan.ChamberID == chamberID && !plan.Cancelled {
			result = append(result, plan.Clone())
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, p.admission.List(chamberID)
}

// Queue reports the live waiting queue, taking part in the same read boundary
// as the plan map.
func (p *Planner) Queue(chamberID string) []admission.QueueEntry {
	_, queue := p.ActiveWithQueue(chamberID)
	return queue
}

func (p *Planner) require(planID uuid.UUID) (model.ConvoyPlan, error) {
	plan, ok := p.Get(planID)
	if !ok {
		return model.ConvoyPlan{}, errors.New("convoy plan not found")
	}
	return plan, nil
}
