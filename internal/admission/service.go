package admission

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Service records queue changes and delegates safe release decisions.
type Service struct {
	controller *Controller
	now        func() time.Time
}

func NewService(controller *Controller, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{controller: controller, now: now}
}

// BeginSubmission stages a plan at the tail of the waiting queue. It is meant
// to run inside the chamber submit boundary, so the entries are never visible
// without the plan and cycle that own them.
func (s *Service) BeginSubmission(plan model.ConvoyPlan) error {
	return s.controller.queue.Add(plan.ChamberID, plan.ID, plan.Vessels)
}

// CommitSubmission evaluates the head vessel against the sealed gates and, when
// safe, raises its proceed signal and marks it released. A rejection or error
// leaves no signal change behind. Must run inside the submit boundary.
func (s *Service) CommitSubmission(ctx context.Context, plan model.ConvoyPlan, cycleValue model.TransitCycle) (Decision, model.SignalState, *model.SignalState, error) {
	return s.controller.Commit(ctx, plan, cycleValue)
}

// RollbackSubmission removes every queue entry staged for the plan and restores
// the head vessel's prior signal (dropping it when there was none). It is
// idempotent and safe to call when the submission never reached admission.
func (s *Service) RollbackSubmission(plan model.ConvoyPlan, signaledVessel uuid.UUID, previous *model.SignalState) {
	s.controller.queue.RemovePlan(plan.ChamberID, plan.ID)
	if signaledVessel != uuid.Nil {
		s.controller.RestoreSignal(signaledVessel, previous)
	}
}

func (s *Service) RemovePlan(chamberID string, plan model.ConvoyPlan) []QueueEntry {
	return s.controller.queue.RemovePlan(chamberID, plan.ID)
}

func (s *Service) Restore(chamberID string, entries []QueueEntry) {
	s.controller.queue.Restore(chamberID, entries)
}

func (s *Service) List(chamberID string) []QueueEntry {
	return s.controller.queue.List(chamberID)
}
