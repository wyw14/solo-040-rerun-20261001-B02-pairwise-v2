package admission

import (
	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Service coordinates the staged queue with admission decisions. Journaling of
// plan and signal events belongs to the submit boundary in the schedule
// planner, so a rejected submit never persists a success event.
type Service struct {
	controller *Controller
}

func NewService(controller *Controller) *Service {
	return &Service{controller: controller}
}

// Reserve stages a plan's vessels as invisible queue entries.
func (s *Service) Reserve(plan model.ConvoyPlan) error {
	return s.controller.queue.Reserve(plan.ChamberID, plan.ID, plan.Vessels)
}

// Commit publishes a reservation's entries to the live queue.
func (s *Service) Commit(planID uuid.UUID) { s.controller.queue.Commit(planID) }

// Abort discards a plan's queue entries after a failed submit. It works
// whether or not the reservation was committed, so every rollback path is
// covered.
func (s *Service) Abort(chamberID string, planID uuid.UUID) {
	s.controller.queue.RemovePlan(chamberID, planID)
}

// Preflight reports whether a staged cycle may admit its head vessel.
func (s *Service) Preflight(cycle model.TransitCycle) Decision {
	return s.controller.Preflight(cycle)
}

// Grant commits the proceed aspect for a preflight-approved vessel.
func (s *Service) Grant(cycle model.TransitCycle, vesselID uuid.UUID) (model.SignalState, error) {
	return s.controller.Grant(cycle, vesselID)
}

// Revoke reverts an aspect granted during a submit that is rolling back.
func (s *Service) Revoke(vesselID uuid.UUID) { s.controller.Revoke(vesselID) }

func (s *Service) RemovePlan(chamberID string, plan model.ConvoyPlan) []QueueEntry {
	return s.controller.queue.RemovePlan(chamberID, plan.ID)
}

func (s *Service) Restore(chamberID string, entries []QueueEntry) {
	s.controller.queue.Restore(chamberID, entries)
}

func (s *Service) List(chamberID string) []QueueEntry {
	return s.controller.queue.List(chamberID)
}
