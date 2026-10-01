package schedule

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

// Submission is the committed result of a successful Submit.
type Submission struct {
	Plan  model.ConvoyPlan   `json:"plan"`
	Cycle model.TransitCycle `json:"cycle"`
}

// Service exposes planning and signal-publication workflows to the API.
type Service struct {
	planner   *Planner
	publisher *signal.Publisher
}

func NewService(planner *Planner, publisher *signal.Publisher) *Service {
	return &Service{planner: planner, publisher: publisher}
}

// Submit creates the plan, reserves the chamber and starts its transit as a
// single atomic boundary. On error no plan, queue entry, signal or success
// event remains visible.
func (s *Service) Submit(ctx context.Context, chamberID string, direction model.Direction, vessels []model.Vessel) (Submission, error) {
	plan, cycleValue, err := s.planner.Submit(ctx, chamberID, direction, vessels)
	if err != nil {
		return Submission{}, err
	}
	return Submission{Plan: plan, Cycle: cycleValue}, nil
}

func (s *Service) Publish(ctx context.Context, planID uuid.UUID, cycle model.TransitCycle) (signal.Publication, error) {
	plan, ok := s.planner.Get(planID)
	if !ok || plan.Cancelled {
		return signal.Publication{}, errors.New("active convoy plan not found")
	}
	if plan.ChamberID != cycle.ChamberID || plan.ID != cycle.PlanID {
		return signal.Publication{}, errors.New("plan and transit do not match")
	}
	return s.publisher.Publish(ctx, cycle, plan.Vessels, model.SignalProceed)
}

func (s *Service) Cancel(ctx context.Context, planID uuid.UUID, note string, publication signal.Publication) (Cancellation, error) {
	return s.planner.Cancel(ctx, planID, note, s.publisher, publication)
}

func (s *Service) Active(chamberID string) []model.ConvoyPlan {
	return s.planner.Active(chamberID)
}

// Queue returns the live waiting queue under the planner's read boundary.
func (s *Service) Queue(chamberID string) []admission.QueueEntry {
	return s.planner.Queue(chamberID)
}
