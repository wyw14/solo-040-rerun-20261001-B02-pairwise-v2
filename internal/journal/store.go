package journal

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/wyw14/cry-96/internal/model"
)

// Store is the durable boundary used by all stateful coordinators.
type Store interface {
	Append(context.Context, model.Event) (model.Event, error)
	AppendBatch(context.Context, []model.Event) ([]model.Event, error)
	Events(context.Context, string, uint64) ([]model.Event, error)
	SaveSnapshot(context.Context, model.ChamberSnapshot) error
	Snapshot(context.Context, string) (model.ChamberSnapshot, bool, error)
	Chambers(context.Context) ([]string, error)
}

// MemoryStore provides deterministic operation when no data directory is set.
type MemoryStore struct {
	mu        sync.RWMutex
	events    map[string][]model.Event
	snapshots map[string]model.ChamberSnapshot
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		events:    make(map[string][]model.Event),
		snapshots: make(map[string]model.ChamberSnapshot),
	}
}

func (s *MemoryStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	if err := ctx.Err(); err != nil {
		return model.Event{}, err
	}
	if event.ChamberID == "" {
		return model.Event{}, errors.New("event chamber is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	partition := s.events[event.ChamberID]
	event.Sequence = uint64(len(partition) + 1)
	s.events[event.ChamberID] = append(partition, event)
	return event, nil
}

// AppendBatch stores several events from one chamber as an atomic unit: they
// receive contiguous sequence numbers under a single lock, and either all are
// visible afterwards or none are.
func (s *MemoryStore) AppendBatch(ctx context.Context, events []model.Event) ([]model.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, errors.New("event batch is empty")
	}
	chamberID := events[0].ChamberID
	if chamberID == "" {
		return nil, errors.New("event chamber is required")
	}
	for _, event := range events {
		if event.ChamberID != chamberID {
			return nil, errors.New("event batch spans multiple chambers")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	partition := s.events[chamberID]
	stored := make([]model.Event, 0, len(events))
	for _, event := range events {
		event.Sequence = uint64(len(partition) + 1)
		partition = append(partition, event)
		stored = append(stored, event)
	}
	s.events[chamberID] = partition
	return stored, nil
}

func (s *MemoryStore) Events(ctx context.Context, chamberID string, after uint64) ([]model.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	partition := s.events[chamberID]
	result := make([]model.Event, 0, len(partition))
	for _, event := range partition {
		if event.Sequence > after {
			result = append(result, event)
		}
	}
	return result, nil
}

func (s *MemoryStore) SaveSnapshot(ctx context.Context, snapshot model.ChamberSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot.ChamberID == "" {
		return errors.New("snapshot chamber is required")
	}
	s.mu.Lock()
	s.snapshots[snapshot.ChamberID] = snapshot.Clone()
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Snapshot(ctx context.Context, chamberID string) (model.ChamberSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return model.ChamberSnapshot{}, false, err
	}
	s.mu.RLock()
	snapshot, ok := s.snapshots[chamberID]
	s.mu.RUnlock()
	return snapshot.Clone(), ok, nil
}

func (s *MemoryStore) Chambers(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	seen := make(map[string]struct{}, len(s.events)+len(s.snapshots))
	for chamberID := range s.events {
		seen[chamberID] = struct{}{}
	}
	for chamberID := range s.snapshots {
		seen[chamberID] = struct{}{}
	}
	s.mu.RUnlock()
	result := make([]string, 0, len(seen))
	for chamberID := range seen {
		result = append(result, chamberID)
	}
	sort.Strings(result)
	return result, nil
}
