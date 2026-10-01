package journal

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Recorder turns domain transitions into consistently stamped events.
type Recorder struct {
	store Store
	now   func() time.Time
}

func NewRecorder(store Store, now func() time.Time) *Recorder {
	if now == nil {
		now = time.Now
	}
	return &Recorder{store: store, now: now}
}

func (r *Recorder) Record(ctx context.Context, chamberID string, kind model.EventType, cycleID uuid.UUID, generation uint64, payload any) (model.Event, error) {
	event, err := model.NewEvent(chamberID, kind, cycleID, generation, payload, r.now())
	if err != nil {
		return model.Event{}, err
	}
	stamped, err := r.store.AppendBatch(ctx, []model.Event{event})
	if err != nil {
		return model.Event{}, err
	}
	return stamped[0], nil
}

// EventSpec describes one event in a RecordBatch write.
type EventSpec struct {
	Kind       model.EventType
	CycleID    uuid.UUID
	Generation uint64
	Payload    any
}

// RecordBatch persists several events of one chamber atomically: callers
// either see every stamped event or none.
func (r *Recorder) RecordBatch(ctx context.Context, chamberID string, specs []EventSpec) ([]model.Event, error) {
	now := r.now()
	events := make([]model.Event, len(specs))
	for index, spec := range specs {
		event, err := model.NewEvent(chamberID, spec.Kind, spec.CycleID, spec.Generation, spec.Payload, now)
		if err != nil {
			return nil, err
		}
		events[index] = event
	}
	return r.store.AppendBatch(ctx, events)
}

func (r *Recorder) Save(ctx context.Context, snapshot model.ChamberSnapshot) error {
	snapshot.SavedAt = r.now().UTC()
	return r.store.SaveSnapshot(ctx, snapshot)
}

func NewRuntimeStore(dataDirectory string) (Store, error) {
	if dataDirectory == "" {
		return NewMemoryStore(), nil
	}
	return NewFileStore(dataDirectory)
}
