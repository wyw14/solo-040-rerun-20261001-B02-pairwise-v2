package journal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func makeEvent(chamberID string, kind model.EventType) model.Event {
	return model.Event{ID: uuid.New(), ChamberID: chamberID, Type: kind}
}

func TestMemoryStoreAppendBatchAtomic(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	events := []model.Event{
		makeEvent("chamber-1", model.EventPlanCreated),
		makeEvent("chamber-1", model.EventCycleCreated),
		makeEvent("chamber-1", model.EventSignalChanged),
	}
	stored, err := store.AppendBatch(ctx, events)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != 3 {
		t.Fatalf("stored %d events, want 3", len(stored))
	}
	for index, event := range stored {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("event %d sequence = %d, want %d", index, event.Sequence, index+1)
		}
	}
	// A batch spanning chambers must be rejected without writing anything.
	mixed := []model.Event{makeEvent("chamber-1", model.EventPlanCreated), makeEvent("chamber-2", model.EventPlanCreated)}
	if _, err := store.AppendBatch(ctx, mixed); err == nil {
		t.Fatal("batch across chambers must fail")
	}
	chamber2, err := store.Events(ctx, "chamber-2", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chamber2) != 0 {
		t.Fatalf("rejected batch left %d events in chamber-2", len(chamber2))
	}
}

func TestFileStoreAppendBatchIsContiguous(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	batch := []model.Event{
		makeEvent("chamber-1", model.EventPlanCreated),
		makeEvent("chamber-1", model.EventCycleCreated),
	}
	if _, err := store.AppendBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	events, err := store.Events(ctx, "chamber-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Sequence != 1 || events[1].Sequence != 2 {
		t.Fatalf("unexpected stored batch: %+v", events)
	}
	// A further single append continues directly after the batch.
	if _, err := store.Append(ctx, makeEvent("chamber-1", model.EventSignalChanged)); err != nil {
		t.Fatal(err)
	}
	events, _ = store.Events(ctx, "chamber-1", 0)
	if len(events) != 3 || events[2].Sequence != 3 {
		t.Fatalf("sequence did not continue after batch: %+v", events)
	}
}
