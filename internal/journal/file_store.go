package journal

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/wyw14/cry-96/internal/model"
)

// FileStore writes one append-only stream and one snapshot per chamber.
type FileStore struct {
	root string
	mu   sync.Mutex
}

func NewFileStore(root string) (*FileStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("journal root is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(abs, "events"), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(abs, "snapshots"), 0o755); err != nil {
		return nil, err
	}
	return &FileStore{root: abs}, nil
}

func safeChamberID(chamberID string) (string, error) {
	if chamberID == "" || strings.ContainsAny(chamberID, `/\\.`) {
		return "", fmt.Errorf("invalid chamber id %q", chamberID)
	}
	return chamberID, nil
}

func (s *FileStore) eventPath(chamberID string) (string, error) {
	name, err := safeChamberID(chamberID)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.root, "events", name+".jsonl"), nil
}

func (s *FileStore) snapshotPath(chamberID string) (string, error) {
	name, err := safeChamberID(chamberID)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.root, "snapshots", name+".json"), nil
}

func (s *FileStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	stored, err := s.AppendBatch(ctx, []model.Event{event})
	if err != nil {
		return model.Event{}, err
	}
	return stored[0], nil
}

// AppendBatch persists several events from one chamber atomically: they get
// contiguous sequence numbers and are written and synced in one file write.
// If the write or sync fails the file is truncated back to its prior length so
// a partial batch can never become visible on recovery.
func (s *FileStore) AppendBatch(ctx context.Context, events []model.Event) ([]model.Event, error) {
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
	path, err := s.eventPath(chamberID)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, err := readEvents(path)
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, 0, len(events)*256)
	stored := make([]model.Event, 0, len(events))
	partitionLength := uint64(len(existing))
	for _, event := range events {
		event.Sequence = partitionLength + 1
		partitionLength++
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, encoded...)
		buffer = append(buffer, '\n')
		stored = append(stored, event)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	info, statErr := file.Stat()
	written, writeErr := file.Write(buffer)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil || statErr != nil || uint64(written) != uint64(len(buffer)) {
		if statErr == nil {
			_ = os.Truncate(path, info.Size())
		}
		return nil, errors.Join(writeErr, syncErr, closeErr)
	}
	return stored, nil
}

func readEvents(path string) ([]model.Event, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var result []model.Event
	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 1024*1024)
	for scanner.Scan() {
		var event model.Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, scanner.Err()
}

func (s *FileStore) Events(ctx context.Context, chamberID string, after uint64) ([]model.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := s.eventPath(chamberID)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	events, err := readEvents(path)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	filtered := events[:0]
	for _, event := range events {
		if event.Sequence > after {
			filtered = append(filtered, event)
		}
	}
	return filtered, nil
}

func (s *FileStore) SaveSnapshot(ctx context.Context, snapshot model.ChamberSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.snapshotPath(snapshot.ChamberID)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".new"
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.WriteFile(temporary, append(encoded, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func (s *FileStore) Snapshot(ctx context.Context, chamberID string) (model.ChamberSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return model.ChamberSnapshot{}, false, err
	}
	path, err := s.snapshotPath(chamberID)
	if err != nil {
		return model.ChamberSnapshot{}, false, err
	}
	s.mu.Lock()
	data, err := os.ReadFile(path)
	s.mu.Unlock()
	if errors.Is(err, os.ErrNotExist) {
		return model.ChamberSnapshot{}, false, nil
	}
	if err != nil {
		return model.ChamberSnapshot{}, false, err
	}
	var snapshot model.ChamberSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return model.ChamberSnapshot{}, false, err
	}
	return snapshot, true, nil
}

func (s *FileStore) Chambers(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	for _, directory := range []string{"events", "snapshots"} {
		entries, err := os.ReadDir(filepath.Join(s.root, directory))
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				seen[strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(seen))
	for chamberID := range seen {
		result = append(result, chamberID)
	}
	sort.Strings(result)
	return result, nil
}
