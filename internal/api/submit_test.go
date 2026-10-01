package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/wyw14/cry-96/internal/model"
)

func submitTestServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	runtime, err := NewRuntime(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(Router(runtime, ""))
	return server, server.Close
}

func planBody() string {
	return `{"chamber_id":"chamber-1","direction":"upstream","vessels":[` +
		`{"name":"tug","call_sign":"TUG-1","length_m":12},` +
		`{"name":"barge","call_sign":"BRG-9","length_m":40}]}`
}

func TestHTTPSubmitConflictLeavesNoVisiblePlanOrQueue(t *testing.T) {
	server, closeServer := submitTestServer(t)
	defer closeServer()

	post := func() int {
		resp, err := http.Post(server.URL+"/api/plans", "application/json", strings.NewReader(planBody()))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := post(); code != http.StatusCreated {
		t.Fatalf("first submit status = %d, want 201", code)
	}
	if code := post(); code != http.StatusConflict {
		t.Fatalf("second submit status = %d, want 409", code)
	}

	// Plan list contains exactly the accepted plan.
	resp, err := http.Get(server.URL + "/api/plans")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var plans map[string][]model.ConvoyPlan
	if err := json.NewDecoder(resp.Body).Decode(&plans); err != nil {
		t.Fatal(err)
	}
	if len(plans["chamber-1"]) != 1 {
		t.Fatalf("visible plans = %d, want 1", len(plans["chamber-1"]))
	}

	// The chamber snapshot shows one queue (2 vessels) and one active cycle.
	resp, err = http.Get(server.URL + "/api/chambers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var views []struct {
		ChamberID string              `json:"chamber_id"`
		Queue     []json.RawMessage   `json:"queue"`
		Cycle     *model.TransitCycle `json:"cycle"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&views); err != nil {
		t.Fatal(err)
	}
	for _, view := range views {
		if view.ChamberID != "chamber-1" {
			continue
		}
		if len(view.Queue) != 2 {
			t.Fatalf("queue length = %d, want 2", len(view.Queue))
		}
		if view.Cycle == nil {
			t.Fatal("active cycle missing after submit")
		}
	}
}

func TestHTTPConcurrentSubmitsExactlyOneCreated(t *testing.T) {
	server, closeServer := submitTestServer(t)
	defer closeServer()

	const contenders = 40
	start := make(chan struct{})
	statuses := make(chan int, contenders)
	var wg sync.WaitGroup
	wg.Add(contenders)
	for i := 0; i < contenders; i++ {
		go func() {
			defer wg.Done()
			<-start
			resp, err := http.Post(server.URL+"/api/plans", "application/json", strings.NewReader(planBody()))
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
			statuses <- resp.StatusCode
		}()
	}
	close(start)
	wg.Wait()
	close(statuses)

	created, conflicts := 0, 0
	for status := range statuses {
		switch status {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("unexpected status %d", status)
		}
	}
	if created != 1 {
		t.Fatalf("created = %d, want 1", created)
	}
	if conflicts != contenders-1 {
		t.Fatalf("conflicts = %d, want %d", conflicts, contenders-1)
	}
}
