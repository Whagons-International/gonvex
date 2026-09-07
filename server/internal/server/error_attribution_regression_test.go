package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestScheduledFailurePreservesTenantAndRequest(t *testing.T) {
	s := New(config.Config{})
	captured := make(chan runtimeLogEntry, 1)
	s.metrics.onFunctionError = func(entry runtimeLogEntry) { captured <- entry }
	err := s.executeScheduledInternalMutation(context.Background(), scheduledJob{
		ID: "job-a", ProjectID: "project-a", TenantID: "missing-tenant", FunctionPath: "boards.generateBirthdayMessages",
		Args: json.RawMessage(`{"workspaceId":"workspace-a","token":"secret"}`),
	})
	if err == nil {
		t.Fatal("expected early scheduled dispatch failure")
	}
	select {
	case entry := <-captured:
		if entry.Tenant != "missing-tenant" {
			t.Fatalf("lost scheduled tenant: %+v", entry)
		}
		if entry.OperationID != "job-a" {
			t.Fatalf("lost scheduled job ID: %+v", entry)
		}
		var args map[string]any
		if json.Unmarshal(entry.Request, &args) != nil || args["workspaceId"] != "workspace-a" || args["token"] != "[REDACTED]" {
			t.Fatalf("missing or unsafe request: %s", entry.Request)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduled failure was not captured")
	}
}

func TestRuntimeFailureCapturesReleaseAtRecordingTime(t *testing.T) {
	t.Setenv("SOURCE_COMMIT", "1111111111111111111111111111111111111111")
	m := newRuntimeMetrics()
	var recorded runtimeLogEntry
	m.onFunctionError = func(entry runtimeLogEntry) { recorded = entry }
	m.recordRuntimeLog(runtimeLogEntry{Project: "project-a", Path: "tasks.list", Kind: "query", Outcome: "error", Error: "boom"}, time.Now())
	t.Setenv("SOURCE_COMMIT", "2222222222222222222222222222222222222222")
	event, ok := runtimeErrorEvent(recorded)
	if !ok || event.Release != "runtime@1111111111111111111111111111111111111111" {
		t.Fatalf("release must describe execution, not replay: %+v", event)
	}
	if event.Context["runtimeInstance"] == nil || event.Context["runtimeInstance"] == "" {
		t.Fatalf("missing backend instance: %+v", event)
	}
	if event.DeviceID != "" {
		t.Fatal("backend instance must not be counted as a client device")
	}
}

func TestResolvedErrorReopensOnNewOccurrenceWithoutReleaseChange(t *testing.T) {
	for _, release := range []string{"", "same-release"} {
		t.Run(release, func(t *testing.T) {
			tracker := newErrorTracker(10)
			event := capturedError{EventID: "first", Project: "project-a", Message: "boom", Release: release}
			fp, _ := tracker.capture(event)
			group := tracker.groups[fp]
			group.Status = "resolved"
			if _, accepted := tracker.capture(event); accepted || group.Status != "resolved" {
				t.Fatal("duplicate replay reopened resolved error")
			}
			event.EventID = "second"
			tracker.capture(event)
			if group.Status != "unresolved" || !group.Regression {
				t.Fatalf("new occurrence remained hidden: %+v", group)
			}
			group.Status = "ignored"
			event.EventID = "third"
			tracker.capture(event)
			if group.Status != "ignored" {
				t.Fatal("ignored errors must remain ignored")
			}
		})
	}
}
