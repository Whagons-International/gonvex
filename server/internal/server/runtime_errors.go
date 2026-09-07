package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

// Server-side function failures used to exist only as a line in the in-memory
// runtime log ring — which spans minutes at production traffic — while the
// dashboard's Errors tab ingested browser exceptions exclusively. A failing
// action was therefore invisible within a quarter of an hour of failing, and
// invisible immediately in the one place people look for errors.
//
// Every errored query/mutation/action is now captured as an error event through
// the same store, fingerprinting, and grouping the browser SDK feeds, so a
// backend failure shows up beside the frontend ones with its own first/last
// seen, tenant, and user breakdown.

// The durable log restore is capped at metricsLogLimit, so this capacity lets a
// restart replay every visible failed log without dropping the tail of the
// batch before the worker can persist it.
const runtimeErrorQueueSize = metricsLogLimit

// runtimeErrorCapture is the fan-in point wired from recordRuntimeLog. It is
// deliberately non-blocking: telemetry must never hold up request handling.
func (s *Server) queueRuntimeFunctionError(entry runtimeLogEntry) {
	if s == nil || s.runtimeErrors == nil {
		return
	}
	select {
	case s.runtimeErrors <- entry:
	default:
		slog.Warn("dropped runtime error capture", "project", entry.Project, "path", entry.Path, "kind", entry.Kind)
	}
}

func (s *Server) startRuntimeErrorCapture() {
	s.runtimeErrors = make(chan runtimeLogEntry, runtimeErrorQueueSize)
	go func() {
		for entry := range s.runtimeErrors {
			s.captureRuntimeFunctionError(entry)
		}
	}()
}

func (s *Server) captureRuntimeFunctionError(entry runtimeLogEntry) {
	event, ok := runtimeErrorEvent(entry)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.enableProjectErrorTracking(ctx, event.Project)
	available, _, err := s.persistError(ctx, event)
	if err != nil {
		slog.Error("persist runtime function error", "project", event.Project, "path", entry.Path, "error", err)
	}
	if !available || err != nil {
		// No telemetry database (or it rejected the write): keep the in-memory
		// tracker as the fallback, exactly as the browser ingest path does.
		s.errorTracker.capture(event)
	}
}

// runtimeErrorEvent renders a failed function call as a capturedError. Culprit
// carries "<kind> <path>" — the same shape as the CLI log line — so grouping is
// per function and per kind rather than lumping every backend failure together.
func runtimeErrorEvent(entry runtimeLogEntry) (capturedError, bool) {
	message := strings.TrimSpace(entry.Error)
	project := strings.TrimSpace(entry.Project)
	if entry.Outcome != "error" || message == "" || project == "" {
		return capturedError{}, false
	}
	kind := strings.TrimSpace(entry.Kind)
	if kind == "" {
		kind = "function"
	}
	timestamp := strings.TrimSpace(entry.Time)
	if timestamp == "" {
		timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}
	event := capturedError{
		// One event per execution: a retried execution id can never double-count,
		// and the store's (project, event_id) conflict clause absorbs replays.
		EventID:     "runtime-" + strings.TrimSpace(entry.ExecutionID),
		Timestamp:   timestamp,
		Level:       "error",
		Message:     message,
		Name:        kind + "Error",
		Culprit:     kind + " " + strings.TrimSpace(entry.Path),
		Project:     project,
		Tenant:      strings.TrimSpace(entry.Tenant),
		Environment: "server",
		Release:     strings.TrimSpace(entry.Release),
		Tags: map[string]string{
			"source": "runtime",
			"kind":   kind,
			"path":   strings.TrimSpace(entry.Path),
		},
		Context: runtimeErrorContext(entry),
	}
	if strings.TrimSpace(entry.ExecutionID) == "" {
		event.EventID = generatedEventID(event)
	}
	if user := strings.TrimSpace(entry.UserID); user != "" {
		event.User = map[string]any{"id": user}
		if email := strings.TrimSpace(entry.UserEmail); email != "" {
			event.User["email"] = email
		}
	}
	return sanitizeCapturedError(event), true
}

func runtimeErrorContext(entry runtimeLogEntry) map[string]any {
	context := map[string]any{"durationMs": entry.DurationMS}
	for key, value := range map[string]string{
		"executionId":     entry.ExecutionID,
		"runtimeInstance": entry.RuntimeInstance,
		"operationId":     entry.OperationID,
		"connectionId":    entry.ConnectionID,
		"browser":         entry.Browser,
		"deviceType":      entry.DeviceType,
		"platform":        entry.Platform,
		"startedAt":       entry.StartedAt,
		"completedAt":     entry.CompletedAt,
		"cache":           entry.Cache,
		"source":          entry.Source,
		"trigger":         entry.Reason,
	} {
		if value = strings.TrimSpace(value); value != "" {
			context[key] = value
		}
	}
	if entry.RequestSizeBytes > 0 {
		context["requestSizeBytes"] = entry.RequestSizeBytes
	}
	if entry.ResultCount != nil {
		context["resultCount"] = *entry.ResultCount
	}
	if len(entry.Request) > 0 {
		var request any
		if json.Unmarshal(entry.Request, &request) == nil {
			context["request"] = request
		} else {
			context["request"] = map[string]any{"unavailable": "request was not valid JSON"}
		}
	}
	return context
}
