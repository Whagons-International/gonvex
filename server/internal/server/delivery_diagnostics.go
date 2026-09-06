package server

import (
	"encoding/json"
	"time"
)

// Copy traces because multiple listeners can share the result's trace pointer.
// Stamp at physical write time, after batching and the connection lock.
func stampSocketWrite(message serverMessage, at time.Time) (serverMessage, float64) {
	var longest float64
	if original, ok := message.Trace.(*messageTrace); ok && original != nil {
		trace := *original
		trace.ServerSocketWriteStartedAtMS = epochMillis(at)
		if trace.ServerSubscriptionSentAtMS > 0 {
			longest = max(0, trace.ServerSocketWriteStartedAtMS-trace.ServerSubscriptionSentAtMS)
		}
		message.Trace = &trace
	}
	if len(message.Messages) > 0 {
		message.Messages = append([]serverMessage(nil), message.Messages...)
		for i, child := range message.Messages {
			stamped, wait := stampSocketWrite(child, at)
			message.Messages[i] = stamped
			longest = max(longest, wait)
		}
	}
	return message, longest
}

// Bounded identifiers let slow physical writes be matched to subscription acks.
// Query arguments and result contents never enter the log.
func deliveryQueryIDs(message serverMessage) []string {
	ids := []string{}
	var visit func(serverMessage)
	visit = func(m serverMessage) {
		if len(ids) >= 8 {
			return
		}
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
		for _, id := range m.IDs {
			if len(ids) < 8 {
				ids = append(ids, id)
			}
		}
		for _, child := range m.Messages {
			visit(child)
		}
	}
	visit(message)
	return ids
}

// Keep delivery measurements durable in the existing device JSON column.
// This requires no live schema change and preserves browser cohort fields.
func telemetryDeviceJSON(entry transactionTelemetryEntry) string {
	device := map[string]any{}
	_ = json.Unmarshal([]byte(entry.DeviceJSON), &device)
	if device == nil {
		device = map[string]any{}
	}
	if entry.ChangeToAckMS > 0 || entry.ServerSocketWriteStartedAtMS > 0 {
		device["serverDelivery"] = map[string]float64{
			"changeToAckMs":          entry.ChangeToAckMS,
			"socketWriteStartedAtMs": entry.ServerSocketWriteStartedAtMS,
			"socketQueueMs":          entry.ServerSocketQueueMS,
		}
	}
	payload, _ := json.Marshal(device)
	return string(payload)
}
