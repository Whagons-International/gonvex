package server

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSocketWriteStampPreservesSharedTraceAndMeasuresBatchWait(t *testing.T) {
	original := &messageTrace{ServerSubscriptionSentAtMS: 1000}
	message := serverMessage{Type: "query.batch", Messages: []serverMessage{
		{Type: "query.result", ID: "a", Trace: original},
		{Type: "query.result", ID: "b", Trace: original},
	}}
	stamped, wait := stampSocketWrite(message, time.UnixMilli(1250))
	if wait != 250 {
		t.Fatalf("socket queue wait = %v, want 250", wait)
	}
	for _, child := range stamped.Messages {
		payload, _ := json.Marshal(child.Trace)
		var trace map[string]any
		_ = json.Unmarshal(payload, &trace)
		if trace["serverSocketWriteStartedAtMs"] != float64(1250) {
			t.Fatalf("missing physical-write timestamp: %s", payload)
		}
	}
	payload, _ := json.Marshal(original)
	var trace map[string]any
	_ = json.Unmarshal(payload, &trace)
	if trace["serverSocketWriteStartedAtMs"] != nil {
		t.Fatal("modified shared subscription trace")
	}
}

func TestDeliveryTelemetrySurvivesDurableStorage(t *testing.T) {
	message := clientMessage{Kind: "query", Path: "presence.list", Reason: "invalidate",
		Device: json.RawMessage(`{"browserName":"Chrome","browserVersion":"152.0.0.0","deliveryDiagnostics":{"messageDecodeMs":12}}`),
		Trace:  &messageTrace{ServerSubscriptionSentAtMS: 1000, ServerSocketWriteStartedAtMS: 1250},
	}
	entry := transactionEntryFromClientTelemetry("project", "tenant", message)
	entry.ChangeToAckMS = 5879
	if entry.ServerSocketQueueMS != 250 {
		t.Fatalf("queue = %v", entry.ServerSocketQueueMS)
	}
	var device map[string]any
	if err := json.Unmarshal([]byte(telemetryDeviceJSON(entry)), &device); err != nil {
		t.Fatal(err)
	}
	if device["browserVersion"] != "152.0.0.0" {
		t.Fatal("lost browser cohort")
	}
	if device["deliveryDiagnostics"].(map[string]any)["messageDecodeMs"] != float64(12) {
		t.Fatal("lost client diagnostics")
	}
	server := device["serverDelivery"].(map[string]any)
	if server["changeToAckMs"] != float64(5879) || server["socketQueueMs"] != float64(250) {
		t.Fatalf("lost server measurements: %v", server)
	}
}

func TestDeliveryCorrelationIDsAreBounded(t *testing.T) {
	message := serverMessage{Type: "query.fanout", IDs: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}}
	if len(deliveryQueryIDs(message)) != 8 {
		t.Fatal("unbounded delivery log")
	}
	stamped, wait := stampSocketWrite(serverMessage{Type: "sync.delta"}, time.Now())
	if wait != 0 || stamped.Trace != nil {
		t.Fatal("invented subscription timing for sync")
	}
}

func TestPhysicalSocketWriteIncludesDeliveryTimestamp(t *testing.T) {
	before := time.Now().Add(-250 * time.Millisecond)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		client := &wsConn{conn: conn}
		client.writeLocked(serverMessage{Type: "query.result", ID: "query-a", Trace: &messageTrace{ServerSubscriptionSentAtMS: epochMillis(before)}})
	}))
	defer endpoint.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(endpoint.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var message struct {
		Trace *messageTrace `json:"trace"`
	}
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatal(err)
	}
	if message.Trace == nil || message.Trace.ServerSocketWriteStartedAtMS-message.Trace.ServerSubscriptionSentAtMS < 250 {
		t.Fatalf("physical write stamp missing: %+v", message.Trace)
	}
}
