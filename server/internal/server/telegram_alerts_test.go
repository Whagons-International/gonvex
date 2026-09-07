package server

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestTelegramCPUAlertUsesThresholdCooldownAndRecovery(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	alerts := newTelegramAlertManager(config.Config{
		TelegramBotToken: "token", TelegramChatID: "chat", Environment: "production",
		AlertCPUPercent: 200, AlertTTLU: 5 * time.Second, AlertCooldown: 10 * time.Minute,
	})
	alerts.now = func() time.Time { return now }

	alerts.observeCPU(199.9)
	assertNoTelegramAlert(t, alerts)
	alerts.observeCPU(215.5)
	assertTelegramAlertContains(t, alerts, "CPU is 215.5%", "[production]")

	now = now.Add(time.Minute)
	alerts.observeCPU(250)
	assertNoTelegramAlert(t, alerts)
	now = now.Add(10 * time.Minute)
	alerts.observeCPU(250)
	assertTelegramAlertContains(t, alerts, "CPU is 250.0%")

	alerts.observeCPU(40)
	assertTelegramAlertContains(t, alerts, "CPU recovered", "CPU is 40.0%")
	alerts.observeCPU(30)
	assertNoTelegramAlert(t, alerts)
}

func TestTelegramTTLUAlertFiltersTelemetryAndAppliesCooldown(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	alerts := newTelegramAlertManager(config.Config{
		TelegramBotToken: "token", TelegramChatID: "chat", Environment: "staging",
		AlertCPUPercent: 200, AlertTTLU: 5 * time.Second, AlertCooldown: 15 * time.Minute,
	})
	alerts.now = func() time.Time { return now }

	entry := transactionTelemetryEntry{
		Project: "whagons", Kind: "query", Phase: "browser", Reason: "invalidate",
		Path: "tasks.list", ChangeToAckMS: 6200,
		ServerSocketWriteStartedAtMS: 12345, ServerSocketQueueMS: 250, BrowserName: "Chrome", BrowserVersion: "152.0.0.0",
	}
	alerts.observeTTLU(entry)
	assertTelegramAlertContains(t, alerts, "Update propagation took 6.20s", "Project: whagons", "Query: tasks.list", "Socket queue: 250ms", "Browser: Chrome 152.0.0.0")

	now = now.Add(time.Minute)
	entry.ChangeToAckMS = 9000
	alerts.observeTTLU(entry)
	assertNoTelegramAlert(t, alerts)

	now = now.Add(15 * time.Minute)
	alerts.observeTTLU(entry)
	assertTelegramAlertContains(t, alerts, "Update propagation took 9.00s")

	entry.Reason = "initial"
	alerts.observeTTLU(entry)
	assertNoTelegramAlert(t, alerts)
}

func TestTelegramTTLUAlertDoesNotDependOnTelemetryPersistence(t *testing.T) {
	server := New(config.Config{TelemetryEnabled: false})
	defer server.Close()
	server.telegramAlerts = newTelegramAlertManager(config.Config{
		TelegramBotToken: "token", TelegramChatID: "chat",
		AlertTTLU: 5 * time.Second, AlertCooldown: time.Minute,
	})

	server.recordTransactionTelemetry(transactionTelemetryEntry{
		Project: "project-a", Kind: "query", Phase: "browser", Reason: "invalidate",
		Path: "tasks.list", ChangeToAckMS: 6000,
	})
	assertTelegramAlertContains(t, server.telegramAlerts, "Update propagation took 6.00s")
}

func TestTelegramTTLUSubMillisecondThresholdKeepsPrecision(t *testing.T) {
	alerts := newTelegramAlertManager(config.Config{
		TelegramBotToken: "token", TelegramChatID: "chat",
		AlertTTLU: 500 * time.Microsecond, AlertCooldown: time.Minute,
	})
	entry := transactionTelemetryEntry{Kind: "query", Phase: "browser", Reason: "invalidate", ChangeToAckMS: 0.4}
	alerts.observeTTLU(entry)
	assertNoTelegramAlert(t, alerts)
	entry.ChangeToAckMS = 0.5
	alerts.observeTTLU(entry)
	assertTelegramAlertContains(t, alerts, "TTLU alert")
}

func TestTelegramAlertSendUsesBotAPIWithoutLeakingTokenIntoBody(t *testing.T) {
	requests := make(chan url.Values, 1)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/botsecret-token/sendMessage" {
			t.Errorf("path = %q", request.URL.Path)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		values, err := url.ParseQuery(string(body))
		if err != nil {
			t.Fatal(err)
		}
		requests <- values
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()

	alerts := newTelegramAlertManager(config.Config{
		TelegramBotToken: "secret-token", TelegramChatID: "-100123", TelegramAPIURL: api.URL,
		AlertCPUPercent: 200, AlertTTLU: 5 * time.Second, AlertCooldown: time.Minute,
	})
	if err := alerts.send(context.Background(), "CPU high"); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if request.Get("chat_id") != "-100123" || request.Get("text") != "CPU high" {
		t.Fatalf("request = %#v", request)
	}
	if strings.Contains(request.Encode(), "secret-token") {
		t.Fatal("bot token leaked into request body")
	}
}

func TestTelegramAlertsRequireTokenAndChat(t *testing.T) {
	if got := newTelegramAlertManager(config.Config{TelegramBotToken: "token"}); got != nil {
		t.Fatal("alerts enabled without chat ID")
	}
	if got := newTelegramAlertManager(config.Config{TelegramChatID: "chat"}); got != nil {
		t.Fatal("alerts enabled without bot token")
	}
}

func TestTelegramSlowOperationWithoutTelemetryPersistence(t *testing.T) {
	server := New(config.Config{TelemetryEnabled: false})
	defer server.Close()
	server.telegramAlerts = newTelegramAlertManager(config.Config{
		TelegramBotToken: "token", TelegramChatID: "chat", AlertOperationDuration: 5 * time.Second,
	})
	server.metrics.recordRuntimeLog(runtimeLogEntry{
		Project: "whagons", Tenant: "el-rey", Kind: "query", Path: "bulk.tasksByWorkspace",
		Outcome: "ok", DurationMS: 18000,
	}, time.Now())
	assertTelegramAlertContains(t, server.telegramAlerts, "18.00s", "el-rey", "bulk.tasksByWorkspace")
}

func assertTelegramAlertContains(t *testing.T, alerts *telegramAlertManager, fragments ...string) {
	t.Helper()
	select {
	case alert := <-alerts.queue:
		for _, fragment := range fragments {
			if !strings.Contains(alert.text, fragment) {
				t.Fatalf("alert %q does not contain %q", alert.text, fragment)
			}
		}
	default:
		t.Fatal("expected Telegram alert")
	}
}

func assertNoTelegramAlert(t *testing.T, alerts *telegramAlertManager) {
	t.Helper()
	select {
	case alert := <-alerts.queue:
		t.Fatalf("unexpected Telegram alert: %s", alert.text)
	default:
	}
}

func TestTelegramSlowOperationThresholdKindsAndSafeMessage(t *testing.T) {
	for _, kind := range []string{"query", "mutation", "action", "sync"} {
		t.Run(kind, func(t *testing.T) {
			alerts := newTelegramAlertManager(config.Config{TelegramBotToken: "token", TelegramChatID: "chat", AlertOperationDuration: 5 * time.Second})
			entry := runtimeLogEntry{Project: "whagons", Tenant: "el-rey", Kind: kind, Path: "tasks.acknowledge", Outcome: "error", Error: "secret-error", UserEmail: "private@example.test", Request: []byte(`{"password":"private"}`)}
			for _, duration := range []float64{1730, 4999.9, math.NaN(), math.Inf(1), -1} {
				entry.DurationMS = duration
				alerts.observeOperation(entry)
				assertNoTelegramAlert(t, alerts)
			}
			entry.DurationMS = 5000
			alerts.observeOperation(entry)
			select {
			case alert := <-alerts.queue:
				for _, secret := range []string{"secret-error", "private", "password"} {
					if strings.Contains(alert.text, secret) {
						t.Fatalf("private data in alert: %s", alert.text)
					}
				}
				if !strings.Contains(alert.text, "5.00s") || !strings.Contains(alert.text, "Outcome: error") {
					t.Fatal(alert.text)
				}
			default:
				t.Fatal("expected threshold alert")
			}
		})
	}
}

func TestTelegramSlowOperationCooldownScopeAndQueueBackpressure(t *testing.T) {
	now := time.Now()
	alerts := newTelegramAlertManager(config.Config{TelegramBotToken: "token", TelegramChatID: "chat", AlertOperationDuration: 5 * time.Second, AlertCooldown: time.Minute})
	alerts.now = func() time.Time { return now }
	entry := runtimeLogEntry{Project: "p", Tenant: "el-rey", Kind: "query", Path: "tasks.list", DurationMS: 18000}
	alerts.observeOperation(entry)
	assertTelegramAlertContains(t, alerts, "18.00s")
	alerts.observeOperation(entry)
	assertNoTelegramAlert(t, alerts)
	alerts.observeClientOperation(transactionTelemetryEntry{Project: "p", Tenant: "el-rey", Kind: "query", Path: "tasks.list", Phase: "browser", ClientDurationMS: 20000})
	assertNoTelegramAlert(t, alerts)
	for _, other := range []runtimeLogEntry{
		{Project: "p", Tenant: "other", Kind: "query", Path: "tasks.list", DurationMS: 6000},
		{Project: "other", Tenant: "el-rey", Kind: "query", Path: "tasks.list", DurationMS: 6000},
		{Project: "p", Tenant: "el-rey", Kind: "query", Path: "tasks.count", DurationMS: 6000},
		{Project: "p", Tenant: "el-rey", Kind: "mutation", Path: "tasks.list", DurationMS: 6000},
	} {
		alerts.observeOperation(other)
		assertTelegramAlertContains(t, alerts, "6.00s")
	}
	now = now.Add(time.Minute)
	for i := 0; i < cap(alerts.queue); i++ {
		alerts.queue <- telegramAlert{text: "occupied"}
	}
	alerts.observeOperation(entry)
	for len(alerts.queue) > 0 {
		<-alerts.queue
	}
	alerts.observeOperation(entry)
	assertTelegramAlertContains(t, alerts, "18.00s")
}

func TestTelegramClientTimeoutAlertsWithoutPersistence(t *testing.T) {
	server := New(config.Config{TelemetryEnabled: false})
	defer server.Close()
	server.telegramAlerts = newTelegramAlertManager(config.Config{TelegramBotToken: "token", TelegramChatID: "chat", AlertOperationDuration: 5 * time.Second})
	entry := transactionEntryFromClientTelemetry("whagons", "el-rey", clientMessage{Kind: "query", Path: "tasks.count", Reason: "timeout", Outcome: "error", ClientDurationMS: 20000})
	server.recordTransactionTelemetry(entry)
	assertTelegramAlertContains(t, server.telegramAlerts, "client timeout query took 20.00s", "el-rey", "tasks.count")
	entry.Path = "tasks.live"
	entry.Reason = "invalidate"
	server.recordTransactionTelemetry(entry)
	assertNoTelegramAlert(t, server.telegramAlerts)
	entry.Reason = "initial"
	entry.Phase = "server"
	server.recordTransactionTelemetry(entry)
	assertNoTelegramAlert(t, server.telegramAlerts)
}

func TestTelegramSlowOperationCooldownMemoryIsBounded(t *testing.T) {
	alerts := newTelegramAlertManager(config.Config{TelegramBotToken: "token", TelegramChatID: "chat", AlertOperationDuration: time.Second})
	now := time.Now()
	alerts.now = func() time.Time { return now }
	for i := 0; i < telegramSlowOperationKeyLimit; i++ {
		alerts.operationLastSentAt[slowOperationKey{path: fmt.Sprint(i)}] = now
	}
	entry := runtimeLogEntry{Kind: "query", Path: "new", DurationMS: 18000}
	alerts.observeOperation(entry)
	assertNoTelegramAlert(t, alerts)
	if len(alerts.operationLastSentAt) != telegramSlowOperationKeyLimit {
		t.Fatal("cooldown map grew")
	}
	now = now.Add(alerts.cooldown)
	alerts.observeOperation(entry)
	assertTelegramAlertContains(t, alerts, "18.00s")
	if len(alerts.operationLastSentAt) != 1 {
		t.Fatal("expired cooldowns were not reclaimed")
	}
}
