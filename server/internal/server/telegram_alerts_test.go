package server

import (
	"context"
	"io"
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
	}
	alerts.observeTTLU(entry)
	assertTelegramAlertContains(t, alerts, "Update propagation took 6.20s", "Project: whagons", "Query: tasks.list")

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
