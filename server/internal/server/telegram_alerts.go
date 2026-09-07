package server

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
)

const telegramAlertQueueSize = 64
const telegramSlowOperationKeyLimit = 1024

type slowOperationKey struct {
	project, tenant, kind, path string
}

type telegramAlert struct {
	text string
}

type telegramAlertManager struct {
	botToken            string
	chatID              string
	apiURL              string
	environment         string
	cpuThreshold        float64
	ttluThreshold       time.Duration
	operationThreshold  time.Duration
	operationLastSentAt map[slowOperationKey]time.Time
	cooldown            time.Duration
	client              *http.Client
	queue               chan telegramAlert

	mu             sync.Mutex
	cpuActive      bool
	cpuLastSentAt  time.Time
	ttluLastSentAt time.Time
	now            func() time.Time
}

func newTelegramAlertManager(cfg config.Config) *telegramAlertManager {
	if strings.TrimSpace(cfg.TelegramBotToken) == "" || strings.TrimSpace(cfg.TelegramChatID) == "" {
		return nil
	}
	apiURL := strings.TrimRight(strings.TrimSpace(cfg.TelegramAPIURL), "/")
	if apiURL == "" {
		apiURL = "https://api.telegram.org"
	}
	cooldown := cfg.AlertCooldown
	if cooldown <= 0 {
		cooldown = 15 * time.Minute
	}
	return &telegramAlertManager{
		botToken: cfg.TelegramBotToken, chatID: cfg.TelegramChatID, apiURL: apiURL,
		environment: strings.TrimSpace(cfg.Environment), cpuThreshold: cfg.AlertCPUPercent,
		ttluThreshold: cfg.AlertTTLU, cooldown: cooldown,
		operationThreshold:  cfg.AlertOperationDuration,
		operationLastSentAt: make(map[slowOperationKey]time.Time),
		client:              &http.Client{Timeout: 5 * time.Second}, queue: make(chan telegramAlert, telegramAlertQueueSize),
		now: time.Now,
	}
}

func (a *telegramAlertManager) run(ctx context.Context) {
	if a == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case alert := <-a.queue:
			if err := a.send(ctx, alert.text); err != nil {
				slog.Warn("send Telegram runtime alert", "error", err)
			}
		}
	}
}

func (a *telegramAlertManager) observeCPU(percent float64) {
	if a == nil || a.cpuThreshold <= 0 {
		return
	}
	now := a.now().UTC()
	a.mu.Lock()
	defer a.mu.Unlock()
	if percent >= a.cpuThreshold {
		if !a.cpuActive || now.Sub(a.cpuLastSentAt) >= a.cooldown {
			a.cpuActive = true
			a.cpuLastSentAt = now
			a.enqueue(fmt.Sprintf("Gonvex CPU alert [%s]\nCPU is %.1f%%, threshold %.1f%%. 100%% equals one core.", a.label(), percent, a.cpuThreshold))
		}
		return
	}
	if a.cpuActive {
		a.cpuActive = false
		a.enqueue(fmt.Sprintf("Gonvex CPU recovered [%s]\nCPU is %.1f%%, below the %.1f%% threshold.", a.label(), percent, a.cpuThreshold))
	}
}

func (a *telegramAlertManager) observeTTLU(entry transactionTelemetryEntry) {
	if a == nil || a.ttluThreshold <= 0 || entry.Kind != "query" || entry.Phase != "browser" || entry.Reason != "invalidate" {
		return
	}
	latencyMS := entry.ChangeToAckMS
	if latencyMS <= 0 {
		latencyMS = entry.ChangeToBrowserMS
	}
	if latencyMS < float64(a.ttluThreshold)/float64(time.Millisecond) {
		return
	}
	now := a.now().UTC()
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.ttluLastSentAt.IsZero() && now.Sub(a.ttluLastSentAt) < a.cooldown {
		return
	}
	a.ttluLastSentAt = now
	project := entry.Project
	if project == "" {
		project = "default"
	}
	path := entry.Path
	if path == "" {
		path = "unknown query"
	}
	detail := ""
	if entry.ServerSocketWriteStartedAtMS > 0 {
		detail += fmt.Sprintf(" Socket queue: %.0fms.", entry.ServerSocketQueueMS)
	}
	if entry.BrowserName != "" {
		detail += fmt.Sprintf(" Browser: %.40s %.40s.", entry.BrowserName, entry.BrowserVersion)
	}
	a.enqueue(fmt.Sprintf("Gonvex TTLU alert [%s]\nUpdate propagation took %.2fs, threshold %.2fs. Project: %s. Query: %s.%s", a.label(), latencyMS/1000, a.ttluThreshold.Seconds(), project, path, detail))
}

func (a *telegramAlertManager) label() string {
	if a.environment == "" {
		return "runtime"
	}
	return a.environment
}

func (a *telegramAlertManager) observeOperation(entry runtimeLogEntry) {
	if a == nil {
		return
	}
	a.observeSlowOperation(entry.Project, entry.Tenant, entry.Kind, entry.Path, entry.Outcome, "server", entry.DurationMS)
}

func (a *telegramAlertManager) observeClientOperation(entry transactionTelemetryEntry) {
	if a == nil || entry.Phase != "browser" {
		return
	}
	// Live update propagation has its own TTLU alert. A subscription's lifetime
	// is not the duration of an individual request.
	if entry.Kind == "query" && entry.Reason == "invalidate" {
		return
	}
	duration := entry.ClientDurationMS
	if duration <= 0 {
		duration = entry.ClientRoundTripMS
	}
	source := "client"
	if entry.Reason == "timeout" {
		source = "client timeout"
	}
	a.observeSlowOperation(entry.Project, entry.Tenant, entry.Kind, entry.Path, entry.Outcome, source, duration)
}

func (a *telegramAlertManager) observeSlowOperation(project, tenant, kind, path, outcome, source string, durationMS float64) {
	if a.operationThreshold <= 0 || math.IsNaN(durationMS) || math.IsInf(durationMS, 0) || durationMS < float64(a.operationThreshold)/float64(time.Millisecond) {
		return
	}
	switch kind {
	case "query", "mutation", "action", "sync":
	default:
		return
	}
	// Avoid request bodies, user identities and raw error strings in Telegram.
	// Bound client-supplied labels as well as the cooldown map.
	label := func(value, fallback string) string {
		value = strings.Join(strings.Fields(value), " ")
		if value == "" {
			return fallback
		}
		runes := []rune(value)
		if len(runes) > 160 {
			value = string(runes[:160])
		}
		return value
	}
	key := slowOperationKey{label(project, "default"), label(tenant, "unknown"), kind, label(path, "unknown function")}
	now := a.now().UTC()
	a.mu.Lock()
	defer a.mu.Unlock()
	if last, ok := a.operationLastSentAt[key]; ok && now.Sub(last) < a.cooldown {
		return
	}
	for key, last := range a.operationLastSentAt {
		if now.Sub(last) >= a.cooldown {
			delete(a.operationLastSentAt, key)
		}
	}
	if len(a.operationLastSentAt) >= telegramSlowOperationKeyLimit {
		return
	}
	if a.enqueue(fmt.Sprintf("Gonvex slow operation alert [%s]\n%s %s took %.2fs, threshold %.2fs. Project: %s. Tenant: %s. Function: %s. Outcome: %s.", label(a.label(), "runtime"), source, kind, durationMS/1000, a.operationThreshold.Seconds(), key.project, key.tenant, key.path, label(outcome, "unknown"))) {
		a.operationLastSentAt[key] = now
	}
}

func (a *telegramAlertManager) enqueue(text string) bool {
	select {
	case a.queue <- telegramAlert{text: text}:
		return true
	default:
		slog.Warn("Telegram runtime alert dropped because queue is full")
		return false
	}
}

func (a *telegramAlertManager) send(ctx context.Context, text string) error {
	form := url.Values{"chat_id": {a.chatID}, "text": {text}, "disable_web_page_preview": {"true"}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, a.apiURL+"/bot"+a.botToken+"/sendMessage", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := a.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Telegram API returned %s", response.Status)
	}
	return nil
}
