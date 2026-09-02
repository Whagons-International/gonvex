package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
)

const telegramAlertQueueSize = 64

type telegramAlert struct {
	text string
}

type telegramAlertManager struct {
	botToken      string
	chatID        string
	apiURL        string
	environment   string
	cpuThreshold  float64
	ttluThreshold time.Duration
	cooldown      time.Duration
	client        *http.Client
	queue         chan telegramAlert

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
		client: &http.Client{Timeout: 5 * time.Second}, queue: make(chan telegramAlert, telegramAlertQueueSize),
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
	a.enqueue(fmt.Sprintf("Gonvex TTLU alert [%s]\nUpdate propagation took %.2fs, threshold %.2fs. Project: %s. Query: %s.", a.label(), latencyMS/1000, a.ttluThreshold.Seconds(), project, path))
}

func (a *telegramAlertManager) label() string {
	if a.environment == "" {
		return "runtime"
	}
	return a.environment
}

func (a *telegramAlertManager) enqueue(text string) {
	select {
	case a.queue <- telegramAlert{text: text}:
	default:
		slog.Warn("Telegram runtime alert dropped because queue is full")
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
