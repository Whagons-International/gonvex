package config

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultRowsCacheTTL = 10 * time.Minute

const (
	defaultTenantListenerLimit          = 64
	defaultTenantListenerIdleTimeout    = time.Minute
	defaultSharedResultMaxBytes         = 8 << 20
	defaultSharedSubscriptionGrace      = 15 * time.Second
	defaultSharedSubscriptionFanout     = 10000
	defaultSubscriptionRerunConcurrency = 32
)

type Config struct {
	Addr             string
	LandlordURL      string
	PostgresURL      string
	TenantDatabases  map[string]string
	ProjectDatabases map[string]string
	ProjectKeys      map[string]string
	// ProjectHosts maps browser-facing hostnames to project IDs for anonymous
	// registered HTTP functions (for example universal-link and webhook hosts).
	ProjectHosts                map[string]string
	GonvexModuleRoot            string
	PluginCacheDir              string
	ValkeyURL                   string
	RowsCacheTTL                time.Duration
	TelemetryEnabled            bool
	TelemetryLogPath            string
	S3Endpoint                  string
	S3Region                    string
	S3Bucket                    string
	S3AccessKeyID               string
	S3SecretAccessKey           string
	S3ForcePathStyle            bool
	StoragePublicURL            string
	DevSyncKey                  string
	AdminKey                    string
	RequireAuth                 bool
	QueryCacheEnabled           bool
	TenantListenerLimit         int
	TenantListenerIdleTimeout   time.Duration
	SharedResultMaxBytes        int
	SharedSubscriptionGrace     time.Duration
	SharedSubscriptionMaxFanout int
	// SubscriptionRerunConcurrency is the global cap on concurrent
	// database-backed query executions (reactive reruns, sync recomputation,
	// and bootstrap hydration). Zero preserves unlimited behavior for
	// lightweight tests and embedded callers.
	SubscriptionRerunConcurrency int
	// QueryBootstrapConcurrency bounds how many of those executions may be
	// bootstrap hydration (initial subscription executions and sync snapshots)
	// while reactive or foreground work is waiting. Zero derives a quarter of
	// SubscriptionRerunConcurrency; hydration may still borrow idle capacity
	// beyond this share when nothing else is queued.
	QueryBootstrapConcurrency int
	DashboardSecret           string
	TrustedProxyCIDRs         []string
	// DashboardAuthProjectID is the one Gonvex application whose native Google
	// sessions may authenticate to the control-plane dashboard. Keeping this
	// explicit prevents a session minted for an arbitrary customer project from
	// becoming a dashboard credential.
	DashboardAuthProjectID string
	// AuthPublicURL is the browser-facing origin of the Gonvex runtime. Google
	// redirects to AuthPublicURL + /auth/google/callback. A hosted Gonvex
	// installation configures one Google OAuth client here; individual projects
	// only register their own app callback URL with Gonvex.
	AuthPublicURL      string
	GoogleClientID     string
	GoogleClientSecret string
	GoogleAuthorizeURL string
	GoogleTokenURL     string
	GoogleJWKSURL      string
	// FirebaseProjectID turns on verification of Firebase ID tokens presented as
	// app credentials. Empty keeps the legacy behavior: the token's claims are
	// decoded without any signature or expiry check, so any forged JWT
	// authenticates as whatever "sub" it names. Set it in every deployment that
	// authenticates browsers with Firebase.
	FirebaseProjectID string
	FirebaseJWKSURL   string
	// Environment labels projects created/imported on this runtime instance in the
	// dashboard ("local dev", "production", ...). Deployed runtimes set
	// GONVEX_ENVIRONMENT so their projects stop claiming to be local dev.
	Environment string
	// DropEmptyUndeclaredColumns enables conservative declarative cleanup. It is
	// off by default so deploying an older bundle cannot erase newer columns.
	DropEmptyUndeclaredColumns bool
	// Telegram alerts are disabled unless both the bot token and chat ID are
	// configured. Thresholds at or below zero disable that individual alert.
	TelegramBotToken string
	TelegramChatID   string
	TelegramAPIURL   string
	AlertCPUPercent  float64
	AlertTTLU        time.Duration
	AlertCooldown    time.Duration
}

func FromEnv() Config {
	loadDotEnv(".env")

	return Config{
		Addr:                         env("GONVEX_ADDR", ":8080"),
		LandlordURL:                  env("GONVEX_LANDLORD_DATABASE_URL", env("LANDLORD_DATABASE_URL", "")),
		PostgresURL:                  env("DATABASE_URL", env("POSTGRES_URL", "")),
		TenantDatabases:              envStringMap("GONVEX_TENANT_DATABASE_URLS"),
		ProjectDatabases:             envStringMap("GONVEX_PROJECT_DATABASE_URLS"),
		ProjectKeys:                  envStringMap("GONVEX_PROJECT_KEYS"),
		ProjectHosts:                 envStringMap("GONVEX_PROJECT_HOSTS"),
		GonvexModuleRoot:             env("GONVEX_MODULE_ROOT", ""),
		PluginCacheDir:               env("GONVEX_PLUGIN_CACHE_DIR", ""),
		ValkeyURL:                    env("VALKEY_URL", env("REDIS_URL", "")),
		RowsCacheTTL:                 envDuration("GONVEX_ROWS_CACHE_TTL", defaultRowsCacheTTL),
		TelemetryEnabled:             envBool("GONVEX_TELEMETRY_ENABLED", true),
		TelemetryLogPath:             env("GONVEX_TELEMETRY_LOG", "tmp/gonvex-telemetry.jsonl"),
		S3Endpoint:                   env("S3_ENDPOINT", ""),
		S3Region:                     env("S3_REGION", "us-east-1"),
		S3Bucket:                     env("S3_BUCKET", ""),
		S3AccessKeyID:                env("S3_ACCESS_KEY_ID", ""),
		S3SecretAccessKey:            env("S3_SECRET_ACCESS_KEY", ""),
		S3ForcePathStyle:             envBool("S3_FORCE_PATH_STYLE", true),
		StoragePublicURL:             env("GONVEX_PUBLIC_URL", ""),
		DevSyncKey:                   env("GONVEX_DEV_SYNC_KEY", env("GONVEX_PROJECT_KEY", env("GONVEX_DEPLOY_KEY", ""))),
		AdminKey:                     env("GONVEX_ADMIN_KEY", ""),
		RequireAuth:                  envBool("GONVEX_REQUIRE_AUTH", false),
		QueryCacheEnabled:            envBool("GONVEX_BROWSER_QUERY_CACHE_ENABLED", true),
		TenantListenerLimit:          envInt("GONVEX_TENANT_LISTENER_LIMIT", defaultTenantListenerLimit),
		TenantListenerIdleTimeout:    envDuration("GONVEX_TENANT_LISTENER_IDLE_TIMEOUT", defaultTenantListenerIdleTimeout),
		SharedResultMaxBytes:         envInt("GONVEX_SHARED_RESULT_MAX_BYTES", defaultSharedResultMaxBytes),
		SharedSubscriptionGrace:      envDuration("GONVEX_SHARED_SUBSCRIPTION_GRACE", defaultSharedSubscriptionGrace),
		SharedSubscriptionMaxFanout:  envInt("GONVEX_SHARED_SUBSCRIPTION_MAX_FANOUT", defaultSharedSubscriptionFanout),
		SubscriptionRerunConcurrency: envInt("GONVEX_SUBSCRIPTION_RERUN_CONCURRENCY", defaultSubscriptionRerunConcurrency),
		QueryBootstrapConcurrency:    envInt("GONVEX_QUERY_BOOTSTRAP_CONCURRENCY", 0),
		DashboardSecret:              env("GONVEX_DASHBOARD_SESSION_SECRET", env("DASHBOARD_SESSION_SECRET", "")),
		TrustedProxyCIDRs:            envList("GONVEX_TRUSTED_PROXY_CIDRS"),
		DashboardAuthProjectID:       strings.TrimSpace(env("GONVEX_DASHBOARD_AUTH_PROJECT_ID", "")),
		AuthPublicURL:                env("GONVEX_AUTH_URL", env("GONVEX_PUBLIC_URL", "")),
		GoogleClientID:               env("GONVEX_GOOGLE_CLIENT_ID", ""),
		GoogleClientSecret:           env("GONVEX_GOOGLE_CLIENT_SECRET", ""),
		GoogleAuthorizeURL:           env("GONVEX_GOOGLE_AUTHORIZE_URL", "https://accounts.google.com/o/oauth2/v2/auth"),
		GoogleTokenURL:               env("GONVEX_GOOGLE_TOKEN_URL", "https://oauth2.googleapis.com/token"),
		GoogleJWKSURL:                env("GONVEX_GOOGLE_JWKS_URL", "https://www.googleapis.com/oauth2/v3/certs"),
		FirebaseProjectID:            strings.TrimSpace(env("GONVEX_FIREBASE_PROJECT_ID", "")),
		FirebaseJWKSURL:              env("GONVEX_FIREBASE_JWKS_URL", "https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com"),
		Environment:                  env("GONVEX_ENVIRONMENT", "local dev"),
		DropEmptyUndeclaredColumns:   envBool("GONVEX_DROP_EMPTY_UNDECLARED_COLUMNS", false),
		TelegramBotToken:             strings.TrimSpace(env("GONVEX_TELEGRAM_BOT_TOKEN", "")),
		TelegramChatID:               strings.TrimSpace(env("GONVEX_TELEGRAM_CHAT_ID", "")),
		TelegramAPIURL:               strings.TrimRight(env("GONVEX_TELEGRAM_API_URL", "https://api.telegram.org"), "/"),
		AlertCPUPercent:              envFloat("GONVEX_ALERT_CPU_PERCENT", 200),
		AlertTTLU:                    envDuration("GONVEX_ALERT_TTLU", 5*time.Second),
		AlertCooldown:                envDuration("GONVEX_ALERT_COOLDOWN", 15*time.Minute),
	}
}

func (cfg Config) TenantDatabaseURL(tenantID string) string {
	if tenantID != "" && cfg.TenantDatabases != nil {
		if value := cfg.TenantDatabases[tenantID]; value != "" {
			return value
		}
	}
	return cfg.DatabaseURL(tenantID)
}

func (cfg Config) DatabaseURL(projectID string) string {
	if projectID != "" && cfg.ProjectDatabases != nil {
		if value := cfg.ProjectDatabases[projectID]; value != "" {
			return value
		}
	}
	return cfg.PostgresURL
}

func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || os.Getenv(key) != "" {
			continue
		}
		os.Setenv(strings.TrimSpace(key), strings.TrimSpace(value))
	}
}

func env(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return duration
}

func envBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

func envFloat(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return fallback
	}
	return parsed
}

func envStringMap(key string) map[string]string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil
	}
	var parsed map[string]string
	if err := json.Unmarshal([]byte(value), &parsed); err != nil {
		return nil
	}
	return parsed
}

func envList(key string) []string {
	values := []string{}
	for _, value := range strings.Split(os.Getenv(key), ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}
