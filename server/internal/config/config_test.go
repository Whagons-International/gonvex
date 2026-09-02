package config

import (
	"testing"
	"time"
)

func TestDefaultRowsCacheTTLAllowsInvalidationToDriveFreshness(t *testing.T) {
	if defaultRowsCacheTTL != 10*time.Minute {
		t.Fatalf("defaultRowsCacheTTL = %s, want 10m", defaultRowsCacheTTL)
	}
}

func TestDefaultSubscriptionRerunConcurrency(t *testing.T) {
	t.Setenv("GONVEX_SUBSCRIPTION_RERUN_CONCURRENCY", "")
	if got := FromEnv().SubscriptionRerunConcurrency; got != 32 {
		t.Fatalf("SubscriptionRerunConcurrency = %d, want 32", got)
	}
}

func TestValkeyURLAcceptsRedisURLAlias(t *testing.T) {
	t.Setenv("VALKEY_URL", "")
	t.Setenv("REDIS_URL", "redis://redis.example:6379/4")
	if got := FromEnv().ValkeyURL; got != "redis://redis.example:6379/4" {
		t.Fatalf("ValkeyURL = %q", got)
	}

	t.Setenv("VALKEY_URL", "redis://valkey.example:6379/1")
	if got := FromEnv().ValkeyURL; got != "redis://valkey.example:6379/1" {
		t.Fatalf("VALKEY_URL did not take precedence: %q", got)
	}
}

func TestEnvFloatRejectsNonFiniteValues(t *testing.T) {
	for _, value := range []string{"NaN", "+Inf", "-Inf"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("GONVEX_TEST_FLOAT", value)
			if got := envFloat("GONVEX_TEST_FLOAT", 200); got != 200 {
				t.Fatalf("envFloat(%q) = %v, want fallback", value, got)
			}
		})
	}
}
