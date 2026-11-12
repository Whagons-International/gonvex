package dbpool

import (
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestApplySessionDefaultsDisablesJITUnlessConfigured(t *testing.T) {
	parse := func(url string) *pgx.ConnConfig {
		config, err := pgx.ParseConfig(url)
		if err != nil {
			t.Fatal(err)
		}
		applySessionDefaults(config)
		return config
	}
	t.Setenv("GONVEX_PG_JIT", "")
	if got := parse("postgres://u:p@127.0.0.1/db").RuntimeParams["jit"]; got != "off" {
		t.Fatalf("default jit = %q, want off", got)
	}
	if got := parse("postgres://u:p@127.0.0.1/db?jit=on").RuntimeParams["jit"]; got != "on" {
		t.Fatalf("explicit URL jit = %q, want on", got)
	}
	t.Setenv("GONVEX_PG_JIT", "server")
	if _, set := parse("postgres://u:p@127.0.0.1/db").RuntimeParams["jit"]; set {
		t.Fatal("GONVEX_PG_JIT=server must leave the server setting")
	}
}
