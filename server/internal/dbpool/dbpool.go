package dbpool

import (
	"database/sql"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	// The process-wide connection budget remains the hard safety boundary. A
	// two-connection per-database default leaves a single hot tenant limited to
	// four physical connections across its tenant and landlord pools, while most
	// of the safe process budget sits unused.
	defaultMaxOpen = 16
	defaultMaxIdle = 1
	defaultIdleTTL = 5 * time.Minute

	maxIdleEnvironmentVariable = "GONVEX_DB_MAX_IDLE_CONNS"
)

// Limits describes the application-side physical PostgreSQL connection pool.
// A MaxOpen value of zero is database/sql's documented unlimited setting.
type Limits struct {
	MaxOpen int
	MaxIdle int
}

// LimitsFromEnvironment returns bounded per-database limits. The runtime's
// process-wide connection budget is the aggregate safety boundary across all
// active tenant pools.
func LimitsFromEnvironment() Limits {
	limits := Limits{
		MaxOpen: positiveEnvironmentInt("GONVEX_DB_MAX_OPEN_CONNS", defaultMaxOpen),
		MaxIdle: environmentInt(maxIdleEnvironmentVariable, defaultMaxIdle),
	}
	if limits.MaxOpen > 0 && limits.MaxIdle > limits.MaxOpen {
		limits.MaxIdle = limits.MaxOpen
	}
	return limits
}

func positiveEnvironmentInt(name string, fallback int) int {
	value := environmentInt(name, fallback)
	if value == 0 {
		return fallback
	}
	return value
}

func Configure(db *sql.DB) {
	if db == nil {
		return
	}
	limits := LimitsFromEnvironment()
	db.SetMaxOpenConns(limits.MaxOpen)
	db.SetMaxIdleConns(limits.MaxIdle)
	db.SetConnMaxIdleTime(defaultIdleTTL)
}

func environmentInt(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return fallback
	}
	return value
}
