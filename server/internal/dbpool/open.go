package dbpool

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultMaxTotal       = 20
	maxTotalSafetyCeiling = 128
)

// budgetedIdleTTL and budgetedMaxIdle keep a few connections warm per active
// database. Opening a task fans out ~20 parallel queries; with one warm
// connection the rest were opened fresh (~70ms each in production) and queued
// behind the pool, ~982k pool waits a day (2026-11-03). Idle connections still
// hold shared budget slots, so the TTL stays short enough for dormant tenants
// to give them back between maintenance ticks.
const (
	budgetedIdleTTL = 30 * time.Second
	budgetedMaxIdle = 3
)

var runtimeBudget = &connectionBudget{limit: func() int {
	configured := positiveEnvironmentInt("GONVEX_DB_MAX_TOTAL_CONNS", defaultMaxTotal)
	// The runtime shares PostgreSQL with its control plane, migrations, and
	// often other services. Keep a conservative default, but allow an operator
	// to allocate more of a known PostgreSQL connection budget. The absolute
	// ceiling still prevents a typo from creating an effectively unbounded pool.
	if configured > maxTotalSafetyCeiling {
		return maxTotalSafetyCeiling
	}
	return configured
}}

// Open creates a PostgreSQL pool whose physical connections count against the
// process-wide budget shared by every runtime database pool.
func Open(databaseURL string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	applySessionDefaults(config)
	db := sql.OpenDB(&limitedConnector{
		connector: stdlib.GetConnector(*config),
		budget:    runtimeBudget,
	})
	configureBudgeted(db)
	runtimePools.add(db)
	return db, nil
}

func configureBudgeted(db *sql.DB) {
	configureBudgetedWithMaxIdle(db, budgetedMaxIdleFromEnvironment())
}

// budgetedMaxIdleFromEnvironment returns the warm-connection count for a
// budgeted pool. An explicit GONVEX_DB_MAX_IDLE_CONNS wins: production set it
// to 2, but the budgeted default of 3 silently overrode it, so every tenant
// database kept one more idle PostgreSQL session than the operator allowed.
func budgetedMaxIdleFromEnvironment() int {
	limits := LimitsFromEnvironment()
	maxIdle := budgetedMaxIdle
	if environmentInt(maxIdleEnvironmentVariable, -1) >= 0 {
		maxIdle = limits.MaxIdle
	}
	if limits.MaxOpen > 0 && maxIdle > limits.MaxOpen {
		maxIdle = limits.MaxOpen
	}
	return maxIdle
}

// OpenWithMaxIdle is Open with a lower ceiling on warm connections. Secondary
// pools to a database that already has a primary pool (the sync-read pool)
// use it so they do not double the idle sessions held per tenant database.
func OpenWithMaxIdle(databaseURL string, maxIdle int) (*sql.DB, error) {
	db, err := Open(databaseURL)
	if err != nil {
		return nil, err
	}
	configureBudgetedWithMaxIdle(db, boundedMaxIdle(maxIdle))
	return db, nil
}

// boundedMaxIdle never lets a caller keep more warm connections than the
// operator's budgeted limit allows.
func boundedMaxIdle(maxIdle int) int {
	if current := budgetedMaxIdleFromEnvironment(); maxIdle > current {
		return current
	}
	return maxIdle
}

func configureBudgetedWithMaxIdle(db *sql.DB, maxIdle int) {
	Configure(db)
	if maxIdle < 0 {
		maxIdle = 0
	}
	// Keep a few connections warm for an active database so a query burst does not
	// create a fresh TCP/TLS PostgreSQL session per operation. A retained
	// connection still occupies the process-wide budget, so expire it quickly:
	// when more databases are active than the budget can hold, dormant pools
	// release their slots instead of blocking another tenant indefinitely.
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxIdleTime(budgetedIdleTTL)
}

// PGXConn unwraps a connection obtained through sql.Conn.Raw.
func PGXConn(raw any) (*pgx.Conn, bool) {
	limited, ok := raw.(*limitedConn)
	if !ok {
		return nil, false
	}
	connection, ok := limited.Conn.(*stdlib.Conn)
	if !ok {
		return nil, false
	}
	return connection.Conn(), true
}

type connectionBudget struct {
	mu           sync.Mutex
	active       int
	waiters      int
	waitCount    uint64
	waitDuration time.Duration
	changed      chan struct{}
	limit        func() int
}

func (b *connectionBudget) acquire(ctx context.Context) error {
	waitStarted := time.Now()
	waiting := false
	for {
		b.mu.Lock()
		if b.changed == nil {
			b.changed = make(chan struct{})
		}
		if b.active < b.limit() {
			b.active++
			if waiting {
				b.finishWaitLocked(waitStarted)
			}
			b.mu.Unlock()
			return nil
		}
		if !waiting {
			b.waiters++
			waiting = true
		}
		changed := b.changed
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			b.mu.Lock()
			if waiting {
				b.finishWaitLocked(waitStarted)
			}
			b.mu.Unlock()
			return ctx.Err()
		case <-changed:
		}
	}
}

func (b *connectionBudget) finishWaitLocked(startedAt time.Time) {
	if b.waiters > 0 {
		b.waiters--
	}
	b.waitCount++
	b.waitDuration += time.Since(startedAt)
}

// BudgetStats reports process-wide physical connection pressure. SQL pool
// statistics do not include time spent waiting for this cross-pool gate.
type BudgetStats struct {
	Limit        int
	Active       int
	Waiters      int
	WaitCount    uint64
	WaitDuration time.Duration
}

func (b *connectionBudget) stats() BudgetStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BudgetStats{
		Limit:        b.limit(),
		Active:       b.active,
		Waiters:      b.waiters,
		WaitCount:    b.waitCount,
		WaitDuration: b.waitDuration,
	}
}

// RuntimeBudgetStats returns the aggregate connection budget for this runtime
// process, across landlord, registry, telemetry, and tenant database pools.
func RuntimeBudgetStats() BudgetStats {
	return runtimeBudget.stats()
}

func (b *connectionBudget) release() {
	b.mu.Lock()
	if b.active > 0 {
		b.active--
	}
	if b.changed != nil {
		close(b.changed)
		b.changed = make(chan struct{})
	}
	b.mu.Unlock()
}

type limitedConnector struct {
	connector driver.Connector
	budget    *connectionBudget
}

func (c *limitedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := c.budget.acquire(ctx); err != nil {
		return nil, err
	}
	connection, err := c.connector.Connect(ctx)
	if err != nil {
		c.budget.release()
		return nil, err
	}
	return &limitedConn{Conn: connection, budget: c.budget}, nil
}

func (c *limitedConnector) Driver() driver.Driver {
	return c.connector.Driver()
}

type limitedConn struct {
	driver.Conn
	budget *connectionBudget
	once   sync.Once
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.budget.release)
	return err
}

func (c *limitedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if connection, ok := c.Conn.(driver.ConnBeginTx); ok {
		return connection.BeginTx(ctx, opts)
	}
	return nil, driver.ErrSkip
}

func (c *limitedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if connection, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return connection.PrepareContext(ctx, query)
	}
	return nil, driver.ErrSkip
}

func (c *limitedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if connection, ok := c.Conn.(driver.ExecerContext); ok {
		return connection.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *limitedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if connection, ok := c.Conn.(driver.QueryerContext); ok {
		return connection.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *limitedConn) Ping(ctx context.Context) error {
	if connection, ok := c.Conn.(driver.Pinger); ok {
		return connection.Ping(ctx)
	}
	return nil
}

func (c *limitedConn) ResetSession(ctx context.Context) error {
	if connection, ok := c.Conn.(driver.SessionResetter); ok {
		return connection.ResetSession(ctx)
	}
	return nil
}

func (c *limitedConn) IsValid() bool {
	if connection, ok := c.Conn.(driver.Validator); ok {
		return connection.IsValid()
	}
	return true
}

func (c *limitedConn) CheckNamedValue(value *driver.NamedValue) error {
	if connection, ok := c.Conn.(driver.NamedValueChecker); ok {
		return connection.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

// applySessionDefaults turns PostgreSQL JIT off for runtime sessions unless the
// URL or GONVEX_PG_JIT says otherwise. Runtime statements are short OLTP reads
// whose plan costs still cross jit_above_cost (correlated per-row projections
// over a workspace window): compiling them took ~126 ms of a 157 ms query, and
// about half of all PostgreSQL execution time in the load lab.
func applySessionDefaults(config *pgx.ConnConfig) {
	if config.RuntimeParams == nil {
		config.RuntimeParams = map[string]string{}
	}
	if _, explicit := config.RuntimeParams["jit"]; explicit {
		return
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("GONVEX_PG_JIT"))) {
	case "on", "true", "1", "server":
		return // keep the server's setting
	}
	config.RuntimeParams["jit"] = "off"
}
