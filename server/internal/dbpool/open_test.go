package dbpool

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

func TestConnectionBudgetBlocksAtLimitAndResumesAfterRelease(t *testing.T) {
	budget := &connectionBudget{limit: func() int { return 2 }}
	if err := budget.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := budget.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := budget.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire at limit returned %v, want context deadline exceeded", err)
	}
	stats := budget.stats()
	if stats.Limit != 2 || stats.Active != 2 || stats.Waiters != 0 || stats.WaitCount != 1 {
		t.Fatalf("budget stats after timeout = %+v", stats)
	}
	if stats.WaitDuration < 15*time.Millisecond {
		t.Fatalf("budget wait duration = %s, want at least 15ms", stats.WaitDuration)
	}

	budget.release()
	if err := budget.acquire(context.Background()); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	budget.release()
	budget.release()
}

func TestTotalConnectionBudgetDoesNotAllowUnlimitedConfiguration(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_TOTAL_CONNS", "0")
	if got := runtimeBudget.limit(); got != defaultMaxTotal {
		t.Fatalf("total connection limit = %d, want %d", got, defaultMaxTotal)
	}
}

func TestTotalConnectionBudgetHonorsBoundedDeploymentConfiguration(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_TOTAL_CONNS", "48")
	if got := runtimeBudget.limit(); got != 48 {
		t.Fatalf("total connection limit = %d, want configured limit 48", got)
	}
}

func TestTotalConnectionBudgetCannotExceedRuntimeSafetyCeiling(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_TOTAL_CONNS", "200")
	if got := runtimeBudget.limit(); got != maxTotalSafetyCeiling {
		t.Fatalf("total connection limit = %d, want safety ceiling %d", got, maxTotalSafetyCeiling)
	}
}

func TestBudgetedPoolRetainsAtMostOneWarmConnection(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "2")
	budget := &connectionBudget{limit: func() int { return 1 }}
	db := sql.OpenDB(&limitedConnector{connector: testConnector{}, budget: budget})
	configureBudgeted(db)
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	budget.mu.Lock()
	active := budget.active
	budget.mu.Unlock()
	if active != 1 {
		t.Fatalf("retained connections after request = %d, want 1", active)
	}

	stats := db.Stats()
	if stats.Idle != 1 {
		t.Fatalf("idle connections after request = %d, want 1", stats.Idle)
	}
}

type testConnector struct{}

func (testConnector) Connect(context.Context) (driver.Conn, error) { return testConn{}, nil }
func (testConnector) Driver() driver.Driver                        { return testDriver{} }

type testDriver struct{}

func (testDriver) Open(string) (driver.Conn, error) { return testConn{}, nil }

type testConn struct{}

func (testConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (testConn) Close() error                        { return nil }
func (testConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (testConn) Ping(context.Context) error          { return nil }

func TestBudgetedPoolHonorsExplicitMaxIdleEnvironment(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_OPEN_CONNS", "")
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "2")
	if got := retainedIdleConnections(t, configureBudgeted); got != 2 {
		t.Fatalf("idle connections with GONVEX_DB_MAX_IDLE_CONNS=2 = %d, want 2", got)
	}

	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "")
	if got := retainedIdleConnections(t, configureBudgeted); got != budgetedMaxIdle {
		t.Fatalf("idle connections without an override = %d, want budgeted default %d", got, budgetedMaxIdle)
	}
}

func TestSecondaryPoolMaxIdleNeverExceedsOperatorLimit(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_OPEN_CONNS", "")
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "")
	if got := boundedMaxIdle(1); got != 1 {
		t.Fatalf("boundedMaxIdle(1) = %d, want 1", got)
	}
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "0")
	if got := boundedMaxIdle(1); got != 0 {
		t.Fatalf("boundedMaxIdle(1) with GONVEX_DB_MAX_IDLE_CONNS=0 = %d, want 0", got)
	}
	configure := func(db *sql.DB) { configureBudgetedWithMaxIdle(db, boundedMaxIdle(1)) }
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "")
	if got := retainedIdleConnections(t, configure); got != 1 {
		t.Fatalf("secondary pool idle connections = %d, want 1", got)
	}
}

// retainedIdleConnections opens five concurrent connections through a
// configured budgeted pool, releases them, and reports how many stay warm.
func retainedIdleConnections(t *testing.T, configure func(*sql.DB)) int {
	t.Helper()
	budget := &connectionBudget{limit: func() int { return 10 }}
	db := sql.OpenDB(&limitedConnector{connector: testConnector{}, budget: budget})
	configure(db)
	defer db.Close()
	connections := make([]*sql.Conn, 0, 5)
	for range 5 {
		connection, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, connection)
	}
	for _, connection := range connections {
		_ = connection.Close()
	}
	return db.Stats().Idle
}
