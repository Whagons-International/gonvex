package dbpool

import (
	"context"
	"database/sql"
	"testing"
)

func TestReleaseIdleClosesIdleConnectionsAndStopsRetainingThem(t *testing.T) {
	budget := &connectionBudget{limit: func() int { return 10 }}
	registry := &poolRegistry{}
	open := func() *sql.DB {
		db := sql.OpenDB(&limitedConnector{connector: testConnector{}, budget: budget})
		configureBudgeted(db)
		registry.add(db)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	first, second := open(), open()
	for _, db := range []*sql.DB{first, second} {
		if err := db.PingContext(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := budget.stats().Active; got != 2 {
		t.Fatalf("warm connections before drain = %d, want 2", got)
	}

	if released := registry.releaseIdle(); released != 2 {
		t.Fatalf("released idle = %d, want 2", released)
	}
	if got := budget.stats().Active; got != 0 {
		t.Fatalf("connections held after releasing idle = %d, want 0", got)
	}
	if err := first.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := first.Stats().Idle; got != 0 {
		t.Fatalf("draining pool retained %d idle connections, want 0", got)
	}
	// A pool opened after the drain began must not retain idle connections either.
	late := open()
	if err := late.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := late.Stats().Idle; got != 0 || budget.stats().Active != 0 {
		t.Fatalf("pool opened during drain retained idle=%d active=%d, want 0", got, budget.stats().Active)
	}
}
