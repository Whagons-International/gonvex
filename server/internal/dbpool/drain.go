package dbpool

import (
	"database/sql"
	"sync"
	"weak"
)

// poolRegistry tracks every budgeted pool so a draining process can close
// their idle connections. Pools are held weakly: a pool that is closed and
// dropped by its owner disappears from the registry.
type poolRegistry struct {
	mu       sync.Mutex
	pools    []weak.Pointer[sql.DB]
	draining bool
}

var runtimePools = &poolRegistry{}

func (r *poolRegistry) add(db *sql.DB) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.draining {
		db.SetMaxIdleConns(0)
	}
	live := r.pools[:0]
	for _, pool := range r.pools {
		if pool.Value() != nil {
			live = append(live, pool)
		}
	}
	clear(r.pools[len(live):])
	r.pools = append(live, weak.Make(db))
}

func (r *poolRegistry) releaseIdle() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.draining = true
	released := 0
	for _, pool := range r.pools {
		if db := pool.Value(); db != nil {
			released += db.Stats().Idle
			// Zero closes the idle connections now and retains none from here
			// on: each connection is closed as soon as its query finishes.
			db.SetMaxIdleConns(0)
		}
	}
	return released
}

// DrainStats describes what BeginDrain released.
type DrainStats struct {
	IdleClosed int
	InUse      int
}

// BeginDrain prepares a runtime process that is being replaced (a supervised
// worker that received its drain signal) to hand PostgreSQL connections to
// its successor, which runs alongside it against the same max_connections
// with its own full budget: idle connections in every budgeted pool are
// closed and pools stop retaining idle connections, so from now on the
// process holds only connections that are executing a query.
//
// The budget itself is deliberately not lowered. A draining worker still
// serves its remaining WebSockets (and, during a gateway shutdown, all HTTP);
// starving it makes per-request checks such as app-auth revalidation fail and
// de-authenticate live connections.
func BeginDrain() DrainStats {
	idle := runtimePools.releaseIdle()
	return DrainStats{IdleClosed: idle, InUse: runtimeBudget.stats().Active}
}
