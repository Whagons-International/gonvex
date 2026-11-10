package server

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/gonvex/gonvex/server/internal/dbpool"
)

// Sync reads used to open (and close) a brand-new pool per call: one per
// commit notification plus two per sync delivery, so every delivery paid a
// fresh PostgreSQL session (fork, SCRAM, cold catalog caches). At 55 users
// that was 140+ new sessions a second. Sync reads now reuse one budgeted pool
// per tenant database, which keeps the shared warm idle connections.
var syncReadPools sync.Map // databaseURL -> *sql.DB

func syncReadDB(databaseURL string) (*sql.DB, error) {
	if pool, ok := syncReadPools.Load(databaseURL); ok {
		return pool.(*sql.DB), nil
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return nil, err
	}
	if actual, loaded := syncReadPools.LoadOrStore(databaseURL, db); loaded {
		_ = db.Close()
		return actual.(*sql.DB), nil
	}
	return db, nil
}

// syncClockReads batches concurrent clock reads per database. A caller never
// joins a read that was already running when it arrived (that read could
// predate the commit the caller is delivering); it waits for the next read,
// which every caller queued behind the same running read then shares. A burst
// of deliveries after one commit therefore costs about two clock queries.
type syncClockBatch struct {
	done  chan struct{}
	clock syncClock
	err   error
}

type syncClockReader struct {
	mu      sync.Mutex
	running *syncClockBatch
	next    *syncClockBatch
}

var syncClockReaders sync.Map // databaseURL -> *syncClockReader

func batchedSyncClock(ctx context.Context, databaseURL string, read func(context.Context) (syncClock, error)) (syncClock, error) {
	value, _ := syncClockReaders.LoadOrStore(databaseURL, &syncClockReader{})
	reader := value.(*syncClockReader)
	reader.mu.Lock()
	var batch *syncClockBatch
	if reader.running == nil {
		batch = &syncClockBatch{done: make(chan struct{})}
		reader.running = batch
		reader.mu.Unlock()
		reader.start(batch, read)
	} else {
		if reader.next == nil {
			reader.next = &syncClockBatch{done: make(chan struct{})}
		}
		batch = reader.next
		reader.mu.Unlock()
	}
	select {
	case <-batch.done:
		return batch.clock, batch.err
	case <-ctx.Done():
		return syncClock{}, ctx.Err()
	}
}

// start runs one read detached from any single caller, then starts the batch
// that queued behind it, if any.
func (reader *syncClockReader) start(batch *syncClockBatch, read func(context.Context) (syncClock, error)) {
	go func() {
		readCtx, cancel := context.WithTimeout(context.Background(), syncDeliveryTimeout)
		batch.clock, batch.err = read(readCtx)
		cancel()
		reader.mu.Lock()
		close(batch.done)
		next := reader.next
		reader.next = nil
		reader.running = next
		reader.mu.Unlock()
		if next != nil {
			reader.start(next, read)
		}
	}()
}

func readSyncClockFromDB(ctx context.Context, db *sql.DB) (syncClock, error) {
	var clock syncClock
	if err := db.QueryRowContext(
		ctx,
		`SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = true`,
	).Scan(&clock.DatabaseEpoch, &clock.Revision, &clock.RetainedRevision); err != nil {
		return syncClock{}, fmt.Errorf("sync storage is not installed: %w", err)
	}
	return clock, nil
}
