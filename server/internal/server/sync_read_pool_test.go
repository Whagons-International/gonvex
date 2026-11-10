package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Callers that arrive while a clock read is running must not reuse it: it may
// predate the commit they deliver. They share the next read instead.
func TestBatchedSyncClockNeverServesAReadStartedBeforeTheCaller(t *testing.T) {
	url := "postgres://batched-clock-test/" + t.Name()
	var reads atomic.Int64
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	read := func(context.Context) (syncClock, error) {
		n := reads.Add(1)
		if n == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		return syncClock{Revision: uint64(n)}, nil
	}
	first := make(chan syncClock, 1)
	go func() {
		clock, _ := batchedSyncClock(context.Background(), url, read)
		first <- clock
	}()
	<-firstStarted

	var wg sync.WaitGroup
	late := make(chan uint64, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clock, err := batchedSyncClock(context.Background(), url, read)
			if err != nil {
				t.Error(err)
			}
			late <- clock.Revision
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(releaseFirst)
	wg.Wait()
	close(late)
	if got := (<-first).Revision; got != 1 {
		t.Fatalf("first caller revision = %d, want 1", got)
	}
	for revision := range late {
		if revision != 2 {
			t.Fatalf("late caller got revision %d from a read that started before it, want 2", revision)
		}
	}
	if got := reads.Load(); got != 2 {
		t.Fatalf("reads = %d, want 2 (one running, one shared by every late caller)", got)
	}
}
