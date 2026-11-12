package server

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
)

func TestSyncReadPoolKeepsOneWarmConnection(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("GONVEX_ERROR_TEST_POSTGRES_URL"))
	if databaseURL == "" {
		t.Skip("GONVEX_ERROR_TEST_POSTGRES_URL is not set")
	}
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "")
	separator := "?"
	if strings.Contains(databaseURL, "?") {
		separator = "&"
	}
	// A distinct URL gets a fresh pool from the process-wide sync-read cache.
	databaseURL += separator + "application_name=gonvex_sync_read_pool_test"
	db, err := syncReadDB(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		syncReadPools.Delete(databaseURL)
		_ = db.Close()
	})
	connections := make([]*sql.Conn, 0, 3)
	for range 3 {
		connection, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := connection.PingContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		connections = append(connections, connection)
	}
	for _, connection := range connections {
		_ = connection.Close()
	}
	if idle := db.Stats().Idle; idle != syncReadPoolMaxIdle {
		t.Fatalf("sync-read pool idle connections = %d, want %d", idle, syncReadPoolMaxIdle)
	}
}
