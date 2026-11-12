package server

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
	schemasync "github.com/gonvex/gonvex/server/internal/schema"
	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// A visibility change used to reset eager syncs: the client dropped its rows
// and reopened, which re-sent the whole snapshot and closed dialogs bound to
// those rows. Syncs now reconcile and receive only the difference, and a
// landlord-table change (which the tenant change log never records) forces
// that reconcile; progressive syncs used to skip it entirely.
func TestVisibilityChangeReconcilesSyncsInsteadOfResetting(t *testing.T) {
	for _, mode := range []string{"eager", "progressive"} {
		t.Run(mode, func(t *testing.T) {
			harness := newVisibilityReconcileHarness(t, mode)
			harness.setVisible("row-1")
			harness.connection.openSyncWithClock(harness.ctx, clientMessage{ID: "sync-1", Path: harness.path}, harness.databaseURL, harness.clock(t))
			opened := readSyncTestFrames(t, harness.peer, 2)
			if opened[0].Type != "sync.snapshot" || opened[1].Type != "sync.ready" {
				t.Fatalf("open frames = %s, %s", opened[0].Type, opened[1].Type)
			}

			// A tenant visibility table: the change is in the durable log.
			harness.setVisible("row-1", "row-2")
			if _, err := harness.db.Exec(`INSERT INTO ` + quoteIdent(harness.visibilityTable) + ` ("id", "value") VALUES ('grant-1', 'row-2')`); err != nil {
				t.Fatal(err)
			}
			harness.runtime.resetSyncsForVisibilityChange(tableChange{
				project: harness.project, tenant: harness.project, table: harness.visibilityTable,
				operation: "insert", rowIDs: map[string]bool{"grant-1": true},
			})
			harness.expectReconcile(t, []string{"row-2"}, nil)

			// A landlord visibility table: nothing reaches the tenant change log.
			harness.setVisible("row-2")
			harness.runtime.resetSyncsForVisibilityChange(tableChange{
				project: harness.project, tenant: harness.project, table: "users", broad: true,
			})
			harness.expectReconcile(t, nil, []string{"row-1"})
		})
	}
}

type visibilityReconcileHarness struct {
	ctx             context.Context
	runtime         *Server
	connection      *wsConn
	peer            *websocket.Conn
	db              *sql.DB
	databaseURL     string
	project         string
	path            string
	visibilityTable string
	mu              sync.Mutex
	visible         []string
}

func newVisibilityReconcileHarness(t *testing.T, mode string) *visibilityReconcileHarness {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if databaseURL == "" {
		databaseURL = strings.TrimSpace(os.Getenv("GONVEX_ERROR_TEST_POSTGRES_URL"))
	}
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL or GONVEX_ERROR_TEST_POSTGRES_URL is not configured")
	}
	suffix := time.Now().UnixNano()
	harness := &visibilityReconcileHarness{
		ctx: context.Background(), databaseURL: databaseURL,
		project:         fmt.Sprintf("visibility-project-%d", suffix),
		path:            "sync.visibleRows",
		visibilityTable: fmt.Sprintf("visibility_grants_%d", suffix),
	}
	sourceTable := fmt.Sprintf("visibility_rows_%d", suffix)
	columns := map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}, "value": {Type: "string"}}
	tenantTables := map[string]manifest.Table{
		sourceTable:             {Columns: columns},
		harness.visibilityTable: {Columns: columns},
	}
	definition := manifest.SyncDefinition{
		Table: sourceTable, Key: "id", Columns: []string{"id", "value"}, Mode: mode,
		VisibilityTables: []string{harness.visibilityTable, "users"},
	}
	logged, err := syncDefinitionsForSchema(map[string]manifest.SyncDefinition{sourceTable: definition}, manifest.Schema{Tables: tenantTables})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := schemasync.ApplyWithSync(harness.ctx, databaseURL, manifest.Schema{Tables: tenantTables}, logged); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	harness.db = db
	t.Cleanup(func() {
		for _, table := range []string{sourceTable, harness.visibilityTable} {
			_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(table))
			_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent("gonvex_sync_"+table+"_stage") + `()`)
		}
		_ = db.Close()
	})

	app := gonvex.NewApp()
	app.Sync(harness.path, func(*gonvex.QueryCtx, struct{}) ([]map[string]any, error) {
		harness.mu.Lock()
		defer harness.mu.Unlock()
		rows := make([]map[string]any, 0, len(harness.visible))
		for _, id := range harness.visible {
			rows = append(rows, map[string]any{"id": id, "value": "visible"})
		}
		return rows, nil
	}, gonvex.SyncTable(sourceTable).Columns("id", "value"))
	harness.runtime = NewWithApp(config.Config{
		ProjectDatabases:          map[string]string{harness.project: databaseURL},
		TenantListenerLimit:       1,
		TenantListenerIdleTimeout: time.Hour,
	}, app)
	if err := harness.runtime.runtime.SyncManifest(manifest.Manifest{
		Project: harness.project,
		Functions: map[string]manifest.FunctionEntry{
			harness.path: {Kind: manifest.FunctionKindSync, Sync: &definition},
		},
		Schema: manifest.Schema{
			Tables: tenantTables, TenantTables: tenantTables,
			LandlordTables: map[string]manifest.Table{"users": {Columns: columns}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	close(ready)
	listenerKey := tenantListenerKey{project: harness.project, tenant: harness.project}
	harness.runtime.subscriptions.listeners.active[listenerKey] = &tenantListener{
		key: listenerKey, databaseURL: databaseURL, ready: ready, connected: true, cancel: func() {},
	}
	t.Cleanup(func() {
		manager := harness.runtime.subscriptions.listeners
		manager.mu.Lock()
		delete(manager.active, listenerKey)
		manager.mu.Unlock()
		harness.runtime.tenantStores.Close()
	})
	harness.connection, harness.peer = newSyncReadyTestConnection(t, false)
	harness.connection.server = harness.runtime
	harness.connection.project = harness.project
	harness.connection.tenant = harness.project
	harness.runtime.addWSConn(harness.connection)
	t.Cleanup(func() { harness.runtime.removeWSConn(harness.connection) })
	return harness
}

func (h *visibilityReconcileHarness) setVisible(ids ...string) {
	h.mu.Lock()
	h.visible = ids
	h.mu.Unlock()
}

func (h *visibilityReconcileHarness) clock(t *testing.T) syncClock {
	t.Helper()
	clock, err := currentSyncClock(h.ctx, h.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return clock
}

func (h *visibilityReconcileHarness) expectReconcile(t *testing.T, upserts, deleted []string) {
	t.Helper()
	frames := readSyncTestFrames(t, h.peer, 3)
	types := []string{frames[0].Type, frames[1].Type, frames[2].Type}
	if strings.Join(types, ",") != "sync.syncing,sync.delta,sync.ready" {
		t.Fatalf("visibility change frames = %v, want syncing, delta, ready (never sync.reset)", types)
	}
	gotUpserts := make([]string, 0, len(frames[1].Upserts))
	for _, row := range frames[1].Upserts {
		gotUpserts = append(gotUpserts, syncRowKey(row, "id"))
	}
	sort.Strings(gotUpserts)
	if strings.Join(gotUpserts, ",") != strings.Join(upserts, ",") || strings.Join(frames[1].Deleted, ",") != strings.Join(deleted, ",") {
		t.Fatalf("reconcile delta upserts=%v deleted=%v, want upserts=%v deleted=%v", gotUpserts, frames[1].Deleted, upserts, deleted)
	}
}
