package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
	schemasync "github.com/gonvex/gonvex/server/internal/schema"
	"github.com/jackc/pgx/v5"
)

// A single-database project (no tenant database registered) declares its
// tables as tenant tables and serves them from the project database. The
// Skills Vault was in exactly this state: app code had created the tables, no
// deploy ever installed notify triggers, and with a healthy LISTEN connection
// every mutation was dropped as a no-op, so cached queries stayed stale.
func singleDatabaseTenantManifest(project string) manifest.Manifest {
	skills := map[string]manifest.Table{
		"skills": {Columns: map[string]manifest.Column{
			"id":          {Type: "id", PrimaryKey: true},
			"name":        {Type: "string"},
			"approved_at": {Type: "time", Nullable: true},
		}},
	}
	// Same shape the CLI generates for the vault: tables mirrors the tenant
	// tables and the landlord schema is empty.
	return manifest.Manifest{
		Project: project,
		Schema: manifest.Schema{
			Tables:         skills,
			LandlordTables: map[string]manifest.Table{},
			TenantTables:   skills,
		},
		Functions: map[string]manifest.FunctionEntry{
			"sync.skills": {
				Kind: manifest.FunctionKindSync,
				Sync: &manifest.SyncDefinition{Table: "skills", Key: "id", Columns: []string{"id", "name"}},
			},
		},
	}
}

func syncSingleDatabaseProject(t *testing.T, runtime *Server, current manifest.Manifest) map[string]any {
	t.Helper()
	payload, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/dev/sync", bytes.NewReader(payload))
	request.RemoteAddr = "127.0.0.1:41000" // unkeyed dev sync is loopback-only
	runtime.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("sync status %d: %s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func notifyTriggerCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`
		SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
		WHERE c.relname = $1 AND t.tgname LIKE 'gonvex_' || $1 || '_notify_%'
	`, table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// expectTableChangeNotification proves the end of the chain the runtime
// relies on: a committed write reaches LISTEN gonvex_table_change.
func expectTableChangeNotification(t *testing.T, databaseURL string, db *sql.DB, write string, table string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listener, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close(context.Background())
	if _, err := listener.Exec(ctx, "LISTEN "+schemasync.NotifyChannel); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(write); err != nil {
		t.Fatal(err)
	}
	notification, err := listener.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("no %s notification after %q: %v", schemasync.NotifyChannel, write, err)
	}
	if !strings.Contains(notification.Payload, `"`+table+`"`) {
		t.Fatalf("notification payload %s does not name table %s", notification.Payload, table)
	}
}

func TestDevSyncInstallsNotifyTriggersForSingleDatabaseTenantTables(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	databaseURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_single_db_notify_"+tenantRegistryTestSuffix(t))
	const project = "single-db-notify-project"
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// App code created the table before any deploy, and existing rows hold
	// NULL in the nullable column. The deploy must adopt it, not fail.
	if _, err := db.Exec(`
		CREATE TABLE skills (id text PRIMARY KEY, name text NOT NULL, approved_at timestamptz);
		INSERT INTO skills (id, name) VALUES ('pending', 'pending-skill');
	`); err != nil {
		t.Fatal(err)
	}

	runtime := New(config.Config{ProjectDatabases: map[string]string{project: databaseURL}})
	syncSingleDatabaseProject(t, runtime, singleDatabaseTenantManifest(project))

	if got := notifyTriggerCount(t, db, "skills"); got != 3 {
		t.Fatalf("project database has %d skills notify triggers, want 3", got)
	}
	expectTableChangeNotification(t, databaseURL, db, `UPDATE skills SET approved_at = now() WHERE id = 'pending'`, "skills")
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM skills`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("deploy changed existing rows: got %d, want 1", rows)
	}
}

func TestUnchangedDevSyncRepairsMissingSingleDatabaseNotifyTriggers(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	databaseURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_single_db_repair_"+tenantRegistryTestSuffix(t))
	const project = "single-db-repair-project"
	current := singleDatabaseTenantManifest(project)
	runtime := New(config.Config{ProjectDatabases: map[string]string{project: databaseURL}})
	syncSingleDatabaseProject(t, runtime, current)

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Reproduce a database deployed by an older runtime: sync storage is
	// present, so the unchanged-schema shortcut used to skip it forever.
	if _, err := db.Exec(`
		DROP TRIGGER gonvex_skills_notify_insert ON skills;
		DROP TRIGGER gonvex_skills_notify_update ON skills;
		DROP TRIGGER gonvex_skills_notify_delete ON skills;
	`); err != nil {
		t.Fatal(err)
	}

	response := syncSingleDatabaseProject(t, runtime, current)
	if skipped, _ := response["schemaSkipped"].(bool); skipped {
		t.Fatal("unchanged manifest skipped schema despite missing notify triggers")
	}
	if got := notifyTriggerCount(t, db, "skills"); got != 3 {
		t.Fatalf("unchanged sync left %d skills notify triggers, want 3", got)
	}
	expectTableChangeNotification(t, databaseURL, db, `INSERT INTO skills (id, name) VALUES ('fresh', 'fresh-skill')`, "skills")

	// Once everything is installed the shortcut still applies.
	response = syncSingleDatabaseProject(t, runtime, current)
	if skipped, _ := response["schemaSkipped"].(bool); !skipped {
		t.Fatal("fully installed unchanged manifest did not skip the schema reapply")
	}
}

func TestTenantSchemaForProjectDatabaseLeavesSharedLandlordTables(t *testing.T) {
	desired := manifest.Schema{
		LandlordTables: map[string]manifest.Table{
			"settings": {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}},
		},
		TenantTables: map[string]manifest.Table{
			"settings": {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}},
			"skills":   {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}},
		},
	}
	if got := sortedSchemaTableNames(tenantSchemaForTargets(desired, true)); strings.Join(got, ",") != "skills" {
		t.Fatalf("project-database tenant schema = %v, want [skills]", got)
	}
	if got := sortedSchemaTableNames(tenantSchemaForTargets(desired, false)); strings.Join(got, ",") != "settings,skills" {
		t.Fatalf("tenant-database schema = %v, want [settings skills]", got)
	}
}

func TestTenantSchemaTargetsFallBackToProjectDatabaseOnlyWithoutTenants(t *testing.T) {
	server := &Server{
		config: config.Config{ProjectDatabases: map[string]string{
			"solo":  "postgres://localhost/solo",
			"multi": "postgres://localhost/multi",
		}},
		tenants: map[string]tenantTarget{
			tenantStoreKey("multi", "acme"): {ID: "acme", ProjectID: "multi", databaseURL: "postgres://localhost/acme"},
		},
	}
	targets, projectDatabase := server.tenantSchemaTargets("solo")
	if !projectDatabase || len(targets) != 1 || targets[0].databaseURL != "postgres://localhost/solo" {
		t.Fatalf("solo targets = %+v (projectDatabase=%v), want the project database", targets, projectDatabase)
	}
	targets, projectDatabase = server.tenantSchemaTargets("multi")
	if projectDatabase || len(targets) != 1 || targets[0].databaseURL != "postgres://localhost/acme" {
		t.Fatalf("multi targets = %+v (projectDatabase=%v), want only the tenant database", targets, projectDatabase)
	}
}
