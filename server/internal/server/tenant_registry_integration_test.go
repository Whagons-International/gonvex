package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
	schemasync "github.com/gonvex/gonvex/server/internal/schema"
)

func TestProvisionExistingTenantRepairsDurableSyncStorage(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	databaseURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_sync_repair_"+tenantRegistryTestSuffix(t))
	const project = "sync-repair-project"
	desired := manifest.Schema{Tables: map[string]manifest.Table{
		"priorities": {Columns: map[string]manifest.Column{
			"id":       {Type: "string", PrimaryKey: true},
			"tenantId": {Type: "string"},
		}},
	}}
	// Reproduce a tenant created before durable sync: its application schema
	// exists, but the runtime clock/change-log tables do not.
	if _, err := schemasync.Apply(context.Background(), databaseURL, desired); err != nil {
		t.Fatal(err)
	}
	server := New(config.Config{})
	if err := server.runtime.SyncManifest(manifest.Manifest{
		Project: project,
		Schema:  desired,
		Functions: map[string]manifest.FunctionEntry{
			"sync.priorities": {
				Kind: manifest.FunctionKindSync,
				Sync: &manifest.SyncDefinition{
					Table: "priorities", Key: "id", Columns: []string{"id", "tenantId"},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := server.provisionTenantDatabaseWithSync(context.Background(), project, databaseURL); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var clockExists bool
	if err := db.QueryRow(`SELECT to_regclass('_gonvex_sync_clock') IS NOT NULL`).Scan(&clockExists); err != nil {
		t.Fatal(err)
	}
	if !clockExists {
		t.Fatal("existing tenant provisioning did not install durable sync storage")
	}
}

func TestUnchangedDevSyncRepairsMissingDurableSyncStorage(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	databaseURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_sync_skip_repair_"+tenantRegistryTestSuffix(t))
	const project = "sync-skip-repair-project"
	current := manifest.Manifest{
		Project: project,
		Schema: manifest.Schema{Tables: map[string]manifest.Table{
			"priorities": {Columns: map[string]manifest.Column{
				"id": {Type: "string", PrimaryKey: true},
			}},
		}},
		Functions: map[string]manifest.FunctionEntry{
			"sync.priorities": {
				Kind: manifest.FunctionKindSync,
				Sync: &manifest.SyncDefinition{Table: "priorities", Key: "id", Columns: []string{"id"}},
			},
		},
	}
	payload, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(config.Config{ProjectDatabases: map[string]string{project: databaseURL}})
	syncProject := func() map[string]any {
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

	syncProject()
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`DROP TABLE _gonvex_sync_changes, _gonvex_sync_clock`); err != nil {
		t.Fatal(err)
	}

	response := syncProject()
	if skipped, _ := response["schemaSkipped"].(bool); skipped {
		t.Fatal("unchanged manifest skipped schema despite missing durable sync storage")
	}
	var repaired bool
	if err := db.QueryRow(`SELECT to_regclass('_gonvex_sync_clock') IS NOT NULL AND to_regclass('_gonvex_sync_changes') IS NOT NULL`).Scan(&repaired); err != nil {
		t.Fatal(err)
	}
	if !repaired {
		t.Fatal("unchanged sync did not restore durable sync storage")
	}
}

func tenantRegistryTestPostgresURL(t *testing.T) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv("GONVEX_TEST_POSTGRES_URL"))
	if value == "" {
		t.Skip("set GONVEX_TEST_POSTGRES_URL to run PostgreSQL tenant-registry integration tests")
	}
	return value
}

func createTenantRegistryTestDatabase(t *testing.T, baseURL string, name string) string {
	t.Helper()
	databaseURL, err := createProjectDatabase(context.Background(), baseURL, name)
	if err != nil {
		t.Fatalf("create test database %s: %v", name, err)
	}
	t.Cleanup(func() {
		if err := dropProjectDatabase(context.Background(), baseURL, name); err != nil {
			t.Errorf("drop test database %s: %v", name, err)
		}
	})
	return databaseURL
}

func tenantRegistryTestSuffix(t *testing.T) string {
	t.Helper()
	id, err := generateRelationshipID()
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(id[:18], "-", "_")
}

func TestPostgresUUIDv6ProjectIgnoresUnrelatedAppDatabase(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	suffix := tenantRegistryTestSuffix(t)
	controlName := "gonvex_rel_control_" + suffix
	unrelatedName := "phantom_tenant_" + suffix
	controlURL := createTenantRegistryTestDatabase(t, baseURL, controlName)
	unrelatedURL := createTenantRegistryTestDatabase(t, baseURL, unrelatedName)

	unrelatedDB, err := sql.Open("pgx", unrelatedURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unrelatedDB.ExecContext(context.Background(), `CREATE TABLE tasks (id TEXT PRIMARY KEY); CREATE TABLE workspaces (id TEXT PRIMARY KEY)`); err != nil {
		_ = unrelatedDB.Close()
		t.Fatalf("seed unrelated app database: %v", err)
	}
	_ = unrelatedDB.Close()

	cfg := config.Config{
		LandlordURL:      controlURL,
		PostgresURL:      baseURL,
		ProjectDatabases: map[string]string{},
		ProjectKeys:      map[string]string{},
	}
	server := New(cfg)
	createProjectRequest := httptest.NewRequest(http.MethodPost, "/dev/projects", bytes.NewBufferString(`{"name":"Relationship Test","databaseMode":"multiTenant"}`))
	createProjectRequest.Header.Set("content-type", "application/json")
	createProjectRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(createProjectRecorder, createProjectRequest)
	if createProjectRecorder.Code != http.StatusCreated {
		t.Fatalf("create project: expected %d, got %d: %s", http.StatusCreated, createProjectRecorder.Code, createProjectRecorder.Body.String())
	}
	var createdProject createProjectResponse
	if err := json.NewDecoder(createProjectRecorder.Body).Decode(&createdProject); err != nil {
		t.Fatalf("decode created project: %v", err)
	}
	projectID := createdProject.Project.ID
	if !isUUIDv6(projectID) {
		t.Fatalf("new project id is not UUIDv6: %q", projectID)
	}
	t.Cleanup(func() {
		if err := dropProjectDatabase(context.Background(), baseURL, createdProject.Project.Database); err != nil {
			t.Errorf("drop created project database %s: %v", createdProject.Project.Database, err)
		}
	})

	server.hydrateProjectTenantDatabases(context.Background(), projectID)
	server.projectMu.RLock()
	for _, tenant := range server.tenants {
		if tenant.ProjectID == projectID {
			server.projectMu.RUnlock()
			t.Fatalf("UUIDv6 project adopted unrelated database %q as tenant %+v", unrelatedName, tenant)
		}
	}
	server.projectMu.RUnlock()

	createTenantRequest := httptest.NewRequest(http.MethodPost, "/dev/tenants", bytes.NewBufferString(`{"name":"Acme","projectId":"`+projectID+`"}`))
	createTenantRequest.Header.Set("content-type", "application/json")
	createTenantRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(createTenantRecorder, createTenantRequest)
	if createTenantRecorder.Code != http.StatusCreated {
		t.Fatalf("create tenant: expected %d, got %d: %s", http.StatusCreated, createTenantRecorder.Code, createTenantRecorder.Body.String())
	}
	var createdTenant struct {
		Tenant tenantTarget `json:"tenant"`
	}
	if err := json.NewDecoder(createTenantRecorder.Body).Decode(&createdTenant); err != nil {
		t.Fatalf("decode created tenant: %v", err)
	}
	if !isUUIDv6(createdTenant.Tenant.ID) || createdTenant.Tenant.RelationshipID != createdTenant.Tenant.ID {
		t.Fatalf("new tenant identity/relationship is not UUIDv6: %+v", createdTenant.Tenant)
	}
	createdTenantDatabase := tenantDatabaseNameWithAlias(projectID, createdTenant.Tenant.ID, "acme")
	t.Cleanup(func() {
		if err := dropProjectDatabase(context.Background(), baseURL, createdTenantDatabase); err != nil {
			t.Errorf("drop created tenant database %s: %v", createdTenantDatabase, err)
		}
	})

	renameRequest := httptest.NewRequest(http.MethodPatch, "/dev/projects/"+projectID, bytes.NewBufferString(`{"name":"Renamed Relationship Test"}`))
	renameRequest.Header.Set("content-type", "application/json")
	renameRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(renameRecorder, renameRequest)
	if renameRecorder.Code != http.StatusOK {
		t.Fatalf("rename project: expected %d, got %d: %s", http.StatusOK, renameRecorder.Code, renameRecorder.Body.String())
	}
	var renamed struct {
		Project projectTarget `json:"project"`
	}
	if err := json.NewDecoder(renameRecorder.Body).Decode(&renamed); err != nil {
		t.Fatalf("decode renamed project: %v", err)
	}
	if renamed.Project.Name != "Renamed Relationship Test" || renamed.Project.ID != projectID || renamed.Project.Database != createdProject.Project.Database {
		t.Fatalf("rename changed project identity: before=%+v after=%+v", createdProject.Project, renamed.Project)
	}

	// Project keys are the CLI's machine credential. Exercise the authenticated
	// bulk env route to ensure the CLI does not need a dashboard session and a
	// key remains scoped to the project encoded in the URL.
	server.config.RequireAuth = true
	envRequest := httptest.NewRequest(http.MethodPut, "/dev/projects/"+projectID+"/env", bytes.NewBufferString(`{"content":"API_URL=https://api.example.test\nSECRET_TOKEN=shh\n"}`))
	envRequest.Header.Set("content-type", "application/json")
	envRequest.Header.Set("x-gonvex-key", createdProject.ProjectKey)
	envRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(envRecorder, envRequest)
	if envRecorder.Code != http.StatusOK {
		t.Fatalf("push project env with project key: expected %d, got %d: %s", http.StatusOK, envRecorder.Code, envRecorder.Body.String())
	}
	storedEnv, err := server.loadProjectEnv(context.Background(), projectID)
	if err != nil {
		t.Fatalf("load pushed project env: %v", err)
	}
	if len(storedEnv) != 2 || storedEnv[0].Name != "API_URL" || storedEnv[1].Name != "SECRET_TOKEN" {
		t.Fatalf("unexpected pushed project env: %+v", storedEnv)
	}

	restarted := New(cfg)
	loaded, err := restarted.loadTenantRegistry(context.Background(), projectID)
	if err != nil {
		t.Fatalf("load new tenant relationship after restart: %v", err)
	}
	if len(loaded) != 1 || loaded[0].ID != createdTenant.Tenant.ID || loaded[0].RelationshipID != createdTenant.Tenant.RelationshipID {
		t.Fatalf("new relationship did not survive restart: %+v", loaded)
	}
	persistedProjects, err := restarted.loadProjectRegistry(context.Background())
	if err != nil {
		t.Fatalf("load renamed project after restart: %v", err)
	}
	var persistedName string
	for _, project := range persistedProjects {
		if project.ID == projectID {
			persistedName = project.Name
			break
		}
	}
	if persistedName != "Renamed Relationship Test" {
		t.Fatalf("renamed project did not survive restart: got %q", persistedName)
	}

	server.config.RequireAuth = false
	deleteProjectRequest := httptest.NewRequest(http.MethodDelete, "/dev/projects/"+projectID, nil)
	deleteProjectRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(deleteProjectRecorder, deleteProjectRequest)
	if deleteProjectRecorder.Code != http.StatusOK {
		t.Fatalf("delete project: expected %d, got %d: %s", http.StatusOK, deleteProjectRecorder.Code, deleteProjectRecorder.Body.String())
	}
	maintenanceDB, err := openMaintenanceDB(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer maintenanceDB.Close()
	for _, databaseName := range []string{createdProject.Project.Database, createdTenantDatabase} {
		var exists bool
		if err := maintenanceDB.QueryRowContext(context.Background(), `SELECT EXISTS (
			SELECT 1 FROM pg_database WHERE datname = $1
		)`, databaseName).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatalf("project deletion left database %q behind", databaseName)
		}
	}
}

func TestPostgresLegacyProjectTenantBackfillSurvivesRestart(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	suffix := tenantRegistryTestSuffix(t)
	controlName := "gonvex_legacy_control_" + suffix
	projectID := "legacy-" + strings.ReplaceAll(suffix, "_", "-")
	projectName := "gonvex_legacy_project_" + suffix
	tenantName := tenantDatabaseNameWithAlias(projectID, "acme", "acme")
	controlURL := createTenantRegistryTestDatabase(t, baseURL, controlName)
	projectURL := createTenantRegistryTestDatabase(t, baseURL, projectName)
	tenantURL := createTenantRegistryTestDatabase(t, baseURL, tenantName)

	cfg := config.Config{
		LandlordURL: controlURL,
		PostgresURL: baseURL,
		ProjectDatabases: map[string]string{
			projectID: projectURL,
		},
		ProjectKeys: map[string]string{},
	}
	project := projectTarget{
		ID:             projectID,
		Name:           "Legacy Relationship Test",
		Environment:    "test",
		Database:       projectName,
		DatabaseMode:   "multiTenant",
		StorageBucket:  projectID + "-test",
		Status:         "local",
		Description:    "Legacy tenant registry integration test.",
		Provisioned:    true,
		RuntimeCreated: true,
		databaseURL:    projectURL,
		databaseName:   projectName,
	}
	server := New(cfg)
	server.projects[projectID] = project
	if err := server.saveProjectRegistry(context.Background(), project); err != nil {
		t.Fatalf("save legacy project registry: %v", err)
	}

	server.hydrateProjectTenantDatabases(context.Background(), projectID)
	server.projectMu.RLock()
	backfilled, ok := server.tenants[tenantStoreKey(projectID, "acme")]
	server.projectMu.RUnlock()
	if !ok {
		t.Fatalf("legacy project tenant database %q was not backfilled", tenantName)
	}
	if !isUUIDv6(backfilled.RelationshipID) || !backfilled.registered || backfilled.databaseURL != tenantURL {
		t.Fatalf("unexpected backfilled relationship: %+v", backfilled)
	}

	restarted := New(cfg)
	loaded, err := restarted.loadTenantRegistry(context.Background(), projectID)
	if err != nil {
		t.Fatalf("load tenant registry after restart: %v", err)
	}
	if len(loaded) != 1 || loaded[0].ID != "acme" || loaded[0].RelationshipID != backfilled.RelationshipID {
		t.Fatalf("legacy relationship did not survive restart: %+v", loaded)
	}
}

func TestPostgresLandlordHydrationPreservesRegisteredPhysicalTenantDatabase(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	suffix := tenantRegistryTestSuffix(t)
	controlName := "gonvex_preserve_control_" + suffix
	projectName := "gonvex_preserve_project_" + suffix
	originalTenantName, err := generateTenantPhysicalDatabaseName()
	if err != nil {
		t.Fatal(err)
	}
	controlURL := createTenantRegistryTestDatabase(t, baseURL, controlName)
	projectURL := createTenantRegistryTestDatabase(t, baseURL, projectName)
	originalTenantURL := createTenantRegistryTestDatabase(t, baseURL, originalTenantName)
	projectID, err := generateProjectID()
	if err != nil {
		t.Fatal(err)
	}

	projectDB, err := sql.Open("pgx", projectURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projectDB.ExecContext(context.Background(), `
		CREATE TABLE tenants (
			id TEXT PRIMARY KEY,
			name TEXT,
			database TEXT,
			domain TEXT
		);
		INSERT INTO tenants (id, name, database, domain)
		VALUES ('toasting', 'Toasting', 'toasting', 'toasting')
	`); err != nil {
		_ = projectDB.Close()
		t.Fatalf("seed landlord tenant: %v", err)
	}
	_ = projectDB.Close()

	cfg := config.Config{
		LandlordURL: controlURL,
		PostgresURL: baseURL,
		ProjectDatabases: map[string]string{
			projectID: projectURL,
		},
		ProjectKeys: map[string]string{},
	}
	server := New(cfg)
	project := projectTarget{
		ID:             projectID,
		Name:           "Preserve Tenant Relationship Test",
		Environment:    "test",
		Database:       projectName,
		DatabaseMode:   "multiTenant",
		Status:         "local",
		Description:    "Tenant hydration regression test.",
		Provisioned:    true,
		RuntimeCreated: true,
		databaseURL:    projectURL,
		databaseName:   projectName,
	}
	server.projects[projectID] = project
	if err := server.saveProjectRegistry(context.Background(), project); err != nil {
		t.Fatalf("save project relationship: %v", err)
	}
	registered, err := server.saveTenantRegistry(context.Background(), tenantTarget{
		ID:             "toasting",
		ProjectID:      projectID,
		Name:           "Toasting",
		Database:       "toasting",
		databaseName:   originalTenantName,
		databaseURL:    originalTenantURL,
		domain:         "toasting",
		Status:         "active",
		Description:    "Persisted tenant from landlord database.",
		Provisioned:    true,
		RuntimeCreated: false,
	})
	if err != nil {
		t.Fatalf("save tenant relationship: %v", err)
	}

	restarted := New(cfg)
	restarted.hydrateProjectTenantDatabasesUncached(context.Background(), projectID)
	loaded, err := restarted.loadTenantRegistry(context.Background(), projectID)
	if err != nil {
		t.Fatalf("load tenant relationship after hydration: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected one tenant relationship, got %+v", loaded)
	}
	if loaded[0].RelationshipID != registered.RelationshipID {
		t.Fatalf("hydration replaced relationship id: before=%q after=%q", registered.RelationshipID, loaded[0].RelationshipID)
	}
	if loaded[0].databaseName != originalTenantName || loaded[0].databaseURL != originalTenantURL {
		t.Fatalf("hydration orphaned the registered tenant database: before=%q after=%q", originalTenantName, loaded[0].databaseName)
	}
}
