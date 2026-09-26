package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func rotateProjectKeyRequest(t *testing.T, server *Server, projectID string, credential string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/dev/projects/"+projectID+"/key/rotate", nil)
	if credential != "" {
		request.Header.Set("authorization", "Bearer "+credential)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func decodeRotatedProjectKey(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var payload struct {
		ProjectKey string `json:"projectKey"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatal("decode project key rotation response")
	}
	if !strings.HasPrefix(payload.ProjectKey, "gvx_") {
		t.Fatal("rotation did not return a generated project key")
	}
	return payload.ProjectKey
}

func TestProjectKeyRotationRequiresAuthenticatedManagementCredential(t *testing.T) {
	server := New(config.Config{
		AdminKey: "runtime-admin-key",
		ProjectDatabases: map[string]string{
			"project-a": "postgres://example/project-a",
		},
		ProjectKeys: map[string]string{"project-a": "old-project-key"},
	})

	recorder := rotateProjectKeyRequest(t, server, "project-a", "")
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, recorder.Code)
	}
	if bytes.Contains(recorder.Body.Bytes(), []byte("old-project-key")) {
		t.Fatal("unauthorized response disclosed the existing project key")
	}
}

func TestProjectKeyRotationFailsClosedWithoutDurableRegistry(t *testing.T) {
	const projectID = "project-a"
	const priorKey = "old-project-key"
	server := New(config.Config{
		AdminKey: "runtime-admin-key",
		ProjectDatabases: map[string]string{
			projectID: "postgres://example/project-a",
		},
		ProjectKeys: map[string]string{projectID: priorKey},
	})

	recorder := rotateProjectKeyRequest(t, server, projectID, "runtime-admin-key")
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, recorder.Code)
	}
	if bytes.Contains(recorder.Body.Bytes(), []byte(priorKey)) {
		t.Fatal("failed rotation response disclosed the prior project key")
	}
	if !server.acceptsSyncKey(projectID, priorKey, nil) || !server.acceptsProjectEnvKey(projectID, priorKey) {
		t.Fatal("failed durable rotation invalidated the prior project key")
	}

	server.projectMu.RLock()
	stored := server.projects[projectID].syncKey
	configured := server.config.ProjectKeys[projectID]
	server.projectMu.RUnlock()
	if stored != priorKey || configured != priorKey {
		t.Fatal("failed durable rotation changed an in-memory key lookup")
	}
}

func TestPostgresProjectKeyRotationPersistsAcrossRestart(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	suffix := tenantRegistryTestSuffix(t)
	controlURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_key_rotation_"+suffix)
	projectID := "key-rotation-" + suffix
	priorKey, err := generateProjectKey(projectID)
	if err != nil {
		t.Fatal("generate prior synthetic project key")
	}
	server := New(config.Config{
		LandlordURL: controlURL,
		PostgresURL: baseURL,
		AdminKey:    "runtime-admin-key",
		ProjectKeys: map[string]string{projectID: priorKey},
	})
	project := projectTarget{
		ID: projectID, Name: "Project key rotation", Environment: "test",
		Database: "rotation_project", DatabaseMode: "single", StorageBucket: projectID + "-test",
		Status: "test", Description: "Project key rotation persistence test.",
		Provisioned: true, RuntimeCreated: true,
		databaseURL: baseURL, databaseName: "rotation_project", syncKey: priorKey,
	}
	server.projectMu.Lock()
	server.projects[projectID] = project
	server.projectMu.Unlock()
	if err := server.saveProjectRegistry(context.Background(), project); err != nil {
		t.Fatalf("save synthetic project registry: %v", err)
	}

	recorder := rotateProjectKeyRequest(t, server, projectID, "runtime-admin-key")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}
	replacement := decodeRotatedProjectKey(t, recorder)
	if replacement == priorKey || bytes.Contains(recorder.Body.Bytes(), []byte(priorKey)) {
		t.Fatal("successful rotation returned or disclosed the prior project key")
	}
	if server.acceptsSyncKey(projectID, priorKey, nil) || server.acceptsProjectEnvKey(projectID, priorKey) {
		t.Fatal("prior project key remained authorized after rotation")
	}
	if !server.acceptsSyncKey(projectID, replacement, nil) || !server.acceptsProjectEnvKey(projectID, replacement) {
		t.Fatal("replacement project key was not authorized after rotation")
	}

	registry, err := server.openProjectRegistry(context.Background())
	if err != nil || registry == nil {
		t.Fatal("open project registry after rotation")
	}
	defer registry.Close()
	var persisted string
	if err := registry.QueryRowContext(context.Background(), `SELECT project_key FROM gonvex_runtime_projects WHERE id = $1`, projectID).Scan(&persisted); err != nil {
		t.Fatal("read persisted project key after rotation")
	}
	if persisted != replacement || persisted == priorKey {
		t.Fatal("registry did not contain only the replacement project key")
	}

	restarted := New(config.Config{LandlordURL: controlURL, PostgresURL: baseURL, AdminKey: "runtime-admin-key"})
	restarted.hydrateProjects()
	if restarted.acceptsSyncKey(projectID, priorKey, nil) || restarted.acceptsProjectEnvKey(projectID, priorKey) {
		t.Fatal("restarted runtime accepted the prior project key")
	}
	if !restarted.acceptsSyncKey(projectID, replacement, nil) || !restarted.acceptsProjectEnvKey(projectID, replacement) {
		t.Fatal("restarted runtime did not accept the persisted replacement key")
	}
}

func TestPostgresProjectKeyRotationPersistenceFailureLeavesPriorKeyActive(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	suffix := tenantRegistryTestSuffix(t)
	controlURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_key_rotation_failure_"+suffix)
	projectID := "unpersisted-key-rotation-" + suffix
	priorKey, err := generateProjectKey(projectID)
	if err != nil {
		t.Fatal("generate prior synthetic project key")
	}
	server := New(config.Config{
		LandlordURL: controlURL,
		PostgresURL: baseURL,
		AdminKey:    "runtime-admin-key",
		ProjectKeys: map[string]string{projectID: priorKey},
	})
	server.projectMu.Lock()
	server.projects[projectID] = projectTarget{
		ID: projectID, Name: "Unpersisted key rotation", syncKey: priorKey,
	}
	server.projectMu.Unlock()

	recorder := rotateProjectKeyRequest(t, server, projectID, "runtime-admin-key")
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, recorder.Code)
	}
	if bytes.Contains(recorder.Body.Bytes(), []byte(priorKey)) {
		t.Fatal("failed rotation response disclosed the prior project key")
	}
	if !server.acceptsSyncKey(projectID, priorKey, nil) || !server.acceptsProjectEnvKey(projectID, priorKey) {
		t.Fatal("failed persistence invalidated the prior project key")
	}
}
