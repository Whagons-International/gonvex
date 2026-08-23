package server

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/gonvex/gonvex/pkg/manifest"
	controlidentity "github.com/gonvex/gonvex/server/internal/controlplane/identity"
	"github.com/gonvex/gonvex/server/internal/dbpool"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type projectTarget struct {
	ID                   string `json:"id"`
	Name                 string `json:"name"`
	Environment          string `json:"environment"`
	Database             string `json:"database"`
	DatabaseMode         string `json:"databaseMode"`
	StorageBucket        string `json:"storageBucket"`
	Status               string `json:"status"`
	Description          string `json:"description"`
	Provisioned          bool   `json:"provisioned"`
	RuntimeCreated       bool   `json:"runtimeCreated"`
	TestTab              bool   `json:"testTab"`
	ErrorTrackingEnabled bool   `json:"errorTrackingEnabled"`
	OwnerEmail           string `json:"ownerEmail,omitempty"`
	Role                 string `json:"role,omitempty"`
	databaseURL          string
	databaseName         string
	syncKey              string
}

type createProjectResponse struct {
	Project    projectTarget `json:"project"`
	ProjectKey string        `json:"projectKey"`
}

func (s *Server) handleProjects(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.authorizeAccountRequest(w, r, permissionProjectsRead)
	if !ok {
		return
	}
	s.hydrateProjects()

	// Copy under the project lock, then authorize outside it. canAccessProject
	// opens the control-plane DB; holding projectMu across that work can stall
	// the whole runtime (hydrate/sync wait forever on the same mutex).
	s.projectMu.RLock()
	all := make([]projectTarget, 0, len(s.projects))
	for _, project := range s.projects {
		all = append(all, project)
	}
	s.projectMu.RUnlock()

	projects := make([]projectTarget, 0, len(all))
	for _, project := range all {
		if s.dashboardAuthOptional() || s.canAccessProject(r.Context(), actor, project.ID) {
			if project.OwnerEmail == "" && s.dashboardAuthOptional() {
				project.OwnerEmail = actor.Email
			}
			projects = append(projects, project)
		}
	}

	sort.Slice(projects, func(i, j int) bool {
		return strings.ToLower(projects[i].Name) < strings.ToLower(projects[j].Name)
	})
	writeJSON(w, http.StatusOK, map[string]any{"projects": projects})
}

func (s *Server) hydrateProjects() {
	s.hydratePersistedProjects()
	s.hydrateConfiguredProjects()
}

func normalizedDatabaseMode(mode string) string {
	switch strings.TrimSpace(mode) {
	case "", "single":
		return "single"
	case "multiTenant":
		return "multiTenant"
	default:
		return ""
	}
}

func normalizedDatabaseModeWithDefault(mode string) string {
	normalized := normalizedDatabaseMode(mode)
	if normalized == "" {
		return "single"
	}
	return normalized
}

func normalizedProjectName(name string) string {
	return strings.TrimSpace(name)
}

func (s *Server) hydratePersistedProjects() {
	projects, err := s.loadProjectRegistry(context.Background())
	if err != nil {
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	for _, project := range projects {
		if _, ok := s.projects[project.ID]; ok {
			continue
		}
		s.projects[project.ID] = project
		if s.config.ProjectDatabases == nil {
			s.config.ProjectDatabases = map[string]string{}
		}
		if s.config.ProjectKeys == nil {
			s.config.ProjectKeys = map[string]string{}
		}
		s.config.ProjectDatabases[project.ID] = project.databaseURL
		if project.syncKey != "" {
			s.config.ProjectKeys[project.ID] = project.syncKey
		}
	}
}

func (s *Server) hydrateConfiguredProjects() {
	if len(s.config.ProjectDatabases) == 0 {
		return
	}

	var imported []projectTarget
	s.projectMu.Lock()
	for projectID, databaseURL := range s.config.ProjectDatabases {
		projectID = strings.TrimSpace(projectID)
		if projectID == "" {
			continue
		}
		if _, ok := s.projects[projectID]; ok {
			continue
		}
		project := projectTarget{
			ID:            projectID,
			Name:          projectNameFromID(projectID),
			Environment:   s.config.Environment,
			Database:      databaseNameFromURL(databaseURL, projectID),
			DatabaseMode:  "single",
			StorageBucket: projectID + "-dev",
			Status:        "local",
			Description:   "Configured project database.",
			Provisioned:   true,
			databaseURL:   databaseURL,
			databaseName:  databaseNameFromURL(databaseURL, projectID),
			syncKey:       s.config.ProjectKeys[projectID],
		}
		s.projects[projectID] = project
		imported = append(imported, project)
	}
	s.projectMu.Unlock()

	for _, project := range imported {
		_ = s.saveProjectRegistry(context.Background(), project)
	}
}

func (s *Server) handleCreateProject(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.authorizeAccountRequest(w, r, permissionProjectsCreate)
	if !ok {
		return
	}
	defer r.Body.Close()
	var payload struct {
		Name         string `json:"name"`
		DatabaseMode string `json:"databaseMode"`
		TestTab      bool   `json:"testTab"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	name := normalizedProjectName(payload.Name)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project name is required"})
		return
	}
	databaseMode := normalizedDatabaseMode(payload.DatabaseMode)
	if databaseMode == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "databaseMode must be single or multiTenant"})
		return
	}
	if s.config.PostgresURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "DATABASE_URL is not configured"})
		return
	}

	s.hydrateProjects()

	s.projectMu.Lock()
	defer s.projectMu.Unlock()

	projectID, err := s.uniqueRuntimeProjectIDLocked()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	databaseName := s.uniqueDatabaseNameLocked(projectID)
	databaseURL, err := createProjectDatabase(r.Context(), s.config.PostgresURL, databaseName)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.config.ProjectDatabases == nil {
		s.config.ProjectDatabases = map[string]string{}
	}
	if s.config.ProjectKeys == nil {
		s.config.ProjectKeys = map[string]string{}
	}
	projectKey, err := generateProjectKey(projectID)
	if err != nil {
		_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.config.ProjectDatabases[projectID] = databaseURL
	s.config.ProjectKeys[projectID] = projectKey
	project := projectTarget{
		ID:             projectID,
		Name:           name,
		Environment:    s.config.Environment,
		Database:       databaseName,
		DatabaseMode:   databaseMode,
		StorageBucket:  projectID + "-dev",
		Status:         "local",
		Description:    "Runtime-created project database.",
		Provisioned:    true,
		RuntimeCreated: true,
		TestTab:        payload.TestTab,
		OwnerEmail:     actor.Email,
		Role:           "owner",
		databaseURL:    databaseURL,
		databaseName:   databaseName,
		syncKey:        projectKey,
	}
	s.projects[projectID] = project
	if err := s.saveProjectRegistry(r.Context(), project); err != nil {
		_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		delete(s.projects, projectID)
		delete(s.config.ProjectDatabases, projectID)
		delete(s.config.ProjectKeys, projectID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := s.ensureProjectOwnerMember(r.Context(), project.ID, actor); err != nil {
		_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		_ = s.deleteProjectRegistry(context.Background(), projectID)
		delete(s.projects, projectID)
		delete(s.config.ProjectDatabases, projectID)
		delete(s.config.ProjectKeys, projectID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, createProjectResponse{Project: project, ProjectKey: projectKey})
}

func (s *Server) handleUpdateProject(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.authorizeAccountRequest(w, r, permissionProjectsUpdate)
	if !ok {
		return
	}
	defer r.Body.Close()
	projectID := strings.TrimSpace(r.PathValue("project"))
	if !s.canManageProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}
	var payload struct {
		Name                 *string `json:"name"`
		DatabaseMode         *string `json:"databaseMode"`
		ErrorTrackingEnabled *bool   `json:"errorTrackingEnabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if payload.Name == nil && payload.DatabaseMode == nil && payload.ErrorTrackingEnabled == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no project fields provided"})
		return
	}
	name := ""
	if payload.Name != nil {
		name = normalizedProjectName(*payload.Name)
		if name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project name is required"})
			return
		}
	}
	databaseMode := ""
	if payload.DatabaseMode != nil {
		databaseMode = normalizedDatabaseMode(*payload.DatabaseMode)
		if databaseMode == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "databaseMode must be single or multiTenant"})
			return
		}
	}

	s.hydrateProjects()

	s.projectMu.Lock()
	project, ok := s.projects[projectID]
	if !ok {
		s.projectMu.Unlock()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "project not found"})
		return
	}
	if payload.Name != nil {
		project.Name = name
	}
	if payload.DatabaseMode != nil {
		project.DatabaseMode = databaseMode
	}
	if payload.ErrorTrackingEnabled != nil {
		project.ErrorTrackingEnabled = *payload.ErrorTrackingEnabled
	}
	if err := s.saveProjectRegistry(r.Context(), project); err != nil {
		s.projectMu.Unlock()
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.projects[projectID] = project
	s.projectMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"project": project})
}

func (s *Server) handleProjectKey(w http.ResponseWriter, r *http.Request) {
	adminKey := s.acceptsAdminKey(syncKey(r))
	var actor dashboardActor
	signedIn := false
	if !adminKey {
		actor, signedIn = s.authorizeAccountRequest(w, r, permissionProjectsKeysRead)
		if !signedIn {
			return
		}
	}
	s.hydrateProjects()
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if signedIn && !adminKey && !s.canManageProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	project, ok := s.projects[projectID]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "project not found"})
		return
	}
	projectKey := project.syncKey
	if projectKey == "" && s.config.ProjectKeys != nil {
		projectKey = s.config.ProjectKeys[projectID]
	}
	if projectKey == "" {
		var err error
		projectKey, err = generateProjectKey(projectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if s.config.ProjectKeys == nil {
			s.config.ProjectKeys = map[string]string{}
		}
		s.config.ProjectKeys[projectID] = projectKey
	}
	project.syncKey = projectKey
	s.projects[projectID] = project
	if err := s.saveProjectRegistry(r.Context(), project); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"projectKey": projectKey})
}

// handleRotateProjectKey replaces a project's sync credential in one guarded
// operation. Persistence happens before either in-memory lookup is changed, and
// projectMu prevents concurrent key checks from observing a partially applied
// rotation. The prior key is deliberately never copied into the response or a
// log field.
func (s *Server) handleRotateProjectKey(w http.ResponseWriter, r *http.Request) {
	adminKey := s.acceptsAdminKey(syncKey(r))
	var actor dashboardActor
	signedIn := false
	if !adminKey {
		actor, signedIn = s.authorizeAccountRequest(w, r, permissionProjectsKeysWrite)
		if !signedIn {
			return
		}
	}
	s.hydrateProjects()
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if signedIn && !adminKey && !s.canManageProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	project, ok := s.projects[projectID]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "project not found"})
		return
	}
	projectKey, err := generateProjectKey(projectID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "project key rotation failed"})
		return
	}
	if err := s.persistProjectKey(r.Context(), projectID, projectKey); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "project key rotation could not be persisted"})
		return
	}
	if s.config.ProjectKeys == nil {
		s.config.ProjectKeys = map[string]string{}
	}
	project.syncKey = projectKey
	s.config.ProjectKeys[projectID] = projectKey
	s.projects[projectID] = project
	writeJSON(w, http.StatusOK, map[string]any{"projectKey": projectKey})
}

func (s *Server) handleDeleteProject(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.authorizeAccountRequest(w, r, permissionProjectsDelete)
	if !ok {
		return
	}
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if !s.canManageProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}

	// Include relationships loaded from the registry after a restart. Auth can
	// create tenant databases immediately before a later create/doctor step
	// fails, so project rollback must delete those physical databases too.
	s.hydrateProjectTenantDatabases(r.Context(), projectID)
	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	project, ok := s.projects[projectID]
	if !ok || !project.RuntimeCreated {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "runtime-created project not found"})
		return
	}
	tenantDatabaseNames := map[string]bool{}
	for _, tenant := range s.tenants {
		if tenant.ProjectID == projectID && tenant.RuntimeCreated && tenant.databaseName != "" {
			tenantDatabaseNames[tenant.databaseName] = true
		}
	}
	for databaseName := range tenantDatabaseNames {
		if err := dropProjectDatabase(r.Context(), s.config.PostgresURL, databaseName); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("drop tenant database %s: %v", databaseName, err)})
			return
		}
	}
	if err := dropProjectDatabase(r.Context(), s.config.PostgresURL, project.databaseName); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	_ = dropProjectDatabase(r.Context(), s.config.PostgresURL, telemetryDatabaseName(projectID))
	delete(s.projects, projectID)
	delete(s.config.ProjectDatabases, projectID)
	delete(s.config.ProjectKeys, projectID)
	if err := s.deleteProjectRegistry(r.Context(), projectID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.invalidateAppAuthRequirement(projectID)
	for key, tenant := range s.tenants {
		if tenant.ProjectID != projectID {
			continue
		}
		delete(s.tenants, key)
		if s.config.TenantDatabases != nil {
			delete(s.config.TenantDatabases, key)
		}
	}
	s.invalidateProjectTenantHydration(projectID)
	s.tenantStores.Close()
	s.cache.invalidateRows(r.Context(), projectID, tenantIDFromRequest(projectID, ""), "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) projectRegistryURL() string {
	if strings.TrimSpace(s.config.ControlPlaneURL) != "" {
		return s.config.ControlPlaneURL
	}
	return s.config.PostgresURL
}

func (s *Server) openProjectRegistry(ctx context.Context) (*sql.DB, error) {
	registryURL := s.projectRegistryURL()
	if strings.TrimSpace(registryURL) == "" {
		return nil, nil
	}
	db, err := dbpool.Open(registryURL)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	s.registryMu.Lock()
	if s.registryReady {
		s.registryMu.Unlock()
		return db, nil
	}
	connection, err := db.Conn(ctx)
	if err != nil {
		s.registryMu.Unlock()
		db.Close()
		return nil, err
	}
	// CREATE TABLE IF NOT EXISTS is not race-free in PostgreSQL when two
	// runtime startup/request paths create the same relation concurrently. Hold
	// a cluster-wide advisory lock on one dedicated connection for the complete
	// control-plane migration.
	if _, err := connection.ExecContext(ctx, `SELECT pg_advisory_lock(1735351662, 20260713)`); err != nil {
		connection.Close()
		s.registryMu.Unlock()
		db.Close()
		return nil, err
	}
	if err := ensureProjectRegistry(ctx, connection); err != nil {
		_, _ = connection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(1735351662, 20260713)`)
		connection.Close()
		s.registryMu.Unlock()
		db.Close()
		return nil, err
	}
	_, _ = connection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(1735351662, 20260713)`)
	connection.Close()
	s.registryReady = true
	s.registryMu.Unlock()
	return db, nil
}

// pooledProjectRegistry returns the process-wide connection pool used by
// latency-sensitive authorization reads. Callers must not close the returned
// database; database/sql replaces broken connections in the pool as needed.
func (s *Server) pooledProjectRegistry(ctx context.Context) (*sql.DB, error) {
	s.authRegistryMu.Lock()
	defer s.authRegistryMu.Unlock()
	if s.authRegistryDB != nil {
		return s.authRegistryDB, nil
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return db, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)
	s.authRegistryDB = db
	return db, nil
}

type projectRegistryExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func ensureProjectRegistry(ctx context.Context, db projectRegistryExecer) error {
	// Normal startup never upgrades or reads legacy identity tables. Their data
	// needs account-merging decisions that belong to the explicit migration.
	if _, err := db.ExecContext(ctx, `DO $$ BEGIN
		IF to_regclass('public.gonvex_auth_users') IS NOT NULL
			OR to_regclass('public.gonvex_auth_memberships') IS NOT NULL
			OR EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema()
					AND table_name IN ('gonvex_auth_codes', 'gonvex_auth_sessions', 'gonvex_auth_refresh_tokens')
					AND column_name = 'user_id'
			) THEN
			RAISE EXCEPTION 'identity-v2 migration required: run gonvex migrate identity-v2 --plan, --apply, and --verify before starting Gonvex';
		END IF;
	END $$`); err != nil {
		return fmt.Errorf("control-plane identity schema is not ready: %w", err)
	}
	// Identity v2 lives in the Control Plane. Legacy identity data is imported
	// only by the explicit identity-v2 migration command, never by runtime reads.
	if err := controlidentity.InstallSchema(ctx, db); err != nil {
		return fmt.Errorf("install control-plane identity schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_runtime_projects (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		environment TEXT NOT NULL,
		database_name TEXT NOT NULL,
		database_mode TEXT NOT NULL DEFAULT 'single',
		database_url TEXT NOT NULL,
		storage_bucket TEXT NOT NULL,
		status TEXT NOT NULL,
		description TEXT NOT NULL,
		project_key TEXT NOT NULL DEFAULT '',
		provisioned BOOLEAN NOT NULL DEFAULT TRUE,
		runtime_created BOOLEAN NOT NULL DEFAULT TRUE,
		test_tab BOOLEAN NOT NULL DEFAULT FALSE,
		error_tracking_enabled BOOLEAN NOT NULL DEFAULT FALSE,
		owner_email TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE gonvex_runtime_projects ADD COLUMN IF NOT EXISTS test_tab BOOLEAN NOT NULL DEFAULT FALSE`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE gonvex_runtime_projects ADD COLUMN IF NOT EXISTS error_tracking_enabled BOOLEAN NOT NULL DEFAULT FALSE`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE gonvex_runtime_projects ADD COLUMN IF NOT EXISTS database_mode TEXT NOT NULL DEFAULT 'single'`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE gonvex_runtime_projects ADD COLUMN IF NOT EXISTS owner_email TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_runtime_tenants (
		relationship_id TEXT PRIMARY KEY,
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		tenant_id TEXT NOT NULL,
		name TEXT NOT NULL,
		database_alias TEXT NOT NULL DEFAULT '',
		database_name TEXT NOT NULL DEFAULT '',
		database_url TEXT NOT NULL DEFAULT '',
		domain TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'local',
		description TEXT NOT NULL DEFAULT '',
		provisioned BOOLEAN NOT NULL DEFAULT FALSE,
		runtime_created BOOLEAN NOT NULL DEFAULT FALSE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		UNIQUE (project_id, tenant_id)
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS gonvex_runtime_tenants_project_database
		ON gonvex_runtime_tenants (project_id, database_name)
		WHERE database_name <> ''`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_runtime_tenants_project
		ON gonvex_runtime_tenants (project_id, name, tenant_id)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_dashboard_accounts (
		email TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT 'standard',
		password_hash TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	// One-way upgrade from the pre-Account dashboard identity table. This is
	// migration code only; the runtime reads and writes the canonical table.
	if _, err := db.ExecContext(ctx, `DO $$ BEGIN
		IF to_regclass('public.gonvex_dashboard_users') IS NOT NULL THEN
			EXECUTE 'INSERT INTO gonvex_dashboard_accounts (email, name, role, password_hash, created_at, updated_at)
				SELECT email, name, role, password_hash, created_at, updated_at FROM gonvex_dashboard_users
				ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name, role = EXCLUDED.role,
				password_hash = EXCLUDED.password_hash, updated_at = EXCLUDED.updated_at';
			DROP TABLE gonvex_dashboard_users;
		END IF;
	END $$`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `UPDATE gonvex_dashboard_accounts SET role = 'standard' WHERE role = 'user'`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_account_access_tokens (
		id TEXT PRIMARY KEY,
		owner_email TEXT NOT NULL,
		name TEXT NOT NULL,
		token_prefix TEXT NOT NULL,
		token_hash TEXT NOT NULL UNIQUE,
		permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
		expires_at TIMESTAMPTZ,
		last_used_at TIMESTAMPTZ,
		revoked_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_account_access_tokens_by_owner
		ON gonvex_account_access_tokens (owner_email, created_at DESC)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_project_members (
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		email TEXT NOT NULL,
		name TEXT NOT NULL DEFAULT '',
		role TEXT NOT NULL DEFAULT 'dev',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (project_id, email)
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_project_members_by_email ON gonvex_project_members (email, project_id)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_project_invitations (
		id TEXT PRIMARY KEY,
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		email TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT 'dev',
		token_hash TEXT NOT NULL,
		invited_by TEXT NOT NULL DEFAULT '',
		expires_at TIMESTAMPTZ NOT NULL,
		accepted_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_dashboard_notifications (
		id TEXT PRIMARY KEY,
		email TEXT NOT NULL,
		type TEXT NOT NULL DEFAULT 'info',
		title TEXT NOT NULL,
		body TEXT NOT NULL DEFAULT '',
		project_id TEXT,
		read_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_dashboard_notifications_by_email ON gonvex_dashboard_notifications (email, created_at DESC)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_runtime_manifests (
		project_id TEXT PRIMARY KEY,
		manifest JSONB NOT NULL,
		module_hash TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `DO $$ BEGIN
		IF EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'gonvex_runtime_manifests' AND column_name = 'bundle_hash'
		) THEN
			IF NOT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'gonvex_runtime_manifests' AND column_name = 'module_hash'
			) THEN
				ALTER TABLE gonvex_runtime_manifests RENAME COLUMN bundle_hash TO module_hash;
			ELSE
				UPDATE gonvex_runtime_manifests SET module_hash = bundle_hash WHERE module_hash = '';
				ALTER TABLE gonvex_runtime_manifests DROP COLUMN bundle_hash;
			END IF;
		END IF;
	END $$`); err != nil {
		return err
	}
	// One-way update migration. Runtime code never reads the legacy name after
	// this block; existing audit history is retained under the reducer term.
	if _, err := db.ExecContext(ctx, `DO $$ BEGIN
		IF to_regclass('public.gonvex_runtime_mutation_logs') IS NOT NULL
			AND to_regclass('public.gonvex_runtime_reducer_logs') IS NULL THEN
			ALTER TABLE gonvex_runtime_mutation_logs RENAME TO gonvex_runtime_reducer_logs;
		END IF;
	END $$`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_runtime_reducer_logs (
		id BIGSERIAL PRIMARY KEY,
		project_id TEXT NOT NULL DEFAULT '',
		kind TEXT NOT NULL,
		entry JSONB NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	// Canonicalize retained audit history while it is already being migrated.
	// The JSON entry is what the dashboard reads, so update both representations.
	if _, err := db.ExecContext(ctx, `UPDATE gonvex_runtime_reducer_logs
		SET kind = 'reducer', entry = jsonb_set(entry, '{kind}', to_jsonb('reducer'::text), true)
		WHERE kind IN ('mutation', 'internalMutation')`); err != nil {
		return err
	}
	// The table now also stores FAILED queries/actions/http calls, which the
	// in-memory ring drops within minutes (see runtimeLogIsDurable). Existing
	// deployments carried a kind CHECK that only allowed obsolete write-kind names and
	// silently rejected every one of those inserts.
	if _, err := db.ExecContext(ctx, `ALTER TABLE gonvex_runtime_reducer_logs
		DROP CONSTRAINT IF EXISTS gonvex_runtime_mutation_logs_kind_check`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_runtime_reducer_logs_by_project
		ON gonvex_runtime_reducer_logs (project_id, id DESC)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_scheduled_jobs (
		id TEXT PRIMARY KEY,
		project_id TEXT NOT NULL,
		tenant_id TEXT NOT NULL DEFAULT '',
		function_path TEXT NOT NULL,
		args JSONB,
		run_at TIMESTAMPTZ NOT NULL,
		scheduled_for TIMESTAMPTZ NOT NULL,
		cron_name TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed')),
		claim_sequence BIGINT NOT NULL DEFAULT 0,
		claim_token TEXT NOT NULL DEFAULT '',
		lease_until TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		completed_at TIMESTAMPTZ
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_due_oneshot
		ON gonvex_scheduled_jobs (run_at, id)
		WHERE status = 'pending' AND cron_name = ''`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_due_cron
		ON gonvex_scheduled_jobs (run_at, id)
		WHERE status = 'pending' AND cron_name <> ''`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_completed
		ON gonvex_scheduled_jobs (completed_at, id)
		WHERE status = 'completed'`); err != nil {
		return err
	}
	// Project environment variables, stored in the runtime registry (not in any
	// browsable tenant/project database) and hidden from the Data browser.
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_project_env (
		project_id TEXT NOT NULL,
		name TEXT NOT NULL,
		value TEXT NOT NULL DEFAULT '',
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (project_id, name)
	)`)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_auth_providers (
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		provider TEXT NOT NULL,
		enabled BOOLEAN NOT NULL DEFAULT TRUE,
		signup_mode TEXT NOT NULL DEFAULT 'personal',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (project_id, provider)
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS signup_mode TEXT NOT NULL DEFAULT 'personal'`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_auth_redirect_uris (
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		provider TEXT NOT NULL,
		redirect_uri TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (project_id, provider, redirect_uri)
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_auth_transactions (
		token_hash TEXT PRIMARY KEY,
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		redirect_uri TEXT NOT NULL,
		app_state TEXT NOT NULL,
		code_challenge TEXT NOT NULL,
		nonce TEXT NOT NULL,
		google_redirect_uri TEXT NOT NULL,
		expires_at TIMESTAMPTZ NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_auth_transactions_by_expiry
		ON gonvex_auth_transactions (expires_at)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_auth_codes (
		code_hash TEXT PRIMARY KEY,
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
		redirect_uri TEXT NOT NULL,
		code_challenge TEXT NOT NULL,
		expires_at TIMESTAMPTZ NOT NULL,
		used_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_auth_codes_by_expiry
		ON gonvex_auth_codes (expires_at)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_auth_sessions (
		token_hash TEXT PRIMARY KEY,
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
		family_id TEXT NOT NULL DEFAULT '',
		expires_at TIMESTAMPTZ NOT NULL,
		revoked_at TIMESTAMPTZ,
		last_seen_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_auth_sessions_by_account
		ON gonvex_auth_sessions (project_id, account_id, expires_at DESC)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_auth_sessions_by_family
		ON gonvex_auth_sessions (family_id) WHERE family_id <> ''`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_auth_sessions_by_expiry
		ON gonvex_auth_sessions (expires_at)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_auth_refresh_tokens (
		token_hash TEXT PRIMARY KEY,
		family_id TEXT NOT NULL,
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
		expires_at TIMESTAMPTZ NOT NULL,
		used_at TIMESTAMPTZ,
		revoked_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_auth_refresh_tokens_by_family
		ON gonvex_auth_refresh_tokens (family_id, created_at DESC)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_auth_refresh_tokens_by_expiry
		ON gonvex_auth_refresh_tokens (expires_at)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_auth_membership_invitations (
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		tenant_id TEXT NOT NULL,
		email TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT 'member',
		permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
		invited_by TEXT NOT NULL DEFAULT '',
		expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '7 days'),
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (project_id, tenant_id, email),
		FOREIGN KEY (project_id, tenant_id) REFERENCES gonvex_runtime_tenants(project_id, tenant_id) ON DELETE CASCADE
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE gonvex_auth_membership_invitations
		ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '7 days')`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_auth_membership_invitations_by_email
		ON gonvex_auth_membership_invitations (project_id, email)`); err != nil {
		return err
	}
	if err := ensureControlPlaneFunctionSchema(ctx, db); err != nil {
		return err
	}
	// Backfill the project-shaped membership scope used by single-database auth
	// projects. This also upgrades projects enabled before that scope existed.
	if _, err := db.ExecContext(ctx, `INSERT INTO gonvex_runtime_tenants (
		relationship_id, project_id, tenant_id, name, status, description, provisioned, runtime_created, updated_at
	)
	SELECT 'auth-single:' || p.id, p.id, p.id, p.name, 'active',
		'Single-database app membership scope.', TRUE, FALSE, now()
	FROM gonvex_runtime_projects p
	WHERE COALESCE(NULLIF(p.database_mode, ''), 'single') = 'single'
	AND EXISTS (SELECT 1 FROM gonvex_auth_providers a WHERE a.project_id = p.id)
	ON CONFLICT (project_id, tenant_id) DO UPDATE SET name = EXCLUDED.name, updated_at = now()`); err != nil {
		return err
	}
	return nil
}

func (s *Server) loadProjectRegistry(ctx context.Context) ([]projectTarget, error) {
	// Prefer the shared registry pool so listing projects cannot starve (or be
	// starved by) per-tenant dbpool budgets during multi-tenant local/prod use.
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, `SELECT id, name, environment, database_name, database_url, storage_bucket, status, description, project_key, provisioned, runtime_created, COALESCE(test_tab, false), COALESCE(error_tracking_enabled, false), COALESCE(NULLIF(database_mode, ''), 'single'), COALESCE(owner_email, '') FROM gonvex_runtime_projects ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []projectTarget
	for rows.Next() {
		var project projectTarget
		if err := rows.Scan(&project.ID, &project.Name, &project.Environment, &project.databaseName, &project.databaseURL, &project.StorageBucket, &project.Status, &project.Description, &project.syncKey, &project.Provisioned, &project.RuntimeCreated, &project.TestTab, &project.ErrorTrackingEnabled, &project.DatabaseMode, &project.OwnerEmail); err != nil {
			return nil, err
		}
		project.DatabaseMode = normalizedDatabaseModeWithDefault(project.DatabaseMode)
		project.Database = project.databaseName
		projects = append(projects, project)
	}
	return projects, rows.Err()
}

func (s *Server) saveProjectRegistry(ctx context.Context, project projectTarget) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()

	databaseName := project.databaseName
	if databaseName == "" {
		databaseName = project.Database
	}
	project.DatabaseMode = normalizedDatabaseModeWithDefault(project.DatabaseMode)
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_projects (
		id, name, environment, database_name, database_mode, database_url, storage_bucket, status, description, project_key, provisioned, runtime_created, test_tab, error_tracking_enabled, owner_email, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, now())
	ON CONFLICT (id) DO UPDATE SET
		name = EXCLUDED.name,
		environment = EXCLUDED.environment,
		database_name = EXCLUDED.database_name,
		database_mode = EXCLUDED.database_mode,
		database_url = EXCLUDED.database_url,
		storage_bucket = EXCLUDED.storage_bucket,
		status = EXCLUDED.status,
		description = EXCLUDED.description,
		project_key = EXCLUDED.project_key,
		provisioned = EXCLUDED.provisioned,
		runtime_created = EXCLUDED.runtime_created,
		test_tab = EXCLUDED.test_tab,
		error_tracking_enabled = EXCLUDED.error_tracking_enabled,
		owner_email = EXCLUDED.owner_email,
		updated_at = now()`,
		project.ID,
		project.Name,
		project.Environment,
		databaseName,
		project.DatabaseMode,
		project.databaseURL,
		project.StorageBucket,
		project.Status,
		project.Description,
		project.syncKey,
		project.Provisioned,
		project.RuntimeCreated,
		project.TestTab,
		project.ErrorTrackingEnabled,
		project.OwnerEmail,
	)
	if err != nil {
		return err
	}
	if project.DatabaseMode == "multiTenant" {
		_, err = db.ExecContext(ctx, `DELETE FROM gonvex_runtime_tenants
			WHERE project_id = $1 AND tenant_id = $1 AND relationship_id = $2`,
			project.ID, singleAppAuthTenantRelationshipID(project.ID))
	} else {
		_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_tenants (
			relationship_id, project_id, tenant_id, name, status, description, provisioned, runtime_created, updated_at
		)
		SELECT $2, p.id, p.id, p.name, 'active', 'Single-database app membership scope.', TRUE, FALSE, now()
		FROM gonvex_runtime_projects p WHERE p.id = $1
		AND EXISTS (SELECT 1 FROM gonvex_auth_providers a WHERE a.project_id = p.id)
		ON CONFLICT (project_id, tenant_id) DO UPDATE SET name = EXCLUDED.name, updated_at = now()`,
			project.ID, singleAppAuthTenantRelationshipID(project.ID))
	}
	return err
}

// persistProjectKey updates only the project credential so key rotation cannot
// partially fail after an unrelated registry side effect. Rotation fails closed
// when durable registry storage is unavailable so a restart can never revive a
// retired credential from static process configuration.
func (s *Server) persistProjectKey(ctx context.Context, projectID string, projectKey string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil {
		return err
	}
	if db == nil {
		return fmt.Errorf("project registry is unavailable")
	}
	defer db.Close()
	result, err := db.ExecContext(ctx, `UPDATE gonvex_runtime_projects
		SET project_key = $2, updated_at = now()
		WHERE id = $1`, projectID, projectKey)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated != 1 {
		return fmt.Errorf("project key rotation expected one registry row, updated %d", updated)
	}
	return nil
}

func (s *Server) persistProjectErrorTrackingEnabled(ctx context.Context, projectID string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `UPDATE gonvex_runtime_projects SET error_tracking_enabled = TRUE, updated_at = now() WHERE id = $1`, projectID)
	return err
}

func (s *Server) deleteProjectRegistry(ctx context.Context, projectID string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `DELETE FROM gonvex_runtime_manifests WHERE project_id = $1`, projectID); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM gonvex_scheduled_jobs WHERE project_id = $1`, projectID); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `DELETE FROM gonvex_runtime_projects WHERE id = $1`, projectID)
	return err
}

func (s *Server) ensureProjectOwnerMember(ctx context.Context, projectID string, actor dashboardActor) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()
	name := strings.TrimSpace(actor.Name)
	if name == "" {
		name = displayNameFromEmail(actor.Email)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_project_members (
		project_id, email, name, role
	) VALUES ($1, $2, $3, 'owner')
	ON CONFLICT (project_id, email) DO UPDATE SET
		name = EXCLUDED.name,
		role = 'owner'`,
		projectID, actor.Email, name)
	return err
}

// ensureSyncedProjectListed makes a project that arrived via `gonvex dev` sync
// appear in GET /dev/projects. Local zero-config runtimes accept any project id
// against the shared POSTGRES_URL without writing gonvex_runtime_projects, so
// the dashboard previously showed an empty chooser even while the app worked.
func (s *Server) ensureSyncedProjectListed(ctx context.Context, projectID string, projectKey string) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return
	}
	projectKey = strings.TrimSpace(projectKey)
	s.hydrateProjects()

	s.projectMu.Lock()
	if existing, ok := s.projects[projectID]; ok {
		if projectKey != "" && strings.TrimSpace(existing.syncKey) == "" {
			existing.syncKey = projectKey
			s.projects[projectID] = existing
			if s.config.ProjectKeys == nil {
				s.config.ProjectKeys = map[string]string{}
			}
			s.config.ProjectKeys[projectID] = projectKey
			s.projectMu.Unlock()
			_ = s.saveProjectRegistry(ctx, existing)
			return
		}
		s.projectMu.Unlock()
		return
	}

	databaseURL := ""
	if s.config.ProjectDatabases != nil {
		databaseURL = strings.TrimSpace(s.config.ProjectDatabases[projectID])
	}
	if databaseURL == "" {
		databaseURL = strings.TrimSpace(s.config.PostgresURL)
	}
	if databaseURL == "" {
		s.projectMu.Unlock()
		return
	}
	if s.config.ProjectDatabases == nil {
		s.config.ProjectDatabases = map[string]string{}
	}
	if s.config.ProjectKeys == nil {
		s.config.ProjectKeys = map[string]string{}
	}
	s.config.ProjectDatabases[projectID] = databaseURL
	if projectKey != "" {
		s.config.ProjectKeys[projectID] = projectKey
	}
	project := projectTarget{
		ID:             projectID,
		Name:           projectNameFromID(projectID),
		Environment:    s.config.Environment,
		Database:       databaseNameFromURL(databaseURL, projectID),
		DatabaseMode:   "single",
		StorageBucket:  projectID + "-dev",
		Status:         "local",
		Description:    "Registered from gonvex dev sync.",
		Provisioned:    true,
		RuntimeCreated: false,
		databaseURL:    databaseURL,
		databaseName:   databaseNameFromURL(databaseURL, projectID),
		syncKey:        projectKey,
	}
	s.projects[projectID] = project
	s.projectMu.Unlock()
	_ = s.saveProjectRegistry(ctx, project)
}

func (s *Server) saveRuntimeManifest(ctx context.Context, next manifest.Manifest) error {
	projectID := strings.TrimSpace(next.Project)
	if projectID == "" {
		return nil
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()
	payload, err := json.Marshal(next)
	if err != nil {
		return err
	}
	moduleHash := ""
	if next.Module != nil {
		moduleHash = next.Module.Identity()
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_manifests (
		project_id, manifest, module_hash, updated_at
	) VALUES ($1, $2::jsonb, $3, now())
	ON CONFLICT (project_id) DO UPDATE SET
		manifest = EXCLUDED.manifest,
		module_hash = EXCLUDED.module_hash,
		updated_at = now()`,
		projectID,
		string(payload),
		moduleHash,
	)
	return err
}

func (s *Server) loadRuntimeManifests(ctx context.Context) ([]manifest.Manifest, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT manifest FROM gonvex_runtime_manifests ORDER BY updated_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var manifests []manifest.Manifest
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var next manifest.Manifest
		if err := json.Unmarshal(payload, &next); err != nil {
			return nil, err
		}
		if next.Functions == nil {
			next.Functions = map[string]manifest.FunctionEntry{}
		}
		if next.Schema.Tables == nil {
			next.Schema = manifest.EmptySchema()
		}
		manifests = append(manifests, next)
	}
	return manifests, rows.Err()
}

func (s *Server) loadRuntimeManifest(ctx context.Context, projectID string) (manifest.Manifest, bool, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return manifest.Manifest{}, false, nil
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return manifest.Manifest{}, false, err
	}
	defer db.Close()
	var payload []byte
	err = db.QueryRowContext(ctx, `SELECT manifest FROM gonvex_runtime_manifests WHERE project_id = $1`, projectID).Scan(&payload)
	if err == sql.ErrNoRows {
		return manifest.Manifest{}, false, nil
	}
	if err != nil {
		return manifest.Manifest{}, false, err
	}
	var next manifest.Manifest
	if err := json.Unmarshal(payload, &next); err != nil {
		return manifest.Manifest{}, false, err
	}
	if next.Project == "" {
		next.Project = projectID
	}
	if next.Functions == nil {
		next.Functions = map[string]manifest.FunctionEntry{}
	}
	if next.Schema.Tables == nil {
		next.Schema = manifest.EmptySchema()
	}
	return next, true, nil
}

func generateProjectKey(projectID string) (string, error) {
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate project key: %w", err)
	}
	encodedProject := base64.RawURLEncoding.EncodeToString([]byte(strings.TrimSpace(projectID)))
	return "gvx_" + encodedProject + "." + base64.RawURLEncoding.EncodeToString(bytes[:]), nil
}

func projectNameFromID(projectID string) string {
	value := strings.TrimSpace(strings.ReplaceAll(projectID, "-", " "))
	if value == "" {
		return "Project"
	}
	return value
}

func databaseNameFromURL(databaseURL string, projectID string) string {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "gonvex_" + strings.ReplaceAll(slug(projectID), "-", "_")
	}
	name := strings.Trim(strings.TrimSpace(parsed.Path), "/")
	if name == "" {
		return "gonvex_" + strings.ReplaceAll(slug(projectID), "-", "_")
	}
	return name
}

func projectIDFromProjectKey(key string) string {
	payload, ok := strings.CutPrefix(strings.TrimSpace(key), "gvx_")
	if !ok {
		return ""
	}
	encodedProject, _, ok := strings.Cut(payload, ".")
	if !ok {
		parts := strings.Split(strings.TrimSpace(key), "_")
		if len(parts) != 3 || parts[0] != "gvx" {
			return ""
		}
		encodedProject = parts[1]
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encodedProject)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(decoded))
}

func generateProjectID() (string, error) {
	projectID, err := uuid.NewV6()
	if err != nil {
		return "", fmt.Errorf("generate project id: %w", err)
	}
	return projectID.String(), nil
}

func generateRelationshipID() (string, error) {
	relationshipID, err := uuid.NewV6()
	if err != nil {
		return "", fmt.Errorf("generate relationship id: %w", err)
	}
	return relationshipID.String(), nil
}

// generateTenantPhysicalDatabaseName returns a pure UUIDv6 used as the Postgres
// database identifier. Human-readable org/tenant labels must not be embedded in
// the physical name — they belong in gonvex_runtime_tenants.
func generateTenantPhysicalDatabaseName() (string, error) {
	id, err := uuid.NewV6()
	if err != nil {
		return "", fmt.Errorf("generate tenant database name: %w", err)
	}
	return id.String(), nil
}

func isUUIDv6(value string) bool {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	return err == nil && parsed.Version() == uuid.Version(6)
}

func isUUIDProjectID(value string) bool {
	_, err := uuid.Parse(strings.TrimSpace(value))
	return err == nil
}

func (s *Server) uniqueRuntimeProjectIDLocked() (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		projectID, err := generateProjectID()
		if err != nil {
			return "", err
		}
		if _, ok := s.projects[projectID]; ok {
			continue
		}
		if s.config.ProjectDatabases != nil && s.config.ProjectDatabases[projectID] != "" {
			continue
		}
		return projectID, nil
	}
	return "", fmt.Errorf("generate project id: exhausted collision retries")
}

func (s *Server) uniqueProjectIDLocked(name string) string {
	base := slug(name)
	if base == "" {
		base = "project"
	}
	return uniqueName(base, func(value string) bool {
		if _, ok := s.projects[value]; ok {
			return true
		}
		if s.config.ProjectDatabases != nil && s.config.ProjectDatabases[value] != "" {
			return true
		}
		return false
	})
}

func (s *Server) uniqueDatabaseNameLocked(projectID string) string {
	base := "gonvex_" + strings.ReplaceAll(slug(projectID), "-", "_")
	return uniqueName(base, func(value string) bool {
		for _, project := range s.projects {
			if project.databaseName == value {
				return true
			}
		}
		return false
	})
}

func uniqueName(base string, taken func(string) bool) string {
	if !taken(base) {
		return base
	}
	for suffix := 2; ; suffix++ {
		candidate := fmt.Sprintf("%s_%d", base, suffix)
		if !taken(candidate) {
			return candidate
		}
	}
}

func createProjectDatabase(ctx context.Context, baseURL string, databaseName string) (string, error) {
	db, err := openMaintenanceDB(baseURL)
	if err != nil {
		return "", err
	}
	defer db.Close()
	// The maintenance role may have CREATEDB without owning the database
	// (common in managed/test PostgreSQL).  Explicitly make it the owner so it
	// also owns the public schema and can provision tenant-local tables.
	var owner string
	if err := db.QueryRowContext(ctx, `SELECT current_user`).Scan(&owner); err != nil {
		return "", err
	}
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+quoteIdent(databaseName)+" OWNER "+quoteIdent(owner)); err != nil {
		return "", err
	}
	return databaseURL(baseURL, databaseName)
}

func dropProjectDatabase(ctx context.Context, baseURL string, databaseName string) error {
	db, err := openMaintenanceDB(baseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	quoted := quoteIdent(databaseName)
	_, _ = db.ExecContext(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", databaseName)
	_, err = db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoted)
	return err
}

func openMaintenanceDB(baseURL string) (*sql.DB, error) {
	maintenanceURL, err := databaseURL(baseURL, "postgres")
	if err != nil {
		return nil, err
	}
	return dbpool.Open(maintenanceURL)
}

func databaseURL(baseURL string, databaseName string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = "/" + databaseName
	return parsed.String(), nil
}

var slugPattern = regexp.MustCompile(`[^a-z0-9]+`)

func slug(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return '-'
	}, value)
	value = slugPattern.ReplaceAllString(value, "-")
	return strings.Trim(value, "-")
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
