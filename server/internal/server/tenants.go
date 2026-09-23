package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/dbpool"
	"github.com/gonvex/gonvex/server/internal/schema"
	"github.com/gonvex/gonvex/server/internal/sqlmigration"
	"github.com/jackc/pgx/v5/pgconn"
)

const projectTenantHydrationTTL = 5 * time.Second

type tenantTarget struct {
	RelationshipID string `json:"relationshipId"`
	ID             string `json:"id"`
	ProjectID      string `json:"projectId"`
	Name           string `json:"name"`
	Database       string `json:"database"`
	Status         string `json:"status"`
	Description    string `json:"description"`
	Provisioned    bool   `json:"provisioned"`
	RuntimeCreated bool   `json:"runtimeCreated"`
	databaseURL    string
	databaseName   string
	domain         string
	registered     bool
}

func (s *Server) handleTenants(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	s.hydrateProjectTenantDatabases(r.Context(), project)

	s.projectMu.RLock()
	includeLegacyGlobals := project == "" || !isUUIDProjectID(project)
	tenants := make([]tenantTarget, 0, len(s.tenants))
	for _, tenant := range s.tenants {
		if project == "" || tenant.ProjectID == project || (tenant.ProjectID == "" && includeLegacyGlobals) {
			tenants = append(tenants, tenant)
		}
	}
	s.projectMu.RUnlock()
	tenants = dedupeTenantTargets(tenants)

	sort.Slice(tenants, func(i, j int) bool {
		if tenants[i].ProjectID == tenants[j].ProjectID {
			return strings.ToLower(tenants[i].Name) < strings.ToLower(tenants[j].Name)
		}
		return tenants[i].ProjectID < tenants[j].ProjectID
	})
	writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
}

func dedupeTenantTargets(tenants []tenantTarget) []tenantTarget {
	byScopeAndDatabase := map[string]int{}
	result := make([]tenantTarget, 0, len(tenants))
	for _, tenant := range tenants {
		databaseKey := normalizeDatabaseAlias(tenant.Database)
		if databaseKey == "" {
			databaseKey = normalizeDatabaseAlias(tenant.databaseName)
		}
		if databaseKey == "" {
			databaseKey = normalizeDatabaseAlias(tenant.ID)
		}
		key := tenant.ProjectID + ":" + databaseKey
		if index, ok := byScopeAndDatabase[key]; ok {
			if tenantTargetPriority(tenant) > tenantTargetPriority(result[index]) {
				result[index] = tenant
			}
			continue
		}
		byScopeAndDatabase[key] = len(result)
		result = append(result, tenant)
	}
	return result
}

func tenantTargetPriority(tenant tenantTarget) int {
	if tenant.registered {
		return 4
	}
	if tenant.Description == "Persisted tenant from landlord database." {
		return 3
	}
	if tenant.Description == "Discovered local tenant database." {
		return 2
	}
	if tenant.ProjectID != "" {
		return 1
	}
	return 0
}

func matchingRegisteredTenant(
	tenants map[string]tenantTarget,
	project string,
	documentID string,
	databaseAlias string,
	domain string,
) (tenantTarget, bool) {
	documentID = strings.TrimSpace(documentID)
	databaseAlias = strings.TrimSpace(databaseAlias)
	domain = strings.TrimSpace(domain)
	var fallback tenantTarget
	foundFallback := false
	for _, tenant := range tenants {
		if tenant.ProjectID != project || !tenant.registered {
			continue
		}
		if documentID != "" && tenant.ID == documentID {
			return tenant, true
		}
		if databaseAlias != "" && normalizeDatabaseAlias(tenant.Database) == normalizeDatabaseAlias(databaseAlias) {
			fallback = tenant
			foundFallback = true
			continue
		}
		if !foundFallback && domain != "" && strings.EqualFold(strings.TrimSpace(tenant.domain), domain) {
			fallback = tenant
			foundFallback = true
		}
	}
	return fallback, foundFallback
}

func (s *Server) loadConfiguredTenantDatabases() {
	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	if s.explicitTenantDatabases == nil {
		s.explicitTenantDatabases = make(map[string]string, len(s.config.TenantDatabases))
		for key, databaseURL := range s.config.TenantDatabases {
			s.explicitTenantDatabases[key] = databaseURL
		}
	}
	for key, databaseURL := range s.config.TenantDatabases {
		project, tenantID := splitTenantDatabaseKey(key)
		if tenantID == "" || databaseURL == "" {
			continue
		}
		relationshipID := ""
		if project != "" {
			relationshipID, _ = generateRelationshipID()
		}
		storeKey := tenantStoreKey(project, tenantID)
		s.tenants[storeKey] = tenantTarget{
			RelationshipID: relationshipID,
			ID:             tenantID,
			ProjectID:      project,
			Name:           tenantID,
			Database:       databaseNameFromURL(databaseURL, tenantID),
			Status:         "local",
			Description:    "Configured tenant database.",
			Provisioned:    true,
			databaseURL:    databaseURL,
			databaseName:   databaseNameFromURL(databaseURL, tenantID),
			RuntimeCreated: false,
		}
	}
}

func splitTenantDatabaseKey(key string) (string, string) {
	project, tenantID, ok := strings.Cut(strings.TrimSpace(key), ":")
	if !ok {
		return "", strings.TrimSpace(key)
	}
	return strings.TrimSpace(project), strings.TrimSpace(tenantID)
}

func (s *Server) loadTenantRegistry(ctx context.Context, project string) ([]tenantTarget, error) {
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, nil
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `SELECT
		relationship_id, tenant_id, name, database_alias, database_name,
		database_url, domain, status, description, provisioned, runtime_created
		FROM gonvex_runtime_tenants
		WHERE project_id = $1
		ORDER BY name, tenant_id`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tenants := []tenantTarget{}
	for rows.Next() {
		var tenant tenantTarget
		if err := rows.Scan(
			&tenant.RelationshipID,
			&tenant.ID,
			&tenant.Name,
			&tenant.Database,
			&tenant.databaseName,
			&tenant.databaseURL,
			&tenant.domain,
			&tenant.Status,
			&tenant.Description,
			&tenant.Provisioned,
			&tenant.RuntimeCreated,
		); err != nil {
			return nil, err
		}
		tenant.ProjectID = project
		tenant.registered = true
		tenants = append(tenants, tenant)
	}
	return tenants, rows.Err()
}

func (s *Server) saveTenantRegistry(ctx context.Context, tenant tenantTarget) (tenantTarget, error) {
	tenant.ProjectID = strings.TrimSpace(tenant.ProjectID)
	tenant.ID = strings.TrimSpace(tenant.ID)
	if tenant.ProjectID == "" || tenant.ID == "" {
		return tenant, nil
	}
	if tenant.RelationshipID == "" {
		var err error
		tenant.RelationshipID, err = generateRelationshipID()
		if err != nil {
			return tenant, err
		}
	}
	if tenant.Name == "" {
		tenant.Name = tenant.ID
	}
	if tenant.Status == "" {
		tenant.Status = "local"
	}
	if tenant.databaseName == "" {
		if tenant.databaseURL != "" {
			tenant.databaseName = databaseNameFromURL(tenant.databaseURL, tenant.Database)
		} else {
			tenant.databaseName = tenant.Database
		}
	}

	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return tenant, err
	}
	defer db.Close()

	// A landlord-backed tenant may have a non-human tenant id while an older
	// runtime inferred the same relationship from its project-scoped database
	// alias. Reuse that row's UUID instead of creating duplicate ownership rows.
	var existingRelationshipID string
	err = db.QueryRowContext(ctx, `SELECT relationship_id
		FROM gonvex_runtime_tenants
		WHERE project_id = $1
		  AND (tenant_id = $2 OR ($3 <> '' AND database_name = $3))
		ORDER BY CASE WHEN tenant_id = $2 THEN 0 ELSE 1 END
		LIMIT 1`, tenant.ProjectID, tenant.ID, tenant.databaseName).Scan(&existingRelationshipID)
	if err != nil && err != sql.ErrNoRows {
		return tenant, err
	}
	if existingRelationshipID != "" {
		tenant.RelationshipID = existingRelationshipID
	}

	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_tenants (
		relationship_id, project_id, tenant_id, name, database_alias,
		database_name, database_url, domain, status, description,
		provisioned, runtime_created, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now())
	ON CONFLICT (relationship_id) DO UPDATE SET
		project_id = EXCLUDED.project_id,
		tenant_id = EXCLUDED.tenant_id,
		name = EXCLUDED.name,
		database_alias = EXCLUDED.database_alias,
		database_name = EXCLUDED.database_name,
		database_url = EXCLUDED.database_url,
		domain = EXCLUDED.domain,
		status = EXCLUDED.status,
		description = EXCLUDED.description,
		provisioned = EXCLUDED.provisioned,
		runtime_created = EXCLUDED.runtime_created,
		updated_at = now()`,
		tenant.RelationshipID,
		tenant.ProjectID,
		tenant.ID,
		tenant.Name,
		tenant.Database,
		tenant.databaseName,
		tenant.databaseURL,
		tenant.domain,
		tenant.Status,
		tenant.Description,
		tenant.Provisioned,
		tenant.RuntimeCreated,
	)
	if err != nil {
		return tenant, err
	}
	tenant.registered = true
	return tenant, nil
}

func (s *Server) deleteTenantRegistry(ctx context.Context, project string, tenant tenantTarget) error {
	project = strings.TrimSpace(project)
	if project == "" {
		return nil
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()
	if tenant.RelationshipID != "" {
		_, err = db.ExecContext(ctx, `DELETE FROM gonvex_runtime_tenants WHERE project_id = $1 AND relationship_id = $2`, project, tenant.RelationshipID)
	} else {
		_, err = db.ExecContext(ctx, `DELETE FROM gonvex_runtime_tenants WHERE project_id = $1 AND tenant_id = $2`, project, tenant.ID)
	}
	return err
}

func (s *Server) hydrateLandlordTenants(ctx context.Context, project string) {
	if err := s.hydrateLandlordTenantsWithError(ctx, project); err != nil {
		slog.Debug("hydrate landlord tenants", "project", project, "error", err)
	}
}

func (s *Server) hydrateLandlordTenantsWithError(ctx context.Context, project string) error {
	project = strings.TrimSpace(project)
	if project == "" {
		return nil
	}
	projectDatabaseURL := s.databaseURLForProject(project)
	if strings.TrimSpace(projectDatabaseURL) == "" {
		return nil
	}
	store, err := s.tenantStores.Store(ctx, tenantStoreKey(project, "__landlord__"), projectDatabaseURL)
	if err != nil {
		return fmt.Errorf("open landlord database: %w", err)
	}
	rows, err := store.DB.QueryContext(ctx, `SELECT id, COALESCE(name, ''), COALESCE(database, ''), COALESCE(domain, '') FROM tenants ORDER BY name, id`)
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "42P01" {
			// The landlord tenants table is an optional discovery source. Generic
			// Gonvex projects can register tenant databases directly instead.
			return nil
		}
		return fmt.Errorf("query landlord tenants: %w", err)
	}
	defer rows.Close()

	existingDatabases := s.existingLocalDatabaseNames(ctx)
	imported := map[string]tenantTarget{}
	for rows.Next() {
		var documentID string
		var name string
		var databaseAlias string
		var domain string
		if err := rows.Scan(&documentID, &name, &databaseAlias, &domain); err != nil {
			return fmt.Errorf("scan landlord tenant: %w", err)
		}
		tenantID := persistedTenantRelationshipID(documentID, databaseAlias, domain)
		if tenantID == "" {
			continue
		}
		if strings.TrimSpace(name) == "" {
			name = tenantID
		}
		tenant := tenantTarget{
			ID:           tenantID,
			ProjectID:    project,
			Name:         name,
			Database:     databaseAlias,
			Status:       "local",
			Description:  "Persisted tenant from landlord database.",
			Provisioned:  false,
			databaseName: tenantDatabaseNameForPersistedTenant(project, tenantID, databaseAlias, domain, existingDatabases),
			domain:       domain,
		}
		s.projectMu.RLock()
		existing, found := matchingRegisteredTenant(
			s.tenants,
			project,
			documentID,
			databaseAlias,
			domain,
		)
		s.projectMu.RUnlock()
		if found {
			tenant.RelationshipID = existing.RelationshipID
			tenant.databaseName = existing.databaseName
			tenant.databaseURL = existing.databaseURL
			tenant.Provisioned = existing.Provisioned
			tenant.RuntimeCreated = existing.RuntimeCreated
		}
		imported[tenantStoreKey(project, tenantID)] = tenant
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate landlord tenants: %w", err)
	}
	if len(imported) == 0 {
		return nil
	}

	resolved := make([]tenantTarget, 0, len(imported))
	s.projectMu.Lock()
	if s.config.TenantDatabases == nil {
		s.config.TenantDatabases = map[string]string{}
	}
	for key, tenant := range imported {
		existing := s.tenants[key]
		tenant.RelationshipID = existing.RelationshipID
		tenant = s.resolveTenantDatabaseURLLocked(project, tenant)
		previousURL := existing.databaseURL
		if existing.Provisioned && existing.databaseURL != "" && existing.databaseURL == tenant.databaseURL {
			tenant.Provisioned = true
		}
		s.tenants[key] = tenant
		if explicitURL := s.explicitTenantDatabases[key]; explicitURL != "" {
			s.config.TenantDatabases[key] = explicitURL
		} else if tenant.databaseURL != "" {
			s.config.TenantDatabases[key] = tenant.databaseURL
		}
		if previousURL != "" && previousURL != tenant.databaseURL {
			go s.cache.invalidateRows(context.Background(), project, tenant.ID, "")
		}
		resolved = append(resolved, tenant)
	}
	s.projectMu.Unlock()

	for _, tenant := range resolved {
		registered, err := s.saveTenantRegistry(ctx, tenant)
		if err != nil {
			slog.Debug("persist landlord tenant relationship", "project", project, "tenant", tenant.ID, "error", err)
			continue
		}
		s.mergeProjectTenants(project, []tenantTarget{registered})
	}
	return nil
}

func (s *Server) resolveTenantDatabaseURLLocked(project string, tenant tenantTarget) tenantTarget {
	if configuredURL := s.configuredTenantDatabaseURLLocked(project, tenant); configuredURL != "" {
		tenant.databaseURL = configuredURL
		tenant.databaseName = databaseNameFromURL(configuredURL, tenant.databaseName)
		return tenant
	}
	if strings.TrimSpace(tenant.databaseName) != "" && strings.TrimSpace(s.config.PostgresURL) != "" {
		if tenantURL, err := databaseURL(s.config.PostgresURL, tenant.databaseName); err == nil {
			tenant.databaseURL = tenantURL
		}
	}
	return tenant
}

// createTenantSlugIDAllowed reports whether this caller may register a
// slug-addressed tenant under a modern (UUID) project. Ownership rows for
// modern projects normally use opaque UUID v6 ids so dashboard/public creation
// can't squat slugs or mint duplicate relationships. Two deliberate
// exceptions: the runtime admin key (CI provisions subdomain-addressed
// tenants like "e2e-parallel", whose wire id must equal the slug — the shape
// adopted legacy rows already use), and the auth-optional "local" developer
// credential so bare local runtimes keep their DX.
func createTenantSlugIDAllowed(actor dashboardActor, ok bool) bool {
	return ok && (actor.credentialKind == "adminKey" || actor.credentialKind == "local")
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var payload struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		ProjectID string `json:"projectId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	project := strings.TrimSpace(payload.ProjectID)
	if project == "" {
		project = projectID(r)
	}
	if project == "" {
		project = "default"
	}
	s.hydrateProjects()
	s.hydrateProjectTenantDatabases(r.Context(), project)

	name := strings.TrimSpace(payload.Name)
	requestedTenantID := strings.TrimSpace(payload.ID)
	modernProject := isUUIDProjectID(project)
	if modernProject && requestedTenantID != "" && !isUUIDv6(requestedTenantID) {
		if actor, ok := s.dashboardActorFromRequest(r); !createTenantSlugIDAllowed(actor, ok) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant id must be a UUID v6"})
			return
		}
	}
	if !modernProject {
		requestedTenantID = slug(requestedTenantID)
	}
	if name == "" {
		name = requestedTenantID
	}
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant name or id is required"})
		return
	}
	if s.config.PostgresURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "DATABASE_URL is not configured"})
		return
	}

	s.projectMu.Lock()
	projectTarget, projectExists := s.projects[project]
	if modernProject && !projectExists {
		s.projectMu.Unlock()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "project not found"})
		return
	}
	if modernProject && normalizedDatabaseModeWithDefault(projectTarget.DatabaseMode) != "multiTenant" {
		s.projectMu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "project is not configured for tenant databases"})
		return
	}

	tenantID := requestedTenantID
	if tenantID == "" {
		if modernProject {
			var err error
			tenantID, err = generateRelationshipID()
			if err != nil {
				s.projectMu.Unlock()
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
		} else {
			tenantID = s.uniqueTenantIDLocked(project, name)
		}
	}
	key := tenantStoreKey(project, tenantID)
	if existing, ok := s.tenants[key]; ok {
		s.projectMu.Unlock()
		if modernProject && !existing.registered {
			registered, err := s.saveTenantRegistry(r.Context(), existing)
			if err != nil || !registered.registered {
				if err == nil {
					err = fmt.Errorf("tenant relationship registry is unavailable")
				}
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("persist tenant relationship: %v", err)})
				return
			}
			existing = registered
			s.mergeProjectTenants(project, []tenantTarget{registered})
		}
		if existing.databaseURL != "" {
			if err := s.provisionTenantDatabaseWithSync(r.Context(), project, existing.databaseURL); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
		}
		s.registerProjectCrons(project)
		writeJSON(w, http.StatusOK, map[string]any{"tenant": existing})
		return
	}
	databaseAlias := slug(name)
	if databaseAlias == "" {
		databaseAlias = tenantID
	}
	if s.tenantDatabaseAliasTakenLocked(project, databaseAlias, key) {
		s.projectMu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "tenant database name already exists for this project"})
		return
	}
	// Physical DB id is an opaque UUIDv6. database_alias keeps a human slug;
	// name keeps the display label shown in the Data tab.
	databaseName, err := generateTenantPhysicalDatabaseName()
	if err != nil {
		s.projectMu.Unlock()
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.projectMu.Unlock()

	tenantDatabaseURL, err := createProjectDatabase(r.Context(), s.config.PostgresURL, databaseName)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already exists") {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "tenant database already exists without a registered relationship"})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}
	if err := s.provisionTenantDatabaseWithSync(r.Context(), project, tenantDatabaseURL); err != nil {
		_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	relationshipID := tenantID
	if !modernProject || !isUUIDv6(tenantID) {
		// Slug-addressed tenants (legacy projects, or admin-created modern
		// tenants) still get an opaque relationship id; only UUID tenant ids
		// double as their own relationship id.
		relationshipID, err = generateRelationshipID()
		if err != nil {
			_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}
	tenant := tenantTarget{
		RelationshipID: relationshipID,
		ID:             tenantID,
		ProjectID:      project,
		Name:           name,
		Database:       databaseAlias,
		Status:         "local",
		Description:    "Runtime-created tenant database.",
		Provisioned:    true,
		RuntimeCreated: true,
		databaseURL:    tenantDatabaseURL,
		databaseName:   databaseName,
	}
	registered, err := s.saveTenantRegistry(r.Context(), tenant)
	if err != nil {
		_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("persist tenant relationship: %v", err)})
		return
	}
	if modernProject && !registered.registered {
		_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "tenant relationship registry is unavailable"})
		return
	}
	tenant = registered

	s.projectMu.Lock()
	if s.config.TenantDatabases == nil {
		s.config.TenantDatabases = map[string]string{}
	}
	s.config.TenantDatabases[key] = tenantDatabaseURL
	s.tenants[key] = tenant
	s.projectMu.Unlock()
	s.invalidateProjectTenantHydration(project)
	s.registerProjectCrons(project)

	writeJSON(w, http.StatusCreated, map[string]any{"tenant": tenant})
}

func (s *Server) handleDeleteTenant(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	if project == "" {
		project = "default"
	}
	tenantID := strings.TrimSpace(r.PathValue("tenant"))
	if tenantID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant id is required"})
		return
	}

	s.hydrateProjectTenantDatabases(r.Context(), project)

	s.projectMu.Lock()
	key := tenantStoreKey(project, tenantID)
	tenant, ok := s.tenants[key]
	if !ok {
		s.projectMu.Unlock()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
		return
	}
	databaseName := tenant.databaseName
	if databaseName == "" {
		databaseName = tenant.Database
	}
	s.projectMu.Unlock()

	if err := s.cleanupProjectLandlordTenantReferences(r.Context(), project, tenant); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := s.deleteTenantRegistry(r.Context(), project, tenant); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("delete tenant relationship: %v", err)})
		return
	}
	if databaseName != "" {
		if err := dropProjectDatabase(r.Context(), s.config.PostgresURL, databaseName); err != nil {
			_, _ = s.saveTenantRegistry(context.Background(), tenant)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}

	s.projectMu.Lock()
	delete(s.tenants, key)
	if s.config.TenantDatabases != nil {
		delete(s.config.TenantDatabases, key)
	}
	s.projectMu.Unlock()
	s.invalidateProjectTenantHydration(project)
	s.tenantStores.Close()
	s.cache.invalidateRows(r.Context(), project, tenantID, "")
	s.registerProjectCrons(project)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) cleanupProjectLandlordTenantReferences(ctx context.Context, project string, tenant tenantTarget) error {
	projectDatabaseURL := s.databaseURLForProject(project)
	if strings.TrimSpace(projectDatabaseURL) == "" {
		return nil
	}
	db, err := dbpool.Open(projectDatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}

	aliases := tenantReferenceAliases(tenant)
	if len(aliases) == 0 {
		return nil
	}
	if err := deleteRowsMatchingAnyColumn(ctx, db, "userTenantMap", []string{"tenantId", "tenant_id"}, aliases); err != nil {
		return err
	}
	if err := deleteRowsMatchingAnyColumn(ctx, db, "users", []string{"tenantId", "tenant_id"}, aliases); err != nil {
		return err
	}
	return deleteRowsMatchingAnyColumn(ctx, db, "tenants", []string{"id", "domain", "database"}, aliases)
}

func tenantReferenceAliases(tenant tenantTarget) []string {
	seen := map[string]bool{}
	aliases := []string{}
	for _, value := range []string{tenant.ID, tenant.domain, tenant.Database, tenant.databaseName, tenant.Name} {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		aliases = append(aliases, value)
	}
	return aliases
}

func deleteRowsMatchingAnyColumn(ctx context.Context, db *sql.DB, table string, candidateColumns []string, aliases []string) error {
	if len(aliases) == 0 || !serverTableExists(ctx, db, table) {
		return nil
	}
	columns, err := serverTableColumns(ctx, db, table)
	if err != nil {
		return err
	}
	columnSet := map[string]bool{}
	for _, column := range columns {
		columnSet[column] = true
	}
	matchedColumns := []string{}
	for _, column := range candidateColumns {
		if columnSet[column] {
			matchedColumns = append(matchedColumns, column)
		}
	}
	if len(matchedColumns) == 0 {
		return nil
	}

	for _, alias := range aliases {
		predicates := make([]string, 0, len(matchedColumns))
		args := make([]any, 0, len(matchedColumns))
		for _, column := range matchedColumns {
			args = append(args, alias)
			predicates = append(predicates, fmt.Sprintf("%s::text = $%d", quoteIdent(column), len(args)))
		}
		query := fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(table), strings.Join(predicates, " OR "))
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

func serverTableExists(ctx context.Context, db *sql.DB, table string) bool {
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)
	`, table).Scan(&exists)
	return err == nil && exists
}

func serverTableColumns(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := []string{}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, rows.Err()
}

func (s *Server) uniqueTenantIDLocked(projectID string, name string) string {
	base := slug(name)
	if base == "" {
		base = "tenant"
	}
	return uniqueName(base, func(value string) bool {
		if _, ok := s.tenants[tenantStoreKey(projectID, value)]; ok {
			return true
		}
		if s.config.TenantDatabases != nil && s.config.TenantDatabases[tenantStoreKey(projectID, value)] != "" {
			return true
		}
		return false
	})
}

func (s *Server) uniqueTenantDatabaseNameLocked(projectID string, tenantID string) string {
	base := tenantDatabaseName(projectID, tenantID)
	return uniqueName(base, func(value string) bool {
		for _, tenant := range s.tenants {
			if tenant.databaseName == value {
				return true
			}
		}
		return false
	})
}

func (s *Server) tenantDatabaseAliasTakenLocked(projectID string, alias string, exceptKey string) bool {
	alias = normalizeDatabaseAlias(alias)
	if alias == "" {
		return false
	}
	for key, tenant := range s.tenants {
		if key == exceptKey || tenant.ProjectID != projectID {
			continue
		}
		if normalizeDatabaseAlias(tenant.Database) == alias {
			return true
		}
	}
	return false
}

func provisionTenantDatabase(ctx context.Context, databaseURL string, desiredSchema manifest.Schema) error {
	if databaseURL == "" {
		return fmt.Errorf("tenant database URL is not configured")
	}
	if _, err := schema.Apply(ctx, databaseURL, desiredSchema); err != nil {
		return err
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	return ensureTenantLocalTables(ctx, db)
}

func (s *Server) provisionTenantDatabaseWithSync(ctx context.Context, project string, databaseURL string) error {
	if databaseURL == "" {
		return fmt.Errorf("tenant database URL is not configured")
	}
	current := s.runtime.ManifestForProject(project)
	desiredSchema := current.Schema.TenantSchema()
	syncDefinitions, err := syncDefinitionsForSchema(manifestSyncDefinitions(current), desiredSchema)
	if err != nil {
		return err
	}
	// Existing tenant databases may predate durable sync. Re-applying the
	// ordinary application schema alone leaves them without _gonvex_sync_clock,
	// causing every sync.openMany subscription to stall. ApplyWithSync is
	// idempotent and repairs both old and newly created tenant databases.
	if _, err := schema.ApplyWithSync(ctx, databaseURL, desiredSchema, syncDefinitions); err != nil {
		return err
	}
	migrations, err := migrationsFromBundle(current.Bundle)
	if err != nil {
		return err
	}
	if _, err := sqlmigration.Apply(ctx, databaseURL, sqlmigration.Filter(migrations, sqlmigration.ScopeTenant), false); err != nil {
		return fmt.Errorf("apply tenant SQL migrations: %w", err)
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	if err := ensureTenantLocalTables(ctx, db); err != nil {
		return err
	}
	return nil
}

func (s *Server) ensureRuntimeTenantDatabase(ctx context.Context, project string, tenantID string, tenantDatabaseURL string) (string, error) {
	project = strings.TrimSpace(project)
	tenantID = strings.TrimSpace(tenantID)
	tenantDatabaseURL = strings.TrimSpace(tenantDatabaseURL)
	result, err, _ := s.tenantProvisions.Do(tenantStoreKey(project, tenantID), func() (any, error) {
		return s.ensureRuntimeTenantDatabaseOnce(ctx, project, tenantID, tenantDatabaseURL)
	})
	if err != nil {
		return "", err
	}
	databaseURL, ok := result.(string)
	if !ok {
		return "", fmt.Errorf("tenant %q provisioning returned an invalid database URL", tenantID)
	}
	return databaseURL, nil
}

func (s *Server) ensureRuntimeTenantDatabaseOnce(ctx context.Context, project string, tenantID string, tenantDatabaseURL string) (string, error) {
	if isUUIDProjectID(project) && tenantID != "" && tenantID != project && tenantDatabaseURL == "" {
		return "", fmt.Errorf("tenant %q is not related to project %q", tenantID, project)
	}
	if project == "" || tenantID == "" || tenantID == project || tenantDatabaseURL == "" {
		return tenantDatabaseURL, nil
	}

	key := tenantStoreKey(project, tenantID)
	s.projectMu.RLock()
	tenant, ok := s.tenants[key]
	projectDatabaseURL := s.config.DatabaseURL(project)
	postgresURL := strings.TrimSpace(s.config.PostgresURL)
	s.projectMu.RUnlock()
	if isUUIDProjectID(project) && tenantDatabaseURL == projectDatabaseURL {
		return "", fmt.Errorf("tenant %q cannot use project %q's landlord database", tenantID, project)
	}
	if !ok || tenant.databaseURL == "" || tenant.databaseURL != tenantDatabaseURL || tenantDatabaseURL == projectDatabaseURL {
		return tenantDatabaseURL, nil
	}
	if tenant.Provisioned {
		return tenantDatabaseURL, nil
	}

	desiredSchema := s.runtime.ManifestForProject(project).Schema.TenantSchema()
	if err := s.provisionTenant(ctx, tenantDatabaseURL, desiredSchema); err == nil {
		if err := s.installTenantSyncLog(ctx, project, tenantDatabaseURL, desiredSchema); err != nil {
			return "", err
		}
		if err := s.markTenantDatabaseProvisioned(ctx, project, tenantID, tenantDatabaseURL); err != nil {
			return "", err
		}
		return tenantDatabaseURL, nil
	} else if !isMissingTenantDatabaseError(err) {
		return "", err
	}

	if postgresURL == "" {
		return "", fmt.Errorf("tenant database %q does not exist and DATABASE_URL is not configured", databaseNameFromURL(tenantDatabaseURL, tenant.databaseName))
	}
	databaseName := strings.TrimSpace(tenant.databaseName)
	if databaseName == "" {
		databaseName = databaseNameFromURL(tenantDatabaseURL, tenantDatabaseName(project, tenantID))
	}
	createdURL, err := createProjectDatabase(ctx, postgresURL, databaseName)
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return "", err
		}
		createdURL, err = databaseURL(postgresURL, databaseName)
		if err != nil {
			return "", err
		}
	}
	if err := s.provisionTenant(ctx, createdURL, desiredSchema); err != nil {
		return "", err
	}
	if err := s.installTenantSyncLog(ctx, project, createdURL, desiredSchema); err != nil {
		return "", err
	}
	if err := s.markTenantDatabaseProvisioned(ctx, project, tenantID, createdURL); err != nil {
		return "", err
	}
	return createdURL, nil
}

func (s *Server) markTenantDatabaseProvisioned(ctx context.Context, project string, tenantID string, databaseURL string) error {
	key := tenantStoreKey(project, tenantID)
	s.projectMu.Lock()
	tenant := s.tenants[key]
	tenant.ID = tenantID
	tenant.ProjectID = project
	tenant.databaseURL = databaseURL
	tenant.databaseName = databaseNameFromURL(databaseURL, tenant.databaseName)
	// Never promote the physical DB id into the human-facing database_alias.
	// UI and registry alias should stay as name/slug; only database_name is the
	// opaque Postgres identifier.
	if tenant.Database == "" {
		if tenant.Name != "" {
			tenant.Database = slug(tenant.Name)
		}
		if tenant.Database == "" {
			tenant.Database = tenant.ID
		}
	}
	tenant.Provisioned = true
	s.tenants[key] = tenant
	if s.config.TenantDatabases == nil {
		s.config.TenantDatabases = map[string]string{}
	}
	s.config.TenantDatabases[key] = databaseURL
	s.projectMu.Unlock()

	registered, err := s.saveTenantRegistry(ctx, tenant)
	if err != nil {
		if isUUIDProjectID(project) {
			return fmt.Errorf("persist tenant relationship: %w", err)
		}
		return nil
	}
	if isUUIDProjectID(project) && !registered.registered {
		return fmt.Errorf("tenant relationship registry is unavailable")
	}
	if registered.registered {
		s.mergeProjectTenants(project, []tenantTarget{registered})
	}
	return nil
}

func (s *Server) provisionCreatedTenant(ctx context.Context, project string, result any) error {
	tenantID := tenantIDFromMutationResult(result)
	if tenantID == "" {
		return nil
	}
	// tenants.create just inserted the landlord row. The mutation path already
	// hydrated this project for the request context, so a TTL-gated hydrate
	// would skip and leave the new tenant unregistered — which surfaces as
	// "tenant X is not related to project Y" for UUID multi-tenant projects.
	s.invalidateProjectTenantHydration(project)
	s.hydrateProjectTenantDatabases(ctx, project)
	databaseURL := s.databaseURLForTenant(project, tenantID)
	if _, err := s.ensureRuntimeTenantDatabase(ctx, project, tenantID, databaseURL); err != nil {
		return err
	}
	s.registerProjectCrons(project)
	return nil
}

func tenantIDFromMutationResult(result any) string {
	switch value := result.(type) {
	case string:
		return strings.TrimSpace(value)
	case map[string]any:
		for _, key := range []string{"_id", "id", "domain", "database"} {
			if id := strings.TrimSpace(fmt.Sprint(value[key])); id != "" && id != "<nil>" {
				return id
			}
		}
	case map[string]string:
		for _, key := range []string{"_id", "id", "domain", "database"} {
			if id := strings.TrimSpace(value[key]); id != "" {
				return id
			}
		}
	}
	return ""
}

func (s *Server) applyTenantSchemasForProject(
	ctx context.Context,
	project string,
	desiredSchema manifest.Schema,
	syncDefinitions map[string]manifest.SyncDefinition,
	options schema.ApplyOptions,
) (schema.Result, error) {
	if err := s.hydrateProjectTenantDatabasesWithError(ctx, project, s.hydrateProjectTenantDatabasesUncachedWithError); err != nil {
		return schema.Result{}, fmt.Errorf("discover tenant databases: %w", err)
	}
	desiredSchema = desiredSchema.TenantSchema()

	s.projectMu.RLock()
	tenants := make([]tenantTarget, 0, len(s.tenants))
	for _, tenant := range s.tenants {
		if tenant.ProjectID == project && tenant.databaseURL != "" {
			tenants = append(tenants, tenant)
		}
	}
	s.projectMu.RUnlock()

	tenantSyncDefinitions, err := syncDefinitionsForSchema(syncDefinitions, desiredSchema)
	if err != nil {
		return schema.Result{}, err
	}
	return applyTenantSchemas(ctx, tenants, desiredSchema, func(ctx context.Context, databaseURL string, desired manifest.Schema) (schema.Result, error) {
		return schema.ApplyWithOptions(ctx, databaseURL, desired, tenantSyncDefinitions, options)
	})
}

func (s *Server) projectSyncStorageInstalled(
	ctx context.Context,
	project string,
	desiredSchema manifest.Schema,
	syncDefinitions map[string]manifest.SyncDefinition,
) (bool, error) {
	landlordDefinitions, err := syncDefinitionsForSchema(syncDefinitions, desiredSchema.LandlordSchema())
	if err != nil {
		return false, err
	}
	if len(landlordDefinitions) > 0 {
		installed, err := schema.SyncStorageInstalled(ctx, s.databaseURLForProject(project))
		if err != nil || !installed {
			return installed, err
		}
	}

	tenantDefinitions, err := syncDefinitionsForSchema(syncDefinitions, desiredSchema.TenantSchema())
	if err != nil {
		return false, err
	}
	if len(tenantDefinitions) == 0 {
		return true, nil
	}
	if err := s.hydrateProjectTenantDatabasesWithError(ctx, project, s.hydrateProjectTenantDatabasesUncachedWithError); err != nil {
		return false, fmt.Errorf("discover tenant databases: %w", err)
	}
	s.projectMu.RLock()
	tenants := make([]tenantTarget, 0, len(s.tenants))
	for _, tenant := range s.tenants {
		if tenant.ProjectID == project && tenant.databaseURL != "" {
			tenants = append(tenants, tenant)
		}
	}
	s.projectMu.RUnlock()
	for _, tenant := range dedupeTenantTargets(tenants) {
		installed, err := schema.SyncStorageInstalled(ctx, tenant.databaseURL)
		if err != nil || !installed {
			return installed, err
		}
	}
	return true, nil
}

type tenantSchemaApplyFunc func(context.Context, string, manifest.Schema) (schema.Result, error)

// A schema apply uses at most two PostgreSQL connections. Keep rollout
// concurrency deliberately modest now that each database is inspected in bulk;
// tenant count should not turn a deployment into an unbounded connection spike.
const tenantSchemaApplyConcurrency = 4

type tenantSchemaApplyOutcome struct {
	result schema.Result
	err    error
}

func applyTenantSchemas(
	ctx context.Context,
	tenants []tenantTarget,
	desiredSchema manifest.Schema,
	apply tenantSchemaApplyFunc,
) (schema.Result, error) {
	targets := schemaTenantTargets(tenants)
	if len(targets) == 0 {
		return schema.Result{}, nil
	}

	outcomes := make([]tenantSchemaApplyOutcome, len(targets))
	jobs := make(chan int)
	workerCount := min(tenantSchemaApplyConcurrency, len(targets))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				outcomes[index].result, outcomes[index].err = apply(ctx, targets[index].databaseURL, desiredSchema)
			}
		}()
	}
	for index := range targets {
		jobs <- index
	}
	close(jobs)
	workers.Wait()

	result := schema.Result{}
	for index, tenant := range targets {
		outcome := outcomes[index]
		if outcome.err != nil {
			if isMissingTenantDatabaseError(outcome.err) {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: skipped missing tenant database", tenant.ID))
				continue
			}
			return result, fmt.Errorf("tenant %s schema sync failed: %w", tenant.ID, outcome.err)
		}
		for _, statement := range outcome.result.Applied {
			result.Applied = append(result.Applied, fmt.Sprintf("%s: %s", tenant.ID, statement))
		}
		for _, warning := range outcome.result.Warnings {
			result.Warnings = append(result.Warnings, fmt.Sprintf("%s: %s", tenant.ID, warning))
		}
	}
	return result, nil
}

func (s *Server) hydrateProjectTenantDatabases(ctx context.Context, project string) {
	if err := s.hydrateProjectTenantDatabasesWithError(ctx, project, s.hydrateProjectTenantDatabasesUncachedWithError); err != nil {
		slog.Debug("hydrate project tenant databases", "project", project, "error", err)
	}
}

func (s *Server) hydrateProjectTenantDatabasesWith(
	ctx context.Context,
	project string,
	hydrate func(context.Context, string),
) {
	_ = s.hydrateProjectTenantDatabasesWithError(ctx, project, func(ctx context.Context, project string) error {
		hydrate(ctx, project)
		return nil
	})
}

func (s *Server) hydrateProjectTenantDatabasesWithError(
	ctx context.Context,
	project string,
	hydrate func(context.Context, string) error,
) error {
	project = strings.TrimSpace(project)
	if project == "" {
		return nil
	}
	_, err, _ := s.tenantHydrations.Do(project, func() (any, error) {
		if !s.shouldHydrateProjectTenants(project) {
			return nil, nil
		}
		if err := hydrate(ctx, project); err != nil {
			s.invalidateProjectTenantHydration(project)
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (s *Server) hydrateProjectTenantDatabasesUncached(ctx context.Context, project string) {
	if err := s.hydrateProjectTenantDatabasesUncachedWithError(ctx, project); err != nil {
		slog.Debug("hydrate project tenant databases", "project", project, "error", err)
	}
}

func (s *Server) hydrateProjectTenantDatabasesUncachedWithError(ctx context.Context, project string) error {
	// Load the durable registry first so landlord rows can adopt the already
	// migrated physical database instead of generating an empty replacement.
	registered, err := s.loadTenantRegistry(ctx, project)
	if err != nil {
		return fmt.Errorf("load tenant relationship registry: %w", err)
	}
	s.mergeProjectTenants(project, registered)

	// The project's own landlord database is the source of public tenant ids.
	// Its legacy Convex document ids are replaced by domains while preserving
	// any registered database relationship loaded above.
	if err := s.hydrateLandlordTenantsWithError(ctx, project); err != nil {
		return err
	}

	// Project-scoped environment configuration predates the registry. Preserve
	// those explicit mappings and backfill them without exposing global entries
	// to UUIDv6 projects.
	s.projectMu.RLock()
	configured := []tenantTarget{}
	for _, tenant := range s.tenants {
		if tenant.ProjectID == project && tenant.Description == "Configured tenant database." && !tenant.registered {
			configured = append(configured, tenant)
		}
	}
	s.projectMu.RUnlock()
	for _, tenant := range configured {
		persisted, saveErr := s.saveTenantRegistry(ctx, tenant)
		if saveErr != nil {
			slog.Debug("persist configured tenant relationship", "project", project, "tenant", tenant.ID, "error", saveErr)
			continue
		}
		s.mergeProjectTenants(project, []tenantTarget{persisted})
	}

	// Pre-registry projects used <alias>_<project-id> as their only durable
	// relationship. Import that exact historical convention for legacy IDs.
	// UUIDv6 projects never enter this path, so a fresh project cannot adopt any
	// pre-existing database by name or table shape.
	if !shouldMigrateLegacyTenantRelationships(project) || strings.TrimSpace(s.config.PostgresURL) == "" {
		return nil
	}
	legacy, err := s.discoverLegacyProjectTenantDatabases(ctx, project)
	if err != nil {
		return fmt.Errorf("discover legacy tenant databases: %w", err)
	}
	for _, tenant := range legacy {
		if s.projectHasTenantDatabase(project, tenant.databaseName) {
			continue
		}
		persisted, saveErr := s.saveTenantRegistry(ctx, tenant)
		if saveErr != nil {
			// A legacy runtime may not have a writable control-plane registry.
			// Keep its exact project-suffixed relationship working in memory.
			slog.Debug("persist migrated tenant relationship", "project", project, "tenant", tenant.ID, "error", saveErr)
			persisted = tenant
		}
		s.mergeProjectTenants(project, []tenantTarget{persisted})
	}
	return nil
}

func (s *Server) mergeProjectTenants(project string, tenants []tenantTarget) {
	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	if s.config.TenantDatabases == nil {
		s.config.TenantDatabases = map[string]string{}
	}
	for _, tenant := range tenants {
		if tenant.ProjectID != project || tenant.ID == "" {
			continue
		}
		key := tenantStoreKey(project, tenant.ID)
		for existingKey, existing := range s.tenants {
			if existingKey == key || existing.ProjectID != project {
				continue
			}
			sameRelationship := tenant.RelationshipID != "" && existing.RelationshipID == tenant.RelationshipID
			sameDatabase := tenant.databaseName != "" && existing.databaseName == tenant.databaseName
			if sameRelationship || (tenant.registered && sameDatabase) {
				delete(s.tenants, existingKey)
				delete(s.config.TenantDatabases, existingKey)
			}
		}
		if existing, ok := s.tenants[key]; ok && tenantTargetPriority(existing) > tenantTargetPriority(tenant) {
			continue
		}
		s.tenants[key] = tenant
		if explicitURL := s.explicitTenantDatabases[key]; explicitURL != "" {
			s.config.TenantDatabases[key] = explicitURL
		} else if tenant.databaseURL != "" {
			s.config.TenantDatabases[key] = tenant.databaseURL
		}
	}
}

func (s *Server) projectHasTenantDatabase(project string, databaseName string) bool {
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" {
		return false
	}
	s.projectMu.RLock()
	defer s.projectMu.RUnlock()
	for _, tenant := range s.tenants {
		if tenant.ProjectID == project && tenant.databaseName == databaseName {
			return true
		}
	}
	return false
}

func (s *Server) shouldHydrateProjectTenants(project string) bool {
	now := time.Now()
	s.tenantHydrationMu.Lock()
	defer s.tenantHydrationMu.Unlock()
	if last, ok := s.tenantHydrationAt[project]; ok && now.Sub(last) < projectTenantHydrationTTL {
		return false
	}
	s.tenantHydrationAt[project] = now
	return true
}

func (s *Server) invalidateProjectTenantHydration(project string) {
	project = strings.TrimSpace(project)
	if project == "" {
		return
	}
	s.tenantHydrationMu.Lock()
	delete(s.tenantHydrationAt, project)
	s.tenantHydrationMu.Unlock()
}

func (s *Server) discoverLegacyProjectTenantDatabases(ctx context.Context, project string) ([]tenantTarget, error) {
	db, err := openMaintenanceDB(s.config.PostgresURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	projectSuffix := tenantDatabaseProjectSuffix(project)
	projectSuffixPattern := strings.ReplaceAll(projectSuffix, "_", `\_`)
	projectDatabase := databaseNameFromURL(s.databaseURLForProject(project), "")
	rows, err := db.QueryContext(ctx, `
		SELECT datname
		FROM pg_database
		WHERE datistemplate = false AND datname LIKE $1 ESCAPE '\'
		ORDER BY datname
	`, `%\_`+projectSuffixPattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tenants []tenantTarget
	for rows.Next() {
		var databaseName string
		if err := rows.Scan(&databaseName); err != nil {
			return nil, err
		}
		if databaseName == projectDatabase {
			continue
		}
		alias, ok := legacyTenantDatabaseAlias(project, databaseName)
		if !ok || alias == "" {
			continue
		}
		tenantID := strings.ReplaceAll(alias, "_", "-")
		databaseURL, err := databaseURL(s.config.PostgresURL, databaseName)
		if err != nil {
			return nil, err
		}
		tenants = append(tenants, tenantTarget{
			ID:             tenantID,
			ProjectID:      project,
			Name:           tenantID,
			Database:       alias,
			Status:         "local",
			Description:    "Migrated legacy project tenant database.",
			Provisioned:    true,
			RuntimeCreated: true,
			databaseURL:    databaseURL,
			databaseName:   databaseName,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tenants, nil
}

func legacyTenantDatabaseAlias(project string, databaseName string) (string, bool) {
	databaseName = strings.TrimSpace(databaseName)
	projectSuffix := tenantDatabaseProjectSuffix(project)
	if databaseName == "" || projectSuffix == "" {
		return "", false
	}
	alias, ok := strings.CutSuffix(databaseName, "_"+projectSuffix)
	if !ok || alias == "" {
		return "", false
	}
	return alias, true
}

func shouldMigrateLegacyTenantRelationships(project string) bool {
	return strings.TrimSpace(project) != "" && !isUUIDProjectID(project)
}

func (s *Server) existingLocalDatabaseNames(ctx context.Context) map[string]bool {
	names, _ := s.existingLocalDatabaseNamesWithError(ctx)
	return names
}

func (s *Server) existingLocalDatabaseNamesWithError(ctx context.Context) (map[string]bool, error) {
	if strings.TrimSpace(s.config.PostgresURL) == "" {
		return nil, nil
	}
	maintenanceURL, err := databaseURL(s.config.PostgresURL, "postgres")
	if err != nil {
		return nil, err
	}
	store, err := s.tenantStores.Store(ctx, "__maintenance__", maintenanceURL)
	if err != nil {
		return nil, err
	}
	rows, err := store.DB.QueryContext(ctx, `
		SELECT datname
		FROM pg_database
		WHERE datistemplate = false
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

func persistedTenantRelationshipID(documentID string, databaseAlias string, domain string) string {
	documentID = strings.TrimSpace(documentID)
	databaseAlias = strings.TrimSpace(databaseAlias)
	domain = strings.TrimSpace(domain)
	if domain != "" && databaseAlias == domain && isLegacyConvexDocumentID(documentID) {
		return domain
	}
	if documentID != "" {
		return documentID
	}
	return domain
}

func isLegacyConvexDocumentID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'z') {
			return false
		}
	}
	return true
}

func tenantDatabaseNameForPersistedTenant(project string, tenantID string, databaseAlias string, domain string, existingDatabases map[string]bool) string {
	// Prefer an already-existing physical database (legacy slug names, prior
	// provision, or an explicit alias) so hydrate never invents a new UUID for
	// a tenant that already has data on disk.
	for _, candidate := range uniqueStrings([]string{databaseAlias, tenantID, domain}) {
		if existingDatabases[candidate] {
			return candidate
		}
		legacy := legacyTenantDatabaseNameWithAlias(project, tenantID, candidate)
		if existingDatabases[legacy] {
			return legacy
		}
	}
	// Brand-new tenant: opaque UUIDv6 physical name. Display name stays in
	// gonvex_runtime_tenants.name (and database_alias for a human slug).
	return tenantDatabaseNameWithAlias(project, tenantID, databaseAlias)
}

func isMissingTenantDatabaseError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database") && strings.Contains(message, "does not exist")
}

func ensureTenantLocalTables(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS members (
			user_id TEXT PRIMARY KEY,
			role TEXT NOT NULL DEFAULT 'member',
			permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS members_by_role ON members (role)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
