package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

var appAuthMembershipRoles = map[string]bool{
	"owner":  true,
	"admin":  true,
	"member": true,
	"viewer": true,
}

var errAppAuthInvitationRequired = errors.New("this app is invite-only; ask an administrator for a workspace invitation")
var errAppAuthOwnerRequired = errors.New("only a tenant owner can manage owner or admin access")

const appAuthInvitationTTL = 7 * 24 * time.Hour

func normalizeAppAuthMembershipRole(value string) (string, error) {
	role := strings.ToLower(strings.TrimSpace(value))
	if role == "" {
		role = "member"
	}
	if !appAuthMembershipRoles[role] {
		return "", fmt.Errorf("role must be owner, admin, member, or viewer")
	}
	return role, nil
}

func appAuthRoleRank(role string) int {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "owner":
		return 0
	case "admin":
		return 1
	case "member":
		return 2
	default:
		return 3
	}
}

// sanitizeAppAuthPermissions strips a caller-supplied "role" key so custom
// permission JSON can never shadow the role the tenant records.
func sanitizeAppAuthPermissions(permissions map[string]any) map[string]any {
	sanitized := map[string]any{}
	for key, value := range permissions {
		if key != "role" {
			sanitized[key] = value
		}
	}
	return sanitized
}

func marshalAppAuthPermissions(permissions map[string]any) ([]byte, error) {
	raw, err := json.Marshal(sanitizeAppAuthPermissions(permissions))
	if err != nil {
		return nil, fmt.Errorf("permissions must be a JSON object: %w", err)
	}
	return raw, nil
}

func distinctAppAuthIdentities(identities []string) []string {
	unique := make([]string, 0, len(identities))
	seen := map[string]bool{}
	for _, identity := range identities {
		identity = strings.TrimSpace(identity)
		if identity == "" || seen[identity] {
			continue
		}
		seen[identity] = true
		unique = append(unique, identity)
	}
	return unique
}

// tenantMemberDB opens the database that owns membership authority. Role,
// permission, and owner truth live in its members table; the control-plane
// registry only keeps accounts, tenant records, and pending invitations.
func (s *Server) tenantMemberDB(ctx context.Context, projectID string, tenantID string) (*sql.DB, error) {
	s.hydrateProjectTenantDatabases(ctx, projectID)
	databaseURL, err := s.ensureRuntimeTenantDatabase(ctx, projectID, tenantIDFromRequest(projectID, tenantID), s.databaseURLForTenant(projectID, tenantID))
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(databaseURL) == "" {
		return nil, fmt.Errorf("tenant database is unavailable")
	}
	store, err := s.tenantStores.Store(ctx, tenantStoreKey(projectID, tenantID), databaseURL)
	if err != nil {
		return nil, err
	}
	if store.DB == nil {
		return nil, fmt.Errorf("tenant database is unavailable")
	}
	if err := s.ensureTenantLocalSchema(ctx, databaseURL, store.DB); err != nil {
		return nil, err
	}
	return store.DB, nil
}

// ensureTenantLocalSchema installs the framework-owned membership and
// projection tables once per physical tenant database. Membership listeners,
// provisioning, and foreground authorization can all arrive concurrently;
// serializing the DDL prevents PostgreSQL catalog races such as "tuple
// concurrently updated" without placing correctness on a timing assumption.
func (s *Server) ensureTenantLocalSchema(ctx context.Context, databaseURL string, db *sql.DB) error {
	key := strings.TrimSpace(databaseURL)
	s.tenantLocalSchemaMu.Lock()
	ready := s.tenantLocalSchemaReady[key]
	s.tenantLocalSchemaMu.Unlock()
	if ready {
		return nil
	}
	_, err, _ := s.tenantLocalSchemaLoads.Do(key, func() (any, error) {
		s.tenantLocalSchemaMu.Lock()
		ready := s.tenantLocalSchemaReady[key]
		s.tenantLocalSchemaMu.Unlock()
		if ready {
			return nil, nil
		}
		if err := ensureTenantLocalTables(ctx, db); err != nil {
			return nil, err
		}
		s.tenantLocalSchemaMu.Lock()
		s.tenantLocalSchemaReady[key] = true
		s.tenantLocalSchemaMu.Unlock()
		return nil, nil
	})
	return err
}

// tenantMemberRecord is the tenant's own view of a membership. memberID is the
// tenant-local Member id and accountID is the one canonical Account behind it.
type tenantMemberRecord struct {
	memberID    string
	accountID   string
	role        string
	permissions []byte
}

// tenantMemberMatch matches a member by either identity a caller may hold. The
// account id is optional so callers that only know a member id can pass "".
const tenantMemberMatch = `(id = $1 OR account_id = $1
	OR ($2 <> '' AND (id = $2 OR account_id = $2)))`

// loadTenantMemberRecord reads the authoritative membership row. found is false
// when the tenant holds no active member for either identity.
func (s *Server) loadTenantMemberRecord(ctx context.Context, projectID string, tenantID string, memberID string, accountID string) (tenantMemberRecord, bool, error) {
	db, err := s.tenantMemberDB(ctx, projectID, tenantID)
	if err != nil {
		return tenantMemberRecord{}, false, err
	}
	record := tenantMemberRecord{}
	err = db.QueryRowContext(ctx, `SELECT id, account_id, role, permissions
		FROM members WHERE `+tenantMemberMatch+` AND status = 'active'`, memberID, accountID).Scan(
		&record.memberID, &record.accountID, &record.role, &record.permissions,
	)
	if err == sql.ErrNoRows {
		return tenantMemberRecord{}, false, nil
	}
	if err != nil {
		return tenantMemberRecord{}, false, err
	}
	return record, true, nil
}

// activateTenantMember commits the membership in the tenant database and
// nowhere else. account_tenant_index is derived from this commit by the tenant
// outbox and change feed, so there is no second write to keep in step.
func (s *Server) activateTenantMember(ctx context.Context, projectID string, tenantID string, memberID string, accountID string, displayName string, avatarURL string, role string, permissionsJSON []byte) error {
	db, err := s.tenantMemberDB(ctx, projectID, tenantID)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `INSERT INTO members (
		id, account_id, status, display_name, avatar_url, role, permissions, updated_at
	)
		VALUES ($1, $2, 'active', $3, $4, $5, $6, now())
		ON CONFLICT (id) DO UPDATE SET
			account_id = EXCLUDED.account_id,
			status = 'active',
			display_name = EXCLUDED.display_name,
			avatar_url = EXCLUDED.avatar_url,
			role = EXCLUDED.role,
			permissions = EXCLUDED.permissions,
			membership_revision = members.membership_revision + 1,
			updated_at = now()`,
		memberID, accountID, displayName, avatarURL, role, string(permissionsJSON))
	return err
}

// revokeTenantMember withdraws access in the tenant database. That commit is
// the security decision; the outbox row it writes carries the directory update
// afterwards and can never reinstate the member if it lags.
func (s *Server) revokeTenantMember(ctx context.Context, projectID string, tenantID string, memberID string, accountID string) (tenantMemberRecord, bool, error) {
	db, err := s.tenantMemberDB(ctx, projectID, tenantID)
	if err != nil {
		return tenantMemberRecord{}, false, err
	}
	record := tenantMemberRecord{}
	err = db.QueryRowContext(ctx, `UPDATE members
		SET status = 'revoked', membership_revision = membership_revision + 1, updated_at = now()
		WHERE `+tenantMemberMatch+`
		RETURNING id, account_id, role`,
		memberID, accountID).Scan(&record.memberID, &record.accountID, &record.role)
	if err == sql.ErrNoRows {
		return tenantMemberRecord{}, false, nil
	}
	if err != nil {
		return tenantMemberRecord{}, false, err
	}
	return record, true, nil
}

// tenantHasOtherActiveOwner reports whether the tenant keeps an owner besides
// excludedMemberID whose account is still enabled. Owners are counted from the
// tenant's own rows; the registry only answers which accounts are disabled.
func (s *Server) tenantHasOtherActiveOwner(ctx context.Context, projectID string, tenantID string, excludedMemberID string) (bool, error) {
	db, err := s.tenantMemberDB(ctx, projectID, tenantID)
	if err != nil {
		return false, err
	}
	rows, err := db.QueryContext(ctx, `SELECT id, account_id
		FROM members
		WHERE role = 'owner' AND status = 'active'
		AND id <> $1 AND account_id <> $1`, excludedMemberID)
	if err != nil {
		return false, err
	}
	owners := []tenantMemberRecord{}
	for rows.Next() {
		owner := tenantMemberRecord{}
		if err := rows.Scan(&owner.memberID, &owner.accountID); err != nil {
			rows.Close()
			return false, err
		}
		owners = append(owners, owner)
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if len(owners) == 0 {
		return false, nil
	}
	registry, err := s.pooledProjectRegistry(ctx)
	if err != nil || registry == nil {
		return false, fmt.Errorf("project auth store is unavailable")
	}
	for _, owner := range owners {
		var enabled bool
		if err := registry.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM accounts
			WHERE id = $2 AND auth_realm_id = $1 AND disabled_at IS NULL
		)`, projectID, owner.accountID).Scan(&enabled); err != nil {
			return false, err
		}
		if enabled {
			return true, nil
		}
	}
	return false, nil
}

func lockAppAuthMembershipChanges(ctx context.Context, db *sql.DB, projectID string) (*sql.Conn, error) {
	connection, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := connection.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, "gonvex-auth-memberships:"+projectID); err != nil {
		connection.Close()
		return nil, err
	}
	return connection, nil
}

func unlockAppAuthMembershipChanges(connection *sql.Conn, projectID string) {
	if connection == nil {
		return
	}
	_, _ = connection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, "gonvex-auth-memberships:"+projectID)
	_ = connection.Close()
}

func singleAppAuthTenantRelationshipID(projectID string) string {
	return "auth-single:" + strings.TrimSpace(projectID)
}

// ensureSingleAppAuthTenant gives a single-database project one central,
// project-shaped tenant record. Open signup, invite-only projects, and explicit
// role assignments then all record a real member row in the project database,
// through the same machinery multi-tenant projects use.
func (s *Server) ensureSingleAppAuthTenant(ctx context.Context, projectID string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var mode string
	var name string
	var databaseURL string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(database_mode, ''), 'single'), name, database_url
		FROM gonvex_runtime_projects WHERE id = $1`, projectID).Scan(&mode, &name, &databaseURL); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("project %q was not found", projectID)
		}
		return err
	}
	// Keep routing aligned with the persisted project target. Runtime-created
	// projects may not yet have been copied into ProjectDatabases.
	s.projectMu.Lock()
	if s.config.ProjectDatabases == nil {
		s.config.ProjectDatabases = map[string]string{}
	}
	if strings.TrimSpace(databaseURL) != "" {
		s.config.ProjectDatabases[projectID] = databaseURL
	}
	s.projectMu.Unlock()
	if normalizedDatabaseModeWithDefault(mode) != "single" {
		return nil
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_tenants (
		relationship_id, project_id, tenant_id, name, status, description, provisioned, runtime_created, updated_at
	) VALUES ($1, $2, $2, $3, 'active', 'Single-database app membership scope.', TRUE, FALSE, now())
	ON CONFLICT (project_id, tenant_id) DO UPDATE SET name = EXCLUDED.name, updated_at = now()`,
		singleAppAuthTenantRelationshipID(projectID), projectID, name)
	return err
}

// appAuthAccountID normalizes the canonical account id accepted by public
// routes. Identity v2 has no project-user alias to resolve.
func appAuthAccountID(_ context.Context, _ *sql.DB, _ string, accountID string) string {
	return strings.TrimSpace(accountID)
}

// appAuthTenantCandidates lists the tenants that may hold a member row for an
// account. account_tenant_index is a directory here and nothing else: it can
// point at a tenant database but never decides whether the account may enter
// one, so every caller re-reads the tenant before acting on the result.
func (s *Server) appAuthTenantCandidates(ctx context.Context, db *sql.DB, projectID string, accountID string) ([]appAuthTenant, error) {
	var mode string
	var projectName string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(database_mode, ''), 'single'), name
		FROM gonvex_runtime_projects WHERE id = $1`, projectID).Scan(&mode, &projectName); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("project %q was not found", projectID)
		}
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT i.tenant_id, t.name
		FROM account_tenant_index i
		JOIN gonvex_runtime_tenants t ON t.tenant_id = i.tenant_id
		WHERE t.project_id = $1 AND i.account_id = $2 AND i.status = 'active'
		ORDER BY lower(t.name), i.tenant_id`, projectID, accountID)
	if err != nil {
		return nil, err
	}
	candidates := []appAuthTenant{}
	seen := map[string]bool{}
	for rows.Next() {
		var tenant appAuthTenant
		if err := rows.Scan(&tenant.ID, &tenant.Name); err != nil {
			rows.Close()
			return nil, err
		}
		if seen[tenant.ID] {
			continue
		}
		seen[tenant.ID] = true
		candidates = append(candidates, tenant)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	// The index is an asynchronous directory projection. Enumerate the
	// project's registered tenants as well, then confirm each candidate against
	// its authoritative tenant-local Member row below. This makes newly-created
	// workspaces visible immediately without granting access from the directory.
	allTenants, err := appAuthAllTenantCandidates(ctx, db, projectID)
	if err != nil {
		return nil, err
	}
	for _, tenant := range allTenants {
		if !seen[tenant.ID] {
			seen[tenant.ID] = true
			candidates = append(candidates, tenant)
		}
	}
	if normalizedDatabaseModeWithDefault(mode) == "single" && !seen[projectID] {
		// A single-database project has exactly one membership scope, so probing
		// it directly is cheaper than waiting for the directory projection.
		candidates = append(candidates, appAuthTenant{ID: projectID, Name: projectName})
	}
	return candidates, nil
}

// appAuthAllTenantCandidates enumerates the complete project tenant registry.
// Destructive account operations use this instead of account_tenant_index:
// the directory is intentionally eventual and therefore cannot prove that an
// account has no tenant-local memberships left to revoke.
func appAuthAllTenantCandidates(ctx context.Context, db *sql.DB, projectID string) ([]appAuthTenant, error) {
	rows, err := db.QueryContext(ctx, `SELECT tenant_id, name
		FROM gonvex_runtime_tenants
		WHERE project_id = $1
		ORDER BY lower(name), tenant_id`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	candidates := []appAuthTenant{}
	for rows.Next() {
		var tenant appAuthTenant
		if err := rows.Scan(&tenant.ID, &tenant.Name); err != nil {
			return nil, err
		}
		candidates = append(candidates, tenant)
	}
	return candidates, rows.Err()
}

func (s *Server) listAppAuthTenants(ctx context.Context, projectID string, userID string) ([]appAuthTenant, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, fmt.Errorf("project auth store is unavailable")
	}
	accountID := appAuthAccountID(ctx, db, projectID, userID)
	candidates, err := s.appAuthTenantCandidates(ctx, db, projectID, accountID)
	if err != nil {
		return nil, err
	}
	tenants := make([]appAuthTenant, 0, len(candidates))
	for _, candidate := range candidates {
		member, err := s.loadTenantMember(ctx, projectID, candidate.ID, accountID)
		if err != nil {
			// A candidate the tenant does not confirm is simply not a membership.
			continue
		}
		candidate.Role = member.Role
		candidate.Permissions = member.Permissions
		tenants = append(tenants, candidate)
	}
	sort.SliceStable(tenants, func(left int, right int) bool {
		leftRank, rightRank := appAuthRoleRank(tenants[left].Role), appAuthRoleRank(tenants[right].Role)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		leftName, rightName := strings.ToLower(tenants[left].Name), strings.ToLower(tenants[right].Name)
		if leftName != rightName {
			return leftName < rightName
		}
		return tenants[left].ID < tenants[right].ID
	})
	return tenants, nil
}

func selectAppAuthTenant(tenants []appAuthTenant, requested string) (appAuthTenant, error) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		if len(tenants) == 0 {
			return appAuthTenant{}, fmt.Errorf("this account does not have access to a tenant")
		}
		return tenants[0], nil
	}
	for _, tenant := range tenants {
		if tenant.ID == requested {
			return tenant, nil
		}
	}
	return appAuthTenant{}, fmt.Errorf("app session does not grant access to tenant %q", requested)
}

// resolveAppAuthTenant picks the tenant a session should act in. A tenant named
// explicitly is confirmed against its own member row even when the directory
// has not caught up yet, so entering a tenant never waits on a projection.
func (s *Server) resolveAppAuthTenant(ctx context.Context, projectID string, accountID string, tenants []appAuthTenant, requested string) (appAuthTenant, error) {
	tenant, err := selectAppAuthTenant(tenants, requested)
	requested = strings.TrimSpace(requested)
	if err == nil || requested == "" {
		return tenant, err
	}
	db, registryErr := s.pooledProjectRegistry(ctx)
	if registryErr != nil || db == nil {
		return appAuthTenant{}, err
	}
	var name string
	if lookupErr := db.QueryRowContext(ctx, `SELECT name FROM gonvex_runtime_tenants
		WHERE project_id = $1 AND tenant_id = $2`, projectID, requested).Scan(&name); lookupErr != nil {
		return appAuthTenant{}, err
	}
	member, memberErr := s.loadTenantMember(ctx, projectID, requested, accountID)
	if memberErr != nil {
		return appAuthTenant{}, err
	}
	return appAuthTenant{ID: requested, Name: name, Role: member.Role, Permissions: member.Permissions}, nil
}

func (s *Server) ensureAppAuthMemberships(ctx context.Context, projectID string, user appAuthAccount) error {
	mode, err := s.projectDatabaseMode(ctx, projectID)
	if err != nil {
		return err
	}
	if user.EmailVerified && user.Email != "" {
		if err := s.claimAppAuthInvitations(ctx, projectID, user); err != nil {
			return err
		}
	}
	configuration, err := s.appAuthProviderConfiguration(ctx, projectID)
	if err != nil {
		return err
	}
	tenants, err := s.listAppAuthTenants(ctx, projectID, user.ID)
	if err != nil {
		return err
	}
	if configuration.SignupMode == appAuthSignupInviteOnly {
		if len(tenants) == 0 {
			return errAppAuthInvitationRequired
		}
		return nil
	}
	if len(tenants) > 0 {
		return nil
	}
	if mode == "single" {
		if err := s.ensureSingleAppAuthTenant(ctx, projectID); err != nil {
			return err
		}
		return s.upsertAppAuthMembership(ctx, projectID, projectID, user.ID, "member", map[string]any{})
	}
	_, err = s.ensurePersonalAppAuthTenant(ctx, projectID, user)
	return err
}

type appAuthPendingInvitation struct {
	tenantID        string
	role            string
	permissions     map[string]any
	permissionsJSON []byte
	invitedBy       string
	expiresAt       time.Time
}

func (s *Server) claimAppAuthInvitations(ctx context.Context, projectID string, user appAuthAccount) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `DELETE FROM gonvex_auth_membership_invitations
		WHERE project_id = $1 AND email = $2 AND expires_at <= now()`, projectID, strings.ToLower(user.Email)); err != nil {
		return err
	}
	// Atomically take the invitations before creating their tenant members. A
	// concurrent cancellation or replacement has a definitive winner instead of
	// returning success while an already-read invitation is still claimed.
	rows, err := db.QueryContext(ctx, `DELETE FROM gonvex_auth_membership_invitations
		WHERE project_id = $1 AND email = $2 AND expires_at > now()
		RETURNING tenant_id, role, permissions, invited_by, expires_at`, projectID, strings.ToLower(user.Email))
	if err != nil {
		return err
	}
	invitations := []appAuthPendingInvitation{}
	for rows.Next() {
		var item appAuthPendingInvitation
		if err := rows.Scan(&item.tenantID, &item.role, &item.permissionsJSON, &item.invitedBy, &item.expiresAt); err != nil {
			rows.Close()
			return err
		}
		item.permissions = map[string]any{}
		if len(item.permissionsJSON) > 0 {
			if err := json.Unmarshal(item.permissionsJSON, &item.permissions); err != nil {
				rows.Close()
				return err
			}
		}
		invitations = append(invitations, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	// Accepting an invitation creates the tenant member directly. Nothing about
	// the claim is recorded in the control plane beyond removing the invitation.
	for index, item := range invitations {
		if err := s.upsertAppAuthMembership(ctx, projectID, item.tenantID, user.ID, item.role, item.permissions); err != nil {
			restoreAppAuthInvitations(context.Background(), db, projectID, strings.ToLower(user.Email), invitations[index:])
			return err
		}
	}
	return rows.Err()
}

func restoreAppAuthInvitations(ctx context.Context, db *sql.DB, projectID string, email string, invitations []appAuthPendingInvitation) {
	for _, item := range invitations {
		_, _ = db.ExecContext(ctx, `INSERT INTO gonvex_auth_membership_invitations (
			project_id, tenant_id, email, role, permissions, invited_by, expires_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, now()) ON CONFLICT DO NOTHING`,
			projectID, item.tenantID, email, item.role, string(item.permissionsJSON), item.invitedBy, item.expiresAt)
	}
}

func (s *Server) ensurePersonalAppAuthTenant(ctx context.Context, projectID string, user appAuthAccount) (appAuthTenant, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return appAuthTenant{}, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	connection, err := db.Conn(ctx)
	if err != nil {
		return appAuthTenant{}, err
	}
	defer connection.Close()
	lockKey := "gonvex-auth-personal:" + projectID + ":" + user.ID
	if _, err := connection.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, lockKey); err != nil {
		return appAuthTenant{}, err
	}
	defer connection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, lockKey)

	tenants, err := s.listAppAuthTenants(ctx, projectID, user.ID)
	if err != nil {
		return appAuthTenant{}, err
	}
	if len(tenants) > 0 {
		return tenants[0], nil
	}
	name := strings.TrimSpace(user.Name)
	if name == "" {
		name = strings.Split(user.Email, "@")[0]
	}
	if name == "" {
		name = "My"
	}
	return s.createControlTenant(ctx, &wsConn{
		server:  s,
		project: projectID,
		user:    &gonvex.Account{ID: user.ID, Email: user.Email, Name: user.Name, AvatarURL: user.Picture},
	}, name+"'s workspace", "personal-workspace:"+user.ID)
}

func (s *Server) createAppAuthTenant(ctx context.Context, projectID string, userID string, name string) (appAuthTenant, error) {
	mode, err := s.projectDatabaseMode(ctx, projectID)
	if err != nil {
		return appAuthTenant{}, err
	}
	if mode != "multiTenant" {
		return appAuthTenant{}, fmt.Errorf("project is not configured for tenant databases")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return appAuthTenant{}, fmt.Errorf("tenant name is required")
	}
	if s.config.PostgresURL == "" {
		return appAuthTenant{}, fmt.Errorf("DATABASE_URL is not configured")
	}
	registry, err := s.openProjectRegistry(ctx)
	if err != nil || registry == nil {
		return appAuthTenant{}, fmt.Errorf("project auth store is unavailable")
	}
	defer registry.Close()
	connection, err := registry.Conn(ctx)
	if err != nil {
		return appAuthTenant{}, err
	}
	defer connection.Close()
	lockKey := "gonvex-auth-tenant-create:" + projectID
	if _, err := connection.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, lockKey); err != nil {
		return appAuthTenant{}, err
	}
	defer connection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, lockKey)
	tenantID, err := generateRelationshipID()
	if err != nil {
		return appAuthTenant{}, err
	}
	s.hydrateProjects()
	s.hydrateProjectTenantDatabases(ctx, projectID)

	s.projectMu.Lock()
	databaseAlias := slug(name)
	if databaseAlias == "" {
		databaseAlias = "workspace"
	}
	baseAlias := databaseAlias
	for suffix := 2; s.tenantDatabaseAliasTakenLocked(projectID, databaseAlias, ""); suffix++ {
		databaseAlias = fmt.Sprintf("%s-%d", baseAlias, suffix)
	}
	databaseName, err := generateTenantPhysicalDatabaseName()
	if err != nil {
		s.projectMu.Unlock()
		return appAuthTenant{}, err
	}
	s.projectMu.Unlock()

	tenantDatabaseURL, err := createProjectDatabase(ctx, s.config.PostgresURL, databaseName)
	if err != nil {
		return appAuthTenant{}, err
	}
	cleanupDatabase := true
	defer func() {
		if cleanupDatabase {
			_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		}
	}()
	if err := s.provisionTenantDatabaseWithSync(ctx, projectID, tenantDatabaseURL); err != nil {
		return appAuthTenant{}, err
	}
	target := tenantTarget{
		RelationshipID: tenantID,
		ID:             tenantID, ProjectID: projectID, Name: name, Database: databaseAlias,
		Status: "active", Description: "Auth-created tenant database.", Provisioned: true, RuntimeCreated: true,
		databaseURL: tenantDatabaseURL, databaseName: databaseName,
	}
	registered, err := s.saveTenantRegistry(ctx, target)
	if err != nil || !registered.registered {
		if err == nil {
			err = fmt.Errorf("tenant relationship registry is unavailable")
		}
		return appAuthTenant{}, err
	}
	s.mergeProjectTenants(projectID, []tenantTarget{registered})
	s.invalidateProjectTenantHydration(projectID)
	role := ""
	if userID != "" {
		if err := s.upsertAppAuthMembership(ctx, projectID, tenantID, userID, "owner", map[string]any{}); err != nil {
			_ = s.deleteTenantRegistry(context.Background(), projectID, registered)
			s.projectMu.Lock()
			delete(s.tenants, tenantStoreKey(projectID, tenantID))
			delete(s.config.TenantDatabases, tenantStoreKey(projectID, tenantID))
			s.projectMu.Unlock()
			return appAuthTenant{}, err
		}
		role = "owner"
	}
	cleanupDatabase = false
	s.registerProjectCrons(projectID)
	return appAuthTenant{ID: tenantID, Name: name, Role: role, Permissions: map[string]any{}}, nil
}

func (s *Server) upsertAppAuthMembership(ctx context.Context, projectID string, tenantID string, userID string, role string, permissions map[string]any) error {
	return s.upsertAppAuthMembershipAs(ctx, projectID, tenantID, userID, role, permissions, "")
}

// upsertAppAuthMembershipAs grants or changes a membership. The registry is
// consulted for the tenant record and the account it belongs to, but the change
// itself commits in the tenant database alone.
func (s *Server) upsertAppAuthMembershipAs(ctx context.Context, projectID string, tenantID string, userID string, role string, permissions map[string]any, actorRole string) error {
	role, err := normalizeAppAuthMembershipRole(role)
	if err != nil {
		return err
	}
	permissionsJSON, err := marshalAppAuthPermissions(permissions)
	if err != nil {
		return err
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	membershipLock, err := lockAppAuthMembershipChanges(ctx, db, projectID)
	if err != nil {
		return err
	}
	defer unlockAppAuthMembershipChanges(membershipLock, projectID)
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM gonvex_runtime_tenants WHERE project_id = $1 AND tenant_id = $2
	)`, projectID, tenantID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("tenant %q is not registered for project %q", tenantID, projectID)
	}
	var accountID string
	var displayName string
	var avatarURL string
	if err := db.QueryRowContext(ctx, `SELECT id, name, avatar_url
		FROM accounts WHERE auth_realm_id = $1 AND id = $2 AND disabled_at IS NULL`, projectID, userID).Scan(
		&accountID, &displayName, &avatarURL,
	); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("account %q is unavailable", userID)
		}
		return err
	}
	previous, hadMembership, err := s.loadTenantMemberRecord(ctx, projectID, tenantID, userID, accountID)
	if err != nil {
		return err
	}
	if actorRole == "admin" && (role == "owner" || role == "admin" || (hadMembership && (previous.role == "owner" || previous.role == "admin"))) {
		return errAppAuthOwnerRequired
	}
	if hadMembership && previous.role == "owner" && role != "owner" {
		hasOtherActiveOwner, err := s.tenantHasOtherActiveOwner(ctx, projectID, tenantID, previous.memberID)
		if err != nil {
			return err
		}
		if !hasOtherActiveOwner {
			return fmt.Errorf("a tenant must keep at least one owner")
		}
	}
	memberID := accountID
	if hadMembership {
		memberID = previous.memberID
	} else if generated, idErr := randomID("member"); idErr == nil {
		memberID = generated
	} else {
		return idErr
	}
	if err := s.activateTenantMember(ctx, projectID, tenantID, memberID, accountID, displayName, avatarURL, role, permissionsJSON); err != nil {
		return err
	}
	s.startMembershipProjection(func() {
		s.projectTenantMemberDirectory(projectID, tenantID)
	})
	if hadMembership && (previous.role != role || !equalAppAuthPermissions(previous.permissions, permissionsJSON)) {
		s.revokeAppAuthAccountSessions(ctx, projectID, accountID)
	}
	return nil
}

func equalAppAuthPermissions(left []byte, right []byte) bool {
	var leftValue, rightValue map[string]any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

// removeAppAuthMembership withdraws a membership. Only the tenant commit
// matters for access; live credentials are cut as soon as it succeeds and the
// directory catches up afterwards.
func (s *Server) removeAppAuthMembership(ctx context.Context, projectID string, tenantID string, userID string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	membershipLock, err := lockAppAuthMembershipChanges(ctx, db, projectID)
	if err != nil {
		return err
	}
	defer unlockAppAuthMembershipChanges(membershipLock, projectID)
	accountID := appAuthAccountID(ctx, db, projectID, userID)
	target, hasMembership, err := s.loadTenantMemberRecord(ctx, projectID, tenantID, userID, accountID)
	if err != nil {
		return err
	}
	if !hasMembership {
		return nil
	}
	if target.role == "owner" {
		hasOtherActiveOwner, err := s.tenantHasOtherActiveOwner(ctx, projectID, tenantID, target.memberID)
		if err != nil {
			return err
		}
		if !hasOtherActiveOwner {
			return fmt.Errorf("a tenant must keep at least one owner")
		}
	}
	revoked, found, err := s.revokeTenantMember(ctx, projectID, tenantID, userID, accountID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	s.revokeAppAuthAccountSessions(ctx, projectID, revoked.accountID)
	s.startMembershipProjection(func() {
		s.projectTenantMemberDirectory(projectID, tenantID)
	})
	return nil
}

// revokeAppAuthAccountSessions invalidates stored credentials and live sockets
// for a canonical Account and any tenant-local Member ids supplied by callers.
func (s *Server) revokeAppAuthAccountSessions(ctx context.Context, projectID string, identities ...string) {
	unique := distinctAppAuthIdentities(identities)
	if len(unique) == 0 {
		return
	}
	db, err := s.openProjectRegistry(ctx)
	if err == nil && db != nil {
		for _, identity := range unique {
			_, _ = db.ExecContext(ctx, `UPDATE gonvex_auth_sessions SET revoked_at = COALESCE(revoked_at, now())
				WHERE project_id = $1 AND account_id = $2`, projectID, identity)
			_, _ = db.ExecContext(ctx, `UPDATE gonvex_auth_refresh_tokens SET revoked_at = COALESCE(revoked_at, now())
				WHERE project_id = $1 AND account_id = $2`, projectID, identity)
		}
		db.Close()
	}
	for _, identity := range unique {
		s.revokeAppAuthConnections(projectID, identity)
	}
}

func (s *Server) inviteAppAuthMember(ctx context.Context, projectID string, tenantID string, email string, role string, permissions map[string]any, invitedBy string) error {
	return s.inviteAppAuthMemberAs(ctx, projectID, tenantID, email, role, permissions, invitedBy, "")
}

func (s *Server) inviteAppAuthMemberAs(ctx context.Context, projectID string, tenantID string, email string, role string, permissions map[string]any, invitedBy string, actorRole string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" || !strings.Contains(email, "@") {
		return fmt.Errorf("a valid email is required")
	}
	role, err := normalizeAppAuthMembershipRole(role)
	if err != nil {
		return err
	}
	if actorRole == "admin" && (role == "owner" || role == "admin") {
		return errAppAuthOwnerRequired
	}
	sanitizedPermissions := sanitizeAppAuthPermissions(permissions)
	raw, err := json.Marshal(sanitizedPermissions)
	if err != nil {
		return err
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var accountID string
	err = db.QueryRowContext(ctx, `SELECT id FROM accounts
		WHERE auth_realm_id = $1 AND lower(email) = $2 AND disabled_at IS NULL
		ORDER BY created_at LIMIT 1`, projectID, email).Scan(&accountID)
	if err == nil {
		return s.upsertAppAuthMembershipAs(ctx, projectID, tenantID, accountID, role, sanitizedPermissions, actorRole)
	}
	if err != sql.ErrNoRows {
		return err
	}
	membershipLock, err := lockAppAuthMembershipChanges(ctx, db, projectID)
	if err != nil {
		return err
	}
	defer unlockAppAuthMembershipChanges(membershipLock, projectID)
	if actorRole == "admin" {
		var pendingRole string
		pendingErr := db.QueryRowContext(ctx, `SELECT role FROM gonvex_auth_membership_invitations
			WHERE project_id = $1 AND tenant_id = $2 AND email = $3 AND expires_at > now()`,
			projectID, tenantID, email).Scan(&pendingRole)
		if pendingErr != nil && pendingErr != sql.ErrNoRows {
			return pendingErr
		}
		if pendingErr == nil && (pendingRole == "owner" || pendingRole == "admin") {
			return errAppAuthOwnerRequired
		}
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_auth_membership_invitations (
		project_id, tenant_id, email, role, permissions, invited_by, expires_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
	ON CONFLICT (project_id, tenant_id, email) DO UPDATE SET
		role = EXCLUDED.role, permissions = EXCLUDED.permissions, invited_by = EXCLUDED.invited_by,
		expires_at = EXCLUDED.expires_at, updated_at = now()`,
		projectID, tenantID, email, role, string(raw), invitedBy, time.Now().Add(appAuthInvitationTTL).UTC())
	return err
}

func (s *Server) deleteAppAuthInvitation(ctx context.Context, projectID string, tenantID string, email string) error {
	return s.deleteAppAuthInvitationAs(ctx, projectID, tenantID, email, "")
}

func (s *Server) deleteAppAuthInvitationAs(ctx context.Context, projectID string, tenantID string, email string, actorRole string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return fmt.Errorf("invitation email is required")
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	membershipLock, err := lockAppAuthMembershipChanges(ctx, db, projectID)
	if err != nil {
		return err
	}
	defer unlockAppAuthMembershipChanges(membershipLock, projectID)
	if actorRole == "admin" {
		var pendingRole string
		pendingErr := db.QueryRowContext(ctx, `SELECT role FROM gonvex_auth_membership_invitations
			WHERE project_id = $1 AND tenant_id = $2 AND email = $3 AND expires_at > now()`,
			projectID, tenantID, email).Scan(&pendingRole)
		if pendingErr != nil && pendingErr != sql.ErrNoRows {
			return pendingErr
		}
		if pendingErr == nil && (pendingRole == "owner" || pendingRole == "admin") {
			return errAppAuthOwnerRequired
		}
	}
	result, err := db.ExecContext(ctx, `DELETE FROM gonvex_auth_membership_invitations
		WHERE project_id = $1 AND tenant_id = $2 AND email = $3`, projectID, tenantID, email)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("invitation not found")
	}
	return nil
}

func (s *Server) handleAppAuthTenants(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	identity, err := s.loadAppSessionIdentity(r.Context(), token)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	switch r.Method {
	case http.MethodGet:
		tenants, err := s.listAppAuthTenants(r.Context(), identity.ProjectID, identity.Account.ID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"project": identity.ProjectID, "tenants": tenants})
	case http.MethodPost:
		if !s.allowAppAuthRequest(w, r, "tenant-create", 10, time.Hour) {
			return
		}
		configuration, err := s.appAuthProviderConfiguration(r.Context(), identity.ProjectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if configuration.SignupMode == appAuthSignupInviteOnly {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "this project allows tenant creation only through its control plane"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		defer r.Body.Close()
		var payload struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant request"})
			return
		}
		tenant, err := s.createAppAuthTenant(r.Context(), identity.ProjectID, identity.Account.ID, payload.Name)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"tenant": tenant})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func decodeAppAuthMembershipRequest(w http.ResponseWriter, r *http.Request) (string, string, map[string]any, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	defer r.Body.Close()
	var payload struct {
		Email       string         `json:"email"`
		Role        string         `json:"role"`
		Permissions map[string]any `json:"permissions"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid membership request"})
		return "", "", nil, false
	}
	return payload.Email, payload.Role, payload.Permissions, true
}

func (s *Server) handleAppAuthMe(w http.ResponseWriter, r *http.Request) {
	identity, err := s.loadAppSessionIdentity(r.Context(), bearerToken(r))
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	tenants, err := s.listAppAuthTenants(r.Context(), identity.ProjectID, identity.Account.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	activeTenantID := ""
	var member *gonvex.Member
	requestedTenant := tenantID(r)
	if len(tenants) > 0 {
		active, err := s.resolveAppAuthTenant(r.Context(), identity.ProjectID, identity.Account.canonicalID(), tenants, requestedTenant)
		if err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		activeTenantID = active.ID
		member, err = s.loadTenantMember(r.Context(), identity.ProjectID, activeTenantID, identity.Account.canonicalID())
		if err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"project": identity.ProjectID,
		"account": gonvex.Account{ID: identity.Account.canonicalID(), Email: identity.Account.Email, Name: identity.Account.Name, AvatarURL: identity.Account.Picture},
		"member":  member,
		"tenants": tenants, "activeTenantId": activeTenantID,
	})
}

func (s *Server) handleAppAuthTenantMembers(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.PathValue("tenant"))
	session, _, err := s.validateAppSession(r.Context(), "", bearerToken(r), tenantID)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	if session.Tenant.Role != "owner" && session.Tenant.Role != "admin" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant owner or admin access is required"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		members, invitations, err := s.listAppAuthTenantMembers(r.Context(), session.ProjectID, tenantID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"members": members, "invitations": invitations})
	case http.MethodPost:
		if !s.allowAppAuthRequest(w, r, "tenant-invite", 60, time.Minute) {
			return
		}
		email, role, permissions, ok := decodeAppAuthMembershipRequest(w, r)
		if !ok {
			return
		}
		if role == "owner" && session.Tenant.Role != "owner" {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "only an owner can grant owner access"})
			return
		}
		if err := s.inviteAppAuthMemberAs(r.Context(), session.ProjectID, tenantID, email, role, permissions, session.Account.ID, session.Tenant.Role); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, errAppAuthOwnerRequired) {
				status = http.StatusForbidden
			}
			writeJSON(w, status, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleDeleteAppAuthTenantMember(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "tenant-member-delete", 60, time.Minute) {
		return
	}
	tenantID := strings.TrimSpace(r.PathValue("tenant"))
	memberID := strings.TrimSpace(r.PathValue("member"))
	session, _, err := s.validateAppSession(r.Context(), "", bearerToken(r), tenantID)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	if session.Tenant.Role != "owner" && session.Tenant.Role != "admin" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant owner or admin access is required"})
		return
	}
	targetRole, err := s.appAuthMembershipRole(r.Context(), session.ProjectID, tenantID, memberID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if session.Tenant.Role != "owner" && (targetRole == "owner" || targetRole == "admin") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "only an owner can remove an owner or admin"})
		return
	}
	if err := s.removeAppAuthMembership(r.Context(), session.ProjectID, tenantID, memberID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDeleteAppAuthTenantInvitation(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "tenant-invitation-delete", 60, time.Minute) {
		return
	}
	tenantID := strings.TrimSpace(r.PathValue("tenant"))
	email := strings.TrimSpace(r.PathValue("email"))
	session, _, err := s.validateAppSession(r.Context(), "", bearerToken(r), tenantID)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	if session.Tenant.Role != "owner" && session.Tenant.Role != "admin" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant owner or admin access is required"})
		return
	}
	if err := s.deleteAppAuthInvitationAs(r.Context(), session.ProjectID, tenantID, email, session.Tenant.Role); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errAppAuthOwnerRequired) {
			status = http.StatusForbidden
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// appAuthMembershipRole reads the role from the tenant that owns it, so an
// authorization decision never rests on a control-plane copy.
func (s *Server) appAuthMembershipRole(ctx context.Context, projectID string, tenantID string, userID string) (string, error) {
	member, found, err := s.loadTenantMemberRecord(ctx, projectID, tenantID, userID, "")
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("tenant member not found")
	}
	return member.role, nil
}

type appAuthMemberView struct {
	MemberID    string         `json:"memberId"`
	Email       string         `json:"email"`
	Name        string         `json:"name"`
	Role        string         `json:"role"`
	Permissions map[string]any `json:"permissions,omitempty"`
}

type appAuthInvitationView struct {
	Email       string         `json:"email"`
	Role        string         `json:"role"`
	Permissions map[string]any `json:"permissions,omitempty"`
	ExpiresAt   time.Time      `json:"expiresAt"`
}

// listTenantMemberViews reads the tenant's own member rows and decorates them
// with registry contact details. Role and permissions always come from the
// tenant; only email and display name are looked up centrally.
func (s *Server) listTenantMemberViews(ctx context.Context, registry *sql.DB, projectID string, tenantID string) ([]appAuthMemberView, error) {
	tenantDB, err := s.tenantMemberDB(ctx, projectID, tenantID)
	if err != nil {
		return nil, err
	}
	rows, err := tenantDB.QueryContext(ctx, `SELECT id, account_id,
		display_name, role, permissions
		FROM members WHERE status = 'active'`)
	if err != nil {
		return nil, err
	}
	accountIDs := map[string]string{}
	members := []appAuthMemberView{}
	for rows.Next() {
		var member appAuthMemberView
		var accountID string
		var raw []byte
		if err := rows.Scan(&member.MemberID, &accountID, &member.Name, &member.Role, &raw); err != nil {
			rows.Close()
			return nil, err
		}
		member.Permissions = map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &member.Permissions); err != nil {
				rows.Close()
				return nil, err
			}
		}
		delete(member.Permissions, "role")
		accountIDs[member.MemberID] = accountID
		members = append(members, member)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return members, nil
	}
	contacts, err := appAuthMemberContacts(ctx, registry, projectID, members, accountIDs)
	if err != nil {
		return nil, err
	}
	for index := range members {
		contact, ok := contacts[members[index].MemberID]
		if !ok {
			contact, ok = contacts[accountIDs[members[index].MemberID]]
		}
		if ok {
			members[index].Email = contact.Email
			if strings.TrimSpace(contact.Name) != "" {
				members[index].Name = contact.Name
			}
		}
	}
	sort.SliceStable(members, func(left int, right int) bool {
		leftRank, rightRank := appAuthRoleRank(members[left].Role), appAuthRoleRank(members[right].Role)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		leftEmail, rightEmail := strings.ToLower(members[left].Email), strings.ToLower(members[right].Email)
		if leftEmail != rightEmail {
			return leftEmail < rightEmail
		}
		return members[left].MemberID < members[right].MemberID
	})
	return members, nil
}

// appAuthMemberContacts looks up the registry profile for tenant members, keyed
// by both the member id and the account id so either match resolves.
func appAuthMemberContacts(ctx context.Context, registry *sql.DB, projectID string, members []appAuthMemberView, accountIDs map[string]string) (map[string]appAuthMemberView, error) {
	arguments := []any{projectID}
	placeholders := make([]string, 0, len(members)*2)
	for _, member := range members {
		arguments = append(arguments, member.MemberID)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(arguments)))
		if accountID := accountIDs[member.MemberID]; accountID != "" && accountID != member.MemberID {
			arguments = append(arguments, accountID)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(arguments)))
		}
	}
	identities := strings.Join(placeholders, ", ")
	rows, err := registry.QueryContext(ctx, `SELECT id, id, email, name
		FROM accounts
		WHERE auth_realm_id = $1 AND id IN (`+identities+`)`, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	contacts := map[string]appAuthMemberView{}
	for rows.Next() {
		var userID, accountID string
		var contact appAuthMemberView
		if err := rows.Scan(&userID, &accountID, &contact.Email, &contact.Name); err != nil {
			return nil, err
		}
		contacts[userID] = contact
		if _, taken := contacts[accountID]; !taken {
			contacts[accountID] = contact
		}
	}
	return contacts, rows.Err()
}

func (s *Server) listAppAuthTenantMembers(ctx context.Context, projectID string, tenantID string) ([]appAuthMemberView, []appAuthInvitationView, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, nil, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	members, err := s.listTenantMemberViews(ctx, db, projectID, tenantID)
	if err != nil {
		return nil, nil, err
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM gonvex_auth_membership_invitations
		WHERE project_id = $1 AND tenant_id = $2 AND expires_at <= now()`, projectID, tenantID); err != nil {
		return nil, nil, err
	}
	inviteRows, err := db.QueryContext(ctx, `SELECT email, role, permissions, expires_at
		FROM gonvex_auth_membership_invitations
		WHERE project_id = $1 AND tenant_id = $2 AND expires_at > now()
		  AND revoked_at IS NULL AND accepted_at IS NULL
		ORDER BY lower(email)`, projectID, tenantID)
	if err != nil {
		return nil, nil, err
	}
	defer inviteRows.Close()
	invitations := []appAuthInvitationView{}
	for inviteRows.Next() {
		var invitation appAuthInvitationView
		var raw []byte
		if err := inviteRows.Scan(&invitation.Email, &invitation.Role, &raw, &invitation.ExpiresAt); err != nil {
			return nil, nil, err
		}
		invitation.Permissions = map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &invitation.Permissions); err != nil {
				return nil, nil, err
			}
		}
		invitations = append(invitations, invitation)
	}
	return members, invitations, inviteRows.Err()
}

func (s *Server) handleProjectAuthMemberships(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if !s.authorizeProjectAuthRequest(w, r, projectID, r.Method != http.MethodGet) {
		return
	}
	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant"))
	if tenantID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant is required"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		members, invitations, err := s.listAppAuthTenantMembers(r.Context(), projectID, tenantID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"members": members, "invitations": invitations})
	case http.MethodPut:
		email, role, permissions, ok := decodeAppAuthMembershipRequest(w, r)
		if !ok {
			return
		}
		if err := s.inviteAppAuthMember(r.Context(), projectID, tenantID, email, role, permissions, "project-admin"); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		memberID := strings.TrimSpace(r.URL.Query().Get("member"))
		invitationEmail := strings.TrimSpace(r.URL.Query().Get("email"))
		if memberID == "" && invitationEmail == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "member or invitation email is required"})
			return
		}
		var err error
		if memberID != "" {
			err = s.removeAppAuthMembership(r.Context(), projectID, tenantID, memberID)
		} else {
			err = s.deleteAppAuthInvitation(r.Context(), projectID, tenantID, invitationEmail)
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleProjectAuthTenants(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if !s.authorizeProjectAuthRequest(w, r, projectID, r.Method != http.MethodGet) {
		return
	}
	if r.Method == http.MethodGet {
		db, err := s.openProjectRegistry(r.Context())
		if err != nil || db == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "project auth store is unavailable"})
			return
		}
		defer db.Close()
		// memberCount is a directory statistic taken from the projection, not an
		// access decision, so it may briefly trail a tenant's own member rows.
		rows, err := db.QueryContext(r.Context(), `SELECT t.tenant_id, t.name, (
				SELECT count(*) FROM account_tenant_index i
				WHERE i.tenant_id = t.tenant_id AND i.status = 'active'
			)
			FROM gonvex_runtime_tenants t
			WHERE t.project_id = $1 ORDER BY lower(t.name), t.tenant_id`, projectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		defer rows.Close()
		tenants := []map[string]any{}
		for rows.Next() {
			var id, name string
			var memberCount int
			if err := rows.Scan(&id, &name, &memberCount); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			tenants = append(tenants, map[string]any{"id": id, "name": name, "memberCount": memberCount})
		}
		writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	defer r.Body.Close()
	var payload struct {
		Name       string `json:"name"`
		OwnerEmail string `json:"ownerEmail"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant request"})
		return
	}
	tenant, err := s.createAppAuthTenant(r.Context(), projectID, "", payload.Name)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if strings.TrimSpace(payload.OwnerEmail) != "" {
		if err := s.inviteAppAuthMember(r.Context(), projectID, tenant.ID, payload.OwnerEmail, "owner", map[string]any{}, "project-admin"); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error(), "tenant": tenant})
			return
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"tenant": tenant})
}

func (s *Server) handleProjectAuthAccount(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	accountIDParam := strings.TrimSpace(r.PathValue("account"))
	if projectID == "" || accountIDParam == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project and account are required"})
		return
	}
	if !s.authorizeProjectAuthRequest(w, r, projectID, true) {
		return
	}
	db, err := s.openProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "project auth store is unavailable"})
		return
	}
	defer db.Close()
	membershipLock, err := lockAppAuthMembershipChanges(r.Context(), db, projectID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer unlockAppAuthMembershipChanges(membershipLock, projectID)
	switch r.Method {
	case http.MethodPatch:
		r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
		defer r.Body.Close()
		var payload struct {
			Disabled bool `json:"disabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid account request"})
			return
		}
		accountID := appAuthAccountID(r.Context(), db, projectID, accountIDParam)
		if payload.Disabled {
			if err := s.ensureAppAuthAccountCanBeDeactivated(r.Context(), db, projectID, accountIDParam); err != nil {
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
				return
			}
		}
		result, err := db.ExecContext(r.Context(), `UPDATE accounts
			SET disabled_at = CASE WHEN $3 THEN COALESCE(disabled_at, now()) ELSE NULL END, updated_at = now()
			WHERE auth_realm_id = $1 AND id = $2`, projectID, accountIDParam, payload.Disabled)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
			return
		}
		if payload.Disabled {
			s.revokeAppAuthAccountSessions(r.Context(), projectID, accountIDParam, accountID)
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "disabled": payload.Disabled})
	case http.MethodDelete:
		if err := s.ensureAppAuthAccountCanBeDeactivated(r.Context(), db, projectID, accountIDParam); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		accountID := appAuthAccountID(r.Context(), db, projectID, accountIDParam)
		// Cut live access first so nothing that follows has to rely on how fresh
		// the directory projection happens to be.
		s.revokeAppAuthAccountSessions(r.Context(), projectID, accountIDParam, accountID)
		candidates, err := appAuthAllTenantCandidates(r.Context(), db, projectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		// The global account outlives any single tenant, so it may only be removed
		// once every discoverable tenant has revoked its own member row.
		for _, candidate := range candidates {
			revoked, found, err := s.revokeTenantMember(r.Context(), projectID, candidate.ID, accountIDParam, accountID)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "account was not deleted because tenant membership revocation failed: " + err.Error()})
				return
			}
			if !found {
				continue
			}
			s.revokeAppAuthAccountSessions(r.Context(), projectID, revoked.memberID, revoked.accountID)
			s.startMembershipProjection(func() {
				s.projectTenantMemberDirectory(projectID, candidate.ID)
			})
		}
		result, err := db.ExecContext(r.Context(), `DELETE FROM accounts WHERE auth_realm_id = $1 AND id = $2`, projectID, accountIDParam)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
			return
		}
		s.revokeAppAuthConnections(projectID, accountIDParam)
		s.revokeAppAuthConnections(projectID, accountID)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ensureAppAuthAccountCanBeDeactivated refuses to strip an account that is the
// last remaining owner of a tenant. Ownership is read from each tenant rather
// than from the directory, and an unreachable tenant blocks the change instead
// of being silently treated as ownerless.
func (s *Server) ensureAppAuthAccountCanBeDeactivated(ctx context.Context, db *sql.DB, projectID string, userID string) error {
	accountID := appAuthAccountID(ctx, db, projectID, userID)
	candidates, err := appAuthAllTenantCandidates(ctx, db, projectID)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		member, found, err := s.loadTenantMemberRecord(ctx, projectID, candidate.ID, userID, accountID)
		if err != nil {
			return err
		}
		if !found || member.role != "owner" {
			continue
		}
		hasOtherActiveOwner, err := s.tenantHasOtherActiveOwner(ctx, projectID, candidate.ID, member.memberID)
		if err != nil {
			return err
		}
		if !hasOtherActiveOwner {
			return fmt.Errorf("transfer ownership of %q before disabling or deleting this account", candidate.Name)
		}
	}
	return nil
}
