package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// authenticateControlSocket validates the global Account without selecting or
// opening a tenant database. Control Plane handlers still enforce their own
// account/project-admin authorization before touching Control Plane state.
func (s *Server) authenticateControlSocket(ctx context.Context, requestedProjectID string, token string) (*gonvex.Account, string, error) {
	if !strings.HasPrefix(strings.TrimSpace(token), "gvx_session_") {
		return nil, "", fmt.Errorf("a Gonvex app session is required")
	}
	session, err := s.loadAppSessionIdentity(ctx, token)
	if err != nil {
		return nil, "", err
	}
	if requestedProjectID != "" && requestedProjectID != session.ProjectID {
		return nil, "", fmt.Errorf("app session was issued for a different project")
	}
	// Control Plane calls do not require a project-level application database.
	// Tenant admission opens the selected tenant database separately and remains
	// authoritative there.
	if err := s.requireControlProject(ctx, session.ProjectID); err != nil {
		return nil, "", err
	}
	return &gonvex.Account{
		ID: session.Account.ID, Email: session.Account.Email, Name: session.Account.Name, AvatarURL: session.Account.Picture,
	}, session.ProjectID, nil
}

func (s *Server) authenticateImpersonationSocket(ctx context.Context, requestedProjectID, token, connectionID string) (*gonvex.Account, map[string]any, string, string, string, string, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, nil, "", "", "", "", fmt.Errorf("Control Plane store is unavailable")
	}
	var grantID, projectID, actorID, accountID, tenantID string
	err = db.QueryRowContext(ctx, `UPDATE gonvex_impersonation_grants SET used_at=now(),used_connection_id=$2
		WHERE token_hash=$1 AND used_at IS NULL AND revoked_at IS NULL AND expires_at>now()
		RETURNING id,project_id,actor_account_id,target_account_id,tenant_id`, sha256Hex(token), connectionID).Scan(&grantID, &projectID, &actorID, &accountID, &tenantID)
	if err == sql.ErrNoRows {
		return nil, nil, "", "", "", "", fmt.Errorf("impersonation grant is invalid, expired, revoked, or already used")
	}
	if err != nil {
		return nil, nil, "", "", "", "", err
	}
	if requestedProjectID != "" && requestedProjectID != projectID {
		return nil, nil, "", "", "", "", fmt.Errorf("impersonation grant was issued for a different project")
	}
	account, member, err := s.revalidateImpersonation(ctx, projectID, tenantID, grantID, connectionID)
	if err != nil {
		return nil, nil, "", "", "", "", err
	}
	return account, member.Permissions, projectID, tenantID, grantID, actorID, nil
}

func (s *Server) revalidateImpersonation(ctx context.Context, projectID, tenantID, grantID, connectionID string) (*gonvex.Account, *gonvex.Member, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, nil, fmt.Errorf("Control Plane store is unavailable")
	}
	var account gonvex.Account
	err = db.QueryRowContext(ctx, `SELECT account.id,account.email,account.name,account.avatar_url
		FROM gonvex_impersonation_grants impersonation JOIN accounts account ON account.id=impersonation.target_account_id
		WHERE impersonation.id=$1 AND impersonation.project_id=$2 AND impersonation.tenant_id=$3 AND impersonation.used_connection_id=$4
		AND impersonation.revoked_at IS NULL AND impersonation.expires_at>now() AND account.disabled_at IS NULL`, grantID, projectID, tenantID, connectionID).Scan(&account.ID, &account.Email, &account.Name, &account.AvatarURL)
	if err == sql.ErrNoRows {
		return nil, nil, fmt.Errorf("impersonation grant is no longer valid")
	}
	if err != nil {
		return nil, nil, err
	}
	member, err := s.loadTenantMember(ctx, projectID, tenantID, account.ID)
	if err != nil {
		return nil, nil, err
	}
	return &account, member, nil
}

func (s *Server) authenticateSocket(ctx context.Context, projectID string, currentTenantID string, token string, requestedTenantID string) (*gonvex.Account, map[string]any, string, string, error) {
	if err := s.requireProjectDatabase(projectID); err != nil {
		return nil, nil, "", "", err
	}
	if strings.HasPrefix(strings.TrimSpace(token), "gvx_session_") {
		session, tenantID, err := s.validateAppSession(ctx, projectID, token, requestedTenantID)
		if err != nil {
			return nil, nil, "", "", err
		}
		member, err := s.loadTenantMember(ctx, session.ProjectID, tenantID, session.Account.ID)
		if err != nil {
			return nil, nil, "", "", err
		}
		return &gonvex.Account{ID: session.Account.ID, Email: session.Account.Email, Name: session.Account.Name, AvatarURL: session.Account.Picture}, member.Permissions, session.ProjectID, tenantID, nil
	}
	if strings.TrimSpace(token) != "" {
		return nil, nil, "", "", fmt.Errorf("only Gonvex app sessions are accepted")
	}
	return nil, nil, "", "", fmt.Errorf("a Gonvex app session is required")
}

func (s *Server) loadTenantPermissions(ctx context.Context, projectID string, tenantID string, userID string) (map[string]any, error) {
	member, err := s.loadTenantMember(ctx, projectID, tenantID, userID)
	if err != nil {
		return nil, err
	}
	return member.Permissions, nil
}

// loadTenantMember is the final authorization check for entering a tenant.
// Control-plane directory/index rows can locate a database, but only an active
// member row in that tenant database grants access.
func (s *Server) loadTenantMember(ctx context.Context, projectID string, tenantID string, accountID string) (*gonvex.Member, error) {
	db, err := s.tenantMemberDB(ctx, projectID, tenantID)
	if err != nil {
		return nil, err
	}

	member := &gonvex.Member{}
	var rawPermissions []byte
	if err := db.QueryRowContext(ctx, `
		SELECT id, account_id,
			status, display_name, avatar_url, role, permissions
		FROM members
		WHERE account_id = $1 AND status = 'active'
	`, accountID).Scan(&member.ID, &member.AccountID, &member.Status, &member.DisplayName, &member.AvatarURL, &member.Role, &rawPermissions); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("active tenant member for account %q not found", accountID)
		}
		return nil, err
	}

	permissions := map[string]any{}
	if len(rawPermissions) > 0 {
		var parsed map[string]any
		if err := json.Unmarshal(rawPermissions, &parsed); err != nil {
			return nil, err
		}
		for key, value := range parsed {
			permissions[key] = value
		}
	}
	permissions["role"] = member.Role
	member.Permissions = permissions
	return member, nil
}
