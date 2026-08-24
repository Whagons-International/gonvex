package server

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
)

// handleInternalE2EMember is deliberately admin-key-only and is not mounted
// under the browser Control Plane protocol. It makes CLI provisioning
// resumable without exposing database routing or arbitrary identity choices to
// production clients.
func (s *Server) handleInternalE2EMember(w http.ResponseWriter, r *http.Request) {
	if !s.acceptsAdminKey(syncKey(r)) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "runtime admin key is required"})
		return
	}
	var args struct {
		ProjectID string `json:"projectId"`
		TenantID  string `json:"tenantId"`
		Email     string `json:"email"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&args); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid E2E provisioning payload"})
		return
	}
	args.ProjectID = strings.TrimSpace(args.ProjectID)
	args.TenantID = strings.TrimSpace(args.TenantID)
	args.Email = normalizeDashboardEmail(args.Email)
	if args.ProjectID == "" || args.TenantID == "" || args.Email == "" {
		writeJSON(w, 400, map[string]string{"error": "projectId, tenantId, and email are required"})
		return
	}
	db, err := s.pooledProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, 500, map[string]string{"error": "Control Plane unavailable"})
		return
	}
	var accountID string
	err = db.QueryRowContext(r.Context(), `SELECT id FROM accounts WHERE auth_realm_id=$1 AND lower(email)=lower($2) AND disabled_at IS NULL`, args.ProjectID, args.Email).Scan(&accountID)
	if err == sql.ErrNoRows {
		writeJSON(w, 404, map[string]string{"error": "test actor account not found"})
		return
	}
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if err := s.upsertAppAuthMembershipAs(r.Context(), args.ProjectID, args.TenantID, accountID, "admin", map[string]any{"e2e": true}, "owner"); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	member, err := s.loadTenantMember(r.Context(), args.ProjectID, args.TenantID, accountID)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"projectId": args.ProjectID, "tenantId": args.TenantID, "accountId": accountID, "memberId": member.ID})
}
