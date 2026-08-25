package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// handleProjectAuthProvider is the project-key bootstrap surface for trusted
// operators. Browser applications use generated Control Plane references;
// this endpoint exists so a Firebase-only project can configure its first
// identity provider before any project-admin Account can sign in.
func (s *Server) handleProjectAuthProvider(response http.ResponseWriter, request *http.Request) {
	project := strings.TrimSpace(request.PathValue("project"))
	provider := strings.ToLower(strings.TrimSpace(request.PathValue("provider")))
	if project == "" || (provider != "firebase" && provider != authModeExternalOIDC) {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "project and a supported external provider are required"})
		return
	}
	if !s.authorizeProjectAuthRequest(response, request, project, request.Method != http.MethodGet) {
		return
	}
	db, err := s.pooledProjectRegistry(request.Context())
	if err != nil || db == nil {
		writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "Control Plane store is unavailable"})
		return
	}
	if request.Method == http.MethodGet {
		var mode string
		var enabled, hasAdminCredentials bool
		var signupMode, issuer, audience, jwksURL, firebaseProjectID, firebaseTenantID string
		err := db.QueryRowContext(request.Context(), `SELECT COALESCE(NULLIF(project.auth_mode,''),'gonvex-native'),provider.enabled,
			provider.signup_mode,provider.issuer,provider.audience,provider.jwks_url,provider.firebase_project_id,
			provider.firebase_tenant_id,provider.admin_credentials_encrypted IS NOT NULL
			FROM gonvex_runtime_projects project JOIN gonvex_auth_providers provider
			ON provider.project_id=project.id AND provider.provider=$2 WHERE project.id=$1`, project, provider).Scan(
			&mode, &enabled, &signupMode, &issuer, &audience, &jwksURL, &firebaseProjectID, &firebaseTenantID, &hasAdminCredentials,
		)
		if err != nil {
			writeJSON(response, http.StatusNotFound, map[string]string{"error": "provider is not configured"})
			return
		}
		writeJSON(response, http.StatusOK, map[string]any{
			"provider": provider, "authMode": mode, "enabled": enabled, "signupMode": signupMode,
			"issuer": issuer, "audience": audience, "jwksUrl": jwksURL,
			"firebaseProjectId": firebaseProjectID, "firebaseTenantId": firebaseTenantID,
			"hasAdminCredentials": hasAdminCredentials,
		})
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 1<<20)
	defer request.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(io.LimitReader(request.Body, 1<<20)).Decode(&payload); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "invalid provider configuration"})
		return
	}
	payload["provider"] = provider
	raw, err := json.Marshal(payload)
	if err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "invalid provider configuration"})
		return
	}
	tx, err := db.BeginTx(request.Context(), nil)
	if err != nil {
		writeJSON(response, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer tx.Rollback()
	result, err := s.executeControlReducerWithStore(request.Context(), tx, &wsConn{server: s, project: project}, "control.auth.realms.configure", raw, "")
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(response, http.StatusOK, result)
}
