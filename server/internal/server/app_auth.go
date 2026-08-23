package server

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

const (
	googleProvider           = "google"
	authTransactionTTL       = 10 * time.Minute
	authCodeTTL              = 5 * time.Minute
	appSessionTTL            = 15 * time.Minute
	appRefreshSessionTTL     = 30 * 24 * time.Hour
	appRefreshReuseGrace     = 5 * time.Second
	defaultGoogleKeyCacheTTL = time.Hour
	maxAppAuthRedirectURIs   = 32
	maxAppAuthRedirectLength = 2048
	appAuthSignupPersonal    = "personal"
	appAuthSignupInviteOnly  = "inviteOnly"
)

var pkceValuePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43,128}$`)

type appAuthAccount struct {
	ID             string    `json:"id"`
	AccountID      string    `json:"accountId"`
	Email          string    `json:"email,omitempty"`
	EmailVerified  bool      `json:"emailVerified"`
	Name           string    `json:"name,omitempty"`
	Picture        string    `json:"picture,omitempty"`
	Provider       string    `json:"provider"`
	Disabled       bool      `json:"disabled"`
	CreatedAt      time.Time `json:"createdAt,omitempty"`
	LastSignedInAt time.Time `json:"lastSignedInAt,omitempty"`
}

func (u appAuthAccount) canonicalID() string {
	return u.ID
}

type appAuthTenant struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Role        string         `json:"role"`
	Permissions map[string]any `json:"permissions,omitempty"`
}

type appAuthProviderConfiguration struct {
	Enabled    bool
	SignupMode string
}

type appAuthRequirementCacheEntry struct {
	Enabled   bool
	ExpiresAt time.Time
}

type appAuthRequirementLookup struct {
	done       chan struct{}
	generation uint64
	enabled    bool
	err        error
	retry      bool
}

const (
	appAuthRequirementCacheTTL      = 5 * time.Second
	appAuthRequirementLookupTimeout = 5 * time.Second
)

type authTransaction struct {
	ProjectID         string
	RedirectURI       string
	AppState          string
	CodeChallenge     string
	Nonce             string
	GoogleRedirectURI string
}

type googleIdentity struct {
	Subject       string
	Email         string
	EmailVerified bool
	Name          string
	Picture       string
}

type googleKeyCache struct {
	mu        sync.Mutex
	keys      map[string]*rsa.PublicKey
	expiresAt time.Time
}

// handleProjectGoogleAuth is the project-owner control-plane endpoint used by
// `gonvex auth add google`. The Google client secret belongs to the Gonvex
// installation, never to an individual app project.
func (s *Server) handleProjectGoogleAuth(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	manage := r.Method != http.MethodGet
	if !s.authorizeProjectAuthRequest(w, r, projectID, manage) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		redirectURIs, enabled, err := s.googleAuthConfiguration(r.Context(), projectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		providerConfig, err := s.appAuthProviderConfiguration(r.Context(), projectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		tenantCount, membershipCount, invitationCount := s.appAuthProjectCounts(r.Context(), projectID)
		databaseMode, _ := s.projectDatabaseMode(r.Context(), projectID)
		writeJSON(w, http.StatusOK, map[string]any{
			"provider":          googleProvider,
			"enabled":           enabled,
			"signupMode":        providerConfig.SignupMode,
			"redirectUris":      redirectURIs,
			"runtimeConfigured": s.googleAuthBrokerReady(),
			"ready":             enabled && s.googleAuthBrokerReady(),
			"issues":            s.googleAuthReadinessIssues(),
			"brokerCallbackUrl": s.configuredGoogleCallbackURL(),
			"tenantCount":       tenantCount, "membershipCount": membershipCount, "invitationCount": invitationCount,
			"databaseMode": databaseMode,
		})
	case http.MethodPut:
		if _, err := s.projectDatabaseMode(r.Context(), projectID); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		defer r.Body.Close()
		var payload struct {
			RedirectURI string `json:"redirectUri"`
			SignupMode  string `json:"signupMode"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid Google auth configuration"})
			return
		}
		redirectURI, err := normalizeAppRedirectURI(payload.RedirectURI)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		signupMode, err := normalizeAppAuthSignupMode(payload.SignupMode)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := s.enableGoogleAuth(r.Context(), projectID, redirectURI, signupMode); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		redirectURIs, _, err := s.googleAuthConfiguration(r.Context(), projectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"provider":          googleProvider,
			"enabled":           true,
			"signupMode":        signupMode,
			"redirectUris":      redirectURIs,
			"runtimeConfigured": s.googleAuthBrokerReady(),
			"ready":             s.googleAuthBrokerReady(),
			"issues":            s.googleAuthReadinessIssues(),
			"brokerCallbackUrl": s.configuredGoogleCallbackURL(),
		})
	case http.MethodDelete:
		if rawRedirectURI := strings.TrimSpace(r.URL.Query().Get("redirect_uri")); rawRedirectURI != "" {
			redirectURI, err := normalizeAppRedirectURI(rawRedirectURI)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			if err := s.deleteGoogleAuthRedirectURI(r.Context(), projectID, redirectURI); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			redirectURIs, enabled, err := s.googleAuthConfiguration(r.Context(), projectID)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"provider": googleProvider, "enabled": enabled, "redirectUris": redirectURIs,
			})
			return
		}
		if err := s.disableGoogleAuth(r.Context(), projectID); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"provider": googleProvider, "enabled": false})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) deleteGoogleAuthRedirectURI(ctx context.Context, projectID string, redirectURI string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `DELETE FROM gonvex_auth_redirect_uris
		WHERE project_id = $1 AND provider = $2 AND redirect_uri = $3`, projectID, googleProvider, redirectURI)
	return err
}

func (s *Server) appAuthProjectCounts(ctx context.Context, projectID string) (int, int, int) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return 0, 0, 0
	}
	defer db.Close()
	var tenantCount, membershipCount, invitationCount int
	// membershipCount is a directory statistic read from the projection. It is
	// never consulted to decide access, so trailing a tenant commit is harmless.
	_ = db.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM gonvex_runtime_tenants WHERE project_id = $1),
		(SELECT count(*) FROM account_tenant_index i
			JOIN gonvex_runtime_tenants t ON t.tenant_id = i.tenant_id
			WHERE t.project_id = $1 AND i.status = 'active'),
		(SELECT count(*) FROM gonvex_auth_membership_invitations WHERE project_id = $1 AND expires_at > now())`, projectID).Scan(
		&tenantCount, &membershipCount, &invitationCount,
	)
	return tenantCount, membershipCount, invitationCount
}

func (s *Server) handleProjectAuthAccounts(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if !s.authorizeProjectAuthRequest(w, r, projectID, false) {
		return
	}
	db, err := s.openProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "auth account store is unavailable"})
		return
	}
	defer db.Close()
	rows, err := db.QueryContext(r.Context(), `SELECT a.id, a.id, a.email,
		COALESCE(identity.verified_email, FALSE), a.name, a.avatar_url,
		COALESCE(identity.provider, ''), a.disabled_at IS NOT NULL, a.created_at, a.updated_at
		FROM accounts a
		LEFT JOIN LATERAL (
			SELECT provider, verified_email FROM account_identities
			WHERE account_id = a.id ORDER BY updated_at DESC LIMIT 1
		) identity ON TRUE
		WHERE a.auth_realm_id = $1 ORDER BY a.created_at DESC`, projectID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()
	accounts := []appAuthAccount{}
	for rows.Next() {
		var account appAuthAccount
		if err := rows.Scan(&account.ID, &account.AccountID, &account.Email, &account.EmailVerified, &account.Name, &account.Picture, &account.Provider, &account.Disabled, &account.CreatedAt, &account.LastSignedInAt); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		accounts = append(accounts, account)
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"accounts": accounts})
}

func (s *Server) authorizeProjectAuthRequest(w http.ResponseWriter, r *http.Request, projectID string, manage bool) bool {
	if s.acceptsProjectEnvKey(projectID, syncKey(r)) {
		return true
	}
	actor, ok := s.projectEnvDashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in or project key is required"})
		return false
	}
	permission := permissionProjectsRead
	if manage {
		permission = permissionProjectsUpdate
	}
	if !actor.hasAccountPermission(permission) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "personal access token does not grant the required permission", "permission": permission})
		return false
	}
	if manage && !s.canManageProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return false
	}
	if !manage && !s.canAccessProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project access is required"})
		return false
	}
	return true
}

func (s *Server) projectDatabaseMode(ctx context.Context, projectID string) (string, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return "", fmt.Errorf("project registry is unavailable")
	}
	var mode string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(database_mode, ''), 'single') FROM gonvex_runtime_projects WHERE id = $1`, projectID).Scan(&mode); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("project %q was not found", projectID)
		}
		return "", err
	}
	return normalizedDatabaseModeWithDefault(mode), nil
}

func normalizeAppAuthSignupMode(value string) (string, error) {
	switch strings.TrimSpace(value) {
	case "", appAuthSignupPersonal:
		return appAuthSignupPersonal, nil
	case appAuthSignupInviteOnly, "invite-only", "invited":
		return appAuthSignupInviteOnly, nil
	default:
		return "", fmt.Errorf("signupMode must be personal or inviteOnly")
	}
}

func normalizeAppRedirectURI(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) > maxAppAuthRedirectLength {
		return "", fmt.Errorf("redirect URI must be at most %d bytes", maxAppAuthRedirectLength)
	}
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" {
		return "", fmt.Errorf("redirect URI must be an absolute http or https URL")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return "", fmt.Errorf("redirect URI cannot contain user info or a fragment")
	}
	hostname := strings.ToLower(parsed.Hostname())
	local := hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1"
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && local) {
		return "", fmt.Errorf("redirect URI must use https (http is allowed only for localhost)")
	}
	return parsed.String(), nil
}

func (s *Server) enableGoogleAuth(ctx context.Context, projectID string, redirectURI string, signupMode string) error {
	var err error
	signupMode, err = normalizeAppAuthSignupMode(signupMode)
	if err != nil {
		return err
	}
	if err := s.ensureSingleAppAuthTenant(ctx, projectID); err != nil {
		return err
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "gonvex-auth-redirects:"+projectID); err != nil {
		return err
	}
	previouslyEnabled := false
	if err := tx.QueryRowContext(ctx, `SELECT enabled FROM gonvex_auth_providers
		WHERE project_id = $1 AND provider = $2`, projectID, googleProvider).Scan(&previouslyEnabled); err != nil && err != sql.ErrNoRows {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO gonvex_auth_providers (project_id, provider, enabled, signup_mode, updated_at)
		VALUES ($1, $2, TRUE, $3, now())
		ON CONFLICT (project_id, provider) DO UPDATE SET enabled = TRUE, signup_mode = EXCLUDED.signup_mode, updated_at = now()`, projectID, googleProvider, signupMode); err != nil {
		return err
	}
	var redirectCount int
	var redirectExists bool
	if err := tx.QueryRowContext(ctx, `SELECT count(*), EXISTS (
		SELECT 1 FROM gonvex_auth_redirect_uris WHERE project_id = $1 AND provider = $2 AND redirect_uri = $3
	) FROM gonvex_auth_redirect_uris WHERE project_id = $1 AND provider = $2`,
		projectID, googleProvider, redirectURI).Scan(&redirectCount, &redirectExists); err != nil {
		return err
	}
	if !redirectExists && redirectCount >= maxAppAuthRedirectURIs {
		return fmt.Errorf("a project can register at most %d Google callback URLs", maxAppAuthRedirectURIs)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO gonvex_auth_redirect_uris (project_id, provider, redirect_uri)
		VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, projectID, googleProvider, redirectURI); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.invalidateAppAuthRequirement(projectID)
	if !previouslyEnabled {
		s.enforceNativeAppAuthConnections(projectID)
	}
	return nil
}

func (s *Server) appAuthProviderConfiguration(ctx context.Context, projectID string) (appAuthProviderConfiguration, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return appAuthProviderConfiguration{}, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	configuration := appAuthProviderConfiguration{SignupMode: appAuthSignupPersonal}
	err = db.QueryRowContext(ctx, `SELECT enabled, COALESCE(NULLIF(signup_mode, ''), $3)
		FROM gonvex_auth_providers WHERE project_id = $1 AND provider = $2`, projectID, googleProvider, appAuthSignupPersonal).Scan(
		&configuration.Enabled, &configuration.SignupMode,
	)
	if err == sql.ErrNoRows {
		return configuration, nil
	}
	return configuration, err
}

func (s *Server) disableGoogleAuth(ctx context.Context, projectID string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `UPDATE gonvex_auth_providers SET enabled = FALSE, updated_at = now()
		WHERE project_id = $1 AND provider = $2`, projectID, googleProvider)
	if err == nil {
		s.invalidateAppAuthRequirement(projectID)
	}
	return err
}

func (s *Server) invalidateAppAuthRequirement(projectID string) {
	projectID = strings.TrimSpace(projectID)
	s.appAuthConfigMu.Lock()
	delete(s.appAuthRequirements, projectID)
	if s.appAuthVersions == nil {
		s.appAuthVersions = map[string]uint64{}
	}
	s.appAuthVersions[projectID]++
	s.appAuthConfigMu.Unlock()
}

func (s *Server) nativeAppAuthEnabled(ctx context.Context, projectID string) (bool, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return false, nil
	}

	for {
		now := time.Now()
		s.appAuthConfigMu.Lock()
		cached, ok := s.appAuthRequirements[projectID]
		if ok && now.Before(cached.ExpiresAt) {
			s.appAuthConfigMu.Unlock()
			return cached.Enabled, nil
		}
		if lookup := s.appAuthLookups[projectID]; lookup != nil {
			done := lookup.done
			s.appAuthConfigMu.Unlock()
			select {
			case <-done:
			case <-ctx.Done():
				return false, ctx.Err()
			}
			if lookup.retry {
				continue
			}
			return lookup.enabled, lookup.err
		}
		if s.appAuthLookups == nil {
			s.appAuthLookups = map[string]*appAuthRequirementLookup{}
		}
		lookup := &appAuthRequirementLookup{
			done:       make(chan struct{}),
			generation: s.appAuthVersions[projectID],
		}
		s.appAuthLookups[projectID] = lookup
		s.appAuthConfigMu.Unlock()

		// Subscription contexts may be cancelled while an invalidation fan-out is
		// already in progress. Auth policy is process-scoped, so one stale caller
		// must not poison every live connection waiting on the same lookup.
		lookupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), appAuthRequirementLookupTimeout)
		enabled, cacheable, err := s.loadNativeAppAuthRequirement(lookupCtx, projectID)
		cancel()

		s.appAuthConfigMu.Lock()
		lookup.enabled = enabled
		lookup.err = err
		lookup.retry = lookup.generation != s.appAuthVersions[projectID]
		if err == nil && cacheable && !lookup.retry {
			s.appAuthRequirements[projectID] = appAuthRequirementCacheEntry{
				Enabled:   enabled,
				ExpiresAt: time.Now().Add(appAuthRequirementCacheTTL),
			}
		}
		delete(s.appAuthLookups, projectID)
		close(lookup.done)
		s.appAuthConfigMu.Unlock()
		if lookup.retry {
			continue
		}
		return enabled, err
	}
}

func (s *Server) loadNativeAppAuthRequirement(ctx context.Context, projectID string) (bool, bool, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return false, false, fmt.Errorf("project auth store is unavailable")
	}
	var projectExists, enabled bool
	err = db.QueryRowContext(ctx, `SELECT
		EXISTS (SELECT 1 FROM gonvex_runtime_projects WHERE id = $1),
		EXISTS (SELECT 1 FROM gonvex_auth_providers
			WHERE project_id = $1 AND provider = $2 AND enabled = TRUE)`, projectID, googleProvider).Scan(
		&projectExists, &enabled,
	)
	if err != nil {
		return false, false, err
	}
	// Never let attacker-controlled project headers grow this process cache.
	// Real projects are bounded by the registry or by a module/config already
	// loaded into this runtime; arbitrary unknown IDs stay uncached.
	if !projectExists && !s.knownRuntimeProject(projectID) {
		return false, false, nil
	}
	return enabled, true, nil
}

func (s *Server) knownRuntimeProject(projectID string) bool {
	s.projectMu.RLock()
	_, registered := s.projects[projectID]
	_, databaseConfigured := s.config.ProjectDatabases[projectID]
	_, keyConfigured := s.config.ProjectKeys[projectID]
	s.projectMu.RUnlock()
	if registered || databaseConfigured || keyConfigured {
		return true
	}
	if s.runtime == nil {
		return false
	}
	if s.runtime.EngineForProject(projectID) != nil {
		return true
	}
	for _, loadedProjectID := range s.runtime.ProjectIDs() {
		if loadedProjectID == projectID {
			return true
		}
	}
	return false
}

func (s *Server) projectRequiresAuthentication(ctx context.Context, projectID string) bool {
	// Application data is never anonymously addressable. Provider enablement
	// controls how accounts obtain sessions; it does not weaken the tenant
	// Member gate on Query, Reducer, Action, Replica, or Live Query traffic.
	return true
}

func (s *Server) googleAuthConfiguration(ctx context.Context, projectID string) ([]string, bool, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, false, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var enabled bool
	err = db.QueryRowContext(ctx, `SELECT enabled FROM gonvex_auth_providers WHERE project_id = $1 AND provider = $2`, projectID, googleProvider).Scan(&enabled)
	if err != nil && err != sql.ErrNoRows {
		return nil, false, err
	}
	rows, err := db.QueryContext(ctx, `SELECT redirect_uri FROM gonvex_auth_redirect_uris
		WHERE project_id = $1 AND provider = $2 ORDER BY redirect_uri`, projectID, googleProvider)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	redirectURIs := []string{}
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, false, err
		}
		redirectURIs = append(redirectURIs, value)
	}
	return redirectURIs, enabled, rows.Err()
}

func (s *Server) googleRedirectAllowed(ctx context.Context, projectID string, redirectURI string) (bool, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return false, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var allowed bool
	err = db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM gonvex_auth_providers p
		JOIN gonvex_auth_redirect_uris r ON r.project_id = p.project_id AND r.provider = p.provider
		WHERE p.project_id = $1 AND p.provider = $2 AND p.enabled = TRUE AND r.redirect_uri = $3
	)`, projectID, googleProvider, redirectURI).Scan(&allowed)
	return allowed, err
}

func (s *Server) handleAuthConfig(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.URL.Query().Get("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project is required"})
		return
	}
	_, enabled, err := s.googleAuthConfiguration(r.Context(), projectID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "auth configuration is unavailable"})
		return
	}
	providers := []string{}
	if enabled && s.googleAuthBrokerReady() {
		providers = append(providers, googleProvider)
	}
	providerConfig, _ := s.appAuthProviderConfiguration(r.Context(), projectID)
	mode, _ := s.projectDatabaseMode(r.Context(), projectID)
	writeJSON(w, http.StatusOK, map[string]any{
		"project": projectID, "providers": providers, "databaseMode": mode,
		"signupMode":                  providerConfig.SignupMode,
		"accessTokenLifetimeSeconds":  int(appSessionTTL.Seconds()),
		"refreshTokenLifetimeSeconds": int(appRefreshSessionTTL.Seconds()),
	})
}

func (s *Server) googleAuthBrokerReady() bool {
	return len(s.googleAuthReadinessIssues()) == 0
}

func (s *Server) googleAuthReadinessIssues() []string {
	issues := []string{}
	if strings.TrimSpace(s.config.GoogleClientID) == "" {
		issues = append(issues, "GONVEX_GOOGLE_CLIENT_ID is not configured")
	}
	if strings.TrimSpace(s.config.GoogleClientSecret) == "" {
		issues = append(issues, "GONVEX_GOOGLE_CLIENT_SECRET is not configured")
	}
	if _, err := normalizeAuthPublicURL(s.config.AuthPublicURL); err != nil {
		issues = append(issues, err.Error())
	}
	for _, trustedProxy := range s.config.TrustedProxyCIDRs {
		trustedProxy = strings.TrimSpace(trustedProxy)
		if net.ParseIP(trustedProxy) == nil {
			if _, _, err := net.ParseCIDR(trustedProxy); err != nil {
				issues = append(issues, "GONVEX_TRUSTED_PROXY_CIDRS contains an invalid IP address or CIDR: "+trustedProxy)
			}
		}
	}
	for _, endpoint := range []struct {
		name string
		raw  string
	}{
		{name: "GONVEX_GOOGLE_AUTHORIZE_URL", raw: s.config.GoogleAuthorizeURL},
		{name: "GONVEX_GOOGLE_TOKEN_URL", raw: s.config.GoogleTokenURL},
		{name: "GONVEX_GOOGLE_JWKS_URL", raw: s.config.GoogleJWKSURL},
	} {
		name, raw := endpoint.name, endpoint.raw
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || !parsed.IsAbs() || parsed.Host == "" {
			issues = append(issues, name+" is not a valid absolute URL")
			continue
		}
		hostname := strings.ToLower(parsed.Hostname())
		local := hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1"
		if parsed.Scheme != "https" && !(parsed.Scheme == "http" && local) {
			issues = append(issues, name+" must use https")
		}
	}
	return issues
}

func normalizeAuthPublicURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !parsed.IsAbs() || parsed.Host == "" {
		return "", fmt.Errorf("GONVEX_AUTH_URL must be the browser-facing runtime origin")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", fmt.Errorf("GONVEX_AUTH_URL must contain only scheme, host, and optional port")
	}
	hostname := strings.ToLower(parsed.Hostname())
	local := hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1"
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && local) {
		return "", fmt.Errorf("GONVEX_AUTH_URL must use https (http is allowed only for localhost)")
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func (s *Server) configuredGoogleCallbackURL() string {
	base, err := normalizeAuthPublicURL(s.config.AuthPublicURL)
	if err != nil {
		return ""
	}
	return base + "/auth/google/callback"
}

func (s *Server) googleCallbackURL(_ *http.Request) (string, error) {
	if configured := s.configuredGoogleCallbackURL(); configured != "" {
		return configured, nil
	}
	return "", fmt.Errorf("GONVEX_AUTH_URL is required")
}

func (s *Server) handleGoogleAuthorize(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "authorize", 30, time.Minute) {
		return
	}
	if !s.googleAuthBrokerReady() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Google auth is not configured on this Gonvex runtime"})
		return
	}
	query := r.URL.Query()
	projectID := strings.TrimSpace(query.Get("project"))
	redirectURI, err := normalizeAppRedirectURI(query.Get("redirect_uri"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	state := strings.TrimSpace(query.Get("state"))
	challenge := strings.TrimSpace(query.Get("code_challenge"))
	if projectID == "" || len(state) < 16 || len(state) > 512 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project and a valid state are required"})
		return
	}
	if query.Get("code_challenge_method") != "S256" || !pkceValuePattern.MatchString(challenge) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "PKCE with a valid S256 code challenge is required"})
		return
	}
	allowed, err := s.googleRedirectAllowed(r.Context(), projectID, redirectURI)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "auth configuration is unavailable"})
		return
	}
	if !allowed {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "redirect URI is not registered for this project"})
		return
	}
	callbackURL, err := s.googleCallbackURL(r)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	transactionToken, err := randomID("oauth")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not start auth flow"})
		return
	}
	nonce, err := randomID("nonce")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not start auth flow"})
		return
	}
	if err := s.saveAuthTransaction(r.Context(), transactionToken, authTransaction{
		ProjectID: projectID, RedirectURI: redirectURI, AppState: state,
		CodeChallenge: challenge, Nonce: nonce, GoogleRedirectURI: callbackURL,
	}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not start auth flow"})
		return
	}
	authorizeURL, err := url.Parse(s.config.GoogleAuthorizeURL)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Google authorize URL is invalid"})
		return
	}
	params := authorizeURL.Query()
	params.Set("client_id", s.config.GoogleClientID)
	params.Set("redirect_uri", callbackURL)
	params.Set("response_type", "code")
	params.Set("scope", "openid email profile")
	params.Set("state", transactionToken)
	params.Set("nonce", nonce)
	authorizeURL.RawQuery = params.Encode()
	http.Redirect(w, r, authorizeURL.String(), http.StatusFound)
}

func (s *Server) saveAuthTransaction(ctx context.Context, token string, transaction authTransaction) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	_, _ = db.ExecContext(ctx, `DELETE FROM gonvex_auth_transactions WHERE expires_at <= now()`)
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_auth_transactions (
		token_hash, project_id, redirect_uri, app_state, code_challenge, nonce, google_redirect_uri, expires_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		sha256Hex(token), transaction.ProjectID, transaction.RedirectURI, transaction.AppState,
		transaction.CodeChallenge, transaction.Nonce, transaction.GoogleRedirectURI, time.Now().Add(authTransactionTTL))
	return err
}

func (s *Server) consumeAuthTransaction(ctx context.Context, token string) (authTransaction, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return authTransaction{}, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var transaction authTransaction
	err = db.QueryRowContext(ctx, `DELETE FROM gonvex_auth_transactions
		WHERE token_hash = $1 AND expires_at > now()
		RETURNING project_id, redirect_uri, app_state, code_challenge, nonce, google_redirect_uri`, sha256Hex(token)).Scan(
		&transaction.ProjectID, &transaction.RedirectURI, &transaction.AppState,
		&transaction.CodeChallenge, &transaction.Nonce, &transaction.GoogleRedirectURI,
	)
	if err == sql.ErrNoRows {
		return authTransaction{}, fmt.Errorf("invalid or expired OAuth state")
	}
	return transaction, err
}

func (s *Server) handleGoogleCallback(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "google-callback", 60, time.Minute) {
		return
	}
	transaction, err := s.consumeAuthTransaction(r.Context(), strings.TrimSpace(r.URL.Query().Get("state")))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if providerError := strings.TrimSpace(r.URL.Query().Get("error")); providerError != "" {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": providerError, "state": transaction.AppState})
		return
	}
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	if code == "" {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "missing_google_code", "state": transaction.AppState})
		return
	}
	idToken, err := s.exchangeGoogleCode(r.Context(), code, transaction.GoogleRedirectURI)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "google_exchange_failed", "state": transaction.AppState})
		return
	}
	identity, err := s.verifyGoogleIDToken(r.Context(), idToken, transaction.Nonce)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "invalid_google_identity", "state": transaction.AppState})
		return
	}
	if !identity.EmailVerified || identity.Email == "" {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "verified_google_email_required", "state": transaction.AppState})
		return
	}
	user, err := s.upsertAppAuthAccount(r.Context(), transaction.ProjectID, identity)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "account_creation_failed", "state": transaction.AppState})
		return
	}
	if err := s.ensureAppAuthMemberships(r.Context(), transaction.ProjectID, user); err != nil {
		errorCode := "membership_setup_failed"
		if errors.Is(err, errAppAuthInvitationRequired) {
			errorCode = "invitation_required"
		}
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": errorCode, "state": transaction.AppState})
		return
	}
	authCode, err := s.createAppAuthCode(r.Context(), transaction, user.ID)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "code_creation_failed", "state": transaction.AppState})
		return
	}
	redirectToApp(w, r, transaction.RedirectURI, map[string]string{"code": authCode, "state": transaction.AppState})
}

func redirectToApp(w http.ResponseWriter, r *http.Request, destination string, values map[string]string) {
	target, err := url.Parse(destination)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "invalid app redirect"})
		return
	}
	query := target.Query()
	for key, value := range values {
		query.Set(key, value)
	}
	target.RawQuery = query.Encode()
	http.Redirect(w, r, target.String(), http.StatusFound)
}

func (s *Server) exchangeGoogleCode(ctx context.Context, code string, redirectURI string) (string, error) {
	form := url.Values{
		"code":          {code},
		"client_id":     {s.config.GoogleClientID},
		"client_secret": {s.config.GoogleClientSecret},
		"redirect_uri":  {redirectURI},
		"grant_type":    {"authorization_code"},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.config.GoogleTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("content-type", "application/x-www-form-urlencoded")
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	var payload struct {
		IDToken string `json:"id_token"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return "", err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || payload.IDToken == "" {
		return "", fmt.Errorf("Google token exchange returned %d (%s)", response.StatusCode, payload.Error)
	}
	return payload.IDToken, nil
}

func (s *Server) upsertAppAuthAccount(ctx context.Context, projectID string, identity googleIdentity) (appAuthAccount, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return appAuthAccount{}, fmt.Errorf("auth account store is unavailable")
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return appAuthAccount{}, err
	}
	defer tx.Rollback()

	// Resolve the global Account independently of this project. Exact provider
	// identity wins; a single verified-email account may be reused. Ambiguous
	// email matches intentionally create no silent merge.
	const googleIssuer = "https://accounts.google.com"
	accountID := ""
	err = tx.QueryRowContext(ctx, `SELECT identity.account_id FROM account_identities identity
		JOIN accounts account ON account.id = identity.account_id
		WHERE identity.provider = $1 AND identity.issuer = $2 AND identity.subject = $3
			AND account.auth_realm_id = $4`, googleProvider, googleIssuer, identity.Subject, projectID).Scan(&accountID)
	if err != nil && err != sql.ErrNoRows {
		return appAuthAccount{}, err
	}
	if accountID == "" && identity.EmailVerified && strings.TrimSpace(identity.Email) != "" {
		rows, queryErr := tx.QueryContext(ctx, `SELECT DISTINCT identity.account_id FROM account_identities identity
			JOIN accounts account ON account.id = identity.account_id
			WHERE identity.verified_email AND lower(identity.email) = lower($1)
				AND account.auth_realm_id = $2
			ORDER BY identity.account_id LIMIT 2`, identity.Email, projectID)
		if queryErr != nil {
			return appAuthAccount{}, queryErr
		}
		matches := []string{}
		for rows.Next() {
			var candidate string
			if scanErr := rows.Scan(&candidate); scanErr != nil {
				rows.Close()
				return appAuthAccount{}, scanErr
			}
			matches = append(matches, candidate)
		}
		if rowsErr := rows.Close(); rowsErr != nil {
			return appAuthAccount{}, rowsErr
		}
		if len(matches) == 1 {
			accountID = matches[0]
		}
	}
	if accountID == "" {
		accountID, err = randomID("acct")
		if err != nil {
			return appAuthAccount{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO accounts (id, auth_realm_id, email, name, avatar_url, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, name = EXCLUDED.name,
			avatar_url = EXCLUDED.avatar_url, updated_at = now()
		WHERE accounts.auth_realm_id = EXCLUDED.auth_realm_id`,
		accountID, projectID, identity.Email, identity.Name, identity.Picture); err != nil {
		return appAuthAccount{}, err
	}
	if err := tx.QueryRowContext(ctx, `INSERT INTO account_identities (
		account_id, provider, issuer, subject, email, verified_email, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, now())
	ON CONFLICT (provider, issuer, subject) DO UPDATE SET
		email = EXCLUDED.email, verified_email = EXCLUDED.verified_email, updated_at = now()
	RETURNING account_id`, accountID, googleProvider, googleIssuer, identity.Subject, identity.Email, identity.EmailVerified).Scan(&accountID); err != nil {
		return appAuthAccount{}, err
	}

	var account appAuthAccount
	err = tx.QueryRowContext(ctx, `SELECT id, id, email, $3::boolean, name, avatar_url, $4::text,
		disabled_at IS NOT NULL, created_at, updated_at
		FROM accounts WHERE id = $1 AND auth_realm_id = $2 AND disabled_at IS NULL`,
		accountID, projectID, identity.EmailVerified, googleProvider).Scan(
		&account.ID, &account.AccountID, &account.Email, &account.EmailVerified, &account.Name,
		&account.Picture, &account.Provider, &account.Disabled, &account.CreatedAt, &account.LastSignedInAt,
	)
	if err != nil {
		return appAuthAccount{}, err
	}
	if err := tx.Commit(); err != nil {
		return appAuthAccount{}, err
	}
	return account, nil
}

func (s *Server) createAppAuthCode(ctx context.Context, transaction authTransaction, accountID string) (string, error) {
	code, err := randomID("authcode")
	if err != nil {
		return "", err
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return "", fmt.Errorf("auth code store is unavailable")
	}
	defer db.Close()
	// Code issuance is the final step of the native sign-in flow, but callers
	// (including non-HTTP integrations) may issue a code directly after
	// upserting an Account. Ensure the canonical tenant Member exists before a
	// session can be exchanged, rather than minting a session that cannot enter
	// any tenant.
	var user appAuthAccount
	if err := db.QueryRowContext(ctx, `SELECT a.id, a.id, a.email,
		COALESCE(identity.verified_email, FALSE), a.name, a.avatar_url,
		COALESCE(identity.provider, ''), a.disabled_at IS NOT NULL, a.created_at, a.updated_at
		FROM accounts a
		LEFT JOIN LATERAL (
			SELECT provider, verified_email FROM account_identities
			WHERE account_id = a.id ORDER BY updated_at DESC LIMIT 1
		) identity ON TRUE
		WHERE a.id = $1 AND a.auth_realm_id = $2 AND a.disabled_at IS NULL`, accountID, transaction.ProjectID).Scan(
		&user.ID, &user.AccountID, &user.Email, &user.EmailVerified, &user.Name, &user.Picture,
		&user.Provider, &user.Disabled, &user.CreatedAt, &user.LastSignedInAt); err != nil {
		return "", err
	}
	if err := s.ensureAppAuthMemberships(ctx, transaction.ProjectID, user); err != nil {
		return "", err
	}
	_, _ = db.ExecContext(ctx, `DELETE FROM gonvex_auth_codes WHERE expires_at <= now() OR used_at IS NOT NULL`)
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_auth_codes (
		code_hash, project_id, account_id, redirect_uri, code_challenge, expires_at
	) VALUES ($1, $2, $3, $4, $5, $6)`, sha256Hex(code), transaction.ProjectID, accountID,
		transaction.RedirectURI, transaction.CodeChallenge, time.Now().Add(authCodeTTL))
	return code, err
}

func (s *Server) handleAppAuthToken(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "token", 60, time.Minute) {
		return
	}
	s.cleanupExpiredAppAuthRecords(r.Context())
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	defer r.Body.Close()
	var payload struct {
		GrantType    string `json:"grantType"`
		Project      string `json:"project"`
		Code         string `json:"code"`
		CodeVerifier string `json:"codeVerifier"`
		RedirectURI  string `json:"redirectUri"`
		RefreshToken string `json:"refreshToken"`
		Tenant       string `json:"tenant"`
	}
	if strings.HasPrefix(strings.ToLower(r.Header.Get("content-type")), "application/json") {
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid token request"})
			return
		}
	} else {
		if err := r.ParseForm(); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid token request"})
			return
		}
		payload.GrantType = r.Form.Get("grant_type")
		payload.Project = r.Form.Get("project")
		payload.Code = r.Form.Get("code")
		payload.CodeVerifier = r.Form.Get("code_verifier")
		payload.RedirectURI = r.Form.Get("redirect_uri")
		payload.RefreshToken = r.Form.Get("refresh_token")
		payload.Tenant = r.Form.Get("tenant")
	}
	projectID := strings.TrimSpace(payload.Project)
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project is required"})
		return
	}
	var grant appAuthSessionGrant
	var user appAuthAccount
	var err error
	switch payload.GrantType {
	case "authorization_code":
		if strings.TrimSpace(payload.Code) == "" || !pkceValuePattern.MatchString(payload.CodeVerifier) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "authorization code, redirect URI, and PKCE verifier are required"})
			return
		}
		redirectURI, normalizeErr := normalizeAppRedirectURI(payload.RedirectURI)
		if normalizeErr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": normalizeErr.Error()})
			return
		}
		grant, user, err = s.exchangeAppAuthCode(r.Context(), projectID, strings.TrimSpace(payload.Code), payload.CodeVerifier, redirectURI)
	case "refresh_token":
		if !strings.HasPrefix(strings.TrimSpace(payload.RefreshToken), "gvx_refresh_") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "refresh token is required"})
			return
		}
		grant, user, err = s.refreshAppSession(r.Context(), projectID, strings.TrimSpace(payload.RefreshToken))
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "grantType must be authorization_code or refresh_token"})
		return
	}
	if err != nil {
		var invalidGrant *invalidAppAuthGrantError
		if errors.As(err, &invalidGrant) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": invalidGrant.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "auth session service is temporarily unavailable"})
		}
		return
	}
	s.writeAppAuthSession(w, r, projectID, grant, user, payload.Tenant)
}

func (s *Server) cleanupExpiredAppAuthRecords(ctx context.Context) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return
	}
	defer db.Close()
	_, _ = db.ExecContext(ctx, `DELETE FROM gonvex_auth_transactions WHERE expires_at < now() - interval '1 day'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM gonvex_auth_codes WHERE expires_at < now() - interval '1 day'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM gonvex_auth_sessions WHERE expires_at < now() - interval '7 days'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM gonvex_auth_refresh_tokens WHERE expires_at < now() - interval '7 days'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM gonvex_auth_membership_invitations WHERE expires_at <= now()`)
}

type appAuthSessionGrant struct {
	AccessToken      string
	AccessExpiresAt  time.Time
	RefreshToken     string
	RefreshExpiresAt time.Time
	FamilyID         string
}

type invalidAppAuthGrantError struct {
	message string
}

func (err *invalidAppAuthGrantError) Error() string {
	return err.message
}

func invalidAppAuthGrant(message string) error {
	return &invalidAppAuthGrantError{message: message}
}

func (s *Server) exchangeAppAuthCode(ctx context.Context, projectID string, code string, verifier string, redirectURI string) (appAuthSessionGrant, appAuthAccount, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return appAuthSessionGrant{}, appAuthAccount{}, fmt.Errorf("auth session store is unavailable")
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return appAuthSessionGrant{}, appAuthAccount{}, err
	}
	defer tx.Rollback()
	var storedProject, accountID, storedRedirect, challenge string
	err = tx.QueryRowContext(ctx, `SELECT project_id, account_id, redirect_uri, code_challenge
		FROM gonvex_auth_codes WHERE code_hash = $1 AND used_at IS NULL AND expires_at > now() FOR UPDATE`, sha256Hex(code)).Scan(
		&storedProject, &accountID, &storedRedirect, &challenge,
	)
	if err == sql.ErrNoRows {
		return appAuthSessionGrant{}, appAuthAccount{}, invalidAppAuthGrant("invalid or expired authorization code")
	}
	if err != nil {
		return appAuthSessionGrant{}, appAuthAccount{}, err
	}
	if storedProject != projectID || storedRedirect != redirectURI || !constantTimeString(challenge, pkceChallenge(verifier)) {
		return appAuthSessionGrant{}, appAuthAccount{}, invalidAppAuthGrant("authorization code does not match this client")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE gonvex_auth_codes SET used_at = now() WHERE code_hash = $1`, sha256Hex(code)); err != nil {
		return appAuthSessionGrant{}, appAuthAccount{}, err
	}
	var account appAuthAccount
	if err := tx.QueryRowContext(ctx, `SELECT a.id, a.id, a.email,
		COALESCE(identity.verified_email, FALSE), a.name, a.avatar_url,
		COALESCE(identity.provider, ''), a.created_at, a.updated_at
		FROM accounts a
		LEFT JOIN LATERAL (
			SELECT provider, verified_email FROM account_identities
			WHERE account_id = a.id ORDER BY updated_at DESC LIMIT 1
		) identity ON TRUE
		WHERE a.id = $1 AND a.auth_realm_id = $2 AND a.disabled_at IS NULL`, accountID, projectID).Scan(
		&account.ID, &account.AccountID, &account.Email, &account.EmailVerified, &account.Name, &account.Picture,
		&account.Provider, &account.CreatedAt, &account.LastSignedInAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return appAuthSessionGrant{}, appAuthAccount{}, invalidAppAuthGrant("account is unavailable")
		}
		return appAuthSessionGrant{}, appAuthAccount{}, err
	}
	familyID, err := randomID("family")
	if err != nil {
		return appAuthSessionGrant{}, appAuthAccount{}, err
	}
	grant, err := issueAppAuthSessionGrant(ctx, tx, projectID, accountID, familyID, time.Now().Add(appRefreshSessionTTL).UTC())
	if err != nil {
		return appAuthSessionGrant{}, appAuthAccount{}, err
	}
	if err := tx.Commit(); err != nil {
		return appAuthSessionGrant{}, appAuthAccount{}, err
	}
	return grant, account, nil
}

func issueAppAuthSessionGrant(ctx context.Context, tx *sql.Tx, projectID string, accountID string, familyID string, refreshExpiresAt time.Time) (appAuthSessionGrant, error) {
	accessToken, err := newAppAuthToken("session")
	if err != nil {
		return appAuthSessionGrant{}, err
	}
	refreshToken, err := newAppAuthToken("refresh")
	if err != nil {
		return appAuthSessionGrant{}, err
	}
	grant := appAuthSessionGrant{
		AccessToken: accessToken, AccessExpiresAt: time.Now().Add(appSessionTTL).UTC(),
		RefreshToken: refreshToken, RefreshExpiresAt: refreshExpiresAt.UTC(), FamilyID: familyID,
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO gonvex_auth_sessions
		(token_hash, project_id, account_id, family_id, expires_at)
		VALUES ($1, $2, $3, $4, $5)`, sha256Hex(grant.AccessToken), projectID, accountID, familyID, grant.AccessExpiresAt); err != nil {
		return appAuthSessionGrant{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO gonvex_auth_refresh_tokens
		(token_hash, family_id, project_id, account_id, expires_at)
		VALUES ($1, $2, $3, $4, $5)`, sha256Hex(grant.RefreshToken), familyID, projectID, accountID, grant.RefreshExpiresAt); err != nil {
		return appAuthSessionGrant{}, err
	}
	return grant, nil
}

func newAppAuthToken(kind string) (string, error) {
	id, err := randomID(kind)
	if err != nil {
		return "", err
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "", err
	}
	return "gvx_" + id + "." + base64.RawURLEncoding.EncodeToString(secret[:]), nil
}

func (s *Server) refreshAppSession(ctx context.Context, projectID string, refreshToken string) (appAuthSessionGrant, appAuthAccount, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return appAuthSessionGrant{}, appAuthAccount{}, fmt.Errorf("auth session store is unavailable")
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return appAuthSessionGrant{}, appAuthAccount{}, err
	}
	defer tx.Rollback()
	grant, account, revokedAccountID, err := refreshAppSessionTx(ctx, tx, projectID, refreshToken)
	if err != nil {
		if revokedAccountID != "" {
			if commitErr := tx.Commit(); commitErr != nil {
				return appAuthSessionGrant{}, appAuthAccount{}, commitErr
			}
			s.revokeAppAuthConnections(projectID, revokedAccountID)
		}
		return appAuthSessionGrant{}, appAuthAccount{}, err
	}
	if err := tx.Commit(); err != nil {
		return appAuthSessionGrant{}, appAuthAccount{}, err
	}
	return grant, account, nil
}

// refreshAppSessionTx rotates one refresh token inside the caller's exact
// transaction. revokedAccountID is non-empty only when replay detection wrote
// a family revocation that the caller must commit before returning the error.
func refreshAppSessionTx(ctx context.Context, tx *sql.Tx, projectID string, refreshToken string) (appAuthSessionGrant, appAuthAccount, string, error) {
	var storedProject, accountID, familyID string
	var refreshExpiresAt time.Time
	var usedAt, revokedAt sql.NullTime
	err := tx.QueryRowContext(ctx, `SELECT project_id, account_id, family_id, expires_at, used_at, revoked_at
		FROM gonvex_auth_refresh_tokens WHERE token_hash = $1 FOR UPDATE`, sha256Hex(refreshToken)).Scan(
		&storedProject, &accountID, &familyID, &refreshExpiresAt, &usedAt, &revokedAt,
	)
	if err == sql.ErrNoRows {
		return appAuthSessionGrant{}, appAuthAccount{}, "", invalidAppAuthGrant("invalid or expired refresh token")
	}
	if err != nil {
		return appAuthSessionGrant{}, appAuthAccount{}, "", err
	}
	if storedProject != projectID || revokedAt.Valid || refreshExpiresAt.Before(time.Now()) {
		return appAuthSessionGrant{}, appAuthAccount{}, "", invalidAppAuthGrant("invalid or expired refresh token")
	}
	if usedAt.Valid {
		if time.Since(usedAt.Time) <= appRefreshReuseGrace {
			// Never issue a second child token from the same refresh token. A
			// duplicate request inside the short grace window is rejected without
			// revoking the winner; older reuse is treated as replay and revokes the
			// complete family.
			return appAuthSessionGrant{}, appAuthAccount{}, "", invalidAppAuthGrant("refresh token was already rotated; use the latest session")
		}
		if err := revokeAppAuthFamilyTx(ctx, tx, familyID); err != nil {
			return appAuthSessionGrant{}, appAuthAccount{}, "", err
		}
		return appAuthSessionGrant{}, appAuthAccount{}, accountID, invalidAppAuthGrant("refresh token reuse detected; this login was revoked")
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE gonvex_auth_refresh_tokens SET used_at = now()
			WHERE token_hash = $1 AND used_at IS NULL`, sha256Hex(refreshToken)); err != nil {
			return appAuthSessionGrant{}, appAuthAccount{}, "", err
		}
	}
	var account appAuthAccount
	if err := tx.QueryRowContext(ctx, `SELECT a.id, a.id, a.email,
		COALESCE(identity.verified_email, FALSE), a.name, a.avatar_url,
		COALESCE(identity.provider, ''), a.created_at, a.updated_at
		FROM accounts a
		LEFT JOIN LATERAL (
			SELECT provider, verified_email FROM account_identities
			WHERE account_id = a.id ORDER BY updated_at DESC LIMIT 1
		) identity ON TRUE
		WHERE a.id = $1 AND a.auth_realm_id = $2 AND a.disabled_at IS NULL`, accountID, projectID).Scan(
		&account.ID, &account.AccountID, &account.Email, &account.EmailVerified, &account.Name, &account.Picture,
		&account.Provider, &account.CreatedAt, &account.LastSignedInAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return appAuthSessionGrant{}, appAuthAccount{}, "", invalidAppAuthGrant("account is unavailable")
		}
		return appAuthSessionGrant{}, appAuthAccount{}, "", err
	}
	grant, err := issueAppAuthSessionGrant(ctx, tx, projectID, accountID, familyID, refreshExpiresAt)
	if err != nil {
		return appAuthSessionGrant{}, appAuthAccount{}, "", err
	}
	return grant, account, "", nil
}

func revokeAppAuthFamilyTx(ctx context.Context, tx *sql.Tx, familyID string) error {
	if familyID == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE gonvex_auth_sessions SET revoked_at = COALESCE(revoked_at, now())
		WHERE family_id = $1`, familyID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE gonvex_auth_refresh_tokens SET revoked_at = COALESCE(revoked_at, now())
		WHERE family_id = $1`, familyID)
	return err
}

func (s *Server) writeAppAuthSession(w http.ResponseWriter, r *http.Request, projectID string, grant appAuthSessionGrant, user appAuthAccount, requestedTenant string) {
	tenants, err := s.listAppAuthTenants(r.Context(), projectID, user.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "tenant memberships are unavailable"})
		return
	}
	activeTenantID := ""
	var activeMember *gonvex.Member
	if len(tenants) > 0 {
		active, selectErr := selectAppAuthTenant(tenants, requestedTenant)
		if selectErr != nil {
			active = tenants[0]
		}
		activeTenantID = active.ID
		activeMember, _ = s.loadTenantMember(r.Context(), projectID, activeTenantID, user.canonicalID())
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accessToken": grant.AccessToken, "tokenType": "Bearer",
		"expiresIn": int(appSessionTTL.Seconds()), "expiresAt": grant.AccessExpiresAt.UnixMilli(),
		"refreshToken": grant.RefreshToken, "refreshExpiresAt": grant.RefreshExpiresAt.UnixMilli(),
		"account": gonvex.Account{ID: user.canonicalID(), Email: user.Email, Name: user.Name, AvatarURL: user.Picture},
		"member":  activeMember,
		"tenants": tenants, "activeTenantId": activeTenantID,
	})
}

func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (s *Server) handleAppAuthLogout(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	defer r.Body.Close()
	var payload struct {
		RefreshToken string `json:"refreshToken"`
		All          bool   `json:"all"`
	}
	_ = json.NewDecoder(r.Body).Decode(&payload)
	_ = s.revokeAppAuthSession(r.Context(), bearerToken(r), strings.TrimSpace(payload.RefreshToken), payload.All)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) revokeAppAuthSession(ctx context.Context, accessToken string, refreshToken string, all bool) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var familyID, projectID, accountID string
	err = sql.ErrNoRows
	if accessToken != "" {
		err = tx.QueryRowContext(ctx, `SELECT family_id, project_id, account_id FROM gonvex_auth_sessions
			WHERE token_hash = $1`, sha256Hex(accessToken)).Scan(&familyID, &projectID, &accountID)
	}
	if (accessToken == "" || err == sql.ErrNoRows) && refreshToken != "" {
		err = tx.QueryRowContext(ctx, `SELECT family_id, project_id, account_id FROM gonvex_auth_refresh_tokens
			WHERE token_hash = $1`, sha256Hex(refreshToken)).Scan(&familyID, &projectID, &accountID)
	} else if accessToken == "" {
		return nil
	}
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if all {
		if _, err := tx.ExecContext(ctx, `UPDATE gonvex_auth_sessions SET revoked_at = COALESCE(revoked_at, now())
			WHERE project_id = $1 AND account_id = $2`, projectID, accountID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE gonvex_auth_refresh_tokens SET revoked_at = COALESCE(revoked_at, now())
			WHERE project_id = $1 AND account_id = $2`, projectID, accountID); err != nil {
			return err
		}
	} else if familyID != "" {
		if err := revokeAppAuthFamilyTx(ctx, tx, familyID); err != nil {
			return err
		}
	} else if accessToken != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE gonvex_auth_sessions SET revoked_at = COALESCE(revoked_at, now())
			WHERE token_hash = $1`, sha256Hex(accessToken)); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if all {
		s.revokeAppAuthConnections(projectID, accountID)
	} else if accessToken != "" {
		s.revokeAppAuthTokenConnection(accessToken)
	}
	return nil
}

type validatedAppSession struct {
	ProjectID   string
	Account     appAuthAccount
	Tenant      appAuthTenant
	Permissions map[string]any
}

func (s *Server) loadAppSessionIdentity(ctx context.Context, token string) (validatedAppSession, error) {
	if !strings.HasPrefix(strings.TrimSpace(token), "gvx_session_") {
		return validatedAppSession{}, fmt.Errorf("not a Gonvex app session")
	}
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return validatedAppSession{}, fmt.Errorf("auth session store is unavailable")
	}
	var session validatedAppSession
	err = db.QueryRowContext(ctx, `SELECT s.project_id, account.id, account.id, account.email,
		COALESCE(identity.verified_email, FALSE), account.name, account.avatar_url,
		COALESCE(identity.provider, ''), account.created_at, account.updated_at
		FROM gonvex_auth_sessions s
		JOIN accounts account ON account.id = s.account_id AND account.auth_realm_id = s.project_id
		LEFT JOIN LATERAL (
			SELECT provider, verified_email FROM account_identities
			WHERE account_id = account.id ORDER BY updated_at DESC LIMIT 1
		) identity ON TRUE
		WHERE s.token_hash = $1 AND s.revoked_at IS NULL AND s.expires_at > now()
			AND account.disabled_at IS NULL`, sha256Hex(token)).Scan(
		&session.ProjectID, &session.Account.ID, &session.Account.AccountID, &session.Account.Email, &session.Account.EmailVerified, &session.Account.Name,
		&session.Account.Picture, &session.Account.Provider, &session.Account.CreatedAt, &session.Account.LastSignedInAt,
	)
	if err == sql.ErrNoRows {
		return validatedAppSession{}, fmt.Errorf("invalid or expired app session")
	}
	if err != nil {
		return validatedAppSession{}, err
	}
	_, _ = db.ExecContext(ctx, `UPDATE gonvex_auth_sessions SET last_seen_at = now()
		WHERE token_hash = $1 AND (last_seen_at IS NULL OR last_seen_at < now() - interval '5 minutes')`, sha256Hex(token))
	return session, nil
}

func (s *Server) validateAppSession(ctx context.Context, requestedProjectID string, token string, requestedTenantID string) (validatedAppSession, string, error) {
	session, err := s.loadAppSessionIdentity(ctx, token)
	if err != nil {
		return validatedAppSession{}, "", err
	}
	if requestedProjectID != "" && requestedProjectID != session.ProjectID {
		return validatedAppSession{}, "", fmt.Errorf("app session was issued for a different project")
	}
	tenants, err := s.listAppAuthTenants(ctx, session.ProjectID, session.Account.ID)
	if err != nil {
		return validatedAppSession{}, "", err
	}
	tenant, err := s.resolveAppAuthTenant(ctx, session.ProjectID, session.Account.canonicalID(), tenants, requestedTenantID)
	if err != nil {
		return validatedAppSession{}, "", err
	}
	// The tenant's own member row is the admission decision, re-read here even
	// when discovery already reported a role.
	member, err := s.loadTenantMember(ctx, session.ProjectID, tenant.ID, session.Account.canonicalID())
	if err != nil {
		return validatedAppSession{}, "", err
	}
	tenant.Role = member.Role
	tenant.Permissions = member.Permissions
	session.Tenant = tenant
	session.Permissions = map[string]any{}
	for key, value := range tenant.Permissions {
		session.Permissions[key] = value
	}
	// Membership role is authoritative and cannot be shadowed by arbitrary
	// custom permission JSON from an older record.
	session.Permissions["role"] = tenant.Role
	return session, tenant.ID, nil
}

func (s *Server) verifyGoogleIDToken(ctx context.Context, token string, expectedNonce string) (googleIdentity, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return googleIdentity{}, fmt.Errorf("Google ID token is malformed")
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	if err := decodeJWTPart(parts[0], &header); err != nil || header.Algorithm != "RS256" || header.KeyID == "" {
		return googleIdentity{}, fmt.Errorf("Google ID token has an invalid header")
	}
	key, err := s.googlePublicKey(ctx, header.KeyID, false)
	if err != nil {
		return googleIdentity{}, err
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return googleIdentity{}, fmt.Errorf("Google ID token has an invalid signature")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature); err != nil {
		// A key can rotate before its cache TTL. Refresh once before rejecting it.
		key, refreshErr := s.googlePublicKey(ctx, header.KeyID, true)
		if refreshErr != nil || rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
			return googleIdentity{}, fmt.Errorf("Google ID token signature is invalid")
		}
	}
	var claims struct {
		Issuer        string `json:"iss"`
		Audience      string `json:"aud"`
		Subject       string `json:"sub"`
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
		Name          string `json:"name"`
		Picture       string `json:"picture"`
		Nonce         string `json:"nonce"`
		ExpiresAt     int64  `json:"exp"`
		IssuedAt      int64  `json:"iat"`
	}
	if err := decodeJWTPart(parts[1], &claims); err != nil {
		return googleIdentity{}, fmt.Errorf("Google ID token claims are malformed")
	}
	now := time.Now().Unix()
	if claims.Issuer != "https://accounts.google.com" && claims.Issuer != "accounts.google.com" {
		return googleIdentity{}, fmt.Errorf("Google ID token issuer is invalid")
	}
	if claims.Audience != s.config.GoogleClientID || claims.Subject == "" || claims.ExpiresAt <= now || claims.IssuedAt > now+300 {
		return googleIdentity{}, fmt.Errorf("Google ID token claims are invalid")
	}
	if expectedNonce == "" || !constantTimeString(claims.Nonce, expectedNonce) {
		return googleIdentity{}, fmt.Errorf("Google ID token nonce is invalid")
	}
	return googleIdentity{
		Subject: claims.Subject, Email: strings.ToLower(strings.TrimSpace(claims.Email)),
		EmailVerified: claims.EmailVerified, Name: strings.TrimSpace(claims.Name), Picture: strings.TrimSpace(claims.Picture),
	}, nil
}

func decodeJWTPart(encoded string, target any) error {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

func (s *Server) googlePublicKey(ctx context.Context, keyID string, forceRefresh bool) (*rsa.PublicKey, error) {
	s.googleKeys.mu.Lock()
	defer s.googleKeys.mu.Unlock()
	if !forceRefresh && time.Now().Before(s.googleKeys.expiresAt) {
		if key := s.googleKeys.keys[keyID]; key != nil {
			return key, nil
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.config.GoogleJWKSURL, nil)
	if err != nil {
		return nil, err
	}
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("Google JWKS returned %d", response.StatusCode)
	}
	var document struct {
		Keys []struct {
			KeyType   string `json:"kty"`
			KeyID     string `json:"kid"`
			Algorithm string `json:"alg"`
			Modulus   string `json:"n"`
			Exponent  string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&document); err != nil {
		return nil, err
	}
	keys := map[string]*rsa.PublicKey{}
	for _, item := range document.Keys {
		if item.KeyType != "RSA" || item.KeyID == "" || (item.Algorithm != "" && item.Algorithm != "RS256") {
			continue
		}
		modulus, err := base64.RawURLEncoding.DecodeString(item.Modulus)
		if err != nil {
			continue
		}
		exponentBytes, err := base64.RawURLEncoding.DecodeString(item.Exponent)
		if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 4 {
			continue
		}
		exponent := 0
		for _, value := range exponentBytes {
			exponent = exponent<<8 | int(value)
		}
		if exponent < 3 {
			continue
		}
		keys[item.KeyID] = &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: exponent}
	}
	s.googleKeys.keys = keys
	s.googleKeys.expiresAt = time.Now().Add(jwksCacheTTL(response.Header.Get("cache-control")))
	key := keys[keyID]
	if key == nil {
		return nil, fmt.Errorf("Google signing key %q was not found", keyID)
	}
	return key, nil
}

func jwksCacheTTL(cacheControl string) time.Duration {
	for _, part := range strings.Split(cacheControl, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || strings.ToLower(name) != "max-age" {
			continue
		}
		seconds, err := strconv.Atoi(strings.TrimSpace(value))
		if err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return defaultGoogleKeyCacheTTL
}
