package server

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/dbpool"
)

func TestNormalizeAppRedirectURI(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "https app", value: "https://app.example.com/auth/callback"},
		{name: "localhost http", value: "http://localhost:5173/auth/callback"},
		{name: "loopback http", value: "http://127.0.0.1:4173/auth/callback"},
		{name: "remote http", value: "http://app.example.com/auth/callback", wantErr: true},
		{name: "relative", value: "/auth/callback", wantErr: true},
		{name: "fragment", value: "https://app.example.com/auth/callback#token", wantErr: true},
		{name: "userinfo", value: "https://user@app.example.com/auth/callback", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizeAppRedirectURI(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("normalizeAppRedirectURI(%q) error = %v", test.value, err)
			}
		})
	}
}

func TestPKCEChallengeMatchesRFC7636Example(t *testing.T) {
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	if got, want := pkceChallenge(verifier), "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"; got != want {
		t.Fatalf("challenge = %q, want %q", got, want)
	}
}

func TestAppAuthRateLimitUsesForwardedIPOnlyFromTrustedProxy(t *testing.T) {
	runtime := &Server{config: config.Config{TrustedProxyCIDRs: []string{"10.0.0.0/8"}}}
	trusted := httptest.NewRequest(http.MethodGet, "/auth/config", nil)
	trusted.RemoteAddr = "10.0.0.2:4321"
	trusted.Header.Set("x-forwarded-for", "198.51.100.9, 10.0.0.3")
	if got := runtime.requestRemoteIP(trusted); got != "198.51.100.9" {
		t.Fatalf("trusted proxy client ip = %q", got)
	}
	untrusted := httptest.NewRequest(http.MethodGet, "/auth/config", nil)
	untrusted.RemoteAddr = "203.0.113.7:4321"
	untrusted.Header.Set("x-forwarded-for", "198.51.100.9")
	if got := runtime.requestRemoteIP(untrusted); got != "203.0.113.7" {
		t.Fatalf("untrusted peer spoofed forwarded ip: %q", got)
	}
}

func TestAppAuthTokenInfrastructureFailureIsNotReportedAsInvalidCredentials(t *testing.T) {
	runtime := New(config.Config{})
	request := httptest.NewRequest(http.MethodPost, "/auth/token", bytes.NewBufferString(`{
		"grantType":"refresh_token","project":"missing-store","refreshToken":"gvx_refresh_valid-shape"
	}`))
	request.Header.Set("content-type", "application/json")
	response := httptest.NewRecorder()
	runtime.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("auth store failure returned %d instead of 500: %s", response.Code, response.Body.String())
	}
}

func TestRuntimeRequiresExplicitIdentityV2Migration(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	controlURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_identity_v2_required_"+tenantRegistryTestSuffix(t))
	db, err := dbpool.Open(controlURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := ensureProjectRegistry(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE gonvex_auth_users (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := ensureProjectRegistry(context.Background(), db); err == nil || !strings.Contains(err.Error(), "identity-v2 migration required") {
		t.Fatalf("legacy identity schema did not produce a migration-required error: %v", err)
	}
}

func TestTenantRuntimeRequiresCanonicalMembers(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	tenantURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_identity_v2_tenant_required_"+tenantRegistryTestSuffix(t))
	db, err := dbpool.Open(tenantURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE members (user_id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := ensureTenantLocalTables(context.Background(), db); err == nil || !strings.Contains(err.Error(), "identity-v2 migration required") {
		t.Fatalf("legacy tenant member schema did not produce a migration-required error: %v", err)
	}
}

func TestAppAuthCodeExchangeCreatesProjectScopedSession(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	controlURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_app_auth_"+tenantRegistryTestSuffix(t))
	const projectID = "app-auth-project"
	runtime := New(config.Config{ControlPlaneURL: controlURL, PostgresURL: baseURL, DashboardAuthProjectID: projectID})
	if err := runtime.saveProjectRegistry(context.Background(), projectTarget{
		ID: projectID, Name: "Auth App", Environment: "test", Database: "auth_app",
		DatabaseMode: "single", StorageBucket: "", Status: "test", Description: "auth integration test",
		databaseURL: controlURL, databaseName: "auth_app", syncKey: "project-key",
	}); err != nil {
		t.Fatal(err)
	}
	const redirectURI = "http://localhost:5173/auth/callback"
	if err := runtime.enableGoogleAuth(context.Background(), projectID, redirectURI, appAuthSignupPersonal); err != nil {
		t.Fatal(err)
	}
	if enabled, err := runtime.nativeAppAuthEnabled(context.Background(), "attacker-controlled-missing-project"); err != nil || enabled {
		t.Fatalf("unknown project auth state = %v, %v", enabled, err)
	}
	runtime.appAuthConfigMu.Lock()
	_, cachedUnknownProject := runtime.appAuthRequirements["attacker-controlled-missing-project"]
	runtime.appAuthConfigMu.Unlock()
	if cachedUnknownProject {
		t.Fatal("an unknown project ID was retained in the auth requirement cache")
	}
	const waitingProjectID = "lookup-already-in-progress"
	runtime.appAuthConfigMu.Lock()
	runtime.appAuthLookups[waitingProjectID] = &appAuthRequirementLookup{done: make(chan struct{})}
	runtime.appAuthConfigMu.Unlock()
	waiterContext, cancelWaiter := context.WithCancel(context.Background())
	cancelWaiter()
	if _, err := runtime.nativeAppAuthEnabled(waiterContext, waitingProjectID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled lookup waiter returned %v instead of context cancellation", err)
	}
	runtime.appAuthConfigMu.Lock()
	delete(runtime.appAuthLookups, waitingProjectID)
	runtime.appAuthConfigMu.Unlock()
	// Legacy projects can be deployed by /dev/sync without a row in the newer
	// control-plane registry. They are still bounded by the runtime's known
	// project set and must cache the disabled auth policy. A cancelled
	// subscription context must not turn that lookup into a connection-wide
	// authentication failure.
	const legacyProjectID = "legacy-synced-project"
	runtime.projectMu.Lock()
	runtime.projects[legacyProjectID] = projectTarget{ID: legacyProjectID}
	runtime.projectMu.Unlock()
	cancelledContext, cancelLookup := context.WithCancel(context.Background())
	cancelLookup()
	if enabled, err := runtime.nativeAppAuthEnabled(cancelledContext, legacyProjectID); err != nil || enabled {
		t.Fatalf("legacy project auth state = %v, %v", enabled, err)
	}
	runtime.appAuthConfigMu.Lock()
	legacyCache, cachedLegacyProject := runtime.appAuthRequirements[legacyProjectID]
	runtime.appAuthConfigMu.Unlock()
	if !cachedLegacyProject || legacyCache.Enabled {
		t.Fatalf("legacy project auth state was not cached: cached=%v value=%#v", cachedLegacyProject, legacyCache)
	}
	if !runtime.projectRequiresAuthentication(context.Background(), projectID) {
		t.Fatal("enabling native Google auth did not make the project require authentication")
	}
	if _, _, _, _, err := runtime.authenticateSocket(context.Background(), projectID, projectID, "", ""); err == nil {
		t.Fatal("auth-enabled project accepted an anonymous WebSocket session")
	}
	redirects, enabled, err := runtime.googleAuthConfiguration(context.Background(), projectID)
	if err != nil || !enabled || len(redirects) != 1 || redirects[0] != redirectURI {
		t.Fatalf("unexpected auth configuration: redirects=%v enabled=%v err=%v", redirects, enabled, err)
	}
	const retiredRedirectURI = "http://localhost:4173/retired"
	if err := runtime.enableGoogleAuth(context.Background(), projectID, retiredRedirectURI, appAuthSignupPersonal); err != nil {
		t.Fatal(err)
	}
	removeRedirectRequest := httptest.NewRequest(http.MethodDelete,
		"/dev/projects/"+projectID+"/auth/google?redirect_uri=http%3A%2F%2Flocalhost%3A4173%2Fretired", nil)
	removeRedirectRequest.Header.Set("x-gonvex-key", "project-key")
	removeRedirectResponse := httptest.NewRecorder()
	runtime.Handler().ServeHTTP(removeRedirectResponse, removeRedirectRequest)
	if removeRedirectResponse.Code != http.StatusOK {
		t.Fatalf("could not remove a retired redirect URI: %d %s", removeRedirectResponse.Code, removeRedirectResponse.Body.String())
	}
	redirects, enabled, err = runtime.googleAuthConfiguration(context.Background(), projectID)
	if err != nil || !enabled || len(redirects) != 1 || redirects[0] != redirectURI {
		t.Fatalf("retired callback remained registered: redirects=%v enabled=%v err=%v", redirects, enabled, err)
	}
	db, err := runtime.openProjectRegistry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO gonvex_auth_redirect_uris (project_id, provider, redirect_uri)
		SELECT $1, $2, 'https://preview-' || value || '.example.test/' FROM generate_series(1, 30) value`, projectID, googleProvider); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	registrationErrors := make(chan error, 8)
	var registrationGroup sync.WaitGroup
	for index := 0; index < 8; index++ {
		registrationGroup.Add(1)
		go func(index int) {
			defer registrationGroup.Done()
			registrationErrors <- runtime.enableGoogleAuth(context.Background(), projectID,
				fmt.Sprintf("https://concurrent-%d.example.test/", index), appAuthSignupPersonal)
		}(index)
	}
	registrationGroup.Wait()
	close(registrationErrors)
	successfulRegistrations := 0
	for registrationErr := range registrationErrors {
		if registrationErr == nil {
			successfulRegistrations++
		} else if !strings.Contains(registrationErr.Error(), "at most 32") {
			t.Fatalf("unexpected concurrent callback error: %v", registrationErr)
		}
	}
	redirects, _, err = runtime.googleAuthConfiguration(context.Background(), projectID)
	if err != nil || len(redirects) != maxAppAuthRedirectURIs || successfulRegistrations != 1 {
		t.Fatalf("concurrent callback cap failed: callbacks=%d successes=%d err=%v", len(redirects), successfulRegistrations, err)
	}
	user, err := runtime.upsertAppAuthAccount(context.Background(), projectID, googleIdentity{
		Subject: "google-subject", Email: "user@example.test", EmailVerified: true, Name: "Test User",
	})
	if err != nil {
		t.Fatal(err)
	}
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	code, err := runtime.createAppAuthCode(context.Background(), authTransaction{
		ProjectID: projectID, RedirectURI: redirectURI, CodeChallenge: pkceChallenge(verifier),
	}, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	grant, exchangedUser, err := runtime.exchangeAppAuthCode(context.Background(), projectID, code, verifier, redirectURI)
	if err != nil {
		t.Fatal(err)
	}
	if exchangedUser.ID != user.ID || !strings.HasPrefix(grant.AccessToken, "gvx_session_") || !strings.HasPrefix(grant.RefreshToken, "gvx_refresh_") {
		t.Fatalf("unexpected code exchange: user=%#v access=%q refresh=%q", exchangedUser, grant.AccessToken, grant.RefreshToken)
	}
	session, tenantID, err := runtime.validateAppSession(context.Background(), projectID, grant.AccessToken, "")
	if err != nil {
		t.Fatal(err)
	}
	if session.ProjectID != projectID || session.Account.ID != user.ID || tenantID != projectID {
		t.Fatalf("unexpected validated session: %#v tenant=%q", session, tenantID)
	}
	anonymousConnection := &wsConn{server: runtime, project: projectID, tenant: projectID}
	if err := anonymousConnection.revalidateAppAuth(context.Background()); err == nil {
		t.Fatal("an anonymous live connection survived after native auth was enabled")
	}
	legacyConnection := &wsConn{server: runtime, project: projectID, tenant: projectID, auth: true, authToken: "legacy-session", authCheckedAt: time.Now()}
	if err := legacyConnection.revalidateAppAuth(context.Background()); err == nil {
		t.Fatal("a legacy connection survived after native auth was enabled")
	}
	if _, _, err := runtime.validateAppSession(context.Background(), "another-project", grant.AccessToken, ""); err == nil {
		t.Fatal("expected a cross-project session to be rejected")
	}
	httpCode, err := runtime.createAppAuthCode(context.Background(), authTransaction{
		ProjectID: projectID, RedirectURI: redirectURI, CodeChallenge: pkceChallenge(verifier),
	}, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	tokenRequest := httptest.NewRequest(http.MethodPost, "/auth/token", bytes.NewBufferString(`{
		"grantType":"authorization_code","project":"app-auth-project","code":"`+httpCode+`",
		"codeVerifier":"`+verifier+`","redirectUri":"`+redirectURI+`"}`))
	tokenRequest.Header.Set("content-type", "application/json")
	tokenResponse := httptest.NewRecorder()
	runtime.Handler().ServeHTTP(tokenResponse, tokenRequest)
	var httpGrant struct {
		AccessToken  string          `json:"accessToken"`
		RefreshToken string          `json:"refreshToken"`
		Tenants      []appAuthTenant `json:"tenants"`
	}
	if err := json.Unmarshal(tokenResponse.Body.Bytes(), &httpGrant); err != nil {
		t.Fatal(err)
	}
	if tokenResponse.Code != http.StatusOK || tokenResponse.Header().Get("cache-control") != "no-store" || httpGrant.AccessToken == "" || httpGrant.RefreshToken == "" || len(httpGrant.Tenants) != 1 {
		t.Fatalf("unexpected HTTP token response: status=%d headers=%v body=%s", tokenResponse.Code, tokenResponse.Header(), tokenResponse.Body.String())
	}
	identity, err := runtime.executeControlQuery(context.Background(), &wsConn{
		server: runtime, project: projectID,
		user: &gonvex.Account{ID: exchangedUser.ID, Email: exchangedUser.Email, Name: exchangedUser.Name, AvatarURL: exchangedUser.Picture},
	}, "control.accounts.me", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	identityJSON, _ := json.Marshal(identity)
	if !strings.Contains(string(identityJSON), user.ID) {
		t.Fatalf("native Control Plane identity failed: %s", identityJSON)
	}
	if _, _, err := runtime.exchangeAppAuthCode(context.Background(), projectID, code, verifier, redirectURI); err == nil {
		t.Fatal("expected an authorization code replay to be rejected")
	}
	db, err = runtime.openProjectRegistry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO gonvex_dashboard_accounts (email, name, role, password_hash)
		VALUES ($1, $2, 'admin', 'not-used')`, user.Email, user.Name); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	dashboardRequest := httptest.NewRequest(http.MethodGet, "/dev/auth/me", nil)
	dashboardRequest.Header.Set("authorization", "Bearer "+grant.AccessToken)
	dashboardResponse := httptest.NewRecorder()
	runtime.Handler().ServeHTTP(dashboardResponse, dashboardRequest)
	if dashboardResponse.Code != http.StatusOK || !strings.Contains(dashboardResponse.Body.String(), `"authentication":"nativeGoogle"`) {
		t.Fatalf("trusted native session did not authenticate to dashboard: %d %s", dashboardResponse.Code, dashboardResponse.Body.String())
	}
	runtime.config.DashboardAuthProjectID = "another-project"
	rejectedDashboardRequest := httptest.NewRequest(http.MethodGet, "/dev/auth/me", nil)
	rejectedDashboardRequest.Header.Set("authorization", "Bearer "+grant.AccessToken)
	rejectedDashboardResponse := httptest.NewRecorder()
	runtime.Handler().ServeHTTP(rejectedDashboardResponse, rejectedDashboardRequest)
	if rejectedDashboardResponse.Code != http.StatusUnauthorized {
		t.Fatalf("session from an untrusted auth project reached dashboard: %d %s", rejectedDashboardResponse.Code, rejectedDashboardResponse.Body.String())
	}
	runtime.config.DashboardAuthProjectID = projectID
	refreshed, refreshedUser, err := runtime.refreshAppSession(context.Background(), projectID, grant.RefreshToken)
	if err != nil || refreshedUser.ID != user.ID || refreshed.AccessToken == grant.AccessToken || refreshed.RefreshToken == grant.RefreshToken {
		t.Fatalf("unexpected refresh rotation: grant=%#v user=%#v err=%v", refreshed, refreshedUser, err)
	}
	if _, _, err := runtime.validateAppSession(context.Background(), projectID, refreshed.AccessToken, ""); err != nil {
		t.Fatalf("rotated access token was not valid: %v", err)
	}
	if _, _, err := runtime.refreshAppSession(context.Background(), projectID, grant.RefreshToken); err == nil || !strings.Contains(err.Error(), "already rotated") {
		t.Fatalf("an immediate duplicate refresh issued another token: %v", err)
	}
	if _, _, err := runtime.validateAppSession(context.Background(), projectID, refreshed.AccessToken, ""); err != nil {
		t.Fatalf("duplicate-request grace revoked the winning session: %v", err)
	}
	db, err = runtime.openProjectRegistry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE gonvex_auth_refresh_tokens
		SET used_at = now() - interval '10 seconds' WHERE token_hash = $1`, sha256Hex(grant.RefreshToken)); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	if _, _, err := runtime.refreshAppSession(context.Background(), projectID, grant.RefreshToken); err == nil || !strings.Contains(err.Error(), "reuse detected") {
		t.Fatalf("expected refresh-token reuse to revoke the family, got %v", err)
	}
	if _, _, err := runtime.validateAppSession(context.Background(), projectID, refreshed.AccessToken, ""); err == nil {
		t.Fatal("expected replay detection to revoke the rotated access token")
	}
}

func TestAppAuthSingleDatabaseInviteOnlyRequiresCentralInvitation(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	controlURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_app_auth_single_invite_"+tenantRegistryTestSuffix(t))
	const projectID = "single-invite-project"
	runtime := New(config.Config{ControlPlaneURL: controlURL, PostgresURL: baseURL})
	if err := runtime.saveProjectRegistry(context.Background(), projectTarget{
		ID: projectID, Name: "Private App", Environment: "test", Database: "private_app",
		DatabaseMode: "single", Status: "test", Description: "single invite-only auth test",
		databaseURL: controlURL, databaseName: "private_app", syncKey: "project-key",
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.enableGoogleAuth(context.Background(), projectID, "http://localhost:5173/", appAuthSignupInviteOnly); err != nil {
		t.Fatal(err)
	}
	const invitedEmail = "private-owner@example.test"
	if err := runtime.inviteAppAuthMember(context.Background(), projectID, projectID, invitedEmail, "owner", nil, "project-admin"); err != nil {
		t.Fatal(err)
	}
	invited, err := runtime.upsertAppAuthAccount(context.Background(), projectID, googleIdentity{
		Subject: "private-owner-subject", Email: invitedEmail, EmailVerified: true, Name: "Private Owner",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ensureAppAuthMemberships(context.Background(), projectID, invited); err != nil {
		t.Fatalf("invited single-database user was rejected: %v", err)
	}
	tenants, err := runtime.listAppAuthTenants(context.Background(), projectID, invited.ID)
	if err != nil || len(tenants) != 1 || tenants[0].ID != projectID || tenants[0].Role != "owner" {
		t.Fatalf("single-database membership was not applied: tenants=%#v err=%v", tenants, err)
	}
	outsider, err := runtime.upsertAppAuthAccount(context.Background(), projectID, googleIdentity{
		Subject: "private-outsider-subject", Email: "private-outsider@example.test", EmailVerified: true, Name: "Outsider",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ensureAppAuthMemberships(context.Background(), projectID, outsider); err == nil || !errors.Is(err, errAppAuthInvitationRequired) {
		t.Fatalf("single-database invite-only signup admitted an outsider: %v", err)
	}
}

func TestAppAuthMultiTenantPersonalWorkspaceInvitationsAndSwitching(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	controlURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_app_auth_multi_"+tenantRegistryTestSuffix(t))
	runtime := New(config.Config{ControlPlaneURL: controlURL, PostgresURL: baseURL})
	projectID, err := generateProjectID()
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.saveProjectRegistry(context.Background(), projectTarget{
		ID: projectID, Name: "Multi Auth App", Environment: "test", Database: "auth_multi",
		DatabaseMode: "multiTenant", Status: "test", Description: "multi-tenant auth integration test",
		databaseURL: controlURL, databaseName: "auth_multi", syncKey: "project-key",
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		runtime.projectMu.RLock()
		names := []string{}
		for _, tenant := range runtime.tenants {
			if tenant.ProjectID == projectID && tenant.databaseName != "" {
				names = append(names, tenant.databaseName)
			}
		}
		runtime.projectMu.RUnlock()
		for _, name := range names {
			_ = dropProjectDatabase(context.Background(), baseURL, name)
		}
	})
	// Stop projection goroutines before the cleanup below drops their tenant
	// databases. This also exercises the runtime shutdown boundary that keeps
	// asynchronous Control Plane work from outliving its database owner.
	t.Cleanup(runtime.Close)
	const redirectURI = "http://localhost:5173/"
	if err := runtime.enableGoogleAuth(context.Background(), projectID, redirectURI, appAuthSignupPersonal); err != nil {
		t.Fatal(err)
	}
	owner, err := runtime.upsertAppAuthAccount(context.Background(), projectID, googleIdentity{
		Subject: "owner-google-subject", Email: "owner@example.test", EmailVerified: true, Name: "Owner",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ensureAppAuthMemberships(context.Background(), projectID, owner); err != nil {
		t.Fatal(err)
	}
	tenants, err := runtime.listAppAuthTenants(context.Background(), projectID, owner.ID)
	if err != nil || len(tenants) != 1 || tenants[0].Role != "owner" {
		t.Fatalf("unexpected personal workspace: tenants=%#v err=%v", tenants, err)
	}
	personalTenantID := tenants[0].ID
	secondTenant, err := runtime.createAppAuthTenant(context.Background(), projectID, owner.ID, "Second workspace")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.upsertAppAuthMembership(context.Background(), projectID, secondTenant.ID, owner.ID, "member", nil); err == nil || !strings.Contains(err.Error(), "at least one owner") {
		t.Fatalf("last owner was demoted: %v", err)
	}
	admin, err := runtime.upsertAppAuthAccount(context.Background(), projectID, googleIdentity{
		Subject: "admin-google-subject", Email: "admin@example.test", EmailVerified: true, Name: "Admin",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.upsertAppAuthMembership(context.Background(), projectID, secondTenant.ID, admin.ID, "admin", nil); err != nil {
		t.Fatal(err)
	}
	if err := runtime.inviteAppAuthMemberAs(context.Background(), projectID, secondTenant.ID, owner.Email, "member", nil, admin.ID, "admin"); !errors.Is(err, errAppAuthOwnerRequired) {
		t.Fatalf("an admin changed an owner's role: %v", err)
	}
	if err := runtime.inviteAppAuthMemberAs(context.Background(), projectID, secondTenant.ID, "peer-admin@example.test", "admin", nil, admin.ID, "admin"); !errors.Is(err, errAppAuthOwnerRequired) {
		t.Fatalf("an admin granted privileged membership: %v", err)
	}
	const pendingOwnerEmail = "pending-owner@example.test"
	if err := runtime.inviteAppAuthMember(context.Background(), projectID, secondTenant.ID, pendingOwnerEmail, "owner", nil, owner.ID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.inviteAppAuthMemberAs(context.Background(), projectID, secondTenant.ID, pendingOwnerEmail, "member", nil, admin.ID, "admin"); !errors.Is(err, errAppAuthOwnerRequired) {
		t.Fatalf("an admin overwrote a pending owner invitation: %v", err)
	}
	if err := runtime.deleteAppAuthInvitationAs(context.Background(), projectID, secondTenant.ID, pendingOwnerEmail, "admin"); !errors.Is(err, errAppAuthOwnerRequired) {
		t.Fatalf("an admin cancelled a pending owner invitation: %v", err)
	}
	if err := runtime.deleteAppAuthInvitationAs(context.Background(), projectID, secondTenant.ID, pendingOwnerEmail, "owner"); err != nil {
		t.Fatal(err)
	}
	backupOwner, err := runtime.upsertAppAuthAccount(context.Background(), projectID, googleIdentity{
		Subject: "backup-owner-google-subject", Email: "backup-owner@example.test", EmailVerified: true, Name: "Backup Owner",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.upsertAppAuthMembership(context.Background(), projectID, secondTenant.ID, backupOwner.ID, "owner", nil); err != nil {
		t.Fatal(err)
	}
	db, err := runtime.openProjectRegistry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE accounts SET disabled_at = now()
		WHERE auth_realm_id = $1 AND id = $2`, projectID, backupOwner.ID); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	if err := runtime.upsertAppAuthMembership(context.Background(), projectID, secondTenant.ID, owner.ID, "member", nil); err == nil || !strings.Contains(err.Error(), "at least one owner") {
		t.Fatalf("the last active owner was demoted while only a disabled owner remained: %v", err)
	}
	if err := runtime.removeAppAuthMembership(context.Background(), projectID, secondTenant.ID, owner.ID); err == nil || !strings.Contains(err.Error(), "at least one owner") {
		t.Fatalf("the last active owner was removed while only a disabled owner remained: %v", err)
	}

	const invitedEmail = "invited@example.test"
	if err := runtime.inviteAppAuthMember(context.Background(), projectID, secondTenant.ID, invitedEmail, "viewer", map[string]any{"tasks:read": true, "role": "owner"}, owner.ID); err != nil {
		t.Fatal(err)
	}
	invited, err := runtime.upsertAppAuthAccount(context.Background(), projectID, googleIdentity{
		Subject: "invited-google-subject", Email: invitedEmail, EmailVerified: true, Name: "Invited",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ensureAppAuthMemberships(context.Background(), projectID, invited); err != nil {
		t.Fatal(err)
	}
	invitedTenants, err := runtime.listAppAuthTenants(context.Background(), projectID, invited.ID)
	if err != nil || len(invitedTenants) != 1 || invitedTenants[0].ID != secondTenant.ID || invitedTenants[0].Role != "viewer" {
		t.Fatalf("unexpected claimed invitation: tenants=%#v err=%v", invitedTenants, err)
	}

	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	code, err := runtime.createAppAuthCode(context.Background(), authTransaction{
		ProjectID: projectID, RedirectURI: redirectURI, CodeChallenge: pkceChallenge(verifier),
	}, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	grant, _, err := runtime.exchangeAppAuthCode(context.Background(), projectID, code, verifier, redirectURI)
	if err != nil {
		t.Fatal(err)
	}
	const cancelledEmail = "cancelled@example.test"
	if err := runtime.inviteAppAuthMember(context.Background(), projectID, secondTenant.ID, cancelledEmail, "member", nil, owner.ID); err != nil {
		t.Fatal(err)
	}
	ownerMember, err := runtime.loadTenantMember(context.Background(), projectID, secondTenant.ID, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.executeControlReducer(context.Background(), &wsConn{
		server: runtime, project: projectID, tenant: secondTenant.ID,
		user: &gonvex.Account{ID: owner.ID, Email: owner.Email, Name: owner.Name, AvatarURL: owner.Picture}, member: ownerMember,
	}, "control.invitations.revoke", json.RawMessage(`{"id":"","email":"cancelled@example.test"}`)); err != nil {
		t.Fatalf("could not cancel invitation through the native Control Plane: %v", err)
	}
	_, pendingInvitations, err := runtime.listAppAuthTenantMembers(context.Background(), projectID, secondTenant.ID)
	if err != nil || len(pendingInvitations) != 0 {
		t.Fatalf("cancelled invitation remained active: invitations=%#v err=%v", pendingInvitations, err)
	}
	session, tenantID, err := runtime.validateAppSession(context.Background(), projectID, grant.AccessToken, secondTenant.ID)
	if err != nil || tenantID != secondTenant.ID || session.Permissions["role"] != "owner" {
		t.Fatalf("could not switch to verified tenant: session=%#v tenant=%q err=%v", session, tenantID, err)
	}
	if _, _, err := runtime.validateAppSession(context.Background(), projectID, grant.AccessToken, "not-a-membership"); err == nil {
		t.Fatal("expected an unowned tenant selection to be rejected")
	}
	if _, _, err := runtime.validateAppSession(context.Background(), projectID, grant.AccessToken, personalTenantID); err != nil {
		t.Fatalf("owner could not switch back to personal tenant: %v", err)
	}

	invitedCode, err := runtime.createAppAuthCode(context.Background(), authTransaction{
		ProjectID: projectID, RedirectURI: redirectURI, CodeChallenge: pkceChallenge(verifier),
	}, invited.ID)
	if err != nil {
		t.Fatal(err)
	}
	invitedGrant, _, err := runtime.exchangeAppAuthCode(context.Background(), projectID, invitedCode, verifier, redirectURI)
	if err != nil {
		t.Fatal(err)
	}
	invitedSession, _, err := runtime.validateAppSession(context.Background(), projectID, invitedGrant.AccessToken, secondTenant.ID)
	if err != nil || invitedSession.Permissions["role"] != "viewer" || invitedSession.Permissions["tasks:read"] != true {
		t.Fatalf("invited permissions were not applied: session=%#v err=%v", invitedSession, err)
	}
	memberDB, err := runtime.tenantMemberDB(context.Background(), projectID, secondTenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := memberDB.ExecContext(context.Background(), `UPDATE members
		SET status = 'revoked', membership_revision = membership_revision + 1
		WHERE account_id = $1`, invited.ID); err != nil {
		t.Fatal(err)
	}
	db, err = runtime.openProjectRegistry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE account_tenant_index SET status = 'active'
		WHERE account_id = $1 AND tenant_id = $2`, invited.ID, secondTenant.ID); err != nil {
		db.Close()
		t.Fatal(err)
	}
	var staleDirectoryStatus string
	if err := db.QueryRowContext(context.Background(), `SELECT status FROM account_tenant_index
		WHERE account_id = $1 AND tenant_id = $2`, invited.ID, secondTenant.ID).Scan(&staleDirectoryStatus); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	if staleDirectoryStatus != "active" {
		t.Fatalf("test did not establish a stale active directory entry: %q", staleDirectoryStatus)
	}
	if _, _, err := runtime.validateAppSession(context.Background(), projectID, invitedGrant.AccessToken, secondTenant.ID); err == nil {
		t.Fatal("stale active Control Plane directory entry overrode tenant revocation")
	}
	if _, err := memberDB.ExecContext(context.Background(), `UPDATE members
		SET status = 'active', membership_revision = membership_revision + 1
		WHERE account_id = $1`, invited.ID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.removeAppAuthMembership(context.Background(), projectID, secondTenant.ID, invited.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtime.validateAppSession(context.Background(), projectID, invitedGrant.AccessToken, secondTenant.ID); err == nil {
		t.Fatal("expected membership removal to revoke the user's session")
	}
	if err := runtime.enableGoogleAuth(context.Background(), projectID, redirectURI, appAuthSignupInviteOnly); err != nil {
		t.Fatal(err)
	}
	const expiredEmail = "expired@example.test"
	if err := runtime.inviteAppAuthMember(context.Background(), projectID, secondTenant.ID, expiredEmail, "member", nil, owner.ID); err != nil {
		t.Fatal(err)
	}
	db, err = runtime.openProjectRegistry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE gonvex_auth_membership_invitations SET expires_at = now() - interval '1 second'
		WHERE project_id = $1 AND tenant_id = $2 AND email = $3`, projectID, secondTenant.ID, expiredEmail); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	expiredInviteUser, err := runtime.upsertAppAuthAccount(context.Background(), projectID, googleIdentity{
		Subject: "expired-google-subject", Email: expiredEmail, EmailVerified: true, Name: "Expired Invite",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ensureAppAuthMemberships(context.Background(), projectID, expiredInviteUser); err == nil || !strings.Contains(err.Error(), "invite-only") {
		t.Fatalf("expired invitation admitted an account: %v", err)
	}
	_, activeInvitations, err := runtime.listAppAuthTenantMembers(context.Background(), projectID, secondTenant.ID)
	if err != nil || len(activeInvitations) != 0 {
		t.Fatalf("expired invitation was returned as active: invitations=%#v err=%v", activeInvitations, err)
	}
	outsider, err := runtime.upsertAppAuthAccount(context.Background(), projectID, googleIdentity{
		Subject: "outsider-google-subject", Email: "outsider@example.test", EmailVerified: true, Name: "Outsider",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ensureAppAuthMemberships(context.Background(), projectID, outsider); err == nil || !strings.Contains(err.Error(), "invite-only") {
		t.Fatalf("invite-only signup admitted an uninvited account: %v", err)
	}
}

func TestVerifyGoogleIDTokenChecksSignatureAudienceAndNonce(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	const keyID = "google-test-key"
	exponent := big.NewInt(int64(privateKey.PublicKey.E)).Bytes()
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Header().Set("cache-control", "public, max-age=60")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA",
			"kid": keyID,
			"alg": "RS256",
			"n":   base64.RawURLEncoding.EncodeToString(privateKey.PublicKey.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(exponent),
		}}})
	}))
	defer jwks.Close()

	runtime := &Server{config: config.Config{
		GoogleClientID: "gonvex-google-client",
		GoogleJWKSURL:  jwks.URL,
	}}
	claims := map[string]any{
		"iss":            "https://accounts.google.com",
		"aud":            "gonvex-google-client",
		"sub":            "google-user-123",
		"email":          "USER@example.com",
		"email_verified": true,
		"name":           "Test User",
		"nonce":          "nonce-123",
		"iat":            time.Now().Add(-time.Minute).Unix(),
		"exp":            time.Now().Add(time.Hour).Unix(),
	}
	token := signTestGoogleJWT(t, privateKey, keyID, claims)
	identity, err := runtime.verifyGoogleIDToken(context.Background(), token, "nonce-123")
	if err != nil {
		t.Fatal(err)
	}
	if identity.Subject != "google-user-123" || identity.Email != "user@example.com" || !identity.EmailVerified {
		t.Fatalf("unexpected identity: %#v", identity)
	}
	if _, err := runtime.verifyGoogleIDToken(context.Background(), token, "different-nonce"); err == nil {
		t.Fatal("expected nonce mismatch to fail")
	}

	claims["aud"] = "another-client"
	wrongAudience := signTestGoogleJWT(t, privateKey, keyID, claims)
	if _, err := runtime.verifyGoogleIDToken(context.Background(), wrongAudience, "nonce-123"); err == nil {
		t.Fatal("expected audience mismatch to fail")
	}
}

func signTestGoogleJWT(t *testing.T, privateKey *rsa.PrivateKey, keyID string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": keyID, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := encodedHeader + "." + encodedPayload
	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}
