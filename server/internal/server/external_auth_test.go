package server

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/dbpool"
)

func externalAuthTestKey(t *testing.T) (*rsa.PrivateKey, string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	kid := "firebase-test-key"
	document := map[string]any{"keys": []any{map[string]any{
		"kty": "RSA", "kid": kid, "alg": "RS256",
		"n": base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
	}}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(response).Encode(document)
	}))
	t.Cleanup(server.Close)
	// The production verifier uses the default HTTP client. Trust this test CA
	// only for this test and restore the process-global transport afterwards.
	previous := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = previous })
	return key, kid, server.URL
}

func signExternalAuthToken(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": kid, "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func firebaseClaims(now time.Time) map[string]any {
	return map[string]any{
		"iss": "https://securetoken.google.com/whagons-prod", "aud": "whagons-prod", "sub": "firebase-uid-1",
		"email": "person@example.test", "email_verified": true, "name": "Person",
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(), "auth_time": now.Add(-time.Minute).Unix(),
		"firebase": map[string]any{"sign_in_provider": "google.com", "tenant": "firebase-tenant"},
	}
}

func TestFirebaseIdentityProviderValidatesRequiredClaims(t *testing.T) {
	key, kid, jwksURL := externalAuthTestKey(t)
	now := time.Now().UTC()
	configuration := externalAuthConfiguration{
		Provider: "firebase", Issuer: "https://securetoken.google.com/whagons-prod",
		Audience: "whagons-prod", JWKSURL: jwksURL, FirebaseTenant: "firebase-tenant",
	}
	provider := signedJWTIdentityProvider{firebase: true}
	identity, err := provider.Verify(context.Background(), configuration, signExternalAuthToken(t, key, kid, firebaseClaims(now)))
	if err != nil {
		t.Fatal(err)
	}
	if identity.Subject != "firebase-uid-1" || identity.Provider != "firebase" || !identity.EmailVerified {
		t.Fatalf("unexpected identity: %#v", identity)
	}

	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "wrong issuer", mutate: func(claims map[string]any) { claims["iss"] = "https://attacker.invalid" }},
		{name: "wrong audience", mutate: func(claims map[string]any) { claims["aud"] = "another-project" }},
		{name: "expired", mutate: func(claims map[string]any) { claims["exp"] = now.Add(-time.Second).Unix() }},
		{name: "missing subject", mutate: func(claims map[string]any) { delete(claims, "sub") }},
		{name: "missing auth time", mutate: func(claims map[string]any) { delete(claims, "auth_time") }},
		{name: "custom token shape", mutate: func(claims map[string]any) { delete(claims, "firebase") }},
		{name: "tenant mismatch", mutate: func(claims map[string]any) {
			claims["firebase"] = map[string]any{"sign_in_provider": "google.com", "tenant": "wrong"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claims := firebaseClaims(now)
			test.mutate(claims)
			if _, err := provider.Verify(context.Background(), configuration, signExternalAuthToken(t, key, kid, claims)); err == nil {
				t.Fatal("invalid token was accepted")
			}
		})
	}
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Verify(context.Background(), configuration, signExternalAuthToken(t, otherKey, kid, firebaseClaims(now))); err == nil {
		t.Fatal("invalid signature was accepted")
	}
	configuration.AdminSecret = `{"project_id":"whagons-prod"}`
	policyCalled := false
	provider.accountPolicy = func(_ context.Context, _ externalAuthConfiguration, subject string, authTime time.Time) error {
		policyCalled = true
		if subject != "firebase-uid-1" || authTime.IsZero() {
			t.Fatalf("Firebase policy received subject=%q authTime=%v", subject, authTime)
		}
		return fmt.Errorf("Firebase account is disabled")
	}
	if _, err := provider.Verify(context.Background(), configuration, signExternalAuthToken(t, key, kid, firebaseClaims(now))); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("Firebase Admin account policy was not enforced: %v", err)
	}
	if !policyCalled {
		t.Fatal("Firebase Admin account policy was not called")
	}
}

func TestFirebaseAdminCredentialsAreProjectBoundAndUseTheGoogleTokenEndpoint(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	credentials := map[string]any{
		"project_id": "whagons-prod", "client_email": "firebase-admin@whagons-prod.iam.gserviceaccount.com",
		"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})),
		"token_uri":   "https://oauth2.googleapis.com/token",
	}
	raw, _ := json.Marshal(credentials)
	if _, err := parseFirebaseAdminCredentials(string(raw), "whagons-prod"); err != nil {
		t.Fatal(err)
	}
	if _, err := parseFirebaseAdminCredentials(string(raw), "another-project"); err == nil {
		t.Fatal("Firebase Admin credentials crossed project scope")
	}
	credentials["token_uri"] = "https://attacker.example/token"
	raw, _ = json.Marshal(credentials)
	if _, err := parseFirebaseAdminCredentials(string(raw), "whagons-prod"); err == nil {
		t.Fatal("Firebase Admin credentials accepted a non-Google token endpoint")
	}
}

func TestConcurrentFirebaseIdentityResolutionCreatesOneAccount(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	identity := verifiedExternalIdentity{
		Provider: "firebase", Issuer: "https://securetoken.google.com/whagons-prod", Subject: "same-firebase-uid",
		Email: "same@example.test", EmailVerified: true, Name: "Same Person",
	}
	const workers = 8
	ids := make(chan string, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				errs <- err
				return
			}
			defer tx.Rollback()
			account, err := runtime.resolveExternalAccountTx(context.Background(), tx, project, identity)
			if err == nil {
				err = tx.Commit()
			}
			if err != nil {
				errs <- err
				return
			}
			ids <- account.ID
		}()
	}
	group.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	if len(unique) != 1 {
		t.Fatalf("concurrent resolution produced accounts %v", unique)
	}
	var accountCount, identityCount int
	if err := db.QueryRow(`SELECT count(*) FROM accounts WHERE auth_realm_id=$1`, project).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM account_identities WHERE project_id=$1 AND provider='firebase'`, project).Scan(&identityCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != 1 || identityCount != 1 {
		t.Fatal(fmt.Sprintf("account=%d identity=%d", accountCount, identityCount))
	}
}

func TestPostgresFirebaseExchangeCreatesCanonicalSessionBeforeTenantSelection(t *testing.T) {
	key, kid, jwksURL := externalAuthTestKey(t)
	runtime, db, project := controlPlaneTestRuntime(t)
	if _, err := db.Exec(`UPDATE gonvex_runtime_projects SET auth_mode='firebase' WHERE id=$1`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO gonvex_auth_providers(project_id,provider,enabled,signup_mode,issuer,audience,jwks_url,firebase_project_id,firebase_tenant_id)
		VALUES($1,'firebase',TRUE,'inviteOnly',$2,$3,$4,$3,'firebase-tenant')`, project,
		"https://securetoken.google.com/whagons-prod", "whagons-prod", jwksURL); err != nil {
		t.Fatal(err)
	}
	token := signExternalAuthToken(t, key, kid, firebaseClaims(time.Now().UTC()))
	connection := &wsConn{server: runtime, project: project}
	result, err := runtime.executeControlCall(context.Background(), connection, "action", "control.auth.exchangeExternalToken",
		json.RawMessage(fmt.Sprintf(`{"provider":"firebase","token":%q}`, token)), "firebase-login-1")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(result)
	var session struct {
		AccessToken  string          `json:"accessToken"`
		RefreshToken string          `json:"refreshToken"`
		Account      appAuthAccount  `json:"account"`
		Tenants      []appAuthTenant `json:"tenants"`
	}
	if err := json.Unmarshal(raw, &session); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(session.AccessToken, "gvx_session_") || session.Account.Provider != "firebase" || session.Account.ID == "" || len(session.Tenants) != 0 {
		t.Fatalf("unexpected pre-tenant session: %s", raw)
	}
	validated, err := runtime.loadAppSessionIdentity(context.Background(), session.AccessToken)
	if err != nil || validated.Account.ID != session.Account.ID || validated.ProjectID != project {
		t.Fatalf("canonical session validation = %#v, %v", validated, err)
	}

	tenantAURL := createTenantRegistryTestDatabase(t, tenantRegistryTestPostgresURL(t), "gonvex_firebase_tenant_a_"+tenantRegistryTestSuffix(t))
	tenantBURL := createTenantRegistryTestDatabase(t, tenantRegistryTestPostgresURL(t), "gonvex_firebase_tenant_b_"+tenantRegistryTestSuffix(t))
	for _, tenantURL := range []string{tenantAURL, tenantBURL} {
		tenantDB, openErr := dbpool.Open(tenantURL)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if ensureErr := ensureTenantLocalTables(context.Background(), tenantDB); ensureErr != nil {
			tenantDB.Close()
			t.Fatal(ensureErr)
		}
		tenantDB.Close()
	}
	tenantADB, err := dbpool.Open(tenantAURL)
	if err != nil {
		t.Fatal(err)
	}
	defer tenantADB.Close()
	if _, err := tenantADB.Exec(`INSERT INTO members(id,account_id,status,role,permissions) VALUES('member-firebase',$1,'active','member','{}')`, session.Account.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE gonvex_runtime_projects SET database_mode='multiTenant' WHERE id=$1`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status,provisioned) VALUES
		('firebase-rel-a',$1,'firebase-tenant-a','Firebase A',$2,'active',TRUE),
		('firebase-rel-b',$1,'firebase-tenant-b','Firebase B',$3,'active',TRUE)`, project, tenantAURL, tenantBURL); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO account_tenant_index(account_id,tenant_id,member_id,status) VALUES
		($1,'firebase-tenant-a','member-firebase','active'),
		($1,'firebase-tenant-b','stale-member','active')`, session.Account.ID); err != nil {
		t.Fatal(err)
	}
	runtime.hydrateProjects()
	runtime.hydrateProjectTenantDatabasesUncached(context.Background(), project)
	if _, tenantID, err := runtime.validateAppSession(context.Background(), project, session.AccessToken, "firebase-tenant-a"); err != nil || tenantID != "firebase-tenant-a" {
		t.Fatalf("active Firebase Member was denied: tenant=%q err=%v", tenantID, err)
	}
	if _, _, err := runtime.validateAppSession(context.Background(), project, session.AccessToken, "firebase-tenant-b"); err == nil {
		t.Fatal("tenant A Firebase membership granted tenant B access")
	}
	if _, err := tenantADB.Exec(`UPDATE members SET status='revoked' WHERE account_id=$1`, session.Account.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtime.validateAppSession(context.Background(), project, session.AccessToken, "firebase-tenant-a"); err == nil {
		t.Fatal("stale directory entry bypassed revoked Firebase Member authority")
	}

	// Firebase provider metadata changes do not change the Firebase UID subject.
	claims := firebaseClaims(time.Now().UTC())
	claims["firebase"] = map[string]any{"sign_in_provider": "microsoft.com", "tenant": "firebase-tenant"}
	secondToken := signExternalAuthToken(t, key, kid, claims)
	if _, err := runtime.executeControlCall(context.Background(), connection, "action", "control.auth.exchangeExternalToken",
		json.RawMessage(fmt.Sprintf(`{"provider":"firebase","token":%q,"previousRefreshToken":%q}`, secondToken, session.RefreshToken)), "firebase-login-2"); err != nil {
		t.Fatal(err)
	}
	var accountCount, identityCount, activeRefreshFamilies int
	if err := db.QueryRow(`SELECT count(*) FROM accounts WHERE auth_realm_id=$1`, project).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM account_identities WHERE project_id=$1 AND provider='firebase'`, project).Scan(&identityCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(DISTINCT family_id) FROM gonvex_auth_refresh_tokens WHERE project_id=$1 AND account_id=$2 AND revoked_at IS NULL`, project, session.Account.ID).Scan(&activeRefreshFamilies); err != nil {
		t.Fatal(err)
	}
	if accountCount != 1 || identityCount != 1 {
		t.Fatalf("linked Firebase provider created duplicates: accounts=%d identities=%d", accountCount, identityCount)
	}
	if activeRefreshFamilies != 1 {
		t.Fatalf("Firebase token rotation left %d active Gonvex session families", activeRefreshFamilies)
	}
}
