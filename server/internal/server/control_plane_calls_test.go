package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/dbpool"
)

func controlPlaneTestRuntime(t *testing.T) (*Server, *sql.DB, string) {
	t.Helper()
	baseURL := tenantRegistryTestPostgresURL(t)
	databaseURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_control_calls_"+tenantRegistryTestSuffix(t))
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := ensureProjectRegistry(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO gonvex_runtime_projects(id,name,environment,database_name,database_url,storage_bucket,status,description,owner_email) VALUES('shop','Shop','test','shop','', '', 'active','', 'owner@example.test')`); err != nil {
		t.Fatal(err)
	}
	runtime := New(config.Config{ControlPlaneURL: databaseURL, PostgresURL: baseURL, RequireAuth: true})
	t.Cleanup(runtime.Close)
	return runtime, db, "shop"
}

func TestControlPlaneTenantCreationResumesOneProvisioningWorkflow(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	if _, err := db.Exec(`UPDATE gonvex_runtime_projects SET database_mode='multiTenant' WHERE id=$1`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO accounts(id,auth_realm_id,email,name) VALUES('acct-creator',$1,'creator@example.test','Creator')`, project); err != nil {
		t.Fatal(err)
	}
	connection := &wsConn{server: runtime, project: project, user: &gonvex.Account{ID: "acct-creator", Email: "creator@example.test"}}
	args := json.RawMessage(`{"name":"Created once"}`)
	first, err := runtime.executeControlCall(context.Background(), connection, "reducer", "control.tenants.create", args, "create-command")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.executeControlCall(context.Background(), connection, "reducer", "control.tenants.create", args, "create-command")
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	var firstDecoded, secondDecoded any
	_ = json.Unmarshal(firstJSON, &firstDecoded)
	_ = json.Unmarshal(secondJSON, &secondDecoded)
	if !reflect.DeepEqual(firstDecoded, secondDecoded) {
		t.Fatalf("tenant creation retry changed result: first=%s second=%s", firstJSON, secondJSON)
	}
	firstTenant := first.(appAuthTenant)
	member, err := runtime.loadTenantMember(context.Background(), project, firstTenant.ID, "acct-creator")
	if err != nil || member.Status != "active" || member.Role != "owner" {
		t.Fatalf("created tenant membership is not authoritative: member=%#v err=%v", member, err)
	}
	var tenantCount int
	var databaseName string
	if err := db.QueryRow(`SELECT count(*),max(database_name) FROM gonvex_runtime_tenants WHERE project_id=$1`, project).Scan(&tenantCount, &databaseName); err != nil {
		t.Fatal(err)
	}
	if tenantCount != 1 {
		t.Fatalf("tenant retry created %d registry rows", tenantCount)
	}
	deleteConnection := &wsConn{server: runtime, project: project, tenant: firstTenant.ID, user: connection.user, member: member}
	if _, err := runtime.executeControlCall(context.Background(), deleteConnection, "reducer", "control.tenants.delete", json.RawMessage(`{}`), "delete-command"); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.loadTenantMember(context.Background(), project, firstTenant.ID, "acct-creator"); err == nil {
		t.Fatal("deleted tenant retained an active authoritative member")
	}
	if _, err := runtime.executeControlCall(context.Background(), deleteConnection, "reducer", "control.tenants.delete", json.RawMessage(`{}`), "delete-command"); err != nil {
		t.Fatalf("tenant deletion retry failed: %v", err)
	}
	t.Cleanup(func() {
		if databaseName != "" {
			_ = dropProjectDatabase(context.Background(), tenantRegistryTestPostgresURL(t), databaseName)
		}
	})
}

func TestTenantAdministratorCannotAdministerAnotherTenant(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	if _, err := db.Exec(`UPDATE gonvex_runtime_projects SET database_mode='multiTenant' WHERE id=$1`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO accounts(id,auth_realm_id,email,name) VALUES
		('acct-a',$1,'a@example.test','A'),('acct-b',$1,'b@example.test','B')`, project); err != nil {
		t.Fatal(err)
	}
	create := func(accountID, email, name, command string) appAuthTenant {
		t.Helper()
		result, err := runtime.executeControlCall(context.Background(), &wsConn{
			server: runtime, project: project, user: &gonvex.Account{ID: accountID, Email: email},
		}, "reducer", "control.tenants.create", json.RawMessage(`{"name":"`+name+`"}`), command)
		if err != nil {
			t.Fatal(err)
		}
		return result.(appAuthTenant)
	}
	tenantA := create("acct-a", "a@example.test", "Tenant A", "create-a")
	tenantB := create("acct-b", "b@example.test", "Tenant B", "create-b")
	hash, err := hashDashboardPassword("account-a-password")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,'acct-a',$2)`, project, hash); err != nil {
		t.Fatal(err)
	}
	login, err := runtime.executeControlCall(context.Background(), &wsConn{server: runtime, project: project}, "action", "control.auth.passwordLogin", json.RawMessage(`{"email":"a@example.test","password":"account-a-password"}`), "login-a")
	if err != nil {
		t.Fatal(err)
	}
	loginJSON, _ := json.Marshal(login)
	var grant struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(loginJSON, &grant); err != nil || grant.AccessToken == "" {
		t.Fatalf("login did not return a session: result=%s err=%v", loginJSON, err)
	}
	t.Cleanup(func() {
		for _, tenant := range []appAuthTenant{tenantA, tenantB} {
			var databaseName string
			if err := db.QueryRow(`SELECT database_name FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2`, project, tenant.ID).Scan(&databaseName); err == nil && databaseName != "" {
				_ = dropProjectDatabase(context.Background(), tenantRegistryTestPostgresURL(t), databaseName)
			}
		}
	})
	connection := &wsConn{
		server: runtime, project: project, tenant: tenantB.ID,
		user: &gonvex.Account{ID: "acct-a", Email: "a@example.test"},
		// A stale or browser-derived role must not authorize the call. The host
		// reloads membership from tenant B, where account A has no membership.
		member: &gonvex.Member{ID: "forged", AccountID: "acct-a", Role: "owner", Status: "active"},
		auth:   true, authToken: grant.AccessToken,
	}
	if err := connection.authorizeControlCall(context.Background(), controlTenantAdmin); err == nil {
		t.Fatal("tenant A owner administered tenant B")
	}
}

func TestControlPlaneProjectAdminComesFromControlPlaneMembership(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	ctx := context.Background()
	if runtime.canManageControlProject(ctx, project, "normal@example.test") {
		t.Fatal("normal account gained project administration")
	}
	if !runtime.canManageControlProject(ctx, project, "owner@example.test") {
		t.Fatal("project owner was denied")
	}
	if _, err := db.Exec(`INSERT INTO gonvex_project_members(project_id,email,name,role) VALUES($1,'admin@example.test','Admin','admin')`, project); err != nil {
		t.Fatal(err)
	}
	if !runtime.canManageControlProject(ctx, project, "admin@example.test") {
		t.Fatal("project admin membership was denied")
	}
}

func TestTenantMembershipRevocationRestrictsExistingConnection(t *testing.T) {
	runtime := New(config.Config{})
	t.Cleanup(runtime.Close)
	connection := &wsConn{
		server: runtime, project: "shop", tenant: "tenant-a", auth: true, authToken: "gvx_session_active",
		user:   &gonvex.Account{ID: "acct-revoked"},
		member: &gonvex.Member{ID: "mem-revoked", AccountID: "acct-revoked", Status: "active"},
		subs:   map[string]querySubscription{}, replicas: map[string]*replicaSubscription{},
	}
	runtime.addWSConn(connection)
	t.Cleanup(func() { runtime.removeWSConn(connection) })

	runtime.revokeAppAuthConnections("shop", "acct-revoked")
	connection.mu.Lock()
	authenticated, account, member, replicaScope, visibilityScope := connection.auth, connection.user, connection.member, connection.replicaScope, connection.visibilityScope
	connection.mu.Unlock()
	if authenticated || account != nil || member != nil || replicaScope != "" || visibilityScope != "" {
		t.Fatalf("revoked connection retained tenant authority: auth=%v account=%#v member=%#v replicaScope=%q visibilityScope=%q", authenticated, account, member, replicaScope, visibilityScope)
	}
}

func TestControlPlanePublicAuthSettingsExposeOnlyPublicFields(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	if _, err := db.Exec(`INSERT INTO gonvex_auth_providers(project_id,provider,enabled,signup_mode) VALUES($1,'google',TRUE,'invite')`, project); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.executeControlQuery(context.Background(), &wsConn{server: runtime, project: project}, "control.auth.publicSettings", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(result)
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 1 || fields["providers"] == nil {
		t.Fatalf("public auth settings leaked non-public fields: %s", raw)
	}
}

func TestAuthenticatedControlSocketDoesNotRequireProjectApplicationDatabase(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	hash, err := hashDashboardPassword("correct-password-123")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO accounts(id,auth_realm_id,email,name) VALUES('acct-control',$1,'control@example.test','Control')`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,'acct-control',$2)`, project, hash); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.executeControlCall(context.Background(), &wsConn{server: runtime, project: project}, "action", "control.auth.passwordLogin", json.RawMessage(`{"email":"control@example.test","password":"correct-password-123"}`), "control-login")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(result)
	var grant struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(raw, &grant); err != nil || grant.AccessToken == "" {
		t.Fatalf("login did not return an access token: result=%s err=%v", raw, err)
	}
	account, authenticatedProject, err := runtime.authenticateControlSocket(context.Background(), project, grant.AccessToken)
	if err != nil {
		t.Fatalf("Control Plane auth incorrectly required an application database: %v", err)
	}
	if account.ID != "acct-control" || authenticatedProject != project {
		t.Fatalf("unexpected Control Plane identity: account=%#v project=%q", account, authenticatedProject)
	}
}

func TestControlPlaneRejectsBrowserDatabaseSelectors(t *testing.T) {
	runtime, _, project := controlPlaneTestRuntime(t)
	connection := &wsConn{server: runtime, project: project, user: &gonvex.Account{ID: "acct-one"}}
	if _, err := runtime.executeControlQuery(context.Background(), connection, "control.accounts.me", json.RawMessage(`{"databaseUrl":"postgres://attacker.example/other"}`)); err == nil {
		t.Fatal("Control Plane call accepted an undeclared browser database selector")
	}
}

func TestRejectedCredentialFallsBackOnlyToPublicControlPlaneScope(t *testing.T) {
	runtime, _, project := controlPlaneTestRuntime(t)
	connection := &wsConn{server: runtime, id: "refresh-connection", subs: map[string]querySubscription{}, replicas: map[string]*replicaSubscription{}}
	connection.handle(context.Background(), clientMessage{
		Type: "auth", ID: "auth-expired", Project: project, Token: "expired-token", ControlOnly: true,
	})
	if connection.project != project || !connection.controlOnly || connection.auth || connection.user != nil || connection.tenant != "" {
		t.Fatalf("rejected credential retained more than public project scope: project=%q tenant=%q controlOnly=%v auth=%v user=%#v", connection.project, connection.tenant, connection.controlOnly, connection.auth, connection.user)
	}
	if err := connection.authorizeControlCall(context.Background(), controlPublic); err != nil {
		t.Fatalf("public refresh call was denied after credential rejection: %v", err)
	}
	if err := connection.authorizeControlCall(context.Background(), controlAccount); err == nil {
		t.Fatal("rejected credential retained account authority")
	}
}

func TestControlPlaneReducerIdempotencyReturnsOneCommittedResult(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	connection := &wsConn{server: runtime, project: project, user: &gonvex.Account{ID: "acct-owner", Email: "owner@example.test"}}
	args := json.RawMessage(`{"permissions":["tasks:read"],"expiresInSeconds":300}`)
	first, err := runtime.executeControlCall(context.Background(), connection, "reducer", "control.agentAuth.issue", args, "same-command")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.executeControlCall(context.Background(), connection, "reducer", "control.agentAuth.issue", args, "same-command")
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("idempotent result changed: first=%s second=%s", firstJSON, secondJSON)
	}
	var claims int
	if err := db.QueryRow(`SELECT count(*) FROM gonvex_agent_claim_tokens WHERE project_id=$1`, project).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 1 {
		t.Fatalf("idempotent reducer created %d claim tokens", claims)
	}
}

func TestNativeLoginAndRefreshRotationAreTransactionallyIdempotent(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	hash, err := hashDashboardPassword("correct-password-123")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO accounts(id,auth_realm_id,email,name) VALUES('acct-login',$1,'login@example.test','Login')`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,'acct-login',$2)`, project, hash); err != nil {
		t.Fatal(err)
	}
	connection := &wsConn{server: runtime, project: project}
	loginArgs := json.RawMessage(`{"email":"login@example.test","password":"correct-password-123"}`)
	first, err := runtime.executeControlCall(context.Background(), connection, "action", "control.auth.passwordLogin", loginArgs, "login-command")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.executeControlCall(context.Background(), connection, "action", "control.auth.passwordLogin", loginArgs, "login-command")
	if err != nil {
		t.Fatal(err)
	}
	firstRaw, _ := json.Marshal(first)
	secondRaw, _ := json.Marshal(second)
	if !bytes.Equal(firstRaw, secondRaw) {
		t.Fatalf("login retry minted a different session: first=%s second=%s", firstRaw, secondRaw)
	}
	var login struct {
		RefreshToken string `json:"refreshToken"`
	}
	if err := json.Unmarshal(firstRaw, &login); err != nil || login.RefreshToken == "" {
		t.Fatalf("login result did not contain a refresh token: result=%s err=%v", firstRaw, err)
	}
	refreshArgs, _ := json.Marshal(map[string]string{"refreshToken": login.RefreshToken})
	rotated, err := runtime.executeControlCall(context.Background(), connection, "action", "control.auth.refreshSession", refreshArgs, "refresh-command")
	if err != nil {
		t.Fatal(err)
	}
	rotatedRetry, err := runtime.executeControlCall(context.Background(), connection, "action", "control.auth.refreshSession", refreshArgs, "refresh-command")
	if err != nil {
		t.Fatal(err)
	}
	rotatedRaw, _ := json.Marshal(rotated)
	rotatedRetryRaw, _ := json.Marshal(rotatedRetry)
	if !bytes.Equal(rotatedRaw, rotatedRetryRaw) {
		t.Fatalf("refresh retry minted a different session: first=%s second=%s", rotatedRaw, rotatedRetryRaw)
	}
	var refreshTokens int
	if err := db.QueryRow(`SELECT count(*) FROM gonvex_auth_refresh_tokens WHERE project_id=$1 AND account_id='acct-login'`, project).Scan(&refreshTokens); err != nil {
		t.Fatal(err)
	}
	if refreshTokens != 2 {
		t.Fatalf("login and one rotation created %d refresh tokens", refreshTokens)
	}
}

func TestMemberLoginProvisioningCannotResetExistingAccountPassword(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	accountID := "acct-existing"
	originalHash, err := hashDashboardPassword("original-password-123")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO accounts(id,auth_realm_id,email,name) VALUES($1,$2,'victim@example.test','Victim')`, accountID, project); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,$2,$3)`, project, accountID, originalHash); err != nil {
		t.Fatal(err)
	}
	connection := &wsConn{
		server:  runtime,
		project: project,
		tenant:  "tenant-admins",
		user:    &gonvex.Account{ID: "acct-admin", Email: "admin@example.test"},
		member:  &gonvex.Member{ID: "member-admin", AccountID: "acct-admin", Role: "owner"},
	}
	_, err = runtime.executeControlReducer(context.Background(), connection, "control.accounts.provisionMemberLogin", json.RawMessage(`{
		"email":"victim@example.test","name":"Renamed","password":"attacker-password-123","role":"member","permissions":{}
	}`))
	if err == nil {
		t.Fatal("tenant administrator reset an existing global account password")
	}
	var storedHash string
	if err := db.QueryRow(`SELECT password_hash FROM gonvex_account_passwords WHERE project_id=$1 AND account_id=$2`, project, accountID).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	if storedHash != originalHash || verifyDashboardPassword("attacker-password-123", storedHash) {
		t.Fatal("existing account password changed during rejected provisioning")
	}
}

func TestDeveloperImpersonationIsPermissionGatedAuditedAndSingleUse(t *testing.T) {
	runtime, controlDB, project := controlPlaneTestRuntime(t)
	tenantID := "tenant-support"
	tenantURL := createTenantRegistryTestDatabase(t, tenantRegistryTestPostgresURL(t), "gonvex_impersonation_"+tenantRegistryTestSuffix(t))
	tenantDB, err := dbpool.Open(tenantURL)
	if err != nil {
		t.Fatal(err)
	}
	defer tenantDB.Close()
	if err := ensureTenantLocalTables(context.Background(), tenantDB); err != nil {
		t.Fatal(err)
	}
	if _, err := tenantDB.Exec(`INSERT INTO members(id,account_id,status,role,permissions) VALUES('member-target','acct-target','active','member','{}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := controlDB.Exec(`UPDATE gonvex_runtime_projects SET database_mode='multiTenant' WHERE id=$1`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := controlDB.Exec(`INSERT INTO accounts(id,auth_realm_id,email,name) VALUES('acct-owner',$1,'owner@example.test','Owner'),('acct-target',$1,'target@example.test','Target')`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := controlDB.Exec(`INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status,provisioned)
		VALUES('rel-support',$1,$2,'Support tenant',$3,'active',TRUE)`, project, tenantID, tenantURL); err != nil {
		t.Fatal(err)
	}
	runtime.hydrateProjects()

	normal := &wsConn{server: runtime, project: project, user: &gonvex.Account{ID: "acct-normal", Email: "normal@example.test"}}
	if err := normal.authorizeControlCall(context.Background(), controlProjectAdmin); err == nil {
		t.Fatal("normal account received project administration")
	}
	owner := &wsConn{server: runtime, project: project, user: &gonvex.Account{ID: "acct-owner", Email: "owner@example.test"}}
	result, err := runtime.executeControlReducer(context.Background(), owner, "control.support.createImpersonation", json.RawMessage(`{
		"accountId":"acct-target","tenantId":"tenant-support","reason":"ticket support-42"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	grant := result.(map[string]any)
	token := grant["token"].(string)
	account, _, authenticatedProject, authenticatedTenant, grantID, actorID, err := runtime.authenticateImpersonationSocket(context.Background(), project, token, "conn-support")
	if err != nil {
		t.Fatal(err)
	}
	if account.ID != "acct-target" || authenticatedProject != project || authenticatedTenant != tenantID || actorID != "acct-owner" {
		t.Fatalf("unexpected impersonation identity: account=%#v project=%q tenant=%q actor=%q", account, authenticatedProject, authenticatedTenant, actorID)
	}
	var reason, auditedActor, usedConnection string
	if err := controlDB.QueryRow(`SELECT reason,actor_account_id,used_connection_id FROM gonvex_impersonation_grants WHERE id=$1`, grantID).Scan(&reason, &auditedActor, &usedConnection); err != nil {
		t.Fatal(err)
	}
	if reason != "ticket support-42" || auditedActor != "acct-owner" || usedConnection != "conn-support" {
		t.Fatalf("impersonation audit mismatch: reason=%q actor=%q connection=%q", reason, auditedActor, usedConnection)
	}
	if _, _, _, _, _, _, err := runtime.authenticateImpersonationSocket(context.Background(), project, token, "conn-replay"); err == nil {
		t.Fatal("single-use impersonation token was replayed")
	}
}

func TestInvitationAcceptanceResumesOnlyTheSameCommandAndRejectsReplay(t *testing.T) {
	runtime, controlDB, project := controlPlaneTestRuntime(t)
	tenantID := "tenant-invite"
	tenantURL := createTenantRegistryTestDatabase(t, tenantRegistryTestPostgresURL(t), "gonvex_invitation_"+tenantRegistryTestSuffix(t))
	tenantDB, err := dbpool.Open(tenantURL)
	if err != nil {
		t.Fatal(err)
	}
	defer tenantDB.Close()
	if err := ensureTenantLocalTables(context.Background(), tenantDB); err != nil {
		t.Fatal(err)
	}
	if _, err := controlDB.Exec(`UPDATE gonvex_runtime_projects SET database_mode='multiTenant' WHERE id=$1`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := controlDB.Exec(`INSERT INTO accounts(id,auth_realm_id,email,name) VALUES('acct-invited',$1,'invited@example.test','Invited')`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := controlDB.Exec(`INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status,provisioned)
		VALUES('rel-invite',$1,$2,'Invitation tenant',$3,'active',TRUE)`, project, tenantID, tenantURL); err != nil {
		t.Fatal(err)
	}
	const token = "invitation-secret"
	if _, err := controlDB.Exec(`INSERT INTO gonvex_auth_membership_invitations
		(project_id,tenant_id,email,role,permissions,invited_by,expires_at,id,token_hash)
		VALUES($1,$2,'invited@example.test','member','{}','acct-owner',now()+interval '1 hour','invite-one',$3)`, project, tenantID, sha256Hex(token)); err != nil {
		t.Fatal(err)
	}
	runtime.hydrateProjects()
	connection := &wsConn{server: runtime, project: project, user: &gonvex.Account{ID: "acct-invited", Email: "invited@example.test"}}
	args := json.RawMessage(`{"token":"invitation-secret"}`)
	first, err := runtime.acceptControlInvitation(context.Background(), controlDB, connection, args, "accept-command")
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := runtime.acceptControlInvitation(context.Background(), controlDB, connection, args, "accept-command")
	if err != nil {
		t.Fatalf("same-command recovery failed: %v", err)
	}
	firstJSON, _ := json.Marshal(first)
	resumedJSON, _ := json.Marshal(resumed)
	if string(firstJSON) != string(resumedJSON) {
		t.Fatalf("resumed invitation result changed: first=%s resumed=%s", firstJSON, resumedJSON)
	}
	if _, err := runtime.acceptControlInvitation(context.Background(), controlDB, connection, args, "replay-command"); err == nil {
		t.Fatal("accepted invitation token was replayed by a different command")
	}
	member, err := runtime.loadTenantMember(context.Background(), project, tenantID, "acct-invited")
	if err != nil || member.Status != "active" {
		t.Fatalf("tenant membership was not authoritative after invitation acceptance: member=%#v err=%v", member, err)
	}
}

func TestControlPlaneInvitationLookupRejectsExpiredRevokedAndAcceptedTokens(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	if _, err := db.Exec(`INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,status) VALUES('rel',$1,'tenant-a','Tenant A','active')`, project); err != nil {
		t.Fatal(err)
	}
	insert := func(email, token string, expires time.Time, revoked, accepted bool) {
		var revokedAt any
		if revoked {
			revokedAt = time.Now()
		}
		var acceptedAt any
		if accepted {
			acceptedAt = time.Now()
		}
		if _, err := db.Exec(`INSERT INTO gonvex_auth_membership_invitations(project_id,tenant_id,email,role,expires_at,id,token_hash,revoked_at,accepted_at) VALUES($1,'tenant-a',$2,'member',$3,$4,$5,$6,$7)`, project, email, expires, email, sha256Hex(token), revokedAt, acceptedAt); err != nil {
			t.Fatal(err)
		}
	}
	insert("expired@example.test", "expired", time.Now().Add(-time.Hour), false, false)
	insert("revoked@example.test", "revoked", time.Now().Add(time.Hour), true, false)
	insert("accepted@example.test", "accepted", time.Now().Add(time.Hour), false, true)
	connection := &wsConn{server: runtime, project: project}
	for _, token := range []string{"expired", "revoked", "accepted"} {
		if _, err := runtime.executeControlQuery(context.Background(), connection, "control.invitations.lookup", json.RawMessage(`{"token":"`+token+`"}`)); err == nil {
			t.Fatalf("%s invitation was accepted", token)
		}
	}
}

func TestNativeTelemetryOverwritesSpoofedAttribution(t *testing.T) {
	runtime, _, project := controlPlaneTestRuntime(t)
	connection := &wsConn{server: runtime, id: "conn-real", project: project, tenant: "tenant-real", auth: true, user: &gonvex.Account{ID: "acct-real"}}
	event := capturedError{EventID: "event-1", Message: "boom", Project: "spoof", Tenant: "tenant-spoof", SessionID: "session-spoof", Account: map[string]any{"id": "acct-spoof"}}
	raw, _ := json.Marshal(event)
	connection.handleNativeErrorTelemetry(context.Background(), clientMessage{Type: "error.envelope", ID: "envelope-1", Events: []json.RawMessage{raw}})
	groups, _, available, err := runtime.persistentErrorGroups(context.Background(), project, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if available {
		if len(groups) != 1 {
			t.Fatalf("persisted error groups=%d", len(groups))
		}
		captured := groups[0].Latest
		if captured.Project != project || captured.Tenant != "tenant-real" || captured.SessionID != "conn-real" || captured.Account["id"] != "acct-real" {
			t.Fatalf("spoofed attribution survived: %#v", captured)
		}
		return
	}
	runtime.errorTracker.mu.RLock()
	defer runtime.errorTracker.mu.RUnlock()
	if len(runtime.errorTracker.eventLog) != 1 {
		t.Fatalf("captured events=%d", len(runtime.errorTracker.eventLog))
	}
	captured := runtime.errorTracker.eventLog[0]
	if captured.Project != project || captured.Tenant != "tenant-real" || captured.SessionID != "conn-real" || captured.Account["id"] != "acct-real" {
		t.Fatalf("spoofed attribution survived: %#v", captured)
	}
}

func TestNativeTelemetryRejectsOversizedBatchesBeforeCapture(t *testing.T) {
	runtime, _, project := controlPlaneTestRuntime(t)
	connection := &wsConn{server: runtime, id: "conn-bounded", remoteIP: "203.0.113.4", project: project}
	events := make([]json.RawMessage, 21)
	for index := range events {
		events[index] = json.RawMessage(`{"eventId":"event","message":"boom"}`)
	}
	connection.handleNativeErrorTelemetry(context.Background(), clientMessage{Type: "error.envelope", ID: "too-many", Events: events})
	runtime.errorTracker.mu.RLock()
	defer runtime.errorTracker.mu.RUnlock()
	if len(runtime.errorTracker.eventLog) != 0 {
		t.Fatalf("oversized native envelope captured %d events", len(runtime.errorTracker.eventLog))
	}
}

func TestNativeTelemetryRateLimitIsBoundedPerSubject(t *testing.T) {
	tracker := newErrorTracker(100)
	now := time.Now()
	for index := 0; index < 3; index++ {
		if !tracker.allowLimit("project:subject", now, 3) {
			t.Fatalf("event %d was rejected before the configured limit", index)
		}
	}
	if tracker.allowLimit("project:subject", now, 3) {
		t.Fatal("telemetry subject exceeded its configured rate limit")
	}
	if !tracker.allowLimit("project:other", now, 3) {
		t.Fatal("one telemetry subject consumed another subject's allowance")
	}
}

func TestNativeTelemetryHeartbeatPreservesRegisteredRelease(t *testing.T) {
	runtime, db, project := controlPlaneTestRuntime(t)
	connection := &wsConn{server: runtime, id: "conn-release", project: project, tenant: "tenant-a", auth: true, user: &gonvex.Account{ID: "acct-one"}}
	connection.handleNativeErrorTelemetry(context.Background(), clientMessage{Type: "error.register", ID: "register", Release: "2.4.1", Environment: "production"})
	connection.handleNativeErrorTelemetry(context.Background(), clientMessage{Type: "error.heartbeat", ID: "heartbeat"})
	var release, environment string
	if err := db.QueryRow(`SELECT release,environment FROM gonvex_support_sessions WHERE id='conn-release'`).Scan(&release, &environment); err != nil {
		t.Fatal(err)
	}
	if release != "2.4.1" || environment != "production" {
		t.Fatalf("heartbeat erased registration metadata: release=%q environment=%q", release, environment)
	}
}
