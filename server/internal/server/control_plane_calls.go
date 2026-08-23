package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type controlAuthorization string

const (
	controlPublic       controlAuthorization = "public"
	controlAccount      controlAuthorization = "account"
	controlTenantAdmin  controlAuthorization = "tenantAdmin"
	controlProjectAdmin controlAuthorization = "projectAdmin"
)

type controlFunction struct {
	kind string
	auth controlAuthorization
}

type controlStore interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func concreteControlDB(store controlStore) (*sql.DB, error) {
	db, ok := store.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("Control Plane operation cannot open a nested transaction")
	}
	return db, nil
}

func concreteControlTx(store controlStore) (*sql.Tx, error) {
	tx, ok := store.(*sql.Tx)
	if !ok {
		return nil, fmt.Errorf("Control Plane operation requires its invocation transaction")
	}
	return tx, nil
}

type controlCommitError struct {
	err   error
	after func()
}

func (err *controlCommitError) Error() string { return err.err.Error() }

var controlFunctions = map[string]controlFunction{
	"control.accounts.me":                   {kind: "query", auth: controlAccount},
	"control.accounts.updatePassword":       {kind: "reducer", auth: controlAccount},
	"control.accounts.provisionMemberLogin": {kind: "reducer", auth: controlTenantAdmin},
	"control.auth.passwordLogin":            {kind: "action", auth: controlPublic},
	"control.auth.refreshSession":           {kind: "action", auth: controlPublic},
	"control.auth.logout":                   {kind: "reducer", auth: controlAccount},
	"control.auth.publicSettings":           {kind: "query", auth: controlPublic},
	"control.auth.realms.list":              {kind: "query", auth: controlProjectAdmin},
	"control.auth.realms.configure":         {kind: "reducer", auth: controlProjectAdmin},
	"control.tenants.mine":                  {kind: "query", auth: controlAccount},
	"control.tenants.create":                {kind: "reducer", auth: controlAccount},
	"control.tenants.getByDomain":           {kind: "query", auth: controlPublic},
	"control.tenants.updateProfile":         {kind: "reducer", auth: controlTenantAdmin},
	"control.tenants.updateTimezone":        {kind: "reducer", auth: controlTenantAdmin},
	"control.tenants.delete":                {kind: "reducer", auth: controlTenantAdmin},
	"control.tenants.setException":          {kind: "reducer", auth: controlProjectAdmin},
	"control.tenants.setSeatLimit":          {kind: "reducer", auth: controlProjectAdmin},
	"control.invitations.lookup":            {kind: "query", auth: controlPublic},
	"control.invitations.create":            {kind: "reducer", auth: controlTenantAdmin},
	"control.invitations.accept":            {kind: "reducer", auth: controlAccount},
	"control.invitations.revoke":            {kind: "reducer", auth: controlTenantAdmin},
	"control.agentAuth.issue":               {kind: "reducer", auth: controlProjectAdmin},
	"control.agentAuth.claim":               {kind: "reducer", auth: controlAccount},
	"control.agentAuth.revoke":              {kind: "reducer", auth: controlProjectAdmin},
	"control.project.developers.list":       {kind: "query", auth: controlProjectAdmin},
	"control.project.developers.invite":     {kind: "reducer", auth: controlProjectAdmin},
	"control.project.developers.remove":     {kind: "reducer", auth: controlProjectAdmin},
	"control.assistant.getDefaults":         {kind: "query", auth: controlProjectAdmin},
	"control.assistant.setDefaults":         {kind: "reducer", auth: controlProjectAdmin},
	"control.voice.getConfiguration":        {kind: "query", auth: controlProjectAdmin},
	"control.voice.setRateCard":             {kind: "reducer", auth: controlProjectAdmin},
	"control.voice.setTenantEntitlement":    {kind: "reducer", auth: controlProjectAdmin},
	"control.voice.setUserOverride":         {kind: "reducer", auth: controlProjectAdmin},
	"control.support.listSessions":          {kind: "query", auth: controlProjectAdmin},
	"control.support.listTenants":           {kind: "query", auth: controlProjectAdmin},
	"control.support.listErrors":            {kind: "query", auth: controlProjectAdmin},
	"control.support.heartbeat":             {kind: "reducer", auth: controlAccount},
	"control.support.sendCommand":           {kind: "reducer", auth: controlProjectAdmin},
	"control.support.ackCommand":            {kind: "reducer", auth: controlAccount},
	"control.support.createImpersonation":   {kind: "reducer", auth: controlProjectAdmin},
	"control.demos.create":                  {kind: "reducer", auth: controlProjectAdmin},
	"control.demos.resetPassword":           {kind: "reducer", auth: controlProjectAdmin},
	"control.demos.delete":                  {kind: "reducer", auth: controlProjectAdmin},
}

func (c *wsConn) callControlPlane(ctx context.Context, kind, id, path string, raw json.RawMessage, idempotencyKey string) {
	definition, ok := controlFunctions[path]
	if !ok || definition.kind != kind {
		c.write(serverMessage{Type: kind + ".error", ID: id, Path: path, Error: "unknown Control Plane " + kind})
		return
	}
	if definition.auth == controlPublic {
		limit, window := 120, time.Minute
		if path == "control.auth.passwordLogin" {
			limit, window = 10, 15*time.Minute
		}
		rateSubject := strings.TrimSpace(c.remoteIP)
		if rateSubject == "" {
			rateSubject = c.id
		}
		if allowed, _ := c.server.authRateLimiter.allow("control:"+path+":"+rateSubject, limit, window); !allowed {
			c.write(serverMessage{Type: kind + ".error", ID: id, Path: path, Error: "Control Plane rate limit exceeded"})
			return
		}
	}
	if err := c.authorizeControlCall(ctx, definition.auth); err != nil {
		c.write(serverMessage{Type: kind + ".error", ID: id, Path: path, Error: err.Error()})
		return
	}
	// Execute against an immutable invocation snapshot. Membership-change and
	// session-revocation listeners may clear the live connection concurrently;
	// a handler must never observe a half-cleared identity or panic on c.user.
	invocation := c.controlCallSnapshot()
	result, err := c.server.executeControlCall(ctx, invocation, kind, path, raw, idempotencyKey)
	if err != nil {
		c.write(serverMessage{Type: kind + ".error", ID: id, Path: path, Error: err.Error()})
		if path == "control.tenants.delete" {
			c.clearAuthentication()
			c.write(serverMessage{Type: "auth.error", ID: "tenant-deleted", Error: "tenant access is no longer valid"})
		}
		return
	}
	message := serverMessage{Type: kind + ".result", ID: id, Path: path, Result: explicitNull(result)}
	if kind == "query" {
		message.Reason = "initial"
	}
	c.write(message)
	if path == "control.tenants.delete" {
		c.clearAuthentication()
		c.write(serverMessage{Type: "auth.error", ID: "tenant-deleted", Error: "tenant was deleted"})
	}
}

func (c *wsConn) authorizeControlCall(ctx context.Context, requirement controlAuthorization) error {
	if requirement == controlPublic {
		return nil
	}
	if !c.requireControlAuth(ctx) {
		return fmt.Errorf("authentication is required")
	}
	c.mu.Lock()
	account, project, tenant := c.user, c.project, c.tenant
	c.mu.Unlock()
	if account == nil {
		return fmt.Errorf("authentication is required")
	}
	switch requirement {
	case controlAccount:
		return nil
	case controlTenantAdmin:
		// The tenant database, not account_tenant_index or the connection cache,
		// is authoritative. Recheck it for every privileged tenant operation so a
		// delayed LISTEN notification cannot leave a brief authorization window.
		currentMember, err := c.server.loadTenantMember(ctx, project, tenant, account.ID)
		if err != nil || (currentMember.Role != "owner" && currentMember.Role != "admin") {
			return fmt.Errorf("tenant administrator access is required")
		}
		c.mu.Lock()
		if c.user != nil && c.user.ID == account.ID {
			c.member = currentMember
			c.perms = currentMember.Permissions
		}
		c.mu.Unlock()
		return nil
	case controlProjectAdmin:
		c.mu.Lock()
		impersonating := c.impersonationID != ""
		c.mu.Unlock()
		if impersonating {
			return fmt.Errorf("project administration is unavailable during impersonation")
		}
		if !c.server.canManageControlProject(ctx, project, account.Email) {
			return fmt.Errorf("project administrator access is required")
		}
		return nil
	default:
		return fmt.Errorf("Control Plane authorization is invalid")
	}
}

func (c *wsConn) controlCallSnapshot() *wsConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	snapshot := &wsConn{
		server: c.server, id: c.id, project: c.project, tenant: c.tenant,
		auth: c.auth, controlOnly: c.controlOnly, authToken: c.authToken,
		impersonationID: c.impersonationID, impersonatorID: c.impersonatorID,
	}
	if c.user != nil {
		account := *c.user
		snapshot.user = &account
	}
	if c.member != nil {
		member := *c.member
		snapshot.member = &member
	}
	return snapshot
}

func (s *Server) canManageControlProject(ctx context.Context, projectID, email string) bool {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil || strings.TrimSpace(email) == "" {
		return false
	}
	var allowed bool
	err = db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM gonvex_runtime_projects WHERE id=$1 AND lower(owner_email)=lower($2) AND owner_email<>''
		UNION ALL
		SELECT 1 FROM gonvex_project_members WHERE project_id=$1 AND lower(email)=lower($2) AND role IN ('owner','admin')
	)`, projectID, email).Scan(&allowed)
	return err == nil && allowed
}

func (s *Server) requireControlProject(ctx context.Context, projectID string) error {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return fmt.Errorf("project is required")
	}
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("Control Plane store is unavailable")
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM gonvex_runtime_projects WHERE id=$1 AND status NOT IN ('deleted','disabled')
	)`, projectID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("project is unavailable")
	}
	return nil
}

func (c *wsConn) requireControlAuth(ctx context.Context) bool {
	c.mu.Lock()
	authenticated := c.auth
	c.mu.Unlock()
	if authenticated && c.revalidateAppAuth(ctx) == nil {
		return true
	}
	if authenticated {
		c.clearAuthentication()
	}
	return false
}

func (s *Server) executeControlCall(ctx context.Context, connection *wsConn, kind, path string, raw json.RawMessage, idempotencyKey string) (any, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if kind == "query" {
		return s.executeControlQuery(ctx, connection, path, raw)
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("Control Plane %s calls require an idempotency key", kind)
	}
	accountID := "public"
	if connection.user != nil {
		accountID = connection.user.ID
	}
	if definition, ok := controlFunctions[path]; ok && definition.auth == controlTenantAdmin {
		accountID += "|tenant:" + connection.tenant
	}
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, fmt.Errorf("Control Plane store is unavailable")
	}
	if controlCallUsesAtomicStore(path) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		if cached, found, err := controlIdempotencyResult(ctx, tx, connection.project, accountID, idempotencyKey, kind, path); err != nil || found {
			return cached, err
		}
		result, err := s.executeControlReducerWithStore(ctx, tx, connection, path, raw, idempotencyKey)
		if err != nil {
			var terminal *controlCommitError
			if errors.As(err, &terminal) {
				if err := finishControlIdempotencyError(ctx, tx, connection.project, accountID, idempotencyKey, terminal.err); err != nil {
					return nil, err
				}
				if err := tx.Commit(); err != nil {
					return nil, err
				}
				if terminal.after != nil {
					terminal.after()
				}
				return nil, terminal.err
			}
			return nil, err
		}
		if err := finishControlIdempotency(ctx, tx, connection.project, accountID, idempotencyKey, result); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		s.afterControlCommit(connection, path, raw, result)
		return result, nil
	}
	if cached, found, err := controlIdempotencyResult(ctx, db, connection.project, accountID, idempotencyKey, kind, path); err != nil || found {
		return cached, err
	}
	result, err := s.executeControlReducerWithStore(ctx, db, connection, path, raw, idempotencyKey)
	if err != nil {
		_, _ = db.ExecContext(ctx, `DELETE FROM gonvex_control_idempotency WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3`, connection.project, accountID, idempotencyKey)
		return nil, err
	}
	if err := finishControlIdempotency(ctx, db, connection.project, accountID, idempotencyKey, result); err != nil {
		return nil, err
	}
	s.afterControlCommit(connection, path, raw, result)
	return result, nil
}

func (s *Server) afterControlCommit(connection *wsConn, path string, raw json.RawMessage, result any) {
	switch path {
	case "control.support.sendCommand":
		var args struct {
			SessionID string          `json:"sessionId"`
			Kind      string          `json:"kind"`
			Payload   json.RawMessage `json:"payload"`
		}
		resultMap, resultOK := result.(map[string]any)
		id, idOK := resultMap["id"].(string)
		if decodeControlArgs(raw, &args) == nil && resultOK && idOK {
			s.pushSupportCommand(connection.project, args.SessionID, id, args.Kind, args.Payload)
		}
	}
}

func controlCallUsesAtomicStore(path string) bool {
	switch path {
	case "control.accounts.updatePassword",
		"control.auth.passwordLogin", "control.auth.refreshSession",
		"control.auth.realms.configure",
		"control.tenants.updateProfile", "control.tenants.updateTimezone",
		"control.tenants.setException", "control.tenants.setSeatLimit",
		"control.invitations.create", "control.invitations.revoke",
		"control.agentAuth.issue", "control.agentAuth.claim", "control.agentAuth.revoke",
		"control.project.developers.invite", "control.project.developers.remove",
		"control.assistant.setDefaults", "control.voice.setRateCard", "control.voice.setTenantEntitlement", "control.voice.setUserOverride",
		"control.support.heartbeat", "control.support.sendCommand", "control.support.ackCommand",
		"control.support.createImpersonation",
		"control.demos.resetPassword":
		return true
	default:
		return false
	}
}

func controlIdempotencyResult(ctx context.Context, store controlStore, project, subject, key, kind, path string) (any, bool, error) {
	result, err := store.ExecContext(ctx, `INSERT INTO gonvex_control_idempotency
		(project_id, subject_id, idempotency_key, kind, path) VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT DO NOTHING`, project, subject, key, kind, path)
	if err != nil {
		return nil, false, err
	}
	if affected, _ := result.RowsAffected(); affected == 1 {
		return nil, false, nil
	}
	var state, storedKind, storedPath, storedError string
	var storedResult []byte
	if err := store.QueryRowContext(ctx, `SELECT state, kind, path, result, error FROM gonvex_control_idempotency
		WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3`, project, subject, key).Scan(&state, &storedKind, &storedPath, &storedResult, &storedError); err != nil {
		return nil, false, err
	}
	if storedKind != kind || storedPath != path {
		return nil, true, fmt.Errorf("idempotency key was already used for another operation")
	}
	if state == "pending" {
		result, err := store.ExecContext(ctx, `DELETE FROM gonvex_control_idempotency
			WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3 AND state='pending' AND updated_at < now()-interval '30 seconds'`, project, subject, key)
		if err != nil {
			return nil, true, err
		}
		if affected, _ := result.RowsAffected(); affected == 1 {
			return controlIdempotencyResult(ctx, store, project, subject, key, kind, path)
		}
		return nil, true, fmt.Errorf("Control Plane operation is still in progress")
	}
	if storedError != "" {
		return nil, true, fmt.Errorf("%s", storedError)
	}
	var decoded any
	if len(storedResult) > 0 {
		if err := json.Unmarshal(storedResult, &decoded); err != nil {
			return nil, true, err
		}
	}
	return decoded, true, nil
}

func finishControlIdempotency(ctx context.Context, store controlStore, project, subject, key string, result any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = store.ExecContext(ctx, `UPDATE gonvex_control_idempotency SET state='completed', result=$4, error='', updated_at=now()
		WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3`, project, subject, key, raw)
	return err
}

func finishControlIdempotencyError(ctx context.Context, store controlStore, project, subject, key string, cause error) error {
	_, err := store.ExecContext(ctx, `UPDATE gonvex_control_idempotency SET state='completed', result=NULL, error=$4, updated_at=now()
		WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3`, project, subject, key, cause.Error())
	return err
}

func decodeControlArgs(raw json.RawMessage, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid arguments: %w", err)
	}
	return nil
}

func validateEmptyControlArgs(raw json.RawMessage) error {
	return decodeControlArgs(raw, &struct{}{})
}

func (s *Server) executeControlQuery(ctx context.Context, c *wsConn, path string, raw json.RawMessage) (any, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, fmt.Errorf("Control Plane store is unavailable")
	}
	switch path {
	case "control.accounts.me":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		return map[string]any{"id": c.user.ID, "email": c.user.Email, "name": c.user.Name, "avatarUrl": c.user.AvatarURL}, nil
	case "control.auth.publicSettings":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		rows, err := db.QueryContext(ctx, `SELECT provider FROM gonvex_auth_providers WHERE project_id=$1 AND enabled=TRUE ORDER BY provider`, c.project)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		providers := []string{"password"}
		for rows.Next() {
			var provider string
			if err := rows.Scan(&provider); err != nil {
				return nil, err
			}
			if provider != "password" {
				providers = append(providers, provider)
			}
		}
		return map[string]any{"providers": providers}, rows.Err()
	case "control.auth.realms.list":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		rows, err := db.QueryContext(ctx, `SELECT provider, enabled, signup_mode FROM gonvex_auth_providers WHERE project_id=$1 ORDER BY provider`, c.project)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		items := []map[string]any{{"provider": "password", "enabled": true, "signupMode": "inviteOnly"}}
		for rows.Next() {
			var provider, signup string
			var enabled bool
			if err := rows.Scan(&provider, &enabled, &signup); err != nil {
				return nil, err
			}
			items = append(items, map[string]any{"provider": provider, "enabled": enabled, "signupMode": signup})
		}
		return items, rows.Err()
	case "control.tenants.mine":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		tenants, err := s.listAppAuthTenants(ctx, c.project, c.user.ID)
		if err != nil {
			return nil, err
		}
		items := make([]map[string]any, 0, len(tenants))
		for _, tenant := range tenants {
			var domain, timezone string
			var profile []byte
			_ = db.QueryRowContext(ctx, `SELECT domain,timezone,profile FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL`, c.project, tenant.ID).Scan(&domain, &timezone, &profile)
			var decoded any
			_ = json.Unmarshal(profile, &decoded)
			items = append(items, map[string]any{"id": tenant.ID, "name": tenant.Name, "role": tenant.Role, "permissions": tenant.Permissions, "domain": domain, "timezone": timezone, "profile": decoded})
		}
		return items, nil
	case "control.tenants.getByDomain":
		var args struct {
			Domain string `json:"domain"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		var id, name, domain string
		err := db.QueryRowContext(ctx, `SELECT tenant_id,name,domain FROM gonvex_runtime_tenants WHERE project_id=$1 AND lower(domain)=lower($2) AND deleted_at IS NULL AND status <> 'deleted'`, c.project, strings.TrimSpace(args.Domain)).Scan(&id, &name, &domain)
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("tenant not found")
		}
		return map[string]any{"id": id, "name": name, "domain": domain}, err
	case "control.invitations.lookup":
		var args struct {
			Token string `json:"token"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		var tenantName, role string
		var expires time.Time
		err := db.QueryRowContext(ctx, `SELECT t.name,i.role,i.expires_at FROM gonvex_auth_membership_invitations i JOIN gonvex_runtime_tenants t ON t.project_id=i.project_id AND t.tenant_id=i.tenant_id WHERE i.project_id=$1 AND i.token_hash=$2 AND i.revoked_at IS NULL AND i.accepted_at IS NULL AND i.expires_at>now()`, c.project, sha256Hex(args.Token)).Scan(&tenantName, &role, &expires)
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("invitation is invalid or expired")
		}
		return map[string]any{"tenantName": tenantName, "role": role, "expiresAt": expires}, err
	case "control.project.developers.list":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		return s.listProjectMembers(ctx, c.project)
	case "control.assistant.getDefaults":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		return controlSetting(ctx, db, c.project, "assistant.defaults", "")
	case "control.voice.getConfiguration":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		return controlSettingsByPrefix(ctx, db, c.project, "voice.")
	case "control.support.listSessions":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		return listControlSupportSessions(ctx, db, c.project)
	case "control.support.listTenants":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		rows, err := db.QueryContext(ctx, `SELECT tenant_id,name,domain,status,timezone,seat_limit,created_at FROM gonvex_runtime_tenants WHERE project_id=$1 AND deleted_at IS NULL ORDER BY name,tenant_id`, c.project)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		items := []map[string]any{}
		for rows.Next() {
			var id, name, domain, status, timezone string
			var seatLimit sql.NullInt64
			var created time.Time
			if err := rows.Scan(&id, &name, &domain, &status, &timezone, &seatLimit, &created); err != nil {
				return nil, err
			}
			var seat any
			if seatLimit.Valid {
				seat = seatLimit.Int64
			}
			items = append(items, map[string]any{"id": id, "name": name, "domain": domain, "status": status, "timezone": timezone, "seatLimit": seat, "createdAt": created})
		}
		return items, rows.Err()
	case "control.support.listErrors":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		groups, releases, available, err := s.persistentErrorGroups(ctx, c.project, "", "", "")
		if err != nil {
			return nil, err
		}
		if available {
			return map[string]any{"groups": groups, "releases": releases}, nil
		}
		cachedGroups, cachedReleases := s.errorTracker.listGroups(c.project, "", "", "")
		return map[string]any{"groups": cachedGroups, "releases": cachedReleases}, nil
	default:
		return nil, fmt.Errorf("Control Plane query is not implemented")
	}
}

func (s *Server) executeControlReducer(ctx context.Context, c *wsConn, path string, raw json.RawMessage) (any, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, fmt.Errorf("Control Plane store is unavailable")
	}
	return s.executeControlReducerWithStore(ctx, db, c, path, raw, "")
}

func (s *Server) executeControlReducerWithStore(ctx context.Context, db controlStore, c *wsConn, path string, raw json.RawMessage, idempotencyKey string) (any, error) {
	switch path {
	case "control.accounts.updatePassword":
		var args struct {
			CurrentPassword string `json:"currentPassword"`
			NewPassword     string `json:"newPassword"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		if len(args.NewPassword) < 12 {
			return nil, fmt.Errorf("newPassword must contain at least 12 characters")
		}
		var currentHash string
		err := db.QueryRowContext(ctx, `SELECT password_hash FROM gonvex_account_passwords WHERE project_id=$1 AND account_id=$2`, c.project, c.user.ID).Scan(&currentHash)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if err == nil && !verifyDashboardPassword(args.CurrentPassword, currentHash) {
			return nil, fmt.Errorf("current password is incorrect")
		}
		hash, err := hashDashboardPassword(args.NewPassword)
		if err != nil {
			return nil, err
		}
		_, err = db.ExecContext(ctx, `INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,$2,$3) ON CONFLICT(project_id,account_id) DO UPDATE SET password_hash=EXCLUDED.password_hash,updated_at=now()`, c.project, c.user.ID, hash)
		return map[string]any{"updated": err == nil}, err
	case "control.accounts.provisionMemberLogin":
		var args struct {
			Email       string         `json:"email"`
			Name        string         `json:"name"`
			Password    string         `json:"password"`
			Role        string         `json:"role"`
			Permissions map[string]any `json:"permissions"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		if len(args.Password) < 12 {
			return nil, fmt.Errorf("password must contain at least 12 characters")
		}
		email := normalizeDashboardEmail(args.Email)
		if email == "" {
			return nil, fmt.Errorf("email is required")
		}
		role, err := normalizeAppAuthMembershipRole(args.Role)
		if err != nil {
			return nil, err
		}
		if _, err := marshalAppAuthPermissions(args.Permissions); err != nil {
			return nil, err
		}
		accountID := ""
		resumingProvision := false
		err = db.QueryRowContext(ctx, `SELECT account.id,
			EXISTS(SELECT 1 FROM gonvex_member_login_provisioning provisioning
				WHERE provisioning.project_id=$1 AND provisioning.tenant_id=$3
				  AND provisioning.email=$2 AND provisioning.account_id=account.id)
			FROM accounts account
			WHERE account.auth_realm_id=$1 AND lower(account.email)=lower($2)
			ORDER BY account.id LIMIT 1`, c.project, email, c.tenant).Scan(&accountID, &resumingProvision)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if accountID != "" && !resumingProvision {
			return nil, fmt.Errorf("this account already exists; invite it instead of provisioning its login")
		}
		if accountID == "" {
			accountID, err = randomID("acct")
			if err != nil {
				return nil, err
			}
			hash, err := hashDashboardPassword(args.Password)
			if err != nil {
				return nil, err
			}
			physicalDB, err := concreteControlDB(db)
			if err != nil {
				return nil, err
			}
			tx, err := physicalDB.BeginTx(ctx, nil)
			if err != nil {
				return nil, err
			}
			defer tx.Rollback()
			if _, err = tx.ExecContext(ctx, `INSERT INTO accounts(id,auth_realm_id,email,name,updated_at) VALUES($1,$2,$3,$4,now())`, accountID, c.project, email, strings.TrimSpace(args.Name)); err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO account_identities(account_id,provider,issuer,subject,email,verified_email,updated_at) VALUES($1,'password',$2,$3,$3,TRUE,now())`, accountID, c.project, email); err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,$2,$3)`, c.project, accountID, hash); err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO gonvex_member_login_provisioning(project_id,tenant_id,email,account_id,created_by) VALUES($1,$2,$3,$4,$5)`, c.project, c.tenant, email, accountID, c.user.ID); err != nil {
				return nil, err
			}
			if err = tx.Commit(); err != nil {
				return nil, err
			}
		}
		if err = s.upsertAppAuthMembershipAs(ctx, c.project, c.tenant, accountID, role, args.Permissions, c.member.Role); err != nil {
			return nil, err
		}
		member, err := s.loadTenantMember(ctx, c.project, c.tenant, accountID)
		if err != nil {
			return nil, err
		}
		return map[string]any{"updated": true, "accountId": accountID, "memberId": member.ID}, nil
	case "control.auth.passwordLogin":
		var args struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		var accountID, passwordHash string
		err := db.QueryRowContext(ctx, `SELECT account.id,password.password_hash FROM accounts account JOIN gonvex_account_passwords password ON password.project_id=account.auth_realm_id AND password.account_id=account.id WHERE account.auth_realm_id=$1 AND lower(account.email)=lower($2) AND account.disabled_at IS NULL`, c.project, normalizeDashboardEmail(args.Email)).Scan(&accountID, &passwordHash)
		if err != nil || !verifyDashboardPassword(args.Password, passwordHash) {
			return nil, fmt.Errorf("invalid email or password")
		}
		tx, err := concreteControlTx(db)
		if err != nil {
			return nil, err
		}
		familyID, err := randomID("family")
		if err != nil {
			return nil, err
		}
		grant, err := issueAppAuthSessionGrant(ctx, tx, c.project, accountID, familyID, time.Now().Add(appRefreshSessionTTL).UTC())
		if err != nil {
			return nil, err
		}
		account, err := loadControlAccount(ctx, db, c.project, accountID)
		if err != nil {
			return nil, err
		}
		return s.controlSessionResult(ctx, c.project, grant, account)
	case "control.auth.refreshSession":
		var args struct {
			RefreshToken string `json:"refreshToken"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		tx, err := concreteControlTx(db)
		if err != nil {
			return nil, err
		}
		grant, account, revokedAccountID, err := refreshAppSessionTx(ctx, tx, c.project, args.RefreshToken)
		if err != nil {
			if revokedAccountID != "" {
				return nil, &controlCommitError{err: err, after: func() {
					s.revokeAppAuthConnections(c.project, revokedAccountID)
				}}
			}
			return nil, err
		}
		return s.controlSessionResult(ctx, c.project, grant, account)
	case "control.auth.logout":
		var args struct {
			RefreshToken string `json:"refreshToken"`
			All          bool   `json:"all"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		err := s.revokeAppAuthSession(ctx, c.authToken, args.RefreshToken, args.All)
		return map[string]any{"updated": err == nil}, err
	case "control.auth.realms.configure":
		var args struct {
			Provider   string `json:"provider"`
			Enabled    bool   `json:"enabled"`
			SignupMode string `json:"signupMode"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		args.Provider = strings.TrimSpace(args.Provider)
		normalizedSignupMode, err := normalizeAppAuthSignupMode(args.SignupMode)
		if err != nil {
			return nil, err
		}
		if args.Provider != googleProvider {
			return nil, fmt.Errorf("provider is unsupported")
		}
		_, err = db.ExecContext(ctx, `INSERT INTO gonvex_auth_providers(project_id,provider,enabled,signup_mode) VALUES($1,$2,$3,$4) ON CONFLICT(project_id,provider) DO UPDATE SET enabled=EXCLUDED.enabled,signup_mode=EXCLUDED.signup_mode,updated_at=now()`, c.project, args.Provider, args.Enabled, normalizedSignupMode)
		return map[string]any{"updated": err == nil}, err
	case "control.tenants.updateProfile":
		var args struct {
			Name        string `json:"name"`
			Domain      string `json:"domain"`
			Description string `json:"description"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		if strings.TrimSpace(args.Name) == "" {
			return nil, fmt.Errorf("name is required")
		}
		result, err := db.ExecContext(ctx, `UPDATE gonvex_runtime_tenants SET name=$3,domain=$4,description=$5,profile=jsonb_build_object('description',$5),updated_at=now() WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL`, c.project, c.tenant, strings.TrimSpace(args.Name), strings.TrimSpace(args.Domain), strings.TrimSpace(args.Description))
		return affectedResult(result, err)
	case "control.tenants.create":
		var args struct {
			Name string `json:"name"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		configuration, err := s.appAuthProviderConfiguration(ctx, c.project)
		if err != nil {
			return nil, err
		}
		if configuration.SignupMode == appAuthSignupInviteOnly {
			return nil, fmt.Errorf("this project allows tenant creation only through its control plane")
		}
		return s.createControlTenant(ctx, c, args.Name, idempotencyKey)
	case "control.tenants.updateTimezone":
		var args struct {
			Timezone string `json:"timezone"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		if _, err := time.LoadLocation(args.Timezone); err != nil {
			return nil, fmt.Errorf("timezone is invalid")
		}
		result, err := db.ExecContext(ctx, `UPDATE gonvex_runtime_tenants SET timezone=$3,updated_at=now() WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL`, c.project, c.tenant, args.Timezone)
		return affectedResult(result, err)
	case "control.tenants.delete":
		if err := validateEmptyControlArgs(raw); err != nil {
			return nil, err
		}
		if c.member == nil || c.member.Role != "owner" {
			return nil, fmt.Errorf("tenant owner access is required")
		}
		if err := s.revokeAllTenantMembers(ctx, c.project, c.tenant); err != nil {
			return nil, err
		}
		s.revokeTenantConnectionsExcept(c.project, c.tenant, "tenant was deleted", c.id)
		result, err := db.ExecContext(ctx, `UPDATE gonvex_runtime_tenants SET status='deleted',deleted_at=now(),deletion_idempotency_key=$3,updated_at=now() WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL`, c.project, c.tenant, idempotencyKey)
		if err != nil {
			return nil, err
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			var matches bool
			if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NOT NULL AND deletion_idempotency_key=$3)`, c.project, c.tenant, idempotencyKey).Scan(&matches); err != nil {
				return nil, err
			}
			if !matches {
				return nil, fmt.Errorf("record not found")
			}
		}
		return map[string]any{"updated": true}, nil
	case "control.tenants.setException", "control.tenants.setSeatLimit":
		var args struct {
			TenantID  string          `json:"tenantId"`
			Value     json.RawMessage `json:"value"`
			SeatLimit *int            `json:"seatLimit"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		if args.TenantID == "" {
			return nil, fmt.Errorf("tenantId is required")
		}
		if path == "control.tenants.setSeatLimit" {
			if args.SeatLimit != nil && *args.SeatLimit < 0 {
				return nil, fmt.Errorf("seatLimit cannot be negative")
			}
			result, err := db.ExecContext(ctx, `UPDATE gonvex_runtime_tenants SET seat_limit=$3,updated_at=now() WHERE project_id=$1 AND tenant_id=$2`, c.project, args.TenantID, args.SeatLimit)
			return affectedResult(result, err)
		}
		return upsertControlSetting(ctx, db, c.project, "tenant.exception", args.TenantID, args.Value, c.user.ID)
	case "control.invitations.create":
		var args struct {
			Email       string         `json:"email"`
			Role        string         `json:"role"`
			Permissions map[string]any `json:"permissions"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		email := normalizeDashboardEmail(args.Email)
		role, err := normalizeAppAuthMembershipRole(args.Role)
		if err != nil {
			return nil, err
		}
		if email == "" {
			return nil, fmt.Errorf("email is required")
		}
		if c.member != nil && c.member.Role == "admin" && (role == "owner" || role == "admin") {
			return nil, fmt.Errorf("tenant owner access is required to invite an administrator")
		}
		token, err := randomID("gvx_invite")
		if err != nil {
			return nil, err
		}
		invitationID, err := randomID("invite")
		if err != nil {
			return nil, err
		}
		permissions, err := marshalAppAuthPermissions(args.Permissions)
		if err != nil {
			return nil, err
		}
		_, err = db.ExecContext(ctx, `INSERT INTO gonvex_auth_membership_invitations(project_id,tenant_id,email,role,permissions,invited_by,expires_at,id,token_hash,revoked_at,accepted_at,accepted_account_id,accepted_idempotency_key,updated_at) VALUES($1,$2,$3,$4,$5,$6,now()+interval '7 days',$7,$8,NULL,NULL,NULL,NULL,now()) ON CONFLICT(project_id,tenant_id,email) DO UPDATE SET role=EXCLUDED.role,permissions=EXCLUDED.permissions,invited_by=EXCLUDED.invited_by,expires_at=EXCLUDED.expires_at,id=EXCLUDED.id,token_hash=EXCLUDED.token_hash,revoked_at=NULL,accepted_at=NULL,accepted_account_id=NULL,accepted_idempotency_key=NULL,updated_at=now()`, c.project, c.tenant, email, role, string(permissions), c.user.ID, invitationID, sha256Hex(token))
		return map[string]any{"id": invitationID, "token": token}, err
	case "control.invitations.revoke":
		var args struct {
			ID    string `json:"id"`
			Email string `json:"email"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		result, err := db.ExecContext(ctx, `UPDATE gonvex_auth_membership_invitations SET revoked_at=now(),updated_at=now() WHERE project_id=$1 AND tenant_id=$2 AND (id=$3 OR lower(email)=lower($4)) AND accepted_at IS NULL`, c.project, c.tenant, args.ID, args.Email)
		return affectedResult(result, err)
	case "control.invitations.accept":
		physicalDB, err := concreteControlDB(db)
		if err != nil {
			return nil, err
		}
		return s.acceptControlInvitation(ctx, physicalDB, c, raw, idempotencyKey)
	case "control.agentAuth.issue":
		var args struct {
			Permissions      []string `json:"permissions"`
			ExpiresInSeconds int      `json:"expiresInSeconds"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		if args.ExpiresInSeconds <= 0 || args.ExpiresInSeconds > 86400 {
			return nil, fmt.Errorf("expiresInSeconds must be between 1 and 86400")
		}
		token, err := randomID("gvx_agent_claim")
		if err != nil {
			return nil, err
		}
		id, err := randomID("agent")
		if err != nil {
			return nil, err
		}
		permissions, _ := json.Marshal(args.Permissions)
		_, err = db.ExecContext(ctx, `INSERT INTO gonvex_agent_claim_tokens(id,project_id,token_hash,permissions,expires_at,created_by) VALUES($1,$2,$3,$4,now()+($5 * interval '1 second'),$6)`, id, c.project, sha256Hex(token), permissions, args.ExpiresInSeconds, c.user.ID)
		return map[string]any{"id": id, "token": token}, err
	case "control.agentAuth.claim":
		var args struct {
			Token string `json:"token"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		var id string
		var permissions []byte
		err := db.QueryRowContext(ctx, `UPDATE gonvex_agent_claim_tokens SET claimed_at=now(),claimed_account_id=$3 WHERE project_id=$1 AND token_hash=$2 AND claimed_at IS NULL AND revoked_at IS NULL AND expires_at>now() RETURNING id,permissions`, c.project, sha256Hex(args.Token), c.user.ID).Scan(&id, &permissions)
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("agent token is invalid, expired, revoked, or already claimed")
		}
		var decoded any
		_ = json.Unmarshal(permissions, &decoded)
		return map[string]any{"id": id, "permissions": decoded}, err
	case "control.agentAuth.revoke":
		var args struct {
			ID string `json:"id"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		result, err := db.ExecContext(ctx, `UPDATE gonvex_agent_claim_tokens SET revoked_at=now() WHERE project_id=$1 AND id=$2 AND revoked_at IS NULL`, c.project, args.ID)
		return affectedResult(result, err)
	case "control.project.developers.invite":
		var args struct {
			Email string `json:"email"`
			Name  string `json:"name"`
			Role  string `json:"role"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		role := normalizedProjectRole(args.Role)
		if role == "" {
			return nil, fmt.Errorf("role is invalid")
		}
		_, err := db.ExecContext(ctx, `INSERT INTO gonvex_project_members(project_id,email,name,role) VALUES($1,$2,$3,$4) ON CONFLICT(project_id,email) DO UPDATE SET name=EXCLUDED.name,role=EXCLUDED.role`, c.project, normalizeDashboardEmail(args.Email), strings.TrimSpace(args.Name), role)
		return map[string]any{"updated": err == nil}, err
	case "control.project.developers.remove":
		var args struct {
			Email string `json:"email"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		result, err := db.ExecContext(ctx, `DELETE FROM gonvex_project_members WHERE project_id=$1 AND email=$2`, c.project, normalizeDashboardEmail(args.Email))
		return affectedResult(result, err)
	case "control.assistant.setDefaults", "control.voice.setRateCard", "control.voice.setTenantEntitlement", "control.voice.setUserOverride":
		var args struct {
			ScopeID string          `json:"scopeId"`
			Value   json.RawMessage `json:"value"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		settingKind := map[string]string{"control.assistant.setDefaults": "assistant.defaults", "control.voice.setRateCard": "voice.rateCard", "control.voice.setTenantEntitlement": "voice.tenantEntitlement", "control.voice.setUserOverride": "voice.userOverride"}[path]
		return upsertControlSetting(ctx, db, c.project, settingKind, args.ScopeID, args.Value, c.user.ID)
	case "control.support.heartbeat":
		return s.recordControlHeartbeat(ctx, db, c, raw)
	case "control.support.sendCommand":
		return s.sendControlSupportCommand(ctx, db, c, raw)
	case "control.support.ackCommand":
		return s.ackControlSupportCommand(ctx, db, c, raw)
	case "control.support.createImpersonation":
		var args struct {
			AccountID string `json:"accountId"`
			TenantID  string `json:"tenantId"`
			Reason    string `json:"reason"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		if strings.TrimSpace(args.Reason) == "" {
			return nil, fmt.Errorf("an audit reason is required")
		}
		if _, err := s.loadTenantMember(ctx, c.project, args.TenantID, args.AccountID); err != nil {
			return nil, fmt.Errorf("target account is not an active tenant member")
		}
		token, err := randomID("gvx_imp")
		if err != nil {
			return nil, err
		}
		id, err := randomID("imp")
		if err != nil {
			return nil, err
		}
		_, err = db.ExecContext(ctx, `INSERT INTO gonvex_impersonation_grants(id,project_id,token_hash,actor_account_id,target_account_id,tenant_id,reason,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,now()+interval '5 minutes')`, id, c.project, sha256Hex(token), c.user.ID, args.AccountID, args.TenantID, strings.TrimSpace(args.Reason))
		return map[string]any{"id": id, "token": token, "expiresAt": time.Now().Add(5 * time.Minute).UTC()}, err
	case "control.demos.create":
		var args struct {
			TenantID string `json:"tenantId"`
			Email    string `json:"email"`
			Name     string `json:"name"`
			Password string `json:"password"`
			Label    string `json:"label"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		if len(args.Password) < 12 {
			return nil, fmt.Errorf("password must contain at least 12 characters")
		}
		email := normalizeDashboardEmail(args.Email)
		if email == "" {
			return nil, fmt.Errorf("email is required")
		}
		accountID := ""
		var existingDemo bool
		err := db.QueryRowContext(ctx, `SELECT account.id, EXISTS(
			SELECT 1 FROM gonvex_demo_accounts demo WHERE demo.project_id=$1 AND demo.account_id=account.id AND demo.tenant_id=$3
		) FROM accounts account WHERE account.auth_realm_id=$1 AND lower(account.email)=lower($2) ORDER BY account.id LIMIT 1`, c.project, email, args.TenantID).Scan(&accountID, &existingDemo)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if accountID != "" && !existingDemo {
			return nil, fmt.Errorf("email already belongs to a non-demo account")
		}
		if accountID == "" {
			accountID, err = randomID("acct")
			if err != nil {
				return nil, err
			}
		}
		hash, err := hashDashboardPassword(args.Password)
		if err != nil {
			return nil, err
		}
		physicalDB, err := concreteControlDB(db)
		if err != nil {
			return nil, err
		}
		tx, err := physicalDB.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, `INSERT INTO accounts(id,auth_realm_id,email,name,disabled_at,updated_at) VALUES($1,$2,$3,$4,NULL,now()) ON CONFLICT(id) DO UPDATE SET email=EXCLUDED.email,name=EXCLUDED.name,disabled_at=NULL,updated_at=now()`, accountID, c.project, email, args.Name)
		if err != nil {
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO account_identities(account_id,provider,issuer,subject,email,verified_email,updated_at) VALUES($1,'password',$2,$3,$3,TRUE,now()) ON CONFLICT(provider,issuer,subject) DO UPDATE SET account_id=EXCLUDED.account_id,email=EXCLUDED.email,verified_email=TRUE,updated_at=now()`, accountID, c.project, email)
		if err != nil {
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,$2,$3) ON CONFLICT(project_id,account_id) DO UPDATE SET password_hash=EXCLUDED.password_hash,updated_at=now()`, c.project, accountID, hash)
		if err != nil {
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO gonvex_demo_accounts(project_id,account_id,tenant_id,label) VALUES($1,$2,$3,$4) ON CONFLICT(project_id,account_id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id,label=EXCLUDED.label`, c.project, accountID, args.TenantID, args.Label)
		if err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		if err = s.upsertAppAuthMembership(ctx, c.project, args.TenantID, accountID, "member", map[string]any{}); err != nil {
			return nil, err
		}
		member, err := s.loadTenantMember(ctx, c.project, args.TenantID, accountID)
		if err != nil {
			return nil, err
		}
		return map[string]any{"accountId": accountID, "memberId": member.ID}, nil
	case "control.demos.resetPassword":
		var args struct {
			AccountID string `json:"accountId"`
			Password  string `json:"password"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		if len(args.Password) < 12 {
			return nil, fmt.Errorf("password must contain at least 12 characters")
		}
		hash, err := hashDashboardPassword(args.Password)
		if err != nil {
			return nil, err
		}
		result, err := db.ExecContext(ctx, `UPDATE gonvex_account_passwords SET password_hash=$3,updated_at=now() WHERE project_id=$1 AND account_id=$2 AND EXISTS(SELECT 1 FROM gonvex_demo_accounts WHERE project_id=$1 AND account_id=$2)`, c.project, args.AccountID, hash)
		return affectedResult(result, err)
	case "control.demos.delete":
		var args struct {
			AccountID string `json:"accountId"`
		}
		if err := decodeControlArgs(raw, &args); err != nil {
			return nil, err
		}
		var tenantID string
		if err := db.QueryRowContext(ctx, `SELECT tenant_id FROM gonvex_demo_accounts WHERE project_id=$1 AND account_id=$2`, c.project, args.AccountID).Scan(&tenantID); err != nil {
			return nil, fmt.Errorf("demo account not found")
		}
		if _, _, err := s.revokeTenantMember(ctx, c.project, tenantID, "", args.AccountID); err != nil {
			return nil, err
		}
		if _, err := db.ExecContext(ctx, `DELETE FROM gonvex_demo_accounts WHERE project_id=$1 AND account_id=$2`, c.project, args.AccountID); err != nil {
			return nil, err
		}
		result, err := db.ExecContext(ctx, `UPDATE accounts SET disabled_at=now(),updated_at=now() WHERE id=$1 AND auth_realm_id=$2`, args.AccountID, c.project)
		return affectedResult(result, err)
	default:
		return nil, fmt.Errorf("Control Plane reducer is not implemented")
	}
}

func affectedResult(result sql.Result, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return nil, fmt.Errorf("record not found")
	}
	return map[string]any{"updated": true}, nil
}

// createControlTenant checkpoints every cross-database step in the Control
// Plane. A retry resumes the same tenant and physical database. It never rolls
// back a committed tenant database because a later projection or membership
// step failed.
func (s *Server) createControlTenant(ctx context.Context, c *wsConn, name, idempotencyKey string) (appAuthTenant, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return appAuthTenant{}, fmt.Errorf("tenant name is required")
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return appAuthTenant{}, fmt.Errorf("tenant creation requires an idempotency key")
	}
	mode, err := s.projectDatabaseMode(ctx, c.project)
	if err != nil {
		return appAuthTenant{}, err
	}
	if mode != "multiTenant" {
		return appAuthTenant{}, fmt.Errorf("project is not configured for tenant databases")
	}
	if strings.TrimSpace(s.config.PostgresURL) == "" {
		return appAuthTenant{}, fmt.Errorf("DATABASE_URL is not configured")
	}
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return appAuthTenant{}, fmt.Errorf("Control Plane store is unavailable")
	}
	var tenantID, databaseName, databaseAlias, reservedName, accountID string
	err = db.QueryRowContext(ctx, `SELECT tenant_id,database_name,database_alias,name,account_id
		FROM gonvex_tenant_provisioning WHERE project_id=$1 AND idempotency_key=$2`, c.project, idempotencyKey).
		Scan(&tenantID, &databaseName, &databaseAlias, &reservedName, &accountID)
	if err == sql.ErrNoRows {
		tenantID, err = generateRelationshipID()
		if err != nil {
			return appAuthTenant{}, err
		}
		databaseName, err = generateTenantPhysicalDatabaseName()
		if err != nil {
			return appAuthTenant{}, err
		}
		databaseAlias = slug(name)
		if databaseAlias == "" {
			databaseAlias = "workspace"
		}
		databaseAlias += "-" + strings.ReplaceAll(tenantID[:8], "-", "")
		_, err = db.ExecContext(ctx, `INSERT INTO gonvex_tenant_provisioning
			(project_id,idempotency_key,tenant_id,database_name,database_alias,name,account_id)
			VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(project_id,idempotency_key) DO NOTHING`,
			c.project, idempotencyKey, tenantID, databaseName, databaseAlias, name, c.user.ID)
		if err != nil {
			return appAuthTenant{}, err
		}
		err = db.QueryRowContext(ctx, `SELECT tenant_id,database_name,database_alias,name,account_id
			FROM gonvex_tenant_provisioning WHERE project_id=$1 AND idempotency_key=$2`, c.project, idempotencyKey).
			Scan(&tenantID, &databaseName, &databaseAlias, &reservedName, &accountID)
	}
	if err != nil {
		return appAuthTenant{}, err
	}
	if accountID != c.user.ID {
		return appAuthTenant{}, fmt.Errorf("tenant provisioning command belongs to another account")
	}
	tenantDatabaseURL, err := databaseURL(s.config.PostgresURL, databaseName)
	if err != nil {
		return appAuthTenant{}, err
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_tenants
		(relationship_id,project_id,tenant_id,name,database_alias,database_name,database_url,status,description,provisioned,runtime_created)
		VALUES($1,$2,$1,$3,$4,$5,$6,'active','Account-created tenant database.',FALSE,TRUE)
		ON CONFLICT(project_id,tenant_id) DO UPDATE SET name=EXCLUDED.name,updated_at=now()`,
		tenantID, c.project, reservedName, databaseAlias, databaseName, tenantDatabaseURL)
	if err != nil {
		return appAuthTenant{}, err
	}
	// The registry row was just committed. Bypass the normal short hydration TTL
	// so a second tenant created in the same process is routable immediately.
	s.invalidateProjectTenantHydration(c.project)
	if err := s.hydrateProjectTenantDatabasesWithError(ctx, c.project, s.hydrateProjectTenantDatabasesUncachedWithError); err != nil {
		return appAuthTenant{}, err
	}
	if _, err := s.ensureRuntimeTenantDatabase(ctx, c.project, tenantID, tenantDatabaseURL); err != nil {
		return appAuthTenant{}, err
	}
	if err := s.upsertAppAuthMembership(ctx, c.project, tenantID, c.user.ID, "owner", map[string]any{}); err != nil {
		return appAuthTenant{}, err
	}
	s.registerProjectCrons(c.project)
	return appAuthTenant{ID: tenantID, Name: reservedName, Role: "owner", Permissions: map[string]any{}}, nil
}

func loadControlAccount(ctx context.Context, db controlStore, project, accountID string) (appAuthAccount, error) {
	var account appAuthAccount
	err := db.QueryRowContext(ctx, `SELECT id,id,email,TRUE,name,avatar_url,'password',disabled_at IS NOT NULL,created_at,updated_at FROM accounts WHERE id=$1 AND auth_realm_id=$2 AND disabled_at IS NULL`, accountID, project).Scan(&account.ID, &account.AccountID, &account.Email, &account.EmailVerified, &account.Name, &account.Picture, &account.Provider, &account.Disabled, &account.CreatedAt, &account.LastSignedInAt)
	return account, err
}

func (s *Server) controlSessionResult(ctx context.Context, project string, grant appAuthSessionGrant, account appAuthAccount) (any, error) {
	tenants, err := s.listAppAuthTenants(ctx, project, account.ID)
	if err != nil {
		return nil, err
	}
	activeTenantID := ""
	if len(tenants) > 0 {
		activeTenantID = tenants[0].ID
	}
	return map[string]any{"accessToken": grant.AccessToken, "tokenType": "Bearer", "expiresIn": int(appSessionTTL.Seconds()), "expiresAt": grant.AccessExpiresAt.UnixMilli(), "refreshToken": grant.RefreshToken, "refreshExpiresAt": grant.RefreshExpiresAt.UnixMilli(), "account": map[string]any{"id": account.ID, "email": account.Email, "emailVerified": account.EmailVerified, "name": account.Name, "picture": account.Picture, "provider": account.Provider}, "tenants": tenants, "activeTenantId": activeTenantID}, nil
}

func controlSetting(ctx context.Context, db controlStore, project, kind, scope string) (any, error) {
	var raw []byte
	err := db.QueryRowContext(ctx, `SELECT value FROM gonvex_control_settings WHERE project_id=$1 AND kind=$2 AND scope_id=$3`, project, kind, scope).Scan(&raw)
	if err == sql.ErrNoRows {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, err
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return value, nil
}
func controlSettingsByPrefix(ctx context.Context, db controlStore, project, prefix string) (any, error) {
	rows, err := db.QueryContext(ctx, `SELECT kind,scope_id,value FROM gonvex_control_settings WHERE project_id=$1 AND kind LIKE $2 ORDER BY kind,scope_id`, project, prefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var kind, scope string
		var raw []byte
		if err := rows.Scan(&kind, &scope, &raw); err != nil {
			return nil, err
		}
		var value any
		_ = json.Unmarshal(raw, &value)
		items = append(items, map[string]any{"kind": kind, "scopeId": scope, "value": value})
	}
	return items, rows.Err()
}
func upsertControlSetting(ctx context.Context, db controlStore, project, kind, scope string, value json.RawMessage, actor string) (any, error) {
	if len(value) == 0 {
		value = json.RawMessage(`{}`)
	}
	var valid any
	if err := json.Unmarshal(value, &valid); err != nil {
		return nil, fmt.Errorf("value must be valid JSON")
	}
	_, err := db.ExecContext(ctx, `INSERT INTO gonvex_control_settings(project_id,kind,scope_id,value,updated_by) VALUES($1,$2,$3,$4,$5) ON CONFLICT(project_id,kind,scope_id) DO UPDATE SET value=EXCLUDED.value,updated_by=EXCLUDED.updated_by,updated_at=now()`, project, kind, scope, value, actor)
	return map[string]any{"updated": err == nil}, err
}

func (s *Server) acceptControlInvitation(ctx context.Context, db *sql.DB, c *wsConn, raw json.RawMessage, idempotencyKey string) (any, error) {
	var args struct {
		Token string `json:"token"`
	}
	if err := decodeControlArgs(raw, &args); err != nil {
		return nil, err
	}
	var id, tenantID, email, role string
	var permissions []byte
	var expiresAt time.Time
	var revokedAt, acceptedAt sql.NullTime
	var acceptedAccountID, acceptedIdempotencyKey sql.NullString
	err := db.QueryRowContext(ctx, `SELECT id,tenant_id,email,role,permissions,expires_at,revoked_at,accepted_at,accepted_account_id,accepted_idempotency_key
		FROM gonvex_auth_membership_invitations WHERE project_id=$1 AND token_hash=$2`, c.project, sha256Hex(args.Token)).
		Scan(&id, &tenantID, &email, &role, &permissions, &expiresAt, &revokedAt, &acceptedAt, &acceptedAccountID, &acceptedIdempotencyKey)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("invitation is invalid, expired, revoked, or already accepted")
	}
	if err != nil {
		return nil, err
	}
	if revokedAt.Valid || !expiresAt.After(time.Now()) {
		return nil, fmt.Errorf("invitation is invalid, expired, revoked, or already accepted")
	}
	if acceptedAt.Valid {
		if acceptedAccountID.String != c.user.ID || acceptedIdempotencyKey.String == "" || acceptedIdempotencyKey.String != idempotencyKey {
			return nil, fmt.Errorf("invitation is invalid, expired, revoked, or already accepted")
		}
		member, err := s.loadTenantMember(ctx, c.project, tenantID, c.user.ID)
		if err != nil {
			return nil, err
		}
		return map[string]any{"tenantId": tenantID, "memberId": member.ID}, nil
	}
	if normalizeDashboardEmail(email) != normalizeDashboardEmail(c.user.Email) {
		return nil, fmt.Errorf("invitation does not belong to this account")
	}
	permissionValues := map[string]any{}
	if len(permissions) > 0 && json.Unmarshal(permissions, &permissionValues) != nil {
		return nil, fmt.Errorf("invitation permissions are invalid")
	}
	if err := s.upsertAppAuthMembership(ctx, c.project, tenantID, c.user.ID, role, permissionValues); err != nil {
		return nil, err
	}
	result, err := db.ExecContext(ctx, `UPDATE gonvex_auth_membership_invitations SET accepted_at=now(),accepted_account_id=$3,accepted_idempotency_key=$4,updated_at=now() WHERE project_id=$1 AND id=$2 AND accepted_at IS NULL AND revoked_at IS NULL`, c.project, id, c.user.ID, idempotencyKey)
	if err != nil {
		return nil, err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return nil, fmt.Errorf("invitation was already accepted")
	}
	member, err := s.loadTenantMember(ctx, c.project, tenantID, c.user.ID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"tenantId": tenantID, "memberId": member.ID}, nil
}

func listControlSupportSessions(ctx context.Context, db controlStore, project string) (any, error) {
	rows, err := db.QueryContext(ctx, `SELECT id,tenant_id,account_id,release,environment,last_seen_at FROM gonvex_support_sessions WHERE project_id=$1 ORDER BY last_seen_at DESC LIMIT 500`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id, tenant, account, release, environment string
		var seen time.Time
		if err := rows.Scan(&id, &tenant, &account, &release, &environment, &seen); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{"id": id, "tenantId": tenant, "accountId": account, "release": release, "environment": environment, "lastSeenAt": seen})
	}
	return items, rows.Err()
}
func (s *Server) recordControlHeartbeat(ctx context.Context, db controlStore, c *wsConn, raw json.RawMessage) (any, error) {
	var args struct {
		Release     string `json:"release"`
		Environment string `json:"environment"`
	}
	if err := decodeControlArgs(raw, &args); err != nil {
		return nil, err
	}
	_, err := db.ExecContext(ctx, `INSERT INTO gonvex_support_sessions(id,project_id,tenant_id,account_id,connection_id,release,environment) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id,account_id=EXCLUDED.account_id,release=EXCLUDED.release,environment=EXCLUDED.environment,last_seen_at=now()`, c.id, c.project, c.tenant, c.user.ID, c.id, args.Release, args.Environment)
	return map[string]any{"sessionId": c.id}, err
}
func (s *Server) sendControlSupportCommand(ctx context.Context, db controlStore, c *wsConn, raw json.RawMessage) (any, error) {
	var args struct {
		SessionID string          `json:"sessionId"`
		Kind      string          `json:"kind"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := decodeControlArgs(raw, &args); err != nil {
		return nil, err
	}
	id, err := randomID("command")
	if err != nil {
		return nil, err
	}
	if len(args.Payload) == 0 {
		args.Payload = json.RawMessage(`{}`)
	}
	result, err := db.ExecContext(ctx, `INSERT INTO gonvex_support_commands(id,project_id,session_id,kind,payload,created_by) SELECT $1,$2,$3,$4,$5,$6 WHERE EXISTS(SELECT 1 FROM gonvex_support_sessions WHERE id=$3 AND project_id=$2)`, id, c.project, args.SessionID, args.Kind, args.Payload, c.user.ID)
	if err != nil {
		return nil, err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return nil, fmt.Errorf("support session not found")
	}
	return map[string]any{"id": id}, nil
}
func (s *Server) ackControlSupportCommand(ctx context.Context, db controlStore, c *wsConn, raw json.RawMessage) (any, error) {
	var args struct {
		ID string `json:"id"`
	}
	if err := decodeControlArgs(raw, &args); err != nil {
		return nil, err
	}
	result, err := db.ExecContext(ctx, `UPDATE gonvex_support_commands SET acknowledged_at=now() WHERE id=$1 AND project_id=$2 AND session_id=$3 AND acknowledged_at IS NULL`, args.ID, c.project, c.id)
	return affectedResult(result, err)
}
func (s *Server) pushSupportCommand(project, sessionID, id, kind string, payload json.RawMessage) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		matches := connection.project == project && connection.id == sessionID
		connection.mu.Unlock()
		if matches {
			connection.write(serverMessage{Type: "support.command", ID: id, Result: map[string]any{"kind": kind, "payload": payload}})
		}
	}
}

func (s *Server) revokeAllTenantMembers(ctx context.Context, project, tenant string) error {
	db, err := s.tenantMemberDB(ctx, project, tenant)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE members SET status='revoked',membership_revision=membership_revision+1,updated_at=now() WHERE status='active'`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.startMembershipProjection(func() {
		s.projectTenantMemberDirectory(project, tenant)
	})
	return nil
}

func (s *Server) revokeTenantConnections(project, tenant, reason string) {
	s.revokeTenantConnectionsExcept(project, tenant, reason, "")
}

func (s *Server) revokeTenantConnectionsExcept(project, tenant, reason, exceptConnectionID string) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		matches := connection.project == project && connection.tenant == tenant && connection.id != exceptConnectionID
		connection.mu.Unlock()
		if matches {
			connection.clearAuthentication()
			connection.write(serverMessage{Type: "auth.error", ID: "tenant-revoked", Error: reason})
		}
	}
}
