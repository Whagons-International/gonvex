package server

import "context"

// ensureControlPlaneFunctionSchema installs only host-owned Control Plane
// state. Tenant business data remains in tenant databases.
func ensureControlPlaneFunctionSchema(ctx context.Context, db projectRegistryExecer) error {
	statements := []string{
		`ALTER TABLE gonvex_runtime_projects ADD COLUMN IF NOT EXISTS auth_mode TEXT NOT NULL DEFAULT 'gonvex-native'`,
		`ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS azure_tenant_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS client_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS client_secret_encrypted BYTEA`,
		`ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS issuer TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS audience TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS jwks_url TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS firebase_project_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS firebase_tenant_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS admin_credentials_encrypted BYTEA`,
		`CREATE TABLE IF NOT EXISTS gonvex_auth_identity_events (
			id BIGSERIAL PRIMARY KEY,
			project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
			account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
			provider TEXT NOT NULL,
			issuer TEXT NOT NULL,
			subject TEXT NOT NULL,
			resolution TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE gonvex_auth_transactions ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'google'`,
		`ALTER TABLE gonvex_runtime_tenants ADD COLUMN IF NOT EXISTS timezone TEXT NOT NULL DEFAULT 'UTC'`,
		`ALTER TABLE gonvex_runtime_tenants ADD COLUMN IF NOT EXISTS profile JSONB NOT NULL DEFAULT '{}'::jsonb`,
		`ALTER TABLE gonvex_runtime_tenants ADD COLUMN IF NOT EXISTS seat_limit INTEGER`,
		`ALTER TABLE gonvex_runtime_tenants ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ`,
		`ALTER TABLE gonvex_runtime_tenants ADD COLUMN IF NOT EXISTS deletion_idempotency_key TEXT`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS id TEXT`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS token_hash TEXT`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS accepted_account_id TEXT`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS accepted_idempotency_key TEXT`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS team_ids JSONB NOT NULL DEFAULT '[]'::jsonb`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS allowed_auth_providers JSONB NOT NULL DEFAULT '[]'::jsonb`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS application_payload JSONB NOT NULL DEFAULT '{}'::jsonb`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS handoff_state TEXT NOT NULL DEFAULT 'pending'`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS handoff_command_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE gonvex_auth_membership_invitations ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ`,
		`CREATE UNIQUE INDEX IF NOT EXISTS gonvex_auth_membership_invitations_token ON gonvex_auth_membership_invitations(token_hash) WHERE token_hash IS NOT NULL AND token_hash <> ''`,
		`CREATE TABLE IF NOT EXISTS gonvex_account_passwords (
			project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
			account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
			password_hash TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (project_id, account_id)
		)`,
		`CREATE TABLE IF NOT EXISTS gonvex_member_login_provisioning (
			project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
			tenant_id TEXT NOT NULL,
			email TEXT NOT NULL,
			account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
			created_by TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (project_id, tenant_id, email),
			UNIQUE (project_id, tenant_id, account_id)
		)`,
		`CREATE TABLE IF NOT EXISTS gonvex_tenant_provisioning (
			project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
			idempotency_key TEXT NOT NULL,
			tenant_id TEXT NOT NULL,
			database_name TEXT NOT NULL,
			database_alias TEXT NOT NULL,
			name TEXT NOT NULL,
			account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (project_id, idempotency_key),
			UNIQUE (project_id, tenant_id),
			UNIQUE (database_name)
		)`,
		`CREATE TABLE IF NOT EXISTS gonvex_control_idempotency (
			project_id TEXT NOT NULL,
			subject_id TEXT NOT NULL,
			idempotency_key TEXT NOT NULL,
			kind TEXT NOT NULL,
			path TEXT NOT NULL,
			state TEXT NOT NULL DEFAULT 'pending',
			result JSONB,
			error TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (project_id, subject_id, idempotency_key)
		)`,
		`CREATE TABLE IF NOT EXISTS gonvex_agent_claim_tokens (
			id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
			token_hash TEXT NOT NULL UNIQUE,
			permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
			expires_at TIMESTAMPTZ NOT NULL,
			claimed_at TIMESTAMPTZ,
			claimed_account_id TEXT,
			revoked_at TIMESTAMPTZ,
			created_by TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS gonvex_control_settings (
			project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
			kind TEXT NOT NULL,
			scope_id TEXT NOT NULL DEFAULT '',
			value JSONB NOT NULL DEFAULT '{}'::jsonb,
			updated_by TEXT NOT NULL DEFAULT '',
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (project_id, kind, scope_id)
		)`,
		`CREATE TABLE IF NOT EXISTS gonvex_support_sessions (
			id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL,
			tenant_id TEXT NOT NULL DEFAULT '',
			account_id TEXT NOT NULL DEFAULT '',
			connection_id TEXT NOT NULL DEFAULT '',
			release TEXT NOT NULL DEFAULT '',
			environment TEXT NOT NULL DEFAULT '',
			last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS gonvex_support_commands (
			id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL,
			session_id TEXT NOT NULL REFERENCES gonvex_support_sessions(id) ON DELETE CASCADE,
			kind TEXT NOT NULL,
			payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_by TEXT NOT NULL,
			acknowledged_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS gonvex_impersonation_grants (
			id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
			token_hash TEXT NOT NULL UNIQUE,
			actor_account_id TEXT NOT NULL,
			target_account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
			tenant_id TEXT NOT NULL,
			reason TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL,
			used_at TIMESTAMPTZ,
			used_connection_id TEXT NOT NULL DEFAULT '',
			reconnect_token_hash TEXT NOT NULL DEFAULT '',
			revoked_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE gonvex_impersonation_grants ADD COLUMN IF NOT EXISTS reconnect_token_hash TEXT NOT NULL DEFAULT ''`,
		`CREATE UNIQUE INDEX IF NOT EXISTS gonvex_impersonation_grants_reconnect_token ON gonvex_impersonation_grants(reconnect_token_hash) WHERE reconnect_token_hash <> ''`,
		`CREATE TABLE IF NOT EXISTS gonvex_demo_accounts (
			project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
			account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
			tenant_id TEXT NOT NULL,
			label TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY(project_id, account_id)
		)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
