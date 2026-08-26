CREATE TABLE IF NOT EXISTS gonvex_runtime_projects (
  id text PRIMARY KEY, name text NOT NULL, environment text NOT NULL DEFAULT 'development',
  database_name text NOT NULL DEFAULT '', database_mode text NOT NULL DEFAULT 'single',
  database_url text NOT NULL DEFAULT '', storage_bucket text NOT NULL DEFAULT '',
  status text NOT NULL DEFAULT 'active', description text NOT NULL DEFAULT '',
  project_key text NOT NULL DEFAULT '', provisioned boolean NOT NULL DEFAULT true,
  runtime_created boolean NOT NULL DEFAULT true, test_tab boolean NOT NULL DEFAULT false,
  error_tracking_enabled boolean NOT NULL DEFAULT false, owner_email text NOT NULL DEFAULT '',
  auth_mode text NOT NULL DEFAULT 'gonvex-native',
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS accounts (
  id text PRIMARY KEY, auth_realm_id text NOT NULL DEFAULT '', email text NOT NULL DEFAULT '',
  name text NOT NULL DEFAULT '', avatar_url text NOT NULL DEFAULT '', disabled_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS accounts_by_email ON accounts(lower(email)) WHERE email<>'';
CREATE TABLE IF NOT EXISTS account_identities (
  account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  project_id text NOT NULL DEFAULT '', provider text NOT NULL, issuer text NOT NULL DEFAULT '',
  subject text NOT NULL, email text NOT NULL DEFAULT '', verified_email boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(project_id,provider,issuer,subject)
);
CREATE INDEX IF NOT EXISTS account_identities_by_account ON account_identities(account_id);
CREATE INDEX IF NOT EXISTS account_identities_by_verified_email ON account_identities(lower(email))
  WHERE verified_email AND email<>'';
CREATE TABLE IF NOT EXISTS gonvex_runtime_tenants (
  relationship_id text PRIMARY KEY, project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  tenant_id text NOT NULL, name text NOT NULL, database_alias text NOT NULL DEFAULT '',
  database_name text NOT NULL DEFAULT '', database_url text NOT NULL DEFAULT '', domain text NOT NULL DEFAULT '',
  status text NOT NULL DEFAULT 'active', description text NOT NULL DEFAULT '', timezone text NOT NULL DEFAULT 'UTC',
  profile jsonb NOT NULL DEFAULT '{}'::jsonb, seat_limit integer, provisioned boolean NOT NULL DEFAULT false,
  runtime_created boolean NOT NULL DEFAULT false, deleted_at timestamptz, deletion_idempotency_key text,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(project_id,tenant_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_runtime_tenants_project_database
  ON gonvex_runtime_tenants(project_id,database_name) WHERE database_name<>'';
CREATE TABLE IF NOT EXISTS account_tenant_index (
  account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, tenant_id text NOT NULL,
  member_id text NOT NULL, status text NOT NULL DEFAULT 'active', tenant_membership_revision bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(account_id,tenant_id), UNIQUE(tenant_id,member_id)
);
CREATE INDEX IF NOT EXISTS account_tenant_index_by_tenant ON account_tenant_index(tenant_id,status,account_id);
CREATE TABLE IF NOT EXISTS gonvex_runtime_manifests (
  project_id text PRIMARY KEY, manifest jsonb NOT NULL, module_hash text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS gonvex_project_members (
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  email text NOT NULL, name text NOT NULL DEFAULT '', role text NOT NULL DEFAULT 'dev',
  created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(project_id,email)
);
CREATE TABLE IF NOT EXISTS gonvex_project_invitations (
  id text PRIMARY KEY, project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  email text NOT NULL, role text NOT NULL DEFAULT 'dev', invited_by text NOT NULL DEFAULT '',
  token_hash text NOT NULL DEFAULT '', expires_at timestamptz NOT NULL, accepted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(project_id,email)
);
ALTER TABLE gonvex_project_invitations ADD COLUMN IF NOT EXISTS token_hash text NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS gonvex_dashboard_notifications (
  id text PRIMARY KEY, email text NOT NULL, kind text NOT NULL, title text NOT NULL,
  body text NOT NULL DEFAULT '', project_id text NOT NULL DEFAULT '', metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  read_at timestamptz, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS gonvex_dashboard_notifications_by_email
  ON gonvex_dashboard_notifications(email,created_at DESC);
CREATE TABLE IF NOT EXISTS gonvex_dashboard_accounts (
  email text PRIMARY KEY, name text NOT NULL, role text NOT NULL DEFAULT 'standard',
  password_hash text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS gonvex_account_access_tokens (
  id text PRIMARY KEY, owner_email text NOT NULL, name text NOT NULL, token_prefix text NOT NULL,
  token_hash text NOT NULL UNIQUE, permissions jsonb NOT NULL DEFAULT '[]'::jsonb,
  expires_at timestamptz, last_used_at timestamptz, revoked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS gonvex_project_env (
  project_id text NOT NULL, name text NOT NULL, value text NOT NULL DEFAULT '',
  updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(project_id,name)
);
CREATE TABLE IF NOT EXISTS gonvex_auth_providers (
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  provider text NOT NULL, enabled boolean NOT NULL DEFAULT true, signup_mode text NOT NULL DEFAULT 'personal',
  azure_tenant_id text NOT NULL DEFAULT '', client_id text NOT NULL DEFAULT '', client_secret_encrypted bytea,
  issuer text NOT NULL DEFAULT '', audience text NOT NULL DEFAULT '', jwks_url text NOT NULL DEFAULT '',
  firebase_project_id text NOT NULL DEFAULT '', firebase_tenant_id text NOT NULL DEFAULT '',
  admin_credentials_encrypted bytea, created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(project_id,provider)
);
ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS signup_mode text NOT NULL DEFAULT 'personal';
ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS azure_tenant_id text NOT NULL DEFAULT '';
ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS client_id text NOT NULL DEFAULT '';
ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS client_secret_encrypted bytea;
ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS issuer text NOT NULL DEFAULT '';
ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS audience text NOT NULL DEFAULT '';
ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS jwks_url text NOT NULL DEFAULT '';
ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS firebase_project_id text NOT NULL DEFAULT '';
ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS firebase_tenant_id text NOT NULL DEFAULT '';
ALTER TABLE gonvex_auth_providers ADD COLUMN IF NOT EXISTS admin_credentials_encrypted bytea;
CREATE TABLE IF NOT EXISTS gonvex_auth_redirect_uris (
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  provider text NOT NULL, redirect_uri text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(project_id,provider,redirect_uri)
);
CREATE TABLE IF NOT EXISTS gonvex_auth_transactions (
  token_hash text PRIMARY KEY,
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  redirect_uri text NOT NULL, app_state text NOT NULL, code_challenge text NOT NULL,
  nonce text NOT NULL, google_redirect_uri text NOT NULL, provider text NOT NULL DEFAULT 'google',
  expires_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE gonvex_auth_transactions ADD COLUMN IF NOT EXISTS provider text NOT NULL DEFAULT 'google';
CREATE INDEX IF NOT EXISTS gonvex_auth_transactions_by_expiry ON gonvex_auth_transactions(expires_at);
CREATE TABLE IF NOT EXISTS gonvex_auth_codes (
  code_hash text PRIMARY KEY,
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  redirect_uri text NOT NULL, code_challenge text NOT NULL,
  expires_at timestamptz NOT NULL, used_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS gonvex_auth_codes_by_expiry ON gonvex_auth_codes(expires_at);
CREATE TABLE IF NOT EXISTS gonvex_auth_sessions (
  token_hash text PRIMARY KEY, project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, family_id text NOT NULL DEFAULT '',
  expires_at timestamptz NOT NULL, revoked_at timestamptz, last_seen_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS gonvex_auth_sessions_by_account ON gonvex_auth_sessions(project_id,account_id,expires_at DESC);
CREATE INDEX IF NOT EXISTS gonvex_auth_sessions_by_family ON gonvex_auth_sessions(family_id) WHERE family_id<>'';
CREATE TABLE IF NOT EXISTS gonvex_auth_refresh_tokens (
  token_hash text PRIMARY KEY, family_id text NOT NULL,
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, expires_at timestamptz NOT NULL,
  used_at timestamptz, revoked_at timestamptz, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS gonvex_auth_refresh_tokens_by_family ON gonvex_auth_refresh_tokens(family_id,created_at DESC);
CREATE TABLE IF NOT EXISTS gonvex_account_passwords (
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, password_hash text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(project_id,account_id)
);
CREATE TABLE IF NOT EXISTS gonvex_auth_identity_events (
  id bigserial PRIMARY KEY, project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, provider text NOT NULL,
  issuer text NOT NULL, subject text NOT NULL, resolution text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS gonvex_auth_membership_invitations (
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  tenant_id text NOT NULL, email text NOT NULL, role text NOT NULL DEFAULT 'member',
  permissions jsonb NOT NULL DEFAULT '{}'::jsonb, invited_by text NOT NULL DEFAULT '',
  expires_at timestamptz NOT NULL DEFAULT(now()+interval '7 days'),
  id text, token_hash text, revoked_at timestamptz, accepted_at timestamptz,
  accepted_account_id text, accepted_idempotency_key text,
  team_ids jsonb NOT NULL DEFAULT '[]'::jsonb, allowed_auth_providers jsonb NOT NULL DEFAULT '[]'::jsonb,
  application_payload jsonb NOT NULL DEFAULT '{}'::jsonb, handoff_state text NOT NULL DEFAULT 'pending',
  handoff_command_id text NOT NULL DEFAULT '', completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(project_id,tenant_id,email),
  FOREIGN KEY(project_id,tenant_id) REFERENCES gonvex_runtime_tenants(project_id,tenant_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_auth_membership_invitations_token
  ON gonvex_auth_membership_invitations(token_hash) WHERE token_hash IS NOT NULL AND token_hash<>'';
CREATE TABLE IF NOT EXISTS gonvex_member_login_provisioning (
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  tenant_id text NOT NULL, email text NOT NULL, account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  created_by text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(project_id,tenant_id,email), UNIQUE(project_id,tenant_id,account_id)
);
CREATE TABLE IF NOT EXISTS gonvex_tenant_provisioning (
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  idempotency_key text NOT NULL, tenant_id text NOT NULL, database_name text NOT NULL,
  database_alias text NOT NULL, name text NOT NULL, account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(project_id,idempotency_key),
  UNIQUE(project_id,tenant_id), UNIQUE(database_name)
);
CREATE TABLE IF NOT EXISTS gonvex_control_idempotency (
  project_id text NOT NULL, subject_id text NOT NULL, idempotency_key text NOT NULL,
  kind text NOT NULL, path text NOT NULL, state text NOT NULL DEFAULT 'pending', result jsonb,
  error text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(project_id,subject_id,idempotency_key)
);
CREATE TABLE IF NOT EXISTS gonvex_agent_claim_tokens (
  id text PRIMARY KEY, project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  token_hash text NOT NULL UNIQUE, permissions jsonb NOT NULL DEFAULT '[]'::jsonb,
  expires_at timestamptz NOT NULL, claimed_at timestamptz, claimed_account_id text, revoked_at timestamptz,
  created_by text NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS gonvex_control_settings (
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  kind text NOT NULL, scope_id text NOT NULL DEFAULT '', value jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_by text NOT NULL DEFAULT '', updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(project_id,kind,scope_id)
);
CREATE TABLE IF NOT EXISTS gonvex_support_sessions (
  id text PRIMARY KEY, project_id text NOT NULL, tenant_id text NOT NULL DEFAULT '',
  account_id text NOT NULL DEFAULT '', connection_id text NOT NULL DEFAULT '', release text NOT NULL DEFAULT '',
  environment text NOT NULL DEFAULT '', last_seen_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS gonvex_support_commands (
  id text PRIMARY KEY, project_id text NOT NULL,
  session_id text NOT NULL REFERENCES gonvex_support_sessions(id) ON DELETE CASCADE,
  kind text NOT NULL, payload jsonb NOT NULL DEFAULT '{}'::jsonb, created_by text NOT NULL,
  acknowledged_at timestamptz, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS gonvex_scheduled_jobs (
  id text PRIMARY KEY, project_id text NOT NULL, tenant_id text NOT NULL DEFAULT '',
  function_path text NOT NULL, args jsonb, run_at timestamptz NOT NULL,
  scheduled_for timestamptz NOT NULL, cron_name text NOT NULL DEFAULT '',
  provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','completed')),
  claim_sequence bigint NOT NULL DEFAULT 0, claim_token text NOT NULL DEFAULT '',
  lease_until timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz
);
ALTER TABLE gonvex_scheduled_jobs ADD COLUMN IF NOT EXISTS provenance jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_due_oneshot
  ON gonvex_scheduled_jobs(run_at,id) WHERE status='pending' AND cron_name='';
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_due_cron
  ON gonvex_scheduled_jobs(run_at,id) WHERE status='pending' AND cron_name<>'';
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_completed
  ON gonvex_scheduled_jobs(completed_at,id) WHERE status='completed';
CREATE TABLE IF NOT EXISTS gonvex_impersonation_grants (
  id text PRIMARY KEY, project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  token_hash text NOT NULL UNIQUE, actor_account_id text NOT NULL,
  target_account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, tenant_id text NOT NULL,
  reason text NOT NULL, expires_at timestamptz NOT NULL, used_at timestamptz,
  used_connection_id text NOT NULL DEFAULT '', reconnect_token_hash text NOT NULL DEFAULT '', revoked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_impersonation_grants_reconnect_token
  ON gonvex_impersonation_grants(reconnect_token_hash) WHERE reconnect_token_hash<>'';
CREATE TABLE IF NOT EXISTS gonvex_demo_accounts (
  project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, tenant_id text NOT NULL,
  label text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(project_id,account_id)
);
CREATE TABLE IF NOT EXISTS gonvex_error_events (
  project_id text NOT NULL, event_id text NOT NULL, fingerprint text NOT NULL, occurred_at timestamptz NOT NULL,
  tenant_id text NOT NULL DEFAULT '', release text NOT NULL DEFAULT '', level text NOT NULL DEFAULT 'error',
  account_id text NOT NULL DEFAULT '', device_id text NOT NULL DEFAULT '', payload jsonb NOT NULL,
  PRIMARY KEY(project_id,event_id)
);
CREATE INDEX IF NOT EXISTS gonvex_error_events_group ON gonvex_error_events(project_id,fingerprint,occurred_at DESC);
CREATE TABLE IF NOT EXISTS gonvex_error_groups (
  project_id text NOT NULL, fingerprint text NOT NULL, title text NOT NULL, culprit text NOT NULL DEFAULT '',
  level text NOT NULL DEFAULT 'error', status text NOT NULL DEFAULT 'unresolved', priority text NOT NULL DEFAULT 'medium',
  assignee text NOT NULL DEFAULT '', first_seen timestamptz NOT NULL, last_seen timestamptz NOT NULL,
  event_count bigint NOT NULL DEFAULT 0, tenants jsonb NOT NULL DEFAULT '{}'::jsonb,
  releases jsonb NOT NULL DEFAULT '{}'::jsonb, environments jsonb NOT NULL DEFAULT '{}'::jsonb,
  accounts jsonb NOT NULL DEFAULT '{}'::jsonb, devices jsonb NOT NULL DEFAULT '{}'::jsonb,
  latest_event jsonb NOT NULL, regression boolean NOT NULL DEFAULT false,
  PRIMARY KEY(project_id,fingerprint)
);
CREATE INDEX IF NOT EXISTS gonvex_error_groups_inbox ON gonvex_error_groups(project_id,status,last_seen DESC);
CREATE TABLE IF NOT EXISTS gonvex_performance_events (
  project_id text NOT NULL, event_id text NOT NULL, tenant_id text NOT NULL DEFAULT '',
  account_id text NOT NULL DEFAULT '', kind text NOT NULL, path text NOT NULL,
  reason text NOT NULL DEFAULT '', outcome text NOT NULL, error text NOT NULL DEFAULT '',
  client_sent_at_ms double precision, client_received_at_ms double precision NOT NULL,
  client_duration_ms double precision, trace jsonb, device jsonb,
  created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(project_id,event_id)
);
CREATE INDEX IF NOT EXISTS gonvex_performance_events_recent
  ON gonvex_performance_events(project_id,created_at DESC);
