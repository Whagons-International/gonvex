-- Control Plane schema (SQLite). Timestamps are INTEGER Unix milliseconds,
-- booleans are 0/1 and JSON columns hold JSON text. Every statement is
-- idempotent. Columns added after a table first shipped are also listed in
-- `ADDITIVE_COLUMNS` in provision.rs, which adds them to older files before
-- this script runs so the indexes below always find their columns.
CREATE TABLE IF NOT EXISTS gonvex_runtime_projects (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, environment TEXT NOT NULL DEFAULT 'development',
  database_name TEXT NOT NULL DEFAULT '', database_mode TEXT NOT NULL DEFAULT 'single',
  database_url TEXT NOT NULL DEFAULT '', storage_bucket TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'active', description TEXT NOT NULL DEFAULT '',
  project_key TEXT NOT NULL DEFAULT '', provisioned BOOLEAN NOT NULL DEFAULT 1,
  runtime_created BOOLEAN NOT NULL DEFAULT 1, test_tab BOOLEAN NOT NULL DEFAULT 0,
  error_tracking_enabled BOOLEAN NOT NULL DEFAULT 0, owner_email TEXT NOT NULL DEFAULT '',
  auth_mode TEXT NOT NULL DEFAULT 'gonvex-native',
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE TABLE IF NOT EXISTS accounts (
  id TEXT PRIMARY KEY, auth_realm_id TEXT NOT NULL DEFAULT '', email TEXT NOT NULL DEFAULT '',
  name TEXT NOT NULL DEFAULT '', avatar_url TEXT NOT NULL DEFAULT '', disabled_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS accounts_by_email ON accounts(lower(email)) WHERE email<>'';
CREATE TABLE IF NOT EXISTS account_identities (
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  project_id TEXT NOT NULL DEFAULT '', provider TEXT NOT NULL, issuer TEXT NOT NULL DEFAULT '',
  subject TEXT NOT NULL, email TEXT NOT NULL DEFAULT '', verified_email BOOLEAN NOT NULL DEFAULT 0,
  sign_in_provider TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,provider,issuer,subject)
);
CREATE INDEX IF NOT EXISTS account_identities_by_account ON account_identities(account_id);
CREATE INDEX IF NOT EXISTS account_identities_by_verified_email ON account_identities(lower(email))
  WHERE verified_email AND email<>'';
CREATE TABLE IF NOT EXISTS gonvex_runtime_tenants (
  relationship_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL, name TEXT NOT NULL, database_alias TEXT NOT NULL DEFAULT '',
  database_name TEXT NOT NULL DEFAULT '', database_url TEXT NOT NULL DEFAULT '', domain TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'active', description TEXT NOT NULL DEFAULT '', timezone TEXT NOT NULL DEFAULT 'UTC',
  profile JSON NOT NULL DEFAULT '{}', seat_limit INTEGER, provisioned BOOLEAN NOT NULL DEFAULT 0,
  runtime_created BOOLEAN NOT NULL DEFAULT 0, deleted_at INTEGER, deletion_idempotency_key TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  UNIQUE(project_id,tenant_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_runtime_tenants_project_database
  ON gonvex_runtime_tenants(project_id,database_name) WHERE database_name<>'';
CREATE TABLE IF NOT EXISTS account_tenant_index (
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, tenant_id TEXT NOT NULL,
  member_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
  tenant_membership_revision INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(account_id,tenant_id), UNIQUE(tenant_id,member_id)
);
CREATE INDEX IF NOT EXISTS account_tenant_index_by_tenant ON account_tenant_index(tenant_id,status,account_id);
CREATE TABLE IF NOT EXISTS gonvex_runtime_manifests (
  project_id TEXT PRIMARY KEY, manifest JSON NOT NULL, module_hash TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE TABLE IF NOT EXISTS gonvex_runtime_reducer_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT, project_id TEXT NOT NULL DEFAULT '', kind TEXT NOT NULL,
  entry JSON NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
UPDATE gonvex_runtime_reducer_logs
SET kind='reducer',entry=json_set(entry,'$.kind','reducer')
WHERE kind IN ('mutation','internalMutation');
CREATE INDEX IF NOT EXISTS gonvex_runtime_reducer_logs_by_project
  ON gonvex_runtime_reducer_logs(project_id,id DESC);
CREATE TABLE IF NOT EXISTS gonvex_project_members (
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  email TEXT NOT NULL, name TEXT NOT NULL DEFAULT '', role TEXT NOT NULL DEFAULT 'dev',
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,email)
);
CREATE TABLE IF NOT EXISTS gonvex_project_invitations (
  id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  email TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'dev', invited_by TEXT NOT NULL DEFAULT '',
  token_hash TEXT NOT NULL DEFAULT '', expires_at INTEGER NOT NULL, accepted_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  UNIQUE(project_id,email)
);
CREATE TABLE IF NOT EXISTS gonvex_dashboard_notifications (
  id TEXT PRIMARY KEY, email TEXT NOT NULL, kind TEXT NOT NULL, title TEXT NOT NULL,
  body TEXT NOT NULL DEFAULT '', project_id TEXT NOT NULL DEFAULT '', metadata JSON NOT NULL DEFAULT '{}',
  read_at INTEGER, created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS gonvex_dashboard_notifications_by_email
  ON gonvex_dashboard_notifications(email,created_at DESC);
CREATE TABLE IF NOT EXISTS gonvex_dashboard_accounts (
  email TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'standard',
  password_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  session_revision TEXT NOT NULL DEFAULT (lower(hex(randomblob(16))))
);
-- A random revision also prevents deletion/recreation and role-change rollback
-- from reviving an old dashboard session. Enforced for every SQL writer.
-- Older files gain the column with a constant default (ADDITIVE_COLUMNS);
-- give each such row its own random revision.
UPDATE gonvex_dashboard_accounts SET session_revision = lower(hex(randomblob(16)))
  WHERE session_revision = '';
CREATE TRIGGER IF NOT EXISTS gonvex_dashboard_security_change
AFTER UPDATE OF password_hash, role ON gonvex_dashboard_accounts
FOR EACH ROW WHEN NEW.password_hash IS NOT OLD.password_hash OR NEW.role IS NOT OLD.role
BEGIN
  UPDATE gonvex_dashboard_accounts SET session_revision = lower(hex(randomblob(16)))
    WHERE email = NEW.email;
END;
CREATE TABLE IF NOT EXISTS gonvex_account_access_tokens (
  id TEXT PRIMARY KEY, owner_email TEXT NOT NULL, name TEXT NOT NULL, token_prefix TEXT NOT NULL,
  token_hash TEXT NOT NULL UNIQUE, permissions JSON NOT NULL DEFAULT '[]',
  expires_at INTEGER, last_used_at INTEGER, revoked_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE TABLE IF NOT EXISTS gonvex_project_env (
  project_id TEXT NOT NULL, name TEXT NOT NULL, value TEXT NOT NULL DEFAULT '',
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,name)
);
CREATE TABLE IF NOT EXISTS gonvex_auth_providers (
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  provider TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, signup_mode TEXT NOT NULL DEFAULT 'personal',
  azure_tenant_id TEXT NOT NULL DEFAULT '', client_id TEXT NOT NULL DEFAULT '', client_secret_encrypted BLOB,
  issuer TEXT NOT NULL DEFAULT '', audience TEXT NOT NULL DEFAULT '', jwks_url TEXT NOT NULL DEFAULT '',
  firebase_project_id TEXT NOT NULL DEFAULT '', firebase_tenant_id TEXT NOT NULL DEFAULT '',
  admin_credentials_encrypted BLOB,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,provider)
);
CREATE TABLE IF NOT EXISTS gonvex_auth_redirect_uris (
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  provider TEXT NOT NULL, redirect_uri TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,provider,redirect_uri)
);
CREATE TABLE IF NOT EXISTS gonvex_auth_transactions (
  token_hash TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  redirect_uri TEXT NOT NULL, app_state TEXT NOT NULL, code_challenge TEXT NOT NULL,
  nonce TEXT NOT NULL, google_redirect_uri TEXT NOT NULL, provider TEXT NOT NULL DEFAULT 'google',
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS gonvex_auth_transactions_by_expiry ON gonvex_auth_transactions(expires_at);
CREATE TABLE IF NOT EXISTS gonvex_auth_codes (
  code_hash TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  redirect_uri TEXT NOT NULL, code_challenge TEXT NOT NULL,
  expires_at INTEGER NOT NULL, used_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS gonvex_auth_codes_by_expiry ON gonvex_auth_codes(expires_at);
CREATE TABLE IF NOT EXISTS gonvex_auth_sessions (
  token_hash TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, family_id TEXT NOT NULL DEFAULT '',
  expires_at INTEGER NOT NULL, revoked_at INTEGER, last_seen_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS gonvex_auth_sessions_by_account ON gonvex_auth_sessions(project_id,account_id,expires_at DESC);
CREATE INDEX IF NOT EXISTS gonvex_auth_sessions_by_family ON gonvex_auth_sessions(family_id) WHERE family_id<>'';
CREATE TABLE IF NOT EXISTS gonvex_auth_refresh_tokens (
  token_hash TEXT PRIMARY KEY, family_id TEXT NOT NULL,
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, expires_at INTEGER NOT NULL,
  used_at INTEGER, revoked_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS gonvex_auth_refresh_tokens_by_family ON gonvex_auth_refresh_tokens(family_id,created_at DESC);
CREATE TABLE IF NOT EXISTS gonvex_account_passwords (
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, password_hash TEXT NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,account_id)
);
CREATE TABLE IF NOT EXISTS gonvex_auth_identity_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, provider TEXT NOT NULL,
  issuer TEXT NOT NULL, subject TEXT NOT NULL, resolution TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE TABLE IF NOT EXISTS gonvex_auth_membership_invitations (
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL, email TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'member',
  permissions JSON NOT NULL DEFAULT '{}', invited_by TEXT NOT NULL DEFAULT '',
  expires_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER) + 604800000),
  id TEXT, token_hash TEXT, revoked_at INTEGER, accepted_at INTEGER,
  accepted_account_id TEXT, accepted_idempotency_key TEXT,
  team_ids JSON NOT NULL DEFAULT '[]', allowed_auth_providers JSON NOT NULL DEFAULT '[]',
  application_payload JSON NOT NULL DEFAULT '{}', handoff_state TEXT NOT NULL DEFAULT 'pending',
  handoff_command_id TEXT NOT NULL DEFAULT '', completed_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,tenant_id,email),
  FOREIGN KEY(project_id,tenant_id) REFERENCES gonvex_runtime_tenants(project_id,tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS gonvex_auth_membership_invitations_by_email
  ON gonvex_auth_membership_invitations(project_id,email);
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_auth_membership_invitations_token
  ON gonvex_auth_membership_invitations(token_hash) WHERE token_hash IS NOT NULL AND token_hash<>'';
CREATE TABLE IF NOT EXISTS gonvex_member_login_provisioning (
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL, email TEXT NOT NULL,
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  created_by TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,tenant_id,email), UNIQUE(project_id,tenant_id,account_id)
);
CREATE TABLE IF NOT EXISTS gonvex_tenant_provisioning (
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  idempotency_key TEXT NOT NULL, tenant_id TEXT NOT NULL, database_name TEXT NOT NULL,
  database_alias TEXT NOT NULL, name TEXT NOT NULL, domain TEXT NOT NULL DEFAULT '',
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,idempotency_key), UNIQUE(project_id,tenant_id), UNIQUE(database_name)
);
CREATE TABLE IF NOT EXISTS gonvex_control_idempotency (
  project_id TEXT NOT NULL, subject_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
  kind TEXT NOT NULL, path TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', result JSON,
  error TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,subject_id,idempotency_key)
);
CREATE TABLE IF NOT EXISTS gonvex_agent_claim_tokens (
  id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL UNIQUE, permissions JSON NOT NULL DEFAULT '[]',
  expires_at INTEGER NOT NULL, claimed_at INTEGER, claimed_account_id TEXT, revoked_at INTEGER,
  created_by TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE TABLE IF NOT EXISTS gonvex_control_settings (
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  kind TEXT NOT NULL, scope_id TEXT NOT NULL DEFAULT '', value JSON NOT NULL DEFAULT '{}',
  updated_by TEXT NOT NULL DEFAULT '',
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,kind,scope_id)
);
CREATE TABLE IF NOT EXISTS gonvex_support_sessions (
  id TEXT PRIMARY KEY, project_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '',
  account_id TEXT NOT NULL DEFAULT '', connection_id TEXT NOT NULL DEFAULT '', release TEXT NOT NULL DEFAULT '',
  environment TEXT NOT NULL DEFAULT '',
  last_seen_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE TABLE IF NOT EXISTS gonvex_support_commands (
  id TEXT PRIMARY KEY, project_id TEXT NOT NULL,
  session_id TEXT NOT NULL REFERENCES gonvex_support_sessions(id) ON DELETE CASCADE,
  kind TEXT NOT NULL, payload JSON NOT NULL DEFAULT '{}', created_by TEXT NOT NULL,
  acknowledged_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE TABLE IF NOT EXISTS gonvex_scheduled_jobs (
  id TEXT PRIMARY KEY, project_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '',
  function_path TEXT NOT NULL, args JSON, run_at INTEGER NOT NULL,
  scheduled_for INTEGER NOT NULL, cron_name TEXT NOT NULL DEFAULT '',
  provenance JSON NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','completed')),
  claim_sequence INTEGER NOT NULL DEFAULT 0, claim_token TEXT NOT NULL DEFAULT '',
  lease_until INTEGER,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  completed_at INTEGER, wake_key TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_due_oneshot
  ON gonvex_scheduled_jobs(run_at,id) WHERE status='pending' AND cron_name='';
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_due_cron
  ON gonvex_scheduled_jobs(run_at,id) WHERE status='pending' AND cron_name<>'';
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_completed
  ON gonvex_scheduled_jobs(completed_at,id) WHERE status='completed';
-- Claims take one-shot jobs before crons, then the oldest first. Neither partial
-- index above serves that order, so without this one every claim scanned the
-- whole table, completed history included.
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_claim_order
  ON gonvex_scheduled_jobs((cron_name<>''),run_at,id) WHERE status='pending';
-- A cron has at most one pending run per tenant. A scheduler that falls behind
-- then skips missed runs instead of piling up stale copies. Older duplicates are
-- dropped first so databases that already accumulated a backlog can upgrade.
DELETE FROM gonvex_scheduled_jobs WHERE id IN (
  SELECT id FROM (
    SELECT id, row_number() OVER (
      PARTITION BY project_id,tenant_id,cron_name ORDER BY run_at DESC,id DESC
    ) AS newest
    FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name<>''
  ) ranked WHERE newest>1
);
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_scheduled_jobs_one_pending_cron
  ON gonvex_scheduled_jobs(project_id,tenant_id,cron_name) WHERE status='pending' AND cron_name<>'';
-- scheduler.wakeAt keeps one unclaimed wake per tenant, function and args. A
-- claimed wake leaves this index, so a wake that arrives mid-run queues one more.
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_scheduled_jobs_one_pending_wake
  ON gonvex_scheduled_jobs(project_id,tenant_id,wake_key)
  WHERE status='pending' AND claim_token='' AND wake_key<>'';
CREATE TABLE IF NOT EXISTS gonvex_impersonation_grants (
  id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL UNIQUE, actor_account_id TEXT NOT NULL,
  target_account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, tenant_id TEXT NOT NULL,
  reason TEXT NOT NULL, expires_at INTEGER NOT NULL, used_at INTEGER,
  used_connection_id TEXT NOT NULL DEFAULT '', reconnect_token_hash TEXT NOT NULL DEFAULT '', revoked_at INTEGER,
  delegation_actor JSON,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_impersonation_grants_reconnect_token
  ON gonvex_impersonation_grants(reconnect_token_hash) WHERE reconnect_token_hash<>'';
CREATE TABLE IF NOT EXISTS gonvex_demo_accounts (
  project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, tenant_id TEXT NOT NULL,
  label TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,account_id)
);
CREATE TABLE IF NOT EXISTS gonvex_error_events (
  project_id TEXT NOT NULL, event_id TEXT NOT NULL, fingerprint TEXT NOT NULL, occurred_at INTEGER NOT NULL,
  tenant_id TEXT NOT NULL DEFAULT '', release TEXT NOT NULL DEFAULT '', level TEXT NOT NULL DEFAULT 'error',
  account_id TEXT NOT NULL DEFAULT '', device_id TEXT NOT NULL DEFAULT '', payload JSON NOT NULL,
  PRIMARY KEY(project_id,event_id)
);
CREATE INDEX IF NOT EXISTS gonvex_error_events_group ON gonvex_error_events(project_id,fingerprint,occurred_at DESC);
CREATE TABLE IF NOT EXISTS gonvex_error_groups (
  project_id TEXT NOT NULL, fingerprint TEXT NOT NULL, title TEXT NOT NULL, culprit TEXT NOT NULL DEFAULT '',
  level TEXT NOT NULL DEFAULT 'error', status TEXT NOT NULL DEFAULT 'unresolved', priority TEXT NOT NULL DEFAULT 'medium',
  assignee TEXT NOT NULL DEFAULT '', first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL,
  event_count INTEGER NOT NULL DEFAULT 0, tenants JSON NOT NULL DEFAULT '{}',
  releases JSON NOT NULL DEFAULT '{}', environments JSON NOT NULL DEFAULT '{}',
  accounts JSON NOT NULL DEFAULT '{}', devices JSON NOT NULL DEFAULT '{}',
  latest_event JSON NOT NULL, regression BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY(project_id,fingerprint)
);
CREATE INDEX IF NOT EXISTS gonvex_error_groups_inbox ON gonvex_error_groups(project_id,status,last_seen DESC);
CREATE TABLE IF NOT EXISTS gonvex_performance_events (
  project_id TEXT NOT NULL, event_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '',
  account_id TEXT NOT NULL DEFAULT '', kind TEXT NOT NULL, path TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '', outcome TEXT NOT NULL, error TEXT NOT NULL DEFAULT '',
  client_sent_at_ms REAL, client_received_at_ms REAL NOT NULL,
  client_duration_ms REAL, trace JSON, device JSON,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY(project_id,event_id)
);
CREATE INDEX IF NOT EXISTS gonvex_performance_events_recent
  ON gonvex_performance_events(project_id,created_at DESC);

-- Upload signatures are bearer credentials. Keep successful proxy PUTs consumed
-- even if an application later deletes the object or its tenant metadata.
CREATE TABLE IF NOT EXISTS gonvex_storage_uploads (
  object_key TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT '',
  tenant_id TEXT NOT NULL DEFAULT '',
  file_id TEXT NOT NULL DEFAULT '',
  consumed_at INTEGER
);
