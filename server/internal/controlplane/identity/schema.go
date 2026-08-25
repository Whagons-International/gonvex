package identity

import (
	"context"
	"database/sql"
)

// Execer is the smallest database surface required by InstallSchema. Keeping
// this interface local makes the schema usable with *sql.DB, *sql.Tx, and the
// runtime's test doubles without coupling the identity package to a driver.
type Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// SchemaStatements are deliberately plain PostgreSQL statements. The control
// plane is a projection and directory; tenant databases remain the authority
// for whether a member may enter a tenant.
var SchemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS accounts (
		id TEXT PRIMARY KEY,
		auth_realm_id TEXT NOT NULL DEFAULT '',
		email TEXT NOT NULL DEFAULT '',
		name TEXT NOT NULL DEFAULT '',
		avatar_url TEXT NOT NULL DEFAULT '',
		disabled_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS accounts_by_email ON accounts (lower(email)) WHERE email <> ''`,
	`CREATE TABLE IF NOT EXISTS account_identities (
		account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
		project_id TEXT NOT NULL DEFAULT '',
		provider TEXT NOT NULL,
		issuer TEXT NOT NULL DEFAULT '',
		subject TEXT NOT NULL,
		email TEXT NOT NULL DEFAULT '',
		verified_email BOOLEAN NOT NULL DEFAULT FALSE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (provider, issuer, subject)
	)`,
	`ALTER TABLE account_identities ADD COLUMN IF NOT EXISTS project_id TEXT NOT NULL DEFAULT ''`,
	`UPDATE account_identities identity SET project_id=account.auth_realm_id FROM accounts account WHERE account.id=identity.account_id AND identity.project_id=''`,
	`DO $$ BEGIN
		IF EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conrelid='account_identities'::regclass AND conname='account_identities_pkey'
			AND pg_get_constraintdef(oid) NOT LIKE '%project_id%'
		) THEN
			ALTER TABLE account_identities DROP CONSTRAINT account_identities_pkey;
			ALTER TABLE account_identities ADD PRIMARY KEY(project_id,provider,issuer,subject);
		END IF;
	END $$`,
	`CREATE INDEX IF NOT EXISTS account_identities_by_account ON account_identities (account_id)`,
	`CREATE INDEX IF NOT EXISTS account_identities_by_verified_email
		ON account_identities (lower(email)) WHERE verified_email AND email <> ''`,
	`CREATE TABLE IF NOT EXISTS account_tenant_index (
		account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
		tenant_id TEXT NOT NULL,
		member_id TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'active',
		tenant_membership_revision BIGINT NOT NULL DEFAULT 0,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (account_id, tenant_id),
		UNIQUE (tenant_id, member_id)
	)`,
	`CREATE INDEX IF NOT EXISTS account_tenant_index_by_tenant
		ON account_tenant_index (tenant_id, status, account_id)`,
	`CREATE TABLE IF NOT EXISTS legacy_account_map (
		source TEXT NOT NULL,
		legacy_user_id TEXT NOT NULL,
		account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
		resolution TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (source, legacy_user_id)
	)`,
	`CREATE INDEX IF NOT EXISTS legacy_account_map_by_account ON legacy_account_map (account_id)`,
	`CREATE TABLE IF NOT EXISTS identity_migration_runs (
		id TEXT PRIMARY KEY,
		source TEXT NOT NULL,
		plan_checksum TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'planned',
		started_at TIMESTAMPTZ,
		completed_at TIMESTAMPTZ,
		error TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE TABLE IF NOT EXISTS identity_migration_checkpoints (
		run_id TEXT NOT NULL REFERENCES identity_migration_runs(id) ON DELETE CASCADE,
		scope TEXT NOT NULL,
		completed_index BIGINT NOT NULL DEFAULT -1,
		last_legacy_user_id TEXT NOT NULL DEFAULT '',
		rows_processed BIGINT NOT NULL DEFAULT 0,
		checksum TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'pending',
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (run_id, scope)
	)`,
	`CREATE TABLE IF NOT EXISTS identity_migration_collisions (
		run_id TEXT NOT NULL REFERENCES identity_migration_runs(id) ON DELETE CASCADE,
		kind TEXT NOT NULL,
		source_key TEXT NOT NULL,
		candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
		resolution TEXT NOT NULL DEFAULT 'needs_review',
		resolved_account_id TEXT NOT NULL DEFAULT '',
		reviewed_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (run_id, kind, source_key)
	)`,
}

// InstallSchema is idempotent and safe to call during runtime startup or a
// migration command. It intentionally does not drop or rewrite legacy data.
func InstallSchema(ctx context.Context, db Execer) error {
	for _, statement := range SchemaStatements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
