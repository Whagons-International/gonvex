package identity

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// Queryer is the read surface used by PostgresMigrationStore. *sql.DB and
// *sql.Tx both implement it, as do small database/sql-compatible test doubles.
type Queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// MigrationDB is the database/sql-compatible surface needed by the concrete
// store. Keeping it as an interface allows the control-plane store to be used
// with either a pool or a transaction and keeps tenant authorization outside
// this package.
type MigrationDB interface {
	Execer
	Queryer
}

// TxBeginner is implemented by *sql.DB. A store uses it when available so an
// account, identity, and legacy mapping are committed together. Lightweight
// database doubles may omit it; in that case the three idempotent statements
// are executed through MigrationDB directly.
type TxBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

// PostgresMigrationStore persists identity-v2 migration state in the control
// plane. It intentionally has no connection or query path to a tenant DB.
type PostgresMigrationStore struct {
	DB MigrationDB
}

// LoadExistingAccounts reads the current Control Plane identity graph without
// installing or mutating schema. Migration planning deliberately uses this
// read-only path so --plan can never change the database it is inspecting.
func LoadExistingAccounts(ctx context.Context, db Queryer) ([]ExistingAccount, error) {
	if db == nil {
		return nil, fmt.Errorf("control-plane migration database is required")
	}
	var accountsTable sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('accounts')`).Scan(&accountsTable); err != nil {
		return nil, fmt.Errorf("inspect control-plane identity schema: %w", err)
	}
	if !accountsTable.Valid {
		return []ExistingAccount{}, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT id, auth_realm_id, email, name, avatar_url, disabled_at
		FROM accounts ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("load control-plane accounts (is identity-v2 schema installed?): %w", err)
	}
	defer rows.Close()

	accounts := make([]ExistingAccount, 0)
	byID := make(map[string]int)
	for rows.Next() {
		var account Account
		var disabledAt sql.NullTime
		if err := rows.Scan(&account.ID, &account.AuthRealmID, &account.Email, &account.Name, &account.AvatarURL, &disabledAt); err != nil {
			return nil, err
		}
		if disabledAt.Valid {
			account.DisabledAt = &disabledAt.Time
		}
		byID[account.ID] = len(accounts)
		accounts = append(accounts, ExistingAccount{Account: account, Identities: []AccountIdentity{}})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var identitiesTable sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('account_identities')`).Scan(&identitiesTable); err != nil {
		return nil, fmt.Errorf("inspect control-plane account identity schema: %w", err)
	}
	if !identitiesTable.Valid {
		return accounts, nil
	}

	identityRows, err := db.QueryContext(ctx, `SELECT account_id, provider, issuer, subject, email, verified_email
		FROM account_identities ORDER BY account_id, provider, issuer, subject`)
	if err != nil {
		return nil, fmt.Errorf("load control-plane account identities: %w", err)
	}
	defer identityRows.Close()
	for identityRows.Next() {
		var item AccountIdentity
		if err := identityRows.Scan(&item.AccountID, &item.Provider, &item.Issuer, &item.Subject, &item.Email, &item.VerifiedEmail); err != nil {
			return nil, err
		}
		index, ok := byID[item.AccountID]
		if !ok {
			return nil, fmt.Errorf("account identity references missing account %q", item.AccountID)
		}
		accounts[index].Identities = append(accounts[index].Identities, item)
	}
	if err := identityRows.Err(); err != nil {
		return nil, err
	}
	return accounts, nil
}

func (s PostgresMigrationStore) database() (MigrationDB, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("control-plane migration database is required")
	}
	return s.DB, nil
}

func (s PostgresMigrationStore) BeginIdentityMigration(ctx context.Context, run MigrationRun) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	if strings.TrimSpace(run.ID) == "" || strings.TrimSpace(run.Source) == "" || strings.TrimSpace(run.PlanChecksum) == "" {
		return fmt.Errorf("migration run id, source, and plan checksum are required")
	}
	if strings.TrimSpace(run.Status) == "" {
		run.Status = "running"
	}
	_, err = db.ExecContext(ctx, `INSERT INTO identity_migration_runs (
		id, source, plan_checksum, status, started_at, updated_at
	) VALUES ($1, $2, $3, $4, now(), now())
	ON CONFLICT (id) DO UPDATE SET
		status = CASE WHEN identity_migration_runs.status = 'complete' THEN identity_migration_runs.status ELSE EXCLUDED.status END,
		started_at = COALESCE(identity_migration_runs.started_at, now()),
		updated_at = now()
	WHERE identity_migration_runs.source = EXCLUDED.source
	  AND identity_migration_runs.plan_checksum = EXCLUDED.plan_checksum`, run.ID, run.Source, run.PlanChecksum, run.Status)
	if err != nil {
		return err
	}
	var storedSource, storedChecksum string
	err = db.QueryRowContext(ctx, `SELECT source, plan_checksum FROM identity_migration_runs WHERE id = $1`, run.ID).Scan(&storedSource, &storedChecksum)
	if err != nil {
		return err
	}
	if storedSource != run.Source || storedChecksum != run.PlanChecksum {
		return fmt.Errorf("migration run %q already exists with a different source or plan checksum", run.ID)
	}
	return nil
}

func (s PostgresMigrationStore) SaveCollision(ctx context.Context, runID string, resolution LegacyAccountResolution) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(resolution.Legacy.Source) == "" || strings.TrimSpace(resolution.Legacy.LegacyUserID) == "" {
		return fmt.Errorf("collision run id, source, and legacy user id are required")
	}
	candidates, err := json.Marshal(resolution.Candidates)
	if err != nil {
		return err
	}
	state := "needs_review"
	if !resolution.NeedsReview {
		state = "resolved"
	}
	sourceKey := resolution.Legacy.Source + "\x00" + resolution.Legacy.LegacyUserID
	_, err = db.ExecContext(ctx, `INSERT INTO identity_migration_collisions (
		run_id, kind, source_key, candidates, resolution, resolved_account_id
	) VALUES ($1, $2, $3, $4::jsonb, $5, $6)
	ON CONFLICT (run_id, kind, source_key) DO UPDATE SET
		candidates = EXCLUDED.candidates,
		resolution = EXCLUDED.resolution,
		resolved_account_id = EXCLUDED.resolved_account_id`, runID, string(resolution.Kind), sourceKey, string(candidates), state, resolution.Account.ID)
	return err
}

func (s PostgresMigrationStore) LoadCheckpoint(ctx context.Context, runID, scope string) (MigrationCheckpoint, error) {
	db, err := s.database()
	if err != nil {
		return MigrationCheckpoint{}, err
	}
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(scope) == "" {
		return MigrationCheckpoint{}, fmt.Errorf("checkpoint run id and scope are required")
	}
	var checkpoint MigrationCheckpoint
	err = db.QueryRowContext(ctx, `SELECT run_id, scope, completed_index, last_legacy_user_id,
		rows_processed, checksum, status
		FROM identity_migration_checkpoints WHERE run_id = $1 AND scope = $2`, runID, scope).Scan(
		&checkpoint.RunID, &checkpoint.Scope, &checkpoint.CompletedIndex, &checkpoint.LastLegacyUserID,
		&checkpoint.RowsProcessed, &checkpoint.Checksum, &checkpoint.Status,
	)
	if err == sql.ErrNoRows {
		return MigrationCheckpoint{RunID: runID, Scope: scope, CompletedIndex: -1, Status: "pending"}, nil
	}
	if err != nil {
		return MigrationCheckpoint{}, err
	}
	return checkpoint, nil
}

func (s PostgresMigrationStore) ApplyResolution(ctx context.Context, resolution LegacyAccountResolution) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	if strings.TrimSpace(resolution.Account.ID) == "" {
		return fmt.Errorf("resolved account id is required")
	}
	if strings.TrimSpace(resolution.Legacy.Source) == "" || strings.TrimSpace(resolution.Legacy.LegacyUserID) == "" {
		return fmt.Errorf("legacy source and user id are required")
	}
	if beginner, ok := db.(TxBeginner); ok {
		tx, err := beginner.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := applyResolution(ctx, tx, resolution); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	return applyResolution(ctx, db, resolution)
}

func applyResolution(ctx context.Context, db MigrationDB, resolution LegacyAccountResolution) error {
	account := resolution.Account
	_, err := db.ExecContext(ctx, `INSERT INTO accounts (
		id, auth_realm_id, email, name, avatar_url, disabled_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, now())
	ON CONFLICT (id) DO UPDATE SET
		auth_realm_id = CASE WHEN EXCLUDED.auth_realm_id <> '' THEN EXCLUDED.auth_realm_id ELSE accounts.auth_realm_id END,
		email = CASE WHEN EXCLUDED.email <> '' THEN EXCLUDED.email ELSE accounts.email END,
		name = CASE WHEN EXCLUDED.name <> '' THEN EXCLUDED.name ELSE accounts.name END,
		avatar_url = CASE WHEN EXCLUDED.avatar_url <> '' THEN EXCLUDED.avatar_url ELSE accounts.avatar_url END,
		disabled_at = COALESCE(EXCLUDED.disabled_at, accounts.disabled_at),
		updated_at = now()`, account.ID, account.AuthRealmID, account.Email, account.Name, account.AvatarURL, account.DisabledAt)
	if err != nil {
		return err
	}

	identity := resolution.Identity
	if strings.TrimSpace(identity.Provider) != "" && strings.TrimSpace(identity.Subject) != "" {
		result, err := db.ExecContext(ctx, `INSERT INTO account_identities (
			project_id, account_id, provider, issuer, subject, email, verified_email, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (project_id, provider, issuer, subject) DO UPDATE SET
			email = CASE WHEN EXCLUDED.email <> '' THEN EXCLUDED.email ELSE account_identities.email END,
			verified_email = account_identities.verified_email OR EXCLUDED.verified_email,
			updated_at = now()
		WHERE account_identities.account_id = EXCLUDED.account_id`, account.AuthRealmID, identity.AccountID, identity.Provider, identity.Issuer, identity.Subject, identity.Email, identity.VerifiedEmail)
		if err != nil {
			return err
		}
		if affected, err := result.RowsAffected(); err == nil && affected == 0 {
			return fmt.Errorf("identity %q/%q/%q is already assigned to another account", identity.Provider, identity.Issuer, identity.Subject)
		}
	}

	legacy := resolution.Legacy
	result, err := db.ExecContext(ctx, `INSERT INTO legacy_account_map (
		source, legacy_user_id, account_id, resolution, updated_at
	) VALUES ($1, $2, $3, $4, now())
	ON CONFLICT (source, legacy_user_id) DO UPDATE SET
		resolution = EXCLUDED.resolution,
		updated_at = now()
	WHERE legacy_account_map.account_id = EXCLUDED.account_id`, legacy.Source, legacy.LegacyUserID, account.ID, string(resolution.Kind))
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return fmt.Errorf("legacy identity %q/%q is already mapped to another account", legacy.Source, legacy.LegacyUserID)
	}
	return nil
}

func (s PostgresMigrationStore) SaveCheckpoint(ctx context.Context, checkpoint MigrationCheckpoint) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	if strings.TrimSpace(checkpoint.RunID) == "" || strings.TrimSpace(checkpoint.Scope) == "" {
		return fmt.Errorf("checkpoint run id and scope are required")
	}
	if checkpoint.CompletedIndex < -1 || checkpoint.RowsProcessed < 0 {
		return fmt.Errorf("checkpoint indexes cannot be negative beyond the initial index")
	}
	current, err := s.LoadCheckpoint(ctx, checkpoint.RunID, checkpoint.Scope)
	if err != nil {
		return err
	}
	if current.Checksum != "" && checkpoint.Checksum != "" && current.Checksum != checkpoint.Checksum {
		return fmt.Errorf("checkpoint %q/%q belongs to a different plan checksum", checkpoint.RunID, checkpoint.Scope)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO identity_migration_checkpoints (
		run_id, scope, completed_index, last_legacy_user_id, rows_processed, checksum, status, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
	ON CONFLICT (run_id, scope) DO UPDATE SET
		completed_index = CASE WHEN EXCLUDED.completed_index > identity_migration_checkpoints.completed_index THEN EXCLUDED.completed_index ELSE identity_migration_checkpoints.completed_index END,
		last_legacy_user_id = CASE WHEN EXCLUDED.completed_index >= identity_migration_checkpoints.completed_index THEN EXCLUDED.last_legacy_user_id ELSE identity_migration_checkpoints.last_legacy_user_id END,
		rows_processed = GREATEST(identity_migration_checkpoints.rows_processed, EXCLUDED.rows_processed),
		checksum = CASE WHEN identity_migration_checkpoints.checksum = '' THEN EXCLUDED.checksum ELSE identity_migration_checkpoints.checksum END,
		status = CASE WHEN EXCLUDED.completed_index >= identity_migration_checkpoints.completed_index THEN EXCLUDED.status ELSE identity_migration_checkpoints.status END,
		updated_at = now()`, checkpoint.RunID, checkpoint.Scope, checkpoint.CompletedIndex, checkpoint.LastLegacyUserID, checkpoint.RowsProcessed, checkpoint.Checksum, checkpoint.Status)
	return err
}

func (s PostgresMigrationStore) CompleteIdentityMigration(ctx context.Context, runID string) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	if strings.TrimSpace(runID) == "" {
		return fmt.Errorf("migration run id is required")
	}
	result, err := db.ExecContext(ctx, `UPDATE identity_migration_runs
		SET status = 'complete', completed_at = COALESCE(completed_at, now()), updated_at = now()
		WHERE id = $1`, runID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return fmt.Errorf("identity migration run %q does not exist", runID)
	}
	return nil
}

func (s PostgresMigrationStore) VerifyIdentityMigration(ctx context.Context, plan MigrationPlan) ([]VerificationFinding, error) {
	db, err := s.database()
	if err != nil {
		return nil, err
	}
	findings := make([]VerificationFinding, 0)
	var mapped int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM legacy_account_map WHERE source = $1`, plan.Source).Scan(&mapped); err != nil {
		return nil, err
	}
	if mapped != int64(len(plan.Items)) {
		findings = append(findings, VerificationFinding{Code: "missing_legacy_maps", Scope: plan.Source, Detail: fmt.Sprintf("expected %d mapped legacy rows, found %d", len(plan.Items), mapped)})
	}

	var runStatus string
	err = db.QueryRowContext(ctx, `SELECT status FROM identity_migration_runs WHERE id = $1`, plan.RunID).Scan(&runStatus)
	if err == sql.ErrNoRows {
		findings = append(findings, VerificationFinding{Code: "missing_migration_run", Scope: plan.RunID, Detail: "migration run was not persisted"})
	} else if err != nil {
		return nil, err
	} else if runStatus != "complete" {
		findings = append(findings, VerificationFinding{Code: "migration_not_complete", Scope: plan.RunID, Detail: "migration run status is " + runStatus})
	}

	var collisions int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM identity_migration_collisions WHERE run_id = $1`, plan.RunID).Scan(&collisions); err != nil {
		return nil, err
	}
	if collisions != int64(len(plan.Collisions)) {
		findings = append(findings, VerificationFinding{Code: "collision_records_mismatch", Scope: plan.RunID, Detail: fmt.Sprintf("expected %d persisted collisions, found %d", len(plan.Collisions), collisions)})
	}

	accountIDs := uniqueAccountIDs(plan.Items)
	if len(accountIDs) > 0 {
		var accounts int64
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM accounts WHERE id IN (`+sqlPlaceholders(len(accountIDs))+`)`, stringArgs(accountIDs)...).Scan(&accounts); err != nil {
			return nil, err
		}
		if accounts != int64(len(accountIDs)) {
			findings = append(findings, VerificationFinding{Code: "missing_accounts", Scope: plan.Source, Detail: fmt.Sprintf("expected %d accounts, found %d", len(accountIDs), accounts)})
		}
	}
	identityRows := 0
	for _, item := range plan.Items {
		if strings.TrimSpace(item.Identity.Provider) != "" && strings.TrimSpace(item.Identity.Subject) != "" {
			identityRows++
		}
	}
	if identityRows > 0 {
		var identities int64
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM account_identities WHERE account_id IN (`+sqlPlaceholders(len(accountIDs))+`)`, stringArgs(accountIDs)...).Scan(&identities); err != nil {
			return nil, err
		}
		if identities < int64(identityRows) {
			findings = append(findings, VerificationFinding{Code: "missing_account_identities", Scope: plan.Source, Detail: fmt.Sprintf("expected at least %d provider identities, found %d", identityRows, identities)})
		}
	}
	for _, collision := range plan.Collisions {
		findings = append(findings, VerificationFinding{Code: "identity_collision", Scope: collision.Legacy.Source, LegacyID: collision.Legacy.LegacyUserID, AccountID: collision.Account.ID, Detail: "identity requires explicit review"})
	}
	return findings, nil
}

func uniqueAccountIDs(items []LegacyAccountResolution) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if item.Account.ID != "" && !seen[item.Account.ID] {
			seen[item.Account.ID] = true
			result = append(result, item.Account.ID)
		}
	}
	return result
}

func sqlPlaceholders(count int) string {
	values := make([]string, count)
	for index := range values {
		values[index] = fmt.Sprintf("$%d", index+1)
	}
	return strings.Join(values, ", ")
}

func stringArgs(values []string) []any {
	args := make([]any, len(values))
	for index, value := range values {
		args[index] = value
	}
	return args
}
