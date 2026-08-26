use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, Row, Transaction};

pub const MIGRATION_SCOPE: &str =
    "accounts, auth credentials, tenant members, and derived tenant directory projection";

type DynError = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, DynError>;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Account {
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub auth_realm_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub email: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub avatar_url: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_timestamp"
    )]
    pub disabled_at: Option<DateTime<Utc>>,
}

fn serialize_optional_timestamp<S>(
    value: &Option<DateTime<Utc>>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(value) => {
            serializer.serialize_str(&value.to_rfc3339_opts(SecondsFormat::AutoSi, true))
        }
        None => serializer.serialize_none(),
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountIdentity {
    pub account_id: String,
    pub provider: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub issuer: String,
    pub subject: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub email: String,
    pub verified_email: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyIdentity {
    #[serde(default)]
    pub source: String,
    pub legacy_user_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub provider: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub issuer: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub subject: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub email: String,
    #[serde(default)]
    pub email_verified: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub avatar_url: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExistingAccount {
    pub account: Account,
    #[serde(default)]
    pub identities: Vec<AccountIdentity>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionKind {
    ProviderSubject,
    VerifiedEmail,
    #[default]
    NewAccount,
    Collision,
}

impl ResolutionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::ProviderSubject => "provider_subject",
            Self::VerifiedEmail => "verified_email",
            Self::NewAccount => "new_account",
            Self::Collision => "collision",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyAccountResolution {
    pub legacy: LegacyIdentity,
    pub account: Account,
    pub identity: AccountIdentity,
    pub kind: ResolutionKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<String>,
    pub needs_review: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MigrationPlan {
    pub run_id: String,
    pub source: String,
    pub checksum: String,
    #[serde(
        default,
        serialize_with = "serialize_slice_null_if_empty",
        deserialize_with = "deserialize_vec_or_null"
    )]
    pub items: Vec<LegacyAccountResolution>,
    #[serde(
        default,
        serialize_with = "serialize_slice_null_if_empty",
        deserialize_with = "deserialize_vec_or_null"
    )]
    pub collisions: Vec<LegacyAccountResolution>,
    pub legacy_rows: usize,
    pub unique_accounts: usize,
    pub provider_matches: usize,
    pub email_matches: usize,
    pub new_accounts: usize,
    pub ambiguous_collisions: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerificationFinding {
    pub code: String,
    pub scope: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub legacy_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub account_id: String,
    pub detail: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerificationResult {
    pub plan_checksum: String,
    pub findings: Vec<VerificationFinding>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ChecksumPayload<'a> {
    run_id: &'a str,
    source: &'a str,
    #[serde(serialize_with = "serialize_slice_null_if_empty")]
    items: &'a [LegacyAccountResolution],
    #[serde(serialize_with = "serialize_slice_null_if_empty")]
    collisions: &'a [LegacyAccountResolution],
    legacy_rows: usize,
    unique_accounts: usize,
    provider_matches: usize,
    email_matches: usize,
    new_accounts: usize,
    ambiguous_collisions: usize,
}

fn serialize_slice_null_if_empty<S, T>(
    values: &[T],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: Serialize,
{
    if values.is_empty() {
        serializer.serialize_none()
    } else {
        values.serialize(serializer)
    }
}

fn deserialize_vec_or_null<'de, D, T>(deserializer: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

pub fn plan_identity_migration(
    run_id: &str,
    source: &str,
    records: &[LegacyIdentity],
    existing: &[ExistingAccount],
) -> Result<MigrationPlan> {
    if run_id.is_empty() || source.is_empty() {
        return Err("runID and source are required".into());
    }
    let (mut items, collisions) = resolve_legacy_accounts(records, existing);
    items.sort_by(|left, right| {
        (&left.legacy.source, &left.legacy.legacy_user_id)
            .cmp(&(&right.legacy.source, &right.legacy.legacy_user_id))
    });
    let mut plan = MigrationPlan {
        run_id: run_id.to_owned(),
        source: source.to_owned(),
        checksum: String::new(),
        legacy_rows: records.len(),
        unique_accounts: items
            .iter()
            .map(|item| item.account.id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        provider_matches: items
            .iter()
            .filter(|item| item.kind == ResolutionKind::ProviderSubject)
            .count(),
        email_matches: items
            .iter()
            .filter(|item| item.kind == ResolutionKind::VerifiedEmail)
            .count(),
        new_accounts: items
            .iter()
            .filter(|item| item.kind == ResolutionKind::NewAccount)
            .count(),
        ambiguous_collisions: collisions.len(),
        items,
        collisions,
    };
    plan.checksum = plan_checksum(&plan)?;
    Ok(plan)
}

pub fn validate_plan(plan: &MigrationPlan) -> Result<()> {
    if plan.run_id.trim().is_empty() || plan.source.trim().is_empty() {
        return Err("identity migration plan runId and source are required".into());
    }
    if plan.checksum.trim().is_empty() {
        return Err("identity migration plan checksum is required".into());
    }
    let computed = plan_checksum(plan)?;
    if computed != plan.checksum {
        return Err(format!(
            "identity migration plan checksum mismatch: expected {}, computed {computed}",
            plan.checksum
        )
        .into());
    }
    Ok(())
}

fn plan_checksum(plan: &MigrationPlan) -> Result<String> {
    let payload = ChecksumPayload {
        run_id: &plan.run_id,
        source: &plan.source,
        items: &plan.items,
        collisions: &plan.collisions,
        legacy_rows: plan.legacy_rows,
        unique_accounts: plan.unique_accounts,
        provider_matches: plan.provider_matches,
        email_matches: plan.email_matches,
        new_accounts: plan.new_accounts,
        ambiguous_collisions: plan.ambiguous_collisions,
    };
    Ok(hex_sha256(&serde_json::to_vec(&payload)?))
}

fn resolve_legacy_accounts(
    records: &[LegacyIdentity],
    existing: &[ExistingAccount],
) -> (Vec<LegacyAccountResolution>, Vec<LegacyAccountResolution>) {
    let mut ordered = records.to_vec();
    ordered.sort_by(|left, right| {
        (&left.source, &left.legacy_user_id).cmp(&(&right.source, &right.legacy_user_id))
    });
    let mut provider: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut email: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut accounts: BTreeMap<String, Account> = BTreeMap::new();
    for item in existing {
        accounts.insert(item.account.id.clone(), item.account.clone());
        for identity in &item.identities {
            if !identity.provider.trim().is_empty() && !identity.subject.trim().is_empty() {
                add_index(
                    &mut provider,
                    identity_key(&identity.provider, &identity.issuer, &identity.subject),
                    &item.account.id,
                );
            }
            if identity.verified_email {
                let verified = if identity.email.is_empty() {
                    &item.account.email
                } else {
                    &identity.email
                };
                add_index(&mut email, normalize_email(verified), &item.account.id);
            }
        }
    }
    let mut items = Vec::new();
    let mut collisions = Vec::new();
    for legacy in ordered {
        let mut kind = ResolutionKind::NewAccount;
        let mut candidates = Vec::new();
        let mut account_id = String::new();
        let provider_key = identity_key(&legacy.provider, &legacy.issuer, &legacy.subject);
        if !legacy.provider.trim().is_empty() && !legacy.subject.trim().is_empty() {
            candidates = index_values(&provider, &provider_key);
            if candidates.len() == 1 {
                account_id.clone_from(&candidates[0]);
                kind = ResolutionKind::ProviderSubject;
            } else if candidates.len() > 1 {
                kind = ResolutionKind::Collision;
            }
        }
        if kind == ResolutionKind::NewAccount && legacy.email_verified {
            candidates = index_values(&email, &normalize_email(&legacy.email));
            if candidates.len() == 1 {
                account_id.clone_from(&candidates[0]);
                kind = ResolutionKind::VerifiedEmail;
            } else if candidates.len() > 1 {
                kind = ResolutionKind::Collision;
            }
        }
        if kind == ResolutionKind::NewAccount {
            account_id = deterministic_account_id(&legacy.source, &legacy.legacy_user_id);
            while accounts.contains_key(&account_id) {
                account_id = deterministic_account_id(&account_id, &legacy.legacy_user_id);
            }
            accounts.insert(
                account_id.clone(),
                Account {
                    id: account_id.clone(),
                    auth_realm_id: legacy.source.clone(),
                    email: normalize_email(&legacy.email),
                    name: legacy.name.clone(),
                    avatar_url: legacy.avatar_url.clone(),
                    disabled_at: None,
                },
            );
        }
        let account = accounts.get(&account_id).cloned().unwrap_or_default();
        let resolution = LegacyAccountResolution {
            identity: AccountIdentity {
                account_id: account_id.clone(),
                provider: legacy.provider.trim().to_owned(),
                issuer: legacy.issuer.trim().to_owned(),
                subject: legacy.subject.trim().to_owned(),
                email: normalize_email(&legacy.email),
                verified_email: legacy.email_verified,
            },
            account,
            candidates,
            needs_review: kind == ResolutionKind::Collision,
            kind,
            legacy,
        };
        if kind == ResolutionKind::Collision {
            collisions.push(resolution);
        } else {
            if resolution.identity.verified_email {
                add_index(
                    &mut email,
                    normalize_email(&resolution.legacy.email),
                    &account_id,
                );
            }
            if !resolution.identity.provider.is_empty() && !resolution.identity.subject.is_empty() {
                add_index(&mut provider, provider_key, &account_id);
            }
            items.push(resolution);
        }
    }
    (items, collisions)
}

fn add_index(index: &mut BTreeMap<String, BTreeSet<String>>, key: String, account_id: &str) {
    if !key.is_empty() && !account_id.is_empty() {
        index.entry(key).or_default().insert(account_id.to_owned());
    }
}

fn index_values(index: &BTreeMap<String, BTreeSet<String>>, key: &str) -> Vec<String> {
    index
        .get(key)
        .map(|values| values.iter().cloned().collect())
        .unwrap_or_default()
}

fn identity_key(provider: &str, issuer: &str, subject: &str) -> String {
    let provider = provider.trim().to_lowercase();
    let issuer = issuer.trim();
    let subject = subject.trim();
    if provider.is_empty() || subject.is_empty() {
        String::new()
    } else {
        format!("{provider}\0{issuer}\0{subject}")
    }
}

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}

fn deterministic_account_id(source: &str, legacy_id: &str) -> String {
    format!(
        "acct_{}",
        &hex_sha256(format!("{}\0{}", source.trim(), legacy_id.trim()).as_bytes())[..32]
    )
}

fn hex_sha256(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

const IDENTITY_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS legacy_account_map (
  source text NOT NULL,
  legacy_user_id text NOT NULL,
  account_id text NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  resolution text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(source,legacy_user_id)
);
CREATE INDEX IF NOT EXISTS legacy_account_map_by_account ON legacy_account_map(account_id);
CREATE TABLE IF NOT EXISTS identity_migration_runs (
  id text PRIMARY KEY,
  source text NOT NULL,
  plan_checksum text NOT NULL,
  status text NOT NULL DEFAULT 'planned',
  started_at timestamptz,
  completed_at timestamptz,
  error text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS identity_migration_checkpoints (
  run_id text NOT NULL REFERENCES identity_migration_runs(id) ON DELETE CASCADE,
  scope text NOT NULL,
  completed_index bigint NOT NULL DEFAULT -1,
  last_legacy_user_id text NOT NULL DEFAULT '',
  rows_processed bigint NOT NULL DEFAULT 0,
  checksum text NOT NULL DEFAULT '',
  status text NOT NULL DEFAULT 'pending',
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(run_id,scope)
);
CREATE TABLE IF NOT EXISTS identity_migration_collisions (
  run_id text NOT NULL REFERENCES identity_migration_runs(id) ON DELETE CASCADE,
  kind text NOT NULL,
  source_key text NOT NULL,
  candidates jsonb NOT NULL DEFAULT '[]'::jsonb,
  resolution text NOT NULL DEFAULT 'needs_review',
  resolved_account_id text NOT NULL DEFAULT '',
  reviewed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(run_id,kind,source_key)
);
"#;

pub async fn connect(database_url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await?)
}

pub async fn install_identity_schema(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(IDENTITY_SCHEMA).execute(pool).await?;
    Ok(())
}

pub async fn load_existing_accounts(pool: &PgPool) -> Result<Vec<ExistingAccount>> {
    if !relation_exists(pool, "accounts").await? {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "SELECT id,auth_realm_id,email,name,avatar_url,disabled_at FROM accounts ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    let mut accounts = Vec::with_capacity(rows.len());
    let mut by_id = BTreeMap::new();
    for row in rows {
        let account = Account {
            id: row.try_get("id")?,
            auth_realm_id: row.try_get("auth_realm_id")?,
            email: row.try_get("email")?,
            name: row.try_get("name")?,
            avatar_url: row.try_get("avatar_url")?,
            disabled_at: row.try_get("disabled_at")?,
        };
        by_id.insert(account.id.clone(), accounts.len());
        accounts.push(ExistingAccount {
            account,
            identities: Vec::new(),
        });
    }
    if !relation_exists(pool, "account_identities").await? {
        return Ok(accounts);
    }
    let rows = sqlx::query(
        "SELECT account_id,provider,issuer,subject,email,verified_email FROM account_identities ORDER BY account_id,provider,issuer,subject",
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        let identity = AccountIdentity {
            account_id: row.try_get("account_id")?,
            provider: row.try_get("provider")?,
            issuer: row.try_get("issuer")?,
            subject: row.try_get("subject")?,
            email: row.try_get("email")?,
            verified_email: row.try_get("verified_email")?,
        };
        let Some(index) = by_id.get(&identity.account_id) else {
            return Err(format!(
                "account identity references missing account {:?}",
                identity.account_id
            )
            .into());
        };
        accounts[*index].identities.push(identity);
    }
    Ok(accounts)
}

pub async fn apply_identity_migration(
    pool: &PgPool,
    plan: &MigrationPlan,
    allow_unresolved_collisions: bool,
) -> Result<()> {
    validate_plan(plan)?;
    if !plan.collisions.is_empty() && !allow_unresolved_collisions {
        return Err(format!(
            "identity migration has {} unresolved collisions",
            plan.collisions.len()
        )
        .into());
    }
    begin_migration(pool, plan).await?;
    for collision in &plan.collisions {
        save_collision(pool, &plan.run_id, collision).await?;
    }
    let mut checkpoint = load_checkpoint(pool, &plan.run_id, &plan.source).await?;
    for (index, item) in plan.items.iter().enumerate() {
        if index as i64 <= checkpoint.completed_index {
            continue;
        }
        apply_resolution(pool, item).await?;
        checkpoint = MigrationCheckpoint {
            run_id: plan.run_id.clone(),
            scope: plan.source.clone(),
            completed_index: index as i64,
            last_legacy_user_id: item.legacy.legacy_user_id.clone(),
            rows_processed: index as i64 + 1,
            checksum: plan.checksum.clone(),
            status: "running".to_owned(),
        };
        save_checkpoint(pool, &checkpoint).await?;
    }
    checkpoint.run_id.clone_from(&plan.run_id);
    checkpoint.scope.clone_from(&plan.source);
    checkpoint.completed_index = plan.items.len() as i64 - 1;
    checkpoint.rows_processed = plan.items.len() as i64;
    checkpoint.checksum.clone_from(&plan.checksum);
    checkpoint.status = "complete".to_owned();
    save_checkpoint(pool, &checkpoint).await?;
    let result = sqlx::query(
        "UPDATE identity_migration_runs SET status='complete',completed_at=COALESCE(completed_at,now()),updated_at=now() WHERE id=$1",
    )
    .bind(&plan.run_id)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(format!("identity migration run {:?} does not exist", plan.run_id).into());
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct MigrationCheckpoint {
    run_id: String,
    scope: String,
    completed_index: i64,
    last_legacy_user_id: String,
    rows_processed: i64,
    checksum: String,
    status: String,
}

async fn begin_migration(pool: &PgPool, plan: &MigrationPlan) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO identity_migration_runs(id,source,plan_checksum,status,started_at,updated_at)
           VALUES($1,$2,$3,'running',now(),now())
           ON CONFLICT(id) DO UPDATE SET
             status=CASE WHEN identity_migration_runs.status='complete' THEN identity_migration_runs.status ELSE EXCLUDED.status END,
             started_at=COALESCE(identity_migration_runs.started_at,now()),updated_at=now()
           WHERE identity_migration_runs.source=EXCLUDED.source
             AND identity_migration_runs.plan_checksum=EXCLUDED.plan_checksum"#,
    )
    .bind(&plan.run_id)
    .bind(&plan.source)
    .bind(&plan.checksum)
    .execute(pool)
    .await?;
    let row = sqlx::query("SELECT source,plan_checksum FROM identity_migration_runs WHERE id=$1")
        .bind(&plan.run_id)
        .fetch_one(pool)
        .await?;
    let source: String = row.try_get("source")?;
    let checksum: String = row.try_get("plan_checksum")?;
    if source != plan.source || checksum != plan.checksum {
        return Err(format!(
            "migration run {:?} already exists with a different source or plan checksum",
            plan.run_id
        )
        .into());
    }
    Ok(())
}

async fn save_collision(
    pool: &PgPool,
    run_id: &str,
    resolution: &LegacyAccountResolution,
) -> Result<()> {
    let source_key = format!(
        "{}\0{}",
        resolution.legacy.source, resolution.legacy.legacy_user_id
    );
    sqlx::query(
        r#"INSERT INTO identity_migration_collisions
           (run_id,kind,source_key,candidates,resolution,resolved_account_id)
           VALUES($1,$2,$3,$4,$5,$6)
           ON CONFLICT(run_id,kind,source_key) DO UPDATE SET
             candidates=EXCLUDED.candidates,resolution=EXCLUDED.resolution,
             resolved_account_id=EXCLUDED.resolved_account_id"#,
    )
    .bind(run_id)
    .bind(resolution.kind.as_str())
    .bind(source_key)
    .bind(Value::Array(
        resolution
            .candidates
            .iter()
            .cloned()
            .map(Value::String)
            .collect(),
    ))
    .bind(if resolution.needs_review {
        "needs_review"
    } else {
        "resolved"
    })
    .bind(&resolution.account.id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn load_checkpoint(pool: &PgPool, run_id: &str, scope: &str) -> Result<MigrationCheckpoint> {
    let row = sqlx::query(
        "SELECT run_id,scope,completed_index,last_legacy_user_id,rows_processed,checksum,status FROM identity_migration_checkpoints WHERE run_id=$1 AND scope=$2",
    )
    .bind(run_id)
    .bind(scope)
    .fetch_optional(pool)
    .await?;
    Ok(match row {
        Some(row) => MigrationCheckpoint {
            run_id: row.try_get("run_id")?,
            scope: row.try_get("scope")?,
            completed_index: row.try_get("completed_index")?,
            last_legacy_user_id: row.try_get("last_legacy_user_id")?,
            rows_processed: row.try_get("rows_processed")?,
            checksum: row.try_get("checksum")?,
            status: row.try_get("status")?,
        },
        None => MigrationCheckpoint {
            run_id: run_id.to_owned(),
            scope: scope.to_owned(),
            completed_index: -1,
            status: "pending".to_owned(),
            ..MigrationCheckpoint::default()
        },
    })
}

async fn save_checkpoint(pool: &PgPool, checkpoint: &MigrationCheckpoint) -> Result<()> {
    if checkpoint.completed_index < -1 || checkpoint.rows_processed < 0 {
        return Err("checkpoint indexes cannot be negative beyond the initial index".into());
    }
    let current = load_checkpoint(pool, &checkpoint.run_id, &checkpoint.scope).await?;
    if !current.checksum.is_empty()
        && !checkpoint.checksum.is_empty()
        && current.checksum != checkpoint.checksum
    {
        return Err(format!(
            "checkpoint {:?}/{:?} belongs to a different plan checksum",
            checkpoint.run_id, checkpoint.scope
        )
        .into());
    }
    sqlx::query(
        r#"INSERT INTO identity_migration_checkpoints
           (run_id,scope,completed_index,last_legacy_user_id,rows_processed,checksum,status,updated_at)
           VALUES($1,$2,$3,$4,$5,$6,$7,now())
           ON CONFLICT(run_id,scope) DO UPDATE SET
             completed_index=GREATEST(identity_migration_checkpoints.completed_index,EXCLUDED.completed_index),
             last_legacy_user_id=CASE WHEN EXCLUDED.completed_index>=identity_migration_checkpoints.completed_index THEN EXCLUDED.last_legacy_user_id ELSE identity_migration_checkpoints.last_legacy_user_id END,
             rows_processed=GREATEST(identity_migration_checkpoints.rows_processed,EXCLUDED.rows_processed),
             checksum=CASE WHEN identity_migration_checkpoints.checksum='' THEN EXCLUDED.checksum ELSE identity_migration_checkpoints.checksum END,
             status=CASE WHEN EXCLUDED.completed_index>=identity_migration_checkpoints.completed_index THEN EXCLUDED.status ELSE identity_migration_checkpoints.status END,
             updated_at=now()"#,
    )
    .bind(&checkpoint.run_id)
    .bind(&checkpoint.scope)
    .bind(checkpoint.completed_index)
    .bind(&checkpoint.last_legacy_user_id)
    .bind(checkpoint.rows_processed)
    .bind(&checkpoint.checksum)
    .bind(&checkpoint.status)
    .execute(pool)
    .await?;
    Ok(())
}

async fn apply_resolution(pool: &PgPool, resolution: &LegacyAccountResolution) -> Result<()> {
    let mut transaction = pool.begin().await?;
    let account = &resolution.account;
    sqlx::query(
        r#"INSERT INTO accounts(id,auth_realm_id,email,name,avatar_url,disabled_at,updated_at)
           VALUES($1,$2,$3,$4,$5,$6,now())
           ON CONFLICT(id) DO UPDATE SET
             auth_realm_id=CASE WHEN EXCLUDED.auth_realm_id<>'' THEN EXCLUDED.auth_realm_id ELSE accounts.auth_realm_id END,
             email=CASE WHEN EXCLUDED.email<>'' THEN EXCLUDED.email ELSE accounts.email END,
             name=CASE WHEN EXCLUDED.name<>'' THEN EXCLUDED.name ELSE accounts.name END,
             avatar_url=CASE WHEN EXCLUDED.avatar_url<>'' THEN EXCLUDED.avatar_url ELSE accounts.avatar_url END,
             disabled_at=COALESCE(EXCLUDED.disabled_at,accounts.disabled_at),updated_at=now()"#,
    )
    .bind(&account.id)
    .bind(&account.auth_realm_id)
    .bind(&account.email)
    .bind(&account.name)
    .bind(&account.avatar_url)
    .bind(account.disabled_at)
    .execute(&mut *transaction)
    .await?;
    let identity = &resolution.identity;
    if !identity.provider.trim().is_empty() && !identity.subject.trim().is_empty() {
        let result = sqlx::query(
            r#"INSERT INTO account_identities
               (project_id,account_id,provider,issuer,subject,email,verified_email,updated_at)
               VALUES($1,$2,$3,$4,$5,$6,$7,now())
               ON CONFLICT(project_id,provider,issuer,subject) DO UPDATE SET
                 email=CASE WHEN EXCLUDED.email<>'' THEN EXCLUDED.email ELSE account_identities.email END,
                 verified_email=account_identities.verified_email OR EXCLUDED.verified_email,
                 updated_at=now()
               WHERE account_identities.account_id=EXCLUDED.account_id"#,
        )
        .bind(&account.auth_realm_id)
        .bind(&identity.account_id)
        .bind(&identity.provider)
        .bind(&identity.issuer)
        .bind(&identity.subject)
        .bind(&identity.email)
        .bind(identity.verified_email)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(format!(
                "identity {:?}/{:?}/{:?} is already assigned to another account",
                identity.provider, identity.issuer, identity.subject
            )
            .into());
        }
    }
    let result = sqlx::query(
        r#"INSERT INTO legacy_account_map(source,legacy_user_id,account_id,resolution,updated_at)
           VALUES($1,$2,$3,$4,now())
           ON CONFLICT(source,legacy_user_id) DO UPDATE SET
             resolution=EXCLUDED.resolution,updated_at=now()
           WHERE legacy_account_map.account_id=EXCLUDED.account_id"#,
    )
    .bind(&resolution.legacy.source)
    .bind(&resolution.legacy.legacy_user_id)
    .bind(&account.id)
    .bind(resolution.kind.as_str())
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(format!(
            "legacy identity {:?}/{:?} is already mapped to another account",
            resolution.legacy.source, resolution.legacy.legacy_user_id
        )
        .into());
    }
    transaction.commit().await?;
    Ok(())
}

pub async fn verify_identity_migration(
    pool: &PgPool,
    plan: &MigrationPlan,
) -> Result<VerificationResult> {
    validate_plan(plan)?;
    let mut findings = Vec::new();
    let mapped: i64 = sqlx::query_scalar("SELECT count(*) FROM legacy_account_map WHERE source=$1")
        .bind(&plan.source)
        .fetch_one(pool)
        .await?;
    if mapped != plan.items.len() as i64 {
        findings.push(finding(
            "missing_legacy_maps",
            &plan.source,
            format!(
                "expected {} mapped legacy rows, found {mapped}",
                plan.items.len()
            ),
        ));
    }
    let run_status: Option<String> =
        sqlx::query_scalar("SELECT status FROM identity_migration_runs WHERE id=$1")
            .bind(&plan.run_id)
            .fetch_optional(pool)
            .await?;
    match run_status.as_deref() {
        None => findings.push(finding(
            "missing_migration_run",
            &plan.run_id,
            "migration run was not persisted",
        )),
        Some("complete") => {}
        Some(status) => findings.push(finding(
            "migration_not_complete",
            &plan.run_id,
            format!("migration run status is {status}"),
        )),
    }
    let collisions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM identity_migration_collisions WHERE run_id=$1")
            .bind(&plan.run_id)
            .fetch_one(pool)
            .await?;
    if collisions != plan.collisions.len() as i64 {
        findings.push(finding(
            "collision_records_mismatch",
            &plan.run_id,
            format!(
                "expected {} persisted collisions, found {collisions}",
                plan.collisions.len()
            ),
        ));
    }
    let account_ids: Vec<&str> = plan
        .items
        .iter()
        .map(|item| item.account.id.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if !account_ids.is_empty() {
        let accounts: i64 =
            sqlx::query_scalar("SELECT count(*) FROM accounts WHERE id=ANY($1::text[])")
                .bind(&account_ids)
                .fetch_one(pool)
                .await?;
        if accounts != account_ids.len() as i64 {
            findings.push(finding(
                "missing_accounts",
                &plan.source,
                format!("expected {} accounts, found {accounts}", account_ids.len()),
            ));
        }
        let expected_identities = plan
            .items
            .iter()
            .filter(|item| {
                !item.identity.provider.trim().is_empty()
                    && !item.identity.subject.trim().is_empty()
            })
            .count();
        if expected_identities > 0 {
            let identities: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM account_identities WHERE account_id=ANY($1::text[])",
            )
            .bind(&account_ids)
            .fetch_one(pool)
            .await?;
            if identities < expected_identities as i64 {
                findings.push(finding(
                    "missing_account_identities",
                    &plan.source,
                    format!(
                        "expected at least {expected_identities} provider identities, found {identities}"
                    ),
                ));
            }
        }
    }
    for collision in &plan.collisions {
        findings.push(VerificationFinding {
            code: "identity_collision".to_owned(),
            scope: collision.legacy.source.clone(),
            legacy_id: collision.legacy.legacy_user_id.clone(),
            account_id: collision.account.id.clone(),
            detail: "identity requires explicit review".to_owned(),
        });
    }
    Ok(VerificationResult {
        plan_checksum: plan.checksum.clone(),
        findings,
    })
}

fn finding(code: &str, scope: &str, detail: impl Into<String>) -> VerificationFinding {
    VerificationFinding {
        code: code.to_owned(),
        scope: scope.to_owned(),
        legacy_id: String::new(),
        account_id: String::new(),
        detail: detail.into(),
    }
}

fn plan_mapping(plan: &MigrationPlan) -> Result<BTreeMap<String, String>> {
    let mut mapping = BTreeMap::new();
    for item in &plan.items {
        let legacy_id = item.legacy.legacy_user_id.trim();
        let account_id = item.account.id.trim();
        if legacy_id.is_empty() || account_id.is_empty() {
            return Err("identity-v2 plan contains an incomplete legacy mapping".into());
        }
        if let Some(existing) = mapping.get(legacy_id) {
            if existing != account_id {
                return Err(format!(
                    "legacy id {legacy_id:?} maps to multiple accounts; split the migration by auth realm"
                )
                .into());
            }
        }
        mapping.insert(legacy_id.to_owned(), account_id.to_owned());
    }
    Ok(mapping)
}

#[derive(Clone, Debug)]
struct TenantTarget {
    project_id: String,
    tenant_id: String,
    database_url: String,
}

async fn tenant_targets(control: &PgPool) -> Result<Vec<TenantTarget>> {
    let rows = sqlx::query(
        r#"SELECT tenant.project_id,tenant.tenant_id,
                  CASE WHEN tenant.database_url<>'' THEN tenant.database_url ELSE project.database_url END AS database_url
           FROM gonvex_runtime_tenants tenant
           JOIN gonvex_runtime_projects project ON project.id=tenant.project_id
           WHERE tenant.status='active'
           ORDER BY tenant.project_id,tenant.tenant_id"#,
    )
    .fetch_all(control)
    .await?;
    rows.into_iter()
        .map(|row| {
            let target = TenantTarget {
                project_id: row.try_get("project_id")?,
                tenant_id: row.try_get("tenant_id")?,
                database_url: row.try_get("database_url")?,
            };
            if target.database_url.trim().is_empty() {
                Err(format!(
                    "tenant {}/{} has no database_url in the Control Plane",
                    target.project_id, target.tenant_id
                )
                .into())
            } else {
                Ok(target)
            }
        })
        .collect()
}

async fn relation_exists(pool: &PgPool, relation: &str) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass(format('%I.%I',current_schema(),$1)) IS NOT NULL",
    )
    .bind(relation)
    .fetch_one(pool)
    .await?)
}

async fn column_exists(pool: &PgPool, relation: &str, column: &str) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, bool>(
        r#"SELECT EXISTS(SELECT 1 FROM information_schema.columns
           WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2)"#,
    )
    .bind(relation)
    .bind(column)
    .fetch_one(pool)
    .await?)
}

async fn relation_exists_tx(
    transaction: &mut Transaction<'_, Postgres>,
    relation: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass(format('%I.%I',current_schema(),$1)) IS NOT NULL",
    )
    .bind(relation)
    .fetch_one(&mut **transaction)
    .await?)
}

async fn column_exists_tx(
    transaction: &mut Transaction<'_, Postgres>,
    relation: &str,
    column: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, bool>(
        r#"SELECT EXISTS(SELECT 1 FROM information_schema.columns
           WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2)"#,
    )
    .bind(relation)
    .bind(column)
    .fetch_one(&mut **transaction)
    .await?)
}

pub async fn inspect_runtime_migration(control: &PgPool, plan: &MigrationPlan) -> Result<()> {
    let mapping = plan_mapping(plan)?;
    for (table, column) in [
        ("gonvex_auth_memberships", "user_id"),
        ("gonvex_auth_users", "id"),
    ] {
        if !relation_exists(control, table).await? {
            continue;
        }
        let query = format!("SELECT DISTINCT {column} AS legacy_id FROM {table} ORDER BY {column}");
        for row in sqlx::query(&query).fetch_all(control).await? {
            let legacy_id: String = row.try_get("legacy_id")?;
            if !mapping.contains_key(&legacy_id) {
                return Err(format!(
                    "identity-v2 plan omits {} user {legacy_id:?}",
                    if table == "gonvex_auth_memberships" {
                        "legacy membership"
                    } else {
                        "legacy Control Plane"
                    }
                )
                .into());
            }
        }
    }
    for table in AUTH_CREDENTIAL_TABLES {
        if !relation_exists(control, table).await? {
            continue;
        }
        if !column_exists(control, table, "user_id").await? {
            if !column_exists(control, table, "account_id").await? {
                return Err(
                    format!("{table} has neither legacy user_id nor canonical account_id").into(),
                );
            }
            continue;
        }
        let query = format!("SELECT DISTINCT user_id AS legacy_id FROM {table} ORDER BY user_id");
        for row in sqlx::query(&query).fetch_all(control).await? {
            let legacy_id: String = row.try_get("legacy_id")?;
            if !mapping.contains_key(&legacy_id) {
                return Err(format!("identity-v2 plan omits {table} user {legacy_id:?}").into());
            }
        }
    }
    for target in tenant_targets(control).await? {
        let tenant = connect(&target.database_url).await.map_err(|error| {
            format!(
                "inspect tenant {}/{}: {error}",
                target.project_id, target.tenant_id
            )
        })?;
        if relation_exists(&tenant, "users").await? {
            return Err(format!(
                "inspect tenant {}/{}: legacy application table users still exists; migrate its business profile and foreign-key contract into canonical members before running identity-v2",
                target.project_id, target.tenant_id
            )
            .into());
        }
        if !relation_exists(&tenant, "members").await? {
            continue;
        }
        if column_exists(&tenant, "members", "user_id").await? {
            for row in sqlx::query("SELECT user_id FROM members ORDER BY user_id")
                .fetch_all(&tenant)
                .await?
            {
                let legacy_id: String = row.try_get("user_id")?;
                if !mapping.contains_key(&legacy_id) {
                    return Err(format!(
                        "inspect tenant {}/{}: identity-v2 plan omits tenant member {legacy_id:?}",
                        target.project_id, target.tenant_id
                    )
                    .into());
                }
            }
        } else {
            for column in ["id", "account_id", "status", "membership_revision"] {
                if !column_exists(&tenant, "members", column).await? {
                    return Err(format!(
                        "inspect tenant {}/{}: members has neither the legacy user_id shape nor canonical column {column}",
                        target.project_id, target.tenant_id
                    )
                    .into());
                }
            }
        }
    }
    Ok(())
}

const AUTH_CREDENTIAL_TABLES: [&str; 3] = [
    "gonvex_auth_codes",
    "gonvex_auth_sessions",
    "gonvex_auth_refresh_tokens",
];

pub async fn apply_runtime_migration(control: &PgPool, plan: &MigrationPlan) -> Result<()> {
    let mapping = plan_mapping(plan)?;
    for target in tenant_targets(control).await? {
        migrate_tenant(control, &target, &mapping, plan)
            .await
            .map_err(|error| {
                format!(
                    "migrate tenant {}/{}: {error}",
                    target.project_id, target.tenant_id
                )
            })?;
    }
    migrate_control_auth(control, &mapping).await
}

async fn migrate_control_auth(control: &PgPool, mapping: &BTreeMap<String, String>) -> Result<()> {
    if !relation_exists(control, "gonvex_auth_users").await? {
        return Ok(());
    }
    let legacy_memberships = relation_exists(control, "gonvex_auth_memberships").await?;
    let mut accounts = Vec::new();
    for row in sqlx::query("SELECT id,project_id FROM gonvex_auth_users ORDER BY project_id,id")
        .fetch_all(control)
        .await?
    {
        let legacy_id: String = row.try_get("id")?;
        let Some(account_id) = mapping.get(&legacy_id) else {
            return Err(format!(
                "legacy Control Plane user {legacy_id:?} has no reviewed account mapping"
            )
            .into());
        };
        accounts.push((
            legacy_id,
            row.try_get::<String, _>("project_id")?,
            account_id.clone(),
        ));
    }
    let mut transaction = control.begin().await?;
    for (_, project_id, account_id) in &accounts {
        let result = sqlx::query(
            "UPDATE accounts SET auth_realm_id=$2,updated_at=now() WHERE id=$1 AND (auth_realm_id='' OR auth_realm_id=$2)",
        )
        .bind(account_id)
        .bind(project_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(format!("account {account_id:?} is assigned to another auth realm").into());
        }
    }
    for table in AUTH_CREDENTIAL_TABLES {
        if !relation_exists_tx(&mut transaction, table).await?
            || !column_exists_tx(&mut transaction, table, "user_id").await?
        {
            continue;
        }
        sqlx::query(&format!(
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS account_id TEXT"
        ))
        .execute(&mut *transaction)
        .await?;
        for (legacy_id, _, account_id) in &accounts {
            sqlx::query(&format!(
                "UPDATE {table} SET account_id=$2 WHERE user_id=$1"
            ))
            .bind(legacy_id)
            .bind(account_id)
            .execute(&mut *transaction)
            .await?;
        }
        let missing: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {table} WHERE account_id IS NULL OR account_id=''"
        ))
        .fetch_one(&mut *transaction)
        .await?;
        if missing != 0 {
            return Err(
                format!("{table} has {missing} rows without reviewed account mappings").into(),
            );
        }
        sqlx::query(&format!("ALTER TABLE {table} DROP COLUMN user_id"))
            .execute(&mut *transaction)
            .await?;
        sqlx::query(&format!(
            "ALTER TABLE {table} ALTER COLUMN account_id SET NOT NULL"
        ))
        .execute(&mut *transaction)
        .await?;
        let constraint = format!("{table}_account_id_fkey");
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conname=$1 AND conrelid=to_regclass($2))",
        )
        .bind(&constraint)
        .bind(table)
        .fetch_one(&mut *transaction)
        .await?;
        if !exists {
            sqlx::query(&format!(
                "ALTER TABLE {table} ADD CONSTRAINT {constraint} FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE"
            ))
            .execute(&mut *transaction)
            .await?;
        }
    }
    if legacy_memberships {
        sqlx::query("DROP TABLE gonvex_auth_memberships")
            .execute(&mut *transaction)
            .await?;
    }
    sqlx::query("DROP TABLE gonvex_auth_users")
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}

const CANONICAL_MEMBERS_DDL: &str = r#"CREATE TABLE members (
 id text PRIMARY KEY,
 account_id text NOT NULL UNIQUE,
 status text NOT NULL DEFAULT 'active',
 display_name text NOT NULL DEFAULT '',
 avatar_url text NOT NULL DEFAULT '',
 role text NOT NULL DEFAULT 'member',
 permissions jsonb NOT NULL DEFAULT '{}'::jsonb,
 membership_revision bigint NOT NULL DEFAULT 1,
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
)"#;

async fn migrate_tenant(
    control: &PgPool,
    target: &TenantTarget,
    mapping: &BTreeMap<String, String>,
    plan: &MigrationPlan,
) -> Result<()> {
    let tenant = connect(&target.database_url).await?;
    let mut transaction = tenant.begin().await?;
    if !relation_exists_tx(&mut transaction, "members").await? {
        sqlx::query(CANONICAL_MEMBERS_DDL)
            .execute(&mut *transaction)
            .await?;
    } else if column_exists_tx(&mut transaction, "members", "user_id").await? {
        for statement in [
            "ALTER TABLE members ADD COLUMN IF NOT EXISTS id TEXT",
            "ALTER TABLE members ADD COLUMN IF NOT EXISTS account_id TEXT",
            "ALTER TABLE members ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'",
            "ALTER TABLE members ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE members ADD COLUMN IF NOT EXISTS avatar_url TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE members ADD COLUMN IF NOT EXISTS membership_revision BIGINT NOT NULL DEFAULT 1",
        ] {
            sqlx::query(statement).execute(&mut *transaction).await?;
        }
        let rows = sqlx::query("SELECT user_id FROM members ORDER BY user_id")
            .fetch_all(&mut *transaction)
            .await?;
        for row in rows {
            let legacy_id: String = row.try_get("user_id")?;
            let Some(account_id) = mapping.get(&legacy_id) else {
                return Err(format!("member {legacy_id:?} has no reviewed account mapping").into());
            };
            sqlx::query("UPDATE members SET id=$1,account_id=$2 WHERE user_id=$1")
                .bind(&legacy_id)
                .bind(account_id)
                .execute(&mut *transaction)
                .await?;
        }
        let duplicates: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (SELECT account_id FROM members GROUP BY account_id HAVING count(*)>1) duplicate_accounts",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if duplicates != 0 {
            return Err(format!("members contains {duplicates} duplicate account mappings").into());
        }
        for statement in [
            "ALTER TABLE members DROP CONSTRAINT IF EXISTS members_pkey",
            "ALTER TABLE members DROP COLUMN user_id",
            "ALTER TABLE members ALTER COLUMN id SET NOT NULL",
            "ALTER TABLE members ALTER COLUMN account_id SET NOT NULL",
            "ALTER TABLE members ADD PRIMARY KEY(id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS members_by_account ON members(account_id)",
        ] {
            sqlx::query(statement).execute(&mut *transaction).await?;
        }
    }
    let rows =
        sqlx::query("SELECT id,account_id,status,membership_revision FROM members ORDER BY id")
            .fetch_all(&mut *transaction)
            .await?;
    let mut projections = Vec::with_capacity(rows.len());
    for row in rows {
        projections.push((
            row.try_get::<String, _>("id")?,
            row.try_get::<String, _>("account_id")?,
            row.try_get::<String, _>("status")?,
            row.try_get::<i64, _>("membership_revision")?,
        ));
    }
    transaction.commit().await?;
    for (member_id, account_id, status, revision) in &projections {
        sqlx::query(
            r#"INSERT INTO account_tenant_index
               (account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
               VALUES($1,$2,$3,$4,$5,now())
               ON CONFLICT(account_id,tenant_id) DO UPDATE SET
                 member_id=EXCLUDED.member_id,status=EXCLUDED.status,
                 tenant_membership_revision=EXCLUDED.tenant_membership_revision,updated_at=now()
               WHERE EXCLUDED.tenant_membership_revision>=account_tenant_index.tenant_membership_revision"#,
        )
        .bind(account_id)
        .bind(&target.tenant_id)
        .bind(member_id)
        .bind(status)
        .bind(revision)
        .execute(control)
        .await?;
    }
    sqlx::query(
        r#"INSERT INTO identity_migration_checkpoints
           (run_id,scope,completed_index,last_legacy_user_id,rows_processed,checksum,status,updated_at)
           VALUES($1,$2,0,'',$3,$4,'complete',now())
           ON CONFLICT(run_id,scope) DO UPDATE SET completed_index=0,
             rows_processed=EXCLUDED.rows_processed,checksum=EXCLUDED.checksum,
             status='complete',updated_at=now()"#,
    )
    .bind(&plan.run_id)
    .bind(format!(
        "tenant:{}:{}",
        target.project_id, target.tenant_id
    ))
    .bind(projections.len() as i64)
    .bind(&plan.checksum)
    .execute(control)
    .await?;
    Ok(())
}

pub async fn verify_runtime_migration(control: &PgPool, plan: &MigrationPlan) -> Result<()> {
    if relation_exists(control, "gonvex_auth_users").await?
        || relation_exists(control, "gonvex_auth_memberships").await?
    {
        return Err("identity-v2 verification: legacy Control Plane identity tables remain".into());
    }
    for table in AUTH_CREDENTIAL_TABLES {
        if !relation_exists(control, table).await? {
            continue;
        }
        if column_exists(control, table, "user_id").await? {
            return Err(format!("identity-v2 verification: {table}.user_id remains").into());
        }
        let query = format!(
            "SELECT count(*) FROM {table} value LEFT JOIN accounts account ON account.id=value.account_id WHERE value.account_id IS NULL OR value.account_id='' OR account.id IS NULL"
        );
        let orphaned: i64 = sqlx::query_scalar(&query).fetch_one(control).await?;
        if orphaned != 0 {
            return Err(format!(
                "identity-v2 verification: {table} contains {orphaned} orphaned account references"
            )
            .into());
        }
    }
    for target in tenant_targets(control).await? {
        let tenant = connect(&target.database_url).await?;
        if column_exists(&tenant, "members", "user_id").await? {
            return Err(format!(
                "identity-v2 verification for tenant {}/{}: members.user_id remains",
                target.project_id, target.tenant_id
            )
            .into());
        }
        if relation_exists(&tenant, "users").await? {
            return Err(format!(
                "identity-v2 verification for tenant {}/{}: legacy application table users remains",
                target.project_id, target.tenant_id
            )
            .into());
        }
        let rows =
            sqlx::query("SELECT id,account_id,status,membership_revision FROM members ORDER BY id")
                .fetch_all(&tenant)
                .await?;
        for row in rows {
            let member_id: String = row.try_get("id")?;
            let account_id: String = row.try_get("account_id")?;
            let status: String = row.try_get("status")?;
            let revision: i64 = row.try_get("membership_revision")?;
            if member_id.trim().is_empty() || account_id.trim().is_empty() {
                return Err(format!(
                    "identity-v2 verification for tenant {}/{}: members contains an incomplete identity row",
                    target.project_id, target.tenant_id
                )
                .into());
            }
            let account_exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM accounts WHERE id=$1)")
                    .bind(&account_id)
                    .fetch_one(control)
                    .await?;
            if !account_exists {
                return Err(format!(
                    "identity-v2 verification for tenant {}/{}: member {member_id:?} references missing account {account_id:?}",
                    target.project_id, target.tenant_id
                )
                .into());
            }
            let projected: bool = sqlx::query_scalar(
                r#"SELECT EXISTS(SELECT 1 FROM account_tenant_index
                   WHERE account_id=$1 AND tenant_id=$2 AND member_id=$3
                     AND status=$4 AND tenant_membership_revision=$5)"#,
            )
            .bind(&account_id)
            .bind(&target.tenant_id)
            .bind(&member_id)
            .bind(&status)
            .bind(revision)
            .fetch_one(control)
            .await?;
            if !projected {
                return Err(format!(
                    "identity-v2 verification for tenant {}/{}: member {member_id:?} has no matching directory projection",
                    target.project_id, target.tenant_id
                )
                .into());
            }
        }
        let scope = format!("tenant:{}:{}", target.project_id, target.tenant_id);
        let checkpoint_complete: bool = sqlx::query_scalar(
            r#"SELECT EXISTS(SELECT 1 FROM identity_migration_checkpoints
               WHERE run_id=$1 AND scope=$2 AND checksum=$3 AND status='complete')"#,
        )
        .bind(&plan.run_id)
        .bind(scope)
        .bind(&plan.checksum)
        .fetch_one(control)
        .await?;
        if !checkpoint_complete {
            return Err(format!(
                "identity-v2 verification for tenant {}/{}: migration checkpoint is missing or incomplete",
                target.project_id, target.tenant_id
            )
            .into());
        }
    }
    Ok(())
}

pub fn load_inventory(path: &str, source: &str) -> Result<Vec<LegacyIdentity>> {
    let raw = if path == "-" {
        let mut raw = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut raw)?;
        raw
    } else {
        std::fs::read_to_string(path)?
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("legacy identity inventory is empty".into());
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Inventory {
        records: Vec<LegacyIdentity>,
    }
    let mut records = if raw.starts_with('[') {
        serde_json::from_str::<Vec<LegacyIdentity>>(raw)?
    } else {
        serde_json::from_str::<Inventory>(raw)?.records
    };
    if records.is_empty() {
        return Err("legacy identity inventory contains no records".into());
    }
    let mut seen = BTreeSet::new();
    for (index, record) in records.iter_mut().enumerate() {
        if record.source != record.source.trim() {
            return Err(format!(
                "legacy identity {:?} has whitespace around source",
                record.legacy_user_id
            )
            .into());
        }
        if record.legacy_user_id != record.legacy_user_id.trim() {
            return Err(format!(
                "legacy identity at index {index} has whitespace around legacyUserId"
            )
            .into());
        }
        if record.source.is_empty() {
            record.source = source.to_owned();
        }
        if record.source != source {
            return Err(format!(
                "legacy identity {:?} has source {:?}; expected {source:?}",
                record.legacy_user_id, record.source
            )
            .into());
        }
        if record.legacy_user_id.is_empty() {
            return Err(format!("legacy identity at index {index} has no legacyUserId").into());
        }
        if !seen.insert((record.source.clone(), record.legacy_user_id.clone())) {
            return Err(format!(
                "duplicate legacy identity {:?}/{:?}",
                record.source, record.legacy_user_id
            )
            .into());
        }
    }
    Ok(records)
}

pub fn write_plan(path: &Path, plan: &MigrationPlan) -> Result<()> {
    validate_plan(plan)?;
    let mut raw = serde_json::to_string_pretty(plan)?;
    raw.push('\n');
    match std::fs::read_to_string(path) {
        Ok(existing_raw) => {
            let existing: MigrationPlan = serde_json::from_str(&existing_raw)?;
            validate_plan(&existing)?;
            if existing.checksum != plan.checksum {
                return Err(format!(
                    "plan file {} already exists with checksum {}; refusing to overwrite it with {}",
                    path.display(), existing.checksum, plan.checksum
                )
                .into());
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .and_then(|mut file| std::io::Write::write_all(&mut file, raw.as_bytes()))?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

pub fn read_plan(path: &Path) -> Result<MigrationPlan> {
    let plan: MigrationPlan = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    validate_plan(&plan)?;
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legacy(id: &str, email: &str, verified: bool) -> LegacyIdentity {
        LegacyIdentity {
            source: "project-a".to_owned(),
            legacy_user_id: id.to_owned(),
            email: email.to_owned(),
            email_verified: verified,
            ..LegacyIdentity::default()
        }
    }

    #[test]
    fn planning_is_deterministic_and_unverified_email_does_not_merge() {
        let records = vec![
            legacy("user-b", "same@example.com", false),
            legacy("user-a", "same@example.com", false),
        ];
        let first = plan_identity_migration("run-a", "project-a", &records, &[]).unwrap();
        let second = plan_identity_migration("run-a", "project-a", &records, &[]).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.unique_accounts, 2);
        assert_eq!(first.items[0].legacy.legacy_user_id, "user-a");
        validate_plan(&first).unwrap();
    }

    #[test]
    fn provider_subject_wins_before_verified_email() {
        let account = Account {
            id: "acct-existing".to_owned(),
            auth_realm_id: "project-a".to_owned(),
            email: "old@example.com".to_owned(),
            ..Account::default()
        };
        let existing = ExistingAccount {
            account: account.clone(),
            identities: vec![AccountIdentity {
                account_id: account.id.clone(),
                provider: "firebase".to_owned(),
                issuer: "issuer".to_owned(),
                subject: "uid".to_owned(),
                email: account.email.clone(),
                verified_email: true,
            }],
        };
        let mut record = legacy("legacy", "different@example.com", true);
        record.provider = "firebase".to_owned();
        record.issuer = "issuer".to_owned();
        record.subject = "uid".to_owned();
        let plan = plan_identity_migration("run-a", "project-a", &[record], &[existing]).unwrap();
        assert_eq!(plan.items[0].kind, ResolutionKind::ProviderSubject);
        assert_eq!(plan.items[0].account.id, "acct-existing");
    }

    #[test]
    fn edited_plan_fails_checksum_validation() {
        let mut plan = plan_identity_migration(
            "run-a",
            "project-a",
            &[legacy("legacy", "verified@example.com", true)],
            &[],
        )
        .unwrap();
        plan.items[0].account.name = "forged".to_owned();
        assert!(validate_plan(&plan)
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch"));
    }

    #[test]
    fn rust_plan_preserves_the_published_go_checksum_contract() {
        let mut records: Vec<LegacyIdentity> =
            serde_json::from_str(include_str!("../tests/fixtures/identity-inventory.json"))
                .unwrap();
        for record in &mut records {
            record.source = "project-a".to_owned();
        }
        let plan = plan_identity_migration("run-a", "project-a", &records, &[]).unwrap();
        assert_eq!(
            plan.checksum,
            "e450fc658a34c12fc0e5c8f4d4b5c8e8c1320106d60e7b1cc4978d31890853d6"
        );
        assert_eq!(
            serde_json::to_value(&plan).unwrap()["collisions"],
            Value::Null
        );
    }
}
