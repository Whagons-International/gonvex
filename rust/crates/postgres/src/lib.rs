//! PostgreSQL ownership for the Rust runtime.
//!
//! Browser and module inputs name a project and tenant. They never carry a
//! database URL. This crate resolves physical databases from trusted runtime
//! configuration or the Control Plane, then performs the final tenant-local
//! Member admission check on the selected tenant database.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgListener, PgPoolOptions, PgRow};
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Row, Transaction};
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

mod provision;
pub use provision::{MigrationScope, SqlMigration};

const DEFAULT_MAX_TOTAL_CONNECTIONS: usize = 20;
const MAX_TOTAL_CONNECTIONS: usize = 64;
const DEFAULT_MAX_CONNECTIONS_PER_DATABASE: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolLimits {
    pub max_total_connections: usize,
    pub max_connections_per_database: u32,
    pub max_idle_connections_per_database: u32,
    pub idle_timeout: Duration,
    pub acquire_timeout: Duration,
}

impl Default for PoolLimits {
    fn default() -> Self {
        Self {
            max_total_connections: DEFAULT_MAX_TOTAL_CONNECTIONS,
            max_connections_per_database: DEFAULT_MAX_CONNECTIONS_PER_DATABASE,
            max_idle_connections_per_database: 1,
            idle_timeout: Duration::from_secs(1),
            acquire_timeout: Duration::from_secs(30),
        }
    }
}

impl PoolLimits {
    pub fn bounded(mut self) -> Self {
        self.max_total_connections = self.max_total_connections.clamp(1, MAX_TOTAL_CONNECTIONS);
        self.max_connections_per_database = self
            .max_connections_per_database
            .clamp(1, self.max_total_connections as u32);
        self.max_idle_connections_per_database = self
            .max_idle_connections_per_database
            .min(self.max_connections_per_database);
        self
    }
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("project id is required")]
    MissingProject,
    #[error("tenant id is required")]
    MissingTenant,
    #[error("account id is required")]
    MissingAccount,
    #[error("project {0:?} is not active")]
    ProjectNotFound(String),
    #[error("tenant {tenant:?} does not belong to project {project:?}")]
    TenantNotFound { project: String, tenant: String },
    #[error("tenant {tenant:?} has no configured physical database")]
    TenantDatabaseMissing { tenant: String },
    #[error("active tenant member for account {0:?} not found")]
    MemberNotFound(String),
    #[error("invalid or expired app session")]
    InvalidSession,
    #[error("app session was issued for a different project")]
    SessionProjectMismatch,
    #[error("the account has no active tenant membership")]
    NoTenantMembership,
    #[error("database query admission timed out")]
    AdmissionTimeout,
    #[error("idempotency key {key:?} was already used by reducer {stored_path:?}; refusing to replay it for {requested_path:?}")]
    IdempotencyPathMismatch {
        key: String,
        stored_path: String,
        requested_path: String,
    },
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

#[derive(Clone)]
pub struct PoolRegistry {
    limits: PoolLimits,
    admission: Arc<Semaphore>,
    pools: Arc<RwLock<BTreeMap<String, PgPool>>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolRegistrySnapshot {
    pub pools: usize,
    pub open_connections: u32,
    pub in_use: u32,
    pub idle: usize,
    pub max_open_connections: u32,
    pub admission_limit: usize,
    pub admission_active: usize,
}

impl PoolRegistry {
    pub fn new(limits: PoolLimits) -> Self {
        let limits = limits.bounded();
        Self {
            admission: Arc::new(Semaphore::new(limits.max_total_connections)),
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            limits,
        }
    }

    pub async fn pool(&self, database_url: &str) -> Result<PgPool, DatabaseError> {
        let database_url = database_url.trim();
        if let Some(pool) = self.pools.read().await.get(database_url).cloned() {
            return Ok(pool);
        }
        let pool = PgPoolOptions::new()
            .max_connections(self.limits.max_connections_per_database)
            .min_connections(0)
            .max_lifetime(Duration::from_secs(30 * 60))
            .idle_timeout(self.limits.idle_timeout)
            .acquire_timeout(self.limits.acquire_timeout)
            .connect(database_url)
            .await?;
        let mut pools = self.pools.write().await;
        Ok(pools
            .entry(database_url.to_owned())
            .or_insert_with(|| pool.clone())
            .clone())
    }

    pub async fn admit(&self) -> Result<OwnedSemaphorePermit, DatabaseError> {
        tokio::time::timeout(
            self.limits.acquire_timeout,
            self.admission.clone().acquire_owned(),
        )
        .await
        .map_err(|_| DatabaseError::AdmissionTimeout)?
        .map_err(|_| DatabaseError::AdmissionTimeout)
    }

    pub async fn snapshot(&self) -> PoolRegistrySnapshot {
        let pools = self.pools.read().await;
        let open_connections = pools.values().map(PgPool::size).sum::<u32>();
        let idle = pools.values().map(PgPool::num_idle).sum::<usize>();
        PoolRegistrySnapshot {
            pools: pools.len(),
            open_connections,
            in_use: open_connections.saturating_sub(u32::try_from(idle).unwrap_or(u32::MAX)),
            idle,
            max_open_connections: u32::try_from(pools.len())
                .unwrap_or(u32::MAX)
                .saturating_mul(self.limits.max_connections_per_database),
            admission_limit: self.limits.max_total_connections,
            admission_active: self
                .limits
                .max_total_connections
                .saturating_sub(self.admission.available_permits()),
        }
    }

    pub async fn close(&self) {
        let pools: Vec<PgPool> = self
            .pools
            .write()
            .await
            .split_off("")
            .into_values()
            .collect();
        for pool in pools {
            pool.close().await;
        }
    }

    pub async fn close_database(&self, database_url: &str) {
        if let Some(pool) = self.pools.write().await.remove(database_url.trim()) {
            pool.close().await;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: String,
    pub name: String,
    pub environment: String,
    pub database_mode: String,
    pub status: String,
    pub auth_mode: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantDirectoryEntry {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub domain: String,
    pub timezone: String,
    pub profile: Value,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantRoute {
    pub project_id: String,
    pub tenant_id: String,
    pub database_url: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Member {
    pub id: String,
    pub account_id: String,
    pub status: String,
    pub display_name: String,
    pub avatar_url: String,
    pub role: String,
    pub permissions: Value,
    pub membership_revision: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    pub id: String,
    pub email: String,
    pub email_verified: bool,
    pub name: String,
    pub avatar_url: String,
    pub provider: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionIdentity {
    pub project_id: String,
    pub account: Account,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TenantSession {
    pub identity: SessionIdentity,
    pub route: TenantRoute,
    pub member: Member,
    /// Durable tenant change-feed revision covered by the admission snapshot.
    pub admission_revision: u64,
}

#[derive(Clone, Debug)]
pub struct ImpersonationSession {
    pub tenant: TenantSession,
    pub grant_id: String,
    pub actor_account_id: String,
    pub reconnect_token: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeManifestRecord {
    pub project_id: String,
    pub module_hash: String,
    pub manifest: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClaimedAction {
    pub id: String,
    pub path: String,
    pub args: Value,
    pub actor_account_id: String,
    pub actor_email: String,
    pub attempts: i32,
    pub provenance: Value,
}

#[derive(Clone)]
pub struct ControlPlane {
    pool: PgPool,
    pools: PoolRegistry,
    configured_tenants: Arc<BTreeMap<String, String>>,
}

impl ControlPlane {
    pub async fn listener(&self, channels: &[&str]) -> Result<PgListener, DatabaseError> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        for channel in channels {
            listener.listen(channel).await?;
        }
        Ok(listener)
    }

    pub async fn notify(&self, channel: &str, payload: &str) -> Result<(), DatabaseError> {
        sqlx::query("SELECT pg_notify($1,$2)")
            .bind(channel)
            .bind(payload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn connect(
        database_url: &str,
        pools: PoolRegistry,
        configured_tenants: BTreeMap<String, String>,
    ) -> Result<Self, DatabaseError> {
        let pool = pools.pool(database_url).await?;
        let control = Self {
            pool,
            pools,
            configured_tenants: Arc::new(configured_tenants),
        };
        control.ping().await?;
        control.prepare_identity_schema_upgrade().await?;
        control.ensure_control_schema().await?;
        Ok(control)
    }

    pub async fn ping(&self) -> Result<(), DatabaseError> {
        let _admission = self.pools.admit().await?;
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Starts a trusted host-only Control Plane transaction. Application
    /// modules never receive this value or the underlying pool credentials.
    pub async fn begin_control_transaction(
        &self,
        read_only: bool,
    ) -> Result<TenantTransaction, DatabaseError> {
        let admission = self.pools.admit().await?;
        let mut transaction = self.pool.begin().await?;
        if read_only {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .execute(&mut *transaction)
                .await?;
        }
        Ok(TenantTransaction {
            transaction,
            _admission: admission,
            read_only,
        })
    }

    pub async fn load_session_identity(
        &self,
        token: &str,
        requested_project_id: Option<&str>,
    ) -> Result<SessionIdentity, DatabaseError> {
        let token = token.trim();
        if !token.starts_with("gvx_session_") {
            return Err(DatabaseError::InvalidSession);
        }
        let token_hash = token_hash(token);
        let _admission = self.pools.admit().await?;
        let row = sqlx::query(
            r#"SELECT session.project_id, account.id, account.email,
                      COALESCE(identity.verified_email, FALSE) AS email_verified,
                      account.name, account.avatar_url,
                      COALESCE(identity.provider, '') AS provider
               FROM gonvex_auth_sessions session
               JOIN accounts account
                 ON account.id = session.account_id
                AND account.auth_realm_id = session.project_id
               LEFT JOIN LATERAL (
                   SELECT provider, verified_email
                   FROM account_identities
                   WHERE account_id = account.id
                   ORDER BY updated_at DESC
                   LIMIT 1
               ) identity ON TRUE
               WHERE session.token_hash = $1
                 AND session.revoked_at IS NULL
                 AND session.expires_at > now()
                 AND account.disabled_at IS NULL"#,
        )
        .bind(token_hash.clone())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(DatabaseError::InvalidSession)?;
        let identity = SessionIdentity {
            project_id: row.get("project_id"),
            account: Account {
                id: row.get("id"),
                email: row.get("email"),
                email_verified: row.get("email_verified"),
                name: row.get("name"),
                avatar_url: row.get("avatar_url"),
                provider: row.get("provider"),
            },
        };
        if let Some(requested) = requested_project_id
            .map(str::trim)
            .filter(|requested| !requested.is_empty())
        {
            if requested != identity.project_id {
                return Err(DatabaseError::SessionProjectMismatch);
            }
        }
        sqlx::query(
            r#"UPDATE gonvex_auth_sessions SET last_seen_at = now()
               WHERE token_hash = $1
                 AND (last_seen_at IS NULL OR last_seen_at < now() - interval '5 minutes')"#,
        )
        .bind(token_hash)
        .execute(&self.pool)
        .await?;
        Ok(identity)
    }

    pub async fn authenticate_session(
        &self,
        token: &str,
        requested_project_id: Option<&str>,
        requested_tenant_id: Option<&str>,
    ) -> Result<TenantSession, DatabaseError> {
        let identity = self
            .load_session_identity(token, requested_project_id)
            .await?;
        let tenant_id = if let Some(tenant) = requested_tenant_id
            .map(str::trim)
            .filter(|tenant| !tenant.is_empty())
        {
            tenant.to_owned()
        } else {
            self.first_admitted_tenant(&identity.project_id, &identity.account.id)
                .await?
        };
        let (route, member, admission_revision) = self
            .admit_member(&identity.project_id, &tenant_id, &identity.account.id)
            .await?;
        Ok(TenantSession {
            identity,
            route,
            member,
            admission_revision,
        })
    }

    /// Consumes a one-time impersonation grant or rotates its memory-only
    /// reconnect credential. Tenant admission is still checked afterward.
    pub async fn authenticate_impersonation(
        &self,
        token: &str,
        requested_project_id: Option<&str>,
        requested_tenant_id: Option<&str>,
        connection_id: &str,
    ) -> Result<ImpersonationSession, DatabaseError> {
        let token = token.trim();
        if !(token.starts_with("gvx_imp_") || token.starts_with("gvx_dev_")) {
            return Err(DatabaseError::InvalidSession);
        }
        let next = random_token("dev");
        let _admission = self.pools.admit().await?;
        let mut transaction = self.pool.begin().await?;
        let statement = if token.starts_with("gvx_imp_") {
            r#"UPDATE gonvex_impersonation_grants SET
                 used_at=now(),used_connection_id=$2,reconnect_token_hash=$3
               WHERE token_hash=$1 AND used_at IS NULL AND revoked_at IS NULL AND expires_at>now()
               RETURNING id,project_id,actor_account_id,target_account_id,tenant_id"#
        } else {
            r#"UPDATE gonvex_impersonation_grants SET
                 used_connection_id=$2,reconnect_token_hash=$3
               WHERE reconnect_token_hash=$1 AND used_at IS NOT NULL
                 AND revoked_at IS NULL AND expires_at>now()
               RETURNING id,project_id,actor_account_id,target_account_id,tenant_id"#
        };
        let row = sqlx::query(statement)
            .bind(token_hash(token))
            .bind(connection_id)
            .bind(token_hash(&next))
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DatabaseError::InvalidSession)?;
        let grant_id: String = row.get("id");
        let project_id: String = row.get("project_id");
        let actor_account_id: String = row.get("actor_account_id");
        let account_id: String = row.get("target_account_id");
        let tenant_id: String = row.get("tenant_id");
        if requested_project_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some_and(|requested| requested != project_id)
            || requested_tenant_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some_and(|requested| requested != tenant_id)
        {
            transaction.rollback().await?;
            return Err(DatabaseError::InvalidSession);
        }
        let account_row = sqlx::query(
            r#"SELECT account.id,account.email,account.name,account.avatar_url,
                      COALESCE(identity.verified_email,FALSE) AS email_verified,
                      COALESCE(identity.provider,'') AS provider
               FROM accounts account
               LEFT JOIN LATERAL (
                 SELECT provider,verified_email FROM account_identities
                 WHERE account_id=account.id ORDER BY updated_at DESC LIMIT 1
               ) identity ON TRUE
               WHERE account.id=$1 AND account.auth_realm_id=$2 AND account.disabled_at IS NULL"#,
        )
        .bind(&account_id)
        .bind(&project_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DatabaseError::InvalidSession)?;
        transaction.commit().await?;
        drop(_admission);
        let (route, member, admission_revision) = self
            .admit_member(&project_id, &tenant_id, &account_id)
            .await?;
        Ok(ImpersonationSession {
            tenant: TenantSession {
                identity: SessionIdentity {
                    project_id,
                    account: Account {
                        id: account_row.get("id"),
                        email: account_row.get("email"),
                        email_verified: account_row.get("email_verified"),
                        name: account_row.get("name"),
                        avatar_url: account_row.get("avatar_url"),
                        provider: account_row.get("provider"),
                    },
                },
                route,
                member,
                admission_revision,
            },
            grant_id,
            actor_account_id,
            reconnect_token: next,
        })
    }

    /// Revalidates an already-consumed reconnect credential without rotating
    /// it. Rotation is reserved for an actual reconnect so a health check on
    /// the active socket cannot invalidate that socket's next credential.
    pub async fn validate_impersonation_session(
        &self,
        token: &str,
        requested_project_id: &str,
        requested_tenant_id: &str,
        connection_id: &str,
    ) -> Result<(), DatabaseError> {
        if !token.trim().starts_with("gvx_dev_") {
            return Err(DatabaseError::InvalidSession);
        }
        let _admission = self.pools.admit().await?;
        let row = sqlx::query(
            r#"SELECT project_id,target_account_id,tenant_id
               FROM gonvex_impersonation_grants
               WHERE reconnect_token_hash=$1 AND used_at IS NOT NULL
                 AND used_connection_id=$2 AND revoked_at IS NULL AND expires_at>now()"#,
        )
        .bind(token_hash(token))
        .bind(connection_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(DatabaseError::InvalidSession)?;
        let project_id: String = row.get("project_id");
        let account_id: String = row.get("target_account_id");
        let tenant_id: String = row.get("tenant_id");
        if project_id != requested_project_id || tenant_id != requested_tenant_id {
            return Err(DatabaseError::InvalidSession);
        }
        self.admit_member(&project_id, &tenant_id, &account_id)
            .await?;
        Ok(())
    }

    pub async fn tenant_session_for_account(
        &self,
        project_id: &str,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<TenantSession, DatabaseError> {
        let _admission = self.pools.admit().await?;
        let row = sqlx::query(
            r#"SELECT account.id, account.email,
                      COALESCE(identity.verified_email, FALSE) AS email_verified,
                      account.name, account.avatar_url,
                      COALESCE(identity.provider, '') AS provider
               FROM accounts account
               LEFT JOIN LATERAL (
                   SELECT provider, verified_email
                   FROM account_identities
                   WHERE account_id = account.id
                   ORDER BY updated_at DESC
                   LIMIT 1
               ) identity ON TRUE
               WHERE account.id = $1
                 AND account.auth_realm_id = $2
                 AND account.disabled_at IS NULL"#,
        )
        .bind(account_id)
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(DatabaseError::InvalidSession)?;
        drop(_admission);
        let account = Account {
            id: row.get("id"),
            email: row.get("email"),
            email_verified: row.get("email_verified"),
            name: row.get("name"),
            avatar_url: row.get("avatar_url"),
            provider: row.get("provider"),
        };
        let (route, member, admission_revision) = self
            .admit_member(project_id, tenant_id, &account.id)
            .await?;
        Ok(TenantSession {
            identity: SessionIdentity {
                project_id: project_id.to_owned(),
                account,
            },
            route,
            member,
            admission_revision,
        })
    }

    async fn first_admitted_tenant(
        &self,
        project_id: &str,
        account_id: &str,
    ) -> Result<String, DatabaseError> {
        let admission = self.pools.admit().await?;
        let rows = sqlx::query(
            r#"SELECT tenant.tenant_id
               FROM gonvex_runtime_tenants tenant
               LEFT JOIN account_tenant_index directory
                 ON directory.tenant_id = tenant.tenant_id
                AND directory.account_id = $2
                AND directory.status = 'active'
               WHERE tenant.project_id = $1
                 AND tenant.deleted_at IS NULL
                 AND tenant.status NOT IN ('deleted', 'disabled')
               ORDER BY (directory.account_id IS NOT NULL) DESC,
                        lower(tenant.name), tenant.tenant_id"#,
        )
        .bind(project_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;
        drop(admission);
        for row in rows {
            let tenant_id: String = row.get("tenant_id");
            match self.admit_member(project_id, &tenant_id, account_id).await {
                Ok(_) => return Ok(tenant_id),
                Err(DatabaseError::MemberNotFound(_)) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(DatabaseError::NoTenantMembership)
    }

    pub async fn project(&self, project_id: &str) -> Result<Project, DatabaseError> {
        let project_id = required(project_id, DatabaseError::MissingProject)?;
        let _admission = self.pools.admit().await?;
        let row = sqlx::query(
            r#"SELECT id, name, environment, database_mode, status, auth_mode
               FROM gonvex_runtime_projects
               WHERE id = $1 AND status NOT IN ('deleted', 'disabled')"#,
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| DatabaseError::ProjectNotFound(project_id.to_owned()))?;
        Ok(project_from_row(row))
    }

    pub async fn runtime_manifests(&self) -> Result<Vec<RuntimeManifestRecord>, DatabaseError> {
        let _admission = self.pools.admit().await?;
        let rows = sqlx::query(
            r#"SELECT project_id, module_hash, manifest
               FROM gonvex_runtime_manifests
               WHERE module_hash <> ''
               ORDER BY updated_at, project_id"#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| RuntimeManifestRecord {
                project_id: row.get("project_id"),
                module_hash: row.get("module_hash"),
                manifest: row.get("manifest"),
            })
            .collect())
    }

    pub async fn project_accepts_sync_key(
        &self,
        project_id: &str,
        key: &str,
        fallback_key: Option<&str>,
    ) -> Result<bool, DatabaseError> {
        let _admission = self.pools.admit().await?;
        let project_key = sqlx::query_scalar::<_, String>(
            "SELECT project_key FROM gonvex_runtime_projects WHERE id=$1 AND status NOT IN ('deleted','disabled')",
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(project_key
            .as_deref()
            .filter(|stored| !stored.is_empty())
            .is_some_and(|stored| constant_time_equal(stored.as_bytes(), key.as_bytes()))
            || project_key.as_deref().unwrap_or("").is_empty()
                && fallback_key
                    .filter(|stored| !stored.is_empty())
                    .is_some_and(|stored| constant_time_equal(stored.as_bytes(), key.as_bytes())))
    }

    pub async fn save_runtime_manifest(
        &self,
        record: &RuntimeManifestRecord,
    ) -> Result<(), DatabaseError> {
        let _admission = self.pools.admit().await?;
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_manifests(project_id,manifest,module_hash,updated_at)
               VALUES($1,$2,$3,now()) ON CONFLICT(project_id) DO UPDATE SET
                 manifest=EXCLUDED.manifest,module_hash=EXCLUDED.module_hash,updated_at=now()"#,
        )
        .bind(&record.project_id)
        .bind(Json(record.manifest.clone()))
        .bind(&record.module_hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn tenant_directory_entry(
        &self,
        project_id: &str,
        tenant_id: &str,
    ) -> Result<TenantDirectoryEntry, DatabaseError> {
        let project_id = required(project_id, DatabaseError::MissingProject)?;
        let tenant_id = required(tenant_id, DatabaseError::MissingTenant)?;
        let _admission = self.pools.admit().await?;
        let row = sqlx::query(
            r#"SELECT tenant_id, project_id, name, domain, timezone, profile, status
               FROM gonvex_runtime_tenants
               WHERE project_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
                 AND status NOT IN ('deleted', 'disabled')"#,
        )
        .bind(project_id)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| DatabaseError::TenantNotFound {
            project: project_id.to_owned(),
            tenant: tenant_id.to_owned(),
        })?;
        Ok(tenant_from_row(row))
    }

    pub async fn tenant_routes(&self, project_id: &str) -> Result<Vec<TenantRoute>, DatabaseError> {
        let project_id = required(project_id, DatabaseError::MissingProject)?;
        let _admission = self.pools.admit().await?;
        let rows = sqlx::query(
            r#"SELECT tenant_id,database_url FROM gonvex_runtime_tenants
               WHERE project_id=$1 AND deleted_at IS NULL
                 AND status NOT IN ('deleted','disabled') ORDER BY tenant_id"#,
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let tenant_id: String = row.get("tenant_id");
                let configured = self
                    .configured_tenants
                    .get(&format!("{project_id}:{tenant_id}"))
                    .cloned();
                let database_url = configured.unwrap_or_else(|| row.get("database_url"));
                (!database_url.trim().is_empty()).then_some(TenantRoute {
                    project_id: project_id.to_owned(),
                    tenant_id,
                    database_url,
                })
            })
            .collect())
    }

    pub async fn resolve_tenant(
        &self,
        project_id: &str,
        tenant_id: &str,
    ) -> Result<TenantRoute, DatabaseError> {
        let project_id = required(project_id, DatabaseError::MissingProject)?;
        let tenant_id = required(tenant_id, DatabaseError::MissingTenant)?;
        let scope = format!("{project_id}:{tenant_id}");
        if let Some(database_url) = self.configured_tenants.get(&scope) {
            return Ok(TenantRoute {
                project_id: project_id.to_owned(),
                tenant_id: tenant_id.to_owned(),
                database_url: database_url.clone(),
            });
        }

        let _admission = self.pools.admit().await?;
        let row = sqlx::query(
            r#"SELECT tenant_id,NULLIF(database_url, '') AS database_url
               FROM gonvex_runtime_tenants
               WHERE project_id = $1
                 AND (tenant_id = $2 OR lower(domain) = lower($2))
                 AND deleted_at IS NULL
                 AND status NOT IN ('deleted', 'disabled')"#,
        )
        .bind(project_id)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?;
        let row = row.ok_or_else(|| DatabaseError::TenantDatabaseMissing {
            tenant: tenant_id.to_owned(),
        })?;
        let canonical_tenant_id: String = row.get("tenant_id");
        let database_url: Option<String> = row.get("database_url");
        let database_url = database_url.ok_or_else(|| DatabaseError::TenantDatabaseMissing {
            tenant: canonical_tenant_id.clone(),
        })?;
        Ok(TenantRoute {
            project_id: project_id.to_owned(),
            tenant_id: canonical_tenant_id,
            database_url,
        })
    }

    /// This is the final tenant admission gate. Directory rows only locate the
    /// tenant database; they cannot grant access.
    pub async fn admit_member(
        &self,
        project_id: &str,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<(TenantRoute, Member, u64), DatabaseError> {
        let account_id = required(account_id, DatabaseError::MissingAccount)?;
        let route = self.resolve_tenant(project_id, tenant_id).await?;
        let tenant_pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit().await?;
        let mut transaction = tenant_pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await?;
        let row = sqlx::query(
            r#"SELECT id, account_id, status, display_name, avatar_url, role,
                      permissions, membership_revision
               FROM members
               WHERE account_id = $1 AND status = 'active'"#,
        )
        .bind(account_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| DatabaseError::MemberNotFound(account_id.to_owned()))?;
        let revision: i64 =
            sqlx::query_scalar("SELECT revision FROM _gonvex_sync_clock WHERE singleton=true")
                .fetch_one(&mut *transaction)
                .await?;
        transaction.commit().await?;
        Ok((
            route,
            member_from_row(row),
            u64::try_from(revision).unwrap_or_default(),
        ))
    }

    pub async fn begin_tenant_transaction(
        &self,
        route: &TenantRoute,
        read_only: bool,
    ) -> Result<TenantTransaction, DatabaseError> {
        let admission = self.pools.admit().await?;
        let pool = self.pools.pool(&route.database_url).await?;
        let mut transaction = pool.begin().await?;
        if read_only {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .execute(&mut *transaction)
                .await?;
        }
        Ok(TenantTransaction {
            transaction,
            _admission: admission,
            read_only,
        })
    }

    pub async fn command_revision(
        &self,
        route: &TenantRoute,
        command_id: &str,
    ) -> Result<Option<u64>, DatabaseError> {
        let pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit().await?;
        let revision: i64 = sqlx::query_scalar(
            r#"SELECT COALESCE(MAX(revision), 0)
               FROM _gonvex_sync_changes
               WHERE command_id = $1"#,
        )
        .bind(command_id)
        .fetch_one(&pool)
        .await?;
        Ok((revision > 0).then_some(revision as u64))
    }

    pub async fn ensure_reducer_idempotency(
        &self,
        route: &TenantRoute,
    ) -> Result<(), DatabaseError> {
        let pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit().await?;
        sqlx::raw_sql(
            r#"DO $$ BEGIN
                 IF to_regclass('public._gonvex_mutation_idempotency') IS NOT NULL
                   AND to_regclass('public._gonvex_reducer_idempotency') IS NULL THEN
                   ALTER TABLE _gonvex_mutation_idempotency RENAME TO _gonvex_reducer_idempotency;
                 END IF;
               END $$;
               CREATE TABLE IF NOT EXISTS _gonvex_reducer_idempotency (
                 subject text NOT NULL DEFAULT '',
                 idempotency_key text NOT NULL,
                 path text NOT NULL,
                 result jsonb,
                 created_at timestamptz NOT NULL DEFAULT now(),
                 PRIMARY KEY (subject, idempotency_key)
               );
               CREATE INDEX IF NOT EXISTS gonvex_reducer_idempotency_created_at
                 ON _gonvex_reducer_idempotency (created_at);"#,
        )
        .execute(&pool)
        .await?;
        Ok(())
    }

    pub async fn replay_reducer_result(
        &self,
        route: &TenantRoute,
        subject: &str,
        key: &str,
        path: &str,
    ) -> Result<Value, DatabaseError> {
        let pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit().await?;
        let row = sqlx::query(
            r#"SELECT result, path FROM _gonvex_reducer_idempotency
               WHERE subject = $1 AND idempotency_key = $2"#,
        )
        .bind(subject)
        .bind(key)
        .fetch_one(&pool)
        .await?;
        let stored_path: String = row.get("path");
        if stored_path != path {
            return Err(DatabaseError::IdempotencyPathMismatch {
                key: key.to_owned(),
                stored_path,
                requested_path: path.to_owned(),
            });
        }
        Ok(row
            .try_get::<Option<Value>, _>("result")?
            .unwrap_or(Value::Null))
    }

    pub async fn claim_action(
        &self,
        route: &TenantRoute,
    ) -> Result<Option<ClaimedAction>, DatabaseError> {
        let pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit().await?;
        let mut transaction = pool.begin().await?;
        let row = sqlx::query(
            r#"SELECT id, action_path, args,
                      COALESCE(actor_user_id, '') AS actor_account_id,
                      COALESCE(actor_email, '') AS actor_email,
                      attempts + 1 AS next_attempt, provenance
               FROM _gonvex_action_outbox
               WHERE (status = 'pending' AND available_at <= now())
                  OR (status = 'processing' AND locked_at < now() - interval '5 minutes')
               ORDER BY available_at, created_at
               FOR UPDATE SKIP LOCKED LIMIT 1"#,
        )
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.rollback().await?;
            return Ok(None);
        };
        let claimed = ClaimedAction {
            id: row.get("id"),
            path: row.get("action_path"),
            args: row.get("args"),
            actor_account_id: row.get("actor_account_id"),
            actor_email: row.get("actor_email"),
            attempts: row.get("next_attempt"),
            provenance: row.get("provenance"),
        };
        sqlx::query(
            r#"UPDATE _gonvex_action_outbox
               SET status = 'processing', locked_at = now(), attempts = $2
               WHERE id = $1"#,
        )
        .bind(&claimed.id)
        .bind(claimed.attempts)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(claimed))
    }

    pub async fn complete_action(
        &self,
        route: &TenantRoute,
        action_id: &str,
    ) -> Result<(), DatabaseError> {
        let pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit().await?;
        sqlx::query("DELETE FROM _gonvex_action_outbox WHERE id = $1")
            .bind(action_id)
            .execute(&pool)
            .await?;
        Ok(())
    }

    pub async fn retry_action(
        &self,
        route: &TenantRoute,
        action_id: &str,
        attempts: i32,
        error: &str,
    ) -> Result<(), DatabaseError> {
        let pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit().await?;
        sqlx::query(
            r#"UPDATE _gonvex_action_outbox
               SET status = 'pending', locked_at = NULL,
                   available_at = now() + make_interval(secs => $2),
                   last_error = $3
               WHERE id = $1"#,
        )
        .bind(action_id)
        .bind(attempts.clamp(1, 10))
        .bind(error)
        .execute(&pool)
        .await?;
        Ok(())
    }
}

/// Host-owned attribution installed on one Reducer transaction before commit.
pub struct TransactionAttribution<'a> {
    pub root_command_id: &'a str,
    pub root_channel: &'a str,
    pub channel: &'a str,
    pub actor_account_id: Option<&'a str>,
    pub actor_member_id: Option<&'a str>,
    pub on_behalf_of_member_id: Option<&'a str>,
    pub agent_execution_id: Option<&'a str>,
}

/// One invocation-scoped tenant transaction. The admission permit is held for
/// the transaction lifetime so a burst of module calls cannot bypass the
/// runtime-wide database limit.
pub struct TenantTransaction {
    transaction: Transaction<'static, Postgres>,
    _admission: OwnedSemaphorePermit,
    read_only: bool,
}

impl TenantTransaction {
    pub fn transaction(&mut self) -> &mut Transaction<'static, Postgres> {
        &mut self.transaction
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    pub async fn set_command_id(&mut self, command_id: &str) -> Result<(), DatabaseError> {
        sqlx::query("SELECT set_config('gonvex.command_id', $1, true)")
            .bind(command_id)
            .execute(&mut *self.transaction)
            .await?;
        Ok(())
    }

    pub async fn set_invocation_provenance(
        &mut self,
        attribution: TransactionAttribution<'_>,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"SELECT
                 set_config('gonvex.root_command_id',$1,true),
                 set_config('gonvex.root_invocation_channel',$2,true),
                 set_config('gonvex.invocation_channel',$3,true),
                 set_config('gonvex.actor_account_id',$4,true),
                 set_config('gonvex.actor_member_id',$5,true),
                 set_config('gonvex.on_behalf_of_member_id',$6,true),
                 set_config('gonvex.agent_execution_id',$7,true)"#,
        )
        .bind(attribution.root_command_id)
        .bind(attribution.root_channel)
        .bind(attribution.channel)
        .bind(attribution.actor_account_id.unwrap_or_default())
        .bind(attribution.actor_member_id.unwrap_or_default())
        .bind(attribution.on_behalf_of_member_id.unwrap_or_default())
        .bind(attribution.agent_execution_id.unwrap_or_default())
        .execute(&mut *self.transaction)
        .await?;
        Ok(())
    }

    pub async fn claim_reducer(
        &mut self,
        subject: &str,
        key: &str,
        path: &str,
    ) -> Result<bool, DatabaseError> {
        let result = sqlx::query(
            r#"INSERT INTO _gonvex_reducer_idempotency (subject, idempotency_key, path)
               VALUES ($1, $2, $3)
               ON CONFLICT (subject, idempotency_key) DO NOTHING"#,
        )
        .bind(subject)
        .bind(key)
        .bind(path)
        .execute(&mut *self.transaction)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn store_reducer_result(
        &mut self,
        subject: &str,
        key: &str,
        result: &Value,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"UPDATE _gonvex_reducer_idempotency SET result = $3
               WHERE subject = $1 AND idempotency_key = $2"#,
        )
        .bind(subject)
        .bind(key)
        .bind(Json(result.clone()))
        .execute(&mut *self.transaction)
        .await?;
        Ok(())
    }

    pub async fn enqueue_action(
        &mut self,
        action_path: &str,
        args: &Value,
        actor_account_id: &str,
        actor_email: &str,
        provenance: &Value,
    ) -> Result<String, DatabaseError> {
        let id = uuid::Uuid::new_v4().to_string();
        self.enqueue_action_with_id(&id, action_path, args, actor_account_id, actor_email, provenance).await
    }

    pub async fn enqueue_action_with_id(
        &mut self,
        id: &str,
        action_path: &str,
        args: &Value,
        actor_account_id: &str,
        actor_email: &str,
        provenance: &Value,
    ) -> Result<String, DatabaseError> {
        sqlx::query(
            r#"INSERT INTO _gonvex_action_outbox
               (id, action_path, args, actor_user_id, actor_email, provenance)
               VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6)"#,
        )
        .bind(&id)
        .bind(action_path)
        .bind(Json(args.clone()))
        .bind(actor_account_id)
        .bind(actor_email)
        .bind(Json(provenance.clone()))
        .execute(&mut *self.transaction)
        .await?;
        Ok(id.to_owned())
    }

    pub async fn commit(self) -> Result<(), DatabaseError> {
        self.transaction.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), DatabaseError> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

fn required(value: &str, error: DatabaseError) -> Result<&str, DatabaseError> {
    let value = value.trim();
    if value.is_empty() {
        Err(error)
    } else {
        Ok(value)
    }
}

fn project_from_row(row: PgRow) -> Project {
    Project {
        id: row.get("id"),
        name: row.get("name"),
        environment: row.get("environment"),
        database_mode: row.get("database_mode"),
        status: row.get("status"),
        auth_mode: row.get("auth_mode"),
    }
}

fn tenant_from_row(row: PgRow) -> TenantDirectoryEntry {
    TenantDirectoryEntry {
        id: row.get("tenant_id"),
        project_id: row.get("project_id"),
        name: row.get("name"),
        domain: row.get("domain"),
        timezone: row.get("timezone"),
        profile: row.get("profile"),
        status: row.get("status"),
    }
}

fn member_from_row(row: PgRow) -> Member {
    Member {
        id: row.get("id"),
        account_id: row.get("account_id"),
        status: row.get("status"),
        display_name: row.get("display_name"),
        avatar_url: row.get("avatar_url"),
        role: row.get("role"),
        permissions: row.get("permissions"),
        membership_revision: row.get("membership_revision"),
    }
}

pub fn token_hash(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn random_token(kind: &str) -> String {
    use rand::RngCore as _;
    let mut secret = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut secret);
    format!(
        "gvx_{kind}_{}.{}",
        uuid::Uuid::new_v4().simple(),
        URL_SAFE_NO_PAD.encode(secret)
    )
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && bool::from(left.ct_eq(right))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_connection_limits() {
        let limits = PoolLimits {
            max_total_connections: 500,
            max_connections_per_database: 200,
            max_idle_connections_per_database: 300,
            ..PoolLimits::default()
        }
        .bounded();
        assert_eq!(limits.max_total_connections, 64);
        assert_eq!(limits.max_connections_per_database, 64);
        assert_eq!(limits.max_idle_connections_per_database, 64);
    }

    #[test]
    fn token_hash_matches_the_published_hex_sha256_contract() {
        assert_eq!(
            token_hash("gvx_session_contract"),
            "647b3d58aeef93f10d437ff04dc6dbfde9fb606f3c58833f6cde755dece0e7f0"
        );
    }

    #[tokio::test]
    async fn configured_routes_are_selected_only_by_project_and_tenant_identity() {
        let configured = BTreeMap::from([(
            "project-a:tenant-a".to_owned(),
            "postgres://trusted/tenant-a".to_owned(),
        )]);
        let control = ControlPlane {
            pool: PgPoolOptions::new()
                .connect_lazy("postgres://unused")
                .unwrap(),
            pools: PoolRegistry::new(PoolLimits::default()),
            configured_tenants: Arc::new(configured),
        };
        let route = control
            .resolve_tenant("project-a", "tenant-a")
            .await
            .unwrap();
        assert_eq!(route.database_url, "postgres://trusted/tenant-a");
        assert_eq!(route.project_id, "project-a");
        assert_eq!(route.tenant_id, "tenant-a");
    }
}
