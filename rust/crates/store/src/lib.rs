//! SQLite ownership for the Rust runtime.
//!
//! Browser and module inputs name a project and tenant. They never carry a
//! database URL. This crate resolves physical databases from trusted runtime
//! configuration or the Control Plane, then performs the final tenant-local
//! Member admission check on the selected tenant database.

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::sqlite::SqliteRow;
use sqlx::types::Json;
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use subtle::ConstantTimeEq;
use thiserror::Error;

/// `CAST(unixepoch('subsec') * 1000 AS INTEGER)` as a literal for `concat!`.
macro_rules! now_ms {
    () => {
        "CAST(unixepoch('subsec') * 1000 AS INTEGER)"
    };
}

pub mod capture;
mod data_dir;
pub mod feed;
mod registry;
pub mod rows;
mod sync_barrier;
pub use data_dir::{DataDir, DataDirLock, DATA_DIR_ENV, DEFAULT_DATA_DIR};
pub use feed::{CommitNotice, Provenance, NOW_MS};
pub use registry::{
    database_path, database_url, AdmissionPermit, DatabaseListener, Durability, KeyedLock,
    Notification, PoolLimits, PoolRegistry, PoolRegistrySnapshot,
};

mod provision;
pub mod reference_integrity;
pub mod sql_lexer;
pub use provision::{MigrationScope, SqlMigration};

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
    #[error("ambiguous current and legacy receipt history requires recovery")]
    AmbiguousReducerReceipts,
    #[error("database listener fell behind and must resynchronize")]
    ListenerLagged,
    #[error("database listener closed")]
    ListenerClosed,
    #[error("database file error: {0}")]
    Io(String),
    #[error("value decode failed: {0}")]
    Decode(String),
    #[error("data directory {0} is already in use by another Gonvex process")]
    DataDirLocked(String),
    #[error("reference integrity: {0}")]
    ReferenceIntegrity(String),
    #[error("transaction aborted: an earlier statement failed ({0})")]
    TransactionAborted(String),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
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
    /// Set only for a member session a service principal drives through a
    /// delegation grant. Developer and support impersonation leave it empty.
    pub delegation: Option<SessionDelegation>,
    /// Only a redeemed, unexpired self developer grant supplies this metadata.
    /// Ordinary sessions, support impersonation and service delegation omit it.
    pub developer_session: Option<Value>,
}

impl TenantSession {
    /// Empty identity IDs represent an anonymous tenant session, never a Member.
    pub fn anonymous(route: TenantRoute) -> Self {
        Self {
            identity: SessionIdentity {
                project_id: route.project_id.clone(),
                account: Account {
                    id: String::new(),
                    email: String::new(),
                    email_verified: false,
                    name: String::new(),
                    avatar_url: String::new(),
                    provider: String::new(),
                },
            },
            route,
            member: Member {
                id: String::new(),
                account_id: String::new(),
                status: String::new(),
                display_name: String::new(),
                avatar_url: String::new(),
                role: String::new(),
                permissions: serde_json::json!({}),
                membership_revision: 0,
            },
            admission_revision: 0,
            delegation: None,
            developer_session: None,
        }
    }

    pub fn is_anonymous(&self) -> bool {
        self.identity.account.id.is_empty()
    }
}

/// Impersonation grants whose actor account starts with this prefix were
/// minted by the service principal named after it.
pub const SERVICE_PRINCIPAL_ACTOR_PREFIX: &str = "service:";

/// The service principal behind a delegated member session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionDelegation {
    /// Configured service principal id.
    pub principal: String,
    /// The end actor the service reported when it minted the grant, such as
    /// an API key. It is attribution, never authority.
    #[serde(default)]
    pub actor: Option<DelegationActor>,
}

/// The end actor recorded on a service principal delegation grant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DelegationActor {
    pub kind: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
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
    database_url: Arc<str>,
    /// The Control Plane writer (one connection). Every write uses it.
    pool: SqlitePool,
    /// Read-only Control Plane connections.
    reader: SqlitePool,
    pools: PoolRegistry,
    configured_tenants: Arc<BTreeMap<String, String>>,
}

/// Grant IDs are host-generated. A support caller's free-form reason cannot
/// turn its `imp_` grant into the `devmode_` grant issued by developer.enter.
fn developer_session_metadata(
    grant_id: &str,
    reason: &str,
    actor: &str,
    account: &str,
    tenant: &str,
    expires_at: i64,
) -> Option<Value> {
    (grant_id.starts_with("devmode_") && reason == "developer mode" && !actor.is_empty()
        && actor == account && !tenant.is_empty() && expires_at > unix_millis())
        .then(|| serde_json::json!({ "grantId": grant_id, "accountId": account, "tenantId": tenant, "expiresAt": expires_at }))
}

/// Joins the Account's most recently updated identity as `identity`.
macro_rules! latest_identity_join {
    () => {
        "LEFT JOIN account_identities identity ON identity.rowid = (
           SELECT rowid FROM account_identities
           WHERE account_id = account.id ORDER BY updated_at DESC, rowid DESC LIMIT 1)"
    };
}

impl ControlPlane {
    pub async fn listener(&self, channels: &[&str]) -> Result<DatabaseListener, DatabaseError> {
        let mut listener = self.pools.listener(&self.database_url).await?;
        for channel in channels {
            listener.listen(channel).await?;
        }
        Ok(listener)
    }

    pub async fn notify(&self, channel: &str, payload: &str) -> Result<(), DatabaseError> {
        self.pools
            .notify(&self.database_url, channel, payload)
            .await
    }

    pub fn pools(&self) -> &PoolRegistry {
        &self.pools
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    pub async fn connect(
        database_url: &str,
        pools: PoolRegistry,
        configured_tenants: BTreeMap<String, String>,
    ) -> Result<Self, DatabaseError> {
        let pool = pools.pool(database_url).await?;
        let reader = pools.reader(database_url).await?;
        let control = Self {
            database_url: database_url.trim().into(),
            pool,
            reader,
            pools,
            configured_tenants: Arc::new(configured_tenants),
        };
        control.ping().await?;
        control.prepare_identity_schema_upgrade().await?;
        control.ensure_control_schema().await?;
        Ok(control)
    }

    pub async fn ping(&self) -> Result<(), DatabaseError> {
        let _admission = self.pools.admit(&self.database_url).await?;
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Starts a trusted host-only Control Plane transaction. Application
    /// modules never receive this value or the underlying pool credentials.
    pub async fn begin_control_transaction(
        &self,
        read_only: bool,
    ) -> Result<TenantTransaction, DatabaseError> {
        begin_transaction(&self.pools, &self.database_url, read_only).await
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
        let _admission = self.pools.admit(&self.database_url).await?;
        let row = sqlx::query(concat!(
            r#"SELECT session.project_id, account.id, account.email,
                      COALESCE(identity.verified_email, 0) AS email_verified,
                      account.name, account.avatar_url,
                      COALESCE(identity.provider, '') AS provider
               FROM gonvex_auth_sessions session
               JOIN accounts account
                 ON account.id = session.account_id
                AND account.auth_realm_id = session.project_id
               "#,
            latest_identity_join!(),
            r#"
               WHERE session.token_hash = $1
                 AND session.revoked_at IS NULL
                 AND session.expires_at > "#,
            now_ms!(),
            r#"
                 AND account.disabled_at IS NULL"#
        ))
        .bind(token_hash.clone())
        .fetch_optional(&self.reader)
        .await?
        .ok_or(DatabaseError::InvalidSession)?;
        let identity = SessionIdentity {
            project_id: row.get("project_id"),
            account: account_from_row(&row),
        };
        if let Some(requested) = requested_project_id
            .map(str::trim)
            .filter(|requested| !requested.is_empty())
        {
            if requested != identity.project_id {
                return Err(DatabaseError::SessionProjectMismatch);
            }
        }
        sqlx::query(concat!(
            "UPDATE gonvex_auth_sessions SET last_seen_at = ",
            now_ms!(),
            " WHERE token_hash = $1 AND (last_seen_at IS NULL OR last_seen_at < ",
            now_ms!(),
            " - 300000)"
        ))
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
            delegation: None,
            developer_session: None,
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
        let _admission = self.pools.admit(&self.database_url).await?;
        let mut transaction = self.pool.begin().await?;
        let statement = if token.starts_with("gvx_imp_") {
            concat!(
                "UPDATE gonvex_impersonation_grants SET used_at=",
                now_ms!(),
                r#",used_connection_id=$2,reconnect_token_hash=$3
               WHERE token_hash=$1 AND used_at IS NULL AND revoked_at IS NULL AND expires_at>"#,
                now_ms!(),
                r#"
               RETURNING id,project_id,actor_account_id,target_account_id,tenant_id,delegation_actor,reason,expires_at"#
            )
        } else {
            concat!(
                r#"UPDATE gonvex_impersonation_grants SET
                 used_connection_id=$2,reconnect_token_hash=$3
               WHERE reconnect_token_hash=$1 AND used_at IS NOT NULL
                 AND revoked_at IS NULL AND expires_at>"#,
                now_ms!(),
                r#"
               RETURNING id,project_id,actor_account_id,target_account_id,tenant_id,delegation_actor,reason,expires_at"#
            )
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
        let developer_session = developer_session_metadata(
            &grant_id,
            &row.get::<String, _>("reason"),
            &actor_account_id,
            &account_id,
            &tenant_id,
            row.get::<i64, _>("expires_at"),
        );
        // Returning early drops the transaction, so a grant whose delegation
        // cannot be read is not consumed.
        let delegation = session_delegation(
            &actor_account_id,
            row.get::<Option<Json<Value>>, _>("delegation_actor")
                .map(|actor| actor.0),
        )?;
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
        let account_row = sqlx::query(concat!(
            r#"SELECT account.id,account.email,account.name,account.avatar_url,
                      COALESCE(identity.verified_email,0) AS email_verified,
                      COALESCE(identity.provider,'') AS provider
               FROM accounts account
               "#,
            latest_identity_join!(),
            r#"
               WHERE account.id=$1 AND account.auth_realm_id=$2 AND account.disabled_at IS NULL"#
        ))
        .bind(&account_id)
        .bind(&project_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DatabaseError::InvalidSession)?;
        transaction.commit().await?;
        drop(_admission);
        let (route, member, admission_revision) = if developer_session.is_some() {
            self.admit_developer(&project_id, &tenant_id, &account_id)
                .await?
        } else {
            self.admit_member(&project_id, &tenant_id, &account_id)
                .await?
        };
        Ok(ImpersonationSession {
            tenant: TenantSession {
                identity: SessionIdentity {
                    project_id,
                    account: account_from_row(&account_row),
                },
                route,
                member,
                admission_revision,
                delegation,
                developer_session,
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
        let _admission = self.pools.admit(&self.database_url).await?;
        let row = sqlx::query(concat!(
            r#"SELECT id,project_id,actor_account_id,target_account_id,tenant_id,reason,expires_at
               FROM gonvex_impersonation_grants
               WHERE reconnect_token_hash=$1 AND used_at IS NOT NULL
                 AND used_connection_id=$2 AND revoked_at IS NULL AND expires_at>"#,
            now_ms!()
        ))
        .bind(token_hash(token))
        .bind(connection_id)
        .fetch_optional(&self.reader)
        .await?
        .ok_or(DatabaseError::InvalidSession)?;
        let project_id: String = row.get("project_id");
        let account_id: String = row.get("target_account_id");
        let tenant_id: String = row.get("tenant_id");
        if project_id != requested_project_id || tenant_id != requested_tenant_id {
            return Err(DatabaseError::InvalidSession);
        }
        let developer = developer_session_metadata(
            &row.get::<String, _>("id"),
            &row.get::<String, _>("reason"),
            &row.get::<String, _>("actor_account_id"),
            &account_id,
            &tenant_id,
            row.get::<i64, _>("expires_at"),
        )
        .is_some();
        drop(_admission);
        if developer {
            self.admit_developer(&project_id, &tenant_id, &account_id)
                .await?;
        } else {
            self.admit_member(&project_id, &tenant_id, &account_id)
                .await?;
        }
        Ok(())
    }

    pub async fn tenant_session_for_account(
        &self,
        project_id: &str,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<TenantSession, DatabaseError> {
        let _admission = self.pools.admit(&self.database_url).await?;
        let row = sqlx::query(concat!(
            r#"SELECT account.id, account.email,
                      COALESCE(identity.verified_email, 0) AS email_verified,
                      account.name, account.avatar_url,
                      COALESCE(identity.provider, '') AS provider
               FROM accounts account
               "#,
            latest_identity_join!(),
            r#"
               WHERE account.id = $1
                 AND account.auth_realm_id = $2
                 AND account.disabled_at IS NULL"#
        ))
        .bind(account_id)
        .bind(project_id)
        .fetch_optional(&self.reader)
        .await?
        .ok_or(DatabaseError::InvalidSession)?;
        drop(_admission);
        let account = account_from_row(&row);
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
            delegation: None,
            developer_session: None,
        })
    }

    async fn first_admitted_tenant(
        &self,
        project_id: &str,
        account_id: &str,
    ) -> Result<String, DatabaseError> {
        let admission = self.pools.admit(&self.database_url).await?;
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
        .fetch_all(&self.reader)
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
        let _admission = self.pools.admit(&self.database_url).await?;
        let row = sqlx::query(
            r#"SELECT id, name, environment, database_mode, status, auth_mode
               FROM gonvex_runtime_projects
               WHERE id = $1 AND status NOT IN ('deleted', 'disabled')"#,
        )
        .bind(project_id)
        .fetch_optional(&self.reader)
        .await?
        .ok_or_else(|| DatabaseError::ProjectNotFound(project_id.to_owned()))?;
        Ok(project_from_row(row))
    }

    pub async fn runtime_manifests(&self) -> Result<Vec<RuntimeManifestRecord>, DatabaseError> {
        let _admission = self.pools.admit(&self.database_url).await?;
        let rows = sqlx::query(
            r#"SELECT project_id, module_hash, manifest
               FROM gonvex_runtime_manifests
               WHERE module_hash <> ''
               ORDER BY updated_at, project_id"#,
        )
        .fetch_all(&self.reader)
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
        let _admission = self.pools.admit(&self.database_url).await?;
        let project_key = sqlx::query_scalar::<_, String>(
            "SELECT project_key FROM gonvex_runtime_projects WHERE id=$1 AND status NOT IN ('deleted','disabled')",
        )
        .bind(project_id)
        .fetch_optional(&self.reader)
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
        let _admission = self.pools.admit(&self.database_url).await?;
        sqlx::query(concat!(
            "INSERT INTO gonvex_runtime_manifests(project_id,manifest,module_hash,updated_at) VALUES($1,$2,$3,",
            now_ms!(),
            ") ON CONFLICT(project_id) DO UPDATE SET manifest=excluded.manifest,module_hash=excluded.module_hash,updated_at=excluded.updated_at"
        ))
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
        let _admission = self.pools.admit(&self.database_url).await?;
        let row = sqlx::query(
            r#"SELECT tenant_id, project_id, name, domain, timezone, profile, status
               FROM gonvex_runtime_tenants
               WHERE project_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
                 AND status NOT IN ('deleted', 'disabled')"#,
        )
        .bind(project_id)
        .bind(tenant_id)
        .fetch_optional(&self.reader)
        .await?
        .ok_or_else(|| DatabaseError::TenantNotFound {
            project: project_id.to_owned(),
            tenant: tenant_id.to_owned(),
        })?;
        Ok(tenant_from_row(row))
    }

    pub async fn tenant_routes(&self, project_id: &str) -> Result<Vec<TenantRoute>, DatabaseError> {
        let project_id = required(project_id, DatabaseError::MissingProject)?;
        let _admission = self.pools.admit(&self.database_url).await?;
        let rows = sqlx::query(
            r#"SELECT tenant_id,database_url FROM gonvex_runtime_tenants
               WHERE project_id=$1 AND deleted_at IS NULL
                 AND status NOT IN ('deleted','disabled') ORDER BY tenant_id"#,
        )
        .bind(project_id)
        .fetch_all(&self.reader)
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

        let _admission = self.pools.admit(&self.database_url).await?;
        let row = sqlx::query(
            r#"SELECT tenant_id,NULLIF(database_url, '') AS database_url
               FROM gonvex_runtime_tenants
               WHERE project_id = $1
                 AND (tenant_id = $2 OR lower(domain) = lower($2))
                 AND deleted_at IS NULL
                 AND status NOT IN ('deleted', 'disabled')
               ORDER BY (tenant_id = $2) DESC LIMIT 1"#,
        )
        .bind(project_id)
        .bind(tenant_id)
        .fetch_optional(&self.reader)
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

    /// Public admission requires an active directory row, including configured routes.
    pub async fn resolve_public_tenant(
        &self,
        project: &str,
        tenant: &str,
    ) -> Result<TenantRoute, DatabaseError> {
        self.project(project).await?;
        let row = sqlx::query("SELECT tenant_id FROM gonvex_runtime_tenants WHERE project_id=$1 AND (tenant_id=$2 OR lower(domain)=lower($2)) AND status='active' AND deleted_at IS NULL ORDER BY (tenant_id=$2) DESC LIMIT 1")
            .bind(project).bind(tenant).fetch_optional(&self.reader).await?
            .ok_or_else(|| DatabaseError::TenantDatabaseMissing { tenant: tenant.to_owned() })?;
        self.resolve_tenant(project, row.get::<String, _>("tenant_id").as_str())
            .await
    }

    /// This is the final tenant admission gate. Directory rows only locate the
    /// tenant database; they cannot grant access. The Member row and the
    /// change-feed revision come from one read snapshot.
    pub async fn admit_member(
        &self,
        project_id: &str,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<(TenantRoute, Member, u64), DatabaseError> {
        let account_id = required(account_id, DatabaseError::MissingAccount)?;
        let route = self.resolve_tenant(project_id, tenant_id).await?;
        let mut transaction = begin_transaction(&self.pools, &route.database_url, true).await?;
        let row = sqlx::query(
            r#"SELECT id, account_id, status, display_name, avatar_url, role,
                      permissions, membership_revision
               FROM members
               WHERE account_id = $1 AND status = 'active'"#,
        )
        .bind(account_id)
        .fetch_optional(&mut **transaction.transaction())
        .await?
        .ok_or_else(|| DatabaseError::MemberNotFound(account_id.to_owned()))?;
        let revision: i64 =
            sqlx::query_scalar("SELECT revision FROM _gonvex_sync_clock WHERE singleton=1")
                .fetch_one(&mut **transaction.transaction())
                .await?;
        transaction.commit().await?;
        Ok((
            route,
            member_from_row(row),
            u64::try_from(revision).unwrap_or_default(),
        ))
    }

    /// Admission for a verified self developer grant only. The support principal
    /// is ephemeral: no members or account_tenant_index row is written. Ordinary
    /// sessions and user impersonation must continue through admit_member.
    async fn admit_developer(
        &self,
        project_id: &str,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<(TenantRoute, Member, u64), DatabaseError> {
        let row = sqlx::query(
            r#"SELECT account.name,account.avatar_url FROM accounts account
               WHERE account.id=$2 AND account.auth_realm_id=$1 AND account.disabled_at IS NULL
                 AND (EXISTS(SELECT 1 FROM gonvex_runtime_projects project
                            WHERE project.id=$1 AND project.owner_email<>''
                              AND lower(project.owner_email)=lower(account.email))
                   OR EXISTS(SELECT 1 FROM gonvex_project_members member
                             WHERE member.project_id=$1 AND lower(member.email)=lower(account.email)
                               AND member.role IN ('owner','admin','dev')))"#,
        )
        .bind(project_id)
        .bind(account_id)
        .fetch_optional(&self.reader)
        .await?
        .ok_or(DatabaseError::InvalidSession)?;
        let route = self.resolve_public_tenant(project_id, tenant_id).await?;
        match self
            .admit_member(project_id, &route.tenant_id, account_id)
            .await
        {
            Ok(member) => Ok(member),
            Err(DatabaseError::MemberNotFound(_)) => Ok((
                route,
                Member {
                    id: format!("developer:{account_id}"),
                    account_id: account_id.to_owned(),
                    status: "active".to_owned(),
                    display_name: row.get("name"),
                    avatar_url: row.get("avatar_url"),
                    role: "admin".to_owned(),
                    permissions: serde_json::json!({"developer":true}),
                    membership_revision: 0,
                },
                0,
            )),
            Err(error) => Err(error),
        }
    }

    /// The current row of one Member by ID, whatever its status.
    pub async fn member_by_id(
        &self,
        route: &TenantRoute,
        member_id: &str,
    ) -> Result<Option<Member>, DatabaseError> {
        let tenant_pool = self.pools.reader(&route.database_url).await?;
        let _admission = self.pools.admit(&route.database_url).await?;
        let row = sqlx::query(
            r#"SELECT id, account_id, status, display_name, avatar_url, role,
                      permissions, membership_revision
               FROM members
               WHERE id = $1"#,
        )
        .bind(member_id)
        .fetch_optional(&tenant_pool)
        .await?;
        Ok(row.map(member_from_row))
    }

    pub async fn begin_tenant_transaction(
        &self,
        route: &TenantRoute,
        read_only: bool,
    ) -> Result<TenantTransaction, DatabaseError> {
        begin_transaction(&self.pools, &route.database_url, read_only).await
    }

    pub async fn command_revision(
        &self,
        route: &TenantRoute,
        command_id: &str,
    ) -> Result<Option<u64>, DatabaseError> {
        let pool = self.pools.reader(&route.database_url).await?;
        let _admission = self.pools.admit(&route.database_url).await?;
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
        let _admission = self.pools.admit(&route.database_url).await?;
        // Inspect the catalog on every call; no process cache can outlive a
        // schema reset. Only cold initialization takes the writer.
        const STATUS: &str = r#"SELECT
            EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_reducer_idempotency'),
            EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_mutation_idempotency'),
            EXISTS(SELECT 1 FROM sqlite_schema WHERE type='index'
                AND name='gonvex_reducer_idempotency_created_at' AND tbl_name='_gonvex_reducer_idempotency')"#;
        let initialized = |(current, legacy, indexed): (bool, bool, bool)| {
            if current && legacy {
                Err(DatabaseError::AmbiguousReducerReceipts)
            } else {
                Ok(current && indexed)
            }
        };
        let reader = self.pools.reader(&route.database_url).await?;
        if initialized(sqlx::query_as(STATUS).fetch_one(&reader).await?)? {
            return Ok(());
        }
        // Serialize cold initialization and the legacy rename. Recheck after
        // acquiring the lock so a waiter never repeats finished DDL.
        let _lock = self
            .pools
            .lock(&route.database_url, "gonvex:reducer-receipt-schema")
            .await?;
        let pool = self.pools.pool(&route.database_url).await?;
        let mut transaction = pool.begin().await?;
        let (current, legacy, indexed) =
            sqlx::query_as(STATUS).fetch_one(&mut *transaction).await?;
        if initialized((current, legacy, indexed))? {
            transaction.commit().await?;
            return Ok(());
        }
        if legacy && !current {
            sqlx::query(
                "ALTER TABLE _gonvex_mutation_idempotency RENAME TO _gonvex_reducer_idempotency",
            )
            .execute(&mut *transaction)
            .await?;
        }
        crate::execute_script(
            &mut *transaction,
            concat!(
                r#"CREATE TABLE IF NOT EXISTS _gonvex_reducer_idempotency (
                 subject TEXT NOT NULL DEFAULT '',
                 idempotency_key TEXT NOT NULL,
                 path TEXT NOT NULL,
                 result JSON,
                 created_at INTEGER NOT NULL DEFAULT ("#,
                now_ms!(),
                r#"),
                 PRIMARY KEY (subject, idempotency_key)
               );
               CREATE INDEX IF NOT EXISTS gonvex_reducer_idempotency_created_at
                 ON _gonvex_reducer_idempotency (created_at);"#
            ),
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn replay_reducer_result(
        &self,
        route: &TenantRoute,
        subject: &str,
        key: &str,
        path: &str,
    ) -> Result<Value, DatabaseError> {
        let pool = self.pools.reader(&route.database_url).await?;
        let _admission = self.pools.admit(&route.database_url).await?;
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

    /// Claims the oldest due action, or one whose delivery lease (five
    /// minutes) expired. The single writer serializes claims.
    pub async fn claim_action(
        &self,
        route: &TenantRoute,
    ) -> Result<Option<ClaimedAction>, DatabaseError> {
        let pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit(&route.database_url).await?;
        let row = sqlx::query(concat!(
            "UPDATE _gonvex_action_outbox SET status = 'processing', locked_at = ",
            now_ms!(),
            r#", attempts = attempts + 1
               WHERE id = (
                 SELECT id FROM _gonvex_action_outbox
                 WHERE (status = 'pending' AND available_at <= "#,
            now_ms!(),
            r#")
                    OR (status = 'processing' AND locked_at < "#,
            now_ms!(),
            r#" - 300000)
                 ORDER BY available_at, created_at LIMIT 1)
               RETURNING id, action_path, args,
                         COALESCE(actor_user_id, '') AS actor_account_id,
                         COALESCE(actor_email, '') AS actor_email,
                         attempts AS next_attempt, provenance"#
        ))
        .fetch_optional(&pool)
        .await?;
        Ok(row.map(|row| ClaimedAction {
            id: row.get("id"),
            path: row.get("action_path"),
            args: row.get("args"),
            actor_account_id: row.get("actor_account_id"),
            actor_email: row.get("actor_email"),
            attempts: row.get("next_attempt"),
            provenance: row.get("provenance"),
        }))
    }

    /// Only the current delivery attempt may extend its claim.
    pub async fn renew_action(
        &self,
        route: &TenantRoute,
        action_id: &str,
        attempt: i32,
    ) -> Result<bool, DatabaseError> {
        let pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit(&route.database_url).await?;
        let result = sqlx::query(concat!(
            "UPDATE _gonvex_action_outbox SET locked_at = ",
            now_ms!(),
            " WHERE id = $1 AND status = 'processing' AND attempts = $2"
        ))
        .bind(action_id)
        .bind(attempt)
        .execute(&pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn complete_action(
        &self,
        route: &TenantRoute,
        action_id: &str,
    ) -> Result<(), DatabaseError> {
        let pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit(&route.database_url).await?;
        sqlx::query("DELETE FROM _gonvex_action_outbox WHERE id = $1")
            .bind(action_id)
            .execute(&pool)
            .await?;
        Ok(())
    }

    /// Releases a failed delivery; it becomes due again after `attempts`
    /// seconds (clamped to 1–10).
    pub async fn retry_action(
        &self,
        route: &TenantRoute,
        action_id: &str,
        attempts: i32,
        error: &str,
    ) -> Result<(), DatabaseError> {
        let pool = self.pools.pool(&route.database_url).await?;
        let _admission = self.pools.admit(&route.database_url).await?;
        sqlx::query(concat!(
            r#"UPDATE _gonvex_action_outbox
               SET status = 'pending', locked_at = NULL,
                   available_at = "#,
            now_ms!(),
            r#" + $2,
                   last_error = $3
               WHERE id = $1"#
        ))
        .bind(action_id)
        .bind(retry_delay_ms(attempts))
        .bind(error)
        .execute(&pool)
        .await?;
        Ok(())
    }
}

/// Exponential backoff for a failed Action: 1 s after the first failure,
/// doubling, capped at 15 minutes. A linear 1–10 s backoff retried a
/// permanently failing Action every 10 s forever, and each retry is two
/// writes on the tenant's single writer.
pub fn retry_delay_ms(attempts: i32) -> i64 {
    const CAP_MS: i64 = 15 * 60 * 1000;
    let exponent = attempts.clamp(1, 30) - 1;
    1000i64
        .checked_shl(exponent as u32)
        .unwrap_or(CAP_MS)
        .min(CAP_MS)
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
    transaction: Transaction<'static, Sqlite>,
    _admission: AdmissionPermit,
    read_only: bool,
    provenance: Provenance,
    publisher: Option<(PoolRegistry, Arc<str>)>,
    committed: Option<CommitNotice>,
    failed: Option<String>,
}

/// Opens a transaction on the writer (or, when read-only, on a reader whose
/// WAL snapshot stays fixed for the whole transaction).
pub async fn begin_transaction(
    pools: &PoolRegistry,
    database_url: &str,
    read_only: bool,
) -> Result<TenantTransaction, DatabaseError> {
    let admission = pools.admit(database_url).await?;
    let pool = if read_only {
        pools.reader(database_url).await?
    } else {
        pools.pool(database_url).await?
    };
    let transaction = pool.begin().await?;
    Ok(TenantTransaction {
        transaction,
        _admission: admission,
        read_only,
        provenance: Provenance::default(),
        publisher: (!read_only).then(|| (pools.clone(), Arc::from(database_url.trim()))),
        committed: None,
        failed: None,
    })
}

impl TenantTransaction {
    pub fn transaction(&mut self) -> &mut Transaction<'static, Sqlite> {
        &mut self.transaction
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    pub async fn set_command_id(&mut self, command_id: &str) -> Result<(), DatabaseError> {
        self.provenance.command_id = Some(command_id.to_owned());
        Ok(())
    }

    pub async fn set_invocation_provenance(
        &mut self,
        attribution: TransactionAttribution<'_>,
    ) -> Result<(), DatabaseError> {
        let owned = |value: Option<&str>| value.map(str::to_owned);
        self.provenance.root_command_id = Some(attribution.root_command_id.to_owned());
        self.provenance.root_channel = Some(attribution.root_channel.to_owned());
        self.provenance.channel = Some(attribution.channel.to_owned());
        self.provenance.actor_account_id = owned(attribution.actor_account_id);
        self.provenance.actor_member_id = owned(attribution.actor_member_id);
        self.provenance.on_behalf_of_member_id = owned(attribution.on_behalf_of_member_id);
        self.provenance.agent_execution_id = owned(attribution.agent_execution_id);
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
        self.enqueue_action_with_id(
            &id,
            action_path,
            args,
            actor_account_id,
            actor_email,
            provenance,
        )
        .await
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
        .bind(id)
        .bind(action_path)
        .bind(Json(args.clone()))
        .bind(actor_account_id)
        .bind(actor_email)
        .bind(Json(provenance.clone()))
        .execute(&mut *self.transaction)
        .await?;
        Ok(id.to_owned())
    }

    /// Records that a statement in this transaction failed. Postgres aborts
    /// the whole transaction on any statement error; SQLite keeps it usable,
    /// so callers that run module SQL mark the failure and `commit` refuses,
    /// preserving "a failed intent rolls back completely".
    pub fn mark_failed(&mut self, reason: impl Into<String>) {
        if self.failed.is_none() {
            self.failed = Some(reason.into());
        }
    }

    pub fn failed(&self) -> Option<&str> {
        self.failed.as_deref()
    }

    /// Clears a recorded failure after the caller rolled back to a savepoint
    /// taken before it, which Postgres also treats as recovery.
    pub fn clear_failure(&mut self) {
        self.failed = None;
    }

    /// Commits and returns the revision it published, if it changed rows.
    pub async fn commit_with_revision(mut self) -> Result<Option<u64>, DatabaseError> {
        if let Some(reason) = self.failed.take() {
            self.transaction.rollback().await?;
            return Err(DatabaseError::TransactionAborted(reason));
        }
        let notice = if self.read_only {
            None
        } else {
            let finalized = async {
                reference_integrity::flush(&mut self.transaction).await?;
                feed::finalize(&mut self.transaction, &self.provenance).await
            }
            .await;
            match finalized {
                Ok(notice) => notice,
                Err(error) => {
                    self.transaction.rollback().await?;
                    return Err(error);
                }
            }
        };
        self.transaction.commit().await?;
        // Acknowledge and publish only once the commit is on disk; one WAL
        // sync covers every commit that finished before it started.
        if let Some((pools, database_url)) = &self.publisher {
            pools.durable(database_url).await?;
        }
        let revision = notice.as_ref().map(|notice| notice.revision);
        if let (Some(notice), Some((pools, database_url))) = (notice, self.publisher.take()) {
            pools
                .notify(&database_url, feed::CHANGE_FEED_CHANNEL, &notice.payload())
                .await?;
            self.committed = Some(notice);
        }
        Ok(revision)
    }

    pub async fn commit(self) -> Result<(), DatabaseError> {
        self.commit_with_revision().await.map(|_| ())
    }

    pub async fn rollback(self) -> Result<(), DatabaseError> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

#[cfg(test)]
mod transaction_commit_tests;

fn required(value: &str, error: DatabaseError) -> Result<&str, DatabaseError> {
    let value = value.trim();
    if value.is_empty() {
        Err(error)
    } else {
        Ok(value)
    }
}

fn project_from_row(row: SqliteRow) -> Project {
    Project {
        id: row.get("id"),
        name: row.get("name"),
        environment: row.get("environment"),
        database_mode: row.get("database_mode"),
        status: row.get("status"),
        auth_mode: row.get("auth_mode"),
    }
}

fn tenant_from_row(row: SqliteRow) -> TenantDirectoryEntry {
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

fn account_from_row(row: &SqliteRow) -> Account {
    Account {
        id: row.get("id"),
        email: row.get("email"),
        email_verified: row.get::<i64, _>("email_verified") != 0,
        name: row.get("name"),
        avatar_url: row.get("avatar_url"),
        provider: row.get("provider"),
    }
}

fn member_from_row(row: SqliteRow) -> Member {
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

/// Runs a multi-statement SQL script (trigger bodies included) on one
/// connection. Unlike `sqlx::raw_sql(..).execute(..)`, the future is `Send`
/// for any borrowed connection, so it can run inside spawned tasks.
pub async fn execute_script(
    connection: &mut sqlx::SqliteConnection,
    sql: &str,
) -> Result<(), DatabaseError> {
    use sqlx::Executor as _;
    connection.execute(sqlx::raw_sql(sql)).await?;
    // Scripts that create or drop triggers can change which tables a writer
    // captures; keep its capture catalog in step (no-op on other connections).
    if sql.to_ascii_uppercase().contains("TRIGGER") {
        capture::refresh(connection).await?;
    }
    Ok(())
}

/// Current Unix time in milliseconds, the unit of every stored timestamp.
pub fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
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

/// Recognizes a grant minted by a service principal. Only the runtime writes
/// grant rows and it validates the actor first, so an actor that cannot be
/// read here means the row was altered. Such a grant is refused: dropping the
/// actor would let a module that trusts `actor.kind` see a weaker claim.
fn session_delegation(
    actor_account_id: &str,
    actor: Option<Value>,
) -> Result<Option<SessionDelegation>, DatabaseError> {
    let Some(principal) = actor_account_id.strip_prefix(SERVICE_PRINCIPAL_ACTOR_PREFIX) else {
        return Ok(None);
    };
    if principal.is_empty() {
        return Err(DatabaseError::InvalidSession);
    }
    let actor = match actor {
        None | Some(Value::Null) => None,
        Some(actor) => Some(
            serde_json::from_value::<DelegationActor>(actor)
                .map_err(|_| DatabaseError::InvalidSession)?,
        ),
    };
    Ok(Some(SessionDelegation {
        principal: principal.to_owned(),
        actor,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn admission_metrics_include_success_and_timeout() {
        let limits = PoolLimits {
            max_total_connections: 1,
            max_connections_per_database: 1,
            acquire_timeout: std::time::Duration::from_millis(20),
            ..PoolLimits::default()
        };
        let registry = PoolRegistry::new(limits);
        let held = registry
            .admit("sqlite:///nonexistent/admission.db")
            .await
            .unwrap();
        assert!(matches!(
            registry.admit("sqlite:///nonexistent/admission.db").await,
            Err(DatabaseError::AdmissionTimeout)
        ));
        let snapshot = serde_json::to_value(registry.snapshot().await).unwrap();
        assert_eq!(snapshot["admissionCount"].as_u64(), Some(2));
        assert!(snapshot["admissionWaitUs"].as_u64().unwrap_or(0) >= 20_000);
        assert!(snapshot["admissionWaitMaxUs"].as_u64().unwrap_or(0) >= 20_000);
        drop(held);
        assert_eq!(registry.snapshot().await.admission_active, 0);
    }

    #[test]
    fn only_verified_self_developer_grants_expose_developer_metadata() {
        let expiry = unix_millis() + 60_000;
        let accepted = developer_session_metadata(
            "devmode_test",
            "developer mode",
            "acct_self",
            "acct_self",
            "tenant_a",
            expiry,
        )
        .unwrap();
        assert_eq!(accepted["accountId"], "acct_self");
        assert_eq!(accepted["tenantId"], "tenant_a");
        assert_eq!(accepted["expiresAt"], expiry);
        assert!(developer_session_metadata(
            "imp_test",
            "developer mode",
            "acct_self",
            "acct_self",
            "tenant_a",
            expiry
        )
        .is_none());
        assert!(developer_session_metadata(
            "devmode_test",
            "developer mode",
            "acct_actor",
            "acct_target",
            "tenant_a",
            expiry
        )
        .is_none());
        assert!(developer_session_metadata(
            "devmode_test",
            "support",
            "acct_self",
            "acct_self",
            "tenant_a",
            expiry
        )
        .is_none());
        assert!(developer_session_metadata(
            "devmode_test",
            "developer mode",
            "acct_self",
            "acct_self",
            "tenant_a",
            0
        )
        .is_none());
    }

    #[test]
    fn only_service_principal_grants_produce_delegated_sessions() {
        // Developer mode and support impersonation grants name an account.
        assert_eq!(
            session_delegation("acct_123", None).unwrap(),
            None,
            "support impersonation is not a delegation"
        );
        assert_eq!(
            session_delegation(
                "acct_123",
                Some(serde_json::json!({"kind":"api_key","name":"x"}))
            )
            .unwrap(),
            None
        );
        assert_eq!(
            session_delegation("service:gateway", None).unwrap(),
            Some(SessionDelegation {
                principal: "gateway".to_owned(),
                actor: None,
            })
        );
        assert_eq!(
            session_delegation(
                "service:gateway",
                Some(serde_json::json!({"kind":"api_key","name":"CI key","reference":"key_1"}))
            )
            .unwrap(),
            Some(SessionDelegation {
                principal: "gateway".to_owned(),
                actor: Some(DelegationActor {
                    kind: "api_key".to_owned(),
                    name: "CI key".to_owned(),
                    reference: Some("key_1".to_owned()),
                }),
            })
        );
        assert!(session_delegation("service:", None).is_err());
        assert!(session_delegation("service:gateway", Some(serde_json::json!("api_key"))).is_err());
        assert!(session_delegation(
            "service:gateway",
            Some(serde_json::json!({"kind":"api_key"}))
        )
        .is_err());
    }

    #[test]
    fn action_retries_back_off_exponentially_to_a_cap() {
        assert_eq!(retry_delay_ms(1), 1_000);
        assert_eq!(retry_delay_ms(2), 2_000);
        assert_eq!(retry_delay_ms(5), 16_000);
        assert_eq!(retry_delay_ms(11), 900_000);
        assert_eq!(retry_delay_ms(40), 900_000);
        assert_eq!(retry_delay_ms(0), 1_000);
    }

    #[test]
    fn clamps_connection_limits() {
        let limits = PoolLimits {
            max_total_connections: 500,
            max_connections_per_database: 200,
            max_idle_connections_per_database: 300,
            ..PoolLimits::default()
        }
        .bounded();
        assert_eq!(limits.max_total_connections, 256);
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
            "sqlite:///trusted/tenant-a.db".to_owned(),
        )]);
        let control = ControlPlane {
            database_url: "sqlite::memory:".into(),
            pool: sqlx::sqlite::SqlitePoolOptions::new()
                .connect_lazy("sqlite::memory:")
                .unwrap(),
            reader: sqlx::sqlite::SqlitePoolOptions::new()
                .connect_lazy("sqlite::memory:")
                .unwrap(),
            pools: PoolRegistry::new(PoolLimits::default()),
            configured_tenants: Arc::new(configured),
        };
        let route = control
            .resolve_tenant("project-a", "tenant-a")
            .await
            .unwrap();
        assert_eq!(route.database_url, "sqlite:///trusted/tenant-a.db");
        assert_eq!(route.project_id, "project-a");
        assert_eq!(route.tenant_id, "tenant-a");
    }
}
