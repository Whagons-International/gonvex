//! Control Plane schema, database file provisioning, migrations and change
//! capture installation on SQLite.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::time::Instant;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection};

use super::data_dir::DataDir;
use super::feed::{self, CommitNotice, Provenance, SYNC_INFRASTRUCTURE_SQL};
use super::registry::database_path;
use super::rows::quote_identifier;
use super::{ControlPlane, DatabaseError, PoolRegistry, TenantRoute};

/// Keyed lock (see [`PoolRegistry::lock`]) that serializes schema changes on
/// one database: migrations, change-capture installation and reference
/// declarations.
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub(crate) const SCHEMA_LOCK: &str = "gonvex_sql_migrations";

fn project_key(project_id: &str) -> String {
    let mut secret = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut secret);
    format!(
        "gvx_{}.{}",
        URL_SAFE_NO_PAD.encode(project_id.trim().as_bytes()),
        URL_SAFE_NO_PAD.encode(secret)
    )
}

#[cfg(test)]
mod project_key_tests {
    use super::*;

    #[test]
    fn generated_key_embeds_the_project_id() {
        let id = "4985bbc5-74e7-4c82-b3aa-fbadc49c8090";
        let key = project_key(id);
        let encoded = key
            .strip_prefix("gvx_")
            .and_then(|value| value.split_once('.'))
            .map(|(project, _)| project)
            .expect("versioned project key");
        assert_eq!(URL_SAFE_NO_PAD.decode(encoded).unwrap(), id.as_bytes());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationScope {
    ControlPlane,
    Tenant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMigration {
    pub name: String,
    pub checksum: String,
    pub scope: MigrationScope,
    pub no_transaction: bool,
    pub sql: String,
}

impl SqlMigration {
    pub fn new(name: String, scope: MigrationScope, no_transaction: bool, sql: String) -> Self {
        let checksum = Sha256::digest(sql.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Self {
            name,
            checksum,
            scope,
            no_transaction,
            sql,
        }
    }
}

/// Columns added to Control Plane tables after the table first shipped.
/// SQLite cannot `ADD COLUMN IF NOT EXISTS` or add a column with an
/// expression default, so older files receive these definitions (constant
/// defaults) before `control_schema.sql` runs.
const ADDITIVE_COLUMNS: &[(&str, &str, &str)] = &[
    (
        "gonvex_dashboard_accounts",
        "session_revision",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_runtime_projects",
        "test_tab",
        "BOOLEAN NOT NULL DEFAULT 0",
    ),
    (
        "gonvex_runtime_projects",
        "error_tracking_enabled",
        "BOOLEAN NOT NULL DEFAULT 0",
    ),
    (
        "gonvex_runtime_projects",
        "database_mode",
        "TEXT NOT NULL DEFAULT 'single'",
    ),
    (
        "gonvex_runtime_projects",
        "owner_email",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_runtime_projects",
        "auth_mode",
        "TEXT NOT NULL DEFAULT 'gonvex-native'",
    ),
    (
        "account_identities",
        "sign_in_provider",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_runtime_tenants",
        "timezone",
        "TEXT NOT NULL DEFAULT 'UTC'",
    ),
    (
        "gonvex_runtime_tenants",
        "profile",
        "JSON NOT NULL DEFAULT '{}'",
    ),
    ("gonvex_runtime_tenants", "seat_limit", "INTEGER"),
    ("gonvex_runtime_tenants", "deleted_at", "INTEGER"),
    ("gonvex_runtime_tenants", "deletion_idempotency_key", "TEXT"),
    (
        "gonvex_project_invitations",
        "token_hash",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_auth_providers",
        "signup_mode",
        "TEXT NOT NULL DEFAULT 'personal'",
    ),
    (
        "gonvex_auth_providers",
        "azure_tenant_id",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_auth_providers",
        "client_id",
        "TEXT NOT NULL DEFAULT ''",
    ),
    ("gonvex_auth_providers", "client_secret_encrypted", "BLOB"),
    (
        "gonvex_auth_providers",
        "issuer",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_auth_providers",
        "audience",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_auth_providers",
        "jwks_url",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_auth_providers",
        "firebase_project_id",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_auth_providers",
        "firebase_tenant_id",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_auth_providers",
        "admin_credentials_encrypted",
        "BLOB",
    ),
    (
        "gonvex_auth_transactions",
        "provider",
        "TEXT NOT NULL DEFAULT 'google'",
    ),
    // Expression defaults cannot be added; new rows always set expires_at.
    (
        "gonvex_auth_membership_invitations",
        "expires_at",
        "INTEGER NOT NULL DEFAULT 0",
    ),
    ("gonvex_auth_membership_invitations", "id", "TEXT"),
    ("gonvex_auth_membership_invitations", "token_hash", "TEXT"),
    (
        "gonvex_auth_membership_invitations",
        "revoked_at",
        "INTEGER",
    ),
    (
        "gonvex_auth_membership_invitations",
        "accepted_at",
        "INTEGER",
    ),
    (
        "gonvex_auth_membership_invitations",
        "accepted_account_id",
        "TEXT",
    ),
    (
        "gonvex_auth_membership_invitations",
        "accepted_idempotency_key",
        "TEXT",
    ),
    (
        "gonvex_auth_membership_invitations",
        "team_ids",
        "JSON NOT NULL DEFAULT '[]'",
    ),
    (
        "gonvex_auth_membership_invitations",
        "allowed_auth_providers",
        "JSON NOT NULL DEFAULT '[]'",
    ),
    (
        "gonvex_auth_membership_invitations",
        "application_payload",
        "JSON NOT NULL DEFAULT '{}'",
    ),
    (
        "gonvex_auth_membership_invitations",
        "handoff_state",
        "TEXT NOT NULL DEFAULT 'pending'",
    ),
    (
        "gonvex_auth_membership_invitations",
        "handoff_command_id",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_auth_membership_invitations",
        "completed_at",
        "INTEGER",
    ),
    (
        "gonvex_auth_membership_invitations",
        "updated_at",
        "INTEGER NOT NULL DEFAULT 0",
    ),
    (
        "gonvex_tenant_provisioning",
        "domain",
        "TEXT NOT NULL DEFAULT ''",
    ),
    (
        "gonvex_scheduled_jobs",
        "provenance",
        "JSON NOT NULL DEFAULT '{}'",
    ),
    (
        "gonvex_scheduled_jobs",
        "wake_key",
        "TEXT NOT NULL DEFAULT ''",
    ),
    ("gonvex_impersonation_grants", "delegation_actor", "JSON"),
];

pub(crate) async fn table_exists(
    connection: &mut SqliteConnection,
    name: &str,
) -> Result<bool, DatabaseError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=$1)",
    )
    .bind(name)
    .fetch_one(connection)
    .await?)
}

pub(crate) async fn column_exists(
    connection: &mut SqliteConnection,
    table: &str,
    column: &str,
) -> Result<bool, DatabaseError> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pragma_table_info($1) WHERE name=$2)")
            .bind(table)
            .bind(column)
            .fetch_one(connection)
            .await?,
    )
}

fn protocol(message: impl Into<String>) -> DatabaseError {
    sqlx::Error::Protocol(message.into()).into()
}

impl ControlPlane {
    pub(crate) async fn prepare_identity_schema_upgrade(&self) -> Result<(), DatabaseError> {
        let _admission = self.pools.admit(&self.database_url).await?;
        let _schema = self
            .pools
            .lock(&self.database_url, "identity-schema-upgrade")
            .await?;
        let mut transaction = self.pool.begin().await?;

        let mut legacy_tables = Vec::new();
        for table in [
            "gonvex_auth_codes",
            "gonvex_auth_sessions",
            "gonvex_auth_refresh_tokens",
        ] {
            if column_exists(&mut transaction, table, "user_id").await? {
                legacy_tables.push(table);
            }
        }
        for table in ["gonvex_auth_memberships", "gonvex_auth_users"] {
            if table_exists(&mut transaction, table).await? {
                legacy_tables.push(table);
            }
        }
        if legacy_tables.is_empty() {
            transaction.commit().await?;
            return Ok(());
        }

        for table in &legacy_tables {
            let populated: bool =
                sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {table} LIMIT 1)"))
                    .fetch_one(&mut *transaction)
                    .await?;
            if populated {
                return Err(protocol(format!(
                    "legacy identity table {table} contains data; run `gonvex-admin migrate identity-v2` before starting this runtime"
                )));
            }
        }

        // An empty pre-identity-v2 installation has no identity to preserve.
        // Remove only tables with the legacy user_id shape and let the
        // canonical schema below recreate them with account_id. Non-empty
        // installations are rejected above and require the reviewed migration.
        for table in [
            "gonvex_auth_refresh_tokens",
            "gonvex_auth_sessions",
            "gonvex_auth_codes",
            "gonvex_auth_memberships",
            "gonvex_auth_users",
        ] {
            if legacy_tables.contains(&table) {
                sqlx::query(&format!("DROP TABLE {table}"))
                    .execute(&mut *transaction)
                    .await?;
            }
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn ensure_control_schema(&self) -> Result<(), DatabaseError> {
        let _admission = self.pools.admit(&self.database_url).await?;
        let _schema = self.pools.lock(&self.database_url, SCHEMA_LOCK).await?;
        let mut transaction = self.pool.begin().await?;
        // Older layouts: renamed objects first, then additive columns, so the
        // canonical script's indexes and updates find what they reference.
        if column_exists(&mut transaction, "gonvex_runtime_manifests", "bundle_hash").await?
            && !column_exists(&mut transaction, "gonvex_runtime_manifests", "module_hash").await?
        {
            sqlx::query(
                "ALTER TABLE gonvex_runtime_manifests RENAME COLUMN bundle_hash TO module_hash",
            )
            .execute(&mut *transaction)
            .await?;
        }
        if table_exists(&mut transaction, "gonvex_runtime_mutation_logs").await?
            && !table_exists(&mut transaction, "gonvex_runtime_reducer_logs").await?
        {
            sqlx::query(
                "ALTER TABLE gonvex_runtime_mutation_logs RENAME TO gonvex_runtime_reducer_logs",
            )
            .execute(&mut *transaction)
            .await?;
        }
        for (table, column, definition) in ADDITIVE_COLUMNS {
            if table_exists(&mut transaction, table).await?
                && !column_exists(&mut transaction, table, column).await?
            {
                sqlx::query(&format!(
                    "ALTER TABLE {table} ADD COLUMN {} {definition}",
                    quote_identifier(column)
                ))
                .execute(&mut *transaction)
                .await?;
            }
        }
        crate::execute_script(&mut transaction, include_str!("control_schema.sql")).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Creates (or opens) a project's own database file and returns its URL.
    /// `data_dir` is the data directory path or `sqlite://` URL.
    pub async fn create_project_database(
        &self,
        data_dir: &str,
        project_id: &str,
    ) -> Result<String, DatabaseError> {
        let url = DataDir::parse(data_dir).project_url(project_id)?;
        self.pools.pool(&url).await?;
        Ok(url)
    }

    /// Creates (or opens) a tenant shard file and returns its URL.
    pub async fn create_tenant_database(
        &self,
        data_dir: &str,
        project_id: &str,
        tenant_id: &str,
    ) -> Result<String, DatabaseError> {
        let url = DataDir::parse(data_dir).tenant_url(project_id, tenant_id)?;
        self.pools.pool(&url).await?;
        Ok(url)
    }

    pub async fn create_runtime_project(
        &self,
        data_dir: &str,
        name: &str,
        database_mode: &str,
        owner_email: &str,
    ) -> Result<serde_json::Value, DatabaseError> {
        if !matches!(database_mode, "single" | "multiTenant") {
            return Err(protocol("databaseMode must be single or multiTenant"));
        }
        let name = name.trim();
        if name.is_empty() || name.len() > 120 {
            return Err(protocol("project name is required"));
        }
        let project_id = uuid::Uuid::new_v4().to_string();
        let data = DataDir::parse(data_dir);
        let database_name = relative_name(&data, &data.project_path(&project_id)?);
        let database_url = self.create_project_database(data_dir, &project_id).await?;
        let project_key = project_key(&project_id);
        let mut transaction = self.begin_control_transaction(false).await?;
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_projects
               (id,name,environment,database_name,database_mode,database_url,storage_bucket,
                status,description,project_key,provisioned,runtime_created,owner_email)
               VALUES($1,$2,'development',$3,$4,$5,$6,'active','Runtime-created project database.',
                      $7,1,1,$8)"#,
        )
        .bind(&project_id)
        .bind(name)
        .bind(&database_name)
        .bind(database_mode)
        .bind(&database_url)
        .bind(format!("{project_id}-dev"))
        .bind(&project_key)
        .bind(owner_email.trim().to_lowercase())
        .execute(&mut **transaction.transaction())
        .await?;
        if !owner_email.trim().is_empty() {
            sqlx::query(
                r#"INSERT INTO gonvex_project_members(project_id,email,name,role)
                   VALUES($1,lower($2),$3,'owner') ON CONFLICT(project_id,email) DO UPDATE SET role='owner'"#,
            )
            .bind(&project_id)
            .bind(owner_email.trim())
            .bind(owner_email.split('@').next().unwrap_or(owner_email))
            .execute(&mut **transaction.transaction())
            .await?;
        }
        if database_mode == "single" {
            sqlx::query(
                r#"INSERT INTO gonvex_runtime_tenants
                   (relationship_id,project_id,tenant_id,name,database_alias,database_name,
                    database_url,status,description,provisioned,runtime_created)
                   VALUES($1,$1,$1,$2,'default',$3,$4,'active','Single-database tenant.',1,1)"#,
            )
            .bind(&project_id)
            .bind(name)
            .bind(&database_name)
            .bind(&database_url)
            .execute(&mut **transaction.transaction())
            .await?;
        }
        transaction.commit().await?;
        Ok(serde_json::json!({
            "project":{
                "id":project_id,"name":name,"environment":"development",
                "database":database_name,"databaseMode":database_mode,
                "storageBucket":format!("{project_id}-dev"),"status":"active",
                "description":"Runtime-created project database.",
            },
            "projectKey":project_key,
        }))
    }

    pub async fn runtime_projects(&self) -> Result<Vec<serde_json::Value>, DatabaseError> {
        let mut transaction = self.begin_control_transaction(true).await?;
        let rows = sqlx::query(
            r#"SELECT id,name,environment,database_name,database_mode,storage_bucket,status,description,
                      provisioned,runtime_created,test_tab,error_tracking_enabled,owner_email
               FROM gonvex_runtime_projects WHERE status NOT IN('deleted','disabled') ORDER BY name,id"#,
        )
        .fetch_all(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "id":row.get::<String,_>("id"),"name":row.get::<String,_>("name"),
                    "environment":row.get::<String,_>("environment"),
                    "database":row.get::<String,_>("database_name"),
                    "databaseMode":row.get::<String,_>("database_mode"),
                    "storageBucket":row.get::<String,_>("storage_bucket"),
                    "status":row.get::<String,_>("status"),
                    "description":row.get::<String,_>("description"),
                    "provisioned":row.get::<bool,_>("provisioned"),
                    "runtimeCreated":row.get::<bool,_>("runtime_created"),
                    "testTab":row.get::<bool,_>("test_tab"),
                    "errorTrackingEnabled":row.get::<bool,_>("error_tracking_enabled"),
                    "ownerEmail":row.get::<String,_>("owner_email"),
                })
            })
            .collect())
    }

    pub async fn runtime_project_key(&self, project_id: &str) -> Result<String, DatabaseError> {
        let mut transaction = self.begin_control_transaction(true).await?;
        let key = sqlx::query_scalar::<_, String>(
            "SELECT project_key FROM gonvex_runtime_projects WHERE id=$1 AND status NOT IN('deleted','disabled')",
        )
        .bind(project_id)
        .fetch_optional(&mut **transaction.transaction())
        .await?
        .ok_or_else(|| DatabaseError::ProjectNotFound(project_id.to_owned()))?;
        transaction.commit().await?;
        Ok(key)
    }

    /// Deletes a runtime-created project and its database files. Only files
    /// inside `{data}/projects/{project}` are removed.
    pub async fn delete_runtime_project(
        &self,
        data_dir: &str,
        project_id: &str,
    ) -> Result<(), DatabaseError> {
        let data = DataDir::parse(data_dir);
        let project_dir = data.project_dir(project_id)?;
        let mut transaction = self.begin_control_transaction(false).await?;
        let project = sqlx::query(
            r#"SELECT runtime_created,database_url FROM gonvex_runtime_projects
               WHERE id=$1 AND status NOT IN('deleted','disabled')"#,
        )
        .bind(project_id)
        .fetch_optional(&mut **transaction.transaction())
        .await?
        .ok_or_else(|| DatabaseError::ProjectNotFound(project_id.to_owned()))?;
        if !project.get::<bool, _>("runtime_created") {
            return Err(protocol(
                "only runtime-created projects may be deleted through this operation",
            ));
        }
        let mut databases = sqlx::query_scalar::<_, String>(
            r#"SELECT database_url FROM gonvex_runtime_tenants
               WHERE project_id=$1 AND runtime_created=1 AND database_url<>''"#,
        )
        .bind(project_id)
        .fetch_all(&mut **transaction.transaction())
        .await?;
        let project_database: String = project.get("database_url");
        if !project_database.is_empty() {
            databases.push(project_database);
        }
        databases.sort();
        databases.dedup();
        for url in &databases {
            owned_database_file(&project_dir, url)?;
        }
        sqlx::query(&format!(
            "UPDATE gonvex_runtime_projects SET status='deleting',updated_at={} WHERE id=$1",
            feed::NOW_MS
        ))
        .bind(project_id)
        .execute(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;

        for url in &databases {
            self.pools.close_database(url).await;
        }
        match std::fs::remove_dir_all(&project_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(DatabaseError::Io(error.to_string())),
        }
        let mut finish = self.begin_control_transaction(false).await?;
        sqlx::query("DELETE FROM gonvex_runtime_projects WHERE id=$1 AND status='deleting'")
            .bind(project_id)
            .execute(&mut **finish.transaction())
            .await?;
        finish.commit().await?;
        Ok(())
    }

    pub async fn runtime_tenants(
        &self,
        project_id: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, DatabaseError> {
        let mut transaction = self.begin_control_transaction(true).await?;
        let rows = sqlx::query(
            r#"SELECT tenant_id,project_id,name,database_alias,database_name,status,
                      description,domain,timezone,profile,provisioned
               FROM gonvex_runtime_tenants
               WHERE ($1='' OR project_id=$1) AND deleted_at IS NULL
                 AND status NOT IN('deleted','disabled') ORDER BY project_id,name,tenant_id"#,
        )
        .bind(project_id.unwrap_or("").trim())
        .fetch_all(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "id":row.get::<String,_>("tenant_id"),
                    "projectId":row.get::<String,_>("project_id"),
                    "name":row.get::<String,_>("name"),
                    "databaseAlias":row.get::<String,_>("database_alias"),
                    "database":row.get::<String,_>("database_name"),
                    "status":row.get::<String,_>("status"),
                    "description":row.get::<String,_>("description"),
                    "domain":row.get::<String,_>("domain"),
                    "timezone":row.get::<String,_>("timezone"),
                    "profile":row.get::<sqlx::types::Json<serde_json::Value>,_>("profile").0,
                    "provisioned":row.get::<bool,_>("provisioned"),
                })
            })
            .collect())
    }

    pub async fn create_runtime_tenant(
        &self,
        data_dir: &str,
        project_id: &str,
        requested_tenant_id: Option<&str>,
        name: &str,
    ) -> Result<(TenantRoute, serde_json::Value), DatabaseError> {
        let project_id = project_id.trim();
        let name = name.trim();
        if project_id.is_empty() || name.is_empty() || name.len() > 120 {
            return Err(protocol("projectId and tenant name are required"));
        }
        let tenant_id = requested_tenant_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        if tenant_id.len() > 120
            || !tenant_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(protocol("tenantId is invalid"));
        }
        let mut lookup = self.begin_control_transaction(true).await?;
        let mode = sqlx::query_scalar::<_, String>(
            "SELECT database_mode FROM gonvex_runtime_projects WHERE id=$1 AND status='active'",
        )
        .bind(project_id)
        .fetch_optional(&mut **lookup.transaction())
        .await?
        .ok_or_else(|| DatabaseError::ProjectNotFound(project_id.to_owned()))?;
        if mode != "multiTenant" {
            return Err(protocol(
                "additional tenant shards require a multiTenant project",
            ));
        }
        if let Some(row) = sqlx::query(
            "SELECT database_url,database_name,database_alias FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL",
        )
        .bind(project_id)
        .bind(&tenant_id)
        .fetch_optional(&mut **lookup.transaction())
        .await?
        {
            let route = TenantRoute {
                project_id: project_id.to_owned(),
                tenant_id: tenant_id.clone(),
                database_url: row.get("database_url"),
            };
            let value = serde_json::json!({
                "id":tenant_id,"projectId":project_id,"name":name,
                "database":row.get::<String,_>("database_name"),
                "databaseAlias":row.get::<String,_>("database_alias"),
                "status":"active",
            });
            lookup.commit().await?;
            return Ok((route, value));
        }
        lookup.commit().await?;

        let data = DataDir::parse(data_dir);
        let database_name = relative_name(&data, &data.tenant_path(project_id, &tenant_id)?);
        let database_alias = format!(
            "tenant_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let database_url = self
            .create_tenant_database(data_dir, project_id, &tenant_id)
            .await?;
        let mut transaction = self.begin_control_transaction(false).await?;
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_tenants
               (relationship_id,project_id,tenant_id,name,database_alias,database_name,
                database_url,status,description,provisioned,runtime_created)
               VALUES($1,$2,$1,$3,$4,$5,$6,'active','Runtime-created tenant shard.',0,1)
               ON CONFLICT(project_id,tenant_id) DO NOTHING"#,
        )
        .bind(&tenant_id)
        .bind(project_id)
        .bind(name)
        .bind(&database_alias)
        .bind(&database_name)
        .bind(&database_url)
        .execute(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;
        Ok((
            TenantRoute {
                project_id: project_id.to_owned(),
                tenant_id: tenant_id.clone(),
                database_url,
            },
            serde_json::json!({
                "id":tenant_id,"projectId":project_id,"name":name,
                "database":database_name,"databaseAlias":database_alias,"status":"active",
            }),
        ))
    }

    pub async fn mark_runtime_tenant_provisioned(
        &self,
        project_id: &str,
        tenant_id: &str,
    ) -> Result<(), DatabaseError> {
        let _admission = self.pools.admit(&self.database_url).await?;
        sqlx::query(&format!(
            "UPDATE gonvex_runtime_tenants SET provisioned=1,updated_at={} WHERE project_id=$1 AND tenant_id=$2",
            feed::NOW_MS
        ))
        .bind(project_id)
        .bind(tenant_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Deletes a runtime-created tenant shard and its file. Only a file inside
    /// `{data}/projects/{project}` is removed.
    pub async fn delete_runtime_tenant(
        &self,
        data_dir: &str,
        project_id: &str,
        tenant_id: &str,
    ) -> Result<(), DatabaseError> {
        let project_dir = DataDir::parse(data_dir).project_dir(project_id)?;
        let mut transaction = self.begin_control_transaction(false).await?;
        let row = sqlx::query(
            r#"SELECT tenant.runtime_created,tenant.database_url,project.database_mode
               FROM gonvex_runtime_tenants tenant
               JOIN gonvex_runtime_projects project ON project.id=tenant.project_id
               WHERE tenant.project_id=$1 AND tenant.tenant_id=$2
                 AND tenant.deleted_at IS NULL AND tenant.status NOT IN('deleted','disabled')"#,
        )
        .bind(project_id)
        .bind(tenant_id)
        .fetch_optional(&mut **transaction.transaction())
        .await?
        .ok_or_else(|| DatabaseError::TenantNotFound {
            project: project_id.to_owned(),
            tenant: tenant_id.to_owned(),
        })?;
        if !row.get::<bool, _>("runtime_created")
            || row.get::<String, _>("database_mode") != "multiTenant"
        {
            return Err(protocol(
                "only runtime-created multi-tenant shards may be deleted through this operation",
            ));
        }
        let database_url = row.get::<String, _>("database_url");
        let file = owned_database_file(&project_dir, &database_url)?;
        sqlx::query(&format!(
            "UPDATE gonvex_runtime_tenants SET status='deleting',updated_at={} WHERE project_id=$1 AND tenant_id=$2",
            feed::NOW_MS
        ))
        .bind(project_id)
        .bind(tenant_id)
        .execute(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;

        self.pools.close_database(&database_url).await;
        remove_database_file(&file)?;

        let mut finish = self.begin_control_transaction(false).await?;
        sqlx::query(
            "DELETE FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2 AND status='deleting'",
        )
        .bind(project_id)
        .bind(tenant_id)
        .execute(&mut **finish.transaction())
        .await?;
        finish.commit().await?;
        Ok(())
    }

    pub fn provision_tenant_database(
        self,
        route: TenantRoute,
        migrations: Vec<SqlMigration>,
    ) -> Pin<Box<dyn Future<Output = Result<(), DatabaseError>> + Send + 'static>> {
        Box::pin(async move {
            let database_url = route.database_url.trim().to_owned();
            let pool = self.pools.pool(&database_url).await?;
            let _admission = self.pools.admit(&database_url).await?;
            let _schema = self.pools.lock(&database_url, SCHEMA_LOCK).await?;
            let mut connection = pool.acquire().await?;
            install_tenant_identity(&mut connection).await?;
            let notices =
                apply_migrations_locked(&mut connection, &migrations, MigrationScope::Tenant)
                    .await?
                    .1;
            install_change_feed_locked(&mut connection).await?;
            drop(connection);
            publish(&self.pools, &database_url, notices).await?;
            Ok(())
        })
    }

    pub fn apply_control_migrations(
        self,
        migrations: Vec<SqlMigration>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, DatabaseError>> + Send + 'static>> {
        Box::pin(async move {
            let _admission = self.pools.admit(&self.database_url).await?;
            apply_migrations(
                &self.pools,
                &self.database_url,
                migrations,
                MigrationScope::ControlPlane,
            )
            .await
        })
    }
}

fn relative_name(data: &DataDir, path: &Path) -> String {
    data.relative(path)
        .unwrap_or_else(|| path.to_path_buf())
        .display()
        .to_string()
}

/// The database file behind `url`, refused unless it lies inside `project_dir`.
fn owned_database_file(project_dir: &Path, url: &str) -> Result<std::path::PathBuf, DatabaseError> {
    let file = DataDir::new(database_path(url)).root().to_path_buf();
    if file.starts_with(project_dir) && file != project_dir {
        Ok(file)
    } else {
        Err(protocol(
            "refusing to delete a database not owned by the Gonvex runtime",
        ))
    }
}

fn remove_database_file(file: &Path) -> Result<(), DatabaseError> {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut name = file.as_os_str().to_owned();
        name.push(suffix);
        match std::fs::remove_file(&name) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(DatabaseError::Io(error.to_string())),
        }
    }
    Ok(())
}

async fn publish(
    pools: &PoolRegistry,
    database_url: &str,
    notices: Vec<CommitNotice>,
) -> Result<(), DatabaseError> {
    for notice in notices {
        pools
            .notify(database_url, feed::CHANGE_FEED_CHANNEL, &notice.payload())
            .await?;
    }
    Ok(())
}

/// Applies `migrations` of `scope` on the database's writer under the schema
/// lock and returns the names applied by this call.
pub(crate) fn apply_migrations<'a>(
    pools: &'a PoolRegistry,
    database_url: &'a str,
    migrations: Vec<SqlMigration>,
    scope: MigrationScope,
) -> BoxFuture<'a, Result<Vec<String>, DatabaseError>> {
    Box::pin(async move {
        let pool = pools.pool(database_url).await?;
        let _schema = pools.lock(database_url, SCHEMA_LOCK).await?;
        let mut connection = pool.acquire().await?;
        let (applied, notices) =
            apply_migrations_locked(&mut connection, &migrations, scope).await?;
        drop(connection);
        if !applied.is_empty() {
            crate::rows::clear_result_column_kinds();
        }
        publish(pools, database_url, notices).await?;
        Ok(applied)
    })
}

type AppliedMigrations = (Vec<String>, Vec<CommitNotice>);

fn apply_migrations_locked<'a>(
    connection: &'a mut SqliteConnection,
    migrations: &'a [SqlMigration],
    scope: MigrationScope,
) -> BoxFuture<'a, Result<AppliedMigrations, DatabaseError>> {
    Box::pin(async move {
        if scope == MigrationScope::Tenant {
            super::reference_integrity::install(connection).await?;
        }
        crate::execute_script(
            &mut *connection,
            &format!(
                r#"CREATE TABLE IF NOT EXISTS gonvex_migrations (
             name TEXT PRIMARY KEY,
             checksum TEXT NOT NULL,
             applied_at INTEGER NOT NULL DEFAULT ({}),
             duration_ms INTEGER NOT NULL
           )"#,
                feed::NOW_MS
            ),
        )
        .await?;
        let mut applied = Vec::new();
        let mut notices = Vec::new();
        for migration in migrations
            .iter()
            .filter(|migration| migration.scope == scope)
        {
            let recorded = sqlx::query_scalar::<_, String>(
                "SELECT checksum FROM gonvex_migrations WHERE name=$1",
            )
            .bind(&migration.name)
            .fetch_optional(&mut *connection)
            .await?;
            if let Some(recorded) = recorded {
                if recorded != migration.checksum {
                    return Err(protocol(format!(
                        "migration {} checksum mismatch: database has {}, artifact has {}",
                        migration.name, recorded, migration.checksum
                    )));
                }
                continue;
            }
            let started = Instant::now();
            let provenance = Provenance {
                command_id: Some(format!("migration:{}", migration.name)),
                channel: Some("system".to_owned()),
                ..Provenance::default()
            };
            if migration.no_transaction {
                // `-- gonvex:no-transaction`: each statement commits on its own.
                crate::execute_script(&mut *connection, &migration.sql).await?;
            }
            let mut transaction = connection.begin().await?;
            let finished = async {
                if !migration.no_transaction {
                    crate::execute_script(&mut transaction, &migration.sql).await?;
                }
                if scope == MigrationScope::Tenant {
                    super::reference_integrity::after_migration(&mut transaction).await?;
                }
                let notice = feed::finalize(&mut transaction, &provenance).await?;
                sqlx::query(
                    "INSERT INTO gonvex_migrations(name,checksum,duration_ms) VALUES($1,$2,$3)",
                )
                .bind(&migration.name)
                .bind(&migration.checksum)
                .bind(started.elapsed().as_millis() as i64)
                .execute(&mut *transaction)
                .await?;
                Ok::<_, DatabaseError>(notice)
            }
            .await;
            match finished {
                Ok(notice) => {
                    transaction.commit().await?;
                    notices.extend(notice);
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            }
            applied.push(migration.name.clone());
        }
        Ok((applied, notices))
    })
}

fn install_tenant_identity(
    connection: &mut SqliteConnection,
) -> BoxFuture<'_, Result<(), DatabaseError>> {
    Box::pin(async move {
        let mut transaction = connection.begin().await?;
        if column_exists(&mut transaction, "members", "user_id").await? {
            return Err(protocol(
                "identity-v2 migration required: tenant members still use user_id",
            ));
        }
        crate::execute_script(&mut transaction, TENANT_IDENTITY_SQL).await?;
        transaction.commit().await?;
        Ok(())
    })
}

/// Installs sync infrastructure and change capture on every application
/// table. Must run under [`SCHEMA_LOCK`].
pub(crate) fn install_change_feed_locked(
    connection: &mut SqliteConnection,
) -> BoxFuture<'_, Result<(), DatabaseError>> {
    Box::pin(async move {
        // Keep DDL and its receipt atomic: a failed install must never become a cache hit.
        let mut tx = connection.begin().await?;
        sqlx::query(
        "CREATE TABLE IF NOT EXISTS _gonvex_sync_bootstrap (name TEXT PRIMARY KEY, digest TEXT NOT NULL)",
    )
    .execute(&mut *tx)
    .await?;
        if !bootstrap_matches(&mut tx, "infrastructure", SYNC_INFRASTRUCTURE_SQL).await?
            || !infrastructure_present(&mut tx).await?
        {
            crate::execute_script(&mut tx, SYNC_INFRASTRUCTURE_SQL).await?;
            record_bootstrap(&mut tx, "infrastructure", SYNC_INFRASTRUCTURE_SQL).await?;
        }
        // One read each for every table's columns, every recorded receipt and
        // every installed trigger. Checking per table issued roughly
        // 4 statements x tables x tenants on every boot (51k statements for
        // 26 Whagons tenants); a warm boot now reads three result sets.
        let tables = application_tables(&mut tx).await?;
        let mut columns_by_table = all_table_columns(&mut tx).await?;
        let receipts: std::collections::HashMap<String, String> = json_rows::<(String, String)>(
            &mut tx,
            "SELECT json_group_array(json_array(name, digest)) FROM _gonvex_sync_bootstrap",
        )
        .await?
        .into_iter()
        .collect();
        let triggers: std::collections::HashMap<(String, String), String> =
            json_rows::<(String, String, String)>(
                &mut tx,
                "SELECT json_group_array(json_array(tbl_name, name, sql)) FROM sqlite_schema
                 WHERE type='trigger' AND sql IS NOT NULL",
            )
            .await?
            .into_iter()
            .map(|(table, name, sql)| ((table, name), sql))
            .collect();
        for table in tables {
            let columns = columns_by_table.remove(&table).unwrap_or_default();
            let key = columns
                .iter()
                .find(|column| column.primary_key)
                .map(|column| column.name.clone())
                .ok_or_else(|| {
                    protocol(format!("change-feed table {table:?} has no primary key"))
                })?;
            let script = feed::capture_triggers_sql(&table, &key, &columns);
            let receipt = format!("table:{table}");
            let receipt_matches = receipts.get(&receipt).map(String::as_str)
                == Some(bootstrap_digest(&script).as_str());
            if !receipt_matches || !triggers_match(&triggers, &table, &script) {
                crate::execute_script(&mut tx, &script).await?;
                record_bootstrap(&mut tx, &receipt, &script).await?;
            }
        }
        tx.commit().await?;
        // The writer's capture hook must know the tables it now captures.
        crate::capture::refresh(connection).await?;
        Ok(())
    })
}

/// Ordinary application tables: not framework (`_gonvex*`, `gonvex_*`),
/// SQLite-internal, virtual (FTS5) tables or their shadow tables
/// (`<vt>_data/_idx/_content/_docsize/_config`). Applications keep FTS in sync
/// with their own triggers; the change feed captures only base tables.
pub(crate) async fn application_tables(
    connection: &mut SqliteConnection,
) -> Result<Vec<String>, DatabaseError> {
    // A correlated scan of `sqlite_schema` per listed table cost ~70 ms on
    // the Whagons schema (396 tables); the excluded names are collected once
    // instead (<1 ms).
    json_rows(
        connection,
        r#"WITH virtual(name) AS MATERIALIZED (
             SELECT name FROM sqlite_schema
             WHERE type='table' AND sql LIKE 'CREATE VIRTUAL TABLE%'),
           excluded(name) AS MATERIALIZED (
             SELECT name FROM virtual
             UNION ALL SELECT name||'_data' FROM virtual
             UNION ALL SELECT name||'_idx' FROM virtual
             UNION ALL SELECT name||'_content' FROM virtual
             UNION ALL SELECT name||'_docsize' FROM virtual
             UNION ALL SELECT name||'_config' FROM virtual)
           SELECT json_group_array(name ORDER BY name) FROM pragma_table_list
           WHERE schema='main' AND type='table'
             AND name NOT LIKE '\_gonvex%' ESCAPE '\'
             AND name NOT LIKE 'gonvex\_%' ESCAPE '\'
             AND name NOT LIKE 'sqlite\_%' ESCAPE '\'
             AND name NOT IN excluded"#,
    )
    .await
}

async fn infrastructure_present(connection: &mut SqliteConnection) -> Result<bool, DatabaseError> {
    const REQUIRED: &[(&str, &str)] = &[
        ("table", "_gonvex_sync_clock"),
        ("table", "_gonvex_sync_changes"),
        ("table", "_gonvex_sync_transactions"),
        ("table", "_gonvex_action_outbox"),
        ("index", "gonvex_sync_changes_revision"),
        ("index", "gonvex_sync_changes_table"),
        ("index", "gonvex_sync_changes_pending"),
        ("index", "gonvex_sync_changes_created_at"),
        ("index", "gonvex_sync_changes_command"),
        ("index", "_gonvex_action_outbox_pending"),
        ("index", "_gonvex_action_outbox_pending_path"),
        ("index", "_gonvex_action_outbox_processing"),
    ];
    for (kind, name) in REQUIRED {
        let present: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type=$1 AND name=$2)",
        )
        .bind(kind)
        .bind(name)
        .fetch_one(&mut *connection)
        .await?;
        if !present {
            return Ok(false);
        }
    }
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _gonvex_sync_clock WHERE singleton=1)")
            .fetch_one(connection)
            .await?,
    )
}

/// `CREATE TRIGGER … END` statements in an install script, as SQLite stores
/// them in `sqlite_schema.sql` (without the terminating semicolon).
fn trigger_statements(script: &str) -> Vec<&str> {
    const CREATE: &str = "CREATE TRIGGER ";
    const END: &str = "\nEND;";
    let mut found = Vec::new();
    let mut rest = script;
    while let Some(start) = rest.find(CREATE) {
        let candidate = &rest[start..];
        let Some(end) = candidate.find(END) else {
            break;
        };
        found.push(&candidate[..end + END.len() - 1]);
        rest = &candidate[end + END.len()..];
    }
    found
}

/// Columns of every ordinary table, in declaration order, from one query.
async fn all_table_columns(
    connection: &mut SqliteConnection,
) -> Result<std::collections::HashMap<String, Vec<crate::rows::ColumnInfo>>, DatabaseError> {
    let rows = json_rows::<(String, String, String, i64, i64)>(
        connection,
        r#"SELECT json_group_array(json_array(m.name, p.name, p.type, p."notnull", p.pk)
                                   ORDER BY m.name, p.cid)
           FROM sqlite_schema AS m JOIN pragma_table_info(m.name) AS p
           WHERE m.type = 'table'"#,
    )
    .await?;
    let mut tables: std::collections::HashMap<String, Vec<crate::rows::ColumnInfo>> =
        std::collections::HashMap::new();
    for (table, name, declared, not_null, primary_key) in rows {
        tables
            .entry(table)
            .or_default()
            .push(crate::rows::ColumnInfo {
                name,
                declared,
                not_null: not_null != 0,
                primary_key: primary_key != 0,
            });
    }
    Ok(tables)
}

/// Rows of a catalog query that aggregates them into one JSON array
/// (`json_group_array`). Each result row crosses sqlx's worker thread on its
/// own, so a 4,000-row catalog read cost far more in hand-offs than in SQL.
async fn json_rows<T: serde::de::DeserializeOwned>(
    connection: &mut SqliteConnection,
    sql: &str,
) -> Result<Vec<T>, DatabaseError> {
    let encoded: Option<String> = sqlx::query_scalar(sql).fetch_one(connection).await?;
    serde_json::from_str(encoded.as_deref().unwrap_or("[]"))
        .map_err(|error| DatabaseError::Decode(format!("schema catalog: {error}")))
}

/// The in-memory form of `installed_triggers_match` for a prefetched
/// `(table, trigger name) -> sql` map.
fn triggers_match(
    installed: &std::collections::HashMap<(String, String), String>,
    table: &str,
    script: &str,
) -> bool {
    let expected = trigger_statements(script);
    let names = feed::capture_trigger_names(table);
    expected.len() == names.len()
        && names.iter().zip(expected).all(|(name, statement)| {
            installed
                .get(&(table.to_owned(), name.clone()))
                .map(String::as_str)
                == Some(statement)
        })
}

fn bootstrap_digest(source: &str) -> String {
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

async fn bootstrap_matches(
    connection: &mut SqliteConnection,
    name: &str,
    source: &str,
) -> Result<bool, DatabaseError> {
    let recorded: Option<String> =
        sqlx::query_scalar("SELECT digest FROM _gonvex_sync_bootstrap WHERE name=$1")
            .bind(name)
            .fetch_optional(connection)
            .await?;
    Ok(recorded.as_deref() == Some(bootstrap_digest(source).as_str()))
}

async fn record_bootstrap(
    connection: &mut SqliteConnection,
    name: &str,
    source: &str,
) -> Result<(), DatabaseError> {
    sqlx::query("INSERT INTO _gonvex_sync_bootstrap(name,digest) VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET digest=excluded.digest")
        .bind(name).bind(bootstrap_digest(source)).execute(connection).await?;
    Ok(())
}

/// Tenant-local Member identity and the Control Plane membership outbox. A
/// deleted Member projects as `revoked` one revision past its last row.
const TENANT_IDENTITY_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS members (
  id TEXT PRIMARY KEY, account_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'active',
  display_name TEXT NOT NULL DEFAULT '', avatar_url TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT 'member', permissions JSON NOT NULL DEFAULT '{}',
  membership_revision INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS members_by_role ON members(role);
CREATE INDEX IF NOT EXISTS members_by_status ON members(status,id);
CREATE TABLE IF NOT EXISTS _gonvex_control_plane_membership_outbox (
  account_id TEXT PRIMARY KEY, member_id TEXT NOT NULL, status TEXT NOT NULL,
  membership_revision INTEGER NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership_insert;
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership_update;
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership_delete;
CREATE TRIGGER gonvex_queue_control_plane_membership_insert AFTER INSERT ON members BEGIN
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(NEW.account_id,NEW.id,NEW.status,NEW.membership_revision,CAST(unixepoch('subsec') * 1000 AS INTEGER))
  ON CONFLICT(account_id) DO UPDATE SET member_id=excluded.member_id,status=excluded.status,
    membership_revision=excluded.membership_revision,updated_at=excluded.updated_at
  WHERE excluded.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
END;
CREATE TRIGGER gonvex_queue_control_plane_membership_update AFTER UPDATE ON members BEGIN
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(NEW.account_id,NEW.id,NEW.status,NEW.membership_revision,CAST(unixepoch('subsec') * 1000 AS INTEGER))
  ON CONFLICT(account_id) DO UPDATE SET member_id=excluded.member_id,status=excluded.status,
    membership_revision=excluded.membership_revision,updated_at=excluded.updated_at
  WHERE excluded.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
END;
CREATE TRIGGER gonvex_queue_control_plane_membership_delete AFTER DELETE ON members BEGIN
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(OLD.account_id,OLD.id,'revoked',OLD.membership_revision+1,CAST(unixepoch('subsec') * 1000 AS INTEGER))
  ON CONFLICT(account_id) DO UPDATE SET member_id=excluded.member_id,status=excluded.status,
    membership_revision=excluded.membership_revision,updated_at=excluded.updated_at
  WHERE excluded.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
END;
"#;

#[cfg(test)]
#[path = "provision_upgrade_tests.rs"]
mod sync_bootstrap_upgrade_tests;

#[cfg(test)]
#[path = "provision_foreign_writer_tests.rs"]
mod sync_bootstrap_foreign_writer_tests;
