use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::Row;
use url::Url;

use super::{ControlPlane, DatabaseError, TenantRoute};

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

impl ControlPlane {
    pub(crate) async fn prepare_identity_schema_upgrade(&self) -> Result<(), DatabaseError> {
        let _admission = self.pools.admit().await?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('gonvex:identity-schema-upgrade'))")
            .execute(&mut *transaction)
            .await?;

        let mut legacy_tables = Vec::new();
        for table in [
            "gonvex_auth_codes",
            "gonvex_auth_sessions",
            "gonvex_auth_refresh_tokens",
        ] {
            let has_legacy_user_id: bool = sqlx::query_scalar(
                r#"SELECT EXISTS(SELECT 1 FROM information_schema.columns
                   WHERE table_schema=current_schema() AND table_name=$1 AND column_name='user_id')"#,
            )
            .bind(table)
            .fetch_one(&mut *transaction)
            .await?;
            if has_legacy_user_id {
                legacy_tables.push(table);
            }
        }
        for table in ["gonvex_auth_memberships", "gonvex_auth_users"] {
            let exists: bool = sqlx::query_scalar(
                "SELECT to_regclass(format('%I.%I',current_schema(),$1)) IS NOT NULL",
            )
            .bind(table)
            .fetch_one(&mut *transaction)
            .await?;
            if exists {
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
                return Err(sqlx::Error::Protocol(format!(
                    "legacy identity table {table} contains data; run `gonvex-admin migrate identity-v2` before starting this runtime"
                ))
                .into());
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
        let _admission = self.pools.admit().await?;
        execute_script_pool(&self.pool, include_str!("control_schema.sql")).await
    }

    pub async fn create_database(
        &self,
        base_url: &str,
        database_name: &str,
    ) -> Result<String, DatabaseError> {
        if !database_name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            || database_name.is_empty()
            || database_name.len() > 63
        {
            return Err(sqlx::Error::Protocol("invalid generated database name".to_owned()).into());
        }
        let mut maintenance_url = Url::parse(base_url)
            .map_err(|error| sqlx::Error::Protocol(format!("invalid database URL: {error}")))?;
        maintenance_url.set_path("/postgres");
        let pool = self.pools.pool(maintenance_url.as_str()).await?;
        let _admission = self.pools.admit().await?;
        let owner: String = sqlx::query_scalar("SELECT current_user")
            .fetch_one(&pool)
            .await?;
        if !owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(sqlx::Error::Protocol("invalid database owner".to_owned()).into());
        }
        let statement = format!("CREATE DATABASE \"{database_name}\" OWNER \"{owner}\"");
        if let Err(error) = sqlx::query(&statement).execute(&pool).await {
            if error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref()
                != Some("42P04")
            {
                return Err(error.into());
            }
        }
        let mut result = Url::parse(base_url)
            .map_err(|error| sqlx::Error::Protocol(format!("invalid database URL: {error}")))?;
        result.set_path(&format!("/{database_name}"));
        Ok(result.to_string())
    }

    pub async fn create_runtime_project(
        &self,
        base_url: &str,
        name: &str,
        database_mode: &str,
        owner_email: &str,
    ) -> Result<serde_json::Value, DatabaseError> {
        if !matches!(database_mode, "single" | "multiTenant") {
            return Err(sqlx::Error::Protocol(
                "databaseMode must be single or multiTenant".to_owned(),
            )
            .into());
        }
        let name = name.trim();
        if name.is_empty() || name.len() > 120 {
            return Err(sqlx::Error::Protocol("project name is required".to_owned()).into());
        }
        let project_id = uuid::Uuid::new_v4().to_string();
        let database_name = format!("gonvex_{}", uuid::Uuid::new_v4().simple());
        let database_url = self.create_database(base_url, &database_name).await?;
        let project_key = project_key(&project_id);
        let mut transaction = self.begin_control_transaction(false).await?;
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_projects
               (id,name,environment,database_name,database_mode,database_url,storage_bucket,
                status,description,project_key,provisioned,runtime_created,owner_email)
               VALUES($1,$2,'development',$3,$4,$5,$6,'active','Runtime-created project database.',
                      $7,TRUE,TRUE,$8)"#,
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
                   VALUES($1,$1,$1,$2,'default',$3,$4,'active','Single-database tenant.',TRUE,TRUE)"#,
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

    pub async fn delete_runtime_project(
        &self,
        base_url: &str,
        project_id: &str,
    ) -> Result<(), DatabaseError> {
        let mut transaction = self.begin_control_transaction(false).await?;
        let project = sqlx::query(
            r#"SELECT runtime_created,database_name,database_url FROM gonvex_runtime_projects
               WHERE id=$1 AND status NOT IN('deleted','disabled') FOR UPDATE"#,
        )
        .bind(project_id)
        .fetch_optional(&mut **transaction.transaction())
        .await?
        .ok_or_else(|| DatabaseError::ProjectNotFound(project_id.to_owned()))?;
        if !project.get::<bool, _>("runtime_created") {
            return Err(sqlx::Error::Protocol(
                "only runtime-created projects may be deleted through this operation".to_owned(),
            )
            .into());
        }
        let mut databases = sqlx::query_as::<_, (String, String)>(
            r#"SELECT database_name,database_url FROM gonvex_runtime_tenants
               WHERE project_id=$1 AND runtime_created=TRUE AND database_name<>''"#,
        )
        .bind(project_id)
        .fetch_all(&mut **transaction.transaction())
        .await?;
        let project_database = (
            project.get::<String, _>("database_name"),
            project.get::<String, _>("database_url"),
        );
        if !project_database.0.is_empty() {
            databases.push(project_database);
        }
        databases.sort();
        databases.dedup();
        for (name, _) in &databases {
            if !name.starts_with("gonvex_")
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            {
                return Err(sqlx::Error::Protocol(
                    "refusing to delete a database not owned by the Gonvex runtime".to_owned(),
                )
                .into());
            }
        }
        sqlx::query(
            "UPDATE gonvex_runtime_projects SET status='deleting',updated_at=now() WHERE id=$1",
        )
        .bind(project_id)
        .execute(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;

        let mut maintenance_url = Url::parse(base_url)
            .map_err(|error| sqlx::Error::Protocol(format!("invalid database URL: {error}")))?;
        maintenance_url.set_path("/postgres");
        let maintenance = self.pools.pool(maintenance_url.as_str()).await?;
        for (name, url) in &databases {
            self.pools.close_database(url).await;
            sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()")
                .bind(name)
                .execute(&maintenance)
                .await?;
            let statement = format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)");
            sqlx::query(&statement).execute(&maintenance).await?;
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
        base_url: &str,
        project_id: &str,
        requested_tenant_id: Option<&str>,
        name: &str,
    ) -> Result<(TenantRoute, serde_json::Value), DatabaseError> {
        let project_id = project_id.trim();
        let name = name.trim();
        if project_id.is_empty() || name.is_empty() || name.len() > 120 {
            return Err(
                sqlx::Error::Protocol("projectId and tenant name are required".to_owned()).into(),
            );
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
            return Err(sqlx::Error::Protocol("tenantId is invalid".to_owned()).into());
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
            return Err(sqlx::Error::Protocol(
                "additional tenant shards require a multiTenant project".to_owned(),
            )
            .into());
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

        let database_name = format!("gonvex_{}", uuid::Uuid::new_v4().simple());
        let database_alias = format!(
            "tenant_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let database_url = self.create_database(base_url, &database_name).await?;
        let mut transaction = self.begin_control_transaction(false).await?;
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_tenants
               (relationship_id,project_id,tenant_id,name,database_alias,database_name,
                database_url,status,description,provisioned,runtime_created)
               VALUES($1,$2,$1,$3,$4,$5,$6,'active','Runtime-created tenant shard.',FALSE,TRUE)
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
        let _admission = self.pools.admit().await?;
        sqlx::query(
            "UPDATE gonvex_runtime_tenants SET provisioned=TRUE,updated_at=now() WHERE project_id=$1 AND tenant_id=$2",
        )
        .bind(project_id)
        .bind(tenant_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_runtime_tenant(
        &self,
        base_url: &str,
        project_id: &str,
        tenant_id: &str,
    ) -> Result<(), DatabaseError> {
        let mut transaction = self.begin_control_transaction(false).await?;
        let row = sqlx::query(
            r#"SELECT tenant.runtime_created,tenant.database_name,tenant.database_url,
                      project.database_mode
               FROM gonvex_runtime_tenants tenant
               JOIN gonvex_runtime_projects project ON project.id=tenant.project_id
               WHERE tenant.project_id=$1 AND tenant.tenant_id=$2
                 AND tenant.deleted_at IS NULL AND tenant.status NOT IN('deleted','disabled')
               FOR UPDATE"#,
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
            return Err(sqlx::Error::Protocol(
                "only runtime-created multi-tenant shards may be deleted through this operation"
                    .to_owned(),
            )
            .into());
        }
        let database_name = row.get::<String, _>("database_name");
        let database_url = row.get::<String, _>("database_url");
        if !database_name.starts_with("gonvex_")
            || !database_name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(sqlx::Error::Protocol(
                "refusing to delete a database not owned by the Gonvex runtime".to_owned(),
            )
            .into());
        }
        sqlx::query(
            "UPDATE gonvex_runtime_tenants SET status='deleting',updated_at=now() WHERE project_id=$1 AND tenant_id=$2",
        )
        .bind(project_id)
        .bind(tenant_id)
        .execute(&mut **transaction.transaction())
        .await?;
        transaction.commit().await?;

        let mut maintenance_url = Url::parse(base_url)
            .map_err(|error| sqlx::Error::Protocol(format!("invalid database URL: {error}")))?;
        maintenance_url.set_path("/postgres");
        let maintenance = self.pools.pool(maintenance_url.as_str()).await?;
        self.pools.close_database(&database_url).await;
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()",
        )
        .bind(&database_name)
        .execute(&maintenance)
        .await?;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{database_name}\" WITH (FORCE)"
        ))
        .execute(&maintenance)
        .await?;

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
            let pool = self.pools.pool(&route.database_url).await?;
            let _admission = self.pools.admit().await?;
            execute_script_pool(&pool, TENANT_IDENTITY_SQL).await?;
            apply_migrations(pool.clone(), migrations, MigrationScope::Tenant).await?;
            install_change_feed(pool).await?;
            Ok(())
        })
    }

    pub fn apply_control_migrations(
        self,
        migrations: Vec<SqlMigration>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, DatabaseError>> + Send + 'static>> {
        Box::pin(async move {
            let _admission = self.pools.admit().await?;
            apply_migrations(self.pool.clone(), migrations, MigrationScope::ControlPlane).await
        })
    }
}

async fn apply_migrations(
    pool: sqlx::PgPool,
    migrations: Vec<SqlMigration>,
    scope: MigrationScope,
) -> Result<Vec<String>, DatabaseError> {
    let mut connection = pool.acquire().await?;
    sqlx::query(
        "SELECT pg_advisory_lock(hashtext('gonvex_sql_migrations:' || current_database()))",
    )
    .execute(&mut *connection)
    .await?;
    let result = apply_migrations_locked(&mut connection, &migrations, scope).await;
    let _ = sqlx::query(
        "SELECT pg_advisory_unlock(hashtext('gonvex_sql_migrations:' || current_database()))",
    )
    .execute(&mut *connection)
    .await;
    result
}

async fn apply_migrations_locked(
    connection: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    migrations: &[SqlMigration],
    scope: MigrationScope,
) -> Result<Vec<String>, DatabaseError> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS gonvex_migrations (
             name text PRIMARY KEY,
             checksum text NOT NULL,
             applied_at timestamptz NOT NULL DEFAULT now(),
             duration_ms bigint NOT NULL
           )"#,
    )
    .execute(&mut **connection)
    .await?;
    let mut applied = Vec::new();
    for migration in migrations
        .iter()
        .filter(|migration| migration.scope == scope)
    {
        let recorded =
            sqlx::query_scalar::<_, String>("SELECT checksum FROM gonvex_migrations WHERE name=$1")
                .bind(&migration.name)
                .fetch_optional(&mut **connection)
                .await?;
        if let Some(recorded) = recorded {
            if recorded != migration.checksum {
                return Err(sqlx::Error::Protocol(format!(
                    "migration {} checksum mismatch: database has {}, artifact has {}",
                    migration.name, recorded, migration.checksum
                ))
                .into());
            }
            continue;
        }
        let started = Instant::now();
        if migration.no_transaction {
            execute_script_connection(connection, &migration.sql).await?;
            sqlx::query(
                "INSERT INTO gonvex_migrations(name,checksum,duration_ms) VALUES($1,$2,$3)",
            )
            .bind(&migration.name)
            .bind(&migration.checksum)
            .bind(started.elapsed().as_millis() as i64)
            .execute(&mut **connection)
            .await?;
        } else {
            sqlx::query("BEGIN").execute(&mut **connection).await?;
            let applied_result = async {
                execute_script_connection(connection, &migration.sql).await?;
                sqlx::query(
                    "INSERT INTO gonvex_migrations(name,checksum,duration_ms) VALUES($1,$2,$3)",
                )
                .bind(&migration.name)
                .bind(&migration.checksum)
                .bind(started.elapsed().as_millis() as i64)
                .execute(&mut **connection)
                .await?;
                Ok::<(), DatabaseError>(())
            }
            .await;
            if let Err(error) = applied_result {
                let _ = sqlx::query("ROLLBACK").execute(&mut **connection).await;
                return Err(error);
            }
            sqlx::query("COMMIT").execute(&mut **connection).await?;
        }
        applied.push(migration.name.clone());
    }
    Ok(applied)
}

async fn install_change_feed(pool: sqlx::PgPool) -> Result<(), DatabaseError> {
    execute_script_pool(&pool, SYNC_INFRASTRUCTURE_SQL).await?;
    let tables = sqlx::query(
        r#"SELECT table_name FROM information_schema.tables
           WHERE table_schema=current_schema() AND table_type='BASE TABLE'
             AND table_name NOT LIKE '\_gonvex%' ESCAPE '\'
             AND table_name NOT LIKE 'gonvex\_%' ESCAPE '\'
           ORDER BY table_name"#,
    )
    .fetch_all(&pool)
    .await?;
    for table in tables {
        let table: String = table.get("table_name");
        let columns = sqlx::query(
            r#"SELECT column_name FROM information_schema.columns
               WHERE table_schema=current_schema() AND table_name=$1 ORDER BY ordinal_position"#,
        )
        .bind(&table)
        .fetch_all(&pool)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("column_name"))
        .collect::<Vec<_>>();
        let key = sqlx::query_scalar::<_, String>(
            r#"SELECT attribute.attname
               FROM pg_index index
               JOIN pg_class relation ON relation.oid=index.indrelid
               JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
               JOIN pg_attribute attribute ON attribute.attrelid=relation.oid
                AND attribute.attnum=ANY(index.indkey)
               WHERE namespace.nspname=current_schema() AND relation.relname=$1
                 AND index.indisprimary ORDER BY attribute.attnum LIMIT 1"#,
        )
        .bind(&table)
        .fetch_optional(&pool)
        .await?
        .ok_or_else(|| {
            sqlx::Error::Protocol(format!("change-feed table {table:?} has no primary key"))
        })?;
        install_table_trigger(&pool, &table, &key, &columns).await?;
    }
    Ok(())
}

async fn install_table_trigger(
    pool: &sqlx::PgPool,
    table: &str,
    key: &str,
    columns: &[String],
) -> Result<(), DatabaseError> {
    for identifier in std::iter::once(table)
        .chain(std::iter::once(key))
        .chain(columns.iter().map(String::as_str))
    {
        if !identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(sqlx::Error::Protocol(format!(
                "invalid change-feed identifier {identifier:?}"
            ))
            .into());
        }
    }
    let artifact = |suffix: &str| {
        let full = format!("gonvex_sync_{table}_{suffix}");
        if full.len() <= 63 {
            full
        } else {
            let hash = Sha256::digest(full.as_bytes());
            let suffix = hash[..6]
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            format!("{}_{suffix}", &full[..50])
        }
    };
    let quote = |identifier: &str| format!("\"{identifier}\"");
    let literal = |value: &str| format!("'{}'", value.replace('\'', "''"));
    let projection = |alias: &str| {
        columns
            .iter()
            .map(|column| format!("{},{}.{}", literal(column), alias, quote(column)))
            .collect::<Vec<_>>()
            .join(",")
    };
    let stage_function = quote(&artifact("stage"));
    let stage_trigger = quote(&artifact("stage_trigger"));
    let finalize_trigger = quote(&artifact("finalize_trigger"));
    let table_ident = quote(table);
    let key_ident = quote(key);
    let statement = format!(
        r#"CREATE OR REPLACE FUNCTION {stage_function}() RETURNS trigger AS $$
DECLARE old_data jsonb; new_data jsonb; changed_columns text[]; row_key text;
BEGIN
  IF TG_OP='INSERT' THEN
    old_data:=NULL; new_data:=jsonb_build_object({new_projection});
    row_key:=NEW.{key_ident}::text; changed_columns:=ARRAY(SELECT jsonb_object_keys(new_data));
  ELSIF TG_OP='DELETE' THEN
    old_data:=jsonb_build_object({old_projection}); new_data:=NULL;
    row_key:=OLD.{key_ident}::text; changed_columns:=ARRAY(SELECT jsonb_object_keys(old_data));
  ELSE
    old_data:=jsonb_build_object({old_projection}); new_data:=jsonb_build_object({new_projection});
    row_key:=NEW.{key_ident}::text;
    changed_columns:=ARRAY(SELECT key FROM jsonb_object_keys(old_data||new_data) changed(key)
                           WHERE old_data->key IS DISTINCT FROM new_data->key ORDER BY key);
  END IF;
  INSERT INTO _gonvex_sync_changes
    (transaction_id,table_name,row_id,operation,old_value,new_value,changed_columns)
  VALUES(txid_current()::bigint,{table_literal},row_key,lower(TG_OP),old_data,new_data,changed_columns);
  RETURN NULL;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS {stage_trigger} ON {table_ident};
DROP TRIGGER IF EXISTS {finalize_trigger} ON {table_ident};
CREATE TRIGGER {stage_trigger} AFTER INSERT OR UPDATE OR DELETE ON {table_ident}
  FOR EACH ROW EXECUTE FUNCTION {stage_function}();
CREATE CONSTRAINT TRIGGER {finalize_trigger} AFTER INSERT OR UPDATE OR DELETE ON {table_ident}
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION gonvex_sync_finalize_transaction();"#,
        old_projection = projection("OLD"),
        new_projection = projection("NEW"),
        table_literal = literal(table),
    );
    execute_script_pool(pool, &statement).await?;
    Ok(())
}

async fn execute_script_pool(pool: &sqlx::PgPool, source: &str) -> Result<(), DatabaseError> {
    for statement in split_statements(source)? {
        sqlx::query(&statement).execute(pool).await?;
    }
    Ok(())
}

async fn execute_script_connection(
    connection: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    source: &str,
) -> Result<(), DatabaseError> {
    for statement in split_statements(source)? {
        sqlx::query(&statement).execute(&mut **connection).await?;
    }
    Ok(())
}

fn split_statements(source: &str) -> Result<Vec<String>, DatabaseError> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut line_comment = false;
    let mut block_comment = 0_u32;
    let mut dollar_tag = String::new();
    let mut chars = source.char_indices().peekable();
    while let Some((index, character)) = chars.next() {
        if line_comment {
            if character == '\n' {
                line_comment = false;
            }
            continue;
        }
        if block_comment > 0 {
            if source[index..].starts_with("/*") {
                block_comment += 1;
                let _ = chars.next();
                continue;
            }
            if source[index..].starts_with("*/") {
                block_comment -= 1;
                let _ = chars.next();
                continue;
            }
            continue;
        }
        if !dollar_tag.is_empty() {
            if source[index..].starts_with(&dollar_tag) {
                for _ in 1..dollar_tag.len() {
                    let _ = chars.next();
                }
                dollar_tag.clear();
            }
            continue;
        }
        if let Some(active_quote) = quote {
            if character == active_quote as char {
                if chars
                    .peek()
                    .is_some_and(|(_, next_character)| *next_character == character)
                {
                    let _ = chars.next();
                    continue;
                }
                quote = None;
            }
            continue;
        }
        if source[index..].starts_with("--") {
            line_comment = true;
            let _ = chars.next();
            continue;
        }
        if source[index..].starts_with("/*") {
            block_comment = 1;
            let _ = chars.next();
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character as u8);
            continue;
        }
        if character == '$' {
            if let Some(relative_end) = source[index + 1..].find('$') {
                let end = index + relative_end + 2;
                let candidate = &source[index..end];
                if candidate
                    .strip_prefix('$')
                    .and_then(|tag| tag.strip_suffix('$'))
                    .is_some_and(|tag| {
                        tag.bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                    })
                {
                    dollar_tag = candidate.to_owned();
                    for _ in 1..dollar_tag.len() {
                        let _ = chars.next();
                    }
                    continue;
                }
            }
        }
        if character == ';' {
            let statement = source[start..=index].trim();
            if !statement.is_empty() {
                statements.push(statement.to_owned());
            }
            start = index + character.len_utf8();
        }
    }
    if quote.is_some() || !dollar_tag.is_empty() || block_comment != 0 {
        return Err(sqlx::Error::Protocol("unterminated SQL quote or comment".to_owned()).into());
    }
    let trailing = source[start..].trim();
    if !trailing.is_empty() {
        statements.push(trailing.to_owned());
    }
    Ok(statements)
}

#[cfg(test)]
mod split_statements_tests {
    use super::split_statements;

    #[test]
    fn handles_multibyte_text_inside_dollar_quoted_statement() {
        let source = "DO $$ BEGIN RAISE NOTICE 'migration café'; END $$;\n".to_owned()
            + "CREATE TABLE migration_text (value text);";

        assert_eq!(
            split_statements(&source).unwrap(),
            vec![
                "DO $$ BEGIN RAISE NOTICE 'migration café'; END $$;".to_owned(),
                "CREATE TABLE migration_text (value text);".to_owned(),
            ]
        );
    }
}

const TENANT_IDENTITY_SQL: &str = r#"
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema=current_schema() AND table_name='members' AND column_name='user_id') THEN
    RAISE EXCEPTION 'identity-v2 migration required: tenant members still use user_id';
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS members (
  id text PRIMARY KEY, account_id text NOT NULL UNIQUE, status text NOT NULL DEFAULT 'active',
  display_name text NOT NULL DEFAULT '', avatar_url text NOT NULL DEFAULT '',
  role text NOT NULL DEFAULT 'member', permissions jsonb NOT NULL DEFAULT '{}'::jsonb,
  membership_revision bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS members_by_role ON members(role);
CREATE INDEX IF NOT EXISTS members_by_status ON members(status,id);
CREATE TABLE IF NOT EXISTS _gonvex_control_plane_membership_outbox (
  account_id text PRIMARY KEY, member_id text NOT NULL, status text NOT NULL,
  membership_revision bigint NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION _gonvex_queue_control_plane_membership() RETURNS trigger AS $$
DECLARE projected_account_id text; projected_member_id text; projected_status text; projected_revision bigint;
BEGIN
  IF TG_OP='DELETE' THEN
    projected_account_id:=OLD.account_id; projected_member_id:=OLD.id;
    projected_status:='revoked'; projected_revision:=OLD.membership_revision+1;
  ELSE
    projected_account_id:=NEW.account_id; projected_member_id:=NEW.id;
    projected_status:=NEW.status; projected_revision:=NEW.membership_revision;
  END IF;
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(projected_account_id,projected_member_id,projected_status,projected_revision,now())
  ON CONFLICT(account_id) DO UPDATE SET member_id=EXCLUDED.member_id,status=EXCLUDED.status,
    membership_revision=EXCLUDED.membership_revision,updated_at=now()
  WHERE EXCLUDED.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership ON members;
CREATE TRIGGER gonvex_queue_control_plane_membership AFTER INSERT OR UPDATE OR DELETE ON members
  FOR EACH ROW EXECUTE FUNCTION _gonvex_queue_control_plane_membership();
"#;

const SYNC_INFRASTRUCTURE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS _gonvex_sync_clock (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), epoch text NOT NULL,
  revision bigint NOT NULL DEFAULT 0, retained_revision bigint NOT NULL DEFAULT 0
);
INSERT INTO _gonvex_sync_clock(singleton,epoch,revision)
VALUES(true,md5(random()::text||clock_timestamp()::text),0) ON CONFLICT(singleton) DO NOTHING;
CREATE TABLE IF NOT EXISTS _gonvex_sync_changes (
  event_id bigserial PRIMARY KEY, transaction_id bigint NOT NULL, revision bigint, ordinal integer,
  command_id text, table_name text NOT NULL, row_id text NOT NULL, operation text NOT NULL,
  old_value jsonb, new_value jsonb, changed_columns text[] NOT NULL DEFAULT ARRAY[]::text[],
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS gonvex_sync_changes_revision ON _gonvex_sync_changes(revision,ordinal)
  WHERE revision IS NOT NULL;
CREATE INDEX IF NOT EXISTS gonvex_sync_changes_created_at ON _gonvex_sync_changes(created_at);
CREATE TABLE IF NOT EXISTS _gonvex_sync_transactions (
  revision bigint PRIMARY KEY, transaction_id bigint NOT NULL UNIQUE,
  root_command_id text NOT NULL, origin_command_id text NOT NULL,
  root_invocation_channel text NOT NULL DEFAULT 'system', invocation_channel text NOT NULL,
  actor_account_id text, actor_member_id text, on_behalf_of_member_id text,
  agent_execution_id text, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE _gonvex_sync_transactions ADD COLUMN IF NOT EXISTS root_invocation_channel text NOT NULL DEFAULT 'system';
CREATE TABLE IF NOT EXISTS _gonvex_action_outbox (
  id text PRIMARY KEY, action_path text NOT NULL, args jsonb NOT NULL, actor_user_id text,
  actor_email text, provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'pending' CHECK(status IN('pending','processing')),
  attempts integer NOT NULL DEFAULT 0, available_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz, last_error text, created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE _gonvex_action_outbox ADD COLUMN IF NOT EXISTS provenance jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE INDEX IF NOT EXISTS _gonvex_action_outbox_pending ON _gonvex_action_outbox(available_at,created_at)
  WHERE status='pending';
CREATE OR REPLACE FUNCTION gonvex_sync_finalize_transaction() RETURNS trigger AS $$
DECLARE revision_text text; next_revision bigint; current_epoch text; changed_tables text[]; notify_payload jsonb;
BEGIN
  revision_text:=current_setting('gonvex.sync_revision',true);
  IF revision_text IS NULL OR revision_text='' THEN
    UPDATE _gonvex_sync_clock SET revision=revision+1 WHERE singleton=true
      RETURNING revision,epoch INTO next_revision,current_epoch;
    PERFORM set_config('gonvex.sync_revision',next_revision::text,true);
    WITH ranked AS (
      SELECT event_id,row_number() OVER(ORDER BY event_id)::integer row_ordinal
      FROM _gonvex_sync_changes WHERE transaction_id=txid_current()::bigint AND revision IS NULL
    ) UPDATE _gonvex_sync_changes changes SET revision=next_revision,ordinal=ranked.row_ordinal,
      command_id=NULLIF(current_setting('gonvex.command_id',true),'')
      FROM ranked WHERE changes.event_id=ranked.event_id;
    SELECT array_agg(DISTINCT table_name ORDER BY table_name) INTO changed_tables
      FROM _gonvex_sync_changes WHERE transaction_id=txid_current()::bigint AND revision=next_revision;
    INSERT INTO _gonvex_sync_transactions
      (revision,transaction_id,root_command_id,origin_command_id,root_invocation_channel,invocation_channel,
       actor_account_id,actor_member_id,on_behalf_of_member_id,agent_execution_id)
    VALUES(
      next_revision,txid_current()::bigint,
      COALESCE(NULLIF(current_setting('gonvex.root_command_id',true),''),NULLIF(current_setting('gonvex.command_id',true),''),''),
      COALESCE(NULLIF(current_setting('gonvex.command_id',true),''),''),
      COALESCE(NULLIF(current_setting('gonvex.root_invocation_channel',true),''),NULLIF(current_setting('gonvex.invocation_channel',true),''),'system'),
      COALESCE(NULLIF(current_setting('gonvex.invocation_channel',true),''),'system'),
      NULLIF(current_setting('gonvex.actor_account_id',true),''),
      NULLIF(current_setting('gonvex.actor_member_id',true),''),
      NULLIF(current_setting('gonvex.on_behalf_of_member_id',true),''),
      NULLIF(current_setting('gonvex.agent_execution_id',true),'')
    ) ON CONFLICT(revision) DO NOTHING;
    notify_payload:=jsonb_build_object('epoch',current_epoch,'revision',next_revision,'tables',changed_tables);
    IF octet_length(notify_payload::text)>7000 THEN
      notify_payload:=jsonb_build_object('epoch',current_epoch,'revision',next_revision);
    END IF;
    PERFORM pg_notify('gonvex_change_feed',notify_payload::text);
  END IF;
  RETURN NULL;
END; $$ LANGUAGE plpgsql;
"#;
