//! Startup provisioning isolates a tenant whose database fails: the other
//! tenants keep serving, the broken one is refused with a clear error until a
//! later provisioning succeeds, and a fault on every tenant still fails start.

use gonvex_store::{DatabaseError, MigrationScope, SqlMigration};

use super::*;

async fn control_with_tenants(
    directory: &std::path::Path,
    tenants: &[(&str, &str)],
) -> ControlPlane {
    let control_url = gonvex_store::database_url(&directory.join("control.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(&control_url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    let fixture = pools.pool(&control_url).await.unwrap();
    for (project, tenant) in tenants {
        sqlx::query("INSERT OR IGNORE INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES($1,$1,'multiTenant','active')")
            .bind(project)
            .execute(&fixture)
            .await
            .unwrap();
        let url = gonvex_store::database_url(&directory.join(format!("{project}-{tenant}.db")));
        sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status) VALUES($1,$2,$3,$3,$4,'active')")
            .bind(format!("{project}:{tenant}"))
            .bind(project)
            .bind(tenant)
            .bind(&url)
            .execute(&fixture)
            .await
            .unwrap();
        // Each tenant already has a task table; `beta`'s data breaks the
        // migration below.
        let writer = pools.pool(&url).await.unwrap();
        sqlx::raw_sql(if *tenant == "beta" {
            "CREATE TABLE tasks(id TEXT PRIMARY KEY,title TEXT); INSERT INTO tasks VALUES('1','same'),('2','same');"
        } else {
            "CREATE TABLE tasks(id TEXT PRIMARY KEY,title TEXT); INSERT INTO tasks VALUES('1','one');"
        })
        .execute(&writer)
        .await
        .unwrap();
    }
    control
}

fn unique_titles() -> Vec<SqlMigration> {
    vec![SqlMigration::new(
        "0001_unique_titles.sql".into(),
        MigrationScope::Tenant,
        false,
        "CREATE UNIQUE INDEX tasks_by_title ON tasks(title);".into(),
    )]
}

#[tokio::test]
async fn one_failing_tenant_is_unavailable_while_the_others_serve() {
    let directory = tempfile::tempdir().unwrap();
    let control = control_with_tenants(
        directory.path(),
        &[
            ("project", "alpha"),
            ("project", "beta"),
            ("project", "gamma"),
        ],
    )
    .await;
    let migrations = unique_titles();
    crate::provision_startup_tenants(&control, "project", &migrations)
        .await
        .unwrap();

    for tenant in ["alpha", "gamma"] {
        let route = control.resolve_tenant("project", tenant).await.unwrap();
        let mut transaction = control
            .begin_tenant_transaction(&route, true)
            .await
            .unwrap();
        let migrated: i64 = sqlx::query_scalar("SELECT count(*) FROM gonvex_migrations")
            .fetch_one(&mut **transaction.transaction())
            .await
            .unwrap();
        assert_eq!(migrated, 1, "{tenant}");
    }
    let error = control.resolve_tenant("project", "beta").await.unwrap_err();
    assert!(
        matches!(&error, DatabaseError::TenantUnavailable { tenant } if tenant == "beta"),
        "{error}"
    );
    let available = control.tenant_routes("project").await.unwrap();
    assert_eq!(
        available
            .iter()
            .map(|route| route.tenant_id.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "gamma"]
    );
    let unavailable = control.unavailable_tenants();
    assert_eq!(unavailable.len(), 1);
    assert_eq!(unavailable[0].0, "project:beta");
    assert!(unavailable[0].1.contains("UNIQUE"), "{unavailable:?}");

    // After the operator repairs the data, provisioning (a push) restores it.
    let beta = control
        .all_tenant_routes("project")
        .await
        .unwrap()
        .into_iter()
        .find(|route| route.tenant_id == "beta")
        .unwrap();
    let writer = control.pools().pool(&beta.database_url).await.unwrap();
    sqlx::query("UPDATE tasks SET title='two' WHERE id='2'")
        .execute(&writer)
        .await
        .unwrap();
    control
        .clone()
        .provision_tenant_database(beta, migrations)
        .await
        .unwrap();
    assert!(control.resolve_tenant("project", "beta").await.is_ok());
    assert!(control.unavailable_tenants().is_empty());
}

#[tokio::test]
async fn startup_fails_when_every_tenant_of_a_project_fails() {
    let directory = tempfile::tempdir().unwrap();
    let control = control_with_tenants(directory.path(), &[("solo", "beta")]).await;
    let error = crate::provision_startup_tenants(&control, "solo", &unique_titles())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("UNIQUE"), "{error}");
    // A systemic fault fails start instead of serving nothing.
    assert!(control.unavailable_tenants().is_empty());
}

#[tokio::test]
async fn a_push_provisions_every_healthy_tenant_and_reports_each_outcome() {
    let directory = tempfile::tempdir().unwrap();
    let control = control_with_tenants(
        directory.path(),
        &[
            ("project", "alpha"),
            ("project", "beta"),
            ("project", "gamma"),
        ],
    )
    .await;
    let routes = control.all_tenant_routes("project").await.unwrap();
    let error = crate::provision_pushed_tenants(&control, "project", &routes, &unique_titles())
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(
        error.error.starts_with("1 of 3 tenants failed"),
        "{}",
        error.error
    );
    let tenants = error.tenants.unwrap();
    let statuses = tenants
        .iter()
        .map(|tenant| {
            (
                tenant["tenant"].as_str().unwrap(),
                tenant["status"].as_str().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    // `gamma`, after the failing tenant, is still migrated.
    assert_eq!(
        statuses,
        [
            ("alpha", "provisioned"),
            ("beta", "failed"),
            ("gamma", "provisioned")
        ]
    );
    assert!(tenants[1]["error"].as_str().unwrap().contains("UNIQUE"));
    // A push failure does not take the tenant out of service: it still
    // matches the previous module, which stays installed.
    assert!(control.resolve_tenant("project", "beta").await.is_ok());
}
