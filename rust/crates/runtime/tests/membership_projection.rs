use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use gonvex_postgres::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
use gonvex_runtime::membership_projector::project_membership_outbox_once;
use sqlx::postgres::PgPoolOptions;

fn scoped_url(base: &str, schema: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}options=-csearch_path%3D{schema}")
}

#[tokio::test]
async fn membership_projection_is_resumable_and_revision_guarded() {
    let Some(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping membership projector test");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let control_schema = format!("gonvex_projector_control_{nonce}");
    let tenant_schema = format!("gonvex_projector_tenant_{nonce}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    for schema in [&control_schema, &tenant_schema] {
        sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
            .execute(&admin)
            .await
            .unwrap();
    }
    let control_url = scoped_url(&base_url, &control_schema);
    let tenant_url = scoped_url(&base_url, &tenant_schema);
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(&control_url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    let route = TenantRoute {
        project_id: "project".to_owned(),
        tenant_id: "tenant".to_owned(),
        database_url: tenant_url.clone(),
    };
    control
        .clone()
        .provision_tenant_database(route.clone(), Vec::new())
        .await
        .unwrap();
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&control_url)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES('project','Project','multiTenant','active')",
    )
    .execute(&fixture)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status) VALUES('tenant','project','tenant','Tenant',$1,'active')",
    )
    .bind(&tenant_url)
    .execute(&fixture)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO accounts(id,auth_realm_id,email,name) VALUES('account','project','actor@example.test','Actor')",
    )
    .execute(&fixture)
    .await
    .unwrap();
    fixture.close().await;

    let tenant = PgPoolOptions::new()
        .max_connections(1)
        .connect(&tenant_url)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO members(id,account_id,status,display_name) VALUES('member','account','active','Actor')",
    )
    .execute(&tenant)
    .await
    .unwrap();
    assert!(project_membership_outbox_once(&control, &route)
        .await
        .unwrap());
    assert!(!project_membership_outbox_once(&control, &route)
        .await
        .unwrap());
    let directory = PgPoolOptions::new()
        .max_connections(1)
        .connect(&control_url)
        .await
        .unwrap();
    let state: (String, i64) = sqlx::query_as(
        "SELECT status,tenant_membership_revision FROM account_tenant_index WHERE account_id='account' AND tenant_id='tenant'",
    )
    .fetch_one(&directory)
    .await
    .unwrap();
    assert_eq!(state, ("active".to_owned(), 1));

    sqlx::query(
        "UPDATE members SET status='revoked',membership_revision=2,updated_at=now() WHERE id='member'",
    )
    .execute(&tenant)
    .await
    .unwrap();
    project_membership_outbox_once(&control, &route)
        .await
        .unwrap();
    let state: (String, i64) = sqlx::query_as(
        "SELECT status,tenant_membership_revision FROM account_tenant_index WHERE account_id='account' AND tenant_id='tenant'",
    )
    .fetch_one(&directory)
    .await
    .unwrap();
    assert_eq!(state, ("revoked".to_owned(), 2));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM _gonvex_control_plane_membership_outbox",
        )
        .fetch_one(&tenant)
        .await
        .unwrap(),
        0
    );

    tenant.close().await;
    directory.close().await;
    pools.close().await;
    for schema in [&tenant_schema, &control_schema] {
        sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .execute(&admin)
            .await
            .unwrap();
    }
    admin.close().await;
}
