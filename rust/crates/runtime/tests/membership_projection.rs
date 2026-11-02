use std::collections::BTreeMap;

use gonvex_runtime::membership_projector::project_membership_outbox_once;
use gonvex_store::{ControlPlane, Durability, PoolLimits, PoolRegistry, TenantRoute};

async fn write(pools: &PoolRegistry, url: &str, statement: &str, bind: Option<&str>) {
    let mut transaction = gonvex_store::begin_transaction(pools, url, false)
        .await
        .unwrap();
    let mut query = sqlx::query(statement);
    if let Some(value) = bind {
        query = query.bind(value);
    }
    query.execute(&mut **transaction.transaction()).await.unwrap();
    transaction.commit().await.unwrap();
}

async fn directory_state(pools: &PoolRegistry, control_url: &str) -> (String, i64) {
    sqlx::query_as(
        "SELECT status,tenant_membership_revision FROM account_tenant_index WHERE account_id='account' AND tenant_id='tenant'",
    )
    .fetch_one(&pools.reader(control_url).await.unwrap())
    .await
    .unwrap()
}

#[tokio::test]
async fn membership_projection_is_resumable_and_revision_guarded() {
    let directory = tempfile::tempdir().unwrap();
    let control_url = gonvex_store::database_url(&directory.path().join("control.db"));
    let tenant_url = gonvex_store::database_url(&directory.path().join("tenant.db"));
    let pools = PoolRegistry::with_durability(PoolLimits::default(), Durability::Normal);
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
    write(
        &pools,
        &control_url,
        "INSERT INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES('project','Project','multiTenant','active')",
        None,
    )
    .await;
    write(
        &pools,
        &control_url,
        "INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status) VALUES('tenant','project','tenant','Tenant',$1,'active')",
        Some(&tenant_url),
    )
    .await;
    write(
        &pools,
        &control_url,
        "INSERT INTO accounts(id,auth_realm_id,email,name) VALUES('account','project','actor@example.test','Actor')",
        None,
    )
    .await;
    write(
        &pools,
        &tenant_url,
        "INSERT INTO members(id,account_id,status,display_name) VALUES('member','account','active','Actor')",
        None,
    )
    .await;

    assert!(project_membership_outbox_once(&control, &route)
        .await
        .unwrap());
    assert!(!project_membership_outbox_once(&control, &route)
        .await
        .unwrap());
    assert_eq!(
        directory_state(&pools, &control_url).await,
        ("active".to_owned(), 1)
    );

    write(
        &pools,
        &tenant_url,
        &format!(
            "UPDATE members SET status='revoked',membership_revision=2,updated_at={} WHERE id='member'",
            gonvex_store::NOW_MS
        ),
        None,
    )
    .await;
    project_membership_outbox_once(&control, &route)
        .await
        .unwrap();
    assert_eq!(
        directory_state(&pools, &control_url).await,
        ("revoked".to_owned(), 2)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _gonvex_control_plane_membership_outbox")
            .fetch_one(&pools.reader(&tenant_url).await.unwrap())
            .await
            .unwrap(),
        0
    );
    pools.close().await;
}
