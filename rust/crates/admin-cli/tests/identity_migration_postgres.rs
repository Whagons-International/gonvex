use std::time::{SystemTime, UNIX_EPOCH};

use gonvex_admin::identity::{
    apply_identity_migration, apply_runtime_migration, connect, inspect_runtime_migration,
    install_identity_schema, plan_identity_migration, verify_identity_migration,
    verify_runtime_migration, LegacyIdentity,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

fn scoped_url(base: &str, schema: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}options=-csearch_path%3D{schema}")
}

#[tokio::test]
async fn identity_migration_converts_tenant_members_and_resumes() {
    let Some(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping identity migration test");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let control_schema = format!("gonvex_identity_control_{nonce}");
    let tenant_schema = format!("gonvex_identity_tenant_{nonce}");
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
    let control = connect(&control_url).await.unwrap();
    sqlx::raw_sql(include_str!("../../postgres/src/control_schema.sql"))
        .execute(&control)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES('project-a','Project','multiTenant','active')",
    )
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status) VALUES('relationship-a','project-a','tenant-a','Tenant',$1,'active')",
    )
    .bind(&tenant_url)
    .execute(&control)
    .await
    .unwrap();
    sqlx::raw_sql(
        r#"CREATE TABLE gonvex_auth_users(id text PRIMARY KEY,project_id text NOT NULL);
           CREATE TABLE gonvex_auth_memberships(user_id text NOT NULL);
           INSERT INTO gonvex_auth_users(id,project_id) VALUES('legacy-a','project-a');
           INSERT INTO gonvex_auth_memberships(user_id) VALUES('legacy-a');"#,
    )
    .execute(&control)
    .await
    .unwrap();
    let tenant = connect(&tenant_url).await.unwrap();
    sqlx::raw_sql(
        r#"CREATE TABLE members(
             user_id text PRIMARY KEY,
             role text NOT NULL DEFAULT 'member',
             permissions jsonb NOT NULL DEFAULT '{}'::jsonb,
             created_at timestamptz NOT NULL DEFAULT now(),
             updated_at timestamptz NOT NULL DEFAULT now()
           );
           INSERT INTO members(user_id,role,permissions)
           VALUES('legacy-a','manager','{"tasks.read":true}');"#,
    )
    .execute(&tenant)
    .await
    .unwrap();
    tenant.close().await;

    let records = vec![LegacyIdentity {
        source: "project-a".to_owned(),
        legacy_user_id: "legacy-a".to_owned(),
        provider: "firebase".to_owned(),
        issuer: "https://securetoken.google.com/project-a".to_owned(),
        subject: "firebase-uid".to_owned(),
        email: "member@example.test".to_owned(),
        email_verified: true,
        name: "Member".to_owned(),
        avatar_url: String::new(),
    }];
    let plan = plan_identity_migration("run-a", "project-a", &records, &[]).unwrap();
    inspect_runtime_migration(&control, &plan).await.unwrap();
    install_identity_schema(&control).await.unwrap();
    apply_identity_migration(&control, &plan, false)
        .await
        .unwrap();
    apply_runtime_migration(&control, &plan).await.unwrap();
    verify_runtime_migration(&control, &plan).await.unwrap();
    assert!(verify_identity_migration(&control, &plan)
        .await
        .unwrap()
        .findings
        .is_empty());

    let tenant = connect(&tenant_url).await.unwrap();
    let member = sqlx::query("SELECT id,account_id,role,permissions FROM members")
        .fetch_one(&tenant)
        .await
        .unwrap();
    assert_eq!(member.get::<String, _>("id"), "legacy-a");
    assert!(member.get::<String, _>("account_id").starts_with("acct_"));
    assert_eq!(member.get::<String, _>("role"), "manager");
    assert_eq!(
        member.get::<serde_json::Value, _>("permissions")["tasks.read"],
        true
    );
    tenant.close().await;

    apply_identity_migration(&control, &plan, false)
        .await
        .unwrap();
    apply_runtime_migration(&control, &plan).await.unwrap();
    verify_runtime_migration(&control, &plan).await.unwrap();

    control.close().await;
    for schema in [&tenant_schema, &control_schema] {
        sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .execute(&admin)
            .await
            .unwrap();
    }
    admin.close().await;
}
