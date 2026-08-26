use std::collections::BTreeMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use gonvex_postgres::{
    token_hash, ControlPlane, DatabaseError, MigrationScope, PoolLimits, PoolRegistry,
    SqlMigration, TenantRoute, TransactionAttribution,
};
use sqlx::{postgres::PgPoolOptions, Row};

fn test_database_url() -> Option<String> {
    env::var("GONVEX_TEST_POSTGRES_URL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn scoped_url(base: &str, schema: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}options=-csearch_path%3D{schema}")
}

#[tokio::test]
async fn tenant_database_is_the_final_membership_authority() {
    let Some(base_url) = test_database_url() else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping PostgreSQL isolation test");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let control_schema = format!("gonvex_rust_control_{nonce}");
    let tenant_a_schema = format!("gonvex_rust_tenant_a_{nonce}");
    let tenant_b_schema = format!("gonvex_rust_tenant_b_{nonce}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .expect("connect test PostgreSQL");

    for schema in [&control_schema, &tenant_a_schema, &tenant_b_schema] {
        sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
            .execute(&admin)
            .await
            .expect("create isolated schema");
    }
    let control_url = scoped_url(&base_url, &control_schema);
    let tenant_a_url = scoped_url(&base_url, &tenant_a_schema);
    let tenant_b_url = scoped_url(&base_url, &tenant_b_schema);
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(
        &control_url,
        pools.clone(),
        BTreeMap::from([
            ("project:tenant-a".to_owned(), tenant_a_url.clone()),
            ("project:tenant-b".to_owned(), tenant_b_url.clone()),
        ]),
    )
    .await
    .unwrap();
    for (tenant_id, database_url) in [
        ("tenant-a", tenant_a_url.clone()),
        ("tenant-b", tenant_b_url.clone()),
    ] {
        control
            .clone()
            .provision_tenant_database(
                TenantRoute {
                    project_id: "project".to_owned(),
                    tenant_id: tenant_id.to_owned(),
                    database_url,
                },
                vec![SqlMigration::new(
                    "0001_tasks.sql".to_owned(),
                    MigrationScope::Tenant,
                    false,
                    "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL)".to_owned(),
                )],
            )
            .await
            .unwrap();
    }
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&control_url)
        .await
        .unwrap();
    sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,environment,database_mode,status,auth_mode) VALUES ('project', 'Project', 'test', 'multiTenant', 'active', 'firebase')")
        .execute(&fixture).await.unwrap();
    sqlx::query("INSERT INTO gonvex_runtime_tenants (relationship_id,project_id, tenant_id, name, status, database_url) VALUES ('tenant-a','project', 'tenant-a', 'A', 'active', $1), ('tenant-b','project', 'tenant-b', 'B', 'active', $2)")
        .bind(&tenant_a_url).bind(&tenant_b_url).execute(&fixture).await.unwrap();
    sqlx::query("INSERT INTO accounts (id,auth_realm_id,email,name) VALUES ('account-1','project','member@example.test','Member')")
        .execute(&fixture).await.unwrap();
    sqlx::query("INSERT INTO account_identities(account_id,project_id,provider,issuer,subject,email,verified_email) VALUES ('account-1','project','firebase','https://securetoken.google.com/project','firebase-uid','member@example.test',TRUE)")
        .execute(&fixture)
        .await
        .unwrap();
    // This stale directory row claims access to B. Admission must ignore it.
    sqlx::query(
        "INSERT INTO account_tenant_index(account_id,tenant_id,member_id,status) VALUES ('account-1', 'tenant-b', 'member-b', 'active')",
    )
    .execute(&fixture)
    .await
    .unwrap();
    sqlx::query("INSERT INTO gonvex_auth_sessions (token_hash,project_id,account_id,expires_at) VALUES ($1,'project','account-1',now()+interval '1 hour')")
        .bind(token_hash("gvx_session_rust_test")).execute(&fixture).await.unwrap();
    fixture.close().await;

    let tenant_a = PgPoolOptions::new()
        .max_connections(1)
        .connect(&tenant_a_url)
        .await
        .unwrap();
    sqlx::query("INSERT INTO members (id, account_id, status, role, permissions) VALUES ('member-a', 'account-1', 'active', 'manager', '{\"tasks.read\":true}')")
        .execute(&tenant_a).await.unwrap();
    tenant_a.close().await;
    let tenant_b = PgPoolOptions::new()
        .max_connections(1)
        .connect(&tenant_b_url)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO members (id, account_id, status) VALUES ('member-b', 'account-1', 'revoked')",
    )
    .execute(&tenant_b)
    .await
    .unwrap();
    tenant_b.close().await;

    let (_, member, _) = control
        .admit_member("project", "tenant-a", "account-1")
        .await
        .expect("active membership grants tenant A");
    assert_eq!(member.id, "member-a");
    assert_eq!(member.role, "manager");
    assert_eq!(member.permissions["tasks.read"], true);

    let control_identity = control
        .load_session_identity("gvx_session_rust_test", Some("project"))
        .await
        .expect("session grants Control Plane identity before tenant selection");
    assert_eq!(control_identity.account.id, "account-1");
    assert_eq!(control_identity.account.provider, "firebase");
    let tenant_session = control
        .authenticate_session("gvx_session_rust_test", Some("project"), Some("tenant-a"))
        .await
        .expect("session plus active Member grants tenant admission");
    assert_eq!(tenant_session.member.id, "member-a");
    assert!(matches!(
        control
            .load_session_identity("gvx_session_rust_test", Some("other-project"))
            .await,
        Err(DatabaseError::SessionProjectMismatch)
    ));

    assert!(matches!(
        control
            .admit_member("project", "tenant-b", "account-1")
            .await,
        Err(DatabaseError::MemberNotFound(account)) if account == "account-1"
    ));

    control
        .ensure_reducer_idempotency(&tenant_session.route)
        .await
        .unwrap();
    let mut first = control
        .begin_tenant_transaction(&tenant_session.route, false)
        .await
        .unwrap();
    assert!(first
        .claim_reducer("account-1", "retry-key", "tasks.create")
        .await
        .unwrap());
    sqlx::query("INSERT INTO tasks VALUES ('task-1', 'Created once')")
        .execute(&mut **first.transaction())
        .await
        .unwrap();
    first
        .store_reducer_result(
            "account-1",
            "retry-key",
            &serde_json::json!({"id":"task-1"}),
        )
        .await
        .unwrap();
    first.commit().await.unwrap();
    let mut replay = control
        .begin_tenant_transaction(&tenant_session.route, false)
        .await
        .unwrap();
    assert!(!replay
        .claim_reducer("account-1", "retry-key", "tasks.create")
        .await
        .unwrap());
    replay.rollback().await.unwrap();
    assert_eq!(
        control
            .replay_reducer_result(
                &tenant_session.route,
                "account-1",
                "retry-key",
                "tasks.create",
            )
            .await
            .unwrap(),
        serde_json::json!({"id":"task-1"})
    );

    let mut attributed = control
        .begin_tenant_transaction(&tenant_session.route, false)
        .await
        .unwrap();
    attributed.set_command_id("agent-command").await.unwrap();
    attributed
        .set_invocation_provenance(TransactionAttribution {
            root_command_id: "root-agent-command",
            root_channel: "ui",
            channel: "agent",
            actor_account_id: Some("account-1"),
            actor_member_id: Some("member-a"),
            on_behalf_of_member_id: Some("member-a"),
            agent_execution_id: Some("agent_execution_1"),
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks VALUES ('task-agent', 'Agent change')")
        .execute(&mut **attributed.transaction())
        .await
        .unwrap();
    attributed.commit().await.unwrap();

    let tenant_a = PgPoolOptions::new()
        .max_connections(1)
        .connect(&tenant_a_url)
        .await
        .unwrap();
    let metadata = sqlx::query(
        r#"SELECT transaction.root_command_id,transaction.origin_command_id,
                  transaction.root_invocation_channel,
                  transaction.invocation_channel,transaction.actor_account_id,
                  transaction.actor_member_id,transaction.on_behalf_of_member_id,
                  transaction.agent_execution_id
           FROM _gonvex_sync_transactions transaction
           JOIN _gonvex_sync_changes change ON change.revision=transaction.revision
           WHERE change.table_name='tasks' AND change.row_id='task-agent'"#,
    )
    .fetch_one(&tenant_a)
    .await
    .unwrap();
    assert_eq!(
        metadata.get::<String, _>("root_command_id"),
        "root-agent-command"
    );
    assert_eq!(
        metadata.get::<String, _>("origin_command_id"),
        "agent-command"
    );
    assert_eq!(metadata.get::<String, _>("invocation_channel"), "agent");
    assert_eq!(metadata.get::<String, _>("root_invocation_channel"), "ui");
    assert_eq!(metadata.get::<String, _>("actor_account_id"), "account-1");
    assert_eq!(metadata.get::<String, _>("actor_member_id"), "member-a");
    assert_eq!(
        metadata.get::<String, _>("on_behalf_of_member_id"),
        "member-a"
    );
    assert_eq!(
        metadata.get::<String, _>("agent_execution_id"),
        "agent_execution_1"
    );

    let mut rolled_back = control
        .begin_tenant_transaction(&tenant_session.route, false)
        .await
        .unwrap();
    rolled_back
        .set_command_id("rollback-command")
        .await
        .unwrap();
    rolled_back
        .set_invocation_provenance(TransactionAttribution {
            root_command_id: "rollback-root",
            root_channel: "ui",
            channel: "agent",
            actor_account_id: Some("account-1"),
            actor_member_id: Some("member-a"),
            on_behalf_of_member_id: Some("member-a"),
            agent_execution_id: Some("agent_execution_rollback"),
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks VALUES ('task-rollback', 'Must roll back')")
        .execute(&mut **rolled_back.transaction())
        .await
        .unwrap();
    rolled_back.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks WHERE id='task-rollback'")
            .fetch_one(&tenant_a)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM _gonvex_sync_transactions WHERE root_command_id='rollback-root'",
        )
        .fetch_one(&tenant_a)
        .await
        .unwrap(),
        0
    );
    tenant_a.close().await;

    let mut outbox = control
        .begin_tenant_transaction(&tenant_session.route, false)
        .await
        .unwrap();
    let action_id = outbox
        .enqueue_action(
            "notifications.send",
            &serde_json::json!({"taskId":"task-1"}),
            "account-1",
            "member@example.test",
            &serde_json::json!({
                "rootCommandId":"root-agent-command",
                "channel":"agent",
                "actorAccountId":"account-1",
                "actorMemberId":"member-a"
            }),
        )
        .await
        .unwrap();
    outbox.commit().await.unwrap();
    let claimed = control
        .claim_action(&tenant_session.route)
        .await
        .unwrap()
        .expect("committed outbox action is claimable");
    assert_eq!(claimed.id, action_id);
    assert_eq!(claimed.path, "notifications.send");
    assert_eq!(claimed.actor_account_id, "account-1");
    assert_eq!(claimed.provenance["rootCommandId"], "root-agent-command");
    assert_eq!(claimed.provenance["actorMemberId"], "member-a");
    control
        .complete_action(&tenant_session.route, &claimed.id)
        .await
        .unwrap();

    pools.close().await;
    for schema in [&tenant_b_schema, &tenant_a_schema, &control_schema] {
        sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .execute(&admin)
            .await
            .expect("drop isolated schema");
    }
    admin.close().await;
}
