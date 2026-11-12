//! Provision and exercise an application's migrations in a fresh disposable
//! directory using the same store and commit path as the runtime.
//! Usage: schema_probe MIGRATIONS_DIR NEW_OUTPUT_DIR [TRANSACTION_SQL ...]
use gonvex_store::{
    ControlPlane, MigrationScope, PoolLimits, PoolRegistry, SqlMigration, TenantRoute,
};
use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

type ProbeResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> ProbeResult {
    let pools = PoolRegistry::new(PoolLimits::default());
    run(std::env::args_os().skip(1).collect(), &pools).await
}

async fn run(args: Vec<OsString>, pools: &PoolRegistry) -> ProbeResult {
    let result = probe(args, pools).await;
    // Finalize the direct writer's cached statements before SQLx closes its
    // connection or Tokio shuts down. Do this on rejected transactions too;
    // otherwise a close panic can hide the constraint error we must report.
    pools.close().await;
    result
}

async fn probe(args: Vec<OsString>, pools: &PoolRegistry) -> ProbeResult {
    if args.len() < 2 {
        return Err(
            "usage: schema_probe MIGRATIONS_DIR NEW_OUTPUT_DIR [TRANSACTION_SQL ...]".into(),
        );
    }
    let source = PathBuf::from(&args[0]);
    let output = PathBuf::from(&args[1]);
    // Never accept an existing tenant directory as a probe destination.
    std::fs::create_dir(&output)?;
    let mut files = std::fs::read_dir(source)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    files.sort();
    let mut migrations = Vec::new();
    for file in files {
        let name = file.file_name().unwrap().to_string_lossy();
        if file.extension().and_then(|value| value.to_str()) != Some("sql")
            || !name
                .as_bytes()
                .get(..4)
                .is_some_and(|prefix| prefix.iter().all(u8::is_ascii_digit))
        {
            continue;
        }
        let sql = std::fs::read_to_string(&file)?;
        if sql
            .lines()
            .any(|line| line.trim() == "-- gonvex:scope tenant")
        {
            migrations.push(SqlMigration::new(
                name.into_owned(),
                MigrationScope::Tenant,
                false,
                sql,
            ));
        }
    }
    let control_url = gonvex_store::database_url(&output.join("control.sqlite"));
    let control = ControlPlane::connect(&control_url, pools.clone(), BTreeMap::new()).await?;
    let route = TenantRoute {
        project_id: "schema-probe".into(),
        tenant_id: "disposable".into(),
        database_url: gonvex_store::database_url(&output.join("tenant.sqlite")),
    };
    let started = std::time::Instant::now();
    control
        .clone()
        .provision_tenant_database(route.clone(), migrations.clone())
        .await?;
    let provision_ms = started.elapsed().as_millis();
    // Reprovisioning must retain the recorded checksums and be a no-op.
    let started = std::time::Instant::now();
    control
        .clone()
        .provision_tenant_database(route.clone(), migrations.clone())
        .await?;
    let reprovision_ms = started.elapsed().as_millis();
    let mut revisions = Vec::new();
    let mut transaction_ms = Vec::new();
    for script in &args[2..] {
        let sql = std::fs::read_to_string(script)?;
        let started = std::time::Instant::now();
        let mut transaction = control.begin_tenant_transaction(&route, false).await?;
        gonvex_store::execute_script(transaction.transaction(), &sql).await?;
        revisions.push(transaction.commit_with_revision().await?);
        transaction_ms.push(started.elapsed().as_millis());
    }
    let pool = pools.pool(&route.database_url).await?;
    let invalid = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await?;
    if !invalid.is_empty() {
        return Err(format!("{} native relationship violations", invalid.len()).into());
    }
    let records: i64 = sqlx::query_scalar("SELECT count(*) FROM _gonvex_record_views")
        .fetch_one(&pool)
        .await?;
    let changes: i64 = sqlx::query_scalar("SELECT count(*) FROM _gonvex_sync_changes")
        .fetch_one(&pool)
        .await?;
    println!(
        "{}",
        serde_json::json!({"migrations":migrations.len(),"records":records,"changes":changes,"revisions":revisions,"provisionMs":provision_ms,"reprovisionMs":reprovision_ms,"transactionMs":transaction_ms,"frameworkRevision":option_env!("GONVEX_BUILD_COMMIT"),"output":output})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn closes_connections_on_success_and_rejected_transactions() {
        let directory = std::env::var_os("GONVEX_TEST_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!("schema-probe-close-{}", uuid::Uuid::new_v4()));
        let migrations = directory.join("migrations");
        std::fs::create_dir_all(&migrations).unwrap();
        std::fs::write(
            migrations.join("0001_fixture.sql"),
            "-- gonvex:scope tenant\n\
             CREATE TABLE targets(_id TEXT PRIMARY KEY);\n\
             CREATE TABLE sources(_id TEXT PRIMARY KEY, targetId TEXT REFERENCES targets(_id) DEFERRABLE INITIALLY DEFERRED);\n\
             INSERT INTO targets VALUES('p');",
        )
        .unwrap();
        let cases = [
            (
                "success",
                "INSERT INTO sources VALUES('valid','p');",
                None,
                1,
            ),
            (
                "commit",
                "INSERT INTO sources VALUES('valid','p'),('invalid','missing');",
                Some("FOREIGN KEY"),
                0,
            ),
            (
                "statement",
                "INSERT INTO sources VALUES('valid','p'); INSERT INTO absent VALUES('x');",
                Some("no such table"),
                0,
            ),
        ];
        for (name, sql, expected, rows) in cases {
            let pools = PoolRegistry::new(PoolLimits::default());
            let output = directory.join(name);
            let script = directory.join(format!("{name}.sql"));
            std::fs::write(&script, sql).unwrap();
            let result = run(
                vec![
                    migrations.clone().into(),
                    output.clone().into(),
                    script.into(),
                ],
                &pools,
            )
            .await;
            if let Some(message) = expected {
                assert!(result.unwrap_err().to_string().contains(message));
            } else {
                result.unwrap();
            }
            assert_eq!(
                pools.snapshot().await.open_connections,
                0,
                "{name}: probe returned with connections still open"
            );
            // Reopen after shutdown: rejected transactions must leave no partial
            // application writes, while successful writes remain durable.
            let pool = pools
                .pool(&gonvex_store::database_url(&output.join("tenant.sqlite")))
                .await
                .unwrap();
            let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM sources")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(stored, rows, "{name}: unexpected persisted rows");
            pools.close().await;
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}
