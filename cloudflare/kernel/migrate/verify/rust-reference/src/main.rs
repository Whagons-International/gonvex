//! gonvex-k02-reference <migrations-dir> <data-dir>
//!
//! Creates a Control Plane, a multi-tenant project and one tenant under
//! <data-dir>, then provisions the tenant with every `NNNN_*.sql` file of
//! <migrations-dir> through `ControlPlane::provision_tenant_database`, the
//! path `/dev/sync` and new tenants take. Prints JSON: the tenant database
//! path and the provision time in ms. `K02_PROVISION_TWICE=1` provisions it a
//! second time, as every later sync and runtime start does.
use std::collections::BTreeMap;
use std::time::Instant;

use gonvex_store::{ControlPlane, MigrationScope, PoolLimits, PoolRegistry, SqlMigration};

/// rust/crates/runtime/src/modules.rs `migrations_from_module`, minus base64.
fn parse(name: &str, source: String) -> SqlMigration {
    let valid = name.len() >= 10
        && name.as_bytes()[..4].iter().all(u8::is_ascii_digit)
        && name.as_bytes().get(4) == Some(&b'_')
        && name.ends_with(".sql")
        && name[5..name.len() - 4].bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'));
    assert!(valid, "migration {name:?} must be named NNNN_description.sql");
    let mut scope = None;
    let mut no_transaction = false;
    for line in source.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(directive) = line.strip_prefix("--") else { break };
        let directive = directive.trim();
        if let Some(value) = directive.strip_prefix("gonvex:scope") {
            scope = Some(match value.trim() {
                "tenant" => MigrationScope::Tenant,
                "control-plane" => MigrationScope::ControlPlane,
                value => panic!("migration {name}: invalid scope {value:?}"),
            });
        } else if directive == "gonvex:no-transaction" {
            no_transaction = true;
        }
    }
    let scope = scope.unwrap_or_else(|| panic!("migration {name}: missing scope directive"));
    SqlMigration::new(name.to_owned(), scope, no_transaction, source)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: gonvex-k02-reference <migrations-dir> <data-dir>");
        std::process::exit(2);
    }
    let mut migrations = Vec::new();
    for entry in std::fs::read_dir(&args[1]).expect("read migrations dir") {
        let path = entry.expect("dir entry").path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if !name.ends_with(".sql") {
            continue;
        }
        migrations.push(parse(&name, std::fs::read_to_string(&path).expect("read migration")));
    }
    migrations.sort_by(|a, b| a.name.cmp(&b.name));
    let data_dir = std::path::Path::new(&args[2]).canonicalize().expect("data dir");
    let data = data_dir.to_string_lossy().into_owned();
    let pools = PoolRegistry::new(PoolLimits::default());
    let control_url = format!("sqlite://{}/control.sqlite3?mode=rwc", data);
    let control = ControlPlane::connect(&control_url, pools, BTreeMap::new()).await.expect("control plane");
    let project = control.create_runtime_project(&data, "k02 reference", "multiTenant", "").await.expect("project");
    let project_id = project["project"]["id"].as_str().unwrap().to_owned();
    let (route, _) = control
        .create_runtime_tenant(&data, &project_id, Some("k02-reference"), "K02 reference")
        .await
        .expect("tenant");
    let started = Instant::now();
    control.clone().provision_tenant_database(route.clone(), migrations.clone()).await.expect("provision");
    let ms = started.elapsed().as_millis();
    // K02_PROVISION_TWICE=1: provision the same tenant again, as every later /dev/sync and
    // runtime start does, to compare a re-provisioned tenant.
    let again_ms = if std::env::var("K02_PROVISION_TWICE").as_deref() == Ok("1") {
        let again = Instant::now();
        control.clone().provision_tenant_database(route.clone(), migrations).await.expect("provision again");
        Some(again.elapsed().as_millis())
    } else {
        None
    };
    println!("{}", serde_json::json!({ "databaseUrl": route.database_url, "provisionMs": ms, "reprovisionMs": again_ms }));
}
